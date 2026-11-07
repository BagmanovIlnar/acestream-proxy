use crate::engine::{Engine, EngineStream};
use crate::playlist::PlaylistHistory;
use crate::transcode::{CodecInfo, Transcodes};
use crate::util::{content_id, fnv64};
use bytes::Bytes;
use futures_util::StreamExt;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI32};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, OnceCell};

/// Страница каталога /search. 200 движок 3.2.11 не успевает собрать за таймаут
/// (однопоточный python, поиск живой) — каждая страница падала, каталог пустой.
/// 50 — меньше работы на запрос; «короткая страница» = движок обрезал.
const PAGE_SIZE: u32 = 50;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ClientKind {
    Ts,
    Hls,
    Browser,
}
impl ClientKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ClientKind::Ts => "ts",
            ClientKind::Hls => "hls",
            ClientKind::Browser => "browser",
        }
    }
}

pub struct Client {
    pub ip: String,
    pub ua: String,
    pub kind: ClientKind,
    pub since: Instant,
    pub last_seen: Instant,
    pub conns: u32,
}

pub struct Stream {
    pub id: String,
    pub content: String,
    pub engine: EngineStream,
    pub created: Instant,
    /// Fan-out of the engine TS feed to all TS clients.
    pub ts_tx: broadcast::Sender<Bytes>,
    pub clients: Mutex<HashMap<u64, Client>>,
    pub pump: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    pub probe: OnceCell<CodecInfo>, // только успех; неудача не кэшируется (get_or_try_init)
    /// Плеер просил полный транскод (?enc=1 после битого remux) — помним,
    /// иначе чужой запрос без параметра откатит стрим в copy и вернёт глитчи
    pub force_encode: AtomicBool,
    /// Выбранная аудиодорожка (?at=N из плеера), тоже липкая
    pub audio_track: AtomicI32,
    pub empty_since: Mutex<Option<Instant>>,
}

impl Stream {
    /// HLS playlist url, if the engine provides one.
    pub fn hls_index_url(&self) -> Option<&str> {
        let a = &self.engine.playback_url;
        let b = &self.engine.playback_url_for_external_player;
        if a.ends_with(".m3u8") {
            Some(a)
        } else if b.ends_with(".m3u8") {
            Some(b)
        } else {
            None
        }
    }

    /// Upstream dir (with trailing slash) that relative segment urls resolve against.
    pub fn hls_dir(&self) -> String {
        match self.hls_index_url() {
            Some(u) => match u.rfind('/') {
                Some(i) => format!("{}/", &u[..i]),
                None => u.to_string(),
            },
            None => String::new(),
        }
    }

    /// Register/refresh a client. Client identity = ip + user agent.
    pub fn touch(&self, ip: &str, ua: &str, kind: ClientKind) {
        let id = fnv64(&format!("{ip}|{ua}"));
        let now = Instant::now();
        let mut c = self.clients.lock().unwrap();
        c.entry(id)
            .and_modify(|e| {
                e.last_seen = now;
                if kind == ClientKind::Ts {
                    e.conns += 1;
                }
            })
            .or_insert_with(|| Client {
                ip: ip.to_string(),
                ua: ua.to_string(),
                kind,
                since: now,
                last_seen: now,
                conns: if kind == ClientKind::Ts { 1 } else { 0 },
            });
        *self.empty_since.lock().unwrap() = None;
    }

    fn release_ts(&self, ip: &str, ua: &str) {
        let id = fnv64(&format!("{ip}|{ua}"));
        let mut c = self.clients.lock().unwrap();
        if let Some(e) = c.get_mut(&id) {
            if e.conns > 0 {
                e.conns -= 1;
            }
            if e.conns == 0 && e.kind == ClientKind::Ts {
                c.remove(&id);
            }
        }
    }
}

/// Deregisters a TS client when the connection body is dropped.
pub struct TsGuard {
    pub stream: Arc<Stream>,
    pub ip: String,
    pub ua: String,
}
impl Drop for TsGuard {
    fn drop(&mut self) {
        self.stream.release_ts(&self.ip, &self.ua);
    }
}

fn env_secs(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

pub struct App {
    pub cfg: crate::Config,
    pub engine: Engine,
    pub http: reqwest::Client,
    pub streams: RwLock<HashMap<String, Arc<Stream>>>,
    pub playlist: PlaylistHistory,
    pub transcodes: Transcodes,
    /// guard: engine search seed in progress
    seeding: std::sync::atomic::AtomicBool,
    /// rebuild запрошен пока сидер ещё работает — он повторит сбор по завершении
    reseed: std::sync::atomic::AtomicBool,
}

impl App {
    pub fn new(cfg: crate::Config) -> Self {
        let engine = Engine::new(&cfg.engine_url);
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            // движок зависает на запросах без UA и рвёт keep-alive: без пула и с UA,
            // как Engine::new — этот клиент тоже ходит в движок (HLS passthrough)
            .user_agent(concat!("acestream-proxy/", env!("CARGO_PKG_VERSION")))
            .pool_max_idle_per_host(0)
            .build()
            .expect("reqwest client");
        let transcodes = Transcodes::new(cfg.transcode_limit);
        let playlist = PlaylistHistory::new(&cfg.tmp_dir, cfg.playlist_ttl_secs);
        App {
            cfg,
            engine,
            http,
            streams: RwLock::new(HashMap::new()),
            playlist,
            transcodes,
            seeding: std::sync::atomic::AtomicBool::new(false),
            reseed: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub fn is_seeding(&self) -> bool {
        self.seeding.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Catalog sweeper: rebuilds /api/channels from the engine (search), forever.
    /// The source is a live p2p index — the engine randomly drops/truncates pages,
    /// so a single run under-collects by a random amount (the floating channel
    /// count between restarts). Sweeping every 30 min and merging (dedup +
    /// last_seen refresh; ensure_fresh prunes the dead via TTL) converges the
    /// count and heals any single under-collected run within one pause.
    pub fn kick_seed(self: &Arc<Self>, resweep: bool) {
        use std::sync::atomic::Ordering;
        if resweep {
            // явный rebuild: текущий сбор перезапустить немедленно
            self.reseed.store(true, Ordering::Relaxed);
        }
        if self.seeding.swap(true, Ordering::Relaxed) {
            return; // свипер уже бежит (флаг живёт весь срок жизни процесса)
        }
        let app = self.clone();
        self.reseed.store(false, Ordering::Relaxed);
        tokio::spawn(async move {
            // полный каталог через /search пагинацией (PAGE_SIZE позиций/страница).
            // Страницы нестабильны (движок случайно роняет/обрезает их), поэтому
            // один прогон даёт случайный недобор — собираем его в merge()
            // (дедуипликация), повторяем витки каждые 30 минут; ensure_fresh(TTL)
            // вымывает мёртвых — счёт сходится к «живым за TTL».
            // Пустая страница = конец каталога, недозаполненная = переспрашиваем.
            // Задержки — env (сек): ACESTREAM_SEED_DELAY_SECS (между страницами,
            // раунд = 3×) и ACESTREAM_SEED_PAUSE_SECS (между витками).
            loop {
                let delay = env_secs("ACESTREAM_SEED_DELAY_SECS", 10);
                let pause = env_secs("ACESTREAM_SEED_PAUSE_SECS", 1800);
                let mut todo: Vec<u32> = (0..20).collect();
                for _ in 0..6 {
                    if todo.is_empty() || app.reseed.load(Ordering::Relaxed) {
                        break;
                    }
                    let token = match app.engine.api_token().await {
                        Ok(t) => t,
                        Err(e) => {
                            tracing::warn!("seed: token failed: {e}");
                            break;
                        }
                    };
                    let mut retry: Vec<u32> = Vec::new();
                    for &page in &todo {
                        if app.reseed.load(Ordering::Relaxed) {
                            break; // rebuild: бросаем виток, полный сбор перезапустится
                        }
                        // движок — однопоточный python-сервер, рвёт соединения при плотном темпе:
                        // две попытки на месте и большая пауза между страницами
                        let mut got: Option<serde_json::Value> = None;
                        for attempt in 0..2 {
                            if attempt > 0 {
                                tokio::time::sleep(Duration::from_secs(5)).await;
                            }
                            match app.engine.search_page(&token, None, None, page, PAGE_SIZE).await {
                                Ok(raw) => {
                                    got = Some(raw);
                                    break;
                                }
                                Err(e) => tracing::warn!("seed page {page} failed: {e}"),
                            }
                        }
                        match got {
                            Some(raw) => {
                                let n = crate::playlist::search_results_len(&raw);
                                app.playlist
                                    .merge(crate::playlist::channels_from_search(&raw));
                                if n == 0 {
                                    retry.retain(|p| *p <= page); // страница пуста — каталог кончился
                                } else if n < PAGE_SIZE as usize {
                                    retry.push(page); // движок мог обрезать страницу — переспросим (merge дедуиплицирует)
                                }
                            }
                            None => retry.push(page),
                        }
                        tokio::time::sleep(Duration::from_secs(delay)).await;
                    }
                    todo = retry;
                    if !todo.is_empty() {
                        tokio::time::sleep(Duration::from_secs(delay * 3)).await;
                    }
                }
                // rebuild (в т.ч. во время сбора) — сразу заново с первой страницы
                if app.reseed.swap(false, Ordering::Relaxed) {
                    tracing::info!("seed: reseed requested, rerun");
                    continue;
                }
                if !todo.is_empty() {
                    tracing::warn!("seed: pages not fetched, next sweep retries: {todo:?}");
                }
                tracing::info!(
                    "playlist: sweep done, {} channels, next sweep in {pause}s",
                    app.playlist.snapshot().len()
                );
                // пауза между витками; rebuild прерывает её немедленно (секундными тиками)
                for _ in 0..pause {
                    if app.reseed.swap(false, Ordering::Relaxed) {
                        tracing::info!("seed: reseed requested, rerun");
                        break;
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        });
    }

    /// Resolve user input into engine content:
    /// active stream id | playlist channel id | raw content (32-hex / url / magnet).
    pub fn resolve_input(&self, input: &str) -> Result<String, String> {
        let t = input.trim();
        if let Some(s) = self.streams.read().unwrap().get(t) {
            return Ok(s.content.clone());
        }
        if let Some(ch) = self.playlist.by_id(t) {
            return Ok(ch.url);
        }
        let bare = t.strip_prefix("acestream://").unwrap_or(t);
        // 32-hex content id or 40-hex infohash (engine search results)
        if (bare.len() == 32 || bare.len() == 40) && bare.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Ok(t.to_string());
        }
        if t.starts_with("http://") || t.starts_with("https://") || t.starts_with("magnet:") {
            return Ok(t.to_string());
        }
        Err(format!("unknown stream/channel/content: {input}"))
    }

    pub fn find(&self, id: &str) -> Option<Arc<Stream>> {
        self.streams.read().unwrap().get(id).cloned()
    }

    /// Loopback TS url: the engine serves ONE downstream client per session, so
    /// probe/ffmpeg must consume the same broadcast as everyone else (a direct
    /// second engine connection fails with "failed to load content").
    pub fn self_stream_url(&self, id: &str) -> String {
        format!("http://127.0.0.1:{}/stream/{id}", self.cfg.port)
    }

    /// Get or create the engine session for the input (any of resolve_input forms).
    pub async fn get_or_start(&self, input: &str) -> Result<Arc<Stream>, String> {
        let content = self.resolve_input(input)?;
        let id = content_id(&content);
        if let Some(s) = self.find(&id) {
            return Ok(s);
        }
        // start the engine session without holding locks (guard across await = !Send)
        let es = self.engine.start(&content).await?;
        let (ts_tx, _) = broadcast::channel(1024);
        let s = Arc::new(Stream {
            id: id.clone(),
            content: content.clone(),
            engine: es,
            created: Instant::now(),
            ts_tx,
            clients: Mutex::new(HashMap::new()),
            pump: tokio::sync::Mutex::new(None),
            probe: OnceCell::new(),
            force_encode: AtomicBool::new(false),
            audio_track: AtomicI32::new(0),
            empty_since: Mutex::new(None),
        });
        // ponytail: on a race the loser's engine session is orphaned; engine's own
        // idle timeout reaps it
        let winner = {
            let mut w = self.streams.write().unwrap();
            match w.entry(id.clone()) {
                std::collections::hash_map::Entry::Occupied(e) => Arc::clone(e.get()),
                std::collections::hash_map::Entry::Vacant(e) => {
                    e.insert(Arc::clone(&s));
                    s.clone()
                }
            }
        };
        if Arc::ptr_eq(&winner, &s) {
            tracing::info!(stream = %id, content = %content, "stream started");
        }
        // self-generated playlist: remember every started stream
        self.playlist.track(&content, None);
        Ok(winner)
    }

    pub async fn kill_stream(&self, id: &str) -> bool {
        let Some(s) = ({ self.streams.write().unwrap().remove(id) }) else {
            return false;
        };
        if let Some(h) = s.pump.lock().await.take() {
            h.abort();
        }
        self.transcodes.kill(id).await;
        // ponytail: engine session is not force-stopped via API (endpoint not documented);
        // engine's own idle timeout reaps it after our connections drop.
        tracing::info!(stream = %id, "stream stopped");
        true
    }

    /// (Re)start the task pumping engine TS bytes into the broadcast channel.
    pub async fn ensure_pump(self: &Arc<Self>, s: &Arc<Stream>) {
        let mut p = s.pump.lock().await;
        if let Some(h) = &*p {
            if !h.is_finished() {
                return;
            }
        }
        let app = Arc::clone(self);
        let st = Arc::clone(s);
        *p = Some(tokio::spawn(pump(app, st)));
    }
}

/// Pull the engine TS feed and fan it out. Exits silently when no TS clients
/// remain (stream stays alive for HLS/browser clients). Kills the stream when
/// the upstream itself dies.
async fn pump(app: Arc<App>, s: Arc<Stream>) {
    let url = app.engine.ts_session_url(&s.content); // sessionless: survives engine session expiry
    let resp = match app.http.get(&url).send().await {
        Ok(r) if r.status().is_success() => r,
        Ok(r) => {
            tracing::warn!(stream = %s.id, status = r.status().as_u16(), "ts pump http error");
            let _ = app.streams.write().unwrap().remove(&s.id);
            app.transcodes.kill(&s.id).await;
            return;
        }
        Err(e) => {
            tracing::warn!(stream = %s.id, err = %e, "ts pump connect failed");
            let _ = app.streams.write().unwrap().remove(&s.id);
            app.transcodes.kill(&s.id).await;
            return;
        }
    };
    let mut body = resp.bytes_stream();
    let mut upstream_ended = false;
    loop {
        match body.next().await {
            Some(Ok(b)) => {
                if s.ts_tx.send(b).is_err() {
                    break; // no TS clients right now; stream stays for hls/browser
                }
            }
            Some(Err(e)) => {
                tracing::warn!(stream = %s.id, err = %e, "ts pump read error");
                break;
            }
            None => {
                upstream_ended = true;
                break;
            }
        }
    }
    if upstream_ended {
        // upstream closed: the session is dead, drop the stream so clients restart it
        let _ = app.streams.write().unwrap().remove(&s.id);
        app.transcodes.kill(&s.id).await;
        tracing::info!(stream = %s.id, "ts pump: upstream ended, stream dropped");
    }
}

/// Periodic cleanup: stale hls/browser clients, idle streams.
pub async fn janitor(app: Arc<App>) {
    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;
        let now = Instant::now();
        let snapshot: Vec<Arc<Stream>> = app.streams.read().unwrap().values().cloned().collect();
        for s in snapshot {
            {
                let mut c = s.clients.lock().unwrap();
                c.retain(|_, e| {
                    (e.kind == ClientKind::Ts && e.conns > 0)
                        || now.duration_since(e.last_seen).as_secs() < app.cfg.stale_secs
                });
            }
            // транскод без браузерных зрителей = жжём CPU впустую (и слот лимита)
            let has_viewers = s.clients.lock().unwrap().values().any(|c| c.kind != ClientKind::Ts);
            if !has_viewers && app.transcodes.contains_live(&s.id).await {
                app.transcodes.kill(&s.id).await;
                tracing::info!(stream = %s.id, "transcode killed: no viewers");
            }
            let should_kill = {
                let empty = s.clients.lock().unwrap().is_empty();
                let has_transcode = app.transcodes.contains_live(&s.id).await;
                if empty && !has_transcode {
                    let mut es = s.empty_since.lock().unwrap();
                    if es.is_none() {
                        *es = Some(now);
                        false
                    } else {
                        now.duration_since(es.unwrap()).as_secs() >= app.cfg.idle_secs
                    }
                } else {
                    *s.empty_since.lock().unwrap() = None;
                    false
                }
            };
            if should_kill {
                app.kill_stream(&s.id).await;
            }
        }
        app.playlist.ensure_fresh(app.cfg.playlist_ttl_secs);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrd};

    // страница ответа мок-движка: n позиций со смещением off (уникальные infohash)
    fn page(n: usize, off: usize) -> serde_json::Value {
        let items: Vec<_> = (0..n)
            .map(|i| {
                serde_json::json!({
                    "name": format!("ch{}", i + off),
                    "infohash": format!("{:040x}", i + off + 1),
                    "categories": ["tv"],
                })
            })
            .collect();
        serde_json::json!({"result": {"results": [{"name": "cat", "items": items}]}})
    }

    async fn wait_for(f: impl Fn() -> bool, secs: u64) -> bool {
        let dl = tokio::time::Instant::now() + std::time::Duration::from_secs(secs);
        while tokio::time::Instant::now() < dl {
            if f() {
                return true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        f()
    }

    /// Свипер против мок-движка: сбор пагинацией, мгновенный пересбор по
    /// rebuild во время паузы, восстановление после двойного нажатия.
    #[tokio::test]
    async fn sweeper_collects_and_rebuilds_immediately() {
        std::env::set_var("ACESTREAM_SEED_DELAY_SECS", "0");
        std::env::set_var("ACESTREAM_SEED_PAUSE_SECS", "1");

        let fetches = Arc::new(AtomicUsize::new(0));
        // p0 = полная страница (PAGE_SIZE), p1 = короткая, p2+ = пусто (конец каталога):
        // виток = 8 запросов (p1 переспрашивается до лимита раундов)
        let f = fetches.clone();
        let mock = axum::Router::new()
            .route(
                "/server/api",
                axum::routing::get(|| async {
                    axum::Json(serde_json::json!({"token": "t"}))
                }),
            )
            .route(
                "/search",
                axum::routing::get(
                    move |q: axum::extract::Query<std::collections::HashMap<String, String>>| {
                        f.fetch_add(1, AtomicOrd::Relaxed);
                        let page_no: u32 = q.get("page").and_then(|p| p.parse().ok()).unwrap_or(0);
                        async move {
                            axum::Json(match page_no {
                                0 => page(PAGE_SIZE as usize, 0),
                                1 => page(10, 1000),
                                _ => serde_json::json!({"result": {"results": []}}),
                            })
                        }
                    },
                ),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });

        let tmp = std::env::temp_dir().join(format!("ace-sweeper-test-{}", std::process::id()));
        let cfg = crate::Config {
            engine_url: format!("http://{addr}"),
            port: 0,
            playlist_ttl_secs: 3600,
            idle_secs: 30,
            stale_secs: 60,
            transcode_limit: 2,
            tmp_dir: tmp.to_string_lossy().into_owned(),
            public_url: None,
        };
        let app = Arc::new(App::new(cfg));

        // 1) старт: свипер собирает каталог (PAGE_SIZE + 10) и уходит в паузу
        app.kick_seed(false);
        assert!(
            wait_for(|| app.playlist.snapshot().len() == (PAGE_SIZE as usize + 10), 10).await,
            "sweep must collect the full catalog"
        );
        let after_sweep1 = fetches.load(AtomicOrd::Relaxed);

        // 2) rebuild во время паузы: новый виток стартует сразу (~1с тик),
        //    а не после полной паузы
        app.kick_seed(true);
        assert!(
            wait_for(|| fetches.load(AtomicOrd::Relaxed) >= after_sweep1 + 8, 10).await,
            "rebuild during pause must rerun the sweep immediately"
        );

        // 3) двойное нажатие rebuild: флаг один, но каталог пересобирается заново
        app.playlist.reset();
        app.kick_seed(true);
        app.kick_seed(true);
        assert!(
            wait_for(
                || app.playlist.snapshot().len() == (PAGE_SIZE as usize + 10),
                10
            )
            .await,
            "double rebuild must still recollect the catalog"
        );
    }
}
