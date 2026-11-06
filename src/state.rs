use crate::engine::{Engine, EngineStream};
use crate::playlist::PlaylistHistory;
use crate::transcode::{CodecInfo, Transcodes};
use crate::util::{content_id, fnv64};
use bytes::Bytes;
use futures_util::StreamExt;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, OnceCell};

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
    pub probe: OnceCell<Result<CodecInfo, String>>,
    pub empty_since: Mutex<Option<Instant>>,
}

impl Stream {
    /// MPEG-TS url (by extension; falls back between the two engine urls).
    pub fn ts_url(&self) -> &str {
        let a = &self.engine.playback_url;
        let b = &self.engine.playback_url_for_external_player;
        if b.ends_with(".ts") || (a.ends_with(".m3u8") && !b.is_empty()) {
            b
        } else {
            a
        }
    }

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

pub struct App {
    pub cfg: crate::Config,
    pub engine: Engine,
    pub http: reqwest::Client,
    pub streams: RwLock<HashMap<String, Arc<Stream>>>,
    pub playlist: PlaylistHistory,
    pub transcodes: Transcodes,
    /// guard: engine search seed in progress
    seeding: std::sync::atomic::AtomicBool,
}

impl App {
    pub fn new(cfg: crate::Config) -> Self {
        let engine = Engine::new(&cfg.engine_url);
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
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
        }
    }

    /// Seed the playlist with live public broadcasts from the engine (search).
    /// Runs in the background (the engine wedges its /server/api for ~30s under
    /// request bursts, so queries are spaced out); fires when the cache is stale.
    /// ponytail: 5 fixed genre queries × 10 items — the engine has no pagination,
    /// full catalog needs an external m3u source.
    pub fn kick_seed(self: &Arc<Self>) {
        use std::sync::atomic::Ordering;
        if !self.playlist.cache_stale(self.cfg.playlist_ttl_secs) {
            return;
        }
        if self.seeding.swap(true, Ordering::Relaxed) {
            return;
        }
        let app = self.clone();
        tokio::spawn(async move {
            // up to 3 passes: the engine's /server/api randomly drops some queries,
            // merge() dedupes so re-running a pass is idempotent
            for _ in 0..3 {
                let mut failed = false;
                let token = match app.engine.api_token().await {
                    Ok(t) => t,
                    Err(e) => {
                        tracing::warn!("seed: token failed: {e}");
                        break;
                    }
                };
                for q in ["", "sport", "news", "movies", "kids"] {
                    match app.engine.search_with(&token, Some(q)).await {
                        Ok(raw) => app
                            .playlist
                            .merge(crate::playlist::channels_from_search(&raw)),
                        Err(e) => {
                            failed = true;
                            tracing::warn!("seed search '{q}' failed: {e}");
                        }
                    }
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
                if !failed {
                    break;
                }
                tokio::time::sleep(Duration::from_secs(20)).await;
            }
            app.seeding.store(false, Ordering::Relaxed);
            tracing::info!("playlist: seeded, {} channels", app.playlist.snapshot().len());
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
    let url = s.ts_url().to_string();
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
            let should_kill = {
                let empty = s.clients.lock().unwrap().is_empty();
                let has_transcode = app.transcodes.contains(&s.id);
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
