use crate::state::{App, Stream};
use crate::transcode;
use crate::util::{fnv64, truncate};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;
use utoipa::{IntoParams, ToSchema};

pub struct ApiError(pub StatusCode, pub String);
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(serde_json::json!({ "error": self.1 }))).into_response()
    }
}

fn e(status: StatusCode, msg: impl Into<String>) -> ApiError {
    ApiError(status, msg.into())
}

// ---------- schemas ----------

#[derive(Serialize, ToSchema)]
pub struct ClientInfo {
    pub id: String,
    pub ip: String,
    pub user_agent: String,
    /// ts | hls | browser
    pub kind: String,
    pub watching_secs: u64,
    pub last_seen_secs: u64,
}

#[derive(Serialize, ToSchema)]
pub struct StreamInfo {
    pub id: String,
    pub content: String,
    pub uptime_secs: u64,
    pub clients: Vec<ClientInfo>,
    /// Filled after first probe (ffprobe)
    pub video: Option<String>,
    pub audio_tracks: Vec<transcode::AudioTrack>,
}

#[derive(Deserialize, ToSchema)]
pub struct StartStreamReq {
    /// Content id (32 hex), acestream://..., magnet:..., http(s)://...
    pub content: String,
    /// Optional channel name for the playlist (defaults to content)
    pub name: Option<String>,
}

#[derive(Serialize, ToSchema)]
pub struct AudioInfo {
    /// true when the stream can be played in a browser without transcoding
    pub passthrough: bool,
    pub tracks: Vec<transcode::AudioTrack>,
}

#[derive(Serialize, ToSchema)]
pub struct ClientRow {
    pub id: String,
    pub ip: String,
    pub user_agent: String,
    pub kind: String,
    pub watching_secs: u64,
    pub stream_id: String,
    pub content: String,
}

#[derive(Serialize, ToSchema)]
pub struct CategoryInfo {
    pub name: String,
    pub count: u32,
}

#[derive(Serialize, ToSchema)]
pub struct HealthInfo {
    pub status: String,
    pub engine: bool,
    pub version: String,
    pub active_streams: usize,
    pub clients: usize,
    /// Сбор каталога каналов идёт в фоне
    pub seeding: bool,
}

#[derive(Deserialize, IntoParams)]
pub struct ChannelsQuery {
    /// Engine category filter (e.g. tv, sport, movies, documentary, news, music)
    pub category: Option<String>,
    /// Case-insensitive substring filter by tvg-country
    pub country: Option<String>,
}

#[derive(Deserialize, IntoParams)]
pub struct SearchQuery {
    /// Search text (empty = default/popular list)
    pub q: Option<String>,
}

#[derive(Serialize, ToSchema)]
pub struct SearchItem {
    pub name: String,
    /// Content id — pass to POST /api/streams or /stream/{id}
    pub content_id: String,
    pub bitrate: Option<u64>,
}

#[derive(Serialize, ToSchema)]
pub struct SearchResult {
    pub items: Vec<SearchItem>,
    /// Raw engine response (fields may vary by engine version)
    pub raw: serde_json::Value,
}

fn extract_search_items(v: &serde_json::Value) -> Vec<SearchItem> {
    let arr = v
        .pointer("/result/results")
        .and_then(|x| x.as_array())
        .or_else(|| v.get("results").and_then(|x| x.as_array()))
        .cloned()
        .unwrap_or_default();
    // engine returns [{name, items: [...]}, ...]; flatten (self item if no nesting)
    let flat = arr
        .iter()
        .flat_map(|c| {
            c.get("items")
                .and_then(|x| x.as_array())
                .cloned()
                .unwrap_or_default()
        })
        .chain(arr.iter().filter(|c| c.get("items").is_none()).cloned());
    flat.filter_map(|i| {
        let cid = i
            .get("content_id")
            .and_then(|x| x.as_str())
            .or_else(|| i.get("id").and_then(|x| x.as_str()))
            .or_else(|| i.get("infohash").and_then(|x| x.as_str()))?;
        let name = i
            .get("name")
            .and_then(|x| x.as_str())
            .or_else(|| i.get("title").and_then(|x| x.as_str()))
            .unwrap_or(cid)
            .to_string();
        Some(SearchItem {
            name,
            content_id: cid.to_string(),
            bitrate: i.get("bitrate").and_then(|x| x.as_u64()),
        })
    })
    .collect()
}

fn stream_info(s: &Stream) -> StreamInfo {
    let now = Instant::now();
    let clients = s
        .clients
        .lock()
        .unwrap()
        .values()
        .map(|c| ClientInfo {
            id: format!("{:016x}", fnv64(&format!("{}|{}", c.ip, c.ua))),
            ip: c.ip.clone(),
            user_agent: truncate(&c.ua, 120),
            kind: c.kind.as_str().to_string(),
            watching_secs: now.duration_since(c.since).as_secs(),
            last_seen_secs: now.duration_since(c.last_seen).as_secs(),
        })
        .collect();
    let (video, audio) = match s.probe.get() {
        Some(ci) => (ci.video.clone(), ci.audio.clone()),
        None => (None, Vec::new()),
    };
    StreamInfo {
        id: s.id.clone(),
        content: truncate(&s.content, 200),
        uptime_secs: now.duration_since(s.created).as_secs(),
        clients,
        video,
        audio_tracks: audio,
    }
}

// ---------- handlers ----------

#[utoipa::path(get, path = "/api/health", responses((status = 200, body = HealthInfo)))]
pub async fn health(State(app): State<Arc<App>>) -> Json<HealthInfo> {
    let engine = app.engine.ping().await.is_ok();
    let streams = app.streams.read().unwrap();
    let clients: usize = streams
        .values()
        .map(|s| s.clients.lock().unwrap().len())
        .sum();
    Json(HealthInfo {
        status: "ok".into(),
        engine,
        version: env!("CARGO_PKG_VERSION").into(),
        active_streams: streams.len(),
        clients,
        seeding: app.is_seeding(),
    })
}

#[utoipa::path(
    post, path = "/api/streams",
    request_body = StartStreamReq,
    responses(
        (status = 200, body = StreamInfo, description = "Stream session ready"),
        (status = 400, description = "unknown content or engine error")
    )
)]
pub async fn start_stream(
    State(app): State<Arc<App>>,
    Json(req): Json<StartStreamReq>,
) -> Result<Json<StreamInfo>, ApiError> {
    let s = app
        .get_or_start(&req.content)
        .await
        .map_err(|m| e(StatusCode::BAD_REQUEST, m))?;
    if req.name.is_some() {
        app.playlist.track(&s.content, req.name.as_deref());
    }
    Ok(Json(stream_info(&s)))
}

#[utoipa::path(get, path = "/api/streams", responses((status = 200, body = [StreamInfo])))]
pub async fn list_streams(State(app): State<Arc<App>>) -> Json<Vec<StreamInfo>> {
    let streams = app.streams.read().unwrap();
    Json(streams.values().map(|s| stream_info(s)).collect())
}

#[utoipa::path(
    get, path = "/api/streams/{id}",
    params(("id" = String, Path, description = "Stream id")),
    responses((status = 200, body = StreamInfo), (status = 404))
)]
pub async fn get_stream(
    State(app): State<Arc<App>>,
    Path(id): Path<String>,
) -> Result<Json<StreamInfo>, ApiError> {
    match app.find(&id) {
        Some(s) => Ok(Json(stream_info(&s))),
        None => Err(e(StatusCode::NOT_FOUND, "stream not found")),
    }
}

#[utoipa::path(
    delete, path = "/api/streams/{id}",
    params(("id" = String, Path)),
    responses((status = 204, description = "Stream killed"), (status = 404))
)]
pub async fn delete_stream(
    State(app): State<Arc<App>>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    if app.kill_stream(&id).await {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(e(StatusCode::NOT_FOUND, "stream not found"))
    }
}

#[utoipa::path(
    get, path = "/api/streams/{id}/audio",
    params(("id" = String, Path, description = "Stream id or channel id (starts the stream)")),
    responses((status = 200, body = AudioInfo), (status = 503, description = "probe failed"))
)]
pub async fn stream_audio(
    State(app): State<Arc<App>>,
    Path(id): Path<String>,
) -> Result<Json<AudioInfo>, ApiError> {
    let s = app
        .get_or_start(&id)
        .await
        .map_err(|m| e(StatusCode::NOT_FOUND, m))?;
    let url = app.self_stream_url(&s.id); // broadcast, не движок: 2-й коннект к сессии движок рвёт с 500
    // get_or_try_init: неудача не кэшируется навсегда и НЕ сносит стрим (раньше снос
    // гарантировал вечный цикл «96с probe → 503 → пересоздать → снова 96с»)
    let probe = s.probe.get_or_try_init(|| transcode::probe(url)).await;
    match probe {
        Ok(info) => Ok(Json(AudioInfo {
            passthrough: transcode::browser_ok(info),
            tracks: info.audio.clone(),
        })),
        Err(err) => Err(e(
            StatusCode::SERVICE_UNAVAILABLE,
            format!("probe failed: {err}"),
        )),
    }
}

#[utoipa::path(get, path = "/api/clients", responses((status = 200, body = [ClientRow])))]
pub async fn list_clients(State(app): State<Arc<App>>) -> Json<Vec<ClientRow>> {
    let now = Instant::now();
    let streams = app.streams.read().unwrap();
    let mut rows = Vec::new();
    for s in streams.values() {
        for c in s.clients.lock().unwrap().values() {
            rows.push(ClientRow {
                id: format!("{:016x}", fnv64(&format!("{}|{}", c.ip, c.ua))),
                ip: c.ip.clone(),
                user_agent: truncate(&c.ua, 120),
                kind: c.kind.as_str().to_string(),
                watching_secs: now.duration_since(c.since).as_secs(),
                stream_id: s.id.clone(),
                content: truncate(&s.content, 120),
            });
        }
    }
    Json(rows)
}

#[utoipa::path(
    get, path = "/api/channels",
    params(ChannelsQuery),
    responses((status = 200, body = [crate::playlist::Channel]))
)]
pub async fn list_channels(
    State(app): State<Arc<App>>,
    Query(q): Query<ChannelsQuery>,
) -> Json<Vec<crate::playlist::Channel>> {
    app.kick_seed(false);
    app.playlist.ensure_fresh(app.cfg.playlist_ttl_secs);
    let chans = app.playlist.snapshot();
    Json(
        chans
            .into_iter()
            .filter(|c| {
                let cat = q
                    .category
                    .as_deref()
                    .map(|cat| {
                        c.categories
                            .iter()
                            .any(|x| x.eq_ignore_ascii_case(cat.trim()))
                    })
                    .unwrap_or(true);
                let cc = q
                    .country
                    .as_deref()
                    .map(|cc| {
                        c.country
                            .as_deref()
                            .map(|x| x.to_lowercase().contains(&cc.to_lowercase()))
                            .unwrap_or(false)
                    })
                    .unwrap_or(true);
                cat && cc
            })
            .collect(),
    )
}

#[utoipa::path(get, path = "/api/channels/categories", responses((status = 200, body = [CategoryInfo], description = "Категории движка (docs.acestream.net: list-of-categories) с количеством каналов")))]
pub async fn list_categories(State(app): State<Arc<App>>) -> Json<Vec<CategoryInfo>> {
    app.kick_seed(false);
    app.playlist.ensure_fresh(app.cfg.playlist_ttl_secs);
    let chans = app.playlist.snapshot();
    let mut m: BTreeMap<String, u32> = BTreeMap::new();
    for c in chans.iter() {
        let cats: Vec<&str> = if c.categories.is_empty() {
            vec![c.group.as_str()]
        } else {
            c.categories.iter().map(String::as_str).collect()
        };
        for cat in cats {
            *m.entry(cat.to_string()).or_insert(0) += 1;
        }
    }
    Json(
        m.into_iter()
            .map(|(name, count)| CategoryInfo { name, count })
            .collect(),
    )
}

#[utoipa::path(
    get, path = "/api/search",
    params(SearchQuery),
    responses(
        (status = 200, body = SearchResult, description = "Публичные трансляции сети Ace Stream (поиск движка)"),
        (status = 502, description = "движок не отдал ответ")
    )
)]
pub async fn search(
    State(app): State<Arc<App>>,
    Query(q): Query<SearchQuery>,
) -> Result<Json<SearchResult>, ApiError> {
    let raw = app
        .engine
        .search(q.q.as_deref())
        .await
        .map_err(|m| e(StatusCode::BAD_GATEWAY, m))?;
    let items = extract_search_items(&raw);
    Ok(Json(SearchResult { items, raw }))
}

#[utoipa::path(
    post, path = "/api/channels/rebuild",
    responses((status = 202, description = "Каталог очищен, сбор каналов запущен в фоне"))
)]
pub async fn rebuild_channels(State(app): State<Arc<App>>) -> StatusCode {
    app.playlist.reset();
    app.kick_seed(true);
    StatusCode::ACCEPTED
}

pub fn router() -> axum::Router<Arc<App>> {
    use axum::routing::{get, post};
    axum::Router::new()
        .route("/health", get(health))
        .route("/streams", get(list_streams).post(start_stream))
        .route("/streams/{id}", get(get_stream).delete(delete_stream))
        .route("/streams/{id}/audio", get(stream_audio))
        .route("/clients", get(list_clients))
        .route("/channels", get(list_channels))
        .route("/channels/categories", get(list_categories))
        .route("/channels/rebuild", post(rebuild_channels))
        .route("/search", get(search))
}
