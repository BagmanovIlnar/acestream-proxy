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
pub struct GroupInfo {
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
}

#[derive(Deserialize, IntoParams)]
pub struct ChannelsQuery {
    /// Case-insensitive substring filter by group-title
    pub group: Option<String>,
    /// Case-insensitive substring filter by tvg-country
    pub country: Option<String>,
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
        Some(Ok(ci)) => (ci.video.clone(), ci.audio.clone()),
        _ => (None, Vec::new()),
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
    let clients: usize = streams.values().map(|s| s.clients.lock().unwrap().len()).sum();
    Json(HealthInfo {
        status: "ok".into(),
        engine,
        version: env!("CARGO_PKG_VERSION").into(),
        active_streams: streams.len(),
        clients,
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
    let url = s.ts_url().to_string();
    let probe = s.probe.get_or_init(|| transcode::probe(url)).await;
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
    app.playlist.ensure_fresh(app.cfg.playlist_ttl_secs);
    let chans = app.playlist.snapshot();
    Json(
        chans
            .into_iter()
            .filter(|c| {
                let g = q
                    .group
                    .as_deref()
                    .map(|g| c.group.to_lowercase().contains(&g.to_lowercase()))
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
                g && cc
            })
            .collect(),
    )
}

#[utoipa::path(get, path = "/api/channels/groups", responses((status = 200, body = [GroupInfo])))]
pub async fn list_groups(State(app): State<Arc<App>>) -> Json<Vec<GroupInfo>> {
    app.playlist.ensure_fresh(app.cfg.playlist_ttl_secs);
    let chans = app.playlist.snapshot();
    let mut m: BTreeMap<String, u32> = BTreeMap::new();
    for c in chans.iter() {
        *m.entry(c.group.clone()).or_insert(0) += 1;
    }
    Json(m.into_iter().map(|(name, count)| GroupInfo { name, count }).collect())
}

pub fn router() -> axum::Router<Arc<App>> {
    use axum::routing::get;
    axum::Router::new()
        .route("/health", get(health))
        .route("/streams", get(list_streams).post(start_stream))
        .route("/streams/{id}", get(get_stream).delete(delete_stream))
        .route("/streams/{id}/audio", get(stream_audio))
        .route("/clients", get(list_clients))
        .route("/channels", get(list_channels))
        .route("/channels/groups", get(list_groups))
}
