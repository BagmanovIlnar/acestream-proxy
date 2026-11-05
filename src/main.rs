mod api;
mod engine;
mod play;
mod playlist;
mod state;
mod transcode;
mod util;

use axum::routing::get;
use axum::Router;
use state::{janitor, App};
use std::net::SocketAddr;
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tracing_subscriber::EnvFilter;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

pub struct Config {
    /// Ace Stream Engine HTTP API base (e.g. http://acestream:6878)
    pub engine_url: String,
    pub port: u16,
    /// Remote m3u playlist to import (channels, groups)
    pub playlist_url: Option<String>,
    pub playlist_refresh_secs: u64,
    /// Kill a stream after this many seconds without clients
    pub idle_secs: u64,
    /// HLS/browser client considered gone after this many seconds without requests
    pub stale_secs: u64,
    /// Max concurrent ffmpeg transcodes
    pub transcode_limit: usize,
    /// Where ffmpeg writes HLS segments
    pub tmp_dir: String,
    /// Override for urls in /playlist.m3u (default: request Host header)
    pub public_url: Option<String>,
}

fn envs(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| default.to_string())
}

impl Config {
    fn from_env() -> Self {
        Config {
            engine_url: envs("ENGINE_URL", "http://127.0.0.1:6878"),
            port: envs("PORT", "8080").parse().unwrap_or(8080),
            playlist_url: std::env::var("PLAYLIST_URL").ok().filter(|v| !v.trim().is_empty()),
            playlist_refresh_secs: envs("PLAYLIST_REFRESH_SECS", "1800").parse().unwrap_or(1800),
            idle_secs: envs("STREAM_IDLE_SECS", "30").parse().unwrap_or(30),
            stale_secs: envs("CLIENT_STALE_SECS", "60").parse().unwrap_or(60),
            transcode_limit: envs("TRANSCODE_LIMIT", "")
                .parse()
                .unwrap_or_else(|_| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(2)),
            tmp_dir: envs("TMP_DIR", &std::env::temp_dir().join("acestream-proxy").to_string_lossy()),
            public_url: std::env::var("PUBLIC_URL").ok().filter(|v| !v.trim().is_empty()),
        }
    }
}

#[derive(OpenApi)]
#[openapi(
    info(title = "Ace Stream Proxy", version = "0.1.0",
        description = "Прокси для Ace Stream: шаринг трансляций между устройствами, API для веб-интерфейса, HLS для браузера с транскодингом и выбором аудиодорожек. Плейлист: /playlist.m3u, Swagger: /swagger-ui"),
    paths(
        api::health, api::start_stream, api::list_streams, api::get_stream,
        api::delete_stream, api::stream_audio, api::list_clients,
        api::list_channels, api::list_groups,
    ),
    components(schemas(
        api::HealthInfo, api::StartStreamReq, api::StreamInfo, api::ClientInfo,
        api::AudioInfo, api::ClientRow, api::GroupInfo, playlist::Channel,
        transcode::AudioTrack,
    )),
    tags(
        (name = "streams", description = "Активные трансляции"),
        (name = "clients", description = "Кто что смотрит"),
        (name = "channels", description = "Каналы из плейлиста"),
    )
)]
struct ApiDoc;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| "info,hyper=warn".into()),
        )
        .init();

    let cfg = Config::from_env();
    let port = cfg.port;
    let app = Arc::new(App::new(cfg));

    if app.cfg.playlist_url.is_some() {
        let a = Arc::clone(&app);
        tokio::spawn(playlist::refresh_loop(a));
    }
    {
        let a = Arc::clone(&app);
        tokio::spawn(janitor(a));
    }

    let router = Router::new()
        .route("/", get(|| async { axum::response::Redirect::temporary("/swagger-ui") }))
        .nest("/api", api::router())
        .route("/stream/{id}", get(play::ts))
        .route("/stream/{id}/index.m3u8", get(play::hls_index))
        .route("/stream/{id}/{*seg}", get(play::hls_seg))
        .route("/play/{id}/master.m3u8", get(play::browser_master))
        .route("/play/{id}/{file}", get(play::browser_seg))
        .route("/playlist.m3u", get(play::playlist_m3u))
        .with_state(Arc::clone(&app))
        .merge(SwaggerUi::new("/swagger-ui").url("/api/openapi.json", ApiDoc::openapi()))
        .layer(CorsLayer::permissive());

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
        .await
        .expect("bind failed");
    tracing::info!(
        "acestream-proxy {ver}: http://0.0.0.0:{port} | engine: {engine} | swagger: http://localhost:{port}/swagger-ui",
        ver = env!("CARGO_PKG_VERSION"),
        engine = app.engine.base
    );
    axum::serve(listener, router.into_make_service_with_connect_info::<SocketAddr>())
        .await
        .expect("server error");
}
