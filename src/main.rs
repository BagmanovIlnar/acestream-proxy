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
    /// Ace Stream Engine host (ip or dns, scheme/port optional)
    pub engine_url: String,
    pub port: u16,
    /// Channels not started within this TTL are pruned from the playlist
    pub playlist_ttl_secs: u64,
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

/// ACESTREAM_URI: ip or dns, scheme/port optional (http:// + :6878 defaults).
fn normalize_engine_uri(raw: &str) -> String {
    let mut s = raw.trim().trim_end_matches('/').to_string();
    if !s.starts_with("http://") && !s.starts_with("https://") {
        s = format!("http://{s}");
    }
    let auth = s
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(&s);
    let has_port = auth
        .split(['/', '?'])
        .next()
        .map(|a| a.contains(':'))
        .unwrap_or(false);
    if !has_port {
        s.push_str(":6878");
    }
    s
}

impl Config {
    fn from_env() -> Self {
        Config {
            engine_url: normalize_engine_uri(&envs(
                "ACESTREAM_URI",
                &envs("ENGINE_URL", "http://127.0.0.1:6878"),
            )),
            port: envs("PORT", "8080").parse().unwrap_or(8080),
            playlist_ttl_secs: envs("ACESTREAM_PLAYLIST_TTL_SECS", "21600")
                .parse()
                .unwrap_or(21600),
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
