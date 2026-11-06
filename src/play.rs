use crate::state::{App, ClientKind, TsGuard};
use crate::transcode;
use crate::util::content_id;
use axum::body::Body;
use axum::extract::{ConnectInfo, Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Redirect, Response};
use bytes::Bytes;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

fn client_ip(headers: &HeaderMap, addr: &SocketAddr) -> String {
    headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.split(',').next())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| addr.ip().to_string())
}

fn ua(headers: &HeaderMap) -> String {
    headers
        .get(header::USER_AGENT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string()
}

/// Настоящий Safari (не Chrome/Edge/Opera, которые врут в UA ради совместимости):
/// только он играет нативный HLS в <video>.
fn is_safari(ua: &str) -> bool {
    ua.contains("Safari")
        && !ua.contains("Chrome")
        && !ua.contains("Chromium")
        && !ua.contains("Edg")
        && !ua.contains("OPR")
}

fn err(status: StatusCode, msg: impl Into<String>) -> Response {
    (status, msg.into()).into_response()
}

/// GET /stream/{id} — direct MPEG-TS pass-through (VLC, Smart TV, players).
pub async fn ts(
    State(app): State<Arc<App>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> Response {
    let s = match app.get_or_start(&id).await {
        Ok(s) => s,
        Err(e) => return err(StatusCode::NOT_FOUND, e),
    };
    let ip = client_ip(&headers, &addr);
    let ua = ua(&headers);
    s.touch(&ip, &ua, ClientKind::Ts);
    let rx = s.ts_tx.subscribe(); // subscribe BEFORE the pump: else send() fails with 0 receivers
    app.ensure_pump(&s).await;
    let guard = TsGuard {
        stream: s.clone(),
        ip,
        ua,
    };
    let (tx, body_rx) = mpsc::channel::<Result<Bytes, std::io::Error>>(64);
    tokio::spawn(async move {
        let _g = guard;
        let mut rx = rx;
        loop {
            match rx.recv().await {
                Ok(b) => {
                    if tx.send(Ok(b)).await.is_err() {
                        break; // client gone
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue, // too slow: skip to live edge
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "video/mp2t"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        Body::from_stream(ReceiverStream::new(body_rx)),
    )
        .into_response()
}

async fn proxy_url(app: &App, url: &str, force_m3u8: bool) -> Response {
    // движок под нагрузкой рвёт свежесозданные соединения — один повтор на месте
    let resp = match app.http.get(url).send().await {
        Err(_) => app.http.get(url).send().await,
        ok => ok,
    };
    match resp {
        Ok(r) if r.status().is_success() => {
            let ct = if force_m3u8 || url.contains(".m3u8") {
                "application/vnd.apple.mpegurl".to_string()
            } else {
                r.headers()
                    .get(header::CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("video/mp2t")
                    .to_string()
            };
            (
                StatusCode::OK,
                [
                    (header::CONTENT_TYPE, ct.as_str()),
                    (header::CACHE_CONTROL, "no-store"),
                ],
                Body::from_stream(r.bytes_stream()),
            )
                .into_response()
        }
        Ok(r) => err(StatusCode::BAD_GATEWAY, format!("engine: {}", r.status())),
        Err(er) => err(StatusCode::BAD_GATEWAY, er.to_string()),
    }
}

/// GET /stream/{id}/index.m3u8 — engine HLS playlist, pass-through.
pub async fn hls_index(
    State(app): State<Arc<App>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> Response {
    let s = match app.get_or_start(&id).await {
        Ok(s) => s,
        Err(e) => return err(StatusCode::NOT_FOUND, e),
    };
    s.touch(&client_ip(&headers, &addr), &ua(&headers), ClientKind::Hls);
    if s.hls_index_url().is_none() {
        return err(
            StatusCode::NOT_FOUND,
            "engine provides no HLS for this stream",
        );
    }
    let url = s.hls_index_url().unwrap().to_string();
    proxy_url(&app, &url, true).await
}

/// GET /stream/{id}/{seg} — engine HLS segments/variant playlists, pass-through.
pub async fn hls_seg(
    State(app): State<Arc<App>>,
    Path((id, seg)): Path<(String, String)>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> Response {
    if seg.contains("..") {
        return err(StatusCode::BAD_REQUEST, "bad path");
    }
    let s = match app.get_or_start(&id).await {
        Ok(s) => s,
        Err(e) => return err(StatusCode::NOT_FOUND, e),
    };
    s.touch(&client_ip(&headers, &addr), &ua(&headers), ClientKind::Hls);
    let url = format!("{}{seg}", s.hls_dir());
    proxy_url(&app, &url, false).await
}

fn disk_mime(p: &PathBuf) -> &'static str {
    match p.extension().and_then(|e| e.to_str()).unwrap_or("") {
        "m3u8" => "application/vnd.apple.mpegurl",
        "m4s" => "video/iso.segment",
        _ => "video/mp4",
    }
}

async fn serve_disk(p: PathBuf) -> Response {
    match tokio::fs::read(&p).await {
        Ok(b) => (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, disk_mime(&p)),
                (header::CACHE_CONTROL, "no-store"),
            ],
            b,
        )
            .into_response(),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

/// GET /play/{id}/master.m3u8 — browser-friendly HLS:
/// redirects to engine HLS when codecs are browser-safe,
/// otherwise starts an ffmpeg transcode (h264 + aac, all audio tracks as
/// alternate AUDIO renditions, switchable in the player).
pub async fn browser_master(
    State(app): State<Arc<App>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Query(q): Query<std::collections::HashMap<String, String>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> Response {
    let s = match app.get_or_start(&id).await {
        Ok(s) => s,
        Err(e) => return err(StatusCode::NOT_FOUND, e),
    };
    s.touch(
        &client_ip(&headers, &addr),
        &ua(&headers),
        ClientKind::Browser,
    );
    // Safari: нативный HLS дешевле транскода, но нужен probe. Probe здесь НЕ блокирует
    // остальных — Chrome/Firefox сразу уходят в транскод ниже, не дожидаясь ffprobe.
    if is_safari(&ua(&headers)) {
        let url = app.self_stream_url(&s.id);
        // get_or_try_init: неудача probe не кэшируется навсегда, стрим НЕ сносим —
        // сорванный probe не повод убивать сессию у всех зрителей. Safari остаётся
        // только в транскоде.
        if let Ok(info) = s.probe.get_or_try_init(|| transcode::probe(url)).await {
            if transcode::browser_ok(info) && s.hls_index_url().is_some() {
                return Redirect::temporary(&format!("/stream/{}/index.m3u8", s.id)).into_response();
            }
        }
    }
    // Chrome/Firefox (и Safari без passthrough): быстрый probe (8с, одна попытка) —
    // выбрать режим транскода: h264/aac источник перематывается без перекодирования
    // (CPU ≈ 0 — важно на слабых серверах). Не успел (холодный p2p) — кодируем
    // «вслепую», как раньше. Аудио без probe решал `-map 0:a:0?`.
    if s.probe.get().is_none() {
        match transcode::probe_fast(&app.self_stream_url(&s.id)).await {
            Ok(info) => {
                tracing::info!(stream = %s.id, video = ?info.video, "transcode mode from probe");
                let _ = s.probe.set(info);
            }
            Err(e) => tracing::info!(stream = %s.id, err = %e, "probe_fast failed, transcoding blind"),
        }
    }
    // ?enc=1: плеер сам просит полный транскод после битого remux (смена разрешения
    // / битые кадры в источнике — copy тащит их в браузер, MSE падает)
    let force_encode = q.get("enc").map(|v| v == "1").unwrap_or(false);
    // ?ad=<мс>: сдвиг звука канала (<0 — раньше, >0 — позже). Запоминаем на стриме:
    // запросы без параметра (другие зрители/плейлист) подстройку не сбросят
    if let Some(ad) = q
        .get("ad")
        .and_then(|v| v.parse::<i32>().ok())
        .filter(|v| (-10_000..=10_000).contains(v))
    {
        s.audio_delay.store(ad, std::sync::atomic::Ordering::Relaxed);
    }
    let t = match app
        .transcodes
        .ensure(&s, &app.self_stream_url(&s.id), &app.cfg.tmp_dir, force_encode)
        .await
    {
        Ok(t) => t,
        Err(e) => return err(StatusCode::SERVICE_UNAVAILABLE, e),
    };
    // ffmpeg умер (вход оборвался) — перезапускаем, иначе отдаём мёртвый плейлист с ENDLIST
    let t = if t.finished().await {
        tracing::warn!(stream = %s.id, "transcode died, restarting");
        app.transcodes.kill(&s.id).await;
        match app
            .transcodes
            .ensure(&s, &app.self_stream_url(&s.id), &app.cfg.tmp_dir, force_encode)
            .await
        {
            Ok(t) => t,
            Err(e) => return err(StatusCode::SERVICE_UNAVAILABLE, e),
        }
    } else {
        t
    };
    // ждём пока ffmpeg напишет master-плейлист; холодный старт p2p (движок буферизует
    // источник, ffmpeg ждёт SPS/PPS+IDR) занимает до ~2 минут — иначе 504 на старте
    let master = t.dir.join("master.m3u8");
    let t0 = std::time::Instant::now();
    for i in 0..300 {
        if master.exists() {
            tracing::info!(stream = %s.id, secs = t0.elapsed().as_secs(), "master.m3u8 ready");
            break;
        }
        // ждём дольше stale_secs — периодически подтверждаем жизнь, иначе janitor
        // сочтёт зрителя протухшим и убьёт транскод на прогреве (мастер никогда не появится)
        if i % 20 == 0 {
            s.touch(&client_ip(&headers, &addr), &ua(&headers), ClientKind::Browser);
        }
        // ffmpeg умер во время прогрева (вход оборвался) — отдаём ошибку сразу,
        // перезапуск сделает следующий запрос клиента (у hls.js ретрай через 10с)
        if t.finished().await {
            tracing::warn!(stream = %s.id, "transcode died during warmup");
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    if !master.exists() {
        return err(
            StatusCode::GATEWAY_TIMEOUT,
            "transcode did not start (see ffmpeg logs)",
        );
    }
    serve_disk(master).await
}

/// GET /play/{id}/{file} — segments of the transcode.
pub async fn browser_seg(
    State(app): State<Arc<App>>,
    Path((id, file)): Path<(String, String)>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> Response {
    if file.contains("..") {
        return err(StatusCode::BAD_REQUEST, "bad path");
    }
    // id в url может быть каналом/контентом/стримом — резолвим как master, иначе сегменты 404
    let key = match app.resolve_input(&id) {
        Ok(content) => content_id(&content),
        Err(_) => id.clone(),
    };
    let Some(s) = app.find(&key) else {
        return err(StatusCode::NOT_FOUND, "stream not active");
    };
    s.touch(
        &client_ip(&headers, &addr),
        &ua(&headers),
        ClientKind::Browser,
    );
    let Some(t) = app.transcodes.get(&s.id) else {
        return err(
            StatusCode::NOT_FOUND,
            "no transcode running (request /play/{id}/master.m3u8 first)",
        );
    };
    serve_disk(t.dir.join(&file)).await
}

/// GET /playlist.m3u — all channels, urls point back to this proxy.
/// ?mode=transcode — urls point to /play/... (H.264+AAC HLS via ffmpeg, for weak devices).
#[utoipa::path(
    get, path = "/api/playlist.m3u",
    params(("mode" = Option<String>, Query, description = "transcode — ссылки на HLS /play/... (для слабых плееров)")),
    responses((status = 200, description = "M3U плейлист всех каналов (ссылки ведут на этот прокси)", content_type = "audio/x-mpegurl"))
)]
pub async fn playlist_m3u(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Query(q): Query<std::collections::HashMap<String, String>>,
) -> Response {
    app.kick_seed(false);
    app.playlist.ensure_fresh(app.cfg.playlist_ttl_secs);
    let host = app.cfg.public_url.clone().unwrap_or_else(|| {
        headers
            .get(header::HOST)
            .and_then(|h| h.to_str().ok())
            .unwrap_or("localhost:8080")
            .to_string()
    });
    let base = if host.starts_with("http") {
        host
    } else {
        format!("http://{host}")
    };
    let esc = |s: &str| s.replace('"', "'");
    let transcode = q
        .get("mode")
        .map(|m| m.as_str() == "transcode")
        .unwrap_or(false);
    let mut out = String::from("#EXTM3U\n");
    for ch in app.playlist.snapshot().iter() {
        out.push_str(&format!(
            "#EXTINF:-1 tvg-id=\"{}\" tvg-logo=\"{}\" group-title=\"{}\",{}\n",
            ch.tvg_id.as_deref().unwrap_or(""),
            ch.logo.as_deref().unwrap_or(""),
            esc(&ch.group),
            esc(&ch.name)
        ));
        if transcode {
            out.push_str(&format!("{base}/play/{}/master.m3u8\n", ch.id));
        } else {
            out.push_str(&format!("{base}/stream/{}\n", ch.id));
        }
    }
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "audio/x-mpegurl"),
            (
                header::CONTENT_DISPOSITION,
                "attachment; filename=\"playlist.m3u\"",
            ),
            (header::CACHE_CONTROL, "no-store"),
        ],
        out,
    )
        .into_response()
}
