use crate::state::Stream;
use serde::Serialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::{Mutex as AsyncMutex, OwnedSemaphorePermit, Semaphore};
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AudioTrack {
    pub index: usize,
    pub codec: String,
    pub lang: Option<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct CodecInfo {
    pub video: Option<String>,
    pub audio: Vec<AudioTrack>,
}

/// Codecs playable by browsers (hls.js / MSE): h264 + aac/mp3.
pub fn browser_ok(c: &CodecInfo) -> bool {
    let v_ok = c.video.as_deref().map(|v| v == "h264").unwrap_or(true);
    let a_ok = c.audio.iter().all(|t| t.codec == "aac" || t.codec == "mp3");
    v_ok && a_ok
}

/// Кодируем всё: надёжный дефолт (без probe и при принудительном транскоде).
pub const ENCODE_ALL: EncodeMode = EncodeMode { video_copy: false, audio_copy: false, audio_track: 0 };

/// Что ffmpeg делает с дорожками: copy — ничего не кодирует (CPU ≈ 0),
/// encode — перекодирует для браузера. audio_track — какая дорожка источника
/// идёт в транскод (?at=N, выбор в плеере).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct EncodeMode {
    pub video_copy: bool,
    pub audio_copy: bool,
    /// какая аудиодорожка источника идёт в транскод (?at=N, выбор в плеере)
    pub audio_track: usize,
}

impl EncodeMode {
    /// Без probe — кодируем всё (безопасный дефолт, играет всегда).
    ///
    /// Важно: `audio_copy` разрешён только вместе с `video_copy`. Если видео
    /// перекодируется, AAC нельзя копировать — PTS источника и timeline
    /// libx264 расходятся, в HLS звук пропадает или уезжает от картинки
    /// (при прямой отдаче TS/HLS движка этого нет: общая шкала времени).
    pub fn from_probe(info: Option<&CodecInfo>) -> Self {
        match info {
            None => ENCODE_ALL,
            Some(i) => {
                let video_copy = i.video.as_deref() == Some("h264");
                let audio_ok = i.audio.first().map(|t| t.codec == "aac").unwrap_or(false);
                EncodeMode {
                    video_copy,
                    audio_copy: video_copy && audio_ok,
                    audio_track: 0,
                }
            }
        }
    }
}

/// Быстрый probe: одна попытка, короткий таймаут — для выбора режима транскода
/// до старта ffmpeg. Не успел (холодный p2p) — кодируем «вслепую», как раньше.
pub async fn probe_fast(ts_url: &str) -> Result<CodecInfo, String> {
    let fut = probe_once(ts_url);
    tokio::time::timeout(Duration::from_secs(8), fut)
        .await
        .map_err(|_| "probe timeout".to_string())?
}

fn envs(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| default.to_string())
}

/// ffprobe the live TS url (reads a few MB, then exits). Retries: the engine
/// sporadically answers 5xx to getstream when many sessions churn.
pub async fn probe(ts_url: String) -> Result<CodecInfo, String> {
    let mut last = String::from("unknown");
    for attempt in 0..3 {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
        match probe_once(&ts_url).await {
            Ok(info) => return Ok(info),
            Err(e) => last = e,
        }
    }
    Err(last)
}

async fn probe_once(ts_url: &str) -> Result<CodecInfo, String> {
    let out = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        tokio::process::Command::new("ffprobe")
            .args([
                "-v",
                "error",
                "-print_format",
                "json",
                "-show_streams",
                ts_url,
            ])
            .output()
            .await
    })
    .await
    .map_err(|_| "ffprobe timeout".to_string())?
    .map_err(|e| format!("ffprobe: {e}"))?;
    if !out.status.success() {
        let err: String = String::from_utf8_lossy(&out.stderr)
            .chars()
            .take(200)
            .collect();
        return Err(format!("ffprobe: {err}"));
    }
    let v: serde_json::Value =
        serde_json::from_slice(&out.stdout).map_err(|e| format!("ffprobe json: {e}"))?;
    let mut video = None;
    let mut audio = Vec::new();
    if let Some(arr) = v["streams"].as_array() {
        for s in arr {
            match s["codec_type"].as_str().unwrap_or("") {
                "video" if video.is_none() => {
                    video = Some(s["codec_name"].as_str().unwrap_or("").to_string())
                }
                "audio" if audio.len() < 8 => audio.push(AudioTrack {
                    index: audio.len(),
                    codec: s["codec_name"].as_str().unwrap_or("").to_string(),
                    lang: s["tags"]["language"].as_str().map(|x| x.to_string()),
                }),
                _ => {}
            }
        }
    }
    if video.is_none() && audio.is_empty() {
        return Err("no streams found".into());
    }
    Ok(CodecInfo { video, audio })
}

// ponytail: одна варианта (video + выбранная аудиодорожка).
// ffmpeg 5/9 с var_stream_map не резолвит %v для аудио-вариантов: плейлисты
// называются буквально "s???.m3u8" — мультитрек в HLS невоспроизводим.
//
// Вход предпочтительно HLS движка (сегмент уже mux A+V): сохраняем PTS (`-copyts`),
// не пересобираем таймлайн через genpts/first_pts. TS — запасной путь.
fn ffmpeg_args(input_url: &str, dir: &Path, mode: EncodeMode) -> Vec<String> {
    let hls_in = input_url.contains(".m3u8");
    let mut a: Vec<String> = vec![
        "-hide_banner".into(),
        "-loglevel".into(),
        "warning".into(),
    ];
    if hls_in {
        // сегменты движка уже содержат A+V на общей шкале — короткий probe,
        // без genpts (он как раз разводит звук и картинку на live TS)
        a.extend([
            "-analyzeduration".into(),
            "5000000".into(),
            "-probesize".into(),
            "5000000".into(),
            "-fflags".into(),
            "+discardcorrupt".into(),
            "-live_start_index".into(),
            "-2".into(),
            "-i".into(),
            input_url.to_string(),
            "-map".into(),
            "0:v:0".into(),
            "-map".into(),
            format!("0:a:{}?", mode.audio_track),
            "-max_muxing_queue_size".into(),
            "9999".into(),
            // сохранить относительный A/V offset источника; сдвиг к нулю — общий
            "-copyts".into(),
            "-start_at_zero".into(),
        ]);
    } else {
        // Ace Stream / MPEG-TS: аудиочасто появляется позже видео в PMT.
        a.extend([
            "-analyzeduration".into(),
            "10000000".into(),
            "-probesize".into(),
            "10000000".into(),
            "-fflags".into(),
            "+genpts+discardcorrupt+igndts".into(),
            "-i".into(),
            input_url.to_string(),
            "-map".into(),
            "0:v:0".into(),
            "-map".into(),
            format!("0:a:{}?", mode.audio_track),
            "-max_muxing_queue_size".into(),
            "9999".into(),
            "-avoid_negative_ts".into(),
            "make_zero".into(),
        ]);
    }
    if mode.video_copy {
        a.extend(["-c:v".into(), "copy".into()]);
    } else {
        a.extend([
            "-c:v".into(),
            "libx264".into(),
            "-preset".into(),
            envs("FFMPEG_PRESET", "veryfast"),
            "-tune".into(),
            "zerolatency".into(),
            "-g".into(),
            "48".into(),
            "-sc_threshold".into(),
            "0".into(),
        ]);
        let scale = envs("FFMPEG_SCALE", "");
        if !scale.is_empty() {
            a.extend(["-vf".into(), format!("scale=-2:{scale}")]);
        }
    }
    if mode.audio_copy {
        a.extend([
            "-c:a".into(),
            "copy".into(),
            "-bsf:a".into(),
            "aac_adtstoasc".into(),
        ]);
    } else {
        // лёгкая подтяжка дрейфа; без first_pts=0 — он сбрасывал звук на 0
        // отдельно от видео и давал стабильное «отставание»
        a.extend([
            "-c:a".into(),
            "aac".into(),
            "-af".into(),
            "aresample=async=1".into(),
            "-ac".into(),
            "2".into(),
            "-b:a".into(),
            "128k".into(),
        ]);
    }
    a.extend([
        "-muxdelay".into(),
        "0".into(),
        "-muxpreload".into(),
        "0".into(),
        "-f".into(),
        "hls".into(),
        "-hls_time".into(),
        "2".into(),
        "-hls_list_size".into(),
        "6".into(),
        "-hls_flags".into(),
        "delete_segments+independent_segments".into(),
        "-hls_segment_type".into(),
        "fmp4".into(),
        dir.join("master.m3u8").to_string_lossy().into_owned(),
    ]);
    a
}

pub struct Transcode {
    pub dir: PathBuf,
    /// Режим, с которым запущен этот ffmpeg: смена режима = перезапуск (см. ensure)
    pub mode: EncodeMode,
    child: AsyncMutex<tokio::process::Child>,
    _permit: OwnedSemaphorePermit, // held while running: caps concurrent transcodes
}

impl Transcode {
    /// ffmpeg exited (input died / crashed): playlist is stale (ENDLIST).
    pub async fn finished(&self) -> bool {
        self.child
            .lock()
            .await
            .try_wait()
            .map(|o| o.is_some())
            .unwrap_or(true)
    }
}

#[derive(Clone)]
pub struct Transcodes {
    sem: Arc<Semaphore>,
    map: Arc<Mutex<HashMap<String, Arc<Transcode>>>>,
}

impl Transcodes {
    pub fn new(limit: usize) -> Self {
        Transcodes {
            sem: Arc::new(Semaphore::new(limit.max(1))),
            map: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn get(&self, id: &str) -> Option<Arc<Transcode>> {
        self.map.lock().unwrap().get(id).cloned()
    }

    /// Есть живой ffmpeg (мёртвый транскод не должен держать стрим и слот семафора).
    pub async fn contains_live(&self, id: &str) -> bool {
        match self.get(id) {
            Some(t) => !t.finished().await,
            None => false,
        }
    }

    /// Start (or return the running) transcode for the stream. force_encode —
    /// полный транскод даже для h264 (плеер сам просит после битого remux).
    pub async fn ensure(
        &self,
        stream: &Arc<Stream>,
        ts_url: &str,
        tmp_dir: &str,
        force_encode: bool,
    ) -> Result<Arc<Transcode>, String> {
        let mut mode = if force_encode
            || stream.force_encode.load(Ordering::Relaxed)
        {
            if force_encode {
                stream.force_encode.store(true, Ordering::Relaxed);
            }
            ENCODE_ALL
        } else {
            EncodeMode::from_probe(stream.probe.get())
        };
        // дорожка (?at=) — влияние на режим: сменилась = перезапуск ffmpeg
        mode.audio_track = stream.audio_track.load(Ordering::Relaxed).max(0) as usize;
        // страховка: нельзя copy AAC к перекодированному видео (разъезд PTS → нет звука)
        if !mode.video_copy {
            mode.audio_copy = false;
        }
        if let Some(t) = self.get(&stream.id) {
            if t.mode == mode {
                return Ok(t);
            }
            // режим сменился (remux ломается о кривой источник) — перезапуск ffmpeg
            self.kill(&stream.id).await;
        }
        let permit = Arc::clone(&self.sem).try_acquire_owned().map_err(|_| {
            "transcode limit reached, close other transcodes or raise TRANSCODE_LIMIT".to_string()
        })?;
        let dir = Path::new(tmp_dir).join(&stream.id);
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).map_err(|e| format!("mkdir: {e}"))?;
        let mut child = tokio::process::Command::new("ffmpeg")
            .args(ffmpeg_args(ts_url, &dir, mode))
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| format!("ffmpeg spawn: {e} (is ffmpeg installed?)"))?;
        if let Some(mut stderr) = child.stderr.take() {
            tokio::spawn(async move {
                use tokio::io::AsyncReadExt;
                let mut buf = vec![0u8; 4096];
                loop {
                    match stderr.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            tracing::warn!(target: "ffmpeg", "{}", String::from_utf8_lossy(&buf[..n]).trim_end())
                        }
                    }
                }
            });
        }
        let t = Arc::new(Transcode {
            dir: dir.clone(),
            mode,
            child: AsyncMutex::new(child),
            _permit: permit,
        });
        self.map
            .lock()
            .unwrap()
            .insert(stream.id.clone(), t.clone());
        tracing::info!(
            stream = %stream.id,
            input = %ts_url,
            hls_in = ts_url.contains(".m3u8"),
            "transcode started -> {}",
            dir.display()
        );
        Ok(t)
    }

    pub async fn kill(&self, id: &str) {
        let Some(t) = self.map.lock().unwrap().remove(id) else {
            return;
        };
        {
            let mut c = t.child.lock().await;
            let _ = c.start_kill();
            let _ = c.wait().await;
        }
        let _ = tokio::fs::remove_dir_all(&t.dir).await;
        tracing::info!(stream = %id, "transcode stopped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_variant_no_var_stream_map() {
        let args = ffmpeg_args("http://x/stream/1", Path::new("/tmp/t"), EncodeMode { video_copy: false, audio_copy: false, audio_track: 0 });
        // ровно одна аудио-дорожка (опциональная), без var_stream_map/master_pl_name
        assert_eq!(args.iter().filter(|a| a.as_str() == "0:a:0?").count(), 1);
        assert!(!args.iter().any(|a| a.contains("0:a:1")));
        assert!(!args.iter().any(|a| a.contains("var_stream_map") || a.contains("master_pl_name")));
        assert_eq!(args.last().unwrap(), "/tmp/t/master.m3u8");
        // probe входа достаточно длинный, чтобы поймать позднюю аудиодорожку в TS
        assert!(args.windows(2).any(|w| w[0] == "-analyzeduration" && w[1] == "10000000"));
        assert!(args.windows(2).any(|w| w[0] == "-probesize" && w[1] == "10000000"));
        assert!(args.windows(2).any(|w| w[0] == "-max_muxing_queue_size" && w[1] == "9999"));
        assert!(args.iter().any(|a| a == "+genpts+discardcorrupt+igndts"));
        assert!(!args.iter().any(|a| a == "-copyts"));
    }

    #[test]
    fn hls_input_preserves_timestamps() {
        let args = ffmpeg_args(
            "http://x/stream/1/index.m3u8",
            Path::new("/tmp/t"),
            EncodeMode { video_copy: false, audio_copy: false, audio_track: 0 },
        );
        assert!(args.iter().any(|a| a == "-copyts"));
        assert!(args.iter().any(|a| a == "-start_at_zero"));
        assert!(args.windows(2).any(|w| w[0] == "-live_start_index" && w[1] == "-2"));
        assert!(args.iter().any(|a| a == "+discardcorrupt"));
        assert!(!args.iter().any(|a| a.contains("genpts")));
        assert!(!args.iter().any(|a| a == "make_zero"));
    }

    #[test]
    fn mode_from_probe_chooses_copy() {
        // без probe — кодируем всё
        assert_eq!(EncodeMode::from_probe(None), EncodeMode { video_copy: false, audio_copy: false, audio_track: 0 });
        // hevc/ac3 — перекодируем всё
        let hevc = CodecInfo {
            video: Some("hevc".into()),
            audio: vec![AudioTrack { index: 0, codec: "ac3".into(), lang: None }],
        };
        assert_eq!(EncodeMode::from_probe(Some(&hevc)), EncodeMode { video_copy: false, audio_copy: false, audio_track: 0 });
        // h264/aac — чистый remux, CPU ≈ 0
        let h264 = CodecInfo {
            video: Some("h264".into()),
            audio: vec![AudioTrack { index: 0, codec: "aac".into(), lang: None }],
        };
        assert_eq!(EncodeMode::from_probe(Some(&h264)), EncodeMode { video_copy: true, audio_copy: true, audio_track: 0 });
        // h264 + ac3: видео копируем, аудио кодируем
        let mixed = CodecInfo {
            video: Some("h264".into()),
            audio: vec![AudioTrack { index: 0, codec: "ac3".into(), lang: None }],
        };
        assert_eq!(EncodeMode::from_probe(Some(&mixed)), EncodeMode { video_copy: true, audio_copy: false, audio_track: 0 });
        // hevc + aac: видео кодируем → аудио тоже (нельзя copy AAC к новому timeline x264)
        let hevc_aac = CodecInfo {
            video: Some("hevc".into()),
            audio: vec![AudioTrack { index: 0, codec: "aac".into(), lang: None }],
        };
        assert_eq!(EncodeMode::from_probe(Some(&hevc_aac)), EncodeMode { video_copy: false, audio_copy: false, audio_track: 0 });
    }

    #[test]
    fn copy_mode_has_no_encoder_flags() {
        let args = ffmpeg_args("http://x/stream/1", Path::new("/tmp/t"), EncodeMode { video_copy: true, audio_copy: true, audio_track: 0 });
        assert!(args.windows(2).any(|w| w[0] == "-c:v" && w[1] == "copy"));
        assert!(args.windows(2).any(|w| w[0] == "-c:a" && w[1] == "copy"));
        assert!(args.windows(2).any(|w| w[0] == "-bsf:a" && w[1] == "aac_adtstoasc"));
        // кодировщикам нечего там делать
        assert!(!args.iter().any(|a| a == "libx264" || a == "aac" || a == "scale=-2:720" || a == "veryfast" || a == "zerolatency"));
    }

    #[test]
    fn audio_track_is_mapped() {
        let mut m = EncodeMode { video_copy: true, audio_copy: false, audio_track: 0 };
        let args = ffmpeg_args("http://x/stream/1", Path::new("/tmp/t"), m);
        assert!(args.iter().any(|a| a == "0:a:0?"));
        m.audio_track = 2;
        let args = ffmpeg_args("http://x/stream/1", Path::new("/tmp/t"), m);
        assert!(args.iter().any(|a| a == "0:a:2?"));
        assert!(!args.iter().any(|a| a == "0:a:0?"));
    }

    #[test]
    fn encode_audio_uses_async_resampler() {
        // лёгкая подтяжка дрейфа; first_pts=0 убран — ломал A/V
        let args = ffmpeg_args("http://x/stream/1", Path::new("/tmp/t"), EncodeMode { video_copy: true, audio_copy: false, audio_track: 0 });
        assert!(args.windows(2).any(|w| w[0] == "-af" && w[1] == "aresample=async=1"));
        let args = ffmpeg_args("http://x/stream/1", Path::new("/tmp/t"), EncodeMode { video_copy: true, audio_copy: true, audio_track: 0 });
        assert!(!args.iter().any(|a| a.contains("aresample")));
    }

    #[test]
    fn video_encode_uses_zerolatency() {
        let args = ffmpeg_args("http://x/stream/1", Path::new("/tmp/t"), EncodeMode { video_copy: false, audio_copy: false, audio_track: 0 });
        assert!(args.windows(2).any(|w| w[0] == "-tune" && w[1] == "zerolatency"));
    }
}
