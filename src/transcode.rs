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

/// Ручная добивка поверх автокалибровки, мс (`FFMPEG_AUDIO_DELAY_MS`).
fn audio_delay_ms() -> i32 {
    envs("FFMPEG_AUDIO_DELAY_MS", "0")
        .parse()
        .unwrap_or(0)
}

/// Куда пишет ffmpeg: live HLS для плеера или короткий mpegts для калибровки A/V.
enum FfmpegOut<'a> {
    Hls(&'a Path),
    /// `-t 4` → один TS-файл; по нему меряем итоговый сдвиг после encode
    Calibrate(&'a Path),
}

// ponytail: одна варианта (video + выбранная аудиодорожка).
// delay_ms: итог автокалибровки (+ env). >0 задержать звук; <0 звук отстаёт —
// двигаем видео вперёд на |N| мс.
fn ffmpeg_args(input_url: &str, out: FfmpegOut<'_>, mode: EncodeMode, delay_ms: i32) -> Vec<String> {
    let hls_in = input_url.contains(".m3u8");
    let scale = envs("FFMPEG_SCALE", "");
    let at = mode.audio_track;
    let cal = matches!(out, FfmpegOut::Calibrate(_));

    let mut a: Vec<String> = vec![
        "-hide_banner".into(),
        "-loglevel".into(),
        "warning".into(),
    ];
    if hls_in {
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
        ]);
    } else {
        a.extend([
            "-analyzeduration".into(),
            "10000000".into(),
            "-probesize".into(),
            "10000000".into(),
            "-fflags".into(),
            "+genpts+discardcorrupt+igndts".into(),
            "-i".into(),
            input_url.to_string(),
        ]);
    }
    if cal {
        // достаточно ~4с вывода, чтобы поймать стабильный A/V offset после энкодера
        a.extend(["-t".into(), "4".into()]);
    }
    a.extend([
        "-max_muxing_queue_size".into(),
        "9999".into(),
        "-max_interleave_delta".into(),
        "0".into(),
    ]);

    if mode.video_copy && mode.audio_copy {
        a.extend([
            "-map".into(),
            "0:v:0".into(),
            "-map".into(),
            format!("0:a:{}?", at),
            "-c:v".into(),
            "copy".into(),
            "-c:a".into(),
            "copy".into(),
        ]);
    } else if mode.video_copy {
        let af = audio_filter(delay_ms);
        a.extend([
            "-map".into(),
            "0:v:0".into(),
            "-map".into(),
            format!("0:a:{}?", at),
            "-c:v".into(),
            "copy".into(),
            "-c:a".into(),
            "aac".into(),
            "-af".into(),
            af,
            "-ac".into(),
            "2".into(),
            "-b:a".into(),
            "128k".into(),
        ]);
    } else {
        let vchain = video_filter_chain(&scale, delay_ms);
        // delay<0 уже в video setpts; delay>0 — adelay в audio; не дублировать asetpts-сдвиг
        let achain = format!("asetpts=PTS-STARTPTS,{}", audio_filter(delay_ms.max(0)));
        let fc = format!("[0:v:0]{vchain}[v];[0:a:{at}]{achain}[a]");
        a.extend([
            "-filter_complex".into(),
            fc,
            "-map".into(),
            "[v]".into(),
            "-map".into(),
            "[a]".into(),
            "-c:v".into(),
            "libx264".into(),
            "-preset".into(),
            envs("FFMPEG_PRESET", "veryfast"),
            "-tune".into(),
            "zerolatency".into(),
            "-fps_mode".into(),
            "cfr".into(),
            "-g".into(),
            "48".into(),
            "-sc_threshold".into(),
            "0".into(),
            "-c:a".into(),
            "aac".into(),
            "-ac".into(),
            "2".into(),
            "-b:a".into(),
            "128k".into(),
        ]);
    }

    a.extend(["-muxdelay".into(), "0".into(), "-muxpreload".into(), "0".into()]);
    match out {
        FfmpegOut::Calibrate(path) => {
            a.extend([
                "-f".into(),
                "mpegts".into(),
                "-y".into(),
                path.to_string_lossy().into_owned(),
            ]);
        }
        FfmpegOut::Hls(dir) => {
            a.extend([
                "-f".into(),
                "hls".into(),
                "-hls_time".into(),
                "2".into(),
                "-hls_list_size".into(),
                "6".into(),
                "-hls_flags".into(),
                "delete_segments+independent_segments".into(),
                "-hls_segment_type".into(),
                "mpegts".into(),
                dir.join("master.m3u8").to_string_lossy().into_owned(),
            ]);
        }
    }
    a
}

/// Первый pts_time указанного потока (v:0 / a:0) — для замера сдвига.
async fn first_pts_time(url: &str, stream_spec: &str) -> Result<f64, String> {
    let out = tokio::time::timeout(
        Duration::from_secs(12),
        tokio::process::Command::new("ffprobe")
            .args([
                "-v",
                "error",
                "-select_streams",
                stream_spec,
                "-show_entries",
                "packet=pts_time",
                "-of",
                "csv=p=0",
                "-read_intervals",
                "%+#12",
                url,
            ])
            .output(),
    )
    .await
    .map_err(|_| "ffprobe pts timeout".to_string())?
    .map_err(|e| format!("ffprobe pts: {e}"))?;
    if !out.status.success() {
        let err: String = String::from_utf8_lossy(&out.stderr).chars().take(160).collect();
        return Err(format!("ffprobe pts: {err}"));
    }
    for line in String::from_utf8_lossy(&out.stdout).lines() {
        let t = line.trim();
        if t.is_empty() || t == "N/A" {
            continue;
        }
        if let Ok(v) = t.parse::<f64>() {
            return Ok(v);
        }
    }
    Err("ffprobe pts: no packets".into())
}

/// Компенсация по нашей шкале: −(audio_pts−video_pts) в мс.
/// audio позже video → звук отстаёт → отрицательная компенсация (двигаем видео).
fn skew_to_compensation_ms(video_pts: f64, audio_pts: f64) -> i32 {
    let skew_ms = ((audio_pts - video_pts) * 1000.0).round() as i32;
    (-skew_ms).clamp(-2000, 2000)
}

/// Короткий пробный encode → замер A/V в выходе → нужный сдвиг для этой
/// трансляции (энкодер+источник). Remux без encode — 0.
async fn calibrate_sync_ms(input_url: &str, mode: EncodeMode) -> i32 {
    if mode.video_copy && mode.audio_copy {
        return 0;
    }
    let cal_dir = std::env::temp_dir().join(format!(
        "ace-avcal-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0)
    ));
    let _ = std::fs::create_dir_all(&cal_dir);
    let cal_file = cal_dir.join("cal.ts");
    let args = ffmpeg_args(input_url, FfmpegOut::Calibrate(&cal_file), mode, 0);
    let run = async {
        let status = tokio::process::Command::new("ffmpeg")
            .args(&args)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .await
            .map_err(|e| format!("calibrate spawn: {e}"))?;
        if !status.success() {
            return Err("calibrate ffmpeg failed".into());
        }
        Ok::<(), String>(())
    };
    let ok = tokio::time::timeout(Duration::from_secs(25), run).await;
    let comp = match ok {
        Ok(Ok(())) if cal_file.exists() => {
            let path = cal_file.to_string_lossy().into_owned();
            match (
                first_pts_time(&path, "v:0").await,
                first_pts_time(&path, "a:0").await,
            ) {
                (Ok(v), Ok(a)) => {
                    let c = skew_to_compensation_ms(v, a);
                    tracing::info!(
                        video_pts = v,
                        audio_pts = a,
                        compensation_ms = c,
                        "av sync calibrated"
                    );
                    c
                }
                (v, a) => {
                    tracing::warn!(
                        video = ?v.err(),
                        audio = ?a.err(),
                        "av sync calibrate: pts read failed"
                    );
                    0
                }
            }
        }
        Ok(Err(e)) => {
            tracing::warn!(err = %e, "av sync calibrate failed");
            0
        }
        Err(_) => {
            tracing::warn!("av sync calibrate timeout");
            0
        }
        Ok(Ok(())) => {
            tracing::warn!("av sync calibrate: no output file");
            0
        }
    };
    let _ = std::fs::remove_dir_all(&cal_dir);
    comp
}

/// Цепочка видео-фильтров: scale? + выравнивание PTS; при delay<0 — задержка
/// картинки на |delay| мс (звук отставал).
fn video_filter_chain(scale: &str, delay_ms: i32) -> String {
    let mut parts: Vec<String> = Vec::new();
    if !scale.is_empty() {
        parts.push(format!("scale=-2:{scale}"));
    }
    if delay_ms < 0 {
        parts.push(format!(
            "setpts=(PTS-STARTPTS)+{}/1000/TB",
            (-delay_ms) as f64
        ));
    } else {
        parts.push("setpts=PTS-STARTPTS".into());
    }
    parts.join(",")
}

/// Аудио-фильтр: aresample + сдвиг.
/// `>0` — adelay; `<0` — сдвиг PTS вперёд (когда видео copy и картинку не двигаем).
fn audio_filter(delay_ms: i32) -> String {
    if delay_ms > 0 {
        format!("adelay={0}|{0},aresample=async=1", delay_ms)
    } else if delay_ms < 0 {
        format!(
            "asetpts=PTS-{}/1000/TB,aresample=async=1",
            (-delay_ms) as f64
        )
    } else {
        "aresample=async=1".into()
    }
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
        // автокалибровка A/V: короткий encode → замер сдвига → основной ffmpeg
        let measured = calibrate_sync_ms(ts_url, mode).await;
        let delay_ms = measured.saturating_add(audio_delay_ms());
        let dir = Path::new(tmp_dir).join(&stream.id);
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).map_err(|e| format!("mkdir: {e}"))?;
        let mut child = tokio::process::Command::new("ffmpeg")
            .args(ffmpeg_args(ts_url, FfmpegOut::Hls(&dir), mode, delay_ms))
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
            measured_ms = measured,
            delay_ms,
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
        let args = ffmpeg_args("http://x/stream/1", FfmpegOut::Hls(Path::new("/tmp/t")), EncodeMode { video_copy: false, audio_copy: false, audio_track: 0 }, 0);
        assert!(!args.iter().any(|a| a.contains("var_stream_map") || a.contains("master_pl_name")));
        assert_eq!(args.last().unwrap(), "/tmp/t/master.m3u8");
        assert!(args.windows(2).any(|w| w[0] == "-analyzeduration" && w[1] == "10000000"));
        assert!(args.windows(2).any(|w| w[0] == "-hls_segment_type" && w[1] == "mpegts"));
        assert!(args.windows(2).any(|w| w[0] == "-fps_mode" && w[1] == "cfr"));
        assert!(args.iter().any(|a| a.contains("filter_complex") || a == "-filter_complex"));
        assert!(args.iter().any(|a| a.contains("setpts=PTS-STARTPTS")));
        assert!(!args.iter().any(|a| a == "-copyts"));
    }

    #[test]
    fn hls_input_live_edge() {
        let args = ffmpeg_args(
            "http://x/stream/1/index.m3u8",
            FfmpegOut::Hls(Path::new("/tmp/t")),
            EncodeMode { video_copy: false, audio_copy: false, audio_track: 0 },
            0,
        );
        assert!(args.windows(2).any(|w| w[0] == "-live_start_index" && w[1] == "-2"));
        assert!(args.iter().any(|a| a == "+discardcorrupt"));
        assert!(!args.iter().any(|a| a.contains("genpts")));
        assert!(args.windows(2).any(|w| w[0] == "-hls_segment_type" && w[1] == "mpegts"));
    }

    #[test]
    fn skew_to_compensation_sign() {
        // audio pts на 300мс позже video → звук отстаёт → −300
        assert_eq!(skew_to_compensation_ms(10.0, 10.3), -300);
        // audio раньше → звук опережает → +200
        assert_eq!(skew_to_compensation_ms(10.0, 9.8), 200);
        assert_eq!(skew_to_compensation_ms(1.0, 1.0), 0);
    }

    #[test]
    fn calibrate_args_use_mpegts_and_duration() {
        let args = ffmpeg_args(
            "http://x/stream/1",
            FfmpegOut::Calibrate(Path::new("/tmp/cal.ts")),
            EncodeMode { video_copy: false, audio_copy: false, audio_track: 0 },
            0,
        );
        assert!(args.windows(2).any(|w| w[0] == "-t" && w[1] == "4"));
        assert!(args.windows(2).any(|w| w[0] == "-f" && w[1] == "mpegts"));
        assert_eq!(args.last().unwrap(), "/tmp/cal.ts");
        assert!(!args.iter().any(|a| a == "hls"));
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
        let args = ffmpeg_args("http://x/stream/1", FfmpegOut::Hls(Path::new("/tmp/t")), EncodeMode { video_copy: true, audio_copy: true, audio_track: 0 }, 0);
        assert!(args.windows(2).any(|w| w[0] == "-c:v" && w[1] == "copy"));
        assert!(args.windows(2).any(|w| w[0] == "-c:a" && w[1] == "copy"));
        assert!(!args.iter().any(|a| a == "aac_adtstoasc"));
        assert!(!args.iter().any(|a| a == "libx264" || a == "aac" || a == "veryfast" || a == "zerolatency"));
    }

    #[test]
    fn audio_track_is_mapped() {
        let mut m = EncodeMode { video_copy: true, audio_copy: false, audio_track: 0 };
        let args = ffmpeg_args("http://x/stream/1", FfmpegOut::Hls(Path::new("/tmp/t")), m, 0);
        assert!(args.iter().any(|a| a == "0:a:0?"));
        m.audio_track = 2;
        let args = ffmpeg_args("http://x/stream/1", FfmpegOut::Hls(Path::new("/tmp/t")), m, 0);
        assert!(args.iter().any(|a| a == "0:a:2?"));
        assert!(!args.iter().any(|a| a == "0:a:0?"));
    }

    #[test]
    fn encode_audio_uses_async_resampler() {
        let args = ffmpeg_args("http://x/stream/1", FfmpegOut::Hls(Path::new("/tmp/t")), EncodeMode { video_copy: true, audio_copy: false, audio_track: 0 }, 0);
        assert!(args.windows(2).any(|w| w[0] == "-af" && w[1] == "aresample=async=1"));
        let args = ffmpeg_args("http://x/stream/1", FfmpegOut::Hls(Path::new("/tmp/t")), EncodeMode { video_copy: true, audio_copy: true, audio_track: 0 }, 0);
        assert!(!args.iter().any(|a| a.contains("aresample")));
    }

    #[test]
    fn video_encode_uses_zerolatency() {
        let args = ffmpeg_args("http://x/stream/1", FfmpegOut::Hls(Path::new("/tmp/t")), EncodeMode { video_copy: false, audio_copy: false, audio_track: 0 }, 0);
        assert!(args.windows(2).any(|w| w[0] == "-tune" && w[1] == "zerolatency"));
    }

    #[test]
    fn delay_applied_in_filters() {
        let args = ffmpeg_args("http://x/stream/1", FfmpegOut::Hls(Path::new("/tmp/t")), EncodeMode { video_copy: false, audio_copy: false, audio_track: 0 }, -350);
        assert!(args.iter().any(|a| a.contains("setpts=(PTS-STARTPTS)+350")));
        let args = ffmpeg_args("http://x/stream/1", FfmpegOut::Hls(Path::new("/tmp/t")), EncodeMode { video_copy: true, audio_copy: false, audio_track: 0 }, 180);
        assert!(args.windows(2).any(|w| w[0] == "-af" && w[1] == "adelay=180|180,aresample=async=1"));
    }

    #[test]
    fn video_filter_delay_negative_shifts_video() {
        // звук отстаёт → картинку двигаем вперёд
        assert!(video_filter_chain("", -300).contains("setpts=(PTS-STARTPTS)+300"));
        assert_eq!(video_filter_chain("", 0), "setpts=PTS-STARTPTS");
        assert_eq!(audio_filter(250), "adelay=250|250,aresample=async=1");
        assert!(audio_filter(-300).contains("asetpts=PTS-300"));
        assert_eq!(audio_filter(0), "aresample=async=1");
    }
}
