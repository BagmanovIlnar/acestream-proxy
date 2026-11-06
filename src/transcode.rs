use crate::state::Stream;
use serde::Serialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
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
            .args(["-v", "error", "-print_format", "json", "-show_streams", ts_url])
            .output()
            .await
    })
    .await
    .map_err(|_| "ffprobe timeout".to_string())?
    .map_err(|e| format!("ffprobe: {e}"))?;
    if !out.status.success() {
        let err: String = String::from_utf8_lossy(&out.stderr).chars().take(200).collect();
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

fn ffmpeg_args(ts_url: &str, dir: &Path, info: &CodecInfo) -> Vec<String> {
    let n = info.audio.len().min(4);
    let mut a: Vec<String> = vec![
        "-hide_banner".into(),
        "-loglevel".into(),
        "warning".into(),
        "-i".into(),
        ts_url.to_string(),
    ];
    a.extend(["-map".into(), "0:v:0".into()]);
    for i in 0..n {
        a.extend(["-map".into(), format!("0:a:{i}")]);
    }
    a.extend([
        "-c:v".into(),
        "libx264".into(),
        "-preset".into(),
        "veryfast".into(),
        "-g".into(),
        "48".into(),
        "-sc_threshold".into(),
        "0".into(),
    ]);
    if n > 0 {
        a.extend([
            "-c:a".into(),
            "aac".into(),
            "-ac".into(),
            "2".into(),
            "-b:a".into(),
            "128k".into(),
        ]);
    }
    a.extend([
        "-f".into(),
        "hls".into(),
        "-hls_time".into(),
        "4".into(),
        "-hls_list_size".into(),
        "6".into(),
        "-hls_flags".into(),
        "delete_segments+independent_segments".into(),
        "-hls_segment_type".into(),
        "fmp4".into(),
    ]);
    if n > 0 {
        // one encode, each audio track becomes an alternate AUDIO rendition in the master playlist
        let mut vsm = String::from("v:0,agroup:aud");
        for i in 0..n {
            let t = &info.audio[i];
            let mut e = format!("a:{i},agroup:aud");
            if i == 0 {
                e.push_str(",default:yes");
            }
            if let Some(lang) = &t.lang {
                if lang.len() <= 3 && lang.chars().all(|c| c.is_ascii_alphanumeric()) {
                    e.push_str(&format!(",language:{lang}"));
                }
            }
            e.push_str(&format!(",name:{}", t.lang.clone().unwrap_or_else(|| format!("Track{}", i + 1))));
            vsm.push(' ');
            vsm.push_str(&e);
        }
        a.extend([
            "-master_pl_name".into(),
            "master.m3u8".into(),
            "-var_stream_map".into(),
            vsm,
            dir.join("s%v.m3u8").to_string_lossy().into_owned(),
        ]);
    } else {
        // no audio: single media playlist served as master
        a.push(dir.join("master.m3u8").to_string_lossy().into_owned());
    }
    a
}

pub struct Transcode {
    pub dir: PathBuf,
    child: AsyncMutex<tokio::process::Child>,
    _permit: OwnedSemaphorePermit, // held while running: caps concurrent transcodes
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

    pub fn contains(&self, id: &str) -> bool {
        self.map.lock().unwrap().contains_key(id)
    }

    pub fn get(&self, id: &str) -> Option<Arc<Transcode>> {
        self.map.lock().unwrap().get(id).cloned()
    }

    /// Start (or return the running) transcode for the stream. Requires probe done.
    pub async fn ensure(&self, stream: &Arc<Stream>, ts_url: &str, tmp_dir: &str) -> Result<Arc<Transcode>, String> {
        if let Some(t) = self.get(&stream.id) {
            return Ok(t);
        }
        let info = stream
            .probe
            .get()
            .cloned()
            .ok_or_else(|| "codec info not probed yet".to_string())?
            .map_err(|e| format!("probe failed: {e}"))?;
        let permit = Arc::clone(&self.sem)
            .try_acquire_owned()
            .map_err(|_| "transcode limit reached, close other transcodes or raise TRANSCODE_LIMIT".to_string())?;
        let dir = Path::new(tmp_dir).join(&stream.id);
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).map_err(|e| format!("mkdir: {e}"))?;
        let mut child = tokio::process::Command::new("ffmpeg")
            .args(ffmpeg_args(ts_url, &dir, &info))
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
                        Ok(n) => tracing::warn!(target: "ffmpeg", "{}", String::from_utf8_lossy(&buf[..n]).trim_end()),
                    }
                }
            });
        }
        let t = Arc::new(Transcode {
            dir: dir.clone(),
            child: AsyncMutex::new(child),
            _permit: permit,
        });
        self.map.lock().unwrap().insert(stream.id.clone(), t.clone());
        tracing::info!(stream = %stream.id, "transcode started -> {}", dir.display());
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
