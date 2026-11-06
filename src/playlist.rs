use crate::util::content_id;
use serde::Serialize;
use std::path::PathBuf;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};
use utoipa::ToSchema;

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct Channel {
    /// Stable id (use in /stream/{id}, /play/{id})
    pub id: String,
    pub name: String,
    /// Content id / url / magnet (what gets sent to the engine)
    pub url: String,
    /// Display group for m3u group-title (first engine category or "Мои"), hidden in JSON API
    #[serde(skip_serializing)]
    pub group: String,
    /// All engine categories (filter via /api/channels?category=)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub categories: Vec<String>,
    pub logo: Option<String>,
    #[serde(rename = "tvgId", alias = "tvg-id")]
    pub tvg_id: Option<String>,
    pub country: Option<String>,
    #[serde(skip)]
    /// unix secs of last start; persisted via x-last attr, used for TTL prune
    pub last_seen: u64,
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Pretty default name for a content string.
fn default_name(content: &str) -> String {
    let bare = content.trim().trim_start_matches("acestream://");
    if (bare.len() == 32 || bare.len() == 40) && bare.bytes().all(|b| b.is_ascii_hexdigit()) {
        return format!("Канал {}", &bare[..8]);
    }
    let last = bare.rsplit(['/']).next().unwrap_or(bare);
    let last = last.split('?').next().unwrap_or(last);
    if last.is_empty() {
        crate::util::truncate(bare, 40)
    } else {
        crate::util::truncate(last, 40)
    }
}

fn non_empty(m: &std::collections::HashMap<String, String>, k: &str) -> Option<String> {
    m.get(k).cloned().filter(|v| !v.is_empty())
}

/// Parse attrs of the form key="value" (or key=value), space separated.
fn parse_attrs(s: &str) -> std::collections::HashMap<String, String> {
    let b: Vec<char> = s.chars().collect();
    let n = b.len();
    let mut m = std::collections::HashMap::new();
    let mut i = 0;
    while i < n {
        while i < n && b[i] == ' ' {
            i += 1;
        }
        let start = i;
        while i < n && b[i] != '=' && b[i] != ' ' {
            i += 1;
        }
        let key: String = b[start..i].iter().collect();
        if key.is_empty() {
            i += 1;
            continue;
        }
        if i < n && b[i] == '=' {
            i += 1;
            if i < n && b[i] == '"' {
                i += 1;
                let vs = i;
                while i < n && b[i] != '"' {
                    i += 1;
                }
                m.insert(key, b[vs..i].iter().collect());
                i += 1;
            } else {
                let vs = i;
                while i < n && b[i] != ' ' {
                    i += 1;
                }
                m.insert(key, b[vs..i].iter().collect());
            }
        }
    }
    m
}

/// Parse m3u text (also used to load our own persisted playlist).
pub fn parse_m3u(text: &str) -> Vec<Channel> {
    let mut out = Vec::new();
    let mut pending: Option<(std::collections::HashMap<String, String>, String)> = None;
    for line in text.lines() {
        let l = line.trim();
        if l.is_empty() || l.starts_with("#EXTM3U") {
            continue;
        }
        if let Some(rest) = l.strip_prefix("#EXTINF") {
            let rest = rest.strip_prefix(':').unwrap_or(rest);
            let mut in_q = false;
            let mut comma = None;
            for (i, ch) in rest.char_indices() {
                match ch {
                    '"' => in_q = !in_q,
                    ',' if !in_q => {
                        comma = Some(i);
                        break;
                    }
                    _ => {}
                }
            }
            let (attrs_s, name) = match comma {
                Some(i) => (&rest[..i], rest[i + 1..].trim().to_string()),
                None => (rest, String::new()),
            };
            pending = Some((parse_attrs(attrs_s), name));
            continue;
        }
        if l.starts_with('#') {
            continue;
        }
        let url = l.to_string();
        let (attrs, name) = pending.take().unwrap_or_default();
        let id = content_id(&url);
        let name = if name.is_empty() {
            default_name(&url)
        } else {
            name
        };
        out.push(Channel {
            id,
            name,
            url,
            group: non_empty(&attrs, "group-title").unwrap_or_else(|| "Мои".into()),
            categories: non_empty(&attrs, "x-cats")
                .map(|s| s.split(',').map(str::trim).filter(|x| !x.is_empty()).map(String::from).collect())
                .unwrap_or_default(),
            logo: non_empty(&attrs, "tvg-logo"),
            tvg_id: non_empty(&attrs, "tvg-id"),
            country: non_empty(&attrs, "tvg-country"),
            last_seen: non_empty(&attrs, "x-last")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0),
        });
    }
    out
}

fn to_m3u(chs: &[Channel]) -> String {
    let esc = |s: &str| s.replace('"', "'");
    let mut out = String::from("#EXTM3U\n");
    for c in chs {
        out.push_str(&format!(
            "#EXTINF:-1 x-last=\"{}\"{} group-title=\"{}\",{}\n{}\n",
            c.last_seen,
            if c.categories.is_empty() {
                String::new()
            } else {
                format!(" x-cats=\"{}\"", esc(&c.categories.join(",")))
            },
            esc(&c.group),
            esc(&c.name),
            c.url
        ));
    }
    out
}

/// Self-generated playlist: every started stream is remembered (in memory while
/// running) and served via /api/channels, /playlist.m3u. TMP_DIR/playlist.m3u is
/// a cache: valid for the TTL, then dropped and regenerated on the next request
/// (only channels started/watched within the TTL survive).
pub struct PlaylistHistory {
    pub channels: RwLock<Vec<Channel>>,
    path: PathBuf,
}

/// Engine /server/api search response -> playable channels (url = 40-hex infohash).
/// Category entries in a /search response (for pagination: page is done when < page_size).
pub fn search_results_len(v: &serde_json::Value) -> usize {
    v.pointer("/result/results")
        .or_else(|| v.get("results"))
        .and_then(|x| x.as_array())
        .map(|a| a.len())
        .unwrap_or(0)
}

pub fn channels_from_search(v: &serde_json::Value) -> Vec<Channel> {
    let mut out = Vec::new();
    if let Some(cats) = v
        .pointer("/result/results")
        .or_else(|| v.get("results"))
        .and_then(|x| x.as_array())
    {
        for cat in cats.iter() {
            let logo = cat
                .get("icons")
                .and_then(|x| x.as_array())
                .and_then(|a| a.first())
                .and_then(|i| i.get("url"))
                .and_then(|u| u.as_str())
                .map(String::from);
            for it in cat.get("items").and_then(|x| x.as_array()).into_iter().flatten() {
                let ih = match it.get("infohash").and_then(|x| x.as_str()) {
                    Some(s) => s,
                    None => continue,
                };
                if it.get("disabled").and_then(|x| x.as_bool()).unwrap_or(false) {
                    continue;
                }
                out.push(Channel {
                    id: content_id(ih),
                    name: it
                        .get("name")
                        .and_then(|x| x.as_str())
                        .unwrap_or(ih)
                        .to_string(),
                    url: ih.to_string(),
                    group: it
                        .get("categories")
                        .and_then(|x| x.as_array())
                        .and_then(|a| a.first())
                        .and_then(|c| c.as_str())
                        .filter(|s| !s.is_empty())
                        .unwrap_or("Трансляции")
                        .to_string(),
                    categories: it
                        .get("categories")
                        .and_then(|x| x.as_array())
                        .map(|a| {
                            a.iter()
                                .filter_map(|c| c.as_str())
                                .filter(|s| !s.is_empty())
                                .map(String::from)
                                .collect()
                        })
                        .unwrap_or_default(),
                    logo: logo.clone(),
                    tvg_id: None,
                    country: it
                        .get("countries")
                        .and_then(|x| x.as_array())
                        .and_then(|a| a.first())
                        .and_then(|c| c.as_str())
                        .map(|s| s.to_uppercase()),
                    last_seen: 0,
                });
            }
        }
    }
    out
}

fn file_age_secs(p: &std::path::Path) -> Option<u64> {
    let m = std::fs::metadata(p).ok()?;
    let modified = m.modified().ok()?;
    modified.elapsed().ok().map(|d| d.as_secs())
}

impl PlaylistHistory {
    pub fn new(tmp_dir: &str, ttl_secs: u64) -> Self {
        let path = std::path::Path::new(tmp_dir).join("playlist.m3u");
        let channels = match file_age_secs(&path) {
            // restart: the cached playlist lives for the TTL too
            Some(age) if age < ttl_secs => std::fs::read_to_string(&path)
                .map(|t| parse_m3u(&t))
                .unwrap_or_default(),
            Some(_) => {
                let _ = std::fs::remove_file(&path); // expired: start clean
                Vec::new()
            }
            None => Vec::new(),
        };
        tracing::info!("playlist: {} channels", channels.len());
        PlaylistHistory {
            channels: RwLock::new(channels),
            path,
        }
    }

    pub fn snapshot(&self) -> Vec<Channel> {
        self.channels.read().unwrap().clone()
    }

    pub fn by_id(&self, id: &str) -> Option<Channel> {
        self.channels.read().unwrap().iter().find(|c| c.id == id).cloned()
    }

    /// Remember/refresh a started stream (upsert by content). Rewrites the m3u file.
    pub fn track(&self, content: &str, name: Option<&str>) {
        let id = content_id(content);
        let now = now_secs();
        let mut chs = self.channels.write().unwrap();
        if let Some(c) = chs.iter_mut().find(|c| c.id == id) {
            c.last_seen = now;
            if let Some(n) = name {
                if !n.trim().is_empty() {
                    c.name = n.trim().to_string();
                }
            }
        } else {
            chs.push(Channel {
                id,
                name: name.map(str::trim).filter(|n| !n.is_empty()).map(String::from).unwrap_or_else(|| default_name(content)),
                url: content.to_string(),
                group: "Мои".into(),
                categories: Vec::new(),
                logo: None,
                tvg_id: None,
                country: None,
                last_seen: now,
            });
            tracing::info!("playlist: added channel '{}'", chs.last().unwrap().name);
        }
        let text = to_m3u(&chs);
        drop(chs);
        let _ = std::fs::create_dir_all(self.path.parent().unwrap_or(std::path::Path::new(".")));
        let _ = std::fs::write(&self.path, text);
    }

    /// Cache lifetime: when the cached playlist is older than ttl it is dropped
    /// and rebuilt on this request, keeping only channels touched within the TTL.
    pub fn ensure_fresh(&self, ttl_secs: u64) {
        if !self.cache_stale(ttl_secs) {
            return;
        }
        let now = now_secs();
        let mut chs = self.channels.write().unwrap();
        chs.retain(|c| now.saturating_sub(c.last_seen) < ttl_secs);
        let text = to_m3u(&chs);
        drop(chs);
        let _ = std::fs::create_dir_all(self.path.parent().unwrap_or(std::path::Path::new(".")));
        let _ = std::fs::write(&self.path, text); // recreated after TTL
        tracing::info!("playlist: cache expired, regenerated");
    }

    /// True when the m3u cache file is missing or older than ttl.
    pub fn cache_stale(&self, ttl_secs: u64) -> bool {
        !matches!(file_age_secs(&self.path), Some(age) if age < ttl_secs)
    }

    /// Wipe the catalog; rebuild via App::kick_seed (engine search).
    pub fn reset(&self) {
        self.channels.write().unwrap().clear();
        let _ = std::fs::write(&self.path, to_m3u(&[]));
        tracing::info!("playlist: reset");
    }

    /// Upsert externally discovered channels (engine search). Existing entries
    /// keep their names; everything gets last_seen=now so it survives the TTL prune.
    pub fn merge(&self, incoming: Vec<Channel>) {
        if incoming.is_empty() {
            return;
        }
        let now = now_secs();
        let mut chs = self.channels.write().unwrap();
        for c in incoming {
            match chs.iter_mut().find(|e| e.id == c.id) {
                Some(e) => e.last_seen = now,
                None => chs.push(Channel { last_seen: now, ..c }),
            }
        }
        let text = to_m3u(&chs);
        drop(chs);
        let _ = std::fs::write(&self.path, text);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_attrs_and_urls() {
        let m3u = "#EXTM3U\n\
                   #EXTINF:-1 tvg-id=\"match\" tvg-logo=\"http://x/l.png\" group-title=\"Спорт, футбол\",Матч ТВ\n\
                   acestream://0123456789abcdef0123456789abcdef\n\
                   #EXTINF:-1 group-title=\"Фильмы\",Кино\n\
                   http://ex.ru/torrent.acelive\n";
        let chans = parse_m3u(m3u);
        assert_eq!(chans.len(), 2);
        assert_eq!(chans[0].name, "Матч ТВ");
        assert_eq!(chans[0].group, "Спорт, футбол"); // quoted comma preserved
        assert_eq!(chans[0].tvg_id.as_deref(), Some("match"));
        assert_eq!(chans[0].url, "acestream://0123456789abcdef0123456789abcdef");
        assert_eq!(chans[1].url, "http://ex.ru/torrent.acelive");
        assert_eq!(chans[1].group, "Фильмы");
        assert_ne!(chans[0].id, chans[1].id);
    }

    #[test]
    fn parses_engine_search_results() {
        let raw = serde_json::json!({"result": {"results": [{"name": "Discovery", "items": [
            {"name": "Discovery HD", "infohash": "6477d56108d979adf1a069b26c820e749b94b9da",
             "categories": ["documentaries"], "countries": ["ru"], "disabled": false},
            {"name": "dead", "infohash": "0000000000000000000000000000000000000000", "disabled": true}
        ]}]}});
        let chs = channels_from_search(&raw);
        assert_eq!(chs.len(), 1);
        assert_eq!(chs[0].url, "6477d56108d979adf1a069b26c820e749b94b9da");
        assert_eq!(chs[0].group, "documentaries");
        assert_eq!(chs[0].country.as_deref(), Some("RU"));
        assert_eq!(chs[0].categories, vec!["documentaries"]);
    }

    #[test]
    fn m3u_roundtrip_keeps_categories() {
        let c = channels_from_search(&serde_json::json!({"result": {"results": [
            {"name": "X", "items": [{"name": "X", "infohash": "6477d56108d979adf1a069b26c820e749b94b9da",
             "categories": ["sport", "tv"]}]}]}})).pop().unwrap();
        let text = to_m3u(&[c]);
        let back = parse_m3u(&text);
        assert_eq!(back.len(), 1);
        assert_eq!(back[0].categories, vec!["sport", "tv"]);
    }

    #[test]
    fn track_persist_roundtrip_and_ttl() {
        let dir = std::env::temp_dir().join(format!("acepl-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let dir_str = dir.to_str().unwrap().to_string();

        let h = PlaylistHistory::new(&dir_str, 3600);
        assert!(h.snapshot().is_empty());
        h.track("0123456789abcdef0123456789abcdef", Some("Матч"));
        h.track("0123456789abcdef0123456789abcdef", Some("Матч2")); // upsert rename
        h.track("http://x.ru/a.acelive", None); // default name from url
        assert_eq!(h.snapshot().len(), 2);
        assert_eq!(h.by_id(&h.snapshot()[0].id).unwrap().name, "Матч2");

        // reload from disk while cache is fresh (restart)
        let h2 = PlaylistHistory::new(&dir_str, 3600);
        assert_eq!(h2.snapshot().len(), 2);
        assert_eq!(h2.by_id(&h.snapshot()[0].id).unwrap().name, "Матч2");

        // fresh cache: ensure_fresh keeps everything
        h2.ensure_fresh(3600);
        assert_eq!(h2.snapshot().len(), 2);

        // expired cache: only channels touched within ttl survive
        std::thread::sleep(std::time::Duration::from_millis(1100));
        h2.ensure_fresh(1);
        assert!(h2.snapshot().is_empty(), "expired playlist regenerated empty");

        // and restart with expired cache starts clean
        let h3 = PlaylistHistory::new(&dir_str, 3600);
        assert!(h3.snapshot().is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
