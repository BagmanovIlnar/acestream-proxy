use crate::util::content_id;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Duration;
use utoipa::ToSchema;

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct Channel {
    /// Stable id (use in /stream/{id}, /play/{id})
    pub id: String,
    pub name: String,
    /// Source url / content id (what gets sent to the engine)
    pub url: String,
    pub group: String,
    pub logo: Option<String>,
    #[serde(rename = "tvgId", alias = "tvg-id")]
    pub tvg_id: Option<String>,
    pub country: Option<String>,
}

#[derive(Default)]
pub struct PlaylistCache {
    pub channels: RwLock<Vec<Channel>>,
}

impl PlaylistCache {
    pub fn by_id(&self, id: &str) -> Option<Channel> {
        self.channels
            .read()
            .unwrap()
            .iter()
            .find(|c| c.id == id)
            .cloned()
    }

    pub fn set(&self, m3u_text: &str) -> usize {
        let chans = parse_m3u(m3u_text);
        let n = chans.len();
        *self.channels.write().unwrap() = chans;
        n
    }
}

/// Parse attrs of the form key="value" (or key=value), space separated.
fn parse_attrs(s: &str) -> HashMap<String, String> {
    let b: Vec<char> = s.chars().collect();
    let n = b.len();
    let mut m = HashMap::new();
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

fn non_empty(m: &HashMap<String, String>, k: &str) -> Option<String> {
    m.get(k).cloned().filter(|v| !v.is_empty())
}

pub fn parse_m3u(text: &str) -> Vec<Channel> {
    let mut out = Vec::new();
    let mut pending: Option<(HashMap<String, String>, String)> = None;

    for line in text.lines() {
        let l = line.trim();
        if l.is_empty() || l.starts_with("#EXTM3U") {
            continue;
        }
        if let Some(rest) = l.strip_prefix("#EXTINF") {
            // find first comma outside quotes: attrs,Name
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
            continue; // other directives ignored
        }
        // url line
        let url = l.to_string();
        let (attrs, name) = pending.take().unwrap_or_default();
        let id = content_id(&url);
        let name = if name.is_empty() {
            format!("Channel {}", &id[..4.min(id.len())])
        } else {
            name
        };
        out.push(Channel {
            id,
            name,
            url: url.clone(),
            group: non_empty(&attrs, "group-title").unwrap_or_else(|| "Без группы".into()),
            logo: non_empty(&attrs, "tvg-logo"),
            tvg_id: non_empty(&attrs, "tvg-id"),
            country: non_empty(&attrs, "tvg-country"),
        });
    }
    out
}

/// Periodically (re)load the playlist from PLAYLIST_URL (http(s):// or file:///path).
pub async fn refresh_loop(app: std::sync::Arc<crate::state::App>) {
    let Some(src) = app.cfg.playlist_url.clone() else {
        return;
    };
    loop {
        let fetched = if let Some(path) = src.strip_prefix("file://") {
            tokio::fs::read_to_string(path).await.map_err(|e| format!("{e}"))
        } else {
            match app
                .http
                .get(&src)
                .timeout(Duration::from_secs(60))
                .send()
                .await
            {
                Ok(r) if r.status().is_success() => r.text().await.map_err(|e| format!("{e}")),
                Ok(r) => Err(format!("http {}", r.status())),
                Err(e) => Err(e.to_string()),
            }
        };
        match fetched {
            Ok(t) => {
                let n = app.playlist.set(&t);
                tracing::info!("playlist loaded: {n} channels");
            }
            Err(e) => tracing::warn!("playlist load error: {e}"),
        }
        tokio::time::sleep(Duration::from_secs(app.cfg.playlist_refresh_secs)).await;
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
    fn no_attrs_line() {
        let chans = parse_m3u("#EXTM3U\n#EXTINF:-1,Name\n0123456789abcdef0123456789abcdef\n");
        assert_eq!(chans[0].name, "Name");
        assert_eq!(chans[0].group, "Без группы");
    }
}
