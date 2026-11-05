use crate::util::{truncate, urlencode};
use serde_json::Value;
use std::time::Duration;

#[derive(Clone)]
pub struct Engine {
    pub base: String,
    pub http: reqwest::Client,
}

/// Parsed /ace/getstream?format=json response.
/// playback_url — HLS (web player), playback_url_for_external_player — MPEG-TS.
#[derive(Debug, Clone, serde::Deserialize, Default)]
#[allow(dead_code)] // stat_url/session_id kept for future engine controls
pub struct EngineStream {
    #[serde(default)]
    pub playback_url: String,
    #[serde(default)]
    pub playback_url_for_external_player: String,
    #[serde(default)]
    pub stat_url: String,
    #[serde(default)]
    pub session_id: Option<String>,
}

/// Build the getstream query part for a content string:
/// 32-hex => id=..., otherwise (http url / magnet / acestream://) => url=<encoded>.
pub fn content_query(content: &str) -> String {
    let c = content.trim();
    let c = c.strip_prefix("acestream://").unwrap_or(c);
    if c.len() == 32 && c.bytes().all(|b| b.is_ascii_hexdigit()) {
        format!("id={c}")
    } else {
        format!("url={}", urlencode(c))
    }
}

impl Engine {
    pub fn new(base: &str) -> Self {
        Engine {
            base: base.trim_end_matches('/').to_string(),
            http: reqwest::Client::new(),
        }
    }

    fn engine_host(&self) -> &str {
        self.base
            .strip_prefix("http://")
            .or_else(|| self.base.strip_prefix("https://"))
            .unwrap_or(&self.base)
    }

    /// Engine returns urls with 127.0.0.1 authority; rewrite authority to ours.
    fn rehost(&self, url: &str) -> String {
        if url.is_empty() {
            return String::new();
        }
        if let Some(scheme_end) = url.find("://") {
            let rest_start = scheme_end + 3;
            let rest = &url[rest_start..];
            let path_start = rest.find(['/', '?']).unwrap_or(rest.len());
            return format!("{}://{}{}", &url[..scheme_end], self.engine_host(), &rest[path_start..]);
        }
        url.to_string()
    }

    pub async fn start(&self, content: &str) -> Result<EngineStream, String> {
        let url = format!(
            "{}/ace/getstream?{}&format=json",
            self.base,
            content_query(content)
        );
        let resp = self
            .http
            .get(&url)
            .timeout(Duration::from_secs(60))
            .send()
            .await
            .map_err(|e| format!("engine request failed: {e}"))?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(format!("engine error {status}: {}", truncate(&text, 300)));
        }
        let v: Value =
            serde_json::from_str(&text).map_err(|e| format!("bad engine response: {e}"))?;
        if let Some(err) = v.get("error").and_then(|e| e.as_str()) {
            return Err(format!("engine: {err}"));
        }
        let mut s: EngineStream =
            serde_json::from_value(v).map_err(|e| format!("engine response: {e}"))?;
        s.playback_url = self.rehost(&s.playback_url);
        s.playback_url_for_external_player = self.rehost(&s.playback_url_for_external_player);
        if s.playback_url.is_empty() && s.playback_url_for_external_player.is_empty() {
            return Err("engine returned no playback url".into());
        }
        Ok(s)
    }

    /// Any HTTP answer means the engine is alive.
    pub async fn ping(&self) -> Result<(), String> {
        let url = format!("{}/server_status", self.base);
        match self
            .http
            .get(&url)
            .timeout(Duration::from_secs(3))
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => Err(e.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_building() {
        assert_eq!(content_query("0123456789abcdef0123456789abcdef"), "id=0123456789abcdef0123456789abcdef");
        assert_eq!(
            content_query("acestream://0123456789abcdef0123456789abcdef"),
            "id=0123456789abcdef0123456789abcdef"
        );
        assert_eq!(content_query("http://x.ru/a.torrent"), "url=http%3A%2F%2Fx.ru%2Fa.torrent");
        assert!(content_query("magnet:?xt=urn:btih:abc").starts_with("url=magnet%3A"));
    }
}
