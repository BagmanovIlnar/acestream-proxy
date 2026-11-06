use crate::util::{chain, truncate, urlencode};
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
/// 32-hex content id / 40-hex infohash => id=..., otherwise (http url / magnet / acestream://) => url=<encoded>.
pub fn content_query(content: &str) -> String {
    let c = content.trim();
    let c = c.strip_prefix("acestream://").unwrap_or(c);
    if c.len() == 40 && c.bytes().all(|b| b.is_ascii_hexdigit()) {
        return format!("infohash={c}"); // engine search results (p2p broadcasts)
    }
    if c.len() == 32 && c.bytes().all(|b| b.is_ascii_hexdigit()) {
        format!("id={c}")
    } else {
        format!("url={}", urlencode(c))
    }
}

impl Engine {
    pub fn new(base: &str) -> Self {
        // ponytail: pool disabled — the engine drops keep-alive conns and pooled
        // reuse fails with "error sending request"; retry-on-stale not worth it
        let http = reqwest::Client::builder()
            .user_agent(concat!("acestream-proxy/", env!("CARGO_PKG_VERSION"))) // the engine hangs on UA-less requests
            .pool_max_idle_per_host(0)
            .build()
            .unwrap_or_default();
        Engine {
            base: base.trim_end_matches('/').to_string(),
            http,
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
            return format!(
                "{}://{}{}",
                &url[..scheme_end],
                self.engine_host(),
                &rest[path_start..]
            );
        }
        url.to_string()
    }

    pub async fn start(&self, content: &str) -> Result<EngineStream, String> {
        // движок периодически рвёт соединения/отвечает 5xx под нагрузкой — повторяем
        let mut last = String::new();
        for attempt in 0..3 {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            match self.start_once(content).await {
                Ok(s) => return Ok(s),
                Err(e) => last = e,
            }
        }
        Err(last)
    }

    async fn start_once(&self, content: &str) -> Result<EngineStream, String> {
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
            .map_err(|e| format!("engine request failed: {}", chain(&e)))?;
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
        // infohash= responses nest fields under "response", id= responses are flat
        let body = v.get("response").filter(|r| r.is_object()).unwrap_or(&v);
        let mut s: EngineStream =
            serde_json::from_value(body.clone()).map_err(|e| format!("engine response: {e}"))?;
        s.playback_url = self.rehost(&s.playback_url);
        s.playback_url_for_external_player = self.rehost(&s.playback_url_for_external_player);
        if s.playback_url.is_empty() && s.playback_url_for_external_player.is_empty() {
            return Err(format!(
                "engine returned no playback url: {}",
                truncate(&text, 300)
            ));
        }
        Ok(s)
    }

    /// Sessionless TS url: /ace/getstream (re)starts the playback session on connect,
    /// unlike the ephemeral playback urls from start() that die with the session.
    pub fn ts_session_url(&self, content: &str) -> String {
        format!("{}/ace/getstream?{}", self.base, content_query(content))
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

    /// Search public p2p broadcasts in the Ace Stream network via the engine's
    /// /server/api (get_api_access_token + method=search).
    pub async fn search(&self, q: Option<&str>) -> Result<serde_json::Value, String> {
        let token = self.api_token().await?;
        self.search_page(&token, q, None, 0, 50).await
    }

    pub async fn api_token(&self) -> Result<String, String> {
        let tok_url = format!("{}/server/api?method=get_api_access_token", self.base);
        let tok_resp: serde_json::Value = self
            .http
            .get(&tok_url)
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| format!("token request failed: {}", chain(&e)))?
            .json()
            .await
            .map_err(|e| format!("token response: {e}"))?;
        Ok(tok_resp
            .get("token")
            .and_then(|t| t.as_str())
            .or_else(|| tok_resp.pointer("/result/token").and_then(|t| t.as_str()))
            .ok_or_else(|| "engine returned no api token".to_string())?
            .to_string())
    }

    /// One search call with an already fetched token (reuse it: the engine's
    /// /server_api wedges for ~30s under request bursts).
    pub async fn search_page(
        &self,
        token: &str,
        q: Option<&str>,
        category: Option<&str>,
        page: u32,
        page_size: u32,
    ) -> Result<serde_json::Value, String> {
        let mut search_url = format!(
            "{}/search?token={}&page={}&page_size={}",
            self.base,
            urlencode(token),
            page,
            page_size.min(200)
        );
        if let Some(c) = category.filter(|c| !c.trim().is_empty()) {
            search_url.push_str(&format!("&category={}", urlencode(c.trim())));
        }
        if let Some(q) = q.filter(|q| !q.trim().is_empty()) {
            search_url.push_str(&format!("&query={}", urlencode(q.trim())));
        }
        self.http
            .get(&search_url)
            .timeout(Duration::from_secs(30))
            .send()
            .await
            .map_err(|e| format!("search request failed: {}", chain(&e)))?
            .json()
            .await
            .map_err(|e| format!("search response: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_building() {
        assert_eq!(
            content_query("0123456789abcdef0123456789abcdef"),
            "id=0123456789abcdef0123456789abcdef"
        );
        assert_eq!(
            content_query("c88509fd554d301028625518bebf2376d01156e0"),
            "infohash=c88509fd554d301028625518bebf2376d01156e0"
        );
        assert_eq!(
            content_query("acestream://0123456789abcdef0123456789abcdef"),
            "id=0123456789abcdef0123456789abcdef"
        );
        assert_eq!(
            content_query("http://x.ru/a.torrent"),
            "url=http%3A%2F%2Fx.ru%2Fa.torrent"
        );
        assert!(content_query("magnet:?xt=urn:btih:abc").starts_with("url=magnet%3A"));
    }
}
