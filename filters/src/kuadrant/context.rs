// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Praxis Contributors

//! Praxis implementation of kuadrant-filter's AttributeResolver trait.
//!
//! This module provides the bridge between Praxis's HTTP filter pipeline
//! and kuadrant-filter's policy enforcement engine.

use super::config::UpstreamConfig;
use super::grpc::{grpc_call, GrpcChannelRegistry};
use kuadrant_filter::data::attribute::{AttributeError, Path};
use kuadrant_filter::kuadrant::resolver::AttributeResolver;
use kuadrant_filter::services::ServiceError;
use praxis_filter::HttpFilterContext;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

/// HTTP reply details from kuadrant-filter's send_http_reply.
#[derive(Debug, Clone)]
pub struct HttpReply {
    pub status_code: u32,
    pub headers: Vec<(String, String)>,
    pub body: Option<Vec<u8>>,
}

/// Storage for gRPC responses (synchronous bridge for async calls).
///
/// Since `AttributeResolver::dispatch_grpc_call()` is synchronous but we need
/// async gRPC calls, we:
/// 1. Block on the async call in `dispatch_grpc_call()`
/// 2. Store the response here
/// 3. Return a token_id
/// 4. `get_grpc_response()` retrieves the stored response
/// 5. Track tokens that need to be digested by the pipeline
#[derive(Debug, Default)]
pub struct GrpcResponseStore {
    responses: HashMap<u32, Vec<u8>>,
    next_token: u32,
    last_token: Option<u32>,
    /// Tokens that have been stored but not yet digested by the pipeline
    pending_digest: Vec<u32>,
    /// HTTP reply to send (set by send_http_reply)
    reply: Option<HttpReply>,
}

#[allow(dead_code, reason = "WIP")]
impl GrpcResponseStore {
    /// Create a new empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Store a response and return its token ID.
    pub fn store(&mut self, response: Vec<u8>) -> u32 {
        let token = self.next_token;
        self.responses.insert(token, response);
        self.last_token = Some(token);
        self.pending_digest.push(token);
        self.next_token += 1;
        token
    }

    /// Get all pending tokens that need to be digested.
    pub fn take_pending_digest(&mut self) -> Vec<u32> {
        std::mem::take(&mut self.pending_digest)
    }

    /// Get the size of a stored response.
    pub fn get_response_size(&self, token: u32) -> Option<usize> {
        self.responses.get(&token).map(|r| r.len())
    }

    /// Push a token back into pending digest queue.
    pub fn push_pending(&mut self, token: u32) {
        self.pending_digest.push(token);
    }

    /// Take the HTTP reply, consuming it.
    pub fn take_reply(&mut self) -> Option<HttpReply> {
        self.reply.take()
    }

    /// Get the last stored response.
    pub fn get_last(&mut self) -> Option<Vec<u8>> {
        self.last_token.and_then(|token| self.responses.remove(&token))
    }
}

/// Request data extracted from HttpFilterContext for 'static lifetime compatibility.
#[derive(Clone)]
struct RequestData {
    method: http::Method,
    uri: http::Uri,
    headers: http::HeaderMap,
    client_addr: Option<std::net::IpAddr>,
    upstream_addr: Option<std::net::SocketAddr>,
    // Note: client port is extracted from IpAddr.to_string() via parse_ip_and_port(),
    // which handles "IP:PORT" format or defaults to port 80
}

/// Parse IP address and port from IpAddr.
///
/// IpAddr.to_string() might return "IP:PORT" or just "IP".
/// Returns (ip_string, port), defaulting to port 80 if not present.
fn parse_ip_and_port(addr: std::net::IpAddr) -> (String, u16) {
    let addr_str = addr.to_string();

    if let Some(colon_pos) = addr_str.rfind(':') {
        // Found colon - might be "IP:PORT" format
        let ip_part = addr_str[..colon_pos].to_string();
        let port_str = &addr_str[colon_pos + 1..];
        let port = port_str.parse::<u16>().unwrap_or(80);
        (ip_part, port)
    } else {
        // No colon - just the IP, default to port 80
        (addr_str, 80)
    }
}

/// Praxis implementation of kuadrant-filter's AttributeResolver trait.
///
/// This struct bridges Praxis's `HttpFilterContext` to kuadrant-filter's
/// policy enforcement by implementing all required I/O operations.
///
/// No lifetime parameter - all data is owned through Arc for 'static compatibility.
#[allow(dead_code, reason = "WIP")]
pub struct PraxisAttributeResolver {
    /// Snapshot of request data (method, URI, headers)
    request_data: Arc<RequestData>,

    /// gRPC channel registry (shared across requests)
    channel_registry: Arc<GrpcChannelRegistry>,

    /// Upstream configurations
    upstreams: Arc<HashMap<String, UpstreamConfig>>,

    /// gRPC response storage (per-request)
    response_store: Arc<RwLock<GrpcResponseStore>>,
}

impl PraxisAttributeResolver {
    /// Create a new resolver from HttpFilterContext.
    ///
    /// Extracts and clones request data for 'static lifetime compatibility.
    #[allow(dead_code, reason = "WIP")]
    pub fn new(
        ctx: &HttpFilterContext<'_>,
        channel_registry: Arc<GrpcChannelRegistry>,
        upstreams: Arc<HashMap<String, UpstreamConfig>>,
        response_store: Arc<RwLock<GrpcResponseStore>>,
    ) -> Self {
        // Clone request data for 'static ownership
        let request_data = Arc::new(RequestData {
            method: ctx.request.method.clone(),
            uri: ctx.request.uri.clone(),
            headers: ctx.request.headers.clone(),
            client_addr: ctx.client_addr,
            upstream_addr: ctx.upstream.as_ref().and_then(|u| u.address.parse().ok()),
        });

        Self {
            request_data,
            channel_registry,
            upstreams,
            response_store,
        }
    }
}

impl AttributeResolver for PraxisAttributeResolver {
    // ========================================================================
    // Attribute access (Envoy CEL attributes)
    // ========================================================================

    fn get_attribute(&self, path: &Path) -> Result<Option<Vec<u8>>, AttributeError> {
        use tracing::debug;

        // Map Envoy-style attribute paths to Praxis request data
        let path_str = path.to_string();

        let result = match path_str.as_str() {
            "request.host" => {
                // Extract host from URI or Host header
                if let Some(host) = self.request_data.uri.host() {
                    Ok(Some(host.as_bytes().to_vec()))
                } else if let Some(host) = self.request_data.headers.get(http::header::HOST) {
                    // Validate UTF-8 for header value
                    let host_str = host
                        .to_str()
                        .map_err(|_| AttributeError::Retrieval("Host header contains invalid UTF-8".to_string()))?;
                    Ok(Some(host_str.as_bytes().to_vec()))
                } else {
                    Ok(None)
                }
            }
            "request.method" => {
                Ok(Some(self.request_data.method.as_str().as_bytes().to_vec()))
            }
            "request.url_path" | "request.path" => {
                Ok(Some(self.request_data.uri.path().as_bytes().to_vec()))
            }
            "request.scheme" => {
                // Return scheme from URI or default to "http"
                let scheme = self.request_data.uri.scheme_str().unwrap_or("http");
                Ok(Some(scheme.as_bytes().to_vec()))
            }
            "request.time" => {
                // Return timestamp as i64 nanoseconds (8 bytes little-endian)
                use std::time::SystemTime;

                let now = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map_err(|e| AttributeError::Retrieval(format!("time error: {}", e)))?;

                let nanos = now.as_secs() as i64 * 1_000_000_000 + now.subsec_nanos() as i64;
                Ok(Some(nanos.to_le_bytes().to_vec()))
            }
            "request.protocol" => {
                // HTTP version as UTF-8 string (required field)
                Ok(Some(b"HTTP/1.1".to_vec()))
            }
            "destination.address" => {
                // Extract from upstream backend address (SocketAddr from load_balancer)
                let addr = self.request_data.upstream_addr
                    .ok_or_else(|| AttributeError::NotAvailable(
                        "destination.address not available: upstream is None".to_string()
                    ))?;
                Ok(Some(addr.ip().to_string().as_bytes().to_vec()))
            }
            "destination.port" => {
                // Extract port from upstream backend address (SocketAddr from load_balancer)
                let addr = self.request_data.upstream_addr
                    .ok_or_else(|| AttributeError::NotAvailable(
                        "destination.port not available: upstream is None".to_string()
                    ))?;
                let port_i64 = addr.port() as i64;
                Ok(Some(port_i64.to_le_bytes().to_vec()))
            }
            "source.address" => {
                // Extract from client_addr - error if not available
                let addr = self.request_data.client_addr
                    .ok_or_else(|| AttributeError::NotAvailable("source.address not available: client_addr is None".to_string()))?;

                let (ip, _port) = parse_ip_and_port(addr);
                Ok(Some(ip.as_bytes().to_vec()))
            }
            "source.port" => {
                // Try to extract port from client_addr string representation
                // Format might be "IP:PORT" or just "IP" (default to 80)
                let addr = self.request_data.client_addr
                    .ok_or_else(|| AttributeError::NotAvailable("source.port not available: client_addr is None".to_string()))?;

                let (_ip, port) = parse_ip_and_port(addr);
                let port_i64 = port as i64;
                Ok(Some(port_i64.to_le_bytes().to_vec()))
            }
            _ => {
                // Unsupported attribute
                Err(AttributeError::NotAvailable(format!(
                    "Attribute `{}` not supported in Praxis AttributeResolver",
                    path_str
                )))
            }
        };

        // Log the result for debugging
        match &result {
            Ok(Some(bytes)) => {
                match std::str::from_utf8(bytes) {
                    Ok(s) => debug!(path = %path_str, value = %s, bytes_len = bytes.len(), hex = ?bytes, "get_attribute -> UTF-8 string"),
                    Err(_) if bytes.len() == 8 => {
                        let value = i64::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]]);
                        debug!(path = %path_str, value = value, hex = ?bytes, "get_attribute -> i64");
                    }
                    Err(_) => debug!(path = %path_str, bytes = ?bytes, "get_attribute -> INVALID UTF-8"),
                }
            }
            Ok(None) => debug!(path = %path_str, "get_attribute -> None"),
            Err(e) => debug!(path = %path_str, error = %e, "get_attribute -> Error"),
        }

        result
    }

    fn get_request_headers(&self) -> Result<Vec<(String, String)>, AttributeError> {
        // Convert HeaderMap to Vec<(String, String)>
        let headers: Vec<(String, String)> = self
            .request_data
            .headers
            .iter()
            .map(|(name, value)| {
                let name_str = name.as_str().to_string();
                let value_str = value
                    .to_str()
                    .unwrap_or("")
                    .to_string();
                (name_str, value_str)
            })
            .collect();

        Ok(headers)
    }

    // ========================================================================
    // Request headers
    // ========================================================================

    fn get_response_headers(&self) -> Result<Vec<(String, String)>, AttributeError> {
        use tracing::debug;
        debug!("get_response_headers called (returning empty - not in response phase)");
        // Not in response phase yet, return empty
        Ok(Vec::new())
    }

    fn get_request_header_value(&self, key: &str) -> Result<Option<String>, AttributeError> {
        // Look up header by name (case-insensitive)
        match self.request_data.headers.get(key) {
            Some(value) => {
                let value_str = value
                    .to_str()
                    .map_err(|e| AttributeError::Retrieval(format!("invalid header value: {}", e)))?
                    .to_string();
                Ok(Some(value_str))
            }
            None => Ok(None),
        }
    }

    fn set_attribute(&self, path: &Path, _value: &[u8]) -> Result<(), AttributeError> {
        use tracing::debug;
        debug!(path = %path, "set_attribute called (no-op)");
        // No-op for now - we don't need to store attributes in the request phase
        Ok(())
    }

    // ========================================================================
    // Response headers
    // ========================================================================

    fn set_request_headers(&self, headers: Vec<(&str, &str)>) -> Result<(), AttributeError> {
        use tracing::debug;
        debug!(?headers, "set_request_headers called (no-op)");
        // No-op - we can't modify request headers after building the resolver
        Ok(())
    }

    fn set_response_headers(&self, headers: Vec<(&str, &str)>) -> Result<(), AttributeError> {
        use tracing::debug;
        debug!(?headers, "set_response_headers called (no-op)");
        // No-op - not in response phase yet
        Ok(())
    }

    // ========================================================================
    // Request body
    // ========================================================================

    fn get_http_request_body(
        &self,
        start: usize,
        size: usize,
    ) -> Result<Option<Vec<u8>>, AttributeError> {
        use tracing::debug;
        debug!(start, size, "get_http_request_body called (returning None - body not buffered)");
        // We don't buffer the request body in this POC
        Ok(None)
    }

    // ========================================================================
    // Response body
    // ========================================================================

    fn get_http_response_body(
        &self,
        start: usize,
        size: usize,
    ) -> Result<Option<Vec<u8>>, AttributeError> {
        use tracing::debug;
        debug!(start, size, "get_http_response_body called (returning None - not in response phase)");
        // Not in response phase yet
        Ok(None)
    }

    // ========================================================================
    // gRPC dispatch
    // ========================================================================

    fn dispatch_grpc_call(
        &self,
        upstream: &str,
        service: &str,
        method: &str,
        _headers: Vec<(&str, &[u8])>,
        message: Vec<u8>,
        timeout: Duration,
    ) -> Result<u32, ServiceError> {
        use tracing::debug;

        debug!(
            upstream = upstream,
            service = service,
            method = method,
            message_len = message.len(),
            "kuadrant: dispatching gRPC call"
        );

        // Debug: log the protobuf message bytes (full message for inspection)
        debug!(
            message_bytes = ?&message,
            total_len = message.len(),
            "kuadrant: protobuf message to send"
        );

        // Get upstream config
        let upstream_config = self
            .upstreams
            .get(upstream)
            .ok_or_else(|| ServiceError::Dispatch(format!("upstream '{}' not found", upstream)))?;

        debug!(url = upstream_config.url(), "kuadrant: connecting to upstream");

        // Get or create Channel (async operation)
        let channel = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.channel_registry
                    .get_or_create(
                        upstream,
                        upstream_config.url(),
                        Duration::from_millis(5000), // connect timeout
                    )
                    .await
            })
        })?;

        debug!("kuadrant: making gRPC call");

        // Debug: log the protobuf message bytes (full message for inspection)
        debug!(
            message_bytes = ?&message,
            total_len = message.len(),
            "TEST: kuadrant: protobuf message to send"
            );

        // Make gRPC call (async operation)
        let response = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                grpc_call(channel, service, method, message, timeout).await
            })
        })?;

        debug!(response_size = response.len(), "kuadrant: gRPC call successful");

        // Store response and return token
        let token = self
            .response_store
            .write()
            .expect("response store lock poisoned")
            .store(response);

        debug!(token = token, "kuadrant: stored response with token");

        Ok(token)
    }

    fn get_grpc_response(&self, size: usize) -> Result<Vec<u8>, ServiceError> {
        use tracing::debug;

        debug!(size, "kuadrant: retrieving gRPC response");

        // Get last stored response
        let response = self
            .response_store
            .write()
            .expect("response store lock poisoned")
            .get_last()
            .ok_or_else(|| ServiceError::Retrieval("no response available".to_string()))?;

        debug!(response_size = response.len(), "kuadrant: retrieved gRPC response");

        Ok(response)
    }

    // ========================================================================
    // HTTP response
    // ========================================================================

    fn send_http_reply(
        &self,
        status_code: u32,
        headers: Vec<(&str, &str)>,
        body: Option<&[u8]>,
    ) -> Result<(), ServiceError> {
        use tracing::debug;
        debug!(status_code, ?headers, body_len = body.map(|b| b.len()), "kuadrant: capturing HTTP reply");

        // Capture the reply details so the filter can return them after pipeline completes
        self.response_store
            .write()
            .expect("response store lock poisoned")
            .reply = Some(HttpReply {
                status_code,
                headers: headers.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
                body: body.map(|b| b.to_vec()),
            });

        Ok(())
    }
}

/// Initialize extensions needed by PraxisAttributeResolver.
///
/// Call this before creating the resolver to set up required state in extensions.
///
/// # Example
/// ```rust,ignore
/// initialize_extensions(&mut ctx, upstreams);
/// let channel_registry = ctx.extensions.get::<Arc<GrpcChannelRegistry>>().unwrap().clone();
/// let upstreams = ctx.extensions.get::<Arc<HashMap<String, UpstreamConfig>>>().unwrap().clone();
/// let response_store = ctx.extensions.get::<Arc<RwLock<GrpcResponseStore>>>().unwrap().clone();
/// let resolver = PraxisAttributeResolver::new(channel_registry, upstreams, response_store);
/// ```
#[allow(dead_code, reason = "WIP")]
pub fn initialize_extensions(
    ctx: &mut HttpFilterContext<'_>,
    upstreams: Arc<HashMap<String, UpstreamConfig>>,
) {
    ctx.extensions.insert(Arc::new(GrpcChannelRegistry::new()));
    ctx.extensions.insert(upstreams);
    ctx.extensions.insert(Arc::new(RwLock::new(GrpcResponseStore::new())));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grpc_response_store() {
        let mut store = GrpcResponseStore::new();

        // Store response
        let token1 = store.store(vec![1, 2, 3]);
        assert_eq!(token1, 0);

        // Store another
        let token2 = store.store(vec![4, 5, 6]);
        assert_eq!(token2, 1);

        // Get last
        let response = store.get_last();
        assert_eq!(response, Some(vec![4, 5, 6]));

        // Get last again (should be None - consumed)
        let response = store.get_last();
        assert_eq!(response, None);
    }

    #[tokio::test]
    async fn test_initialize_extensions() {
        use crate::test_utils::{make_filter_context, make_request};

        let req = make_request(http::Method::GET, "http://example.com/");
        let mut ctx = make_filter_context(&req);

        let upstreams = Arc::new(HashMap::new());
        initialize_extensions(&mut ctx, upstreams);

        // Verify extensions are set
        assert!(ctx.extensions.get::<Arc<GrpcChannelRegistry>>().is_some());
        assert!(ctx.extensions.get::<Arc<HashMap<String, UpstreamConfig>>>().is_some());
        assert!(ctx.extensions.get::<Arc<RwLock<GrpcResponseStore>>>().is_some());
    }
}
