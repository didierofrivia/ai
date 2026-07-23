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

/// Storage for gRPC responses (synchronous bridge for async calls).
///
/// Since `AttributeResolver::dispatch_grpc_call()` is synchronous but we need
/// async gRPC calls, we:
/// 1. Block on the async call in `dispatch_grpc_call()`
/// 2. Store the response here
/// 3. Return a token_id
/// 4. `get_grpc_response()` retrieves the stored response
#[derive(Debug, Default)]
pub struct GrpcResponseStore {
    responses: HashMap<u32, Vec<u8>>,
    next_token: u32,
    last_token: Option<u32>,
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
        self.next_token += 1;
        token
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
        use tracing::trace;

        // Map Envoy-style attribute paths to Praxis request data
        let path_str = path.to_string();
        trace!(path = %path_str, "get_attribute called");

        match path_str.as_str() {
            "request.host" => {
                // Extract host from URI or Host header
                if let Some(host) = self.request_data.uri.host() {
                    return Ok(Some(host.as_bytes().to_vec()));
                }
                if let Some(host) = self.request_data.headers.get(http::header::HOST) {
                    return Ok(Some(host.as_bytes().to_vec()));
                }
                Ok(None)
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
                // TODO: Type metadata issue - CEL treating binary as string
                // Temporarily return None to isolate UTF-8 error
                Ok(None)
            }
            "request.protocol" => {
                // HTTP version as UTF-8 string (required field)
                Ok(Some(b"HTTP/1.1".to_vec()))
            }
            "destination.address" => {
                // TODO: Extract from connection metadata
                // Address is a UTF-8 string "ip:port"
                Ok(Some(b"127.0.0.1".to_vec()))
            }
            "destination.port" => {
                // TODO: Type metadata issue - CEL treating binary as string
                Ok(None)
            }
            "source.address" => {
                // TODO: Extract from connection metadata
                Ok(Some(b"127.0.0.1".to_vec()))
            }
            "source.port" => {
                // TODO: Type metadata issue - CEL treating binary as string
                Ok(None)
            }
            _ => {
                // Unsupported attribute
                Err(AttributeError::NotAvailable(format!(
                    "Attribute `{}` not supported in Praxis AttributeResolver",
                    path_str
                )))
            }
        }
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
            "kuadrant: dispatching gRPC call"
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
        debug!(status_code, ?headers, body_len = body.map(|b| b.len()), "send_http_reply called");
        // TODO: Store this to return as the actual HTTP response
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
