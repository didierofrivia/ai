// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Praxis Contributors

//! gRPC client implementation using tonic for Kuadrant service calls.

use kuadrant_filter::services::ServiceError;
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};
use tower::ServiceExt;

/// Registry of tonic Channels for different upstreams.
///
/// Stored in `HttpFilterContext::extensions` for connection pooling.
/// Each Channel manages its own HTTP/2 connection pool.
#[allow(dead_code, reason = "WIP")]
pub struct GrpcChannelRegistry {
    channels: dashmap::DashMap<String, Channel>,
}

#[allow(dead_code, reason = "WIP")]
impl GrpcChannelRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            channels: dashmap::DashMap::new(),
        }
    }

    /// Get or create a Channel for the given upstream.
    ///
    /// Channels are cached by upstream name for connection pooling.
    ///
    /// # Arguments
    /// * `upstream_name` - Name of the upstream (cache key)
    /// * `endpoint` - gRPC endpoint URL (e.g., "http://authorino:50051")
    /// * `connect_timeout` - Timeout for initial connection
    pub async fn get_or_create(
        &self,
        upstream_name: &str,
        endpoint: &str,
        connect_timeout: Duration,
    ) -> Result<Channel, ServiceError> {
        // Check if channel already exists
        if let Some(channel) = self.channels.get(upstream_name) {
            return Ok(channel.clone());
        }

        // Create new channel
        let channel = Endpoint::from_shared(endpoint.to_string())
            .map_err(|e| ServiceError::Dispatch(format!("invalid endpoint: {}", e)))?
            .connect_timeout(connect_timeout)
            .http2_keep_alive_interval(Duration::from_secs(30))
            .keep_alive_while_idle(true)
            .connect()
            .await
            .map_err(|e| ServiceError::Dispatch(e.to_string()))?;

        // Store in registry
        self.channels.insert(upstream_name.to_string(), channel.clone());

        Ok(channel)
    }
}

impl Default for GrpcChannelRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Make a unary gRPC call with raw protobuf bytes using tonic Channel.
///
/// # Arguments
/// * `channel` - Tonic Channel to use
/// * `service` - Full service name (e.g., "envoy.service.auth.v3.Authorization")
/// * `method` - Method name (e.g., "Check")
/// * `request` - Serialized protobuf message bytes
/// * `timeout` - Call timeout
///
/// # Returns
/// Serialized protobuf response bytes
///
/// # Example
/// ```rust,ignore
/// let response = grpc_call(
///     channel,
///     "envoy.service.auth.v3.Authorization",
///     "Check",
///     request_bytes,
///     Duration::from_millis(1000),
/// ).await?;
/// ```
#[allow(dead_code, reason = "WIP")]
pub async fn grpc_call(
    mut channel: Channel,
    service: &str,
    method: &str,
    request: Vec<u8>,
    timeout: Duration,
) -> Result<Vec<u8>, ServiceError> {
    // Construct gRPC path: /{service}/{method}
    let path = format!("/{service}/{method}");

    // Convert Vec<u8> to Bytes for ProstCodec
    let request_bytes = bytes::Bytes::from(request);

    // Create tonic request with timeout
    let mut tonic_request = tonic::Request::new(request_bytes);
    tonic_request.set_timeout(timeout);

    // Wait for channel to be ready (required by tower::Service trait)
    let ready_channel = channel
        .ready()
        .await
        .map_err(|e| ServiceError::Dispatch(format!("channel not ready: {}", e)))?;

    // Make unary call using ready Channel
    let mut client = tonic::client::Grpc::new(ready_channel);

    // Use tonic's built-in bytes codec
    let codec = tonic::codec::ProstCodec::<bytes::Bytes, bytes::Bytes>::default();

    let response = client
        .unary(tonic_request, path.parse().expect("valid path"), codec)
        .await
        .map_err(|status| ServiceError::Dispatch(format!("gRPC status {}: {}", status.code() as i32, status.message())))?;

    Ok(response.into_inner().to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grpc_channel_registry_empty() {
        let registry = GrpcChannelRegistry::new();
        assert_eq!(registry.channels.len(), 0);
    }

    #[test]
    fn test_grpc_channel_registry_default() {
        let registry = GrpcChannelRegistry::default();
        assert_eq!(registry.channels.len(), 0);
    }
}

