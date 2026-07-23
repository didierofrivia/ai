// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Praxis Contributors

//! Configuration types for the Kuadrant filter.

use serde::Deserialize;
use std::collections::HashMap;

/// Top-level configuration for the Kuadrant filter.
///
/// # Example
///
/// ```yaml
/// filter: kuadrant
///   upstreams:
///     authorino:
///       url: authorino.default.svc:50051
///       timeout: 1000
///       max_connections: 50
///     limitador:
///       url: limitador.default.svc:8081
///       timeout: 500
///       max_connections: 20
///   kuadrant_config:
///     services:
///       authorino:
///         type: auth
///         endpoint: authorino
///         failureMode: deny
/// ```
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code, reason = "WIP")]
pub(super) struct KuadrantFilterConfig {
    /// Upstream endpoint definitions keyed by upstream name.
    upstreams: HashMap<String, UpstreamConfig>,

    /// Kuadrant plugin configuration (from kuadrant-filter crate).
    kuadrant_config: kuadrant_filter::configuration::PluginConfiguration,
}

/// Upstream connection configuration.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code, reason = "WIP")]
pub(super) struct UpstreamConfig {
    /// gRPC endpoint URL (host:port).
    url: String,

    /// Connection timeout in milliseconds.
    timeout: u64,

    /// Maximum concurrent connections for this upstream.
    max_connections: usize,
}

#[allow(dead_code, reason = "WIP")]
impl KuadrantFilterConfig {
    /// Returns a reference to the upstreams map.
    pub(super) fn upstreams(&self) -> &HashMap<String, UpstreamConfig> {
        &self.upstreams
    }

    /// Returns a reference to the kuadrant configuration.
    pub(super) fn kuadrant_config(&self) -> &kuadrant_filter::configuration::PluginConfiguration {
        &self.kuadrant_config
    }

    /// Validates the configuration.
    ///
    /// Checks that all service endpoints reference existing upstreams.
    ///
    /// # Errors
    ///
    /// Returns an error message if any service endpoint references
    /// a non-existent upstream.
    pub(super) fn validate(&self) -> Result<(), String> {
        // Check that all service endpoints reference existing upstreams
        for (service_name, service_config) in &self.kuadrant_config.services {
            let endpoint = &service_config.endpoint;
            if !self.upstreams.contains_key(endpoint) {
                return Err(format!(
                    "service '{}' references non-existent upstream endpoint '{}'",
                    service_name, endpoint
                ));
            }
        }

        Ok(())
    }

    /// Looks up an upstream configuration by name.
    ///
    /// Returns `None` if the upstream is not found.
    pub(super) fn get_upstream(&self, name: &str) -> Option<&UpstreamConfig> {
        self.upstreams.get(name)
    }

    /// Consume this config and return owned upstreams and kuadrant_config.
    pub(super) fn into_parts(
        self,
    ) -> (
        HashMap<String, UpstreamConfig>,
        kuadrant_filter::configuration::PluginConfiguration,
    ) {
        (self.upstreams, self.kuadrant_config)
    }
}

#[allow(dead_code, reason = "WIP")]
impl UpstreamConfig {
    /// Returns the gRPC endpoint URL.
    pub(super) fn url(&self) -> &str {
        &self.url
    }

    /// Returns the connection timeout in milliseconds.
    pub(super) fn timeout(&self) -> u64 {
        self.timeout
    }

    /// Returns the maximum concurrent connections.
    pub(super) fn max_connections(&self) -> usize {
        self.max_connections
    }
}
