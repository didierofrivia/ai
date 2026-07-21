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
///     - authorino:
///         url: authorino.default.svc:50051
///         timeout: 1000
///         max_connections: 50
///   kuadrant_config:
///     services:
///       authorino:
///         type: auth
///         endpoint: authorino
///         failure_mode: deny
/// ```
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct KuadrantFilterConfig {
    /// Upstream endpoint definitions.
    pub upstreams: Vec<UpstreamDefinition>,

    /// Kuadrant plugin configuration (from kuadrant-filter crate).
    pub kuadrant_config: kuadrant_filter::configuration::PluginConfiguration,
}

/// A single upstream definition entry.
///
/// Each entry is a map with one key-value pair where the key is the
/// upstream name and the value is the upstream configuration.
#[derive(Debug, Deserialize)]
pub(super) struct UpstreamDefinition {
    /// The upstream name and its configuration.
    #[serde(flatten)]
    pub upstream: HashMap<String, UpstreamConfig>,
}

/// Upstream connection configuration.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct UpstreamConfig {
    /// gRPC endpoint URL (host:port).
    pub url: String,

    /// Connection timeout in milliseconds.
    pub timeout: u64,

    /// Maximum concurrent connections for this upstream.
    pub max_connections: usize,
}
