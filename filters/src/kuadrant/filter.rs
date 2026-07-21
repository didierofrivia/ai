// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Praxis Contributors

//! Kuadrant filter implementation.

use async_trait::async_trait;
use praxis_filter::{FilterAction, FilterError, HttpFilter, HttpFilterContext};
use std::collections::HashMap;
use std::sync::Arc;

/// Kuadrant filter for integrating with Authorino and Limitador services.
///
/// This filter uses the kuadrant-filter crate's Pipeline pattern to
/// orchestrate calls to Kuadrant services based on configuration.
#[allow(dead_code, reason = "WIP")]
pub struct KuadrantFilter {
    /// Upstream connection configurations keyed by upstream name.
    upstreams: Arc<HashMap<String, super::config::UpstreamConfig>>,

    /// Kuadrant plugin configuration (policy engine).
    kuadrant_config: kuadrant_filter::configuration::PluginConfiguration,
}

impl KuadrantFilter {
    /// Create from parsed YAML config.
    ///
    /// # Errors
    ///
    /// Returns [`FilterError`] if config parsing or validation fails.
    pub fn from_config(config: &serde_yaml::Value) -> Result<Box<dyn HttpFilter>, FilterError> {
        // Parse config from YAML
        let cfg: super::config::KuadrantFilterConfig =
            praxis_filter::parse_filter_config("kuadrant", config)?;

        // Validate configuration (check service endpoints reference existing upstreams)
        cfg.validate()
            .map_err(|e| FilterError::from(format!("kuadrant config validation failed: {}", e)))?;

        // Consume config to extract upstreams and kuadrant_config without cloning
        let (upstreams, kuadrant_config) = cfg.into_parts();

        Ok(Box::new(Self {
            upstreams: Arc::new(upstreams),
            kuadrant_config,
        }))
    }
}

#[async_trait]
impl HttpFilter for KuadrantFilter {
    fn name(&self) -> &'static str {
        "kuadrant"
    }

    async fn on_request(
        &self,
        _ctx: &mut HttpFilterContext<'_>,
    ) -> Result<FilterAction, FilterError> {
        // TODO: Build pipeline, execute, handle response
        Ok(FilterAction::Continue)
    }
}
