// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Praxis Contributors

//! Kuadrant filter implementation.

use async_trait::async_trait;
use praxis_filter::{FilterAction, FilterError, HttpFilter, HttpFilterContext};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, trace};
use kuadrant_filter::kuadrant::pipeline::PipelineFactory;

/// Kuadrant filter for integrating with Authorino and Limitador services.
///
/// This filter uses the kuadrant-filter crate's Pipeline pattern to
/// orchestrate calls to Kuadrant services based on configuration.
///
/// With the `threadsafe` feature enabled in kuadrant-filter, PipelineFactory
/// uses Arc instead of Rc internally, making it Send+Sync compatible with
/// Praxis's multi-threaded environment.
#[allow(dead_code, reason = "WIP")]
pub struct KuadrantFilter {
    /// Upstream connection configurations keyed by upstream name.
    upstreams: Arc<HashMap<String, super::config::UpstreamConfig>>,

    /// Pipeline factory (built once at startup, shared across requests).
    factory: Arc<PipelineFactory>,
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

        // Build PipelineFactory once at startup (same pattern as wasm-shim)
        // With threadsafe feature, this uses Arc internally and is Send+Sync
        let descriptor_manager = Arc::new(
            kuadrant_filter::descriptor_manager::DescriptorManager::default()
        );
        let factory = PipelineFactory::try_from(kuadrant_config, &descriptor_manager)
            .map_err(|e| FilterError::from(format!("failed to compile kuadrant config: {:?}", e)))?;

        Ok(Box::new(Self {
            upstreams: Arc::new(upstreams),
            factory: Arc::new(factory),
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
        ctx: &mut HttpFilterContext<'_>,
    ) -> Result<FilterAction, FilterError> {
        trace!("kuadrant filter: on_request");

        // Log request details for debugging
        debug!(
            method = %ctx.request.method,
            uri = %ctx.request.uri,
            "kuadrant filter: processing request"
        );

        // Initialize extensions needed by PraxisAttributeResolver
        super::context::initialize_extensions(ctx, self.upstreams.clone());

        // Get extension references
        let channel_registry = ctx
            .extensions
            .get::<Arc<super::grpc::GrpcChannelRegistry>>()
            .expect("GrpcChannelRegistry not in extensions")
            .clone();

        let upstreams = ctx
            .extensions
            .get::<Arc<HashMap<String, super::config::UpstreamConfig>>>()
            .expect("Upstreams not in extensions")
            .clone();

        let response_store = ctx
            .extensions
            .get::<Arc<std::sync::RwLock<super::context::GrpcResponseStore>>>()
            .expect("GrpcResponseStore not in extensions")
            .clone();

        // Create the attribute resolver that bridges Praxis <-> kuadrant-filter
        let resolver = super::context::PraxisAttributeResolver::new(
            ctx,
            channel_registry,
            upstreams,
            response_store,
        );

        // Build ReqRespCtx (same as wasm-shim's new_ctx())
        let req_resp_ctx = kuadrant_filter::kuadrant::ReqRespCtx::new(Arc::new(resolver));

        // Use the stored factory to build a pipeline for this request
        // (same pattern as wasm-shim's on_http_request_headers)
        let pipeline = match self.factory.build(req_resp_ctx)
            .map_err(|e| FilterError::from(format!("failed to build pipeline: {:?}", e)))? {
            Some(p) => p,
            None => {
                debug!("kuadrant filter: no matching actionSet, allowing request");
                return Ok(FilterAction::Continue);
            }
        };

        debug!("kuadrant filter: evaluating pipeline");

        // Evaluate the pipeline (same as wasm-shim)
        let state = pipeline.eval();

        match state {
            kuadrant_filter::kuadrant::pipeline::PipelineState::InProgress(_) => {
                debug!("kuadrant filter: pipeline in progress (async work pending)");
                // TODO: Handle async/deferred tasks (store pipeline, wait for gRPC responses)
                Ok(FilterAction::Continue)
            }
            kuadrant_filter::kuadrant::pipeline::PipelineState::Completed { should_resume } => {
                debug!(should_resume, "kuadrant filter: pipeline completed");
                // TODO: Check if request was denied/rate-limited
                Ok(FilterAction::Continue)
            }
        }
    }
}
