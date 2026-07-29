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

    fn response_body_access(&self) -> praxis_filter::BodyAccess {
        // Need to access response body to bridge token metadata to pipeline context
        praxis_filter::BodyAccess::ReadOnly
    }

    fn response_body_mode(&self) -> praxis_filter::BodyMode {
        // Don't buffer - just need the metadata that token_count sets
        praxis_filter::BodyMode::Stream
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
            response_store.clone(),  // Clone Arc so we can use it later for digest
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

        debug!("kuadrant (on_request): evaluating pipeline");

        let state = pipeline.eval();

        let final_action = match state {
            kuadrant_filter::kuadrant::pipeline::PipelineState::InProgress(p) => {
                // Use requires_pause() to determine if waiting for gRPC response
                if p.requires_pause() {
                    debug!("kuadrant (on_request): pipeline paused, waiting for gRPC response(s)");

                    // Digest all available gRPC responses.
                    // Digesting one response may dispatch new gRPC calls, so keep processing
                    // batches until no more responses are available.
                    let mut current_pipeline = *p;
                    let mut pending_tokens = {
                        let mut store = response_store.write().expect("response store lock poisoned");
                        store.take_pending_digest()
                    };

                    while !pending_tokens.is_empty() {
                        debug!(count = pending_tokens.len(), "kuadrant (on_request): digesting gRPC responses");

                        while let Some(token) = pending_tokens.pop() {
                            let response_size = response_store
                                .read()
                                .expect("response store lock poisoned")
                                .get_response_size(token)
                                .unwrap_or(0);

                            debug!(token, response_size, "kuadrant (on_request): digesting gRPC response");

                            match current_pipeline.digest(token, 0, response_size) {
                                kuadrant_filter::kuadrant::pipeline::PipelineState::InProgress(digested) => {
                                    current_pipeline = *digested;
                                }
                                kuadrant_filter::kuadrant::pipeline::PipelineState::Completed { should_resume } => {
                                    debug!(should_resume, token, "kuadrant (on_request): pipeline completed");
                                    return Ok(if should_resume {
                                        FilterAction::Continue
                                    } else {
                                        self.handle_rejection(&response_store)?
                                    });
                                }
                            }
                        }

                        // Get next batch (may have been dispatched during digest)
                        pending_tokens = {
                            let mut store = response_store.write().expect("response store lock poisoned");
                            store.take_pending_digest()
                        };
                    }

                    // No more pending responses - store pipeline and continue
                    let still_paused = current_pipeline.requires_pause();
                    let key = Arc::as_ptr(&self.factory) as usize;

                    ctx.extensions
                        .get::<super::context::KuadrantPipelineStorage>()
                        .expect("KuadrantPipelineStorage not in extensions")
                        .lock()
                        .expect("pipeline storage lock poisoned")
                        .insert(key, current_pipeline);

                    debug!(key, requires_pause = still_paused, "kuadrant (on_request): stored pipeline");
                    FilterAction::Continue
                } else {
                    // Not paused - store pipeline for response phase
                    debug!("kuadrant (on_request): pipeline not paused, storing for response phase");

                    let key = Arc::as_ptr(&self.factory) as usize;
                    let pipeline_storage = ctx
                        .extensions
                        .get::<super::context::KuadrantPipelineStorage>()
                        .expect("KuadrantPipelineStorage not in extensions")
                        .clone();

                    pipeline_storage
                        .lock()
                        .expect("pipeline storage lock poisoned")
                        .insert(key, *p);

                    FilterAction::Continue
                }
            }
            kuadrant_filter::kuadrant::pipeline::PipelineState::Completed { should_resume } => {
                debug!(should_resume, "kuadrant (on_request): pipeline completed immediately");
                if should_resume {
                    FilterAction::Continue
                } else {
                    self.handle_rejection(&response_store)?
                }
            }
        };

        Ok(final_action)
    }

    async fn on_response(
        &self,
        ctx: &mut HttpFilterContext<'_>,
    ) -> Result<FilterAction, FilterError> {
        trace!("kuadrant filter: on_response");

        // Retrieve the pipeline that THIS filter instance stored in on_request
        // Use factory pointer address as unique key to avoid collision with other instances
        let key = Arc::as_ptr(&self.factory) as usize;
        let pipeline_storage = ctx
            .extensions
            .get::<super::context::KuadrantPipelineStorage>()
            .expect("KuadrantPipelineStorage not in extensions")
            .clone();

        let pipeline = {
            let mut storage = pipeline_storage
                .lock()
                .expect("pipeline storage lock poisoned");
            storage.remove(&key)
        };

        if let Some(pipeline) = pipeline {
            debug!(key, "kuadrant (on_response): resuming stored pipeline");

            let state = pipeline.eval();

            match state {
                kuadrant_filter::kuadrant::pipeline::PipelineState::InProgress(p) => {
                    debug!("kuadrant (on_response): pipeline in progress, storing for on_response_body");
                    pipeline_storage
                        .lock()
                        .expect("pipeline storage lock poisoned")
                        .insert(key, *p);
                }
                kuadrant_filter::kuadrant::pipeline::PipelineState::Completed { .. } => {
                    debug!("kuadrant (on_response): pipeline completed");
                }
            }
        } else {
            debug!("kuadrant filter: no stored pipeline, skipping response phase");
        }

        Ok(FilterAction::Continue)
    }

    fn on_response_body(
        &self,
        ctx: &mut HttpFilterContext<'_>,
        _body: &mut Option<bytes::Bytes>,
        _end_of_stream: bool,
    ) -> Result<FilterAction, FilterError> {
        trace!("kuadrant filter: on_response_body");

        // Retrieve the pipeline from on_response (if it stored one)
        let key = Arc::as_ptr(&self.factory) as usize;
        let pipeline_storage = ctx
            .extensions
            .get::<super::context::KuadrantPipelineStorage>()
            .expect("KuadrantPipelineStorage not in extensions")
            .clone();

        let pipeline = {
            let mut storage = pipeline_storage
                .lock()
                .expect("pipeline storage lock poisoned");
            storage.remove(&key)
        };

        if let Some(mut pipeline) = pipeline {
            debug!(key, "kuadrant (on_response_body): resuming stored pipeline");

            // Bridge metadata from token_count filter to pipeline context
            if let Some(total) = ctx.get_metadata("token.total").and_then(|s| s.parse::<i64>().ok()) {
                pipeline.ctx.response_body.set_value("/usage/total_tokens", total);
            }
            if let Some(input) = ctx.get_metadata("token.input").and_then(|s| s.parse::<i64>().ok()) {
                pipeline.ctx.response_body.set_value("/usage/input_tokens", input);
            }
            if let Some(output) = ctx.get_metadata("token.output").and_then(|s| s.parse::<i64>().ok()) {
                pipeline.ctx.response_body.set_value("/usage/output_tokens", output);
            }

            // Get response store
            let response_store = ctx
                .extensions
                .get::<Arc<std::sync::RwLock<super::context::GrpcResponseStore>>>()
                .expect("GrpcResponseStore not in extensions")
                .clone();

            // Evaluate the pipeline after setting response body context

            let state = pipeline.eval();

            match state {
                kuadrant_filter::kuadrant::pipeline::PipelineState::InProgress(p) => {
                    if p.requires_pause() {
                        debug!("kuadrant (on_response_body): digesting pending gRPC responses");

                        let mut current_pipeline = *p;
                        let mut pending_tokens = {
                            let mut store = response_store.write().expect("response store lock poisoned");
                            store.take_pending_digest()
                        };

                        while let Some(token) = pending_tokens.pop() {
                            let response_size = response_store
                                .read()
                                .expect("response store lock poisoned")
                                .get_response_size(token)
                                .unwrap_or(0);

                            debug!(token, "kuadrant (on_response_body): digesting gRPC response");

                            match current_pipeline.digest(token, 0, response_size) {
                                kuadrant_filter::kuadrant::pipeline::PipelineState::InProgress(digested) => {
                                    current_pipeline = *digested;
                                }
                                kuadrant_filter::kuadrant::pipeline::PipelineState::Completed { .. } => {
                                    break;
                                }
                            }
                        }
                    }
                }
                kuadrant_filter::kuadrant::pipeline::PipelineState::Completed { .. } => {
                    debug!("kuadrant (on_response_body): pipeline completed");
                }
            }
        } else {
            debug!("kuadrant filter: no stored pipeline in on_response_body");
        }

        Ok(FilterAction::Continue)
    }
}

impl KuadrantFilter {
    fn handle_rejection(
        &self,
        response_store: &Arc<std::sync::RwLock<super::context::GrpcResponseStore>>,
    ) -> Result<FilterAction, FilterError> {
        let reply = response_store
            .write()
            .expect("response store lock poisoned")
            .take_reply();

        match reply {
            Some(r) => {
                debug!(status_code = r.status_code, "kuadrant filter: request denied by policy");

                let mut rejection = praxis_filter::Rejection::status(r.status_code as u16);

                for (name, value) in r.headers {
                    rejection = rejection.with_header(name, value);
                }

                if let Some(body) = r.body {
                    rejection = rejection.with_body(body);
                }

                Ok(FilterAction::Reject(rejection))
            }
            None => {
                debug!("kuadrant filter: denied without reply details, using default 403");
                Ok(FilterAction::Reject(praxis_filter::Rejection::status(403)))
            }
        }
    }
}
