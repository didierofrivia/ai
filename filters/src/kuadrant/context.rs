// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Praxis Contributors

//! Praxis implementation of kuadrant-filter's AttributeResolver trait.
//!
//! This module provides the bridge between Praxis's HTTP filter pipeline
//! and kuadrant-filter's policy enforcement engine.

use kuadrant_filter::data::attribute::{AttributeError, Path};
use kuadrant_filter::kuadrant::resolver::AttributeResolver;
use kuadrant_filter::services::ServiceError;
use praxis_filter::HttpFilterContext;

/// Praxis implementation of kuadrant-filter's AttributeResolver trait.
///
/// This struct bridges Praxis's `HttpFilterContext` to kuadrant-filter's
/// policy enforcement by implementing all required I/O operations.
pub struct PraxisAttributeResolver<'a> {
    /// Reference to Praxis HTTP filter context
    #[allow(dead_code, reason = "WIP")]
    ctx: &'a HttpFilterContext<'a>,
}

impl<'a> PraxisAttributeResolver<'a> {
    /// Create a new resolver from a Praxis HttpFilterContext
    #[allow(dead_code, reason = "WIP")]
    pub fn new(ctx: &'a HttpFilterContext<'a>) -> Self {
        Self { ctx }
    }
}

impl AttributeResolver for PraxisAttributeResolver<'_> {
    // ========================================================================
    // Attribute access (Envoy CEL attributes)
    // ========================================================================

    fn get_attribute(&self, _path: &Path) -> Result<Option<Vec<u8>>, AttributeError> {
        todo!("get_attribute")
    }

    fn get_request_headers(&self) -> Result<Vec<(String, String)>, AttributeError> {
        todo!("get_request_headers")
    }

    // ========================================================================
    // Request headers
    // ========================================================================

    fn get_response_headers(&self) -> Result<Vec<(String, String)>, AttributeError> {
        todo!("get_response_headers")
    }

    fn get_request_header_value(&self, _key: &str) -> Result<Option<String>, AttributeError> {
        todo!("get_request_header_value")
    }

    fn set_attribute(&self, _path: &Path, _value: &[u8]) -> Result<(), AttributeError> {
        todo!("set_attribute")
    }

    // ========================================================================
    // Response headers
    // ========================================================================

    fn set_request_headers(&self, _headers: Vec<(&str, &str)>) -> Result<(), AttributeError> {
        todo!("set_request_headers")
    }

    fn set_response_headers(&self, _headers: Vec<(&str, &str)>) -> Result<(), AttributeError> {
        todo!("set_response_headers")
    }

    // ========================================================================
    // Request body
    // ========================================================================

    fn get_http_request_body(
        &self,
        _start: usize,
        _size: usize,
    ) -> Result<Option<Vec<u8>>, AttributeError> {
        todo!("get_http_request_body")
    }

    // ========================================================================
    // Response body
    // ========================================================================

    fn get_http_response_body(
        &self,
        _start: usize,
        _size: usize,
    ) -> Result<Option<Vec<u8>>, AttributeError> {
        todo!("get_http_response_body")
    }

    // ========================================================================
    // gRPC dispatch
    // ========================================================================

    fn dispatch_grpc_call(
        &self,
        _upstream: &str,
        _service: &str,
        _method: &str,
        _headers: Vec<(&str, &[u8])>,
        _message: Vec<u8>,
        _timeout_ms: std::time::Duration,
    ) -> Result<u32, ServiceError> {
        todo!("dispatch_grpc_call")
    }

    fn get_grpc_response(&self, _size: usize) -> Result<Vec<u8>, ServiceError> {
        todo!("get_grpc_response")
    }

    // ========================================================================
    // HTTP response
    // ========================================================================

    fn send_http_reply(
        &self,
        _status_code: u32,
        _headers: Vec<(&str, &str)>,
        _body: Option<&[u8]>,
    ) -> Result<(), ServiceError> {
        todo!("send_http_reply")
    }
}
