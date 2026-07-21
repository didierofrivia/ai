// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Praxis Contributors

//! Kuadrant filter integration for Praxis.
//!
//! Provides integration with Kuadrant services (Authorino, Limitador)
//! using the kuadrant-filter crate for configuration and orchestration.

mod config;
mod filter;
mod grpc;

#[cfg(test)]
mod tests;

pub use filter::KuadrantFilter;
