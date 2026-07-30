// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Praxis Contributors

//! Tests for Kuadrant filter configuration parsing and validation.

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::needless_raw_strings,
    clippy::needless_raw_string_hashes,
    reason = "tests"
)]
mod config_tests {
    use super::super::config::KuadrantFilterConfig;

    #[test]
    fn test_valid_config_with_single_upstream() {
        let yaml = r#"
upstreams:
  authorino:
    url: authorino.default.svc:50051
    timeout: 1000
    max_connections: 50
kuadrant_config:
  services:
    authorino:
      type: auth
      endpoint: authorino
      failureMode: deny
  actionSets:
    - name: test
      routeRuleConditions:
        hostnames: ["*.example.com"]
      actions:
        - service: authorino
          scope: auth
"#;

        let config: KuadrantFilterConfig = serde_yaml::from_str(yaml).expect("should parse");

        // Verify upstreams parsed correctly
        assert_eq!(config.upstreams().len(), 1);
        let upstream = config.get_upstream("authorino").expect("authorino upstream should exist");
        assert_eq!(upstream.url(), "authorino.default.svc:50051");
        assert_eq!(upstream.timeout(), 1000);
        assert_eq!(upstream.max_connections(), 50);

        // Verify kuadrant_config parsed correctly
        assert!(config.kuadrant_config().services.contains_key("authorino"));
    }

    #[test]
    fn test_valid_config_with_multiple_upstreams() {
        let yaml = r#"
upstreams:
  authorino:
    url: authorino.default.svc:50051
    timeout: 1000
    max_connections: 50
  limitador:
    url: limitador.default.svc:8081
    timeout: 500
    max_connections: 20
kuadrant_config:
  services:
    auth-service:
      type: auth
      endpoint: authorino
      failureMode: deny
    rate-limit-service:
      type: ratelimit
      endpoint: limitador
      failureMode: allow
  actionSets: []
"#;

        let config: KuadrantFilterConfig = serde_yaml::from_str(yaml).expect("should parse");

        assert_eq!(config.upstreams().len(), 2);
        assert_eq!(config.kuadrant_config().services.len(), 2);

        // Verify both upstreams are accessible
        assert!(config.get_upstream("authorino").is_some());
        assert!(config.get_upstream("limitador").is_some());
    }

    #[test]
    fn test_missing_required_upstream_fields() {
        let yaml = r#"
upstreams:
  authorino:
    url: authorino.default.svc:50051
kuadrant_config:
  services:
    authorino:
      type: auth
      endpoint: authorino
  actionSets: []
"#;

        let result: Result<KuadrantFilterConfig, _> = serde_yaml::from_str(yaml);
        assert!(result.is_err(), "should fail when timeout/max_connections missing");
    }

    #[test]
    fn test_unknown_fields_rejected() {
        let yaml = r#"
upstreams:
  authorino:
    url: authorino.default.svc:50051
    timeout: 1000
    max_connections: 50
    unknown_field: "should fail"
kuadrant_config:
  services:
    authorino:
      type: auth
      endpoint: authorino
  actionSets: []
"#;

        let result: Result<KuadrantFilterConfig, _> = serde_yaml::from_str(yaml);
        assert!(result.is_err(), "should reject unknown fields");
    }
}

#[cfg(test)]
mod validation_tests {
    use super::super::config::KuadrantFilterConfig;

    #[test]
    fn test_validate_service_endpoints_exist() {
        let yaml = r#"
upstreams:
  authorino:
    url: authorino.default.svc:50051
    timeout: 1000
    max_connections: 50
kuadrant_config:
  services:
    my-service:
      type: auth
      endpoint: authorino
      failureMode: deny
  actionSets: []
"#;

        let config: KuadrantFilterConfig = serde_yaml::from_str(yaml).expect("should parse");

        // This should pass validation
        let result = config.validate();
        assert!(result.is_ok(), "valid config should pass validation");
    }

    #[test]
    fn test_validate_fails_on_missing_endpoint() {
        let yaml = r#"
upstreams:
  authorino:
    url: authorino.default.svc:50051
    timeout: 1000
    max_connections: 50
kuadrant_config:
  services:
    my-service:
      type: auth
      endpoint: nonexistent
      failureMode: deny
  actionSets: []
"#;

        let config: KuadrantFilterConfig = serde_yaml::from_str(yaml).expect("should parse");

        // This should fail validation
        let result = config.validate();
        assert!(result.is_err(), "should fail when service.endpoint references missing upstream");
        let err = result.unwrap_err();
        assert!(err.contains("nonexistent"), "error should mention the missing endpoint");
    }

    #[test]
    fn test_validate_multiple_services_multiple_upstreams() {
        let yaml = r#"
upstreams:
  authorino:
    url: authorino.default.svc:50051
    timeout: 1000
    max_connections: 50
  limitador:
    url: limitador.default.svc:8081
    timeout: 500
    max_connections: 20
kuadrant_config:
  services:
    auth:
      type: auth
      endpoint: authorino
      failureMode: deny
    ratelimit:
      type: ratelimit
      endpoint: limitador
      failureMode: allow
  actionSets: []
"#;

        let config: KuadrantFilterConfig = serde_yaml::from_str(yaml).expect("should parse");
        let result = config.validate();
        assert!(result.is_ok(), "all endpoints should resolve");
    }

    #[test]
    fn test_validate_empty_upstreams() {
        let yaml = r#"
upstreams: {}
kuadrant_config:
  services:
    auth:
      type: auth
      endpoint: authorino
      failureMode: deny
  actionSets: []
"#;

        let config: KuadrantFilterConfig = serde_yaml::from_str(yaml).expect("should parse");
        let result = config.validate();
        assert!(result.is_err(), "should fail when no upstreams defined but services reference them");
    }
}
