use super::HealthRoute;
use hyper::{Method, StatusCode};

#[test]
fn health_paths_answer_no_content_for_get_and_head() {
    for path in ["/healthz", "/readyz"] {
        assert!(HealthRoute::matches(path));
        for method in [Method::GET, Method::HEAD] {
            let response = HealthRoute::dispatch(&method);
            assert_eq!(response.status(), StatusCode::NO_CONTENT);
        }
    }
}

#[test]
fn health_paths_report_the_allowed_methods() {
    for method in [Method::POST, Method::DELETE] {
        let response = HealthRoute::dispatch(&method);
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(
            response
                .headers()
                .get("allow")
                .and_then(|value| value.to_str().ok()),
            Some("GET, HEAD")
        );
    }
}

#[test]
fn other_paths_are_not_health_probes() {
    for path in ["/", "/v1", "/v1/healthz", "/unknown"] {
        assert!(!HealthRoute::matches(path));
    }
}
