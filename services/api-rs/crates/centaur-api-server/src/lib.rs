mod anthropic;
mod api_jwt;
pub mod client;
mod error;
mod mcp;
mod model_proxy;
mod openai;
mod routes;
mod slack_proxy;
mod tool_discovery;
pub mod types;

pub use centaur_session_runtime::{SandboxRuntime, SessionRuntime};
pub use error::ApiError;
pub use routes::{
    AppState, build_router_with_app_state, build_router_with_runtime,
    build_router_with_session_and_workflow_runtime, build_router_with_session_runtime,
};
pub use tool_discovery::{
    DiscoveredToolProxyFragment, ToolDiscoveryConfig, ToolDiscoveryError,
    discover_persona_registry, discover_tool_proxy_fragment,
};

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    };

    use async_trait::async_trait;
    use axum::{
        body::{Body, to_bytes},
        http::{Method, Request, StatusCode, header},
    };
    use centaur_sandbox_core::{
        ObservedSandbox, SandboxBackend, SandboxError, SandboxHandle, SandboxId, SandboxIo,
        SandboxResult, SandboxSpec, SandboxStatus,
    };
    use centaur_session_runtime::SandboxRuntime;
    use centaur_session_sqlx::{PgSessionStore, SessionStoreError};
    use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
    use serde_json::{Value, json};
    use sqlx::PgPool;
    use tokio::{
        io::{AsyncWriteExt, DuplexStream},
        sync::Mutex,
    };
    use tower::ServiceExt;

    use super::{AppState, build_router_with_app_state, build_router_with_runtime};

    static DB_TEST_LOCK: Mutex<()> = Mutex::const_new(());

    #[tokio::test]
    async fn router_builds() {
        let pool =
            PgPool::connect_lazy("postgres://postgres:postgres@localhost/centaur_test").unwrap();
        let _router = build_router_with_runtime(
            PgSessionStore::new(pool),
            SandboxRuntime::backend(Arc::new(TestBackend::default()), SandboxSpec::new("test")),
        );
    }

    #[tokio::test]
    async fn metrics_endpoint_renders_http_request_metrics() {
        let pool =
            PgPool::connect_lazy("postgres://postgres:postgres@localhost/centaur_test").unwrap();
        let app = build_router_with_runtime(
            PgSessionStore::new(pool),
            SandboxRuntime::backend(Arc::new(TestBackend::default()), SandboxSpec::new("test")),
        );

        let app = app
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(app.status(), StatusCode::OK);

        let pool =
            PgPool::connect_lazy("postgres://postgres:postgres@localhost/centaur_test").unwrap();
        let app = build_router_with_runtime(
            PgSessionStore::new(pool),
            SandboxRuntime::backend(Arc::new(TestBackend::default()), SandboxSpec::new("test")),
        );
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(
            body.contains(
                r#"http_server_requests_total{method="GET",route="/healthz",status="200"}"#
            )
        );
    }

    #[tokio::test]
    async fn healthz_is_available_before_runtime_is_ready() {
        let app = build_router_with_app_state(AppState::unready());

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn healthz_decodes_slack_client_bearer_jwt_when_present() {
        let app = build_router_with_app_state(AppState::unready());
        let token = encode(
            &Header::new(Algorithm::HS256),
            &json!({
                "iss": "centaur-console",
                "sub": "principal_123",
                "aud": "centaur-api",
                "iat": 1_700_000_000i64,
                "exp": 4_102_444_800i64,
                "slack": {
                    "upload_channels": ["C123456789"],
                    "download_channels": ["C987654321"],
                    "history_channels": ["C111111111"]
                }
            }),
            &EncodingKey::from_secret(b"test-secret"),
        )
        .unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body.get("ok").and_then(Value::as_bool), Some(true));
        assert_eq!(
            body.pointer("/slack_client_jwt/claims/sub")
                .and_then(Value::as_str),
            Some("principal_123")
        );
        assert_eq!(
            body.pointer("/slack_client_jwt/claims/slack/upload_channels/0")
                .and_then(Value::as_str),
            Some("C123456789")
        );
        assert_eq!(
            body.pointer("/slack_client_jwt/claims/slack/download_channels/0")
                .and_then(Value::as_str),
            Some("C987654321")
        );
        assert_eq!(
            body.pointer("/slack_client_jwt/claims/slack/history_channels/0")
                .and_then(Value::as_str),
            Some("C111111111")
        );
    }

    #[tokio::test]
    async fn readyz_reports_starting_until_runtime_is_ready() {
        let state = AppState::unready();
        let app = build_router_with_app_state(state.clone());

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let pool =
            PgPool::connect_lazy("postgres://postgres:postgres@localhost/centaur_test").unwrap();
        state.mark_ready(
            centaur_session_runtime::SessionRuntime::new(
                PgSessionStore::new(pool),
                SandboxRuntime::backend(Arc::new(TestBackend::default()), SandboxSpec::new("test")),
            ),
            None,
            None,
        );
        let app = build_router_with_app_state(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn runtime_routes_report_unavailable_until_runtime_is_ready() {
        for request in [
            Request::builder()
                .method(Method::GET)
                .uri("/api/session/slack%3AC123%3A123.456")
                .body(Body::empty())
                .unwrap(),
            Request::builder()
                .method(Method::POST)
                .uri("/api/session/slack%3AC123%3A123.456")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"harness_type":"codex"}"#))
                .unwrap(),
            Request::builder()
                .method(Method::POST)
                .uri("/api/session/slack%3AC123%3A123.456/messages")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"messages":[]}"#))
                .unwrap(),
            Request::builder()
                .method(Method::POST)
                .uri("/api/session/slack%3AC123%3A123.456/execute")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"input_lines":[]}"#))
                .unwrap(),
            Request::builder()
                .method(Method::GET)
                .uri("/api/session/slack%3AC123%3A123.456/events")
                .body(Body::empty())
                .unwrap(),
            Request::builder()
                .method(Method::POST)
                .uri("/api/sandboxes/drain")
                .body(Body::empty())
                .unwrap(),
            Request::builder()
                .method(Method::GET)
                .uri("/api/workflows/schedules")
                .body(Body::empty())
                .unwrap(),
            Request::builder()
                .method(Method::GET)
                .uri("/api/workflows/runs")
                .body(Body::empty())
                .unwrap(),
            Request::builder()
                .method(Method::POST)
                .uri("/api/workflows/runs")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"workflow_name":"agent_turn","input":{}}"#))
                .unwrap(),
            Request::builder()
                .method(Method::GET)
                .uri("/api/workflows/runs/run-1")
                .body(Body::empty())
                .unwrap(),
            Request::builder()
                .method(Method::POST)
                .uri("/api/workflows/runs/run-1/cancel")
                .body(Body::empty())
                .unwrap(),
            Request::builder()
                .method(Method::POST)
                .uri("/api/workflows/events")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"event_name":"test.event","payload":{}}"#))
                .unwrap(),
            Request::builder()
                .method(Method::POST)
                .uri("/api/webhooks/test")
                .body(Body::empty())
                .unwrap(),
        ] {
            let app = build_router_with_app_state(AppState::unready());
            let response = app.oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        }
    }

    #[tokio::test]
    async fn mcp_requires_bearer_before_runtime_is_ready() {
        let app = build_router_with_app_state(AppState::unready());

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/mcp")
                    .header(header::HOST, "centaur.local")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let challenge = response
            .headers()
            .get(header::WWW_AUTHENTICATE)
            .and_then(|value| value.to_str().ok())
            .unwrap();
        assert!(challenge.contains("Bearer"));
        assert!(challenge.contains(
            "resource_metadata=\"http://centaur.local/.well-known/oauth-protected-resource/mcp\""
        ));
    }

    #[tokio::test]
    async fn append_messages_does_not_apply_a_session_body_limit() {
        let pool =
            PgPool::connect_lazy("postgres://postgres:postgres@localhost/centaur_test").unwrap();
        let app = build_router_with_runtime(
            PgSessionStore::new(pool),
            SandboxRuntime::backend(Arc::new(TestBackend::default()), SandboxSpec::new("test")),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/session/slack%3AC123%3A123.456/messages")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::CONTENT_LENGTH, (256 * 1024 * 1024 + 1).to_string())
                    .body(Body::from(r#"{"messages":"not-an-array"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_ne!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn execute_does_not_apply_a_session_body_limit() {
        let pool =
            PgPool::connect_lazy("postgres://postgres:postgres@localhost/centaur_test").unwrap();
        let app = build_router_with_runtime(
            PgSessionStore::new(pool),
            SandboxRuntime::backend(Arc::new(TestBackend::default()), SandboxSpec::new("test")),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/session/slack%3AC123%3A123.456/execute")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::CONTENT_LENGTH, (256 * 1024 * 1024 + 1).to_string())
                    .body(Body::from(r#"{"input_lines":"not-an-array"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_ne!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn session_context_exposes_slack_channel_and_thread_ts() {
        let pool =
            PgPool::connect_lazy("postgres://postgres:postgres@localhost/centaur_test").unwrap();
        let app = build_router_with_runtime(
            PgSessionStore::new(pool),
            SandboxRuntime::backend(Arc::new(TestBackend::default()), SandboxSpec::new("test")),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/session/slack%3AC123%3A123.456")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["thread_key"], "slack:C123:123.456");
        assert_eq!(body["platform"], "slack");
        assert_eq!(body["slack"]["channel_id"], "C123");
        assert_eq!(body["slack"]["thread_ts"], "123.456");
        assert!(body.get("discord").is_none());
        assert!(body.get("linear").is_none());
    }

    #[tokio::test]
    async fn session_context_exposes_discord_guild_channel_and_thread() {
        let pool =
            PgPool::connect_lazy("postgres://postgres:postgres@localhost/centaur_test").unwrap();
        let app = build_router_with_runtime(
            PgSessionStore::new(pool),
            SandboxRuntime::backend(Arc::new(TestBackend::default()), SandboxSpec::new("test")),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/session/discord%3A111%3A222%3A333")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["thread_key"], "discord:111:222:333");
        assert_eq!(body["platform"], "discord");
        assert_eq!(body["discord"]["guild_id"], "111");
        assert_eq!(body["discord"]["channel_id"], "222");
        assert_eq!(body["discord"]["thread_id"], "333");
        assert!(body.get("slack").is_none());
        assert!(body.get("linear").is_none());
    }

    #[tokio::test]
    async fn session_context_exposes_linear_issue_comment_and_session() {
        let pool =
            PgPool::connect_lazy("postgres://postgres:postgres@localhost/centaur_test").unwrap();
        let app = build_router_with_runtime(
            PgSessionStore::new(pool),
            SandboxRuntime::backend(Arc::new(TestBackend::default()), SandboxSpec::new("test")),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/session/linear%3AISSUE%3Ac%3ACMT%3As%3ASESS")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["thread_key"], "linear:ISSUE:c:CMT:s:SESS");
        assert_eq!(body["platform"], "linear");
        assert_eq!(body["linear"]["issue_id"], "ISSUE");
        assert_eq!(body["linear"]["comment_id"], "CMT");
        assert_eq!(body["linear"]["agent_session_id"], "SESS");
        assert!(body.get("slack").is_none());
        assert!(body.get("discord").is_none());
    }

    #[tokio::test]
    async fn session_context_exposes_github_repo_number_and_review_comment() {
        let pool =
            PgPool::connect_lazy("postgres://postgres:postgres@localhost/centaur_test").unwrap();
        let app = build_router_with_runtime(
            PgSessionStore::new(pool),
            SandboxRuntime::backend(Arc::new(TestBackend::default()), SandboxSpec::new("test")),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/session/github%3A0xSplits%2Fcentaur%3A704%3Arc%3A99")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["thread_key"], "github:0xSplits/centaur:704:rc:99");
        assert_eq!(body["platform"], "github");
        assert_eq!(body["github"]["owner"], "0xSplits");
        assert_eq!(body["github"]["repo"], "centaur");
        assert_eq!(body["github"]["number"], 704);
        assert_eq!(body["github"]["kind"], "pr");
        assert_eq!(body["github"]["review_comment_id"], 99);
        assert!(body.get("slack").is_none());
        assert!(body.get("discord").is_none());
        assert!(body.get("linear").is_none());
    }

    #[tokio::test]
    async fn session_context_omits_slack_for_non_slack_thread_key() {
        let pool =
            PgPool::connect_lazy("postgres://postgres:postgres@localhost/centaur_test").unwrap();
        let app = build_router_with_runtime(
            PgSessionStore::new(pool),
            SandboxRuntime::backend(Arc::new(TestBackend::default()), SandboxSpec::new("test")),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/session/cli%3Atest")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["thread_key"], "cli:test");
        assert_eq!(body["platform"], "unknown");
        assert!(body.get("slack").is_none());
        assert!(body.get("discord").is_none());
        assert!(body.get("linear").is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn anthropic_messages_streams_sse_from_scripted_stdout() {
        let _lock = DB_TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let app = anthropic_test_app(
            store,
            vec![
                json!({"type":"assistant","message":{"content":[{"type":"text","text":"PONG"}]}})
                    .to_string(),
                json!({"type":"result","result":"PONG"}).to_string(),
            ],
        );
        let thread_key = format!("api-test:{}", uuid::Uuid::new_v4());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/messages")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("X-Claude-Code-Session-Id", thread_key)
                    .body(Body::from(
                        json!({
                            "model": "claude-test",
                            "max_tokens": 16,
                            "stream": true,
                            "messages": [{"role": "user", "content": "ping"}]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        let events = sse_event_names(&body);
        assert_eq!(
            events,
            vec![
                "message_start",
                "ping",
                "content_block_start",
                "content_block_delta",
                "content_block_stop",
                "message_delta",
                "message_stop",
            ]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn anthropic_messages_returns_non_streaming_message() {
        let _lock = DB_TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let app = anthropic_test_app(
            store,
            vec![
                json!({"type":"assistant","message":{"content":[{"type":"text","text":"PONG"}]}})
                    .to_string(),
                json!({"type":"result","result":"PONG"}).to_string(),
            ],
        );
        let thread_key = format!("api-test:{}", uuid::Uuid::new_v4());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/messages")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("X-Claude-Code-Session-Id", thread_key)
                    .body(Body::from(
                        json!({
                            "model": "claude-test",
                            "max_tokens": 16,
                            "messages": [{"role": "user", "content": "ping"}]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["type"], json!("message"));
        assert_eq!(value["role"], json!("assistant"));
        assert_eq!(value["model"], json!("claude-test"));
        assert_eq!(value["content"], json!([{"type": "text", "text": "PONG"}]));
        assert_eq!(value["stop_reason"], json!("end_turn"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn anthropic_messages_threads_model_and_system_into_sandbox_env() {
        let _lock = DB_TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let (app, backend) = anthropic_test_app_with_backend(
            store,
            vec![
                json!({"type":"assistant","message":{"content":[{"type":"text","text":"PONG"}]}})
                    .to_string(),
                json!({"type":"result","result":"PONG"}).to_string(),
            ],
        );
        let thread_key = format!("api-test:{}", uuid::Uuid::new_v4());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/messages")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("X-Claude-Code-Session-Id", thread_key)
                    .body(Body::from(
                        json!({
                            "model": "claude-opus-4-test",
                            "system": [
                                {"type": "text", "text": "caller layer one"},
                                {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "ignored"}},
                                {"type": "text", "text": "caller layer two"}
                            ],
                            "max_tokens": 16,
                            "messages": [{"role": "user", "content": "ping"}]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let specs = backend.created_specs().await;
        assert_eq!(specs.len(), 1);
        assert_eq!(
            env_value(&specs[0], "CLAUDE_MODEL"),
            Some("claude-opus-4-test")
        );
        assert_eq!(
            env_value(&specs[0], "CENTAUR_EXTRA_SYSTEM_PROMPT"),
            Some("caller layer one\ncaller layer two")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn anthropic_messages_generates_thread_key_when_header_absent() {
        let _lock = DB_TEST_LOCK.lock().await;
        let Some(store) = test_store().await else {
            return;
        };
        let app = anthropic_test_app(
            store,
            vec![
                json!({"type":"assistant","message":{"content":[{"type":"text","text":"PONG"}]}})
                    .to_string(),
                json!({"type":"result","result":"PONG"}).to_string(),
            ],
        );
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/messages")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({
                            "model": "claude-test",
                            "max_tokens": 16,
                            "messages": [{"role": "user", "content": "ping"}]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();
        assert!(
            value["id"]
                .as_str()
                .is_some_and(|id| id.starts_with("msg_"))
        );
    }

    #[derive(Default)]
    struct TestBackend {
        next_id: AtomicU64,
    }

    #[async_trait]
    impl SandboxBackend for TestBackend {
        fn name(&self) -> &'static str {
            "test"
        }

        async fn create(&self, _spec: SandboxSpec) -> SandboxResult<SandboxHandle> {
            let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
            Ok(SandboxHandle::new(
                SandboxId::new(format!("test-{id}")),
                self.name(),
            ))
        }

        async fn open_io(&self, _id: &SandboxId) -> SandboxResult<SandboxIo> {
            unreachable!("router construction should not open sandbox I/O")
        }

        async fn status(&self, _id: &SandboxId) -> SandboxResult<SandboxStatus> {
            Ok(SandboxStatus::Running)
        }

        async fn observe(&self, id: &SandboxId) -> SandboxResult<ObservedSandbox> {
            Ok(ObservedSandbox::new(
                id.clone(),
                self.name(),
                SandboxStatus::Running,
            ))
        }

        async fn list_observed(&self) -> SandboxResult<Vec<ObservedSandbox>> {
            Ok(Vec::new())
        }

        async fn stop(&self, _id: &SandboxId) -> SandboxResult<()> {
            Ok(())
        }

        async fn pause(&self, _id: &SandboxId) -> SandboxResult<()> {
            Err(SandboxError::Unsupported {
                backend: self.name(),
                operation: "pause",
            })
        }

        async fn resume(&self, _id: &SandboxId) -> SandboxResult<()> {
            Err(SandboxError::Unsupported {
                backend: self.name(),
                operation: "resume",
            })
        }
    }

    struct ScriptedStdoutBackend {
        next_id: AtomicU64,
        script: Mutex<Vec<String>>,
        created_specs: Mutex<Vec<SandboxSpec>>,
    }

    impl ScriptedStdoutBackend {
        fn new(script: Vec<String>) -> Self {
            Self {
                next_id: AtomicU64::new(0),
                script: Mutex::new(script),
                created_specs: Mutex::new(Vec::new()),
            }
        }

        async fn created_specs(&self) -> Vec<SandboxSpec> {
            self.created_specs.lock().await.clone()
        }
    }

    #[async_trait]
    impl SandboxBackend for ScriptedStdoutBackend {
        fn name(&self) -> &'static str {
            "scripted"
        }

        async fn create(&self, spec: SandboxSpec) -> SandboxResult<SandboxHandle> {
            self.created_specs.lock().await.push(spec);
            let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
            Ok(SandboxHandle::new(
                SandboxId::new(format!("scripted-{id}")),
                self.name(),
            ))
        }

        async fn open_io(&self, _id: &SandboxId) -> SandboxResult<SandboxIo> {
            let script = self.script.lock().await.clone();
            let (io, mut stdout, mut stdin) = mock_io();
            tokio::spawn(async move {
                let _ = tokio::io::copy(&mut stdin, &mut tokio::io::sink()).await;
            });
            tokio::spawn(async move {
                for line in script {
                    let _ = stdout.write_all(line.as_bytes()).await;
                    let _ = stdout.write_all(b"\n").await;
                }
            });
            Ok(io)
        }

        async fn status(&self, _id: &SandboxId) -> SandboxResult<SandboxStatus> {
            Ok(SandboxStatus::Running)
        }

        async fn observe(&self, id: &SandboxId) -> SandboxResult<ObservedSandbox> {
            Ok(ObservedSandbox::new(
                id.clone(),
                self.name(),
                SandboxStatus::Running,
            ))
        }

        async fn list_observed(&self) -> SandboxResult<Vec<ObservedSandbox>> {
            Ok(Vec::new())
        }

        async fn stop(&self, _id: &SandboxId) -> SandboxResult<()> {
            Ok(())
        }

        async fn pause(&self, _id: &SandboxId) -> SandboxResult<()> {
            Ok(())
        }

        async fn resume(&self, _id: &SandboxId) -> SandboxResult<()> {
            Ok(())
        }
    }

    fn mock_io() -> (SandboxIo, DuplexStream, DuplexStream) {
        let (stdin_near, stdin_far) = tokio::io::duplex(64 * 1024);
        let (stdout_near, stdout_far) = tokio::io::duplex(64 * 1024);
        let (stderr_near, _stderr_far) = tokio::io::duplex(1024);
        let io = SandboxIo::new(
            Box::pin(stdin_near),
            Box::pin(stdout_near),
            Box::pin(stderr_near),
        );
        (io, stdout_far, stdin_far)
    }

    async fn test_store() -> Option<PgSessionStore> {
        let Ok(url) = std::env::var("SESSION_RUNTIME_TEST_DATABASE_URL") else {
            eprintln!("skipping: SESSION_RUNTIME_TEST_DATABASE_URL not set");
            return None;
        };
        let store = PgSessionStore::connect(&url)
            .await
            .expect("connect test db");
        match store.run_migrations().await {
            Ok(()) => Some(store),
            Err(SessionStoreError::Sqlx(error)) => panic!("run migrations: {error}"),
            Err(error) => panic!("run migrations: {error}"),
        }
    }

    fn anthropic_test_app(store: PgSessionStore, script: Vec<String>) -> axum::Router {
        let (app, _) = anthropic_test_app_with_backend(store, script);
        app
    }

    fn anthropic_test_app_with_backend(
        store: PgSessionStore,
        script: Vec<String>,
    ) -> (axum::Router, Arc<ScriptedStdoutBackend>) {
        let backend = Arc::new(ScriptedStdoutBackend::new(script));
        let app = build_router_with_runtime(
            store,
            SandboxRuntime::backend(backend.clone(), SandboxSpec::new("scripted")),
        );
        (app, backend)
    }

    fn env_value<'a>(spec: &'a SandboxSpec, name: &str) -> Option<&'a str> {
        spec.env
            .iter()
            .find(|env| env.name == name)
            .map(|env| env.value.as_str())
    }

    fn sse_event_names(body: &str) -> Vec<&str> {
        body.lines()
            .filter_map(|line| line.strip_prefix("event:"))
            .map(str::trim)
            .filter(|event| !event.is_empty())
            .collect()
    }
}
