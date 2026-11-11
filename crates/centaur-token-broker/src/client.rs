//! Token endpoint requests and their outcome classification.
//!
//! SECURITY: nothing here logs or returns form values, tokens, or response
//! bodies. Errors carry only a stage, an OAuth error code, and an HTTP status.

use std::time::Duration;

use reqwest::{
    header::{ACCEPT, HeaderMap},
    multipart, redirect,
};
use serde_json::Value;

use crate::telemetry;

const MAX_BODY_BYTES: usize = 64 * 1024;
/// Caps a credential's `refresh_timeout_seconds`, so a refresh, which makes at
/// most two requests, fits in the shutdown grace period (see the chart's
/// `tokenBroker.terminationGracePeriodSeconds`).
pub(crate) const MAX_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum TokenProtocol {
    OAuth,
    Preqin,
    GitHubApp,
}

pub(crate) struct TokenRequest<'a> {
    /// The `provider` metrics label.
    pub provider: &'a str,
    pub url: &'a str,
    pub form: Vec<(&'static str, String)>,
    pub protocol: TokenProtocol,
    pub headers: HeaderMap,
    pub timeout: Duration,
}

#[derive(Debug, PartialEq)]
pub(crate) struct TokenResponse {
    pub access_token: String,
    /// `None` when the endpoint did not rotate the refresh token.
    pub refresh_token: Option<String>,
    /// `None` when the endpoint omitted `expires_in`.
    pub expires_in: Option<i64>,
}

/// A failed token request. Retryable failures are transient (network, 5xx,
/// 429, gateway-shaped 4xx, malformed 2xx, configuration); the rest mean the
/// credential is dead until a person re-authorizes it.
#[derive(Debug, PartialEq)]
pub(crate) struct RefreshError {
    pub stage: &'static str,
    /// The OAuth error code, or for network failures, what failed.
    pub code: Option<String>,
    pub status: Option<u16>,
    pub retryable: bool,
}

impl RefreshError {
    fn retryable(stage: &'static str, status: Option<u16>) -> Self {
        Self {
            stage,
            code: None,
            status,
            retryable: true,
        }
    }

    /// A request that failed before a complete response arrived.
    fn network(error: &reqwest::Error, status: Option<u16>) -> Self {
        let failure = if error.is_timeout() {
            "timeout"
        } else if error.is_connect() {
            "connect"
        } else if error.is_body() || error.is_decode() {
            "body"
        } else {
            "request"
        };
        Self {
            code: Some(failure.to_owned()),
            ..Self::retryable("network", status)
        }
    }

    fn dead(stage: &'static str, code: String, status: u16) -> Self {
        Self {
            stage,
            code: Some(code),
            status: Some(status),
            retryable: false,
        }
    }

    /// A credential or OAuth app that cannot build a request, such as a missing
    /// client secret. Retried with backoff so fixing the app revives it.
    pub fn config(code: &str) -> Self {
        Self {
            stage: "config",
            code: Some(code.to_owned()),
            status: None,
            retryable: true,
        }
    }

    /// The label recorded as `dead_reason`: the OAuth error code, else the stage.
    pub fn reason(&self) -> &str {
        self.code.as_deref().unwrap_or(self.stage)
    }
}

#[derive(Clone)]
pub(crate) struct TokenClient {
    http: reqwest::Client,
}

impl TokenClient {
    pub fn new() -> Result<Self, reqwest::Error> {
        Ok(Self {
            http: reqwest::Client::builder()
                .redirect(redirect::Policy::none())
                .user_agent("centaur-token-broker")
                .build()?,
        })
    }

    pub async fn post(&self, request: TokenRequest<'_>) -> Result<TokenResponse, RefreshError> {
        let provider = request.provider;
        let result = self.send(request).await;
        telemetry::record_token_request(provider, request_result(&result));
        result
    }

    async fn send(&self, request: TokenRequest<'_>) -> Result<TokenResponse, RefreshError> {
        let builder = self
            .http
            .post(request.url)
            .timeout(request.timeout)
            .header(ACCEPT, "application/json");
        let builder = if request.protocol == TokenProtocol::Preqin {
            let form = request
                .form
                .into_iter()
                .fold(multipart::Form::new(), |form, (name, value)| {
                    form.text(name, value)
                });
            builder.multipart(form)
        } else if request.protocol == TokenProtocol::OAuth {
            builder.form(&request.form)
        } else {
            builder
        };
        // Credential headers go last so they override the defaults.
        let mut response = builder
            .headers(request.headers)
            .send()
            .await
            .map_err(|error| RefreshError::network(&error, None))?;
        let status = response.status().as_u16();
        let mut body = Vec::new();
        while body.len() < MAX_BODY_BYTES {
            match response.chunk().await {
                Ok(Some(chunk)) => body.extend_from_slice(&chunk),
                Ok(None) => break,
                Err(error) => return Err(RefreshError::network(&error, Some(status))),
            }
        }
        body.truncate(MAX_BODY_BYTES);
        if request.protocol == TokenProtocol::GitHubApp {
            interpret_github_app(status, &body, chrono::Utc::now().timestamp())
        } else {
            interpret(status, &body, request.protocol == TokenProtocol::Preqin)
        }
    }
}

/// The `result` metrics label for one token request.
fn request_result(result: &Result<TokenResponse, RefreshError>) -> &'static str {
    let Err(error) = result else {
        return "success";
    };
    match (error.stage, error.status) {
        ("network", _) => match error.code.as_deref() {
            Some("timeout") => "timeout",
            Some("connect") => "connection_failed",
            _ => "network_error",
        },
        // Any response that marks the credential dead, including Preqin's
        // bare 4xx.
        _ if !error.retryable => "rejected",
        ("parse", _) => "malformed",
        (_, Some(429)) => "rate_limited",
        (_, Some(status)) if status >= 500 => "server_error",
        (_, Some(300..=399)) => "redirect",
        _ => "client_error",
    }
}

/// Classifies a token endpoint response.
fn interpret(status: u16, body: &[u8], strict_4xx: bool) -> Result<TokenResponse, RefreshError> {
    let parsed: Option<Value> = serde_json::from_slice(body).ok();
    let error_code = parsed
        .as_ref()
        .and_then(|body| body.get("error"))
        .and_then(Value::as_str)
        .filter(|code| !code.trim().is_empty())
        .map(str::to_owned);

    if !(200..300).contains(&status) {
        if status / 100 == 5 || status == 429 {
            return Err(RefreshError {
                code: error_code,
                ..RefreshError::retryable("http", Some(status))
            });
        }
        return match error_code {
            Some(code) => Err(RefreshError::dead("oauth", code, status)),
            None if strict_4xx => Err(RefreshError::dead("http", format!("http_{status}"), status)),
            // Most likely a gateway or rate limiter rather than the IdP.
            None => Err(RefreshError::retryable("http", Some(status))),
        };
    }

    // A gateway can corrupt a 2xx body without the credential being invalid.
    let malformed = || RefreshError::retryable("parse", Some(status));
    let body = parsed
        .as_ref()
        .and_then(Value::as_object)
        .ok_or_else(malformed)?;
    // Slack reports token errors as `{"ok": false, "error": ...}` with a 200.
    if body.get("ok") == Some(&Value::Bool(false))
        && let Some(code) = error_code
    {
        return Err(RefreshError::dead("oauth", code, status));
    }
    let access_token = present_string(body.get("access_token")).ok_or_else(malformed)?;
    let expires_in = match body.get("expires_in") {
        None | Some(Value::Null) => None,
        Some(Value::Number(number)) => Some(
            number
                .as_i64()
                .or_else(|| number.as_f64().map(|seconds| seconds as i64))
                .ok_or_else(malformed)?,
        ),
        Some(Value::String(seconds)) => Some(seconds.trim().parse().map_err(|_| malformed())?),
        Some(_) => return Err(malformed()),
    };
    Ok(TokenResponse {
        access_token,
        refresh_token: present_string(body.get("refresh_token")),
        expires_in,
    })
}

/// GitHub installation tokens use `token` and an absolute RFC 3339 expiry.
/// Never include GitHub's response message (or body) in an error.
fn interpret_github_app(status: u16, body: &[u8], now: i64) -> Result<TokenResponse, RefreshError> {
    if !(200..300).contains(&status) {
        return Err(RefreshError {
            stage: "http",
            code: Some(format!("http_{status}")),
            status: Some(status),
            retryable: status >= 500 || status == 429 || (300..400).contains(&status),
        });
    }
    let malformed = || RefreshError::retryable("parse", Some(status));
    let body: Value = serde_json::from_slice(body).map_err(|_| malformed())?;
    let access_token = present_string(body.get("token")).ok_or_else(malformed)?;
    let expires_at = body
        .get("expires_at")
        .and_then(Value::as_str)
        .ok_or_else(malformed)?;
    let expires_at = chrono::DateTime::parse_from_rfc3339(expires_at).map_err(|_| malformed())?;
    let expires_in = expires_at
        .timestamp()
        .checked_sub(now)
        .filter(|ttl| *ttl > 0)
        .ok_or_else(malformed)?;
    Ok(TokenResponse {
        access_token,
        refresh_token: None,
        expires_in: Some(expires_in),
    })
}

fn present_string(value: Option<&Value>) -> Option<String> {
    value
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::{RefreshError, TokenResponse, interpret};

    fn retryable(stage: &'static str, status: u16) -> RefreshError {
        RefreshError {
            stage,
            code: None,
            status: Some(status),
            retryable: true,
        }
    }

    fn dead(stage: &'static str, code: &str, status: u16) -> RefreshError {
        RefreshError {
            stage,
            code: Some(code.to_owned()),
            status: Some(status),
            retryable: false,
        }
    }

    #[test]
    fn github_app_responses_require_a_token_and_future_absolute_expiry() {
        use super::interpret_github_app;
        let now = 1_700_000_000;
        let expiry = chrono::DateTime::from_timestamp(now + 3600, 0)
            .unwrap()
            .to_rfc3339();
        let body = serde_json::json!({"token": "installation-token", "expires_at": expiry});
        let token = interpret_github_app(201, &serde_json::to_vec(&body).unwrap(), now).unwrap();
        assert_eq!(token.access_token, "installation-token");
        assert_eq!(token.expires_in, Some(3600));
        assert_eq!(token.refresh_token, None);
        for body in [
            serde_json::json!({"token": "installation-token"}),
            serde_json::json!({"token": "", "expires_at": expiry}),
            serde_json::json!({"token": "installation-token", "expires_at": "invalid"}),
            serde_json::json!({"token": "installation-token", "expires_at": "2000-01-01T00:00:00Z"}),
        ] {
            let error =
                interpret_github_app(201, &serde_json::to_vec(&body).unwrap(), now).unwrap_err();
            assert_eq!(error.stage, "parse");
            assert!(error.retryable);
        }
        for (status, retryable) in [
            (401, false),
            (403, false),
            (404, false),
            (422, false),
            (429, true),
            (500, true),
            (302, true),
        ] {
            let error = interpret_github_app(status, br#"{"message":"secret response body"}"#, now)
                .unwrap_err();
            assert_eq!(error.retryable, retryable);
            assert_eq!(error.reason(), format!("http_{status}"));
            assert!(!format!("{error:?}").contains("secret response body"));
        }
    }

    #[test]
    fn parses_successful_responses() {
        assert_eq!(
            interpret(
                200,
                br#"{"access_token":"AT","refresh_token":"RT","expires_in":3600}"#,
                false
            ),
            Ok(TokenResponse {
                access_token: "AT".into(),
                refresh_token: Some("RT".into()),
                expires_in: Some(3600),
            })
        );
        // No rotation, no expiry, and string or fractional expiries.
        assert_eq!(
            interpret(200, br#"{"access_token":"AT","refresh_token":""}"#, false),
            Ok(TokenResponse {
                access_token: "AT".into(),
                refresh_token: None,
                expires_in: None,
            })
        );
        for body in [
            br#"{"access_token":"AT","expires_in":"60"}"#.as_slice(),
            br#"{"access_token":"AT","expires_in":60.9}"#,
        ] {
            assert_eq!(interpret(200, body, false).unwrap().expires_in, Some(60));
        }
    }

    #[test]
    fn rejected_credentials_are_dead() {
        assert_eq!(
            interpret(400, br#"{"error":"invalid_grant"}"#, false),
            Err(dead("oauth", "invalid_grant", 400))
        );
        assert_eq!(
            interpret(
                200,
                br#"{"ok":false,"error":"invalid_refresh_token"}"#,
                false
            ),
            Err(dead("oauth", "invalid_refresh_token", 200))
        );
        assert_eq!(
            interpret(400, b"", true),
            Err(dead("http", "http_400", 400))
        );
        assert_eq!(
            dead("oauth", "invalid_grant", 400).reason(),
            "invalid_grant"
        );
    }

    #[test]
    fn transient_failures_are_retryable() {
        assert_eq!(
            interpret(503, b"upstream down", false),
            Err(retryable("http", 503))
        );
        assert_eq!(interpret(429, b"", true), Err(retryable("http", 429)));
        assert_eq!(interpret(400, b"", false), Err(retryable("http", 400)));
        assert_eq!(interpret(302, b"", false), Err(retryable("http", 302)));
        assert_eq!(
            interpret(503, br#"{"error":"temporarily_unavailable"}"#, false),
            Err(RefreshError {
                code: Some("temporarily_unavailable".into()),
                ..retryable("http", 503)
            })
        );
        for body in [
            b"not json{".as_slice(),
            br#"[]"#,
            br#"{"access_token":""}"#,
            br#"{"access_token":"AT","expires_in":"soon"}"#,
        ] {
            assert_eq!(interpret(200, body, false), Err(retryable("parse", 200)));
        }
    }
}
