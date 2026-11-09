use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tracing::info;

use crate::barrier::{InterceptRule, InterceptRuleInput, SharedState};

#[derive(Serialize)]
struct StatusResponse {
    status: &'static str,
}

#[derive(Serialize)]
struct ResumeResponse {
    status: &'static str,
    barrier_id: Option<String>,
}

#[derive(Serialize)]
struct EventsResponse {
    had_timeouts: bool,
}

#[derive(Deserialize)]
struct RawInterceptRequest {
    #[serde(alias = "clientPid", alias = "client_pid", alias = "targetPid")]
    target_pid: Option<u32>,
    #[serde(alias = "sessionTag", alias = "session_tag", alias = "session")]
    session_tag: Option<String>,
    #[serde(alias = "queryPattern", alias = "onQueryMatch", default)]
    query_pattern: Option<String>,
    #[serde(default)]
    ast: Option<crate::ast::AstSelector>,
    #[serde(alias = "withParams", alias = "with_params")]
    with_params: Option<Vec<serde_json::Value>>,
    action: Option<crate::barrier::BarrierAction>,
    times: Option<u32>,
}

pub async fn run_control_server(addr: SocketAddr, state: SharedState) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/reset", post(reset_handler))
        .route("/intercept", post(intercept_handler))
        .route(
            "/barriers",
            get(list_barriers_handler).post(list_barriers_handler),
        )
        .route("/events", get(events_handler))
        .route("/resume/:barrier_id", post(resume_handler))
        .route("/resume-next", post(resume_next_handler))
        .route("/flush-trace", post(flush_trace_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("HTTP Control API running at http://{}", addr);
    axum::serve(listener, app).await?;
    Ok(())
}

async fn flush_trace_handler(State(state): State<SharedState>) -> impl IntoResponse {
    match state.flush_trace().await {
        Ok(_) => (
            StatusCode::OK,
            Json(StatusResponse {
                status: "trace_flushed",
            }),
        ),
        Err(_err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(StatusResponse {
                status: "flush_failed",
            }),
        ),
    }
}

async fn reset_handler(State(state): State<SharedState>) -> impl IntoResponse {
    state.reset().await;
    (StatusCode::OK, Json(StatusResponse { status: "ok" }))
}

async fn intercept_handler(
    State(state): State<SharedState>,
    Json(payload): Json<RawInterceptRequest>,
) -> impl IntoResponse {
    let action = payload
        .action
        .unwrap_or(crate::barrier::BarrierAction::PauseBeforeQuery);

    let input = InterceptRuleInput {
        target_pid: payload.target_pid,
        session_tag: payload.session_tag,
        query_pattern: payload.query_pattern,
        ast: payload.ast,
        with_params: payload.with_params,
        action,
        times: payload.times.or(Some(1)),
    };

    match InterceptRule::try_from(input) {
        Ok(rule) => {
            state.add_rule(rule).await;
            (
                StatusCode::OK,
                Json(serde_json::json!({ "status": "rule_added" })),
            )
        }
        Err(err) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": err.to_string() })),
        ),
    }
}

async fn list_barriers_handler(State(state): State<SharedState>) -> impl IntoResponse {
    let list = state.active_barrier_ids().await;
    Json(list)
}

async fn events_handler(State(state): State<SharedState>) -> impl IntoResponse {
    let had_timeouts = state.has_timeouts().await;
    Json(EventsResponse { had_timeouts })
}

async fn resume_handler(
    State(state): State<SharedState>,
    Path(barrier_id): Path<String>,
) -> impl IntoResponse {
    if state.resume_barrier(&barrier_id).await {
        (StatusCode::OK, Json(StatusResponse { status: "resumed" }))
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(StatusResponse {
                status: "barrier_not_found",
            }),
        )
    }
}

async fn resume_next_handler(State(state): State<SharedState>) -> impl IntoResponse {
    if let Some(id) = state.resume_first().await {
        (
            StatusCode::OK,
            Json(ResumeResponse {
                status: "resumed",
                barrier_id: Some(id),
            }),
        )
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(ResumeResponse {
                status: "no_active_barriers",
                barrier_id: None,
            }),
        )
    }
}
