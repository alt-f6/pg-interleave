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

use crate::barrier::{InterceptRule, SharedState};

#[derive(Serialize)]
struct StatusResponse {
    status: &'static str,
}

#[derive(Serialize)]
struct ResumeResponse {
    status: &'static str,
    barrier_id: Option<String>,
}

#[derive(Deserialize)]
struct InterceptRequest {
    target_pid: Option<u32>,
    query_pattern: String,
    action: Option<crate::barrier::BarrierAction>,
}

pub async fn run_control_server(addr: SocketAddr, state: SharedState) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/reset", post(reset_handler))
        .route("/intercept", post(intercept_handler))
        .route(
            "/barriers",
            get(list_barriers_handler).post(list_barriers_handler),
        )
        .route("/resume/:barrier_id", post(resume_handler))
        .route("/resume-next", post(resume_next_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("HTTP Control API running at http://{}", addr);
    axum::serve(listener, app).await?;
    Ok(())
}

async fn reset_handler(State(state): State<SharedState>) -> impl IntoResponse {
    state.reset().await;
    Json(StatusResponse { status: "ok" })
}

async fn intercept_handler(
    State(state): State<SharedState>,
    Json(payload): Json<InterceptRequest>,
) -> impl IntoResponse {
    let action = payload
        .action
        .unwrap_or(crate::barrier::BarrierAction::PauseBeforeQuery);
    state
        .add_rule(InterceptRule {
            target_pid: payload.target_pid,
            query_pattern: payload.query_pattern,
            action,
        })
        .await;

    Json(StatusResponse {
        status: "rule_added",
    })
}

async fn list_barriers_handler(State(state): State<SharedState>) -> impl IntoResponse {
    let st = state.0.lock().await;
    let list: Vec<String> = st.active_barriers.keys().cloned().collect();
    Json(list)
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
