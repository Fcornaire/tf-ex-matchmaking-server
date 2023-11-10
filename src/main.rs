pub mod args;
pub mod constants;
pub mod extensions;
pub mod jobs;
pub mod msg;
pub mod services;
pub mod state;
pub mod ws_handler;

use axum::{routing::get, Router};

use lazy_static::lazy_static;
use tracing::Level;

use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use tower_http::{
    services::ServeDir,
    trace::{DefaultMakeSpan, TraceLayer},
};

use tracing_subscriber::FmtSubscriber;

use crate::{
    state::AppState,
    ws_handler::{matchmaking::matchmaking_handler, signaling::signaling_handler},
};

use crate::jobs::spawn_jobs;

use args::Args;

lazy_static! {
    pub static ref ARGS: Arc<Args> = Arc::new(Args::new(
        std::env::var("GEOIP_CLIENT_ID").unwrap(),
        std::env::var("GEOIP_API_KEY").unwrap()
    ));
}

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .pretty()
        // .with_file(true)
        // .with_line_number(true)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let assets_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("assets");
    let state = Arc::new(AppState::new());

    // build our application with some routes
    let app = Router::new()
        .fallback_service(ServeDir::new(assets_dir).append_index_html_on_directories(true))
        .route("/ws", get(matchmaking_handler))
        .route("/room/:room_id", get(signaling_handler))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::default().include_headers(true)),
        )
        .with_state(state.clone());

    let mut server_task = tokio::spawn(async move {
        let port = std::env::var("PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(3000);

        let addr = SocketAddr::from(([0, 0, 0, 0], port));

        tracing::info!("Server matchmaking started and listening on {}", addr);

        axum::Server::bind(&addr)
            .serve(app.into_make_service_with_connect_info::<SocketAddr>())
            .await
            .unwrap();
    });

    let mut jobs_task = tokio::spawn(async move {
        tracing::info!("Jobs started");
        spawn_jobs(state.clone()).await;
    });

    tokio::select! {
        _ = (&mut server_task) => {jobs_task.abort()},
        _ = (&mut jobs_task) => {server_task.abort()},
    }
}
