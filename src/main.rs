pub mod args;
pub mod constants;
pub mod extensions;
pub mod jobs;
pub mod msg;
pub mod services;
pub mod state;
pub mod telemetry;
pub mod ws_handler;

use axum::{middleware, routing::get, Router};

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
    ws_handler::{
        matchmaking::matchmaking_handler,
        refuse_browsers,
        signaling::{ping_measurement_handler, signaling_handler},
    },
};

use crate::jobs::spawn_jobs;

use args::Args;

lazy_static! {
    pub static ref ARGS: Arc<Args> = Arc::new(Args::new(
        std::env::var("GEOIP_CLIENT_ID").ok(),
        std::env::var("GEOIP_API_KEY").ok()
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

    if ARGS.geoip_credentials().is_some() {
        tracing::info!("GeoIP enabled");
    } else if ARGS.geoip_client_id.is_some() != ARGS.geoip_api_key.is_some() {
        tracing::warn!("Only one of GEOIP_CLIENT_ID / GEOIP_API_KEY is set; GeoIP disabled");
    } else {
        tracing::info!("GeoIP disabled (GEOIP_CLIENT_ID / GEOIP_API_KEY not set); quickplay will treat all players as equal distance");
    }

    let metrics_handle = telemetry::install();

    let db_path = std::env::var("DB_PATH").unwrap_or_else(|_| "tfex.db".to_string());
    let storage = match services::storage::Storage::open(&db_path) {
        Ok(storage) => {
            let known_players = storage.player_count().await.unwrap_or(0);
            tracing::info!(
                "Rating storage ready at {} ({} known players)",
                db_path,
                known_players
            );
            Some(Arc::new(storage))
        }
        Err(err) => {
            tracing::error!(
                "Rating storage DISABLED, failed to open {}: {}",
                db_path,
                err
            );
            None
        }
    };


    let assets_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("assets");
    let state = Arc::new(AppState::new_with_storage(storage));

    let metrics_port = std::env::var("METRICS_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4738);

    tokio::spawn(telemetry::serve(
        state.clone(),
        metrics_handle,
        metrics_port,
    ));

    // build our application with some routes
    let app = Router::new()
        .fallback_service(ServeDir::new(assets_dir).append_index_html_on_directories(true))
        .route("/ws", get(matchmaking_handler))
        .route("/room/:room_id", get(signaling_handler))
        .route("/ping_measurement/:room_id", get(ping_measurement_handler))
        .route_layer(middleware::from_fn(refuse_browsers))
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
