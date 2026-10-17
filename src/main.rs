pub mod args;
pub mod constants;
pub mod extensions;
pub mod jobs;
pub mod msg;
pub mod relay;
pub mod reports;
pub mod services;
pub mod shutdown;
pub mod state;
pub mod telemetry;
pub mod version;
pub mod ws_handler;

use axum::{
    extract::DefaultBodyLimit,
    middleware,
    routing::{get, post},
    Router,
};

use lazy_static::lazy_static;
use tracing::Level;

use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use tower_http::{
    services::ServeDir,
    trace::{DefaultMakeSpan, TraceLayer},
};

use tracing_subscriber::FmtSubscriber;

use crate::{
    constants::{
        DEFAULT_RELAY_PORT, DEFAULT_RELAY_PORT_CHECK_HOST, DEFAULT_REPORTS_MAX_MB, MAX_REPORT_BYTES,
    },
    relay::Relay,
    reports::report_handler,
    state::AppState,
    version::ExVersion,
    ws_handler::{
        matchmaking::matchmaking_handler,
        refuse_browsers,
        relay::{relay_ping_measurement_handler, relay_room_handler},
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


    let relay_ports = std::env::var("RELAY_PORT")
        .ok()
        .and_then(|s| relay::parse_ports(&s))
        .unwrap_or_else(|| vec![DEFAULT_RELAY_PORT]);

    let relay = match Relay::bind_first_free(&relay_ports).await {
        Ok(relay) => {
            tracing::info!("Relay listening on udp {}", relay.udp_port());
            Some(relay)
        }
        Err(err) => {
            tracing::error!(
                "Relay DISABLED, failed to bind udp {:?}: {}",
                relay_ports,
                err
            );
            None
        }
    };

    if let Some(relay) = &relay {
        if let Err(err) = resolve_relay_public_port(relay).await {
            tracing::error!("Relay public port unknown, exiting: {}", err);
            std::process::exit(1);
        }

        tracing::info!("Relay announces udp {}", relay.public_port());
    }

    let reports = std::env::var("REPORTS_DIR").ok().and_then(|dir| {
        let max_mb = std::env::var("REPORTS_MAX_MB")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(DEFAULT_REPORTS_MAX_MB);

        match reports::Reports::open(&dir, max_mb * 1024 * 1024) {
            Ok(reports) => {
                tracing::info!("Reports enabled, saved to {} (max {} MB)", dir, max_mb);
                Some(Arc::new(reports))
            }
            Err(err) => {
                tracing::error!("Reports DISABLED, cannot write to {}: {}", dir, err);
                None
            }
        }
    });

    let min_client_version = match std::env::var("MIN_CLIENT_VERSION") {
        Err(_) => {
            tracing::warn!("MIN_CLIENT_VERSION is not set");
            ExVersion::new(0, 0, 0)
        }
        Ok(value) => {
            let Some(version) = ExVersion::parse(&value) else {
                tracing::error!("Cannot start: MIN_CLIENT_VERSION must be a TF.EX version");
                std::process::exit(1);
            };

            version
        }
    };

    tracing::info!("Clients need TF.EX {} or newer", min_client_version);

    let assets_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("assets");
    let state = Arc::new(
        AppState::new_with_storage(storage)
            .with_relay(relay)
            .with_reports(reports)
            .with_min_client_version(min_client_version),
    );

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
        .route("/relay/room/:room_id", get(relay_room_handler))
        .route(
            "/relay/ping_measurement/:room_id",
            get(relay_ping_measurement_handler),
        )
        .route(
            "/report",
            post(report_handler).layer(DefaultBodyLimit::max(MAX_REPORT_BYTES)),
        )
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

    let drain_state = state.clone();

    let mut jobs_task = tokio::spawn(async move {
        tracing::info!("Jobs started");
        spawn_jobs(state.clone()).await;
    });

    tokio::select! {
        _ = (&mut server_task) => {jobs_task.abort()},
        _ = (&mut jobs_task) => {server_task.abort()},
        _ = shutdown::drain_on_signal(drain_state) => {
            server_task.abort();
            jobs_task.abort();
        },
    }
}

async fn resolve_relay_public_port(relay: &Relay) -> std::io::Result<()> {
    let Ok(value) = std::env::var("RELAY_PUBLIC_PORTS") else {
        return Ok(());
    };

    let ports = relay::parse_ports(&value).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid RELAY_PUBLIC_PORTS '{value}'"),
        )
    })?;

    if let [port] = ports[..] {
        relay.set_public_port(port);
        return Ok(());
    }

    let host = std::env::var("RELAY_PORT_CHECK_HOST")
        .unwrap_or_else(|_| DEFAULT_RELAY_PORT_CHECK_HOST.to_string());

    relay.discover_public_port(&host, &ports).await.map(|_| ())
}
