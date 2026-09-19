use std::{net::SocketAddr, sync::Arc, time::Duration};

use axum::{extract::State, routing::get, Router};
use metrics::{describe_counter, describe_gauge, describe_histogram};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use metrics_util::MetricKindMask;
use tracing::{error, info};

use crate::state::{lobby::LobbyKind, AppState};

pub const CLIENTS_CONNECTED: &str = "tfex_clients_connected";
pub const LOBBIES: &str = "tfex_lobbies";
pub const GAMES_ACTIVE: &str = "tfex_games_active";
pub const PLAYERS_SEATED: &str = "tfex_players_seated";
pub const SPECTATORS: &str = "tfex_spectators";
pub const QUICKPLAY_QUEUE_DEPTH: &str = "tfex_quickplay_queue_depth";
pub const SIGNALING_ROOMS: &str = "tfex_signaling_rooms";
pub const SIGNALING_PEERS: &str = "tfex_signaling_peers";

pub const CONNECTIONS_TOTAL: &str = "tfex_connections_total";
pub const DISCONNECTS_TOTAL: &str = "tfex_disconnects_total";
pub const LOBBIES_CREATED_TOTAL: &str = "tfex_lobbies_created_total";
pub const JOINS_TOTAL: &str = "tfex_joins_total";
pub const QUICKPLAY_ENTER_TOTAL: &str = "tfex_quickplay_enter_total";
pub const MATCHES_STARTED_TOTAL: &str = "tfex_matches_started_total";
pub const MATCHES_ENDED_TOTAL: &str = "tfex_matches_ended_total";
pub const END_GAME_OUTCOMES_TOTAL: &str = "tfex_end_game_outcomes_total";
pub const SERIES_OUTCOMES_TOTAL: &str = "tfex_series_outcomes_total";
pub const PRIVATE_JOIN_FAILURES_TOTAL: &str = "tfex_private_join_failures_total";
pub const GEOIP_FAILURES_TOTAL: &str = "tfex_geoip_failures_total";
pub const SKIN_CHUNKS_TOTAL: &str = "tfex_skin_chunks_total";
pub const RATING_REPORTS_TOTAL: &str = "tfex_rating_reports_total";
pub const RATING_RATED_TOTAL: &str = "tfex_rating_rated_matches_total";
pub const PLAYER_RATING: &str = "tfex_player_rating";

pub const MATCH_DURATION_SECONDS: &str = "tfex_match_duration_seconds";
pub const QUICKPLAY_WAIT_SECONDS: &str = "tfex_quickplay_wait_seconds";
pub const KEEPALIVE_RTT_SECONDS: &str = "tfex_keepalive_rtt_seconds";

pub fn kind_label(kind: LobbyKind) -> &'static str {
    match kind {
        LobbyKind::Standard => "standard",
        LobbyKind::QuickPlay => "quickplay",
        LobbyKind::Private => "private",
    }
}

/// Must run before anything can emit a metric
pub fn install() -> PrometheusHandle {
    let handle = PrometheusBuilder::new()
        .idle_timeout(MetricKindMask::GAUGE, Some(Duration::from_secs(300)))
        .set_buckets_for_metric(
            Matcher::Full(KEEPALIVE_RTT_SECONDS.to_string()),
            &[0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.3, 0.5, 1.0],
        )
        .expect("keepalive rtt buckets")
        .set_buckets_for_metric(
            Matcher::Full(QUICKPLAY_WAIT_SECONDS.to_string()),
            &[1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0],
        )
        .expect("quickplay wait buckets")
        .set_buckets_for_metric(
            Matcher::Full(MATCH_DURATION_SECONDS.to_string()),
            &[60.0, 180.0, 300.0, 600.0, 1200.0, 1800.0, 3600.0],
        )
        .expect("match duration buckets")
        .install_recorder()
        .expect("failed to install metrics recorder");

    describe();

    handle
}

fn describe() {
    describe_gauge!(
        CLIENTS_CONNECTED,
        "Clients connected to the matchmaking websocket"
    );
    describe_gauge!(LOBBIES, "Lobbies currently alive, by kind");
    describe_gauge!(GAMES_ACTIVE, "Lobbies with a match running, by kind");
    describe_gauge!(PLAYERS_SEATED, "Players seated in lobbies, by kind");
    describe_gauge!(SPECTATORS, "Spectators across all lobbies");
    describe_gauge!(
        QUICKPLAY_QUEUE_DEPTH,
        "Players waiting in the quickplay queue"
    );
    describe_gauge!(SIGNALING_ROOMS, "Open WebRTC signaling rooms");
    describe_gauge!(SIGNALING_PEERS, "Peers connected to the signaling endpoint");
    describe_gauge!(
        PLAYER_RATING,
        "Hidden rating of the current top players, by player_id and name"
    );

    describe_counter!(
        CONNECTIONS_TOTAL,
        "Matchmaking websocket connections accepted"
    );
    describe_counter!(
        DISCONNECTS_TOTAL,
        "Matchmaking websocket disconnections, by reason"
    );
    describe_counter!(LOBBIES_CREATED_TOTAL, "Lobbies created, by kind");
    describe_counter!(JOINS_TOTAL, "Members seated into lobbies, by role");
    describe_counter!(QUICKPLAY_ENTER_TOTAL, "Quickplay entries, by outcome");
    describe_counter!(
        MATCHES_STARTED_TOTAL,
        "Matches started, by kind and trigger"
    );
    describe_counter!(MATCHES_ENDED_TOTAL, "Matches that reported an end, by kind");
    describe_counter!(
        END_GAME_OUTCOMES_TOTAL,
        "End-of-match window resolutions, by outcome"
    );
    describe_counter!(
        SERIES_OUTCOMES_TOTAL,
        "Best-of series lifecycle events, by outcome"
    );
    describe_counter!(
        PRIVATE_JOIN_FAILURES_TOTAL,
        "Rejected private-code joins, by reason"
    );
    describe_counter!(
        GEOIP_FAILURES_TOTAL,
        "Connections dropped because the geoip lookup failed"
    );
    describe_counter!(SKIN_CHUNKS_TOTAL, "Custom skin chunks handled, by result");

    describe_histogram!(
        MATCH_DURATION_SECONDS,
        "Time between match start and reported end"
    );
    describe_histogram!(
        QUICKPLAY_WAIT_SECONDS,
        "Queue time before a quickplay seat was found"
    );
    describe_histogram!(KEEPALIVE_RTT_SECONDS, "Keep-alive round trip per client");
}

async fn metrics_handler(
    State((state, handle)): State<(Arc<AppState>, PrometheusHandle)>,
) -> String {
    state.publish_metrics_gauges().await;
    handle.run_upkeep();
    handle.render()
}

pub async fn serve(state: Arc<AppState>, handle: PrometheusHandle, port: u16) {
    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .with_state((state, handle));

    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    match axum::Server::try_bind(&addr) {
        Ok(server) => {
            info!("Metrics listening on {}", addr);

            if let Err(err) = server.serve(app.into_make_service()).await {
                error!("Metrics server stopped: {}", err);
            }
        }
        Err(err) => error!("Metrics server could not bind {}: {}", addr, err),
    }
}
