use std::sync::Arc;

use tokio::time::{self, Duration};

use crate::{
    constants::{
        CONNECTIVITY_TICK_INTERVAL, LEADERBOARD_REFRESH_SECS, PURGE_JOB_INTERVAL,
        QUICK_PLAY_TICK_INTERVAL, RANKED_TICK_INTERVAL, REPORTS_PURGE_INTERVAL,
        REPORTS_RETENTION_SECS,
    },
    state::AppState,
};

async fn purge(state: Arc<AppState>) {
    let mut interval = time::interval(Duration::from_secs(PURGE_JOB_INTERVAL));
    loop {
        interval.tick().await;
        state.purge_lobby_async().await;
        state.purge_stale_match_reports().await;
    }
}

async fn quick_play(state: Arc<AppState>) {
    let mut interval = time::interval(Duration::from_secs(QUICK_PLAY_TICK_INTERVAL));
    loop {
        interval.tick().await;
        state.quick_play_tick().await;
    }
}

async fn ranked(state: Arc<AppState>) {
    let mut interval = time::interval(Duration::from_secs(RANKED_TICK_INTERVAL));
    loop {
        interval.tick().await;
        state.ranked_tick().await;
    }
}

async fn connectivity(state: Arc<AppState>) {
    let mut interval = time::interval(Duration::from_secs(CONNECTIVITY_TICK_INTERVAL));
    loop {
        interval.tick().await;
        state.connectivity_tick().await;
    }
}

async fn leaderboard(state: Arc<AppState>) {
    let mut interval = time::interval(Duration::from_secs(LEADERBOARD_REFRESH_SECS));
    loop {
        interval.tick().await;
        state.refresh_leaderboard().await;
    }
}

async fn purge_reports(state: Arc<AppState>) {
    let mut interval = time::interval(Duration::from_secs(REPORTS_PURGE_INTERVAL));
    loop {
        interval.tick().await;

        if let Some(reports) = state.reports() {
            let max_age = Duration::from_secs(REPORTS_RETENTION_SECS);

            match tokio::task::spawn_blocking(move || reports.purge_older_than(max_age)).await {
                Ok(Ok(removed)) if removed > 0 => {
                    tracing::info!("Removed {} expired report(s)", removed)
                }
                Ok(Err(err)) => tracing::warn!("Report purge failed: {}", err),
                _ => {}
            }
        }
    }
}

pub async fn spawn_jobs(state: Arc<AppState>) {
    tokio::select! {
        _ = (purge(state.clone())) => {
        },
        _ = (quick_play(state.clone())) => {
        },
        _ = (ranked(state.clone())) => {
        },
        _ = (connectivity(state.clone())) => {
        },
        _ = (leaderboard(state.clone())) => {
        },
        _ = (purge_reports(state.clone())) => {
        }
    }
}
