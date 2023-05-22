use std::{char::MAX, sync::Arc};

use tokio::time::{self, Duration};

use crate::{
    constants::{
        MAKE_MATCH_QP_JOB_INTERVAL, MATCHED_HISTORY_EXPIRE_DURATION, MONITOR_JOB_INTERVAL,
        UPDATE_MATCHED_HISTORY_JOB_INTERVAL,
    },
    state::{AppState, Clients, ClientsExts},
};

async fn monitor(state: Arc<AppState>) {
    let mut interval = time::interval(Duration::from_secs(MONITOR_JOB_INTERVAL));
    loop {
        interval.tick().await;
        tracing::info!("[Job] Monitor: {:?}", state);
    }
}

async fn make_match_for_quick_play(state: Arc<AppState>) {
    let mut interval = time::interval(Duration::from_secs(MAKE_MATCH_QP_JOB_INTERVAL));
    let clients: &mut Clients = &mut state.clients.clone();

    loop {
        interval.tick().await;
        tracing::info!("[Job]: Making matchs for quick play");
        let matched = clients.make_match_for_quick_play_async().await;
        tracing::info!("[Job]: Made {} matchs for quick play", matched);
    }
}

async fn update_matched_history(state: Arc<AppState>) {
    let mut interval = time::interval(Duration::from_secs(UPDATE_MATCHED_HISTORY_JOB_INTERVAL));
    let clients: &mut Clients = &mut state.clients.clone();

    loop {
        interval.tick().await;

        tracing::info!("[Job]: Clearing match history");
        let removed = clients
            .update_matched_history_async(Duration::from_secs(MATCHED_HISTORY_EXPIRE_DURATION))
            .await;
        tracing::info!("[Job]: Cleared {} match history", removed);
    }
}

pub async fn spawn_jobs(state: Arc<AppState>) {
    tokio::select! {
        _ = (monitor(state.clone())) => {
        },
        _ = (make_match_for_quick_play(state.clone())) => {
        },
        _ = (update_matched_history(state.clone())) => {
        },
    }
}
