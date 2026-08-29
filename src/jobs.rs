use std::sync::Arc;

use tokio::time::{self, Duration};

use crate::{
    constants::{MONITOR_JOB_INTERVAL, PURGE_EMPTY_LOBBIES_JOB_INTERVAL, QUICK_PLAY_TICK_INTERVAL},
    state::AppState,
};

async fn monitor(state: Arc<AppState>) {
    let mut interval = time::interval(Duration::from_secs(MONITOR_JOB_INTERVAL));
    loop {
        interval.tick().await;
        tracing::info!("[Job] Monitor: {:?}", state);
    }
}

async fn purge_lobby(state: Arc<AppState>) {
    let mut interval = time::interval(Duration::from_secs(PURGE_EMPTY_LOBBIES_JOB_INTERVAL));
    loop {
        interval.tick().await;
        state.purge_lobby_async().await;
    }
}

async fn quick_play(state: Arc<AppState>) {
    let mut interval = time::interval(Duration::from_secs(QUICK_PLAY_TICK_INTERVAL));
    loop {
        interval.tick().await;
        state.quick_play_tick().await;
    }
}

pub async fn spawn_jobs(state: Arc<AppState>) {
    tokio::select! {
        _ = (monitor(state.clone())) => {
        },
        _ = (purge_lobby(state.clone())) => {
        },
        _ = (quick_play(state.clone())) => {
        }
    }
}
