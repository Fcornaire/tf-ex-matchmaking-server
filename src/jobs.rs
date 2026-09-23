use std::sync::Arc;

use tokio::time::{self, Duration};

use crate::{
    constants::{PURGE_JOB_INTERVAL, QUICK_PLAY_TICK_INTERVAL},
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

pub async fn spawn_jobs(state: Arc<AppState>) {
    tokio::select! {
        _ = (purge(state.clone())) => {
        },
        _ = (quick_play(state.clone())) => {
        }
    }
}
