use std::sync::Arc;

use tokio::time::{self, Duration};

use crate::{constants::MONITOR_JOB_INTERVAL, state::AppState};

async fn monitor(state: Arc<AppState>) {
    let mut interval = time::interval(Duration::from_secs(MONITOR_JOB_INTERVAL));
    loop {
        interval.tick().await;
        tracing::info!("[Job] Monitor: {:?}", state);
    }
}

pub async fn spawn_jobs(state: Arc<AppState>) {
    tokio::select! {
        _ = (monitor(state.clone())) => {
        },
    }
}
