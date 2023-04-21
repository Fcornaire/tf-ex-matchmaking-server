use std::sync::Arc;

use futures_util::future::FutureExt;

use tokio::time::{self, Duration};

use crate::state::AppState;

async fn monitor(state: Arc<AppState>) {
    let mut interval = time::interval(Duration::from_secs(10));
    loop {
        interval.tick().await;
        tracing::info!("App: {:?}", state);
    }
}

pub async fn spawn_jobs(state: Arc<AppState>) {
    let (monitor, _handle) = FutureExt::remote_handle(monitor(state.clone()));
    let mut monitor_task = tokio::spawn(monitor);

    tokio::select! {
        _ = (&mut monitor_task) => {
        }
    }
}
