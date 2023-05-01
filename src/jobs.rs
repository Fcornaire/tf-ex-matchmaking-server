use std::sync::Arc;

use futures_util::future::FutureExt;

use tokio::time::{self, Duration};

use crate::state::{AppState, Clients, ClientsExts};

async fn monitor(state: Arc<AppState>) {
    let mut interval = time::interval(Duration::from_secs(10));
    loop {
        interval.tick().await;
        tracing::info!("App: {:#?}", state);
    }
}

async fn make_match_for_quick_play(state: Arc<AppState>) {
    let mut interval = time::interval(Duration::from_secs(5));
    let clients: &mut Clients = &mut state.clients.clone();

    loop {
        interval.tick().await;
        clients.make_match_for_quick_play_async().await;
    }
}

pub async fn spawn_jobs(state: Arc<AppState>) {
    let (monitor, _handle) = FutureExt::remote_handle(monitor(state.clone()));
    let (make_match_for_quick_play, _handle) =
        FutureExt::remote_handle(make_match_for_quick_play(state.clone()));

    let mut monitor_task = tokio::spawn(monitor);
    let mut make_match_for_quick_play_task = tokio::spawn(make_match_for_quick_play);

    tokio::select! {
        _ = (&mut monitor_task) => {
        },
        _ = (&mut make_match_for_quick_play_task) => {
        },
    }
}
