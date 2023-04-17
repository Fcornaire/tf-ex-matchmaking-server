use std::sync::Arc;

use futures_util::future::FutureExt;

use tokio::{
    task::JoinError,
    time::{self, Duration},
};

use crate::state::AppState;

async fn monitor(state: Arc<AppState>) {
    let mut interval = time::interval(Duration::from_secs(2));
    loop {
        interval.tick().await;
        println!("App: {:?}", state);
    }
}

pub async fn spawn_jobs(state: Arc<AppState>) -> Result<(), JoinError> {
    let (monitor, _handle) = FutureExt::remote_handle(monitor(state.clone()));
    tokio::spawn(monitor).await
}
