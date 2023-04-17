use std::sync::Arc;

use futures_util::future::FutureExt;

use rand::Rng;
use tokio::time::{self, Duration};

use crate::state::AppState;

async fn monitor(state: Arc<AppState>) {
    let mut interval = time::interval(Duration::from_secs(30));
    loop {
        interval.tick().await;
        println!("App: {:?}", state);
    }
}

async fn direct_work_async(state: Arc<AppState>) {
    let mut interval = time::interval(Duration::from_secs(10));
    loop {
        interval.tick().await;
        let arc = state.clients.clone();
        let clients = &mut arc.lock().await;

        if clients.len() == 2 {
            let keys = clients.keys().cloned().collect::<Vec<_>>();
            // let values = clients.values().cloned().collect::<Vec<_>>();

            //TODO: implements

            let mut rng = rand::thread_rng();
            let is_server = rng.gen_range(0..2) == 0;

            let fst = clients.get_mut(&keys[0]).unwrap();
            fst.set_addr(keys[1]);
            fst.set_is_server(is_server);

            let snd = clients.get_mut(&keys[1]).unwrap();
            snd.set_addr(keys[0]);
            snd.set_is_server(!is_server);
        }
    }
}

pub async fn spawn_jobs(state: Arc<AppState>) {
    let (monitor, _handle) = FutureExt::remote_handle(monitor(state.clone()));
    let mut monitor_task = tokio::spawn(monitor);

    let (direct, _handle) = FutureExt::remote_handle(direct_work_async(state.clone()));
    let mut direct_task = tokio::spawn(direct);

    tokio::select! {
        _ = (&mut monitor_task) => {
        }
        _ = (&mut direct_task) => {
        }
    }
}
