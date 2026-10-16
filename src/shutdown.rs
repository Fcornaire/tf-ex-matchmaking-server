use std::{collections::HashSet, sync::Arc, time::Duration};

use tokio::time::{interval, Instant};
use tracing::{info, warn};

use crate::{
    constants::{
        DEFAULT_DRAIN_DEADLINE_SECS, DRAIN_REMINDER_SECS, DRAIN_STATUS_LOG_SECS, DRAIN_TICK_SECS,
    },
    state::{drain::match_ends_notice, AppState},
};

/// Waits for SIGTERM and drains
pub async fn drain_on_signal(state: Arc<AppState>) {
    shutdown_signal().await;

    let deadline_secs = std::env::var("DRAIN_DEADLINE_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(DEFAULT_DRAIN_DEADLINE_SECS);

    info!("Shutdown requested, draining for up to {}s", deadline_secs);

    state.start_drain().await;
    info!("Draining: {:?}", state.drain_status().await);

    state.notify_matches(match_ends_notice(deadline_secs)).await;

    let started = Instant::now();
    let mut reminders: Vec<u64> = DRAIN_REMINDER_SECS
        .iter()
        .copied()
        .filter(|&secs| secs < deadline_secs)
        .collect();

    let deadline = tokio::time::sleep(Duration::from_secs(deadline_secs));
    let second_signal = shutdown_signal();
    tokio::pin!(deadline, second_signal);

    let mut tick = interval(Duration::from_secs(DRAIN_TICK_SECS));
    let mut idle_last_tick = HashSet::new();
    let mut last_log = Instant::now();

    loop {
        tokio::select! {
            _ = tick.tick() => {
                idle_last_tick = state.drain_tick(&idle_last_tick).await;

                let status = state.drain_status().await;

                if status.is_drained() {
                    info!("Drained, exiting");
                    return;
                }

                let remaining = deadline_secs.saturating_sub(started.elapsed().as_secs());

                if let Some(&next) = reminders.first() {
                    if remaining <= next {
                        reminders.remove(0);
                        state.notify_matches(match_ends_notice(next)).await;
                    }
                }

                if last_log.elapsed() >= Duration::from_secs(DRAIN_STATUS_LOG_SECS) {
                    info!("Draining: {:?}", status);
                    last_log = Instant::now();
                }
            }
            _ = &mut deadline => {
                warn!("Drain deadline reached, exiting with {:?}", state.drain_status().await);
                return;
            }
            _ = &mut second_signal => {
                warn!("Second shutdown signal, exiting with {:?}", state.drain_status().await);
                return;
            }
        }
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("failed to listen for SIGTERM");

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
