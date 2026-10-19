use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::sync::{Mutex, MutexGuard};

/// Timers per room
#[derive(Debug, Clone, Default)]
pub struct Countdown {
    running: Arc<Mutex<HashMap<String, u64>>>,
    next_generation: Arc<AtomicU64>,
}

impl Countdown {
    pub async fn start(&self, room_id: &str) -> u64 {
        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
        self.running
            .lock()
            .await
            .insert(room_id.to_string(), generation);
        generation
    }

    pub async fn start_if_idle(&self, room_id: &str) -> Option<u64> {
        let mut running = self.running.lock().await;

        if running.contains_key(room_id) {
            return None;
        }

        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
        running.insert(room_id.to_string(), generation);
        Some(generation)
    }

    pub async fn wait(&self, room_id: &str, generation: u64, secs: u64) -> bool {
        tokio::time::sleep(Duration::from_secs(secs)).await;
        self.running.lock().await.get(room_id) == Some(&generation)
    }

    pub async fn cancel(&self, room_id: &str) -> bool {
        self.running.lock().await.remove(room_id).is_some()
    }

    pub async fn lock(&self) -> MutexGuard<'_, HashMap<String, u64>> {
        self.running.lock().await
    }
}

#[derive(Debug, Clone, Default)]
pub struct Countdowns {
    pub end_game: Countdown,
    pub series: Countdown,
}
