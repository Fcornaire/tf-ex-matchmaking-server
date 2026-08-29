/// This file contains all the constants used in the project.
/// The interval are in seconds.
pub static MONITOR_JOB_INTERVAL: u64 = 3600; // 1 hour
pub static PURGE_EMPTY_LOBBIES_JOB_INTERVAL: u64 = 10;
pub static KEEP_ALIVE_INTERVAL: u64 = 3;

pub const MAX_PLAYERS: usize = 4;

/// How long players get to pick an end match option before the undecided ones are dropped
pub static END_GAME_CHOICE_SECONDS: u64 = 30;

/// Brute forcing join codes has to stay slower than code space
pub static PRIVATE_JOIN_COOLDOWN_SECS: u64 = 3;

pub static QUICK_PLAY_TICK_INTERVAL: u64 = 1;

/// Once a non full quickplay lobby is all ready, wait this long for extra players
pub static QUICK_PLAY_AUTO_START_GRACE_SECS: u64 = 5;
