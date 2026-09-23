pub const SECONDS_PER_DAY: i64 = 86_400;

pub static PURGE_JOB_INTERVAL: u64 = 10;
pub static KEEP_ALIVE_INTERVAL: u64 = 3;

pub const MAX_PLAYERS: usize = 4;

/// How long players get to pick an end match option before the undecided ones are dropped
pub static END_GAME_CHOICE_SECONDS: u64 = 30;

pub static PRIVATE_JOIN_COOLDOWN_SECS: u64 = 3;

pub static QUICK_PLAY_TICK_INTERVAL: u64 = 1;

/// Once a non full quickplay lobby is all ready, wait this long for extra players
pub static QUICK_PLAY_AUTO_START_GRACE_SECS: u64 = 5;

pub static SERIES_READY_SECONDS: u64 = 60;

/// Vanilla versus towers eligible in a series
pub const SERIES_TOWER_COUNT: i32 = 16;

/// `TowerFall.Modes.TeamDeathmatch`
pub const TEAM_DEATHMATCH_MODE: i32 = 5;

pub const SERIES_TEAM_SIZE: usize = 2;
