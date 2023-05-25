/// This file contains all the constants used in the project.
/// The interval are in seconds.
pub static MONITOR_JOB_INTERVAL: u64 = 10;
pub static MAKE_MATCH_QP_JOB_INTERVAL: u64 = 3;
pub static UPDATE_MATCHED_HISTORY_JOB_INTERVAL: u64 = 5;
pub static MATCHED_HISTORY_EXPIRE_DURATION: u64 = 60;
pub static SEND_CLIENT_TASK_INTERVAL: u64 = 3;

/// the distances are in meters
pub static MIN_MATCH_DISTANCE: f64 = 500000.0;
pub static MAX_MATCH_DISTANCE: f64 = 20500000.0;
