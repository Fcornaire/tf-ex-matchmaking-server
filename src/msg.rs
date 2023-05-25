use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
pub enum ServerMsg {
    RegisterDirectResponse {
        success: bool,
        code: String,
    },
    MatchWithDirectCodeResponse {
        success: bool,
        opponent_addr: String,
        opponent_name: String,
        draw: i32,
        seed: i32,
    },
    RegisterQuickPlayResponse {
        success: bool,
    },
    Matched {
        opponent_addr: String,
    },
    QuickPlayPossibleMatch {
        opponent_addr: String,
        opponent_name: String,
    },
    AcceptQuickPlayResponse {
        success: bool,
        opponent_addr: String,
        opponent_name: String,
        draw: i32,
        seed: i32,
    },
    DeniedQuickPlay {},
    TotalAvailablePlayersInQuickPlayQueue {
        total: i32,
    },
}

#[derive(Debug, Deserialize)]
pub enum ClientMsg {
    RegisterDirect { addr: String, name: String },
    MatchWithDirectCode { code: String },
    RegisterQuickPlay { addr: String, name: String },
    CancelQuickPlay { addr: String },
    AcceptQuickPlay { addr: String },
}
