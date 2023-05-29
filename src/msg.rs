use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
pub enum ServerMsg {
    RegisterDirectResponse {
        success: bool,
        code: String,
    },
    MatchWithDirectCodeResponse {
        success: bool,
        opponent_name: String,
        seed: i32,
    },
    RegisterQuickPlayResponse {
        success: bool,
    },
    Matched {
        opponent_addr: String,
    },
    QuickPlayPossibleMatch {
        room_id: String,
        opponent_name: String,
    },
    AcceptQuickPlayResponse {
        success: bool,
        opponent_name: String,
        seed: i32,
    },
    DeniedQuickPlay {},
    TotalAvailablePlayersInQuickPlayQueue {
        total: i32,
    },
}

#[derive(Debug, Deserialize)]
pub enum ClientMsg {
    RegisterDirect { name: String },
    MatchWithDirectCode { code: String },
    RegisterQuickPlay { name: String },
    CancelQuickPlay {},
    AcceptQuickPlay {},
}
