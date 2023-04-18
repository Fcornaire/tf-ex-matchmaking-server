use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
pub enum ServerMsg {
    RegisterResponse {
        success: bool,
        details: String,
    },
    Opponent {
        opponent_addr: String,
        local: String,
        is_server: bool,
    },
}

#[derive(Debug, Deserialize)]
pub enum ClientMsg {
    RegisterDirect { addr: String },
}
