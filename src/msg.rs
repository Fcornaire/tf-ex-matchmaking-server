use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
pub enum ServerMsg {
    RegisterResponse {
        success: bool,
        details: String,
    },
    Opponent {
        opponent_addr: String,
        local_port: u16,
        is_server: bool,
    },
}

#[derive(Debug, Deserialize)]
pub enum ClientMsg {
    RegisterDirect,
}
