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
        draw: i32,
        seed: i32,
    },
}

#[derive(Debug, Deserialize)]
pub enum ClientMsg {
    RegisterDirect { addr: String },
    MatchWithDirectCode { code: String },
}
