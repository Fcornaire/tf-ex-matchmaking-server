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
    },
}

#[derive(Debug, Deserialize)]
pub enum ClientMsg {
    RegisterDirect { addr: String },
    MatchWithDirectCode { code: String },
}
