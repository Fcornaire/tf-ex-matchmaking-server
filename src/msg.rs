use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
pub enum ServerMsg {
    RegisterResponse { success: bool, details: String },
}

#[derive(Debug, Deserialize)]
pub enum ClientMsg {
    RegisterDirect,
}
