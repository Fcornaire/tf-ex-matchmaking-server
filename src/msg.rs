use serde::{Deserialize, Serialize};

use crate::state::lobby::{Lobby, Player};

#[derive(Debug, Serialize)]
pub enum ServerMsg {
    KeepAlive {},
    GetLobbiesResponse {
        lobbies: Vec<Lobby>,
    },
    CreateLobbyResponse {
        success: bool,
        message: String,
        lobby: Option<Lobby>,
    },
    JoinLobbyResponse {
        success: bool,
        message: String,
        room_peer_id: String,
        room_chat_peer_id: String,
    },
    LobbyUpdate {
        lobby: Lobby,
    },
    LeaveLobbyResponse {
        success: bool,
        message: String,
    },
}

#[derive(Debug, Deserialize)]
pub enum ClientMsg {
    KeepAlive {},
    GetLobbies {},
    CreateLobby {
        lobby: Lobby,
    },
    JoinLobby {
        room_id: String,
        name: String,
        is_player: bool,
    },
    UpdatePlayer {
        player: Player,
    },
    LeaveLobby {},
}
