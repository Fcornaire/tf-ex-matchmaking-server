use serde::{Deserialize, Serialize};

use crate::state::lobby::{GameData, Lobby, Mod, Player};

#[derive(Debug, Serialize, Clone)]
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
    },
    LobbyUpdate {
        lobby: Lobby,
    },
    PingUpdate {
        pings: Vec<PlayerPing>,
    },
    LeaveLobbyResponse {
        success: bool,
        message: String,
    },
    LeaveLobbyForce {},
    RematchLobby {},
    StartLobby {},
    ArcherSelectLobby {},
    PrivateJoinResult {
        success: bool,
        message: String,
        room_peer_id: String,
        lobby: Option<Lobby>,
    },
    QuickPlayStatus {
        queued: bool,
        searching: u32,
        message: String,
    },
    QuickPlayMatchFound {
        lobby: Lobby,
        room_peer_id: String,
    },
    SpectatorJoined {
        room_peer_id: String,
    },
    SkinChunk {
        chunk: SkinChunk,
        from: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkinChunk {
    pub bundle_id: String,
    pub custom_archer_id: String,
    pub chunk_index: u32,
    pub chunk_count: u32,
    pub data: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct PlayerPing {
    pub addr: String,
    pub ping: u32,
}

#[derive(Debug, Deserialize)]
pub enum ClientMsg {
    KeepAlive {},
    Identify {
        player_id: String,
        name: String,
    },
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
    UpdateLobbySettings {
        max_players: usize,
        game_data: GameData,
        mods: Vec<Mod>,
    },
    LeaveLobby {},
    RematchLobbyChoice {},
    StartLobbyChoice {},
    ArcherSelectChoice {},
    MatchEnded {
        #[serde(default)]
        winner_seat: Option<i32>,
        #[serde(default)]
        frame: Option<i32>,
        #[serde(default)]
        checksum: Option<String>,
    },
    JoinPrivate {
        code: String,
        name: String,
        is_player: bool,
    },
    EnterQuickPlay {
        name: String,
        #[serde(default)]
        is_wide: bool,
    },
    ExitQuickPlay {},
    SkinChunk {
        chunk: SkinChunk,
    },
}
