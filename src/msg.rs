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
        lobby: Option<Lobby>,
    },
    LobbyUpdate {
        lobby: Lobby,
    },
    LeaveLobbyResponse {
        success: bool,
        message: String,
    },
    RematchLobby {},
    StartLobby {},
    ArcherSelectLobby {},
    SeriesLobby {},
    PrivateJoinResult {
        success: bool,
        message: String,
        room_peer_id: String,
        lobby: Option<Lobby>,
    },
    QuickPlayStatus {
        queued: bool,
        searching_standard: u32,
        searching_wide: u32,
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
    KickedFromLobby {
        reason: String,
    },
}

impl ServerMsg {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::KeepAlive {} => "KeepAlive",
            Self::GetLobbiesResponse { .. } => "GetLobbiesResponse",
            Self::CreateLobbyResponse { .. } => "CreateLobbyResponse",
            Self::JoinLobbyResponse { .. } => "JoinLobbyResponse",
            Self::LobbyUpdate { .. } => "LobbyUpdate",
            Self::LeaveLobbyResponse { .. } => "LeaveLobbyResponse",
            Self::RematchLobby {} => "RematchLobby",
            Self::StartLobby {} => "StartLobby",
            Self::ArcherSelectLobby {} => "ArcherSelectLobby",
            Self::SeriesLobby {} => "SeriesLobby",
            Self::PrivateJoinResult { .. } => "PrivateJoinResult",
            Self::QuickPlayStatus { .. } => "QuickPlayStatus",
            Self::QuickPlayMatchFound { .. } => "QuickPlayMatchFound",
            Self::SpectatorJoined { .. } => "SpectatorJoined",
            Self::SkinChunk { .. } => "SkinChunk",
            Self::KickedFromLobby { .. } => "KickedFromLobby",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkinChunk {
    pub bundle_id: String,
    pub custom_archer_id: String,
    pub chunk_index: u32,
    pub chunk_count: u32,
    pub data: String,
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
    SeriesContinueChoice {},
    SeriesPickMap {
        map_id: i32,
    },
    MatchEnded {
        #[serde(default)]
        winner_seat: Option<i32>,
        #[serde(default)]
        frame: Option<i32>,
        #[serde(default)]
        checksum: Option<String>,
        #[serde(default)]
        scores: Option<Vec<i32>>,
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
    PeerMeasured {
        room_peer_id: String,
    },
}
