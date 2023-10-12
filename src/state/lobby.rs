use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Lobby {
    name: String,
    room_id: String,
    room_chat_id: String,
    players: Vec<Player>,
    game_data: GameData,
}

impl Lobby {
    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn game_data(&self) -> GameData {
        self.game_data.clone()
    }

    pub fn players(&self) -> Vec<Player> {
        self.players.clone()
    }

    pub fn room_id(&self) -> String {
        self.room_id.clone()
    }

    pub fn room_chat_id(&self) -> String {
        self.room_chat_id.clone()
    }

    pub fn update_player(&mut self, player: Player) {
        self.players
            .iter_mut()
            .find(|p| p.addr() == player.addr())
            .map(|p| *p = player);
    }

    pub fn add_player(&mut self, player: Player) {
        self.players.push(player);
    }

    pub fn remove_player(&mut self, addr: String) {
        self.players.retain(|p| p.addr() != addr);
    }

    pub fn update_seed(&mut self, seed: i32) {
        self.game_data.seed = seed;
    }

    //TODO: update to 4 players when implemented
    pub fn is_full(&self) -> bool {
        self.players.len() == 2
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct GameData {
    pub map_id: i32,
    pub mode: i32,
    pub match_length: i32,
    pub variants: Vec<String>,
    pub seed: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Player {
    name: String,
    addr: String,
    archer_index: i32,
    archer_alt_index: i32,
    ready: bool,
    room_peer_id: String,
    room_chat_peer_id: String,
    is_host: bool,
}

impl Player {
    pub fn new(
        name: String,
        addr: String,
        room_peer_id: String,
        room_chat_peer_id: String,
        is_host: bool,
    ) -> Self {
        Self {
            name,
            addr,
            archer_index: 0,
            archer_alt_index: 0,
            room_peer_id,
            ready: false,
            room_chat_peer_id,
            is_host,
        }
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn is_host(&self) -> bool {
        self.is_host
    }

    pub fn addr(&self) -> String {
        self.addr.clone()
    }

    pub fn update_addr(&mut self, addr: String) {
        self.addr = addr;
    }
}

#[derive(Default)]
pub struct LobbyBuilder {
    name: Option<String>,
    room_id: Option<String>,
    room_chat_id: Option<String>,
    players: Option<Vec<Player>>,
    game_data: Option<GameData>,
}

impl LobbyBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }

    pub fn game_data(mut self, game_data: GameData) -> Self {
        self.game_data = Some(game_data);
        self
    }

    pub fn players(mut self, players: Vec<Player>) -> Self {
        self.players = Some(players);
        self
    }

    pub fn room_id(mut self, room_id: String) -> Self {
        self.room_id = Some(room_id);
        self
    }

    pub fn room_chat_id(mut self, room_chat_id: String) -> Self {
        self.room_chat_id = Some(room_chat_id);
        self
    }

    pub fn build(self) -> Lobby {
        Lobby {
            name: self.name.expect("lobby name is required"),
            game_data: self.game_data.expect("lobby game data is required"),
            players: self.players.unwrap_or_default(),
            room_id: self.room_id.expect("lobby room id is required"),
            room_chat_id: self.room_chat_id.expect("lobby room chat id is required"),
        }
    }
}
