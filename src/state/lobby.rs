use std::collections::HashMap;
use std::time::Instant;

use rand::Rng;
use serde::{Deserialize, Serialize};
use tracing::error;

use crate::constants::{MAX_PLAYERS, SERIES_TEAM_SIZE, TEAM_DEATHMATCH_MODE};
use crate::services::geoip::Location;
use crate::state::series::Series;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum EndGameChoice {
    Rematch,
    Archer,
    Continue,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub struct EndGameVote {
    #[serde(skip)]
    pub addr: String,
    #[serde(default)]
    pub room_peer_id: String,
    pub choice: EndGameChoice,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Mod {
    name: String,
    data: HashMap<String, String>,
}

impl Mod {
    pub fn new(name: String, data: HashMap<String, String>) -> Self {
        Self { name, data }
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn data(&self) -> HashMap<String, String> {
        self.data.clone()
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub enum LobbyKind {
    #[default]
    Standard,
    QuickPlay,
    Private,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Lobby {
    name: String,
    room_id: String,
    #[serde(default)]
    max_players: usize,
    players: Vec<Player>,
    spectators: Vec<Player>,
    game_data: GameData,
    end_game_choice: Option<Vec<EndGameVote>>,
    #[serde(default)]
    in_game: bool,
    mods: Vec<Mod>,
    #[serde(default)]
    kind: LobbyKind,
    #[serde(default)]
    join_code: Option<String>,
    #[serde(default)]
    series: Option<Series>,
    #[serde(skip)]
    origin: Option<Location>,
    #[serde(skip)]
    membership_changed_at: Option<Instant>, //When a player deselect, select, change archers,...
    #[serde(skip)]
    all_ready_since: Option<Instant>,
    #[serde(skip)]
    in_game_since: Option<Instant>,
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

    pub fn spectators(&self) -> Vec<Player> {
        self.spectators.clone()
    }

    pub fn room_id(&self) -> String {
        self.room_id.clone()
    }

    pub fn mods(&self) -> Vec<Mod> {
        self.mods.clone()
    }

    pub fn in_game(&self) -> bool {
        self.in_game
    }

    pub fn kind(&self) -> LobbyKind {
        self.kind
    }

    pub fn join_code(&self) -> Option<String> {
        self.join_code.clone()
    }

    pub fn origin(&self) -> Option<Location> {
        self.origin.clone()
    }

    pub fn series(&self) -> Option<&Series> {
        self.series.as_ref()
    }

    pub fn series_mut(&mut self) -> Option<&mut Series> {
        self.series.as_mut()
    }

    pub fn is_series_lobby(&self) -> bool {
        self.game_data.best_of > 0
    }

    pub fn series_in_progress(&self) -> bool {
        self.series.as_ref().is_some_and(|s| s.is_in_progress())
    }

    pub fn all_players_ready(&self) -> bool {
        !self.players.is_empty() && self.players.iter().all(|p| p.ready)
    }

    pub fn is_team_mode(&self) -> bool {
        self.game_data.mode == TEAM_DEATHMATCH_MODE
    }

    pub fn start_series(&mut self) -> Result<(), String> {
        let sides = self.series_sides()?;

        self.series = Some(Series::new(self.game_data.best_of as u32, sides));

        Ok(())
    }

    fn series_sides(&self) -> Result<Vec<Vec<usize>>, String> {
        let mut seats: Vec<usize> = self.players.iter().map(|p| p.seat).collect();
        seats.sort_unstable();

        if seats.len() == 2 {
            return Ok(seats.into_iter().map(|seat| vec![seat]).collect());
        }

        if !self.is_team_mode() || seats.len() != 2 * SERIES_TEAM_SIZE {
            return Err("A series needs a 1v1 or a 2v2 team deathmatch".to_string());
        }

        let sides: Vec<Vec<usize>> = (0..2)
            .map(|team| {
                let mut side: Vec<usize> = self
                    .players
                    .iter()
                    .filter(|p| p.team == team)
                    .map(|p| p.seat)
                    .collect();
                side.sort_unstable();
                side
            })
            .collect();

        if sides.iter().any(|side| side.len() != SERIES_TEAM_SIZE) {
            return Err("A 2v2 series needs two full teams".to_string());
        }

        Ok(sides)
    }

    pub fn begin_series_game(&mut self) -> Option<i32> {
        let map_id = self.series.as_mut()?.begin_game()?;
        self.game_data.map_id = map_id;

        Some(map_id)
    }

    pub fn abort_series(&mut self, reason: &str) -> bool {
        match self.series.as_mut() {
            Some(series) if series.is_in_progress() => {
                series.abort(reason);
                true
            }
            _ => false,
        }
    }

    pub fn secs_since_membership_change(&self) -> u64 {
        self.membership_changed_at
            .map(|at| at.elapsed().as_secs())
            .unwrap_or(u64::MAX)
    }

    pub fn secs_since_all_ready(&mut self) -> Option<u64> {
        if self.players.is_empty() || self.players.iter().any(|p| !p.ready) {
            self.all_ready_since = None;
            return None;
        }

        let started = *self.all_ready_since.get_or_insert_with(Instant::now);

        Some(started.elapsed().as_secs())
    }

    pub fn promote_host_if_needed(&mut self) {
        if self.kind != LobbyKind::QuickPlay
            || self.players.is_empty()
            || self.players.iter().any(|p| p.is_host)
        {
            return;
        }

        if let Some(lowest) = self.players.iter_mut().min_by_key(|p| p.seat) {
            lowest.is_host = true;
        }
    }

    pub fn set_in_game(&mut self, in_game: bool) {
        self.in_game_since = in_game.then(Instant::now);
        self.in_game = in_game;
    }

    pub fn match_duration_secs(&self) -> Option<f64> {
        self.in_game_since
            .map(|since| since.elapsed().as_secs_f64())
    }

    pub fn take_match_duration_secs(&mut self) -> Option<f64> {
        self.in_game_since
            .take()
            .map(|since| since.elapsed().as_secs_f64())
    }

    pub fn end_game_choice(&self) -> Option<Vec<EndGameVote>> {
        self.end_game_choice.clone()
    }

    pub fn live_end_game_votes(&self) -> Vec<EndGameVote> {
        self.end_game_choice
            .clone()
            .unwrap_or_default()
            .into_iter()
            .filter(|vote| self.players.iter().any(|p| p.addr() == vote.addr))
            .collect()
    }

    pub fn players_without_choice(&self) -> Vec<String> {
        let votes = self.live_end_game_votes();

        self.players
            .iter()
            .map(|p| p.addr())
            .filter(|addr| !votes.iter().any(|vote| vote.addr == *addr))
            .collect()
    }

    pub fn update_player(&mut self, addr: &str, player: Player) {
        let Some(p) = self.players.iter_mut().find(|p| p.addr == addr) else {
            return;
        };

        let custom_variants = if player.custom_variants.is_empty() {
            std::mem::take(&mut p.custom_variants)
        } else {
            player.custom_variants.clone()
        };

        *p = Player {
            addr: std::mem::take(&mut p.addr),
            room_peer_id: std::mem::take(&mut p.room_peer_id),
            is_host: p.is_host,
            seat: p.seat,
            custom_variants,
            ..player
        };
    }

    pub fn has_member(&self, addr: &str) -> bool {
        self.players.iter().any(|p| p.addr() == addr)
            || self.spectators.iter().any(|p| p.addr() == addr)
    }

    pub fn add_player(&mut self, mut player: Player) {
        player.seat = self.first_free_seat();
        self.players.push(player);
        self.membership_changed_at = Some(Instant::now());
    }

    fn first_free_seat(&self) -> usize {
        (0..MAX_PLAYERS)
            .find(|seat| !self.players.iter().any(|p| p.seat == *seat))
            .unwrap_or(self.players.len())
    }

    pub fn add_spectator(&mut self, player: Player) {
        self.spectators.push(player);
    }

    pub fn remove_player_or_spectator(&mut self, addr: String) {
        let seated_before = self.players.len();
        self.players.retain(|p| p.addr() != addr);
        self.spectators.retain(|p| p.addr() != addr);
        self.membership_changed_at = Some(Instant::now());

        if self.players.len() != seated_before {
            self.abort_series("PLAYER LEFT");
        }
    }

    pub fn update_seed(&mut self, seed: i32) {
        self.game_data.seed = seed;
    }

    pub fn update_settings(&mut self, max_players: usize, game_data: GameData, mods: Vec<Mod>) {
        let seed = self.game_data.seed;

        self.max_players = max_players.clamp(self.players.len().max(2), MAX_PLAYERS);
        self.game_data = game_data;
        self.game_data.seed = seed;
        self.mods = mods;
        self.series = None;

        self.unready_all();
    }

    pub fn unready_all(&mut self) {
        self.players.iter_mut().for_each(|p| p.ready = false);
        self.membership_changed_at = Some(Instant::now());
    }

    pub fn initialize_end_game_choice(&mut self) {
        self.end_game_choice = Some(vec![]);
    }

    pub fn rematch(&mut self, addr: String) {
        self.record_choice(addr, EndGameChoice::Rematch);
    }

    pub fn archer_select_choice(&mut self, addr: String) {
        self.record_choice(addr, EndGameChoice::Archer);
    }

    pub fn continue_choice(&mut self, addr: String) {
        self.record_choice(addr, EndGameChoice::Continue);
    }

    fn record_choice(&mut self, addr: String, choice: EndGameChoice) {
        let Some(room_peer_id) = self
            .players
            .iter()
            .find(|p| p.addr == addr)
            .map(|p| p.room_peer_id())
        else {
            return;
        };

        let Some(end_game_choice) = &mut self.end_game_choice else {
            error!("No end game choice meaning the game is not over or already over");
            return;
        };

        match end_game_choice.iter_mut().find(|vote| vote.addr == addr) {
            Some(vote) => vote.choice = choice,
            None => end_game_choice.push(EndGameVote {
                addr,
                room_peer_id,
                choice,
            }),
        }
    }

    pub fn reset(&mut self) {
        self.players.iter_mut().for_each(|p| p.ready = false);
        self.end_game_choice = Some(vec![]);
        self.update_seed(rand::thread_rng().gen_range(0..100000));
    }

    pub fn ready_all(&mut self) -> usize {
        let forced = self.players.iter().filter(|p| !p.ready).count();
        self.players.iter_mut().for_each(|p| p.ready = true);
        forced
    }

    pub fn set_all_ready(&mut self) {
        self.ready_all();
    }

    pub fn backdate_membership(&mut self, secs: u64) {
        self.membership_changed_at =
            Instant::now().checked_sub(std::time::Duration::from_secs(secs));
    }

    pub fn backdate_ready(&mut self, secs: u64) {
        self.all_ready_since = Instant::now().checked_sub(std::time::Duration::from_secs(secs));
    }

    pub fn max_players(&self) -> usize {
        self.max_players.clamp(2, MAX_PLAYERS)
    }

    pub fn is_full(&self) -> bool {
        self.players.len() >= self.max_players()
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
    /// 0 for single match; 1, 3, 5 for the series length
    #[serde(default)]
    pub best_of: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Player {
    name: String,
    #[serde(skip)]
    addr: String,
    archer_index: i32,
    archer_alt_index: i32,
    ready: bool,
    room_peer_id: String,
    is_host: bool,
    #[serde(default)]
    seat: usize,
    #[serde(default = "neutral_team")]
    team: i32,
    #[serde(default)]
    custom_archer_id: String,
    #[serde(default)]
    archer_mods: Vec<String>,
    #[serde(default)]
    custom_variants: Vec<String>,
}

fn neutral_team() -> i32 {
    -1
}

impl Player {
    pub fn new(name: String, addr: String, room_peer_id: String, is_host: bool) -> Self {
        Self {
            name,
            addr,
            archer_index: 0,
            archer_alt_index: 0,
            room_peer_id,
            ready: false,
            is_host,
            seat: 0,
            team: neutral_team(),
            custom_archer_id: String::new(),
            archer_mods: Vec::new(),
            custom_variants: Vec::new(),
        }
    }

    pub fn seat(&self) -> usize {
        self.seat
    }

    pub fn ready(&self) -> bool {
        self.ready
    }

    pub fn team(&self) -> i32 {
        self.team
    }

    pub fn set_team(&mut self, team: i32) {
        self.team = team;
    }

    pub fn set_seat(&mut self, seat: usize) {
        self.seat = seat;
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

    pub fn room_peer_id(&self) -> String {
        self.room_peer_id.clone()
    }

    pub fn update_room_peer_id(&mut self, room_peer_id: String) {
        self.room_peer_id = room_peer_id;
    }
}

#[derive(Default)]
pub struct LobbyBuilder {
    name: Option<String>,
    room_id: Option<String>,
    max_players: Option<usize>,
    players: Option<Vec<Player>>,
    game_data: Option<GameData>,
    mods: Option<Vec<Mod>>,
    kind: Option<LobbyKind>,
    join_code: Option<String>,
    origin: Option<Location>,
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

    pub fn max_players(mut self, max_players: usize) -> Self {
        self.max_players = Some(max_players.clamp(2, MAX_PLAYERS));
        self
    }

    pub fn mods(mut self, mods: Vec<Mod>) -> Self {
        self.mods = Some(mods);
        self
    }

    pub fn kind(mut self, kind: LobbyKind) -> Self {
        self.kind = Some(kind);
        self
    }

    pub fn join_code(mut self, join_code: Option<String>) -> Self {
        self.join_code = join_code;
        self
    }

    pub fn origin(mut self, origin: Option<Location>) -> Self {
        self.origin = origin;
        self
    }

    pub fn build(self) -> Lobby {
        Lobby {
            name: self.name.expect("lobby name is required"),
            game_data: self.game_data.expect("lobby game data is required"),
            players: self.players.unwrap_or_default(),
            room_id: self.room_id.expect("lobby room id is required"),
            max_players: self.max_players.unwrap_or(MAX_PLAYERS),
            spectators: vec![],
            end_game_choice: None,
            in_game: false,
            mods: self.mods.unwrap_or_default(),
            kind: self.kind.unwrap_or_default(),
            join_code: self.join_code,
            series: None,
            origin: self.origin,
            membership_changed_at: None,
            all_ready_since: None,
            in_game_since: None,
        }
    }
}
