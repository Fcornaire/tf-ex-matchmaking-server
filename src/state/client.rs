use std::{collections::HashMap, net::SocketAddr};

use time::{macros::format_description, OffsetDateTime};

use crate::{
    constants::{MAX_MATCH_DISTANCE, MIN_MATCH_DISTANCE},
    services::geoip::Location,
};

#[derive(Debug, Clone)]
pub struct Client {
    pub name: String,
    pub code: String,
    pub matchmaking_type: MatchmakingType,
    pub matched: Option<SocketAddr>,
    pub seed: i32,
    pub location: Location,
    pub possible_match: Option<SocketAddr>,
    pub matched_history: HashMap<SocketAddr, String>,
    pub max_match_distance: Option<f64>,
    pub room_id: Option<String>,
    pub room_chat_id: Option<String>,
}

#[derive(Debug, Clone)]
pub enum MatchmakingType {
    Direct,
    QuickPlay,
}

impl Client {
    pub fn new_direct(code: String, name: String) -> Self {
        Self {
            name,
            code,
            matchmaking_type: MatchmakingType::Direct,
            matched: None,
            seed: -1,
            location: Location::default(),
            possible_match: None,
            matched_history: HashMap::new(),
            max_match_distance: None,
            room_id: None,
            room_chat_id: None,
        }
    }

    pub fn new_quickplay(name: String) -> Self {
        Self {
            name,
            code: "".to_string(),
            matchmaking_type: MatchmakingType::QuickPlay,
            matched: None,
            seed: -1,
            location: Location::default(),
            possible_match: None,
            matched_history: HashMap::new(),
            max_match_distance: Some(MIN_MATCH_DISTANCE),
            room_id: None,
            room_chat_id: None,
        }
    }

    pub fn extend_max_match_distance(&mut self) {
        if let Some(max_distance) = self.max_match_distance {
            self.max_match_distance =
                Some((max_distance + MIN_MATCH_DISTANCE).min(MAX_MATCH_DISTANCE));
        }
    }

    pub fn match_max_distance(&self) -> f64 {
        if let Some(max_distance) = self.max_match_distance {
            return max_distance;
        }
        MAX_MATCH_DISTANCE
    }

    pub fn add_match_history(&mut self, opponent: SocketAddr) {
        if !self.matched_history.contains_key(&opponent) {
            let format = format_description!(
                "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond] [offset_hour sign:mandatory]:[offset_minute]:[offset_second]"
            );

            self.matched_history
                .insert(opponent, OffsetDateTime::now_utc().format(&format).unwrap());
        }
    }

    pub fn already_matched_with(&self, opponent: SocketAddr) -> bool {
        self.matched_history.contains_key(&opponent)
    }

    pub fn set_matched(&mut self, to_update: SocketAddr) {
        self.matched = Some(to_update);
    }

    pub fn is_direct(&self) -> bool {
        match self.matchmaking_type {
            MatchmakingType::Direct => true,
            _ => false,
        }
    }

    pub fn is_quick_play(&self) -> bool {
        match self.matchmaking_type {
            MatchmakingType::QuickPlay => true,
            _ => false,
        }
    }

    pub fn is_near(&self, client: Client, max_distance: f64) -> bool {
        self.location.is_near(client.location, max_distance)
    }

    pub fn set_match(&mut self, addr: SocketAddr) {
        self.possible_match = Some(addr);
    }

    pub fn already_have_a_match(&self) -> bool {
        self.possible_match.is_some()
    }

    pub fn have_a_match_with(&self, addr: SocketAddr) -> bool {
        self.possible_match == Some(addr)
    }

    pub fn is_matched(&self) -> bool {
        self.matched.is_some()
    }

    pub fn is_matched_to(&self, addr: SocketAddr) -> bool {
        self.is_matched() && self.matched == Some(addr)
    }

    pub fn match_for_quick_play(&mut self) {
        let opponent = self.possible_match.take().unwrap();

        self.matched = Some(opponent);
    }

    pub fn unmatch_quick_play(&mut self) {
        self.matched = None;
        self.possible_match = None;
    }

    pub fn set_location(&mut self, location: Location) {
        self.location = location;
    }

    pub fn remove_match_history(&mut self, ip: &SocketAddr) {
        self.matched_history.remove(ip);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::constants::{MAX_MATCH_DISTANCE, MIN_MATCH_DISTANCE};

    #[test]
    fn test_extend_max_match_distance_with_max_distance_less_than_max() {
        let mut state = Client::new_quickplay("127.0.0.1:3000".parse().unwrap());
        state.max_match_distance = Some(10.0);
        state.extend_max_match_distance();
        assert_eq!(state.max_match_distance, Some(MIN_MATCH_DISTANCE + 10.0));
    }

    #[test]
    fn test_extend_max_match_distance_with_max_distance_equal_to_max() {
        let mut state = Client::new_quickplay("127.0.0.1:3000".parse().unwrap());
        state.max_match_distance = Some(MAX_MATCH_DISTANCE);
        state.extend_max_match_distance();
        assert_eq!(state.max_match_distance, Some(MAX_MATCH_DISTANCE));
    }

    #[test]
    fn test_extend_max_match_distance_with_max_distance_greater_than_max() {
        let mut state = Client::new_quickplay("127.0.0.1:3000".parse().unwrap());
        state.max_match_distance = Some(MAX_MATCH_DISTANCE + 10.0);
        state.extend_max_match_distance();
        assert_eq!(state.max_match_distance, Some(MAX_MATCH_DISTANCE));
    }
}
