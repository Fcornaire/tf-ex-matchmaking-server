use std::time::Instant;

use crate::services::geoip::Location;

#[derive(Debug, Clone)]
pub struct Client {
    pub name: String,
    pub location: Location,
    pub room: Option<Room>,
    pub alive_counter: u8,
    last_keep_alive_sent: Option<Instant>,
    pub last_private_join_attempt: Option<Instant>,
    pub player_id: Option<String>,
}

impl Client {
    pub fn new(location: Location) -> Self {
        Self {
            name: "".to_string(),
            location,
            room: None,
            alive_counter: 0,
            last_keep_alive_sent: None,
            last_private_join_attempt: None,
            player_id: None,
        }
    }

    pub fn keep_alive_sent(&mut self) {
        self.last_keep_alive_sent = Some(Instant::now());
    }

    pub fn take_keep_alive_rtt(&mut self) -> Option<u32> {
        self.last_keep_alive_sent.take().map(|sent| {
            let micros = sent.elapsed().as_micros();

            ((micros + 500) / 1000).min(u32::MAX as u128) as u32
        })
    }

    pub fn alive(&mut self) {
        self.alive_counter = 0;
    }

    pub fn is_alive(&self) -> bool {
        self.alive_counter < 3
    }

    pub fn maybe_not_alive(&mut self) {
        self.alive_counter += 1;
    }

    pub fn is_near(&self, client: Client, max_distance: f64) -> bool {
        self.location.is_near(client.location, max_distance)
    }

    pub fn set_location(&mut self, location: Location) {
        self.location = location;
    }

    pub fn peer_id(&self, room_id: String) -> Option<String> {
        if let Some(room) = &self.room {
            if room.id() == room_id {
                return Some(room.peer_id());
            }
        }

        None
    }

    pub fn is_peer_id_live(&self, peer_id: String) -> bool {
        if let Some(room) = &self.room {
            return room.peer.as_ref().unwrap().id() == peer_id;
        }

        false
    }

    pub fn set_live(&mut self, room_id: String, is_live: bool) {
        if let Some(room) = &mut self.room {
            if room.id() == room_id {
                room.peer.as_mut().unwrap().set_live(is_live);
            }
        }
    }

    pub fn set_live_by_peer_id(&mut self, peer_id: String, is_live: bool) {
        if let Some(room) = &mut self.room {
            if room.peer.as_ref().unwrap().id() == peer_id {
                room.peer.as_mut().unwrap().set_live(is_live);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Room {
    id: String,
    peer: Option<Peer>,
}

impl Room {
    pub fn new_without_peer(id: String) -> Self {
        Self { id, peer: None }
    }

    pub fn new(id: String) -> Self {
        let peer_id = uuid::Uuid::new_v4().to_string();

        Self {
            id,
            peer: Some(Peer::new(peer_id)),
        }
    }

    pub fn id(&self) -> String {
        self.id.clone()
    }

    pub fn peer_id(&self) -> String {
        match &self.peer {
            Some(peer) => peer.id(),
            None => "".to_string(),
        }
    }

    pub fn is_live(&self) -> bool {
        match &self.peer {
            Some(peer) => peer.is_live(),
            None => true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Peer {
    pub id: String,
    pub is_live: bool,
}

impl Peer {
    pub fn new(id: String) -> Self {
        Self { id, is_live: false }
    }

    pub fn id(&self) -> String {
        self.id.clone()
    }

    pub fn is_live(&self) -> bool {
        self.is_live
    }

    pub fn set_live(&mut self, is_live: bool) {
        self.is_live = is_live;
    }
}
