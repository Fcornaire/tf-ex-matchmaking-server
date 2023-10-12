use crate::services::geoip::Location;

#[derive(Debug, Clone)]
pub struct Client {
    pub name: String,
    pub location: Location,
    pub room: Option<Room>,
    pub room_chat: Option<Room>,
}

impl Client {
    pub fn new(location: Location) -> Self {
        Self {
            name: "".to_string(),
            location,
            room: None,
            room_chat: None,
        }
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

        if let Some(room_chat) = &self.room_chat {
            if room_chat.id() == room_id {
                return Some(room_chat.peer_id());
            }
        }

        None
    }

    pub fn set_live(&mut self, room_id: String, is_live: bool) {
        if let Some(room) = &mut self.room {
            if room.id() == room_id {
                room.peer.as_mut().unwrap().set_live(is_live);
            }
        }

        if let Some(room_chat) = &mut self.room_chat {
            if room_chat.id() == room_id {
                room_chat.peer.as_mut().unwrap().set_live(is_live);
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
