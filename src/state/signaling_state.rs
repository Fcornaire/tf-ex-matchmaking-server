use std::collections::{HashMap, HashSet};

use axum::{extract::ws::Message, Error};
use matchbox_protocol::PeerId;
use serde::Deserialize;

#[derive(Debug, Deserialize, Default, Clone, PartialEq, Eq, Hash)]
pub struct RoomId(String);

impl From<RoomId> for String {
    fn from(room_id: RoomId) -> Self {
        room_id.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RequestedRoom {
    pub id: RoomId,
    pub next: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct Peer {
    pub uuid: PeerId,
    pub room: RequestedRoom,
    pub sender: tokio::sync::mpsc::UnboundedSender<std::result::Result<Message, Error>>,
}

#[derive(Default, Debug, Clone)]
pub struct SignalingState {
    pub clients: HashMap<PeerId, Peer>,
    pub rooms: HashMap<RequestedRoom, HashSet<PeerId>>,
}

impl SignalingState {
    /// Add a peer, returning the peers already in room
    pub fn add_peer(&mut self, peer: Peer) -> Vec<PeerId> {
        let peer_id = peer.uuid;
        let room = peer.room.clone();
        self.clients.insert(peer.uuid, peer);
        let peers = self.rooms.entry(room.clone()).or_default();

        let ret = peers.iter().cloned().collect();
        match room.next {
            None => {
                peers.insert(peer_id);
                ret
            }
            Some(num_players) => {
                if peers.len() == num_players - 1 {
                    peers.clear(); // the room is complete, we can forget about it now
                } else {
                    peers.insert(peer_id);
                }
                ret
            }
        }
    }

    /// Remove a peer from the state if it existed, returning the peer removed.
    #[must_use]
    pub fn remove_peer(&mut self, peer_id: &PeerId) -> Option<Peer> {
        let peer = self.clients.remove(peer_id);

        if let Some(ref peer) = peer {
            // Best effort to remove peer from their room
            _ = self
                .rooms
                .get_mut(&peer.room)
                .map(|room| room.remove(peer_id));
        }
        peer
    }

    /// Send a message to a peer without blocking.
    pub fn try_send(&self, id: PeerId, message: Message) -> Result<(), String> {
        let peer = self.clients.get(&id);
        let peer = match peer {
            Some(peer) => peer,
            None => {
                return Err("UnknownPeer".to_string());
            }
        };

        match peer.sender.send(Ok(message)) {
            Ok(_) => Ok(()),
            Err(e) => {
                return Err(format!("Error sending to {:?}: {:?}", id, e));
            }
        }
    }
}
