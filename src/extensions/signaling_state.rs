use std::sync::Arc;

use async_trait::async_trait;
use axum::extract::ws::Message;
use matchbox_protocol::{JsonPeerEvent, PeerId};
use tokio::sync::Mutex;
use tracing::{error, info};

use crate::state::signaling_state::SignalingState;

#[async_trait]
pub trait SignalingStateExts {
    async fn remove_signaling_peer(&mut self, peer_uuid: PeerId);
}

#[async_trait]
impl SignalingStateExts for Arc<Mutex<SignalingState>> {
    async fn remove_signaling_peer(&mut self, peer_uuid: PeerId) {
        let mut state = self.lock().await;

        if let Some(removed_peer) = state.remove_peer(&peer_uuid) {
            let room = removed_peer.room;
            let peers = state
                .rooms
                .get(&room)
                .map(|room_peers| {
                    room_peers
                        .iter()
                        .copied()
                        .filter(|peer_id| *peer_id != peer_uuid)
                        .collect::<Vec<PeerId>>()
                })
                .unwrap_or_default();
            // Tell each connected peer about the disconnected peer.
            let event = Message::Text(JsonPeerEvent::PeerLeft(removed_peer.uuid).to_string());
            for peer_id in peers {
                match state.try_send(peer_id, event.clone()) {
                    Ok(()) => info!("Sent peer remove to: {:?}", peer_id),
                    Err(e) => error!("Failure sending peer remove: {e:?}"),
                }
            }
        } else {
            info!("Peer not found, probably already removed: {:?}", peer_uuid);
        }
    }
}
