use axum::{
    extract::{
        connect_info::ConnectInfo,
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Query, State,
    },
    response::IntoResponse,
    Error,
};
use futures::{stream::SplitSink, StreamExt};
use matchbox_protocol::{JsonPeerEvent, JsonPeerRequest, PeerId, PeerRequest};
use std::{collections::HashMap, net::SocketAddr, str::FromStr, sync::Arc};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{error, info, warn};

use crate::state::{
    signaling_state::{Peer, RequestedRoom, RoomId},
    AppState,
};

/// The handler for the HTTP request to upgrade to WebSockets.
/// This is the last point where we can extract TCP/IP metadata such as IP address of the client.
pub async fn signaling_handler(
    ws: WebSocketUpgrade,
    room_id: Option<Path<RoomId>>,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<AppState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> impl IntoResponse {
    info!("`{addr}` connected.");

    let room_id = room_id.map(|path| path.0).unwrap_or_default();
    let next = params
        .get("next")
        .and_then(|next| next.parse::<usize>().ok());

    // Finalize the upgrade process by returning upgrade callback to client
    ws.on_upgrade(move |websocket| handle_ws(websocket, state, RequestedRoom { id: room_id, next }))
}

fn parse_request(request: Result<Message, Error>) -> Result<JsonPeerRequest, String> {
    if let Ok(request) = request {
        let request = match request {
            Message::Text(text) => JsonPeerRequest::from_str(&text),
            Message::Close(_) => return Err("Close".to_string()),
            _ => return Err("UnsupportedType".to_string()),
        };

        return Ok(request.unwrap());
    }

    Err(format!("Can't parse request {}", request.err().unwrap()))
}

fn spawn_sender_task(
    sender: SplitSink<WebSocket, Message>,
) -> mpsc::UnboundedSender<Result<Message, Error>> {
    let (client_sender, receiver) = mpsc::unbounded_channel();
    tokio::task::spawn(UnboundedReceiverStream::new(receiver).forward(sender));
    client_sender
}

/// One of these handlers is spawned for every web socket.
async fn handle_ws(websocket: WebSocket, state: Arc<AppState>, requested_room: RequestedRoom) {
    let (ws_sender, mut ws_receiver) = websocket.split();
    let sender = spawn_sender_task(ws_sender);

    let peer_uuid = uuid::Uuid::new_v4().into();

    {
        let mut add_peer_state = state.signaling.lock().await;

        let peers = add_peer_state.add_peer(Peer {
            uuid: peer_uuid,
            sender: sender.clone(),
            room: requested_room.clone(),
        });

        let event_text = JsonPeerEvent::IdAssigned(peer_uuid).to_string();
        let event = Message::Text(event_text.clone());

        if let Err(e) = add_peer_state.try_send(peer_uuid, event) {
            error!("error sending to {peer_uuid:?}: {e:?}");
        } else {
            info!("{:?} -> {:?}", peer_uuid, event_text);
        };

        let event_text = JsonPeerEvent::NewPeer(peer_uuid).to_string();
        let event = Message::Text(event_text.clone());

        for peer_id in peers {
            // Tell everyone about this new peer
            if let Err(e) = add_peer_state.try_send(peer_id, event.clone()) {
                error!("error sending to {peer_id:?}: {e:?}");
            } else {
                info!("{:?} -> {:?}", peer_id, event_text);
            }
        }
    }

    // The state machine for the data channel established for this websocket.
    while let Some(request) = ws_receiver.next().await {
        let request = match parse_request(request) {
            Ok(request) => request,
            Err(e) => {
                error!("Error untangling request: {:?}", e);
                continue;
            }
        };

        info!("{:?} <- {:?}", peer_uuid, request);

        match request {
            PeerRequest::Signal { receiver, data } => {
                let event = Message::Text(
                    JsonPeerEvent::Signal {
                        sender: peer_uuid,
                        data,
                    }
                    .to_string(),
                );
                let state = state.signaling.lock().await;
                if let Some(peer) = state.clients.get(&receiver) {
                    if let Err(e) = peer.sender.send(Ok(event)) {
                        error!("error sending: {:?}", e);
                    }
                } else {
                    warn!("peer not found ({receiver:?}), ignoring signal");
                }
            }
            PeerRequest::KeepAlive => {
                // Do nothing. KeepAlive packets are used to protect against idle websocket
                // connections getting automatically disconnected, common for reverse proxies.
            }
        }
    }

    // Peer disconnected or otherwise ended communication.
    info!("Removing peer: {:?}", peer_uuid);
    let mut state = state.signaling.lock().await;
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
    }
}
