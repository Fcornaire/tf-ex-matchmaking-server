use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Query, State,
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    Error,
};
use futures::{stream::SplitSink, StreamExt};
use matchbox_protocol::{JsonPeerEvent, JsonPeerRequest, PeerRequest};
use std::{collections::HashMap, str::FromStr, sync::Arc, time::Duration};
use tokio::{sync::mpsc, time::interval};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{debug, error, info, warn};

use super::is_room_peer_known;

use crate::{
    constants::KEEP_ALIVE_INTERVAL,
    extensions::{clients::ClientsExts, signaling_state::SignalingStateExts},
    state::{
        signaling_state::{Peer, RequestedRoom, RoomId},
        AppState,
    },
};

/// The handler for the HTTP request to upgrade to WebSockets.
pub async fn signaling_handler(
    ws: WebSocketUpgrade,
    room_id: Option<Path<RoomId>>,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<AppState>>,
) -> Response {
    if !is_room_peer_known(&state, &requested_room_id(&room_id), &params).await {
        return StatusCode::NOT_FOUND.into_response();
    }

    upgrade(ws, room_id, params, state, false).into_response()
}

/// Lobby ping measurement
pub async fn ping_measurement_handler(
    ws: WebSocketUpgrade,
    room_id: Option<Path<RoomId>>,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<AppState>>,
) -> Response {
    if !is_room_peer_known(&state, &requested_room_id(&room_id), &params).await {
        return StatusCode::NOT_FOUND.into_response();
    }

    upgrade(ws, room_id, params, state, true).into_response()
}

fn requested_room_id(room_id: &Option<Path<RoomId>>) -> String {
    room_id
        .as_ref()
        .map(|path| String::from(path.0.clone()))
        .unwrap_or_default()
}

fn upgrade(
    ws: WebSocketUpgrade,
    room_id: Option<Path<RoomId>>,
    params: HashMap<String, String>,
    state: Arc<AppState>,
    ping_measurement: bool,
) -> impl IntoResponse {
    let room_id = room_id.map(|path| path.0).unwrap_or_default();
    let next = params
        .get("next")
        .and_then(|next| next.parse::<usize>().ok());
    let requested_peer = params
        .get("peer")
        .and_then(|peer| uuid::Uuid::parse_str(peer).ok());

    // Finalize the upgrade process by returning upgrade callback to client
    ws.on_upgrade(move |websocket| {
        handle_ws(
            websocket,
            state,
            RequestedRoom { id: room_id, next },
            requested_peer,
            ping_measurement,
        )
    })
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
async fn handle_ws(
    websocket: WebSocket,
    state: Arc<AppState>,
    requested_room: RequestedRoom,
    requested_peer: Option<uuid::Uuid>,
    ping_measurement: bool,
) {
    let (ws_sender, mut ws_receiver) = websocket.split();
    let signaling = if ping_measurement {
        state.ping_measurement_signaling.clone()
    } else {
        state.signaling.clone()
    };
    let kind = if ping_measurement {
        "PingMeasurement"
    } else {
        "Signaling"
    };
    let sender = spawn_sender_task(ws_sender);

    let room_id: String = requested_room.clone().id.into();

    let peer_uuid: matchbox_protocol::PeerId = match requested_peer {
        Some(uuid) => uuid.into(),
        None => {
            warn!("{kind}: no peer token for room {room_id}, refusing");
            return;
        }
    };

    let Some(client) = state
        .clients
        .get_client_by_room_peer(room_id.clone(), peer_uuid.0.to_string())
        .await
    else {
        warn!("{kind}: peer {peer_uuid:?} is not a member of room {room_id}, refusing");
        return;
    };

    let who = client.label();
    info!("{kind}: {who} connected to room {room_id}");

    if !ping_measurement {
        state
            .clients
            .set_client_live_by_peer_id(peer_uuid.0.to_string(), true)
            .await;
    }

    {
        let mut add_peer_state = signaling.lock().await;

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

    let cloned_signaling_recv = signaling.clone();
    let cloned_state_loop = state.clone();
    let requested_room_clone = requested_room.clone();
    let who_recv = who.clone();
    let who_loop = who.clone();

    // The state machine for the data channel established for this websocket.
    let mut recv_task = tokio::spawn(async move {
        while let Some(request) = ws_receiver.next().await {
            let request = match parse_request(request) {
                Ok(request) => request,
                Err(e) => {
                    warn!("Error untangling request: {:?}", e);
                    continue;
                }
            };

            match request {
                PeerRequest::Signal { receiver, data } => {
                    debug!("{kind}: {} sent a signal to {:?}", who_recv, receiver);

                    let event = Message::Text(
                        JsonPeerEvent::Signal {
                            sender: peer_uuid,
                            data,
                        }
                        .to_string(),
                    );
                    let state = cloned_signaling_recv.lock().await;
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
    });

    let mut loop_task = tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(KEEP_ALIVE_INTERVAL));

        loop {
            interval.tick().await;

            let client = cloned_state_loop
                .clients
                .get_client_in_room(requested_room_clone.clone().id.into())
                .await;

            if client.is_none() {
                warn!("{kind}: {who_loop} not found in matchmaking, closing connection");
                break;
            }
        }
    });

    tokio::select! {
        _ = (&mut recv_task) => {
            info!("{kind}: {} disconnected", who);
            loop_task.abort();
        }
        _ = (&mut loop_task) => {
            info!("{kind}: {} left matchmaking, closing", who);
            recv_task.abort();
        }
    }

    if !ping_measurement {
        state
            .clone()
            .clients
            .set_client_live_by_peer_id(peer_uuid.0.to_string(), false)
            .await;
    }

    // Peer disconnected or otherwise ended communication.
    signaling.clone().remove_signaling_peer(peer_uuid).await;

    state.clone().remove_all_empty_signaling_rooms().await;
}
