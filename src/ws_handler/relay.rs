use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Query, State,
    },
    http::StatusCode,
    response::{IntoResponse, Response},
};
use futures::{SinkExt, StreamExt};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{sync::mpsc, time::interval};
use tracing::{info, warn};

use super::is_room_peer_known;

use crate::{
    constants::KEEP_ALIVE_INTERVAL,
    extensions::clients::ClientsExts,
    relay::{Channel, ConnectionEvent, JoinError, Relay, RoomKey},
    state::AppState,
};

pub async fn relay_room_handler(
    ws: WebSocketUpgrade,
    Path(room_id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<AppState>>,
) -> Response {
    if !is_room_peer_known(&state, &room_id, &params).await {
        return StatusCode::NOT_FOUND.into_response();
    }

    upgrade(ws, room_id, params, state, Channel::Match)
}

pub async fn relay_ping_measurement_handler(
    ws: WebSocketUpgrade,
    Path(room_id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<AppState>>,
) -> Response {
    if !is_room_peer_known(&state, &room_id, &params).await {
        return StatusCode::NOT_FOUND.into_response();
    }

    upgrade(ws, room_id, params, state, Channel::PingMeasurement)
}

fn upgrade(
    ws: WebSocketUpgrade,
    room_id: String,
    params: HashMap<String, String>,
    state: Arc<AppState>,
    channel: Channel,
) -> Response {
    let Some(relay) = state.relay() else {
        return StatusCode::NOT_FOUND.into_response();
    };

    let peer = params
        .get("peer")
        .and_then(|peer| uuid::Uuid::parse_str(peer).ok())
        .map(|peer| peer.to_string());

    let key = RoomKey { room_id, channel };

    ws.on_upgrade(move |websocket| handle_ws(websocket, state, relay, key, peer))
        .into_response()
}

async fn handle_ws(
    websocket: WebSocket,
    state: Arc<AppState>,
    relay: Arc<Relay>,
    key: RoomKey,
    peer: Option<String>,
) {
    let room_id = key.room_id.clone();
    let channel = key.channel;

    let Some(peer) = peer else {
        warn!("Relay: no peer token for room {room_id}, refusing");
        return;
    };

    let Some(client) = state
        .clients
        .get_client_by_room_peer(room_id.clone(), peer.clone())
        .await
    else {
        warn!("Relay: peer {peer} is not a member of room {room_id}, refusing");
        return;
    };

    let who = client.label();
    let is_spectator = channel == Channel::Match && state.is_room_spectator(&room_id, &peer).await;
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();

    let token = match relay.join(key, peer, is_spectator, events_tx) {
        Ok(token) => token,
        Err(JoinError::SpectatorsFull) => {
            info!("Relay: no spectator slot left in room {room_id}, refusing {who}");
            refuse(websocket, ConnectionEvent::SpectatorsFull {}).await;
            return;
        }
        Err(error) => {
            warn!("Relay: {who} cannot join room {room_id} ({error:?}), refusing");
            return;
        }
    };

    info!("Relay: {who} joined room {room_id} ({channel:?})");

    let (mut sink, mut stream) = websocket.split();

    let mut send_task = tokio::spawn(async move {
        while let Some(event) = events_rx.recv().await {
            let Ok(text) = serde_json::to_string(&event) else {
                continue;
            };

            if sink.send(Message::Text(text)).await.is_err() {
                break;
            }
        }
    });

    let mut recv_task = tokio::spawn(async move {
        while let Some(Ok(message)) = stream.next().await {
            if let Message::Close(_) = message {
                break;
            }
        }
    });

    let membership_state = state.clone();
    let membership_room = room_id.clone();
    let mut membership_task = tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(KEEP_ALIVE_INTERVAL));

        loop {
            interval.tick().await;

            let client = membership_state
                .clients
                .get_client_in_room(membership_room.clone())
                .await;

            if client.is_none() {
                break;
            }
        }
    });

    tokio::select! {
        _ = &mut recv_task => {}
        _ = &mut send_task => {}
        _ = &mut membership_task => {
            info!("Relay: room {room_id} left matchmaking, closing {who}");
        }
    }

    recv_task.abort();
    send_task.abort();
    membership_task.abort();

    relay.leave(token);

    info!("Relay: {who} left room {room_id} ({channel:?})");
}

async fn refuse(mut websocket: WebSocket, event: ConnectionEvent) {
    if let Ok(text) = serde_json::to_string(&event) {
        let _ = websocket.send(Message::Text(text)).await;
    }

    let _ = websocket.send(Message::Close(None)).await;
}
