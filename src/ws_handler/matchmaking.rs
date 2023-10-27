use std::{net::SocketAddr, sync::Arc};

use axum::{
    extract::{ConnectInfo, State},
    response::IntoResponse,
    TypedHeader,
};
use axum_typed_websockets::{Message, WebSocket, WebSocketUpgrade};
use futures::{SinkExt, StreamExt};
use headers::HeaderMap;
use tokio::{
    sync::mpsc,
    time::{interval, Duration},
};
use tracing::{debug, error, info, warn};

use crate::{
    constants::KEEP_ALIVE_INTERVAL,
    extensions::clients::ClientsExts,
    msg::{ClientMsg, ServerMsg},
    state::{lobby::Lobby, AppState},
};

pub async fn matchmaking_handler(
    ws: WebSocketUpgrade<ServerMsg, ClientMsg>,
    user_agent: Option<TypedHeader<headers::UserAgent>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let user_agent = if let Some(TypedHeader(user_agent)) = user_agent {
        user_agent.to_string()
    } else {
        String::from("Unknown browser")
    };

    let mut address = addr;
    for (key, value) in headers.iter() {
        if key.as_str().to_lowercase() == "x-forwarded-for" {
            let fordwarded_for = value.to_str().unwrap();
            address = format!("{fordwarded_for}:{}", addr.port()).parse().unwrap();
        }
    }

    info!("`{user_agent}` at {address} connected.");

    ws.on_upgrade(move |socket| handle_socket(socket, address, state))
}

/// Actual websocket statemachine (one will be spawned per connection)
pub async fn handle_socket(
    socket: WebSocket<ServerMsg, ClientMsg>,
    ip: SocketAddr,
    state: Arc<AppState>,
) {
    let client = state.clone().clients.get_client(ip).await;
    if client.is_some() {
        info!("Client {} already connected", ip);
        return;
    }

    let res = state.clone().add_new_client(ip).await;
    if res.is_err() {
        info!("Client {} could not be added : {}", ip, res.unwrap_err());
        return;
    }

    let (mut sink, mut stream) = socket.split();
    let (sender, mut receiver) = mpsc::channel::<Message<ServerMsg>>(512);

    // spawn a task that forwards messages from the mpsc to the sink
    tokio::spawn(async move {
        while let Some(message) = receiver.recv().await {
            if sink.send(message.into()).await.is_err() {
                break;
            }
        }
    });

    match state.clone().add_connexion(ip, sender.clone()).await {
        Ok(()) => {}
        Err(err) => {
            warn!("Client {} could not be added : {}", ip, err);
            info!("Thus Websocket context {} destroyed", ip);
            return;
        }
    }

    let cloned_state_recv = state.clone();
    let cloned_state_keep_alive = state.clone();

    let send_task_in_rcv = sender.clone();
    let send_task_in_keep_alive = sender.clone();

    let mut recv_task = tokio::spawn(async move {
        while let Some(msg) = stream.next().await {
            debug!("Received message: {:?}", msg);
            match msg {
                Ok(Message::Item(item)) => match item {
                    ClientMsg::KeepAlive {} => {
                        match cloned_state_recv.update_alive_client(ip).await {
                            Ok(()) => {}
                            Err(err) => {
                                error!("Error when trying to make client alive : {}", err);
                            }
                        }
                    }
                    ClientMsg::GetLobbies {} => {
                        info!("Client {} requested lobbies", ip);

                        let lobbies: Vec<Lobby> = cloned_state_recv.get_lobbies(ip).await;

                        send_task_in_rcv
                            .send(Message::Item(ServerMsg::GetLobbiesResponse { lobbies }))
                            .await
                            .unwrap();
                    }
                    ClientMsg::CreateLobby { mut lobby } => {
                        info!("Client {} want to create a lobby", ip);

                        match cloned_state_recv.add_lobby(&mut lobby, ip).await {
                            Ok(final_lobby) => {
                                send_task_in_rcv
                                    .send(Message::Item(ServerMsg::CreateLobbyResponse {
                                        success: true,
                                        message: "".to_string(),
                                        lobby: Some(final_lobby),
                                    }))
                                    .await
                                    .unwrap();
                            }
                            Err(err) => {
                                warn!("Error client {} creating lobby : {}", ip, err);
                                send_task_in_rcv
                                    .send(Message::Item(ServerMsg::CreateLobbyResponse {
                                        success: false,
                                        message: err,
                                        lobby: None,
                                    }))
                                    .await
                                    .unwrap();
                            }
                        }
                    }
                    ClientMsg::JoinLobby {
                        room_id,
                        name,
                        is_player,
                    } => {
                        info!(
                            "Client {} want to join lobby {room_id}, is a player {is_player}",
                            ip
                        );

                        match cloned_state_recv
                            .join_lobby(room_id.clone(), ip, name, is_player)
                            .await
                        {
                            Ok((room_peer_id, room_chat_peer_id)) => {
                                send_task_in_rcv
                                    .send(Message::Item(ServerMsg::JoinLobbyResponse {
                                        success: true,
                                        message: "".to_string(),
                                        room_peer_id,
                                        room_chat_peer_id,
                                    }))
                                    .await
                                    .unwrap();

                                cloned_state_recv
                                    .send_lobby_update(room_id.clone())
                                    .await
                                    .unwrap();
                            }
                            Err(err) => {
                                warn!("Error client {} joining lobby : {}", ip, err);

                                send_task_in_rcv
                                    .send(Message::Item(ServerMsg::JoinLobbyResponse {
                                        success: false,
                                        message: err,
                                        room_peer_id: "".to_string(),
                                        room_chat_peer_id: "".to_string(),
                                    }))
                                    .await
                                    .unwrap();
                            }
                        }
                    }
                    ClientMsg::UpdatePlayer { player } => {
                        info!("Client {} want to update player", ip);

                        match cloned_state_recv.update_player(ip, player).await {
                            Ok(room_id) => {
                                cloned_state_recv
                                    .send_lobby_update(room_id.clone())
                                    .await
                                    .unwrap();
                            }
                            Err(err) => {
                                error!("Error updating player : {}", err);
                                break;
                            }
                        }
                    }
                    ClientMsg::LeaveLobby {} => {
                        info!("Client {} want to leave lobby", ip);

                        match cloned_state_recv.remove_from_lobby(ip).await {
                            Ok(room_id) => {
                                cloned_state_recv
                                    .send_lobby_update(room_id.clone())
                                    .await
                                    .unwrap();

                                cloned_state_recv.purge_lobby().await;

                                send_task_in_rcv
                                    .send(Message::Item(ServerMsg::LeaveLobbyResponse {
                                        success: true,
                                        message: "".to_string(),
                                    }))
                                    .await
                                    .unwrap();
                            }
                            Err(err) => {
                                send_task_in_rcv
                                    .send(Message::Item(ServerMsg::LeaveLobbyResponse {
                                        success: false,
                                        message: err,
                                    }))
                                    .await
                                    .unwrap();
                                break;
                            }
                        }
                    }
                },
                Ok(Message::Close(_fame)) => {
                    info!("Client {} closed connection", ip);

                    send_task_in_rcv
                        .send(Message::Close(None))
                        .await
                        .unwrap_or_else(|_| ());

                    break;
                }
                Ok(_) => {}
                Err(err) => {
                    tracing::error!("got error: {}", err);
                }
            };
        }
    });

    let mut keep_alive_task = tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(KEEP_ALIVE_INTERVAL));

        loop {
            interval.tick().await;
            let mut clients = cloned_state_keep_alive.clients.lock().await;

            let client = clients.iter_mut().find(|c| (*c.0) == ip).map(|c| c.1);

            if client.is_none() {
                tracing::info!("Client {} not found", ip);
                break;
            }

            let client = client.unwrap();

            if !(*client).is_alive() {
                tracing::info!("Client {} is no more alive, aborting...", ip);
                break;
            }

            send_task_in_keep_alive
                .send(Message::Item(ServerMsg::KeepAlive {}))
                .await
                .unwrap();

            client.maybe_not_alive();
        }
    });

    tokio::select! {
        _ = (&mut keep_alive_task) => {
            recv_task.abort();
        },
        _ = (&mut recv_task) => {
            keep_alive_task.abort();
        }
    }

    let mut clients = state.clone().clients.clone();

    let client = clients.get_client(ip).await;

    if let Some(cli) = client {
        if cli.room.is_some() {
            info!("Client {} was in a lobby", ip);

            match state.remove_from_lobby(ip).await {
                Ok(room_id) => {
                    state.send_lobby_update(room_id.clone()).await.unwrap();

                    state.purge_lobby().await;
                }
                Err(err) => {
                    error!(
                        "Error removing from lobby (might already be deleted): {}",
                        err
                    );
                }
            }
        }
    }

    clients.remove_client(ip).await;

    match state.clone().remove_connexion(ip).await {
        Ok(()) => {}
        Err(err) => {
            error!("Error removing connexion : {}", err);
        }
    }

    info!("Websocket context {} destroyed", ip);
}
