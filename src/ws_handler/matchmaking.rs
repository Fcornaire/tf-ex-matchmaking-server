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
use tracing::{debug, info};

use crate::{
    constants::SEND_CLIENT_TASK_INTERVAL,
    extensions::clients::ClientsExts,
    msg::{ClientMsg, ServerMsg},
    state::{AppState, Clients},
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
            address = format!("{fordwarded_for}:1234").parse().unwrap();
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
    let (mut sink, mut stream) = socket.split();
    let (sender, mut receiver) = mpsc::channel::<Message<ServerMsg>>(16);

    // spawn a task that forwards messages from the mpsc to the sink
    tokio::spawn(async move {
        while let Some(message) = receiver.recv().await {
            if sink.send(message.into()).await.is_err() {
                break;
            }
        }
    });

    let cloned_state_recv = state.clone();
    let cloned_state_send = state.clone();

    let send_task_in_rcv = sender.clone();
    let send_task_in_send = sender.clone();

    let mut recv_task = tokio::spawn(async move {
        while let Some(msg) = stream.next().await {
            debug!("Received message: {:?}", msg);
            match msg {
                Ok(Message::Item(item)) => match item {
                    ClientMsg::RegisterDirect { name } => {
                        let clients: &mut Clients = &mut cloned_state_recv.clients.clone();

                        let (code, result) = clients.register_direct_async(ip, name).await;

                        if !result {
                            send_task_in_rcv
                                .send(Message::Item(ServerMsg::RegisterDirectResponse {
                                    success: false,
                                    code,
                                }))
                                .await
                                .unwrap();
                            break;
                        }

                        info!("Client {} registered for direct", ip);
                        send_task_in_rcv
                            .send(Message::Item(ServerMsg::RegisterDirectResponse {
                                success: true,
                                code,
                            }))
                            .await
                            .unwrap();
                    }
                    ClientMsg::MatchWithDirectCode { code } => {
                        let clients: &mut Clients = &mut cloned_state_recv.clients.clone();

                        let result = clients.match_with_direct_code_async(ip, code.clone()).await;

                        if let (Some(_), seed, room_id, name) = result {
                            info!("Client {} matched with direct code {}", ip, code);
                            send_task_in_rcv
                                .send(Message::Item(ServerMsg::MatchWithDirectCodeResponse {
                                    success: true,
                                    opponent_name: name,
                                    room_id,
                                    seed,
                                }))
                                .await
                                .unwrap();
                        } else {
                            send_task_in_rcv
                                .send(Message::Item(ServerMsg::MatchWithDirectCodeResponse {
                                    success: false,
                                    opponent_name: String::from(""),
                                    room_id: String::from(""),
                                    seed: -1,
                                }))
                                .await
                                .unwrap();
                        }
                    }
                    ClientMsg::RegisterQuickPlay { name } => {
                        let clients: &mut Clients = &mut cloned_state_recv.clients.clone();

                        let result = clients
                            .register_for_quick_play_async(
                                cloned_state_recv.get_geoip_client(),
                                ip,
                                name,
                            )
                            .await;

                        if !result {
                            send_task_in_rcv
                                .send(Message::Item(ServerMsg::RegisterQuickPlayResponse {
                                    success: false,
                                }))
                                .await
                                .unwrap();
                            break;
                        }

                        info!("Client {} registered for quick play", ip);
                        send_task_in_rcv
                            .send(Message::Item(ServerMsg::RegisterQuickPlayResponse {
                                success: true,
                            }))
                            .await
                            .unwrap();
                    }
                    ClientMsg::CancelQuickPlay {} => {
                        let clients: &mut Clients = &mut cloned_state_recv.clients.clone();

                        clients.cancel_quick_play_async(ip).await;

                        info!("Client {} canceled quick play", ip);
                    }
                    ClientMsg::AcceptQuickPlay {} => {
                        let clients: &mut Clients = &mut cloned_state_recv.clients.clone();

                        let result = clients.accept_quick_play_async(ip).await;

                        if let (Some(_), seed, name) = result {
                            info!("Client {} accepted quick play", ip);
                            send_task_in_rcv
                                .send(Message::Item(ServerMsg::AcceptQuickPlayResponse {
                                    success: true,
                                    seed,
                                    opponent_name: name,
                                }))
                                .await
                                .unwrap();
                        } else {
                            send_task_in_rcv
                                .send(Message::Item(ServerMsg::AcceptQuickPlayResponse {
                                    success: false,
                                    opponent_name: String::from(""),
                                    seed: -1,
                                }))
                                .await
                                .unwrap();
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

    let mut send_task = tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(SEND_CLIENT_TASK_INTERVAL));
        let mut has_matched: bool = false;

        loop {
            interval.tick().await;

            let clients: &mut Clients = &mut cloned_state_send.clients.clone();

            send_task_in_send
                .send(Message::Item(
                    ServerMsg::TotalAvailablePlayersInQuickPlayQueue {
                        total: clients
                            .get_total_matchable_clients_in_quick_play_async()
                            .await,
                    },
                ))
                .await
                .unwrap();

            let current_socket = clients.get_client(ip).await;

            if let Some(client) = current_socket {
                if !client.is_matched() {
                    let is_waiting = client.is_waiting_choice;

                    if is_waiting {
                        continue;
                    }

                    if let Some(addr) = client.possible_match {
                        clients.set_client_waiting_choice(ip).await;

                        let opponent_name = clients.get_client(addr).await.unwrap().name;

                        send_task_in_send
                            .send(Message::Item(ServerMsg::QuickPlayPossibleMatch {
                                room_id: client.room_id.unwrap().clone(),
                                opponent_name,
                            }))
                            .await
                            .unwrap();

                        has_matched = true;
                    } else {
                        if has_matched {
                            has_matched = false;
                            send_task_in_send
                                .send(Message::Item(ServerMsg::DeniedQuickPlay {}))
                                .await
                                .unwrap();
                        }
                    }
                }
            }
        }
    });

    tokio::select! {
        _ = (&mut send_task) => {
            recv_task.abort();
        },
        _ = (&mut recv_task) => {
            send_task.abort();
        }
    }

    state.clone().clients.clone().remove_client(ip).await;
    info!("Websocket context {} destroyed", ip);
}
