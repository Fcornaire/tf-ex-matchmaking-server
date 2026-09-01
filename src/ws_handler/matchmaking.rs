use std::{
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
    str::FromStr,
    sync::Arc,
};

use axum::{
    extract::{ConnectInfo, State},
    response::IntoResponse,
    TypedHeader,
};
use axum_typed_websockets::{Message, WebSocket, WebSocketUpgrade};
use futures::{SinkExt, StreamExt};
use headers::HeaderMap;
use metrics::counter;
use tokio::{
    sync::mpsc,
    time::{interval, Duration},
};
use tracing::{debug, error, info, warn};

use crate::{
    constants::KEEP_ALIVE_INTERVAL,
    extensions::{clients::ClientsExts, str::StrExts},
    msg::{ClientMsg, ServerMsg},
    state::{lobby::Lobby, AppState, QuickPlayOutcome},
    telemetry,
};

pub async fn matchmaking_handler(
    ws: WebSocketUpgrade<ServerMsg, ClientMsg>,
    user_agent: Option<TypedHeader<headers::UserAgent>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    info!(
        "New connection from raw {} with  headers {:?}",
        addr, headers
    );

    let user_agent = if let Some(TypedHeader(user_agent)) = user_agent {
        user_agent.to_string()
    } else {
        String::from("Unknown browser")
    };

    let mut addressv4: Option<SocketAddr> = None;
    let mut addressv6: Option<SocketAddr> = None;

    for (key, value) in headers.iter() {
        match key.as_str().to_lowercase().as_str() {
            "x-forwarded-for" => {
                if value.to_str().unwrap().contains(",") {
                    let splitted = value.to_str().unwrap().split(",").collect::<Vec<&str>>();
                    for ip in splitted.iter().filter(|ip| !ip.is_an_ip_and_private()) {
                        if let Ok(ipv6) = Ipv6Addr::from_str(ip) {
                            if addressv6.is_some() {
                                continue;
                            }

                            addressv6 =
                                Some(SocketAddr::new(std::net::IpAddr::V6(ipv6), addr.port()));
                        } else if let Ok(ipv4) = ip.parse::<Ipv4Addr>() {
                            if addressv4.is_some() {
                                continue;
                            }

                            addressv4 =
                                Some(SocketAddr::new(std::net::IpAddr::V4(ipv4), addr.port()));
                        }
                    }
                } else {
                    if let Ok(ipv6) = Ipv6Addr::from_str(value.to_str().unwrap()) {
                        addressv6 = Some(SocketAddr::new(std::net::IpAddr::V6(ipv6), addr.port()));
                    } else if let Ok(ipv4) = value.to_str().unwrap().parse::<Ipv4Addr>() {
                        addressv4 = Some(SocketAddr::new(std::net::IpAddr::V4(ipv4), addr.port()));
                    }
                }
            }

            _ => {}
        }
    }

    if addressv4.is_none() && addressv6.is_none() {
        addressv4 = Some(addr);
    }

    info!(
        "`{user_agent}` at ipv4 {:#?} / ipv6 {:#?} connected.",
        addressv4, addressv6
    );

    let ips: Vec<SocketAddr> = vec![addressv4, addressv6]
        .into_iter()
        .filter_map(|x| x)
        .collect();

    ws.on_upgrade(move |socket| handle_socket(socket, ips, state.clone()))
}

/// Actual websocket statemachine (one will be spawned per connection)
pub async fn handle_socket(
    socket: WebSocket<ServerMsg, ClientMsg>,
    ips: Vec<SocketAddr>,
    state: Arc<AppState>,
) {
    let Some(ip) = ips
        .iter()
        .find(|x| x.is_ipv4())
        .or_else(|| ips.first())
        .copied()
    else {
        return;
    };
    let client = state.clone().clients.get_client(ip).await;
    if client.is_some() {
        info!("Client {} already connected", ip);
        return;
    }

    let res = state.clone().add_new_client(ips.clone()).await;
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
        Ok(()) => {
            counter!(telemetry::CONNECTIONS_TOTAL).increment(1);
        }
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
            if !matches!(msg, Ok(Message::Item(ClientMsg::KeepAlive {}))) {
                debug!("Received message: {:?}", msg);
            }
            match msg {
                Ok(Message::Item(item)) => match item {
                    ClientMsg::KeepAlive {} => {
                        match cloned_state_recv.update_alive_client(ip).await {
                            Ok(()) => {}
                            Err(err) => {
                                error!("Error when trying to make client alive : {}", err);
                            }
                        }

                        if let Some(room_id) = cloned_state_recv.record_keep_alive_rtt(ip).await {
                            if let Err(err) = cloned_state_recv.send_ping_update(room_id).await {
                                error!("Error broadcasting ping update : {}", err);
                            }
                        }
                    }
                    ClientMsg::Identify { player_id, name } => {
                        if let Err(err) = cloned_state_recv.identify_client(ip, player_id, name).await
                        {
                            warn!("Client {} identify rejected : {}", ip, err);
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

                        cloned_state_recv.exit_quick_play(ip).await;

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

                        cloned_state_recv.exit_quick_play(ip).await;

                        match cloned_state_recv
                            .join_lobby(room_id.clone(), ip, name, is_player)
                            .await
                        {
                            Ok(room_peer_id) => {
                                send_task_in_rcv
                                    .send(Message::Item(ServerMsg::JoinLobbyResponse {
                                        success: true,
                                        message: "".to_string(),
                                        room_peer_id: room_peer_id.clone(),
                                    }))
                                    .await
                                    .unwrap();

                                if let Err(err) =
                                    cloned_state_recv.send_lobby_update(room_id.clone()).await
                                {
                                    warn!(
                                        "Could not send lobby update (lobby likely gone): {}",
                                        err
                                    );
                                }

                                if !is_player {
                                    cloned_state_recv
                                        .notify_host_of_late_spectator(&room_id, &room_peer_id)
                                        .await;
                                }
                            }
                            Err(err) => {
                                warn!("Error client {} joining lobby : {}", ip, err);

                                send_task_in_rcv
                                    .send(Message::Item(ServerMsg::JoinLobbyResponse {
                                        success: false,
                                        message: err,
                                        room_peer_id: "".to_string(),
                                    }))
                                    .await
                                    .unwrap();
                            }
                        }
                    }
                    ClientMsg::EnterQuickPlay { name, is_wide } => {
                        info!(
                            "Client {} entered the quickplay queue (wide: {is_wide})",
                            ip
                        );

                        let outcome = cloned_state_recv.enter_quick_play(ip, name, is_wide).await;

                        let outcome_label = match &outcome {
                            QuickPlayOutcome::Queued { .. } => "queued",
                            QuickPlayOutcome::Seated { .. } => "seated",
                            QuickPlayOutcome::Paired { .. } => "paired",
                            QuickPlayOutcome::Failed { .. } => "failed",
                        };
                        counter!(telemetry::QUICKPLAY_ENTER_TOTAL, "outcome" => outcome_label)
                            .increment(1);

                        match outcome {
                            QuickPlayOutcome::Queued { searching } => {
                                send_task_in_rcv
                                    .send(Message::Item(ServerMsg::QuickPlayStatus {
                                        queued: true,
                                        searching,
                                        message: "".to_string(),
                                    }))
                                    .await
                                    .unwrap();
                            }
                            QuickPlayOutcome::Seated {
                                lobby,
                                room_peer_id,
                            } => {
                                info!(
                                    "Client {} seated into quickplay lobby {}",
                                    ip,
                                    lobby.room_id()
                                );

                                send_task_in_rcv
                                    .send(Message::Item(ServerMsg::QuickPlayMatchFound {
                                        lobby: lobby.clone(),
                                        room_peer_id,
                                    }))
                                    .await
                                    .unwrap();

                                if let Err(err) =
                                    cloned_state_recv.send_lobby_update(lobby.room_id()).await
                                {
                                    warn!(
                                        "Could not send lobby update (lobby likely gone): {}",
                                        err
                                    );
                                }
                            }
                            QuickPlayOutcome::Paired {
                                lobby,
                                caller_peer_id,
                                partner_addr,
                                partner_peer_id,
                            } => {
                                info!(
                                    "Client {} paired with {} in quickplay lobby {}",
                                    ip,
                                    partner_addr,
                                    lobby.room_id()
                                );

                                send_task_in_rcv
                                    .send(Message::Item(ServerMsg::QuickPlayMatchFound {
                                        lobby: lobby.clone(),
                                        room_peer_id: caller_peer_id,
                                    }))
                                    .await
                                    .unwrap();

                                cloned_state_recv
                                    .send_msg_to_addrs(
                                        &[partner_addr],
                                        ServerMsg::QuickPlayMatchFound {
                                            lobby,
                                            room_peer_id: partner_peer_id,
                                        },
                                    )
                                    .await;
                            }
                            QuickPlayOutcome::Failed { message } => {
                                warn!("Quickplay enter failed for {} : {}", ip, message);

                                send_task_in_rcv
                                    .send(Message::Item(ServerMsg::QuickPlayStatus {
                                        queued: false,
                                        searching: 0,
                                        message,
                                    }))
                                    .await
                                    .unwrap();
                            }
                        }
                    }
                    ClientMsg::ExitQuickPlay {} => {
                        info!("Client {} left the quickplay queue", ip);

                        let searching = cloned_state_recv.exit_quick_play(ip).await;

                        send_task_in_rcv
                            .send(Message::Item(ServerMsg::QuickPlayStatus {
                                queued: false,
                                searching,
                                message: "".to_string(),
                            }))
                            .await
                            .unwrap();
                    }
                    ClientMsg::JoinPrivate {
                        code,
                        name,
                        is_player,
                    } => {
                        info!(
                            "Client {} want to join a private lobby by code, is a player {is_player}",
                            ip
                        );

                        cloned_state_recv.exit_quick_play(ip).await;

                        match cloned_state_recv
                            .join_private(code, ip, name, is_player)
                            .await
                        {
                            Ok((lobby, room_peer_id)) => {
                                send_task_in_rcv
                                    .send(Message::Item(ServerMsg::PrivateJoinResult {
                                        success: true,
                                        message: "".to_string(),
                                        room_peer_id: room_peer_id.clone(),
                                        lobby: Some(lobby.clone()),
                                    }))
                                    .await
                                    .unwrap();

                                if let Err(err) =
                                    cloned_state_recv.send_lobby_update(lobby.room_id()).await
                                {
                                    warn!(
                                        "Could not send lobby update (lobby likely gone): {}",
                                        err
                                    );
                                }

                                if !is_player {
                                    cloned_state_recv
                                        .notify_host_of_late_spectator(
                                            &lobby.room_id(),
                                            &room_peer_id,
                                        )
                                        .await;
                                }
                            }
                            Err(err) => {
                                warn!("Error client {} joining private lobby : {}", ip, err);

                                send_task_in_rcv
                                    .send(Message::Item(ServerMsg::PrivateJoinResult {
                                        success: false,
                                        message: err,
                                        room_peer_id: "".to_string(),
                                        lobby: None,
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
                                match cloned_state_recv
                                    .initialize_end_game_choice(room_id.clone())
                                    .await
                                {
                                    Ok(()) => {}
                                    Err(err) => {
                                        error!("Error initializing end game choice : {}", err);
                                    }
                                }

                                if let Err(err) =
                                    cloned_state_recv.send_lobby_update(room_id.clone()).await
                                {
                                    warn!(
                                        "Could not send lobby update (lobby likely gone): {}",
                                        err
                                    );
                                }
                            }
                            Err(err) => {
                                error!("Error updating player : {}", err);
                            }
                        }
                    }
                    ClientMsg::UpdateLobbySettings {
                        max_players,
                        game_data,
                        mods,
                    } => {
                        info!("Client {} want to update lobby settings", ip);

                        match cloned_state_recv
                            .update_lobby_settings(ip, max_players, game_data, mods)
                            .await
                        {
                            Ok(room_id) => {
                                if let Err(err) =
                                    cloned_state_recv.send_lobby_update(room_id.clone()).await
                                {
                                    warn!(
                                        "Could not send lobby update (lobby likely gone): {}",
                                        err
                                    );
                                }
                            }
                            Err(err) => {
                                error!("Error updating lobby settings : {}", err);
                            }
                        }
                    }
                    ClientMsg::LeaveLobby {} => {
                        info!("Client {} want to leave lobby", ip);

                        match cloned_state_recv.remove_from_lobby(ip).await {
                            Ok(room_id) => {
                                if let Err(err) =
                                    cloned_state_recv.send_lobby_update(room_id.clone()).await
                                {
                                    warn!(
                                        "Could not send lobby update (lobby likely gone): {}",
                                        err
                                    );
                                }

                                cloned_state_recv.purge_lobby_async().await;

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
                            }
                        }
                    }
                    ClientMsg::StartLobbyChoice {} => {
                        info!("Client {} wants to start the lobby early", ip);

                        if let Err(err) = cloned_state_recv.start_lobby(ip).await {
                            warn!("Error starting lobby : {}", err);
                        }
                    }
                    ClientMsg::MatchEnded {
                        winner_seat,
                        frame,
                        checksum,
                    } => {
                        if let Some(seat) = winner_seat {
                            cloned_state_recv
                                .handle_match_report(ip, seat, frame, checksum)
                                .await;
                        }

                        if let Some(room_id) = cloned_state_recv.room_id_for(ip).await {
                            cloned_state_recv.start_end_game_countdown(room_id).await;
                        }
                    }
                    ClientMsg::RematchLobbyChoice {} => {
                        info!("Client {} want to rematch lobby", ip);

                        match cloned_state_recv.rematch_lobby(ip).await {
                            Ok(room_id) => {
                                cloned_state_recv.end_game_action(room_id.clone()).await;
                                if let Err(err) =
                                    cloned_state_recv.send_lobby_update(room_id.clone()).await
                                {
                                    warn!(
                                        "Could not send lobby update (lobby likely gone): {}",
                                        err
                                    );
                                }
                            }
                            Err(err) => {
                                error!("Error rematching lobby : {}", err);
                            }
                        }
                    }
                    ClientMsg::SkinChunk { chunk } => {
                        if let Err(err) = cloned_state_recv.relay_skin_chunk(ip, chunk).await {
                            warn!("Skin chunk from {} rejected : {}", ip, err);
                        }
                    }
                    ClientMsg::ArcherSelectChoice {} => {
                        info!("Client {} want to go select archer", ip);

                        match cloned_state_recv.archer_select_choice_lobby(ip).await {
                            Ok(room_id) => {
                                cloned_state_recv.end_game_action(room_id.clone()).await;
                                if let Err(err) =
                                    cloned_state_recv.send_lobby_update(room_id.clone()).await
                                {
                                    warn!(
                                        "Could not send lobby update (lobby likely gone): {}",
                                        err
                                    );
                                }
                            }
                            Err(err) => {
                                error!("Error go selecting archer : {}", err);
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
                    warn!("Error receiving message : {}", err);
                }
            };
        }
    });

    let mut keep_alive_task = tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(KEEP_ALIVE_INTERVAL));

        loop {
            interval.tick().await;
            let mut clients = cloned_state_keep_alive.clients.lock().await;

            let client = clients
                .iter_mut()
                .find(|((ipv4, _), _)| *ipv4 == ip)
                .map(|c| c.1);

            if client.is_none() {
                tracing::info!("Client {} not found", ip);
                break;
            }

            let client = client.unwrap();

            if !(*client).is_alive() {
                tracing::info!("Client {} is no more alive, aborting...", ip);
                break;
            }

            client.keep_alive_sent();
            client.maybe_not_alive();
            drop(clients);

            send_task_in_keep_alive
                .send(Message::Item(ServerMsg::KeepAlive {}))
                .await
                .unwrap();
        }
    });

    let disconnect_reason;

    tokio::select! {
        _ = (&mut keep_alive_task) => {
            recv_task.abort();
            disconnect_reason = "timeout";
        },
        _ = (&mut recv_task) => {
            keep_alive_task.abort();
            disconnect_reason = "clean";
        }
    }

    counter!(telemetry::DISCONNECTS_TOTAL, "reason" => disconnect_reason).increment(1);

    state.exit_quick_play(ip).await;

    let mut clients = state.clone().clients.clone();

    let client = clients.get_client(ip).await;

    if let Some(cli) = client {
        if cli.room.is_some() {
            info!("Client {} was in a lobby", ip);

            match state.remove_from_lobby(ip).await {
                Ok(room_id) => {
                    if let Err(err) = state.send_lobby_update(room_id.clone()).await {
                        warn!("Could not send lobby update (lobby likely gone): {}", err);
                    }

                    state.purge_lobby_async().await;
                }
                Err(err) => {
                    warn!(
                        "Error removing {} from lobby (might already be deleted): {}",
                        ip, err
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
