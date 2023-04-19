use axum::{
    extract::{State, TypedHeader},
    response::IntoResponse,
    routing::get,
    Router,
};
use futures::{SinkExt, StreamExt};
use msg::{ClientMsg, ServerMsg};

use std::{
    net::{SocketAddr, UdpSocket},
    path::PathBuf,
    sync::Arc,
};
use tower_http::{
    services::ServeDir,
    trace::{DefaultMakeSpan, TraceLayer},
};

use tokio::{
    sync::mpsc,
    time::{self, Duration},
};

use axum_typed_websockets::{Message, WebSocket, WebSocketUpgrade};

use crate::state::{AppState, Clients, ClientsExts};

use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::jobs::spawn_jobs;

//allows to extract the IP of connecting user
use axum::extract::connect_info::ConnectInfo;
pub mod jobs;
pub mod msg;
pub mod state;

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "example_websockets=debug,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer().with_level(true))
        .init();

    let assets_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("assets");
    let state = Arc::new(AppState::new());

    // build our application with some routes
    let app = Router::new()
        .fallback_service(ServeDir::new(assets_dir).append_index_html_on_directories(true))
        .route("/ws", get(ws_handler))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::default().include_headers(true)),
        )
        .with_state(state.clone());

    let mut server_task = tokio::spawn(async move {
        let port = std::env::var("PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(8080);

        let addr = SocketAddr::from(([0, 0, 0, 0], port));

        axum::Server::bind(&addr)
            .serve(app.into_make_service_with_connect_info::<SocketAddr>())
            .await
            .unwrap();
        tracing::debug!("listening on {}", addr);
    });

    let mut jobs_task = tokio::spawn(async move { spawn_jobs(state.clone()).await });

    tokio::select! {
        _ = (&mut server_task) => {jobs_task.abort()},
        _ = (&mut jobs_task) => {server_task.abort()},
    }
}

/// The handler for the HTTP request (this gets called when the HTTP GET lands at the start
/// of websocket negotiation). After this completes, the actual switching from HTTP to
/// websocket protocol will occur.
/// This is the last point where we can extract TCP/IP metadata such as IP address of the client
/// as well as things from HTTP headers such as user-agent of the browser etc.
async fn ws_handler(
    ws: WebSocketUpgrade<ServerMsg, ClientMsg>,
    user_agent: Option<TypedHeader<headers::UserAgent>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let user_agent = if let Some(TypedHeader(user_agent)) = user_agent {
        user_agent.to_string()
    } else {
        String::from("Unknown browser")
    };
    println!("`{user_agent}` at {addr} connected.");

    ws.on_upgrade(move |socket| handle_socket(socket, addr, state))
}

/// Actual websocket statemachine (one will be spawned per connection)
async fn handle_socket(
    socket: WebSocket<ServerMsg, ClientMsg>,
    private_ip: SocketAddr,
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
            match msg {
                Ok(Message::Item(ClientMsg::RegisterDirect { addr })) => {
                    let clients: &mut Clients = &mut cloned_state_recv.clients.clone();

                    let who: SocketAddr = addr.parse().unwrap();

                    let result = clients.register_direct_async(private_ip, who).await;

                    if !result {
                        send_task_in_rcv
                            .send(Message::Item(ServerMsg::RegisterResponse {
                                success: false,
                                details: String::from("Registration failed"),
                            }))
                            .await
                            .unwrap();
                        break;
                    }

                    println!("Client {} registered for direct", who);
                    send_task_in_rcv
                        .send(Message::Item(ServerMsg::RegisterResponse {
                            success: true,
                            details: String::from("Welcome to the server!"),
                        }))
                        .await
                        .unwrap();
                }
                Ok(Message::Close(_fame)) => {
                    println!("Client {} closed connection", private_ip);
                    return;
                }
                Ok(_) => {}
                Err(err) => {
                    tracing::error!("got error: {}", err);
                }
            };
        }
    });

    let mut send_task = tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(9));

        loop {
            interval.tick().await;

            let clients: &mut Clients = &mut cloned_state_send.clients.clone();
            let current_socket = clients.get_client(private_ip).await;

            if let Some(client) = current_socket {
                if let Some(addr) = client.matched {
                    let mut is_server = false;
                    if let Some(is_srv) = client.is_server() {
                        is_server = is_srv;
                    }

                    // let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
                    // let res =
                    //     hole_punch_connect::server::make_match(&socket, addr, client.public_ip);
                    // match res {
                    //     Ok(_) => {
                    //         println!("Matched {} with {}", client.public_ip, addr);
                    //     }
                    //     Err(e) => {
                    //         println!("Error matching {} with {}: {}", client.public_ip, addr, e);
                    //     }
                    // }

                    send_task_in_send
                        .send(Message::Item(ServerMsg::Opponent {
                            opponent_addr: addr.to_string(),
                            is_server,
                            local: private_ip.to_string(),
                        }))
                        .await
                        .unwrap();

                    clients.remove_client(private_ip).await;
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

    //TODO: remove client from state

    println!("Websocket context {} destroyed", private_ip);
}
