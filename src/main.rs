use axum::{
    extract::{State, TypedHeader},
    response::IntoResponse,
    routing::get,
    Router,
};
use msg::{ClientMsg, ServerMsg};

use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use tower_http::{
    services::ServeDir,
    trace::{DefaultMakeSpan, TraceLayer},
};

use axum_typed_websockets::{Message, WebSocket, WebSocketUpgrade};

use crate::state::{AppState, DirectClientsExts};

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

    let mut server = tokio::spawn(async move {
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

    let mut jobs = tokio::spawn(async move {
        if let Err(join_error) = spawn_jobs(state.clone()).await {
            println!("Error spawning jobs: {:?}", join_error);
            return;
        }
    });

    tokio::select! {
        _ = (&mut server) => {},
        _ = (&mut jobs) => {},
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
    mut socket: WebSocket<ServerMsg, ClientMsg>,
    who: SocketAddr,
    state: Arc<AppState>,
) {
    while let Some(msg) = socket.recv().await {
        return match msg {
            Ok(Message::Item(ClientMsg::RegisterDirect)) => {
                let registered = &mut state.clients_registered_for_direct.clone();

                let result = registered.register_direct_async(who).await;

                if !result {
                    socket
                        .send(Message::Item(ServerMsg::RegisterResponse {
                            success: false,
                            details: String::from("Registration failed"),
                        }))
                        .await
                        .unwrap();
                    break;
                }

                println!("Client {} registered for direct", who);
                socket
                    .send(Message::Item(ServerMsg::RegisterResponse {
                        success: true,
                        details: String::from("Welcome to the server!"),
                    }))
                    .await
                    .unwrap();
            }
            Ok(_) => {}
            Err(err) => {
                tracing::error!("got error: {}", err);
            }
        };
    }

    println!("Websocket context {} destroyed", who);
}
