#[tokio::main]
async fn main() {
    tf_ex_matchmaking_server::init_tracing();
    tf_ex_matchmaking_server::run(std::sync::Arc::new(
        tf_ex_matchmaking_server::ranking::NoRanking,
    ))
    .await;
}
