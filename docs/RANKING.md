# Adding your own ranking

The server ships without ranking. Ratings, the ranked queue, the leaderboard and player profiles all go through the `Ranking` trait

## Setting up a crate

Make a binary crate that depends on this one:

```toml
[dependencies]
tf-ex-matchmaking-server = { git = "https://github.com/Fcornaire/tf-ex-matchmaking-server", branch = "main" }
async-trait = "0.1"
tokio = { version = "1", features = ["full"] }
```

Then start the server with your implementation:

```rust
#[tokio::main]
async fn main() {
    tf_ex_matchmaking_server::init_tracing();
    tf_ex_matchmaking_server::run(Arc::new(MyRanking::new())).await;
}
```

Every trait method has a default, so you only override what you need

## The hooks

| Method                                 | Called when                                                                             |
| -------------------------------------- | --------------------------------------------------------------------------------------- |
| `describe_metrics`                     | Once at startup, after the metrics recorder is installed                                |
| `run_jobs`                             | Once at startup, beside the core background jobs                                        |
| `publish_metrics`                      | Every time the core publishes its gauges                                                |
| `on_identified`                        | A player sends `Identify` with their player id                                          |
| `enter_ranked`                         | A player asks to join the ranked queue                                                  |
| `exit_ranked`                          | A player leaves the ranked queue                                                        |
| `leaderboard` / `player_profile`       | A player opens the leaderboard or a profile                                             |
| `on_match_reported`                    | A match finished in any lobby kind                                                      |
| `settle_departure` / `after_departure` | A player leaves a lobby or disconnects from it (`Departure::LinkLost`: game link lost)  |
| `note_connection_lost`                 | A player's connection times out (missed keep alives), before their departure is handled |
| `is_lobby_update_held`                 | Before a lobby update is broadcast, return `true` to hold                               |
| `is_series_start_held`                 | Before the next game of a series starts, return `true` to hold                          |
| `on_drain_start`                       | The server is stopping and starts draining current games                                |
