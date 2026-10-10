pub mod matchmaking;
pub mod signaling;

use axum::{
    http::{header::ORIGIN, Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use tracing::info;

pub async fn refuse_browsers<B>(request: Request<B>, next: Next<B>) -> Response {
    if let Some(origin) = request.headers().get(ORIGIN) {
        info!(
            "Refusing browser connection to {} from origin {:?}",
            request.uri().path(),
            origin
        );
        return StatusCode::FORBIDDEN.into_response();
    }

    next.run(request).await
}
