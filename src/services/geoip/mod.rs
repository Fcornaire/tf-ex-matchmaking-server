use std::net::IpAddr;

use base64::{engine::general_purpose, Engine as _};
use hyper::{client::HttpConnector, Body, Client, Method, Request, StatusCode};
use hyper_tls::HttpsConnector;

use crate::ARGS;

use self::response::GeoipAPICityResponse;

mod response;

#[derive(Debug, Default, Clone)]
pub struct Location {
    latitude: f64,
    longitude: f64,
    country_code: String,
}

impl Location {
    pub fn new(latitude: f64, longitude: f64, country_code: String) -> Self {
        Self {
            latitude,
            longitude,
            country_code,
        }
    }

    pub fn get_latitude(&self) -> f64 {
        self.latitude
    }

    pub fn get_longitude(&self) -> f64 {
        self.longitude
    }

    pub fn get_country_code(&self) -> String {
        self.country_code.clone()
    }

    fn to_geoutils_location(&self) -> geoutils::Location {
        geoutils::Location::new(self.latitude, self.longitude)
    }

    pub fn distance_to(&self, other: Location) -> f64 {
        let current = self.to_geoutils_location();
        let other = other.to_geoutils_location();

        current.haversine_distance_to(&other).meters()
    }

    pub fn is_near(&self, other: Location, max_distance: f64) -> bool {
        let distance = self.distance_to(other);

        distance <= max_distance
    }
}

#[derive(Debug, Clone)]
pub struct GeoipClient {
    client: Client<HttpsConnector<HttpConnector>>,
}

impl GeoipClient {
    pub fn new() -> Self {
        let https = HttpsConnector::new();
        let client = Client::builder().build::<_, hyper::Body>(https);

        Self { client }
    }

    pub async fn get_location(&self, ip: IpAddr) -> Result<Location, String> {
        let Some((username, password)) = ARGS.geoip_credentials() else {
            return Ok(Location::default());
        };

        let url = format!("https://geolite.info/geoip/v2.1/city/{}", ip.to_string());

        let authorization = format!("{username}:{password}");

        let encoded: String = general_purpose::STANDARD.encode(authorization);

        let req = Request::builder()
            .method(Method::GET)
            .uri(url)
            .header("Authorization", format!("Basic {}", encoded))
            .body(Body::empty())
            .unwrap();

        let req = self.client.request(req).await;

        if let Ok(res) = req {
            if res.status() != StatusCode::OK {
                let body = hyper::body::to_bytes(res.into_body()).await.unwrap();
                let body = String::from_utf8(body.to_vec()).unwrap();

                if body.contains("IP_ADDRESS_RESERVED") {
                    //Propably a local ip
                    return Ok(Location::default());
                }

                let err = format!("Error while getting location from ip {} : {}", ip, body);

                tracing::error!(err);

                return Err(err);
            }

            let body = hyper::body::to_bytes(res.into_body()).await.unwrap();
            let body = String::from_utf8(body.to_vec()).unwrap();
            let body: GeoipAPICityResponse = serde_json::from_str(&body).unwrap();

            let latitude = body.location.latitude;
            let longitude = body.location.longitude;
            let country_code = body.country.iso_code.as_str();

            return Ok(Location::new(latitude, longitude, country_code.to_string()));
        }

        tracing::warn!("Failed to find location.Fallback to default...");

        Ok(Location::default())
    }
}
