use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct GeoipAPICountryResponse {
    pub iso_code: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GeoipAPILocationResponse {
    pub latitude: f64,
    pub longitude: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GeoipAPICityResponse {
    pub country: GeoipAPICountryResponse,
    pub location: GeoipAPILocationResponse,
}
