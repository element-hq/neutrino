pub mod storage;

const DEFAULT_BIND_ADDR: &str = "0.0.0.0:8008";
const DEFAULT_SERVER_NAME: &str = "localhost";
const DEFAULT_LOCALPART: &str = "alice";

#[derive(Debug, Clone)]
pub struct Config {
    pub server_name: String,
    pub bind_addr: String,
    pub localpart: String,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            server_name: std::env::var("NEUTRINO_SERVER_NAME")
                .unwrap_or_else(|_| DEFAULT_SERVER_NAME.to_string()),
            bind_addr: std::env::var("NEUTRINO_BIND_ADDR")
                .unwrap_or_else(|_| DEFAULT_BIND_ADDR.to_string()),
            localpart: DEFAULT_LOCALPART.to_string(),
        }
    }

    pub fn user_id(&self) -> String {
        format!("@{}:{}", self.localpart, self.server_name)
    }
}
