//! Key resolution for named peers, over `/_matrix/key/v2/server`.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;

use neutrino_ctl::Config;
use neutrino_event::{KeyResolveError, KeyResolver, verify_key_response};
use ruma::ServerName;

use super::client::FederationClient;

/// [`KeyResolver`] for DNS/IP-named peers (the dev binary, the Complement
/// image): `(server_name, key_id)` is answered by the peer's own
/// `/_matrix/key/v2/server`, fetched over the route outbound federation takes
/// (direct, or via `federation_proxy`) and checked against its self-signature.
/// Resolved keys are cached for the process lifetime: this deployment's own
/// key never rotates, and a peer rotating keys is a new key id, not a new
/// value under an old one.
pub struct HttpKeyResolver {
    client: FederationClient,
    cache: Mutex<HashMap<(String, String), [u8; 32]>>,
}

impl HttpKeyResolver {
    pub fn new(config: &Config) -> Self {
        Self {
            client: FederationClient::new(
                config.server_name.clone(),
                config.federation_proxy.as_deref(),
            ),
            cache: Mutex::default(),
        }
    }

    fn cached(&self, server_name: &str, key_id: &str) -> Option<[u8; 32]> {
        self.cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&(server_name.to_owned(), key_id.to_owned()))
            .copied()
    }
}

impl KeyResolver for HttpKeyResolver {
    fn verify_key<'a>(
        &'a self,
        server_name: &'a str,
        key_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<[u8; 32], KeyResolveError>> + Send + 'a>> {
        Box::pin(async move {
            if let Some(key) = self.cached(server_name, key_id) {
                return Ok(key);
            }
            let fail = |reason: String| KeyResolveError {
                server_name: server_name.to_owned(),
                key_id: key_id.to_owned(),
                reason,
            };
            let dest = <&ServerName>::try_from(server_name)
                .map_err(|e| fail(format!("not a valid server name: {e}")))?;
            let response = self
                .client
                .server_keys(dest)
                .await
                .map_err(|e| fail(e.to_string()))?;
            let key = verify_key_response(&response, server_name, key_id).map_err(fail)?;
            self.cache
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert((server_name.to_owned(), key_id.to_owned()), key);
            Ok(key)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::{Json, Router, routing::get};
    use neutrino_event::event_id::b64_unpadded;
    use neutrino_event::{EventSigner, SIGNING_KEY_ID};
    use ruma::canonical_json::CanonicalJsonValue;
    use serde_json::{Value, json};

    use super::*;
    use crate::federation::test_support::spawn_stub;

    /// A stub whose `/_matrix/key/v2/server` body is whatever `body` holds
    /// at request time (the signer needs the stub's name, so the body is
    /// filled in after binding).
    async fn key_stub() -> (ruma::OwnedServerName, Arc<Mutex<Value>>) {
        let body = Arc::new(Mutex::new(Value::Null));
        let served = body.clone();
        let app = Router::new().route(
            "/_matrix/key/v2/server",
            get(move || {
                let served = served.clone();
                async move { Json(served.lock().unwrap().clone()) }
            }),
        );
        (spawn_stub(app).await, body)
    }

    fn signed_key_response(signer: &EventSigner, server_name: &str) -> Value {
        let CanonicalJsonValue::Object(mut obj) = CanonicalJsonValue::try_from(json!({
            "server_name": server_name,
            "valid_until_ts": 1,
            "verify_keys": { SIGNING_KEY_ID: { "key": b64_unpadded(&signer.public_key()) } },
            "old_verify_keys": {},
        }))
        .unwrap() else {
            unreachable!()
        };
        signer.sign_json(&mut obj);
        serde_json::to_value(CanonicalJsonValue::Object(obj)).unwrap()
    }

    #[tokio::test]
    async fn fetches_verifies_and_caches_the_peer_key() {
        let (dest, body) = key_stub().await;
        let signer = EventSigner::new(&[3u8; 32], dest.as_str());
        *body.lock().unwrap() = signed_key_response(&signer, dest.as_str());

        let resolver = HttpKeyResolver::new(&Config {
            server_name: "local.test".to_owned(),
            ..Config::default()
        });
        let key = resolver
            .verify_key(dest.as_str(), SIGNING_KEY_ID)
            .await
            .expect("resolves");
        assert_eq!(key, signer.public_key());

        // Cached: the stub can go bad without affecting the answer.
        *body.lock().unwrap() = json!({ "garbage": true });
        let again = resolver
            .verify_key(dest.as_str(), SIGNING_KEY_ID)
            .await
            .expect("cached");
        assert_eq!(again, key);
        // But an unknown key id is a fresh fetch, and the bad body fails it.
        assert!(
            resolver
                .verify_key(dest.as_str(), "ed25519:2")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn rejects_a_response_naming_another_server() {
        let (dest, body) = key_stub().await;
        let signer = EventSigner::new(&[4u8; 32], "impostor");
        *body.lock().unwrap() = signed_key_response(&signer, "impostor");
        let resolver = HttpKeyResolver::new(&Config::default());
        let err = resolver
            .verify_key(dest.as_str(), SIGNING_KEY_ID)
            .await
            .unwrap_err();
        assert!(err.reason.contains("does not name"), "{}", err.reason);
    }
}
