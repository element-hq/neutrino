//! Client-Server API HTTP handlers.
//!
//! Every endpoint the embedded client reaches lives under here, grouped by
//! concern: [`stubs`] (login/register/capabilities and other canned
//! responses), [`keys`] (E2EE key stubs), [`rooms`] (the real createRoom /
//! members / send handlers), and [`sync`] (the MSC4186 sliding-sync HTTP edge
//! plus the legacy `/sync` translation in [`legacy_sync`], both backed by the
//! [`sliding_sync`] engine).
//!
//! Each submodule owns its routes via a `pub(crate) fn routes() ->
//! Router<AppState>`; [`routes`] merges them into the client half of the
//! router. The federation half is assembled separately in
//! `crate::federation::routes`.

use axum::Router;
use neutrino_common::Config;

use crate::AppState;

pub(crate) mod keys;
pub(crate) mod legacy_sync;
pub(crate) mod rooms;
pub(crate) mod sliding_sync;
pub(crate) mod stubs;
pub(crate) mod sync;

/// All Client-Server API routes, merged into one sub-router.
pub(crate) fn routes(config: &Config) -> Router<AppState> {
    Router::new()
        .merge(stubs::routes(config))
        .merge(keys::routes())
        .merge(rooms::routes())
        .merge(sync::routes())
}
