## project

The project is a minimal rust-based Matrix homeserver which will be embedded into an Android device using UniFFI. The server is only capable of sending and receiving message / state events, meaning this project only implements a subset of the Matrix specification. The specification targeted is https://spec.matrix.org/v1.18/ - strictly only the Client-Server API and Server-Server API.

The server only targets room version 12, along with MSC4242: State DAGs https://github.com/matrix-org/matrix-spec-proposals/pull/4242 . This means the Server-Server API does not need to implement /event_auth, /state or /state_ids. EDUs and End-to-End encryption are NOT implemented, but MUST be stubbed out at the HTTP handler layer to ensure the client application functions correctly. Ruma https://github.com/ruma/ruma MUST be used. The homeserver will be running in a trusted network. This means events MUST NOT have signatures and signature checks should not be run, which means servers DO NOT need a server signing key. The Client-Server API is never exposed on the network, it’s entirely embedded in the mobile device. As such, there is no need to make the Client-Server API performant or have any kind of access control. Registration and Login should be stubbed out.

see PLAN.md for current status and task breakdown.
read PLAN.md at the start of every session before doing anything else. 


## stack
- axum (routing + handlers)
- tokio (async runtime)
- serde + serde_json (serialization)
- thiserror (error types)
- uuid (id generation)
- Ruma for Matrix types
- tracing + tracing-subscriber (logging)
## crate structure - keep big dependencies separate (namely UniFFI) to improve compile times and avoid rebuilds, etc.
neutrino - local development binary
neutrino-main - server entrypoint, common between neutrino and neutrino-ffi
neutrino-http - top-level router, could potentially be split into c2s and s2s APIs in the future
neutrino-store - storage trait
neutrino-store-sqlite - SQLite implementation of storage trait
neutrino-common - common type definitions
neutrino-ffi - UniFFI binding layer, calls into neutrino-main and neutrino-lb
neutrino-lb - Low-bandwidth bidirectional proxy - translates server-to-server HTTP + JSON requests into CoAP + CBOR (CBOR could be done in HTTP layer?) - see MSC3079 https://github.com/matrix-org/matrix-spec-proposals/blob/kegan/low-bandwidth/proposals/3079-low-bandwidth-csapi.md 

## coding rules
errors
- all errors use thiserror. no anyhow.
- all handlers return Result<Json<T>, AppError>
- AppError variants: NotFound, BadRequest, Internal. map to 404, 400, 500.
- never use .unwrap() or .expect() in handler or storage code.
-	 potentially `#![deny(clippy::unwrap_used)] in handler crate to enforce this

storage
- handlers never touch store directly. always go through StorageBackend trait.
- sqlite layer implemented in neutrino-store-sqlite
- do not introduce any other database dependency without explicitly being asked to.

async
- no blocking calls inside async fns. use tokio::task::spawn if needed.
- do not add unnecessary .clone() — check if a reference works first.

style
- run cargo fmt before finishing any task
- run cargo clippy and fix all warnings before finishing any task
- no dead code. no unused imports.
- keep functions short. if a handler is over ~40 lines, split it.
- keep types simple, or name them - no `Option<(String, u64, Vec<u8>, &’src PhantomData<Box<dyn Trait>>)>

## testing

Look at any relevant unit tests in the Synapse repository https://github.com/element-hq/synapse/tree/develop/tests and port over ONLY relevant tests to Rust.

Look at any relevant Complement tests in https://github.com/matrix-org/complement and confirm that ONLY relevant tests pass.

If there are no relevant tests in either repository, ask for suggestions.

## what not to do
- do not add dependencies to Cargo.toml without asking first
- do not modify main.rs router wiring unless the task explicitly requires it
- do not implement pagination, auth, or rate limiting — see PLAN.md non-goals
- do not create new files outside the module structure above without asking
- do not refactor working code that is not part of the current task
- do not modify CLAUDE.md
- do not erase lines in LOG.md
- do not delete tests. Ask before modifying tests.

## before finishing any task
1. cargo fmt
2. cargo clippy -- -D warnings
3. cargo test
4. update the status checkboxes in PLAN.md
5. if a decision was made, append it to the decisions log in PLAN.md
6. Append a 2-line summary of your change to LOG.md 

## asking for clarification
if a task is ambiguous or conflicts with these rules, stop and ask.
do not make assumptions about intent and proceed silently.
one clarifying question is better than a wrong implementation.

## Code Review

Report every issue you find, including ones you are uncertain about or consider low-severity.
Do not filter for importance or confidence at this stage - a separate verification step will do that.
Your goal here is coverage: it is better to surface a finding that later gets filtered out than to silently drop a real bug.
For each finding, include your confidence level and an estimated severity so a downstream filter can rank them.

Reviewing code may be supplied via patch/diff files.