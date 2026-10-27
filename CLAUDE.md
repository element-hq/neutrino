## project

A minimal Rust Matrix homeserver, embedded in Android via UniFFI. It sends and receives message and state events — a subset of the Client-Server and Server-Server APIs of https://spec.matrix.org/v1.16/ and nothing else.

- Room version 12 with MSC4242: State DAGs (https://github.com/matrix-org/matrix-spec-proposals/pull/4242), wire id `org.matrix.msc4242.12`. A federation medium may declare one extra room version at startup. No `/event_auth`, `/state` or `/state_ids`.
- Ruma MUST be used.
- EDUs and E2EE are not implemented, but MUST be stubbed at the HTTP handler layer so the client works. Registration and login are stubbed.
- Runs on a trusted network (unsigned events, the default) or an untrusted one (signed mode, `Config.trusted_network = false`).
- The Client-Server API is for the on-device client only: no access control, no performance work.
- Non-goals: rate limiting, distributed tracing (HTTP access logging is fine), restricted rooms.

How the crates fit together, and the invariants between them: ARCHITECTURE.md. Read it before changing anything that crosses a crate boundary.

## coding rules

Before writing any code but AFTER understanding the problem, stop at the first rung that holds:

1. Does this need to be built at all? (YAGNI)
2. Does it already exist in this codebase? Reuse the helper, util, or pattern that's already here.
3. Does the standard library already do this? Use it.
4. Does a native platform feature cover it? Use it.
5. Does an already-installed dependency solve it? Use it.
6. Can this be one line? Make it one line.
7. Only then: write the minimum code that works.

Deletion over addition. Trivial one-liners need no test.

- Errors use thiserror; no anyhow.
- Never `.unwrap()` or `.expect()` in handler or storage code.
- Handlers go through the storage traits (`neutrino-store`), never a concrete store.
- No blocking calls inside async fns; use `tokio::task::spawn_blocking`.
- Don't `.clone()` where a reference works.
- Name complex types rather than nesting tuples and generics.

## comment rules

Comments are expensive: they diverge from the code and mislead. Write one only for what the code can't say — a reason, a constraint, an invariant. Keep it precise and minimal.

- Don't enumerate responsibilities that may grow: prefer `/// The version of the room this state machine is tracking` over `… — how its events are named, redacted and signed.`
- Describe the current state, never the transition. No "previously", "now", "no longer", "moved from"; no plan, phase or task labels. If a comment only explains why the code changed, it belongs in the commit message.
- Don't cite CLAUDE.md or other docs from code; state the fact.

## changes

- Touch only what the task needs. Don't improve adjacent code, comments or formatting; match the existing style.
- Remove what your change orphaned; mention unrelated dead code rather than deleting it.
- Turn the task into a verifiable goal (a failing test, a passing check) and loop until it holds.
- If a task is ambiguous or conflicts with these rules, stop and ask. One question beats a wrong implementation.
- Before a multi-file change, run the `scope-scan` skill.
- For new behaviour, run the `port-tests` skill to find Synapse / Complement tests to port.
- "Review the diff" → the `review-diff` skill.

Ask first before:
- adding a dependency;
- creating files outside the existing crate layout;
- modifying existing tests;
- modifying CLAUDE.md.

## workflow

During development, scope to one crate: `cargo test -p <crate>`, `cargo clippy -p <crate> --tests -- -D warnings`. Workspace-wide runs are 5-10× slower; use them for the final check or cross-crate changes.

Before finishing any task:
1. `cargo fmt`
2. `cargo clippy -- -D warnings`
3. `cargo test` for every crate touched
