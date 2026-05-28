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

## before starting any non-trivial task

If the task touches more than one file, do a scope-scan before the first edit.

A scope-scan is one read-only command that surfaces the full shape of the work:
- behavioural change to a function used elsewhere → `mcp__rust-analyzer__references <symbol>`
- adding an assertion or stricter check → `cargo test --workspace --no-fail-fast` to see every failing target at once
- helper-signature refactor → `references` on the helper, plus its callers' files
- non-symbol pattern (string, JSON shape) → `grep -rn` once across `crates/`

Run the scan ONCE upfront, not after the first failure surfaces. Acting on one
failure at a time hides the fan-out structure and forces sequential work where
parallel was possible.

After the scan, decide strategy:
- 1 unit of work: just do it.
- 2+ units, independent (different crates, no shared edits): fire one `Agent`
  call per unit in a SINGLE message, multiple tool blocks. **Sequential
  delegation on independent work is a code smell.**
- 2+ units, coupled (changes ripple between them): sequential, but commit
  to an order before starting the first one.

For mechanical rewrites across many call sites (>20), write a Python rewriter
with `find_matching_paren` + arg-aware splitting, designed up-front for
trailing commas, inline comments, and multi-line calls. Half-baked scripts that
need 3 iterations cost more than 5 extra min of robust design.

## before declaring code done

after the test suite passes, before saying "done", spend 60 seconds auditing the design:

- every new struct field: is it derivable from another field? if yes, drop it
- every new method that wraps/adapts existing code: does inner have an optimised version i'm bypassing? if yes, swap-in-the-override-case and delegate
- every "i know this is slower but…" comment in the diff: turn it into a question — is the slow version actually necessary, or am i avoiding 30 more seconds of thought?
- every helper with a _with_X, _for_new_Y, _lazy_Z suffix: am i papering over a design hole? could a wrapper / different abstraction collapse the variant?
- the diff summary: any new struct that grew past one field, any new fn over ~30 lines — what would i delete if i had to halve the diff?
- this is not "is the code correct" — cargo test does that. this is "is the code actually good".

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
For each finding, include your confidence level (low, medium, high, certain) and an estimated severity (nit, minor, major, critical) so a downstream filter can rank them.
Number each issue (I1, I2, I3, ...) so they can be referred to downstream.

Reviewing code may be supplied via patch/diff files.


## rust-analyzer (LSP)
For non-trivial work, prefer the `mcp__rust-analyzer__*` tools over manual
grep / `cargo build` / file-reading:

- `references` — impact analysis. Before changing a public symbol's signature
  (helpers, trait methods, anything `pub`/`pub(crate)`), run `references`
  to enumerate real call sites. Don't grep for `foo(` — too many false
  positives from comments/strings/macros.
- `definition` — navigate to a symbol's declaration, including across
  crate boundaries and through re-exports. Use this instead of grep + Read.
- `hover` — type + docs on any symbol. First port of call when meeting a
  new type or unfamiliar trait method.
- `diagnostics` — type-check feedback for a single file. Drives the inner
  edit loop. `cargo build` / `cargo test` are for final verification only.
- `edit_file` — multi-edit atomic writes. Use when applying ≥3 edits to
  the same file in one logical change.
- `rename_symbol` — workspace-wide rename. One call replaces N file edits.

Reserve grep / `cargo build` / Read for: non-source files (TOML, MD, JSON),
generated code, situations where rust-analyzer is unavailable, or when
investigating raw bytes (e.g. tests that pin a literal string).

## Iteration loops
Default to per-crate during development:
- `cargo test -p <crate>` instead of `--workspace`
- `cargo clippy -p <crate> --tests -- -D warnings` instead of `--workspace --all-targets`

Workspace-wide builds are 5-10× slower; reserve them for: final verification
before declaring a task done, cross-crate refactors where one crate's
signature change ripples through another, or when explicitly asked for a
"full check".

## Parallelisation
When a task fans out across independent crates / modules (e.g., updating
test fixtures in each of `neutrino-state`, `neutrino-http`,
`neutrino-store-sqlite`), run subagents in parallel — single message,
multiple `Agent` tool calls. Sequential subagents on independent work
costs ~3-4× wall-clock for no benefit.

## Scope triage before refactors
Any task that touches multiple call sites: first action is `references`
(or `grep -c` if the symbol isn't LSP-known yet) to count. Then decide:
- 1-5 sites: hand-edit
- 5-20 sites: hand-edit per file, or `rename_symbol` if it's a rename
- 20+ sites: write a mechanical rewriter (Python script with `find_matching_paren` + arg-aware `split_args`, designed up-front for
  trailing commas, inline comments, multi-line calls) or delegate to a
  subagent with a clear pattern spec.

## Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.