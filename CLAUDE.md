## project

The project is a minimal rust-based Matrix homeserver which will be embedded into an Android device using UniFFI. The server is only capable of sending and receiving message / state events, meaning this project only implements a subset of the Matrix specification. The specification targeted is https://spec.matrix.org/v1.18/ - strictly only the Client-Server API and Server-Server API.

The server only targets room version 12, along with MSC4242: State DAGs https://github.com/matrix-org/matrix-spec-proposals/pull/4242 . This means the Server-Server API does not need to implement /event_auth, /state or /state_ids. EDUs and End-to-End encryption are NOT implemented, but MUST be stubbed out at the HTTP handler layer to ensure the client application functions correctly. Ruma https://github.com/ruma/ruma MUST be used. The homeserver will be running in either a trusted or untrusted network. The Client-Server API is never exposed on the network, it’s entirely embedded in the mobile device. As such, there is no need to make the Client-Server API performant or have any kind of access control. Registration and Login should be stubbed out.

## stack
- axum (routing + handlers)
- tokio (async runtime)
- serde + serde_json (serialization)
- thiserror (error types)
- uuid (id generation)
- Ruma for Matrix types
- tracing + tracing-subscriber (logging)

## crate structure
Crates are organised by scope, narrowest first: 
 - `neutrino-ctl` is the orthogonal server-wide control plane: config, host-pushed commands
 - `neutrino-event` = everything event-scoped: canonical JSON, wire format parsing, event builder, etc
 - `neutrino-room` = everything single-room-scoped: auth rules, state resolution, etc
 - `neutrino-engine` = everything multi-room-scoped: registry, per-room actor, inbound/outbound workers, anti-entropy reconciliation, gapfilling
 - `neutrino-store` - storage trait (`StorageBackend` + the fine-grained per-area store traits, `WithStateProvider`).
 - `neutrino-store-sqlite` - SQLite implementation of the storage traits (`SqliteStore`) + the SQLite-backed `StateProvider`.
 - `neutrino-http` - top-level router + C-S and S-S handlers
 - `neutrino-main` - shared server entrypoint, common between neutrino and neutrino-ffi; composes the stack and re-exports `Config`/`Command` from neutrino-ctl.
 - `neutrino-ffi` - UniFFI binding layer, calls into neutrino-main and neutrino-lb
 - `neutrino` - local development binary
 - `neutrino-lb` - Low-bandwidth bidirectional proxy - translates server-to-server HTTP + JSON requests into CoAP + CBOR

The dependency stack is a clean gradient: ctl/event (base, no internal deps) → room → engine → http → main → ffi/neutrino.

## coding rules

Before writing any code but AFTER understanding the problem, stop at the first rung that holds:

1. Does this need to be built at all? (YAGNI)
2. Does it already exist in this codebase? Reuse the helper, util, or pattern that's already here, don't re-write it.
3. Does the standard library already do this? Use it.
4. Does a native platform feature cover it? Use it.
5. Does an already-installed dependency solve it? Use it.
6. Can this be one line? Make it one line.
7. Only then: write the minimum code that works.

errors
- all errors use thiserror. no anyhow.
- all handlers return Result<Json<T>, AppError>
- AppError variants: NotFound, BadRequest, Internal. map to 404, 400, 500.
- never use .unwrap() or .expect() in handler or storage code.

storage
- handlers never touch store directly. always go through StorageBackend trait.
- sqlite layer implemented in neutrino-store-sqlite

async
- no blocking calls inside async fns. use tokio::task::spawn if needed.
- do not add unnecessary .clone() — check if a reference works first.

style
- run cargo fmt before finishing any task
- run cargo clippy and fix all warnings before finishing any task
- no dead code. no unused imports.
- keep functions short. if a handler is over ~40 lines, split it.
- keep types simple, or name them - no `Option<(String, u64, Vec<u8>, &’src PhantomData<Box<dyn Trait>>)>

## comment rules

Comments are expensive. They diverge from the code, they mislead, they have a cost. Keep them precise, concise and minimal.
For example, when describing a room version field, prefer:

> /// The version of the room this state machine is tracking

over:

> /// The version of the room this state machine is tracking — how its events are named, redacted and signed.

because the version may add extra responsibilities not tracked in the "how".

Similarly, when modifying code, do not explain _transitions_, just explain the _current final state_. For example:

```
    -    room_version   TEXT NOT NULL CHECK (room_version = 'org.matrix.msc4242.12'),
+    -- The room's version, verbatim from the create event's
+    -- `content.room_version`. Not pinned to one value: the registry
+    -- (`neutrino_event::RoomVersions`) is the authority on which versions this
+    -- build speaks, and one store may legitimately hold rooms of several (a
+    -- medium that declares its own version still reads rooms created before
+    -- the cut-over). The only structural requirement is that it is non-empty.
+    room_version   TEXT NOT NULL CHECK (room_version <> ''),
```

This entire comment is redundant because it is explaining the _transition_ from a fixed static version to multiple versions.
The correct comment is no comment at all: the SQL column is self-explanatory.

## testing

Look at any relevant unit tests in the Synapse repository https://github.com/element-hq/synapse/tree/develop/tests and port over ONLY relevant tests to Rust.

Look at any relevant Complement tests in https://github.com/matrix-org/complement and confirm that ONLY relevant tests pass.

If there are no relevant tests in either repository, ask for suggestions.

## what not to do
- do not add dependencies to Cargo.toml without asking first
- do not modify main.rs router wiring unless the task explicitly requires it
- do not create new files outside the module structure above without asking
- do not refactor working code that is not part of the current task
- do not modify CLAUDE.md
- ask before modifying tests.

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

## before declaring code done

- Deletion over addition. The best code is the code never written.
- Trivial one-liners need no test.

## before finishing any task
1. cargo fmt
2. cargo clippy -- -D warnings
3. cargo test
4. update the status checkboxes in PLAN.md
5. if a decision was made, append it to the decisions log in PLAN.md

## asking for clarification
if a task is ambiguous or conflicts with these rules, stop and ask.
do not make assumptions about intent and proceed silently.
one clarifying question is better than a wrong implementation.

## Reviewing the git diff
When asked to "review the current git diff" (or "review the diff" / "review my changes" / "review this branch"), ALWAYS delegate to fresh-context subagents.

Procedure:

- Spawn 5 general-purpose subagents in parallel (one Agent tool block each):
     - code review — correctness bugs + reuse / simplification / efficiency. Prefix findings REVIEW*.

     - security review — auth, input validation, state-res / auth-rule soundness, anything that could mask a security-relevant bug.
       Prefix findings SEC*.

     - spec-conformance review — verify behaviour against the targeted spec:
       Matrix v1.18 (Client-Server + Server-Server API), MSC4242 (State DAGs),
       room version 12. MUST consult the actual normative text (WebFetch the spec
       section / MSC, or cite it) and check the diff clause by clause: MUST/REJECT
       rules, event auth rules, wire/PDU format, redaction algorithm, state-res
       steps, M_* error codes. Flag behaviour that is plausible but not what the
       spec mandates, AND anything implemented that the spec doesn't require
       (over-implementation). Out-of-scope per this file (signatures, EDUs, E2EE,
       pagination, auth): note if the diff touches them, don't review them.
       Prefix findings SPEC*.

     - test-quality review — assess the TESTS in the diff, not production-code
       correctness. Are new tests meaningful or tautological/vacuous (an oracle
       that restates the code under test, an assertion that can't fail)? Do they
       pin the invariant the change is actually about? Is coverage adequate (happy
       path only, or error/rejection/edge paths too)? Any #[ignore]d, over-broad,
       or wrong-thing assertions? For property tests: can the generator actually
       produce the inputs the property claims to cover, or does it pass vacuously?
       Prefix findings TEST*.

     - architecture review — module boundaries, trait/type design, coupling, and
       whether the change fits the crate structure documented in this file. PLUS
       duplication & consolidation: actively search the wider codebase (rust-
       analyzer references/definition, grep) for existing code this diff duplicates
       or could delegate to — flag "this is the same as X, merge/delegate" and
       "this field/method is derivable from an existing one".
       don't duplicate; delegate to inner rather than reimplement an optimised
       method; collapse _with_X / _for_Y variant helpers into one abstraction.
       This requires reading beyond the diff. Prefix findings ARCH*.
       VERY IMPORTANT: ALSO hunt reinvented primitives,
       not just duplicated code: a poll where a signal/watch exists, a hand-rolled retry/backoff where one exists,
       a busy-loop, an O(n) scan where an indexed store method exists.
       Ask "is there existing infrastructure this mechanism should use?", not only "is this code duplicated?".
       Prefix these ARCH-MECH*. Project rule: derive. If none, SAY SO.

- Give every subagent: the scope (`git diff HEAD` unless told otherwise — they run it themselves), the "Code Review" rubric above, and these constraints: report EVERY finding numbered (with subagent prefix) with confidence (low/medium/high/certain) + severity (nit/minor/major/critical); read surrounding source to verify claims; do NOT edit files; return findings as the final message.
- On return, synthesize: filter/rank, drop false positives (state WHY), present the merged list. Do NOT apply any fix without asking first.

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