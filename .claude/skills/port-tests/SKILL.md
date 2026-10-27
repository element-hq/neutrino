---
name: port-tests
description: Find and port the relevant Synapse unit tests and Complement tests for a feature or fix. Use when adding or changing server behaviour that the Matrix reference implementations already test.
---

1. **Synapse unit tests** — search https://github.com/element-hq/synapse/tree/develop/tests for tests covering the behaviour. Port ONLY the relevant ones to Rust, next to the code under test (or under the crate's `tests/` for cross-crate behaviour). Port the assertion, not Synapse's harness.

2. **Complement** — search https://github.com/matrix-org/complement for tests covering the behaviour and confirm the relevant ones pass:
   - Run: `cargo xtask complement` (builds the image from `docker/complement/`, then runs each `complement/allowlist.txt` entry as its own `go test -run`). `cargo xtask complement -run <Name>` runs one test, bypassing the allowlist.
   - When a test now passes, add it to `complement/allowlist.txt`.

3. If neither repository has relevant tests, ask the user for suggestions rather than inventing coverage.

Ask before modifying existing tests.
