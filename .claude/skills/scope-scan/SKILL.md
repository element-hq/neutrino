---
name: scope-scan
description: Size a multi-file change before the first edit — one read-only scan to find every affected site, then choose hand-edit / rename / rewriter / parallel subagents. Use before any task that touches more than one file, changes a symbol's signature or behaviour, or tightens a check.
---

## The scan

Run ONE read-only command that surfaces the full shape of the work, upfront — not after the first failure:

- behavioural change to a function used elsewhere → `mcp__rust-analyzer__references <symbol>`
- adding an assertion or stricter check → `cargo test --workspace --no-fail-fast` to see every failing target at once
- helper-signature refactor → `references` on the helper, plus its callers' files
- non-symbol pattern (string, JSON shape) → `grep -rn` once across `crates/`

Acting on one failure at a time hides the fan-out and forces sequential work where parallel was possible.

## Strategy

- 1 unit of work: just do it.
- 2+ units, independent (different crates, no shared edits): one `Agent` call per unit in a SINGLE message. Sequential delegation on independent work is a code smell.
- 2+ units, coupled (changes ripple between them): sequential, but commit to an order before the first one.

By call-site count:
- 1-5 sites: hand-edit.
- 5-20 sites: hand-edit per file, or `rename_symbol` if it's a rename.
- 20+ sites: a mechanical rewriter (Python with `find_matching_paren` + arg-aware `split_args`, designed up-front for trailing commas, inline comments, multi-line calls) or a subagent with a clear pattern spec.

## rust-analyzer tools

Prefer the `mcp__rust-analyzer__*` tools (when configured) over grep / `cargo build` / reading files:

- `references` — impact analysis before changing any `pub`/`pub(crate)` symbol. Don't grep for `foo(`: comments, strings and macros give false positives.
- `definition` — jump to a declaration, across crates and re-exports.
- `hover` — type + docs; first stop for an unfamiliar symbol.
- `diagnostics` — per-file type-check for the inner edit loop; `cargo build` / `cargo test` are for final verification.
- `edit_file` — atomic multi-edit when applying ≥3 edits to one file.
- `rename_symbol` — workspace-wide rename in one call.

Use grep / Read for non-Rust files (TOML, MD, JSON, SQL), generated code, raw-byte tests, or when rust-analyzer is unavailable.
