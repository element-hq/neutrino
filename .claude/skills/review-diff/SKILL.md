---
name: review-diff
description: Review the current git diff (or a branch) with five fresh-context reviewers — code, security, spec conformance, test quality, architecture — then synthesise one ranked list. Use when asked to "review the current git diff", "review the diff", "review my changes" or "review this branch".
---

Always delegate to fresh-context subagents; never review inline.

Spawn 5 general-purpose subagents in parallel (one Agent tool block each, in a single message):

- **code review** — correctness bugs + reuse / simplification / efficiency. Prefix findings REVIEW*.

- **security review** — auth, input validation, state-res / auth-rule soundness, signed-mode signature and `X-Matrix` handling, anything that could mask a security-relevant bug. Prefix findings SEC*.

- **spec-conformance review** — verify behaviour against the targeted spec: Matrix v1.16 (Client-Server + Server-Server API), MSC4242 (State DAGs), room version 12. MUST consult the actual normative text (WebFetch the spec section / MSC, or cite it) and check the diff clause by clause: MUST/REJECT rules, event auth rules, wire/PDU format, redaction algorithm, state-res steps, M_* error codes. Flag behaviour that is plausible but not what the spec mandates, AND anything implemented that the spec doesn't require (over-implementation). Out of scope per CLAUDE.md (EDUs, E2EE, C-S auth/registration — all stubbed): note if the diff touches them, don't review them. Prefix findings SPEC*.

- **test-quality review** — assess the TESTS in the diff, not production-code correctness. Are new tests meaningful or tautological/vacuous (an oracle that restates the code under test, an assertion that can't fail)? Do they pin the invariant the change is actually about? Is coverage adequate (happy path only, or error/rejection/edge paths too)? Any `#[ignore]`d, over-broad, or wrong-thing assertions? For property tests: can the generator actually produce the inputs the property claims to cover, or does it pass vacuously? Prefix findings TEST*.

- **architecture review** — module boundaries, trait/type design, coupling, and whether the change fits the crate relationships and cross-crate invariants in ARCHITECTURE.md. PLUS duplication & consolidation: actively search the wider codebase (rust-analyzer references/definition, grep) for existing code this diff duplicates or could delegate to — flag "this is the same as X, merge/delegate" and "this field/method is derivable from an existing one". Don't duplicate; delegate to inner rather than reimplement an optimised method; collapse `_with_X` / `_for_Y` variant helpers into one abstraction. This requires reading beyond the diff. Prefix findings ARCH*.
  VERY IMPORTANT: also hunt reinvented primitives, not just duplicated code: a poll where a signal/watch exists, a hand-rolled retry/backoff where one exists, a busy-loop, an O(n) scan where an indexed store method exists. Ask "is there existing infrastructure this mechanism should use?", not only "is this code duplicated?". Prefix these ARCH-MECH*. Project rule: derive. If none, SAY SO.

Give every subagent:
- the scope: `git diff HEAD` unless told otherwise — they run it themselves;
- its own dimension's description above, verbatim;
- these constraints: report EVERY finding numbered (with the subagent prefix) with confidence (low/medium/high/certain) + severity (nit/minor/major/critical); read surrounding source to verify claims; do NOT edit files; return findings as the final message.

On return, synthesise: filter and rank, drop false positives (state WHY), present the merged list. Do NOT apply any fix without asking first.
