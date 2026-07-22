# xtask

Neutrino uses the [`xtask` pattern](https://github.com/matklad/cargo-xtask) for the developer
tasks that don't fit a plain `cargo` invocation. Run `cargo xtask <command>` from the workspace
root.

Android artifact building and publishing (the `.aar` and its Kotlin bindings) live in the
[`neutrino-iroh`](https://github.com/element-hq/neutrino-iroh) repository, which composes this crate
over its iroh/BLE federation medium and produces the consumer artifact. This crate is built from
source there; there is no Android tooling here.

## `complement`

Runs the [Complement](https://github.com/matrix-org/complement) suite against the
`neutrino:complement` Docker image (built on demand, unless `SKIP_IMAGE_BUILD` is set and the
image already exists). By default it runs each entry in `complement/allowlist.txt` as its own
`go test -run`, aggregating the results. Flags:

- `--in-repo` runs the in-repo neutrino-specific tests under `complement/tests/...` instead of the
  allowlist.
- trailing args are forwarded verbatim to `go test` (e.g. `cargo xtask complement -- -run TestFoo
  -v`); an explicit `-run` bypasses the allowlist for debugging a single test.
