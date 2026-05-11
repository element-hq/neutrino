# Complement integration

This directory holds neutrino's integration with the
[Matrix Complement](https://github.com/matrix-org/complement) compliance suite.

## Layout

- `go.mod`/`go.sum` — declare an in-repo Go module that pins the complement
  framework version. Used both by `tests/` and by `scripts/complement.sh` (via
  `setup-go`'s `go-version-file` in CI) as the source of truth for the
  complement version.
- `tests/` — neutrino-specific complement tests (currently just
  `complement.TestMain` scaffolding; project-specific tests can be added here).
- `allowlist.txt` — curated list of upstream complement test names that we
  expect to pass. One test (or regex fragment) per line; lines starting with
  `#` are ignored. Joined with `|` into a `go test -run` expression by the
  runner script.

## Scope

- Only the curated **client-server** tests in `allowlist.txt` run by default.
- Federation is **out of scope**. Nginx in the container terminates TLS on
  `:8448` so complement is happy, but no federation traffic is implemented.
- The allowlist grows test-by-test as endpoints land. A failing allowlisted
  test means either fix neutrino or remove the test from the allowlist with a
  rationale.

## Running

```sh
# Build the image, fetch matrix-org/complement@main into ./complement-main/,
# and run the allowlist.
bash scripts/complement.sh

# Override the complement version (any branch, tag, or commit).
COMPLEMENT_REF=v0.x bash scripts/complement.sh

# Use an existing complement checkout instead of the project-local fetch.
COMPLEMENT_DIR=/path/to/complement bash scripts/complement.sh

# Run a specific upstream test, ignoring the allowlist.
bash scripts/complement.sh -run TestVersionStructure

# Run neutrino's in-repo tests (under ./complement/tests/...).
bash scripts/complement.sh --in-repo
```

The script:

1. Builds `neutrino:complement` from `docker/complement/Dockerfile`.
2. For upstream tests: fetches a complement tarball if needed, joins the
   allowlist into a `go test -run` regex, and invokes
   `go test ./tests/csapi/...` against the built image.
3. For `--in-repo` tests: invokes `go test ./tests/...` from this directory.

## Allowlist policy

- One test (or regex fragment) per line. Lines starting with `#` are ignored.
- Add tests as the endpoints they exercise become implemented.
- Prefer specific `Test...` names over broad prefixes; broad prefixes pull in
  unrelated subtests that may not pass yet.
