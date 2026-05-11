#!/usr/bin/env bash
set -euo pipefail

# Run complement against neutrino.
#
# By default, runs the curated allowlist of upstream complement CS-API tests
# from a project-local complement checkout fetched into ./complement-${COMPLEMENT_REF}/.
# Pass --in-repo to instead run neutrino-specific tests under ./complement/tests/.
#
# Env vars:
#   COMPLEMENT_DIR  Path to a complement checkout. Overrides the default
#                   project-local fetch.
#   COMPLEMENT_REF  Branch/tag/commit of matrix-org/complement to fetch when
#                   COMPLEMENT_DIR is unset. Defaults to main.
#   IMAGE_TAG       Tag for the built image. Defaults to neutrino:complement.
#
# Extra positional args are forwarded to `go test` (e.g. -run, -v).

usage() {
    cat >&2 <<EOF
Usage: $0 [--in-repo] [extra go test args]...
  --in-repo
        Run neutrino's in-repo complement tests (./complement/tests/...)
        instead of the upstream allowlist.
EOF
}

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
IMAGE_TAG="${IMAGE_TAG:-neutrino:complement}"
COMPLEMENT_REF="${COMPLEMENT_REF:-main}"
ALLOWLIST="${REPO_ROOT}/complement/allowlist.txt"

USE_IN_REPO=
# Forwarded to `go test`. Expanded below as ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}
# rather than "${EXTRA_ARGS[@]}" because bash 3.2 (the macOS system bash)
# treats expansion of an empty array as an unbound variable under `set -u`.
# The conditional form expands to nothing when the array is empty.
EXTRA_ARGS=()
while [ $# -gt 0 ]; do
    case "$1" in
        --in-repo) USE_IN_REPO=1; shift ;;
        -h|--help) usage; exit 0 ;;
        *) EXTRA_ARGS+=("$1"); shift ;;
    esac
done

cd "${REPO_ROOT}"

echo "Building ${IMAGE_TAG}..."
docker build \
    -f docker/complement/Dockerfile \
    -t "${IMAGE_TAG}" \
    .

export COMPLEMENT_BASE_IMAGE="${IMAGE_TAG}"

if [ -n "${USE_IN_REPO}" ]; then
    cd "${REPO_ROOT}/complement"
    echo "Running in-repo complement tests..."
    # See above comment for explanation of weirdness.
    exec go test -v -timeout 5m ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"} ./tests/...
fi

# Upstream tests need a complement checkout. Use COMPLEMENT_DIR if set,
# otherwise fetch a tarball into a project-local directory.
if [ -z "${COMPLEMENT_DIR:-}" ]; then
    COMPLEMENT_DIR="${REPO_ROOT}/complement-${COMPLEMENT_REF}"
    if [ ! -d "${COMPLEMENT_DIR}" ]; then
        echo "Fetching matrix-org/complement@${COMPLEMENT_REF} into ${COMPLEMENT_DIR}..."
        TARBALL="$(mktemp -t complement.XXXXXX.tar.gz)"
        trap 'rm -f "${TARBALL}"' EXIT
        wget -q -O "${TARBALL}" \
            "https://github.com/matrix-org/complement/archive/${COMPLEMENT_REF}.tar.gz"
        mkdir -p "${COMPLEMENT_DIR}"
        tar -xzf "${TARBALL}" --strip-components=1 -C "${COMPLEMENT_DIR}"
    fi
fi

if [ ! -s "${ALLOWLIST}" ]; then
    echo "allowlist ${ALLOWLIST} is empty or missing" >&2
    exit 1
fi

cd "${COMPLEMENT_DIR}"

# Ad-hoc override: if the caller passed their own -run, bypass the allowlist
# entirely and forward EXTRA_ARGS once. Useful for debugging a specific test.
for arg in ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}; do
    if [ "$arg" = "-run" ]; then
        echo "Running ad-hoc test selection (allowlist bypassed)"
        exec go test -v -timeout 5m ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"} ./tests/csapi/...
    fi
done

# Iterate the allowlist line-by-line and run `go test -run <line>` per entry.
# We can't paste entries into a single -run because Go splits the -run regex on
# every unbracketed `/`, including the literal `/` inside subtest names like
# POST_/register_can_create_a_user, which jumbles per-level matching across
# different parent tests.
overall_exit=0
ran_any=
while IFS= read -r entry || [ -n "$entry" ]; do
    case "$entry" in
        ''|\#*) continue ;;
    esac
    ran_any=1
    echo
    echo "=== Allowlist entry: ${entry}"
    if ! go test -v -timeout 5m -run "${entry}" ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"} ./tests/csapi/...; then
        overall_exit=1
    fi
done < "${ALLOWLIST}"

if [ -z "${ran_any}" ]; then
    echo "allowlist contains no enabled tests" >&2
    exit 1
fi

exit "${overall_exit}"
