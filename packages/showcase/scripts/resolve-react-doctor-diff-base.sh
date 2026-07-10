#!/usr/bin/env sh
# Resolve the react-doctor `--base` ref (for `--scope changed`) for the current branch.
#
# Prints ONE token to stdout (consumed via $() by the app `react-doctor:diff`
# script); human-readable diagnostics go to stderr. Resolution order:
#   1. The base branch of this branch's *open* PR (via `gh`), preferring the
#      remote-tracking ref `origin/<base>`, then a local `<base>` — the branch
#      the change merges into.
#   2. The literal `parent` keyword (react-doctor's fork-point heuristic) when
#      there is no open PR, `gh` is missing/unauthenticated, or neither ref
#      resolves.
# POSIX sh (invoked as `sh scripts/...`); no bashisms, no `pipefail`.
set -eu

fallback=parent

# emit <token> <reason>
emit() {
	printf '%s\n' "$1"
	printf 'react-doctor diff base: %s (%s)\n' "$1" "$2" >&2
	exit 0
}

if ! command -v gh >/dev/null 2>&1; then
	emit "$fallback" "gh CLI not found"
fi

# Bound the network call so a pre-push never hangs on a slow/offline network
# (`gh pr view` has no timeout flag). `timeout` is GNU coreutils — skip the
# optional prefix where it's unavailable rather than fail.
timeout_cmd=""
if command -v timeout >/dev/null 2>&1; then
	timeout_cmd="timeout 5"
else
	# Without GNU coreutils (stock macOS) the hang guard is absent — say so
	# instead of silently dropping it.
	printf 'react-doctor diff base: no `timeout` binary — gh call runs unguarded\n' >&2
fi

# Only an *open* PR's base is a meaningful merge target; `select` yields nothing
# for a closed/merged PR, so those fall through to the `parent` fallback. The
# unquoted ${timeout_cmd} is an intentional optional-prefix expansion.
base=$(${timeout_cmd} gh pr view --json state,baseRefName --jq 'select(.state == "OPEN").baseRefName' 2>/dev/null || true)

[ -z "$base" ] && emit "$fallback" "no open PR base resolved via gh"

# Never hand react-doctor a ref it would reject (its guard allows only
# [A-Za-z0-9_./-]) or a range — fall back instead of hard-failing the push.
case "$base" in
	*[!A-Za-z0-9_./-]* | *..*) emit "$fallback" "PR base '$base' is not a safe ref" ;;
esac

if git rev-parse --verify "refs/remotes/origin/$base" >/dev/null 2>&1; then
	emit "origin/$base" "PR base (remote-tracking)"
elif git rev-parse --verify "refs/heads/$base" >/dev/null 2>&1; then
	emit "$base" "PR base (local)"
else
	emit "$fallback" "PR base '$base' not found in origin/ or heads/"
fi
