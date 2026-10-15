#!/usr/bin/env bash
# Tests for next-release-version.sh. Run: .github/scripts/next-release-version.test.sh
set -euo pipefail

script="$(cd "$(dirname "$0")" && pwd)/next-release-version.sh"
failures=0

expect() {
  local name="$1" want="$2"
  shift 2
  local got
  if got="$("$script" "$@" 2>/dev/null)"; then :; else got="error"; fi
  if [[ "$got" != "$want" ]]; then
    echo "FAIL ${name}: want '${want}', got '${got}'" >&2
    failures=$((failures + 1))
  fi
}

# The regression: a stale workspace version must not hide newer tags.
expect "stale workspace minor" "2.8.0 2.9.0" minor 2.1.4 v2.1.4 v2.2.0 v2.7.0 v2.8.0
expect "stale workspace patch" "2.8.0 2.8.1" patch 2.1.4 v2.8.0 v2.2.0
expect "stale workspace major" "2.8.0 3.0.0" major 2.1.4 v2.8.0
# Numeric, not lexical, ordering.
expect "numeric ordering" "2.10.0 2.11.0" minor 2.1.4 v2.9.0 v2.10.0
# A workspace version ahead of every tag still wins.
expect "workspace ahead" "3.0.0 3.1.0" minor 3.0.0 v2.8.0
expect "no tags" "2.1.4 2.1.5" patch 2.1.4
# Pre-release and foreign tags never set the base.
expect "prerelease ignored" "2.8.0 2.9.0" minor 2.1.4 v2.8.0 v3.0.0-rc1 nightly 9.9.9
# A leading-zero tag is not a release version (08 would be octal).
expect "leading zero ignored" "2.8.0 2.9.0" minor 2.1.4 v2.8.0 v2.08.0
# Bad input fails.
expect "bad bump" "error" hotfix 2.1.4
expect "bad workspace version" "error" minor 2.1 v2.8.0

if ((failures > 0)); then
  echo "${failures} failure(s)" >&2
  exit 1
fi
echo "next-release-version: ok"
