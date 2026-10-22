#!/usr/bin/env bash
# Computes the next release version for the Release workflow.
#
# usage: next-release-version.sh <patch|minor|major> <workspace-version> [tag ...]
#
# The base is the highest of the workspace version and every plain `vX.Y.Z`
# tag given. The workspace version alone is not a safe base: releases are
# published from a tag, and the version in Cargo.toml can lag the latest tag
# (it sat at 2.1.4 while v2.2.0 through v2.8.0 existed, so the workflow kept
# proposing a tag that was already taken). Prints `<base> <next>`.
set -euo pipefail

bump="${1:?bump (patch|minor|major) required}"
workspace_version="${2:?workspace version required}"
shift 2

semver='^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$'
if ! [[ "$workspace_version" =~ $semver ]]; then
  echo "Workspace version '${workspace_version}' is not X.Y.Z" >&2
  exit 1
fi

base="$workspace_version"
for tag in "$@"; do
  version="${tag#v}"
  # Pre-release and other non-release tags never set the base.
  if [[ "$tag" == v* && "$version" =~ $semver ]]; then
    base="$(printf '%s\n%s\n' "$base" "$version" | sort -V | tail -n1)"
  fi
done

IFS=. read -r major minor patch <<< "$base"
case "$bump" in
  major)
    major=$((major + 1))
    minor=0
    patch=0
    ;;
  minor)
    minor=$((minor + 1))
    patch=0
    ;;
  patch)
    patch=$((patch + 1))
    ;;
  *)
    echo "Unsupported bump: ${bump}" >&2
    exit 1
    ;;
esac

echo "${base} ${major}.${minor}.${patch}"
