#!/usr/bin/env bash
set -euo pipefail

blocked='rusqlite|libsqlite3-sys|mongodb|bson|aws-sdk-s3|lapin|amq-protocol|sqlx|redb|sled|rocksdb|tinystoragedrivers-(document|kv|blob|queue|secrets|connect)|tinystoragedrivers'
packages=(tinyagents-harness tinyagents-graph tinyagents-tasks tinyagents-orchestration)

for package in "${packages[@]}"; do
  tree=$(cargo tree -p "$package" --all-features -e normal,build --prefix none | awk '{print $1}')
  if matches=$(grep -xE "$blocked" <<<"$tree"); then
    printf 'normal/build dependency tree for %s contains a storage engine:\n%s\n' "$package" "$matches" >&2
    exit 1
  fi
done
