#!/usr/bin/env bash
set -euo pipefail

baseline=$(cargo tree -p tinyagents-harness-tools -e normal --no-default-features)
media=$(cargo tree -p tinyagents-harness-tools -e normal --no-default-features --features media)
diff -u <(printf '%s\n' "$baseline") <(printf '%s\n' "$media")
if cargo tree -p tinyagents-harness-tools -e normal --no-default-features --features media \
  | rg 'tinyinference-(video|image)|(^| )((flate2|tar|zip))($| )'; then
  echo "media feature introduced a provider/media dependency" >&2
  exit 1
fi
cargo tree -p tinyagents-harness-tools -e features -i reqwest --no-default-features --features net \
  | rg 'tinyagents-harness-tools feature "net"'
