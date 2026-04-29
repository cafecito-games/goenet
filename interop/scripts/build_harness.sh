#!/usr/bin/env bash
set -euo pipefail

interop_dir="$(cd "$(dirname "$0")/.." && pwd)"
bin_dir="$interop_dir/bin"
scenario="${1:-all}"

mkdir -p "$bin_dir" "$interop_dir/build"

require_enet_root() {
  local enet_root="$1"

  if [[ ! -f "$enet_root/include/enet.h" ]]; then
    echo "interop: expected ENet header at $enet_root/include/enet.h" >&2
    return 1
  fi
}

build_one() {
  local name="$1"
  local enet_root="$2"
  local source="$interop_dir/cases/$name.c"
  local output="$bin_dir/$name"

  if [[ ! -f "$source" ]]; then
    echo "interop: unknown scenario: $name" >&2
    return 1
  fi
  require_enet_root "$enet_root"

  cc -std=c99 -Wall -Wextra -Wno-unused-parameter \
    -I"$interop_dir/include" \
    -I"$enet_root/include" \
    "$interop_dir/lib/harness.c" \
    "$source" \
    -o "$output"
}

if [[ "$scenario" == "all" ]]; then
  shopt -s nullglob
  sources=("$interop_dir"/cases/*.c)
  shopt -u nullglob
  if [[ ${#sources[@]} -eq 0 ]]; then
    echo "interop: no scenarios found in $interop_dir/cases" >&2
    exit 1
  fi
  enet_root="$("$interop_dir/scripts/resolve_env.sh")"
  for source in "${sources[@]}"; do
    build_one "$(basename "$source" .c)" "$enet_root"
  done
else
  if [[ ! -f "$interop_dir/cases/$scenario.c" ]]; then
    echo "interop: unknown scenario: $scenario" >&2
    exit 1
  fi
  enet_root="$("$interop_dir/scripts/resolve_env.sh")"
  build_one "$scenario" "$enet_root"
fi
