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

needs_rebuild() {
  local output="$1"
  local cache_key_file="$2"
  local expected_enet_root="$3"
  shift 3

  if [[ ! -f "$output" ]]; then
    return 0
  fi

  if [[ ! -f "$cache_key_file" ]]; then
    return 0
  fi

  if [[ "$(<"$cache_key_file")" != "$expected_enet_root" ]]; then
    return 0
  fi

  for dep in "$@"; do
    if [[ "$dep" -nt "$output" ]]; then
      return 0
    fi
  done

  return 1
}

build_one() {
  local name="$1"
  local enet_root="$2"
  local source="$interop_dir/cases/$name.c"
  local output="$bin_dir/$name"
  local cache_key_file="$interop_dir/build/$name.enet_root"
  local deps=(
    "$source"
    "$interop_dir/lib/harness.c"
    "$interop_dir/include/harness.h"
    "$interop_dir/scripts/build_harness.sh"
    "$enet_root/include/enet.h"
  )

  if [[ ! -f "$source" ]]; then
    echo "interop: unknown scenario: $name" >&2
    return 1
  fi
  require_enet_root "$enet_root"
  if ! needs_rebuild "$output" "$cache_key_file" "$enet_root" "${deps[@]}"; then
    echo "SKIP $name"
    return
  fi

  cc -std=c99 -Wall -Wextra -Wno-unused-parameter -Wno-typedef-redefinition \
    -I"$interop_dir/include" \
    -I"$enet_root/include" \
    "$interop_dir/lib/harness.c" \
    "$source" \
    -o "$output"
  printf '%s\n' "$enet_root" > "$cache_key_file"
  echo "BUILD $name"
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
