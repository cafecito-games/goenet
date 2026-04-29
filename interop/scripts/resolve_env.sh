#!/usr/bin/env bash
set -euo pipefail

interop_dir="$(cd "$(dirname "$0")/.." && pwd)"
env_file="${GOENET_INTEROP_ENVFILE:-$interop_dir/.env}"
process_enet_source_dir="${ENET_SOURCE_DIR:-}"

if [[ -f "$env_file" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$env_file"
  set +a
fi

if [[ -n "$process_enet_source_dir" ]]; then
  ENET_SOURCE_DIR="$process_enet_source_dir"
fi

if [[ -z "${ENET_SOURCE_DIR:-}" ]]; then
  echo "interop: ENET_SOURCE_DIR must be set in environment or interop/.env" >&2
  exit 1
fi

printf '%s\n' "$ENET_SOURCE_DIR"
