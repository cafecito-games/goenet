#!/usr/bin/env bash
set -euo pipefail

interop_dir="$(cd "$(dirname "$0")/.." && pwd)"
output="${1:-$interop_dir/bin/enet-harness}"

resolved_enet_source_dir="$("$interop_dir/scripts/resolve_env.sh")"
export ENET_SOURCE_DIR="$resolved_enet_source_dir"

exec "$interop_dir/build_c_harness.sh" "$output"
