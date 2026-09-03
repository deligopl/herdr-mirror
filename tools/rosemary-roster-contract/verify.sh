#!/usr/bin/env bash
set -euo pipefail

readonly EXPECTED_COMMIT=7d589949ab50d3f5ec2c0d542f411ef31c6a75f3
readonly EXPECTED_ROSTER_SHA256=87bd9d62d4c7abfa6e76ad6adfcd45f067e2c3c845c49195a5eb96829acf0d28
readonly TOOL_DIR="$(cd "$(dirname "$0")" && pwd)"
readonly REPO_ROOT="$(cd "$TOOL_DIR/../.." && pwd)"
readonly ROSEMARY_CHECKOUT="${1:?usage: verify.sh READ_ONLY_ROSEMARY_CHECKOUT}"
readonly MODE="${2:---verify}"
readonly FIXTURE="$REPO_ROOT/tests/fixtures/rosemary-roster-contract.json"

[[ "$(git -C "$ROSEMARY_CHECKOUT" rev-parse HEAD)" == "$EXPECTED_COMMIT" ]] \
  || { echo "Rosemary checkout is not at $EXPECTED_COMMIT" >&2; exit 1; }
[[ -z "$(git -C "$ROSEMARY_CHECKOUT" status --porcelain)" ]] \
  || { echo "Rosemary checkout is dirty; refusing source-lock verification" >&2; exit 1; }
[[ "$(shasum -a 256 "$ROSEMARY_CHECKOUT/crates/server/src/roster.rs" | awk '{print $1}')" == "$EXPECTED_ROSTER_SHA256" ]] \
  || { echo "Rosemary roster derivation hash does not match" >&2; exit 1; }

readonly TEMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TEMP_DIR"' EXIT
ln -s "$ROSEMARY_CHECKOUT" "$TEMP_DIR/rosemary"
cp "$TOOL_DIR/Cargo.toml.in" "$TEMP_DIR/Cargo.toml"
cp "$ROSEMARY_CHECKOUT/Cargo.lock" "$TEMP_DIR/Cargo.lock"
mkdir "$TEMP_DIR/src"
cp "$TOOL_DIR/main.rs" "$TEMP_DIR/src/main.rs"
ROSEMARY_SOURCE_COMMIT="$EXPECTED_COMMIT" \
ROSEMARY_ROSTER_SHA256="$EXPECTED_ROSTER_SHA256" \
  CARGO_TARGET_DIR="$REPO_ROOT/target/rosemary-roster-contract" \
  cargo run --quiet --offline --manifest-path "$TEMP_DIR/Cargo.toml" >"$TEMP_DIR/generated.json"
if [[ "$MODE" == "--print" ]]; then
  cat "$TEMP_DIR/generated.json"
else
  [[ "$MODE" == "--verify" ]] || { echo "usage: verify.sh READ_ONLY_ROSEMARY_CHECKOUT [--verify|--print]" >&2; exit 1; }
  cmp "$FIXTURE" "$TEMP_DIR/generated.json"
  echo "Rosemary roster contract fixture matches derive_observed at $EXPECTED_COMMIT"
fi
