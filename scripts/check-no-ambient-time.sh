#!/usr/bin/env sh
# CI gate: ambient wall-clock reads (`SystemTime::now()`) may appear ONLY in src/clock.rs,
# the determinism seam. Everything else must take time from an injected `Clock`.
#
# This is belt-and-suspenders alongside the clippy `disallowed-methods` rule: a plain text
# grep can't be fooled by how the call is spelled (`SystemTime::now()` vs the fully-qualified
# `std::time::SystemTime::now()`), which path-based lints sometimes are.
set -eu

hits=$(grep -rn --include='*.rs' 'SystemTime::now' src | grep -v '^src/clock.rs:' || true)

if [ -n "$hits" ]; then
    echo "FAIL: ambient SystemTime::now() found outside src/clock.rs:"
    echo "$hits"
    echo
    echo "Route it through the injected Clock instead (e.g. self.now_ms() / engine.now_ms())."
    exit 1
fi

echo "OK: ambient wall-clock reads are confined to src/clock.rs"
