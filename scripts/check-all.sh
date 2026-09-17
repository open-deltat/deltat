#!/usr/bin/env sh
# Every enforced principle, in one command. CI runs this before the test suite; run it locally
# before opening a PR.
#
# The contract for adding to this list: a principle that is only written down decays. When you
# establish a new invariant, add its check here in the same PR. Each check must print why it
# failed AND what to do about it, because the person who trips it is usually not the person who
# wrote it.
set -eu

cd "$(dirname "$0")/.."

fail=0
for check in scripts/check-*.sh; do
    case "$check" in
        scripts/check-all.sh) continue ;;
    esac
    printf '\n── %s ──\n' "$(basename "$check" .sh)"
    sh "$check" || fail=1
done

printf '\n'
if [ "$fail" -ne 0 ]; then
    echo "FAIL: at least one enforced principle regressed (see above)."
    exit 1
fi
echo "OK: all enforced principles hold."
