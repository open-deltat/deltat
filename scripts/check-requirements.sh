#!/usr/bin/env sh
# CI gate: the requirement corpus stays internally consistent.
#
# Requirement IDs are the project's stable contract (docs/REQUIREMENTS.md: "never delete an ID").
# Prose cannot enforce that, so this does:
#
#   1. No ID is defined twice IN THE SAME FILE. Across files is legitimate: REQUIREMENTS.md is
#      authoritative and the companion docs restate or extend an ID on purpose.
#   2. Every ID referenced in docs/ is defined somewhere in docs/. Catches typos and renames.
#   3. Ratchet: the number of "✅" requirements that name no verifying test may not grow.
#      PRIN-10 says a requirement is met only when running code verifies it. Most of the corpus
#      predates that being checked, so this fails on an increase rather than on the backlog.
#
# Baselines live in scripts/requirements-baseline.txt as "key=value" lines. Burn them down;
# never raise one to make a build pass.
#
# KNOWN LIMITATION, and it is the finding that motivated this script: the corpus declares IDs
# three different ways (list item, bare line-start bold, table cell). All three are accepted
# below. Converging on one convention would make this check exact instead of best-effort.
set -eu

cd "$(dirname "$0")/.."

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

# Scan prose only. Fenced code blocks contain illustrative markup (MAINTENANCE.md documents the
# declaration styles by showing them), and counting an example as a declaration is how a doc that
# explains the convention gets flagged for using it.
mkdir -p "$tmp/docs"
for f in docs/*.md; do
    awk '/^[[:space:]]*```/ { fence = !fence; next } !fence' "$f" > "$tmp/docs/$(basename "$f")"
done
# Every scan below reads the stripped copies via "$@".
set -- "$tmp"/docs/*.md

# A definition is a bolded ID opening a line, optionally after a list marker or a table pipe.
# The bold may carry a title after the ID ("**VIS-13a EXISTENCE-HIDING**"), so the closing
# ** need not follow immediately. The ID charset is deliberately loose: the corpus legitimately
# contains VIS-00, PROTO-AUTH-08, MCP-K1, PAY-x402-01, SHR-10d and GAP-AUDIT.
def_re='^[[:space:]]*([-|][[:space:]]*)?\*\*([A-Z][A-Za-z0-9]*(-[A-Za-z0-9]+)+)'
id_re='\b[A-Z][A-Za-z0-9]*(-[A-Za-z0-9]+)+\b'

strip_def() { sed -E 's/^[[:space:]]*([-|][[:space:]]*)?\*\*//'; }

status=0
baseline_file=scripts/requirements-baseline.txt
read_baseline() {
    v=$(grep "^$1=" "$baseline_file" 2>/dev/null | cut -d= -f2 || true)
    [ -n "${v:-}" ] && echo "$v" || echo 0
}

# ── 1. duplicate definitions within one file ────────────────────────────────
: > "$tmp/dupes.txt"
for f in "$@"; do
    grep -oE "$def_re" "$f" | strip_def | sort | uniq -d \
      | sed "s|^|  $f: |" >> "$tmp/dupes.txt"
done

dupes=$(wc -l < "$tmp/dupes.txt" | tr -d ' ')
dupes_baseline=$(read_baseline dupes)

if [ "$dupes" -gt "$dupes_baseline" ]; then
    echo "FAIL: in-file duplicate ID declarations rose from $dupes_baseline to $dupes:"
    cat "$tmp/dupes.txt"
    echo
    echo "An ID is a stable contract. Merge the two entries, or give the new one its own ID."
    echo "(Some of the baseline is legitimate: MCP.md declares each ID twice, once in prose and"
    echo "once in its summary table. An explicit declaration marker would let this be exact.)"
    status=1
elif [ "$dupes" -lt "$dupes_baseline" ]; then
    echo "OK: duplicate declarations fell from $dupes_baseline to $dupes. Lower the ratchet."
else
    echo "OK: duplicate declarations holding at $dupes_baseline."
fi

# ── 2. dangling references (ratcheted) ──────────────────────────────────────
grep -rhoE "$def_re" "$@" | strip_def | sort -u > "$tmp/defs.txt"
# Only consider IDs whose prefix the corpus already knows, so an ordinary hyphenated
# capitalised phrase in prose cannot masquerade as a requirement reference.
sed -E 's/-[^-]+$//' "$tmp/defs.txt" | sort -u > "$tmp/prefixes.txt"
grep -rhoE "$id_re" "$@" | sort -u > "$tmp/refs.txt"

: > "$tmp/dangling.txt"
while IFS= read -r ref; do
    grep -qxF "$(printf '%s' "$ref" | sed -E 's/-[^-]+$//')" "$tmp/prefixes.txt" || continue
    grep -qxF "$ref" "$tmp/defs.txt" || printf '%s\n' "$ref" >> "$tmp/dangling.txt"
done < "$tmp/refs.txt"

dangling=$(wc -l < "$tmp/dangling.txt" | tr -d ' ')
dangling_baseline=$(read_baseline dangling)

if [ "$dangling" -gt "$dangling_baseline" ]; then
    echo "FAIL: dangling requirement references rose from $dangling_baseline to $dangling:"
    sed 's/^/  /' "$tmp/dangling.txt"
    echo
    echo "Either the ID is a typo, or it was renamed and this reference was missed."
    status=1
elif [ "$dangling" -lt "$dangling_baseline" ]; then
    echo "OK: dangling references fell from $dangling_baseline to $dangling. Lower the ratchet."
else
    echo "OK: dangling references holding at $dangling_baseline."
fi

# ── 3. the "✅ names its proof" ratchet ──────────────────────────────────────
# Build the set of function names once; a per-token grep over the tree would be quadratic.
grep -rhoE '\bfn [a-z_][a-z0-9_]*' src tests benches 2>/dev/null \
  | sed 's/^fn //' | sort -u > "$tmp/fns.txt"

: > "$tmp/unproven.txt"
grep -hE "$def_re" "$@" | grep '✅' > "$tmp/done.txt" || true
while IFS= read -r line; do
    [ -n "$line" ] || continue
    id=$(printf '%s' "$line" | strip_def | grep -oE "^$id_re" || true)
    [ -n "$id" ] || continue
    proven=0
    for tok in $(printf '%s' "$line" | grep -oE '`[a-z_][a-z0-9_]{4,}`' | tr -d '`' | sort -u); do
        grep -qxF "$tok" "$tmp/fns.txt" && { proven=1; break; }
    done
    [ "$proven" -eq 0 ] && printf '%s\n' "$id" >> "$tmp/unproven.txt"
done < "$tmp/done.txt"

unproven=$(sort -u "$tmp/unproven.txt" | wc -l | tr -d ' ')
unproven_baseline=$(read_baseline unproven)

if [ "$unproven" -gt "$unproven_baseline" ]; then
    echo "FAIL: ✅ requirements naming no verifying test rose from $unproven_baseline to $unproven."
    echo
    echo "PRIN-10: a requirement is met only when running code verifies it. Name the test in the"
    echo "requirement line, in backticks, e.g. \`buffer_conflict_is_order_independent\`."
    sort -u "$tmp/unproven.txt" | sed 's/^/  /'
    status=1
elif [ "$unproven" -lt "$unproven_baseline" ]; then
    echo "OK: unproven ✅ requirements fell from $unproven_baseline to $unproven. Lower the ratchet."
else
    echo "OK: unproven ✅ requirements holding at $unproven_baseline."
fi

exit "$status"
