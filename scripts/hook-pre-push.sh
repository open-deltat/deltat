#!/usr/bin/env sh
# Claude Code PreToolUse hook: run every enforced principle before a push leaves this machine.
#
# Wired up in .claude/settings.json. Reads the tool-call JSON on stdin, does nothing unless the
# command is a `git push`, and blocks the push (non-zero exit) if any check regressed.
#
# Why block here as well as in CI: CI tells you after the fact, in a place you have to go and look.
# This tells you at the moment you would have introduced the regression, which is the only moment
# the context is still in your head.
#
# To opt out locally, override the hook in .claude/settings.local.json (gitignored).
set -eu

input=$(cat)

case "$input" in
    *'git push'*) ;;
    *) exit 0 ;;
esac

repo=$(git rev-parse --show-toplevel 2>/dev/null) || exit 0
[ -f "$repo/scripts/check-all.sh" ] || exit 0

if ! output=$(sh "$repo/scripts/check-all.sh" 2>&1); then
    printf 'Push blocked: an enforced principle regressed.\n\n%s\n' "$output" >&2
    exit 2
fi

exit 0
