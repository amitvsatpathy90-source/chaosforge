#!/usr/bin/env bash
# ADR status gate — every ADR must declare a machine-checkable Status, and a SUPERSEDED one must
# point at its successor. The doc set's correctness depends on supersession being explicit
# (architectural design principle: "only the LATEST is current"); this makes that enforceable instead of a convention a
# new ADR can silently skip.
#
# It reads the Status the ADRs already carry (a `**Status**` line, prose or table form) rather than
# requiring a new frontmatter field — no reformatting of 35 existing files.
set -euo pipefail

adr_dir="docs/adrs"
# DECISION-PENDING is the project's own status for a decision deferred to a future game day
# (ADR-0513/0515) — a real, current state, so it belongs in the allowed set.
allowed='ACCEPTED|PROPOSED|SUPERSEDED|DEPRECATED|REJECTED|DECISION-PENDING'
fail=0

shopt -s nullglob
files=("$adr_dir"/ADR-*.md)
if [ ${#files[@]} -eq 0 ]; then
  echo "::error::no ADRs found under $adr_dir"; exit 1
fi

for f in "${files[@]}"; do
  status_line="$(grep -iE '\*\*status' "$f" | head -1 || true)"
  if [ -z "$status_line" ]; then
    echo "::error file=$f::no '**Status**' line"; fail=1; continue
  fi
  up="$(printf '%s' "$status_line" | tr '[:lower:]' '[:upper:]')"
  if ! printf '%s' "$up" | grep -qE "$allowed"; then
    echo "::error file=$f::Status not one of $allowed -> $status_line"; fail=1; continue
  fi
  # A genuinely superseded ADR must name its successor so "which one is current?" is answerable.
  # (ACCEPTED-but-"narrowed by" ADRs are still current and are not required to.)
  if printf '%s' "$up" | grep -q 'SUPERSEDED' && ! grep -qiE 'ADR-[0-9]{3,4}' "$f"; then
    echo "::error file=$f::SUPERSEDED but references no successor ADR"; fail=1
  fi
done

if [ "$fail" -ne 0 ]; then
  echo "ADR status gate: FAILED"; exit 1
fi
echo "ADR status gate: OK (${#files[@]} ADRs, all with a valid Status)"
