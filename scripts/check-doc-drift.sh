#!/usr/bin/env bash
#
# check-doc-drift.sh — catches the AUTOMATABLE half of doc drift: a doc that references a test,
# class, or ADR that no longer exists in the code. This is the "dangling reference" failure mode —
# grep-shaped, so grep-checkable.
#
# WHERE TO RUN IT:
#   - Run locally or in CI pipelines to catch dangling class/test and ADR references
#     across README, design docs, and architectural decision records.
#
# It is deliberately CONSERVATIVE — only two checks, both false-positive-free, because a drift check
# that cries wolf gets switched off. A dangling reference is the build's problem, not a warning.
#
# THE CLAIM-ANCHORING CONVENTION this enforces:
#   Every "proven by <X>" / "done" / gate-closure claim should name a real test/class/ADR. If it
#   names one, this script guarantees it still exists. If a claim can't name one, it isn't
#   verifiable — rewrite it so it can, or mark it explicitly as intent, not status.
#
set -uo pipefail
cd "$(dirname "$0")/.." || exit 2

misses=""

# Scan public documentation and ADRs
docs=()
for d in README.md ./*-design.md docs/adrs/*.md; do
  [ -f "$d" ] && docs+=("$d")
done

if [ ${#docs[@]} -eq 0 ]; then 
  echo "check-doc-drift: no doc files found"
  exit 0
fi

echo "check-doc-drift: scanning ${#docs[@]} file(s)"

# --- Check 1: every Test/IT class named in the docs exists as a .java file (test or main) ---
frameworks="SpringBootTest DataJpaTest WebMvcTest WebFluxTest JsonTest RestClientTest JdbcTest DataRedisTest ParameterizedTest RepeatedTest"
for t in $(grep -rhoE "\b[A-Z][A-Za-z0-9]+(Test|IT)\b" "${docs[@]}" 2>/dev/null | sort -u); do
  case "$t" in ArchTest | Test | IT) continue ;; esac
  [[ " $frameworks " == *" $t "* ]] && continue          # framework annotation, not a repo class
  [[ "$t" =~ ^[A-Z0-9]+$ ]] && continue                  # all-caps word (LIMIT/EXIT/INIT), not a class
  if ! find . -path ./build -prune -o -path ./target -prune -o -name "${t}.java" -print 2>/dev/null | grep -q .; then
    misses+="  dangling test/class reference: ${t} (named in docs, no ${t}.java in tree)"$'\n'
  fi
done

# --- Check 2: every ADR-NNN referenced in the docs has a file in docs/adrs/ ---
for a in $(grep -rhoE "ADR-[0-9]+" "${docs[@]}" 2>/dev/null | sort -u); do
  if ! ls "docs/adrs/${a}.md" >/dev/null 2>&1; then
    misses+="  dangling ADR reference: ${a} (referenced in docs, no docs/adrs/${a}.md)"$'\n'
  fi
done

if [ -n "$misses" ]; then
  echo "DOC DRIFT DETECTED — a doc references something that no longer exists:"
  printf "%s" "$misses"
  echo "Fix the reference (or the code), then re-run."
  exit 1
fi

echo "check-doc-drift: OK — every referenced test/class/ADR exists."