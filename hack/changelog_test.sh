#!/usr/bin/env bash
# Tests for hack/changelog.sh. Run: make changelog-test
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CL="$ROOT/hack/changelog.sh"
URL="https://github.com/middlendian/roommate-csi"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
fail=0

check() { # name, expected-file, actual-file
  if diff -u "$2" "$3"; then echo "ok   $1"; else echo "FAIL $1"; fail=1; fi
}
expect_error() { # name, command...
  local name="$1"; shift
  if "$@" >/dev/null 2>&1; then echo "FAIL $name (expected non-zero exit)"; fail=1; else echo "ok   $name"; fi
}

# --- first release: [Unreleased] links to commits/main, no previous tag ---
cat >"$TMP/first.md" <<'EOF'
# Changelog

## [Unreleased]

### Added

- Thing one.

[Unreleased]: https://github.com/middlendian/roommate-csi/commits/main
EOF
prev=$("$CL" promote v0.1.0 2026-09-23 "$URL" "$TMP/first.md")
cat >"$TMP/first.want" <<'EOF'
# Changelog

## [Unreleased]

## [0.1.0] - 2026-09-23

### Added

- Thing one.

[Unreleased]: https://github.com/middlendian/roommate-csi/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/middlendian/roommate-csi/releases/tag/v0.1.0
EOF
check "first release" "$TMP/first.want" "$TMP/first.md"
[[ -z "$prev" ]] && echo "ok   first release prints no previous tag" || { echo "FAIL first release printed prev=$prev"; fail=1; }

# --- notes for the version just promoted ---
"$CL" notes v0.1.0 "$TMP/first.md" >"$TMP/notes.got"
printf '\n### Added\n\n- Thing one.\n\n' >"$TMP/notes.want"
check "notes" "$TMP/notes.want" "$TMP/notes.got"

# --- second release: compare link present, older sections kept ---
cp "$TMP/first.md" "$TMP/second.md"
sed -i 's|^## \[Unreleased\]$|## [Unreleased]\n\n### Fixed\n\n- Thing two.|' "$TMP/second.md"
prev=$("$CL" promote v0.2.0-rc.1 2026-10-01 "$URL" "$TMP/second.md")
[[ "$prev" == "v0.1.0" ]] && echo "ok   second release prints previous tag" || { echo "FAIL second release prev=$prev"; fail=1; }
grep -qx '## \[0.2.0-rc.1\] - 2026-10-01' "$TMP/second.md" && echo "ok   prerelease heading" || { echo "FAIL prerelease heading"; fail=1; }
grep -qx "\[0.2.0-rc.1\]: $URL/compare/v0.1.0...v0.2.0-rc.1" "$TMP/second.md" && echo "ok   compare link" || { echo "FAIL compare link"; fail=1; }
grep -qx "\[Unreleased\]: $URL/compare/v0.2.0-rc.1...HEAD" "$TMP/second.md" && echo "ok   unreleased link" || { echo "FAIL unreleased link"; fail=1; }
"$CL" notes v0.2.0-rc.1 "$TMP/second.md" | grep -q 'Thing two' && echo "ok   notes stop at next section" || { echo "FAIL notes 0.2.0-rc.1"; fail=1; }
"$CL" notes v0.2.0-rc.1 "$TMP/second.md" | grep -q 'Thing one' && { echo "FAIL notes leaked older section"; fail=1; } || echo "ok   notes exclude older section"

# --- error cases ---
# The duplicate-version guard needs its own file where [Unreleased] still
# has content, so promoting an already-published version is rejected
# because of the duplicate section specifically, not because Unreleased
# happens to be empty too (first.md's Unreleased is empty after the promote
# above, which would make the two guards indistinguishable here).
cp "$TMP/first.md" "$TMP/dup.md"
sed -i 's|^## \[Unreleased\]$|## [Unreleased]\n\n- Another.|' "$TMP/dup.md"
expect_error "duplicate version" "$CL" promote v0.1.0 2026-09-24 "$URL" "$TMP/dup.md"

# The empty-unreleased guard gets its own file too, promoting a version
# that is NOT a duplicate, so only the empty-Unreleased guard can fire.
cp "$TMP/first.md" "$TMP/empty-unreleased.md"
expect_error "empty unreleased" "$CL" promote v0.1.1 2026-09-24 "$URL" "$TMP/empty-unreleased.md"

expect_error "bad version" "$CL" promote 0.1.1 2026-09-24 "$URL" "$TMP/first.md"
expect_error "notes missing version" "$CL" notes v9.9.9 "$TMP/first.md"
printf '# Changelog\n\n## [Unreleased]\n\n- x\n' >"$TMP/nolink.md"
expect_error "no unreleased link" "$CL" promote v0.1.0 2026-09-24 "$URL" "$TMP/nolink.md"

# --- the real CHANGELOG must always be well-formed for promote, regardless
# of whether [Unreleased] currently holds content. Right after a release
# merges, main's CHANGELOG.md has an empty [Unreleased] until the next PR
# adds entries — promoting it as v0.1.0 in that state would legitimately
# fail on the empty-Unreleased guard, which used to make this case flaky
# against the real file depending on release state. So: the heading and
# link line are always required; content is only promoted (as v999.0.0,
# a version that can never collide with a real release) when present. ---
grep -qx '## \[Unreleased\]' "$ROOT/CHANGELOG.md" && echo "ok   real CHANGELOG has Unreleased heading" \
  || { echo "FAIL real CHANGELOG missing Unreleased heading"; fail=1; }
grep -q '^\[Unreleased\]: ' "$ROOT/CHANGELOG.md" && echo "ok   real CHANGELOG has Unreleased link" \
  || { echo "FAIL real CHANGELOG missing Unreleased link"; fail=1; }

# Same emptiness check changelog.sh's promote() uses, so this test's branch
# tracks the real guard instead of guessing at promote's exit status for
# whatever reason it might fail.
real_content=$(awk '/^## \[Unreleased\]$/{found=1; next} found && /^## \[/{exit} found && /^\[.+\]:/{exit} found && /[^[:space:]]/{print; exit}' "$ROOT/CHANGELOG.md")

cp "$ROOT/CHANGELOG.md" "$TMP/real.md"
if [[ -n "$real_content" ]]; then
  # [Unreleased] has content: promote MUST succeed and notes MUST be non-empty.
  if "$CL" promote v999.0.0 2026-09-23 "$URL" "$TMP/real.md" >/dev/null 2>&1; then
    "$CL" notes v999.0.0 "$TMP/real.md" | grep -q '[^[:space:]]' \
      && echo "ok   real CHANGELOG (Unreleased promoted)" \
      || { echo "FAIL real CHANGELOG promoted but notes empty"; fail=1; }
  else
    echo "FAIL real CHANGELOG promote failed despite non-empty Unreleased"; fail=1
  fi
else
  echo "ok   real CHANGELOG (Unreleased empty, promote skipped)"
fi

exit "$fail"
