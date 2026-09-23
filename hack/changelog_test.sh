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
cp "$TMP/first.md" "$TMP/dup.md"
expect_error "duplicate version" "$CL" promote v0.1.0 2026-09-24 "$URL" "$TMP/dup.md"
expect_error "empty unreleased" "$CL" promote v0.1.1 2026-09-24 "$URL" "$TMP/dup.md"
expect_error "bad version" "$CL" promote 0.1.1 2026-09-24 "$URL" "$TMP/first.md"
expect_error "notes missing version" "$CL" notes v9.9.9 "$TMP/first.md"
printf '# Changelog\n\n## [Unreleased]\n\n- x\n' >"$TMP/nolink.md"
expect_error "no unreleased link" "$CL" promote v0.1.0 2026-09-24 "$URL" "$TMP/nolink.md"

# --- the real CHANGELOG must be promotable as a first release ---
cp "$ROOT/CHANGELOG.md" "$TMP/real.md"
"$CL" promote v0.1.0 2026-09-23 "$URL" "$TMP/real.md" >/dev/null && "$CL" notes v0.1.0 "$TMP/real.md" | grep -q '### Added' \
  && echo "ok   real CHANGELOG" || { echo "FAIL real CHANGELOG"; fail=1; }

exit "$fail"
