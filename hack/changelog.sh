#!/usr/bin/env bash
# CHANGELOG.md operations for the release workflows (.github/workflows/
# cut-release.yml and release.yml), kept here so they're testable:
# hack/changelog_test.sh. Requires GNU sed and awk.
#
#   changelog.sh promote <vX.Y.Z> <YYYY-MM-DD> <repo-url> [file]
#       Move [Unreleased] entries under a new "## [X.Y.Z] - date" heading
#       and rewrite the link references. Prints the previous release tag,
#       or nothing on the first release.
#   changelog.sh notes <vX.Y.Z> [file]
#       Print the body of that version's section.
set -euo pipefail

VERSION_RE='^v[0-9]+\.[0-9]+\.[0-9]+(-[A-Za-z0-9.-]+)?$'

die() { echo "changelog.sh: $*" >&2; exit 1; }

check_version() {
  [[ "$1" =~ $VERSION_RE ]] || die "version '$1' does not look like vX.Y.Z[-prerelease]"
}

promote() {
  local version="$1" today="$2" repo_url="$3" file="${4:-CHANGELOG.md}"
  check_version "$version"
  local semver="${version#v}"

  grep -qx '## \[Unreleased\]' "$file" || die "no '## [Unreleased]' heading in $file"
  grep -qE "^## \[${semver//./\\.}\] - " "$file" && die "$file already has a ## [$semver] section"
  grep -q '^\[Unreleased\]: ' "$file" || die "no '[Unreleased]: ' link reference in $file"

  local content
  content=$(awk '/^## \[Unreleased\]$/{found=1; next} found && /^## \[/{exit} found && /^\[.+\]:/{exit} found && /[^[:space:]]/{print; exit}' "$file")
  [[ -n "$content" ]] || die "## [Unreleased] in $file is empty; add entries before cutting a release"

  # Previous tag comes from the [Unreleased] compare link. Before the first
  # release that link is .../commits/main, and there is no previous tag.
  local prev
  prev=$(sed -n 's|^\[Unreleased\]: .*/compare/\(.*\)\.\.\.HEAD$|\1|p' "$file")

  sed -i "0,/^## \[Unreleased\]$/{s|^## \[Unreleased\]$|## [Unreleased]\n\n## [$semver] - $today|}" "$file"
  sed -i "s|^\[Unreleased\]: .*$|[Unreleased]: $repo_url/compare/$version...HEAD|" "$file"
  local link="$repo_url/releases/tag/$version"
  [[ -n "$prev" ]] && link="$repo_url/compare/$prev...$version"
  sed -i "/^\[Unreleased\]: /a[$semver]: $link" "$file"

  echo "$prev"
}

notes() {
  local version="$1" file="${2:-CHANGELOG.md}"
  check_version "$version"
  local out
  out=$(awk -v v="${version#v}" '
    /^## \[/ {
      if (printing) exit
      if (index($0, "## [" v "]") == 1) { printing = 1; next }
    }
    printing && /^\[.+\]:/ { exit }
    printing { print }
  ' "$file"; echo x)
  out="${out%x}"
  [[ -n "${out//[[:space:]]/}" ]] || die "no '## [${version#v}]' section with content in $file"
  printf '%s' "$out"
}

cmd="${1:-}"; shift || true
case "$cmd" in
  promote) [[ $# -ge 3 ]] || die "usage: promote <vX.Y.Z> <YYYY-MM-DD> <repo-url> [file]"; promote "$@" ;;
  notes)   [[ $# -ge 1 ]] || die "usage: notes <vX.Y.Z> [file]"; notes "$@" ;;
  *) die "usage: changelog.sh {promote|notes} ..." ;;
esac
