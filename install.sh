#!/bin/sh
set -eu

repo=teddytennant/inlet
dest="${HOME:-}/.local/bin"
system=0

if [ "${1:-}" = "--system" ]; then
  dest=/usr/local/bin
  system=1
  shift
fi
if [ $# -ne 0 ]; then
  echo "usage: install.sh [--system]" >&2
  exit 2
fi
if [ "$system" -eq 0 ] && [ -z "${HOME:-}" ]; then
  echo "HOME is unset" >&2
  exit 1
fi

if [ "$(uname -s)" != "Linux" ]; then
  echo "inlet is a linux binary. the cell needs user namespaces." >&2
  exit 1
fi

case $(uname -m) in
  x86_64) target=x86_64-unknown-linux-musl ;;
  aarch64 | arm64) target=aarch64-unknown-linux-musl ;;
  *)
    echo "no build for $(uname -m). want x86_64 or aarch64." >&2
    exit 1
    ;;
esac

asset=inlet-$target
base="https://github.com/${repo}/releases/latest/download"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

fetch() {
  if [ -n "${GITHUB_TOKEN:-}" ]; then
    curl -fsSL \
      -H "Authorization: Bearer ${GITHUB_TOKEN}" \
      -H "Accept: application/octet-stream" \
      -o "$2" \
      "$1"
  else
    curl -fsSL -o "$2" "$1"
  fi
}

fetch "$base/$asset" "$tmp/$asset"
fetch "$base/SHA256SUMS" "$tmp/SHA256SUMS"

line=$(grep "  ${asset}\$" "$tmp/SHA256SUMS" || true)
if [ -z "$line" ]; then
  echo "no checksum for $asset" >&2
  exit 1
fi
printf '%s\n' "$line" | (cd "$tmp" && sha256sum -c -)

if [ "$system" -eq 1 ] && [ "$(id -u)" -ne 0 ]; then
  sudo mkdir -p "$dest"
  sudo cp "$tmp/$asset" "$dest/inlet"
  sudo chmod 755 "$dest/inlet"
else
  mkdir -p "$dest"
  cp "$tmp/$asset" "$dest/inlet"
  chmod 755 "$dest/inlet"
fi

case ":${PATH}:" in
  *":${dest}:"*) ;;
  *) printf '%s\n' "installed ${dest}/inlet. add it to PATH." ;;
esac

if [ -t 0 ]; then
  "$dest/inlet" init
fi
