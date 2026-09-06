#!/usr/bin/env bash
#
# Builds the interactive lab: one HTML file with the raft core inside it.
#
#   ./scripts/build-lab.sh [out.html]
#
# The wasm is gzipped and base64'd into the page, then decompressed in the
# browser by DecompressionStream. Raw base64 would be ~4.6 MB; compressed it is
# ~1.3 MB, and every browser that can run Go's wasm can also inflate gzip.
set -euo pipefail

cd "$(dirname "$0")/.."
FRAGMENT=0
if [ "${1:-}" = "--fragment" ]; then FRAGMENT=1; shift; fi
OUT="${1:-raftlab.html}"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

EXEC_JS="$(go env GOROOT)/lib/wasm/wasm_exec.js"
if [ ! -f "$EXEC_JS" ]; then
  # Go moved this file in 1.24. Fall back for older toolchains.
  EXEC_JS="$(go env GOROOT)/misc/wasm/wasm_exec.js"
fi
[ -f "$EXEC_JS" ] || { echo "cannot find wasm_exec.js under $(go env GOROOT)" >&2; exit 1; }

echo "building wasm..."
# -s -w strips the symbol table and DWARF: nothing here is debugged through a
# stack trace, and it is a third of the binary.
GOOS=js GOARCH=wasm go build -ldflags="-s -w" -o "$TMP/lab.wasm" ./cmd/raftwasm

echo "assembling $OUT..."
python3 - "$TMP/lab.wasm" "$EXEC_JS" cmd/raftlab/lab.html "$OUT" "$FRAGMENT" <<'PY'
import base64, gzip, pathlib, sys

wasm, execjs, template, out, fragment = sys.argv[1:6]
blob = base64.b64encode(gzip.compress(pathlib.Path(wasm).read_bytes(), 9)).decode()
page = pathlib.Path(template).read_text()

for token, value in (("__WASM_EXEC__", pathlib.Path(execjs).read_text()),
                     ("__WASM_B64__", blob)):
    if token not in page:
        sys.exit(f"template is missing {token}")
    page = page.replace(token, value, 1)

# --fragment omits the document wrapper, for a host that supplies its own
# <head>. Same page either way, so the two cannot drift.
if fragment == "1":
    pathlib.Path(out).write_text(page)
else:
    pathlib.Path(out).write_text(
        "<!doctype html>\n<html lang=\"en\">\n<head>\n"
        "<meta charset=\"utf-8\">\n"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n"
        "<title>raftkv · consensus lab</title>\n"
        "<style>*,*::before,*::after{box-sizing:border-box}body{margin:0}</style>\n"
        "</head>\n<body>\n" + page + "\n</body>\n</html>\n")
PY

printf '%s — %s KB\n' "$OUT" "$(( $(wc -c < "$OUT") / 1024 ))"
