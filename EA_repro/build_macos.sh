#!/usr/bin/env bash
# Build a standalone macOS executable for the EA repro tool.
#
#   ./build_macos.sh
#
# Produces dist/EA-Repro, which you can double-click or run from a terminal.
#
# The binary bundles Python and the app (the UI and CLI in one). Docker Desktop
# still needs to be installed and running on the machine that uses it.
#
# PyInstaller cannot cross-compile, so run this on macOS to get a macOS binary.
set -euo pipefail
cd "$(dirname "$0")"

PY="${PYTHON:-python3}"

echo "› ensuring PyInstaller is available…"
"$PY" -m PyInstaller --version >/dev/null 2>&1 || "$PY" -m pip install --user pyinstaller

echo "› building EA-Repro (onefile)…"
"$PY" -m PyInstaller \
  --onefile \
  --name EA-Repro \
  --paths . \
  --hidden-import ea_repro \
  --collect-submodules botocore \
  --collect-data botocore \
  --collect-submodules boto3 \
  --collect-data boto3 \
  --noconfirm \
  --clean \
  ea_repro_ui.py

echo
echo "✓ built: dist/EA-Repro"
echo "  run it:  ./dist/EA-Repro         (opens http://localhost:8500)"
echo "  or CLI:  ./dist/EA-Repro up --documents 50000"
