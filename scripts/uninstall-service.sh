#!/bin/bash
# Reverses install-service.sh: unloads the launchd job and removes its plist.
# Does not touch logs/ or a currently-running unmanaged process.
set -euo pipefail

if [ "$(uname)" != "Darwin" ]; then
    echo "uninstall-service.sh only supports macOS (launchd)." >&2
    exit 1
fi

LABEL="com.localagentsociety.vortexia"
PLIST_DST="$HOME/Library/LaunchAgents/$LABEL.plist"

launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true
rm -f "$PLIST_DST"

echo "vortexia launchd service removed ($LABEL). It will no longer start at login."
echo "If a vortexia process is still running, stop it with: node src/index.js stop"
