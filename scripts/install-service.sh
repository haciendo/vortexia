#!/bin/bash
# Installs vortexia as a macOS launchd LaunchAgent: starts at login, and
# restarts automatically on crash (see launchd/com.localagentsociety.vortexia.plist.template
# for the exact semantics). Idempotent — safe to re-run after an update.
set -euo pipefail

if [ "$(uname)" != "Darwin" ]; then
    echo "install-service.sh only supports macOS (launchd) today." >&2
    echo "See docs/service-persistence.md for the Linux/Windows plan." >&2
    exit 1
fi

ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
LABEL="com.localagentsociety.vortexia"
PLIST_DST="$HOME/Library/LaunchAgents/$LABEL.plist"
NODE_BIN="$(command -v node)"

if [ -z "$NODE_BIN" ]; then
    echo "ERROR: node not found on PATH. Install Node and re-run." >&2
    exit 1
fi

mkdir -p "$ROOT/logs"

sed -e "s#__NODE_BIN__#$NODE_BIN#g" \
    -e "s#__VORTEXIA_ROOT__#$ROOT#g" \
    "$ROOT/launchd/com.localagentsociety.vortexia.plist.template" > "$PLIST_DST"

# If vortexia is currently running unmanaged (started by hand, e.g. from a
# Claude Code session), stop it first so launchd doesn't collide with it —
# two processes fighting over the same MQTT port would just crash-loop.
STOPPED_UNMANAGED_PID=""
if [ -f "$ROOT/vortexia.pid" ]; then
    EXISTING_PID="$(cat "$ROOT/vortexia.pid" 2>/dev/null || true)"
    if [ -n "$EXISTING_PID" ] && kill -0 "$EXISTING_PID" 2>/dev/null; then
        echo "Stopping unmanaged vortexia (pid $EXISTING_PID) before handing off to launchd..."
        kill -TERM "$EXISTING_PID"
        sleep 1
        STOPPED_UNMANAGED_PID="$EXISTING_PID"
    fi
fi

launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true

# `launchctl bootstrap` targets the gui/<uid> domain, which only exists while
# that user has an active Aqua (window server) session. Run this script over
# SSH, from a headless/background context, or before login, and bootstrap
# fails with "5: Input/output error" — nothing to do with code signing (this
# plist just execs plain node, no .app bundle involved). Don't let set -e
# kill the script here: if we already stopped an unmanaged instance above,
# leaving vortexia down on top of a launchd failure is worse than either
# failure alone, so fall back to restarting it unmanaged.
set +e
launchctl bootstrap "gui/$(id -u)" "$PLIST_DST"
BOOTSTRAP_STATUS=$?
set -e

if [ "$BOOTSTRAP_STATUS" -ne 0 ]; then
    echo "" >&2
    echo "ERROR: launchctl bootstrap failed (exit $BOOTSTRAP_STATUS)." >&2
    echo "This is almost always because gui/$(id -u) has no active window-server" >&2
    echo "session right now (e.g. install ran over SSH, headlessly, or pre-login)." >&2
    echo "Re-run this script from a logged-in GUI Terminal session to register" >&2
    echo "the launchd service." >&2
    if [ -n "$STOPPED_UNMANAGED_PID" ]; then
        echo "" >&2
        echo "Restarting vortexia unmanaged so it isn't left down..." >&2
        nohup "$NODE_BIN" "$ROOT/src/index.js" start >>"$ROOT/logs/launchd.out.log" 2>>"$ROOT/logs/launchd.err.log" &
        disown
        sleep 1
        if [ -f "$ROOT/vortexia.pid" ] && kill -0 "$(cat "$ROOT/vortexia.pid")" 2>/dev/null; then
            echo "vortexia is running again unmanaged (pid $(cat "$ROOT/vortexia.pid"))." >&2
        else
            echo "Failed to restart vortexia unmanaged — check $ROOT/logs/launchd.err.log" >&2
        fi
    fi
    exit "$BOOTSTRAP_STATUS"
fi

launchctl enable "gui/$(id -u)/$LABEL"

echo "vortexia installed as a launchd service ($LABEL)."
echo "  Plist  → $PLIST_DST"
echo "  Logs   → $ROOT/logs/vortexia.log (app-level, daily rotation, 7-day retention)"
echo "           $ROOT/logs/launchd.{out,err}.log (pre-logger / crash output)"
echo ""
echo "Manage it with:"
echo "  launchctl kickstart -k gui/\$(id -u)/$LABEL   # restart"
echo "  launchctl bootout gui/\$(id -u)/$LABEL        # stop + unload"
echo "  node src/index.js status                     # check from inside $ROOT"
