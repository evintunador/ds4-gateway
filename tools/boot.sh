#!/bin/sh
# Boot entry for the ds4-gateway LaunchDaemon.
#
# The daemon points at ~/dev/ds4-gateway-deploy/current/tools/boot.sh, so the
# version that boots is whatever `ds4ctl promote` last blessed — blue/green
# deploys never change it. Boot is deterministic: the blue slot (:9001), with
# the tailnet front door re-pointed to match, regardless of which color was
# live before the reboot.
set -eu
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"

DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$DIR"
mkdir -p logs

# tailscaled may still be coming up at boot; retry for up to a minute
i=0
until tailscale serve --bg 9001 2>/dev/null; do
    i=$((i + 1))
    [ "$i" -ge 30 ] && echo "boot.sh: giving up on tailscale serve (will serve loopback only)" && break
    sleep 2
done

exec uv run python -m ds4gateway --config config.toml --port 9001 --color blue
