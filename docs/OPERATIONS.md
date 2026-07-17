# Operations runbook

## Current deployment (stage 1, manual — no boot persistence yet)

| Component | Where | Notes |
|---|---|---|
| gateway (blue) | 127.0.0.1:9001 | `uv run python -m ds4gateway --config config.toml` from the repo root |
| ds4-server | 127.0.0.1:8001 | spawned/supervised by the gateway; do not start by hand while the gateway manages it |
| tailscale serve | `https://<machine>.<tailnet>.ts.net` → :9001 | persists across reboots on its own |
| logs | `logs/gateway.log`, `logs/ds4-server.log` | |

**After a reboot** — if the LaunchDaemon is installed, nothing: it runs
`current/tools/boot.sh`, which starts the promoted release in the blue slot
(:9001) and re-points tailscale serve at it. Until the daemon is installed
(`ds4ctl install-daemon` prints the sudo commands; it is deliberately never
installed automatically), start manually:

```sh
~/dev/ds4-gateway-deploy/current/tools/boot.sh   # or from the repo checkout
```

Check with `bin/ds4ctl status`. Note the daemon restarts a crashed gateway at
most every 5 minutes (ThrottleInterval) and only on crash — a clean watchdog
exit or `launchctl bootout` stays down.

## Sleep settings

The API dies if the Mac sleeps. Required once: `sudo pmset -c sleep 0`
(never sleep on AC; battery behavior untouched). Lid closed still sleeps
without an external display — that requires `pmset disablesleep 1`
(sledgehammer; not recommended).

## Everyday controls

```sh
bin/ds4ctl status         # power / gate / backend / queues
bin/ds4ctl off            # stop model, free ~81GB (gaming mode)
bin/ds4ctl off --for 2h   # ...with auto-relaunch
bin/ds4ctl on             # reload now (takes a bit; first requests page weights in)
```

## Onboarding a friend

1. Send a tailnet invite; they install Tailscale and join.
2. They point any OpenAI-compatible SDK at
   `https://<machine>.<tailnet>.ts.net/v1` — any non-empty API key string
   works (identity comes from Tailscale, not the key).
3. Optionally add a fairness weight under `[scheduler.weights]` in
   `config.toml` (key = their tailscale login, e.g. `"alice@github" = 2`)
   and restart the gateway.

## Deploys and swaps (stage 2)

```sh
bin/ds4ctl deploy          # blue/green: ship committed HEAD to the inactive color
bin/ds4ctl deploy --release ~/dev/ds4-gateway-deploy/releases/<dir>   # rollback
bin/ds4ctl swap-model      # red/yellow: two-phase model swap (edit config.toml first)
bin/ds4ctl promote         # make the live release the boot version (manual by design)
```

Release layout under `~/dev/ds4-gateway-deploy/`: `releases/<ts>-<sha>/`
(full checkouts with their own venvs), `live` -> what serves traffic now,
`current` -> what the LaunchDaemon will boot (moves ONLY on promote).
A deploy that fails health checks leaves the old gateway untouched.

## Simulating conditions on a live gateway

```sh
# pretend to be on battery at 50% (remote users get 503s; owner unaffected)
curl -X POST 127.0.0.1:9001/admin/power_override \
  -d '{"on_ac": false, "percent": 50}'
curl -X POST 127.0.0.1:9001/admin/power_override -d '{"clear": true}'
```

## Load / stress testing

```sh
uv run python tools/simulate.py --duration 60 --users 3 --report-every 10
```

Simulated identities work by setting the `Tailscale-User-Login` header
directly, which the gateway trusts from loopback. This means the simulator
must run ON this machine against `127.0.0.1:9001` — pointed at the public
ts.net URL, tailscale serve overwrites the header and all traffic becomes
"you". Keep rates gentle against the real model (it is serial); the mock
backend (`tests/mock_backend.py`) is the right target for high-rate runs.

## Emergency stops

```sh
bin/ds4ctl off                      # stop the model, keep gateway up (clean 503s)
tailscale serve reset               # unpublish from the tailnet entirely
pkill -f "python -m ds4gateway"     # kill gateway (also stops ds4-server via cleanup)
pkill -f ds4-server                 # last resort if a model process is orphaned
```
