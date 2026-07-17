# ds4-gateway

Gating, fairness, and deployment gateway for the local `ds4-server`
(DeepSeek V4 Flash, `~/dev/ds4` — kept pristine, never modified by this project).

```
tailnet users ──> tailscale serve (HTTPS, injects Tailscale-User-Login)
                        │
                  gateway :9001/:9002 (blue/green, binds 127.0.0.1 only)
                  │  power gate: AC + battery >= 80%, owner always bypasses
                  │  WFQ scheduler: weighted turns on the serial backend
                        │
                  ds4-server :8001/:8002 (red/yellow, stock binary, 81GB model)
```

## Running

```sh
uv run python -m ds4gateway --config config.toml   # starts model too (autostart)
tailscale serve --bg 9001                          # expose to the tailnet
```

Clients use any OpenAI/Anthropic-compatible SDK against
`https://<machine>.<tailnet>.ts.net/v1/...`. No API keys — identity comes from
Tailscale. The optional `user` field in request bodies is a self-reported
label only; fairness weights key on the Tailscale login (`[scheduler.weights]`
in `config.toml`).

## ds4ctl

```sh
ds4ctl status        # power / gate / backend / queues
ds4ctl off           # stop ds4-server, free the ~81GB (gaming mode)
ds4ctl off --for 2h  # ...and auto-relaunch afterwards (persists across gateway restarts)
ds4ctl on            # reload the model now
```

## Behavior summary

- Off charger or battery < 80%: non-owner requests get `503 battery_gated`;
  the owner (tailscale login in `[owner]`) is always served; model stays loaded.
- `ds4ctl off`: model process stops entirely (RAM freed); everyone, including
  the owner, gets `503 manually_disabled` until `on` or the timer fires.
- One request runs at a time (ds4-server has a single graph worker); queued
  requests are dispatched by weighted fair queuing with a small same-user
  stickiness bonus for KV prefix-cache reuse.
- Admin API (`/admin/*`) accepts the owner's tailscale identity or bare
  loopback connections (that's how `ds4ctl` talks to it).

## Testing

```sh
uv run python tests/test_e2e.py   # mock backend + real gateway, no model load
```

Power states can be simulated on a live gateway via
`POST /admin/power_override {"on_ac": false, "percent": 50}` / `{"clear": true}`.

## Roadmap

- ~~Stage 2~~ done: versioned releases, blue/green gateway deploys, red/yellow
  model swaps (`ds4ctl deploy / swap-model / promote`).
- Stage 3: LaunchDaemon (written but not loaded until proven), memory watchdog.
- Backlog: runtime & memory benchmark suite.
- Stage 4: generalize for other operators — strip anything specific to this
  machine/owner into config + setup docs, so anyone can deploy this in front
  of their own ds4-server.
- v2: continuous batching in a fork of `antirez/ds4` (clone to
  `~/dev/ds4_custom`), or port the engine into exo; disk-backed KV queue.
