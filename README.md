# ds4-gateway

Serves DeepSeek V4 Flash from this Mac to a handful of friends over
Tailscale, with battery-aware gating, per-user fairness, zero-downtime
deploys, and content-free usage metrics. The inference engine is a stock
[antirez/ds4](https://github.com/antirez/ds4) checkout (`~/dev/ds4`, never
modified); everything here is the layer around it.

```
tailnet users ──> tailscale serve (HTTPS, injects Tailscale-User-Login)
                        │
                  gateway :9001/:9002 (blue/green, binds 127.0.0.1 only)
                  │  power gate: AC + battery >= 80%, owner always bypasses
                  │  WFQ scheduler: weighted turns on the serial backend
                        │
                  ds4-server :8001/:8002 (red/yellow, 81GB model,
                                          256GB disk KV cache)
```

## For clients (friends on the tailnet)

Point any OpenAI- or Anthropic-compatible SDK at
`https://<machine>.<tailnet>.ts.net/v1`. Any non-empty API key works —
identity comes from your Tailscale login, across all your devices. The
optional `user` body field is a free label, not authentication.

When the server says no, it means it:

| Error | Meaning | What to do |
|---|---|---|
| `503 battery_gated` | host is off charger or under 80% | try later |
| `503 manually_disabled` | owner turned it off (`resumes_at_epoch` if timed) | try later |
| `503 model_loading` | model is starting up | retry in a minute |
| `429 queue_full` | you already have several requests queued | slow down |
| `504 queue_timeout` | your turn never came within the window | retry |

Conversations are never written to disk as text; see the data-retention
section of [docs/DESIGN.md](docs/DESIGN.md).

## Everyday commands (owner)

```sh
bin/ds4ctl status              # power / gate / backend / queues / swap progress
bin/ds4ctl off [--for 2h]      # stop the model, free ~81GB; auto-relaunch timer optional
bin/ds4ctl on                  # reload now
bin/ds4ctl stats [--days 7]    # peak hours, per-user tokens (content-free)
bin/ds4ctl weights [LOGIN N | LOGIN --clear]   # fairness weights, live, persistent
bin/ds4ctl bench               # TTFT / decode-rate / footprint benchmark, saved as JSON
```

## Shipping changes

```sh
bin/ds4ctl deploy       # gateway blue/green: ship committed HEAD, zero downtime
bin/ds4ctl swap-model   # model red/yellow: apply config.toml model changes, zero downtime
bin/ds4ctl promote      # bless the live release as the boot version (manual on purpose)
bin/ds4ctl deploy --release ~/dev/ds4-gateway-deploy/releases/<dir>   # rollback
```

Deploys never change what boots; only `promote` moves the `current` symlink.

## If the Mac rebooted

```sh
~/dev/ds4-gateway-deploy/current/tools/boot.sh
```

Or install the LaunchDaemon for automatic boot: `bin/ds4ctl install-daemon`
prints the sudo commands (it is never installed automatically).

## Testing

```sh
uv run python tests/test_e2e.py        # gating/fairness/metrics vs mock backend
uv run python tests/test_lifecycle.py  # spawn/adopt/swap/handoff/watchdog vs fake server
uv run python tools/simulate.py --duration 60   # live traffic simulator
```

## More

- [docs/OPERATIONS.md](docs/OPERATIONS.md) — full runbook: topology, deploys,
  onboarding, KV-cache purge, emergency stops.
- [docs/DESIGN.md](docs/DESIGN.md) — why it's built this way: hardware/engine
  constraints, switch mechanics, data retention. Agents/LLM devs: start with
  [CLAUDE.md](CLAUDE.md).

## Roadmap

- Stage 4: generalize for other operators — strip anything specific to this
  machine/owner into config + setup docs.
- v2: continuous batching in a fork of antirez/ds4 (clone to
  `~/dev/ds4_custom`), or port the engine into exo; per-user in-RAM KV
  sessions come with it.
