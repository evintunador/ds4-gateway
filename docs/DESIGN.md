# Design rationale

Facts and decisions that shaped this codebase but are not visible in it.
Written for future maintainers (human or LLM) — read this before proposing
architectural changes.

## Hard constraints (verified 2026-07, machine: M5 Max, 128GB RAM)

1. **The model is 81GB; the machine has 128GB.** Two fully-resident copies
   cannot coexist. This kills naive blue/green at the model layer and is why
   the model swap plan ("red/yellow") uses ds4-server's `--ssd-streaming`
   mode: the incoming instance runs partially-resident during the switchover,
   then restarts fully resident after the old one exits (two-phase).
2. **ds4-server executes ONE request at a time.** Inference is serialized
   through a single graph worker (upstream README, "HTTP server" section);
   concurrent HTTP requests just queue internally. Therefore fairness lives
   in the gateway as weighted fair queuing (stride scheduling) — there is no
   point sending the backend more than one request at once, and the e2e suite
   asserts the gateway never does.
3. **ds4-server keeps ONE mutable KV prefix checkpoint.** Interleaving
   different users' conversations destroys prefix-cache reuse. That is what
   `sticky_extra_turns` is for: a user may take a couple of consecutive turns
   (only while others wait; bounded) before the scheduler rotates.
4. **ds4-server has no authentication.** It must only ever bind 127.0.0.1.
   All exposure goes through the gateway, which also binds 127.0.0.1 and is
   published tailnet-only via `tailscale serve`.
5. **ds4-server mmaps the GGUF**: it reports healthy within seconds but pages
   weights in lazily, so the first requests after a (re)start are slow.
   `--warm-weights` exists upstream if this matters.
6. **`cwd` must be the ds4 checkout** when spawning ds4-server (Metal shader
   files resolve relative to it). `backend.py` passes `cwd=ds4_dir` for this
   reason; do not "simplify" it away.

## Identity model

- `tailscale serve` injects `Tailscale-User-Login` on proxied requests. The
  header is trustworthy ONLY because the gateway binds loopback: the sole
  network path to it is through local tailscaled. If the bind address ever
  changes, this assumption breaks and the header becomes spoofable.
- One tailscale login covers all of a person's devices — that is the user
  identifier for fairness weights. The OpenAI `user` body field is
  self-reported and used only as a last-resort label (prefixed `claimed:`).
- Bare loopback connections with no forwarding headers are treated as the
  owner (that is how `ds4ctl` and local curl authenticate to `/admin/*`).

## Gate semantics (deliberate asymmetry)

| Condition | Non-owner | Owner | Model in RAM? |
|---|---|---|---|
| AC + battery >= 80% | served | served | yes |
| On battery / < 80% | 503 `battery_gated` | served | **yes — never unloaded by the power gate** |
| `ds4ctl off` | 503 `manually_disabled` | 503 too | **no — process stopped, ~81GB freed** |

The power gate exists to stop *other people* draining the owner's battery;
the owner still wants the model available off-charger, so it stays loaded.
Manual disable exists to reclaim RAM/GPU (gaming), so it fully stops the
process — for everyone. Disable state (including the `off --for 2h` resume
deadline) persists in `state.json` so a gateway restart mid-disable does not
surprise-reload the model.

## Deployment policy (owner's explicit requirements)

- **The LaunchDaemon must not be loaded until the system is proven** (fear:
  a leaky/crashing build auto-starting at every boot).
- **Live switches never change what boots.** Blue/green (gateway) and
  red/yellow (model) flips affect only the running processes; the boot
  version changes only via an explicit manual promote step. A bad flip must
  not become the boot default.
- Blue/green applies to the gateway layer only (ports 9001/9002; the flip is
  re-pointing `tailscale serve`). The model layer is untouched during a
  gateway flip.

## v2 roadmap notes (owner-confirmed facts)

- Continuous batching in the engine: batch>1 slows MoE but does **not**
  meaningfully increase memory; DeepSeek's MLA makes per-sequence KV caches
  small. Plan: fork antirez/ds4 (clone to `~/dev/ds4_custom`, keep `~/dev/ds4`
  pristine as fallback), reference implementation: `~/repos/evintunador/exo`.
  Alternative path: port the ds4 engine into exo and switch to exo entirely.
- Idea: disk-backed queue of KV caches (e.g. 512GB budget) if restoring from
  SSD beats re-prefill.

## Error codes clients may see

`503 battery_gated` (retry when host is charging), `503 manually_disabled`
(owner turned it off; `resumes_at_epoch` present if a timer is set),
`503 model_loading`, `503 model_stopped`, `429 queue_full` (per-user cap),
`504 queue_timeout`, `502 upstream_error`.
