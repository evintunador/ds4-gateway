# Working in this repo (agents)

This fronts a LIVE service with real users. Run `bin/ds4ctl status` before
restarting anything, and prefer `ds4ctl deploy` / `swap-model` (zero
downtime) over killing processes.

Read `docs/DESIGN.md` before proposing architectural changes — it records
the constraints that shaped everything. `docs/OPERATIONS.md` is the runbook.
The owner's deployment policy (promote-gated boot, daemon never
auto-installed) is deliberate; do not "fix" it.

Non-obvious invariants (each has broken something at least once):

- Never modify `~/dev/ds4` — pristine upstream engine; forks go elsewhere.
- ds4-server must SURVIVE gateway replacement: spawned `start_new_session`
  with per-port pidfiles in the shared state dir; new gateways ADOPT before
  spawning. Don't make it a plain child process again.
- `DS4_LOCK_FILE` is set per port because upstream's global single-instance
  lock would veto the deliberate two-instance overlap during swaps.
- The blue/green traffic flip IS `tailscale serve --bg <port>`. `current`
  (boot) moves only on promote; `live` moves on deploy.
- Identity headers are trustworthy ONLY because the gateway binds loopback.
  Changing the bind address silently breaks authentication.
- Watch model memory via dirty phys footprint (`footprint`), never ps rss:
  mmap'd weights are clean reclaimable pages (~130MB rss for an 81GB model).
- The backend is serial (one graph worker) with ONE in-RAM KV checkpoint;
  scheduler stickiness and the disk KV cache both exist because of this.
- `--kv-disk-dir` uses a `{port}` template so swap-overlapping instances
  never share a KV dir. KV checkpoints are conversation-derived state on
  disk — owner-approved; never add plaintext logging of bodies.
- `ds4ctl` runs on system Python 3.9: stdlib only, `from __future__ import
  annotations` required.
- Deploys archive committed HEAD only — commit before `ds4ctl deploy`.

Testing without touching the 81GB model: `tests/test_e2e.py` (mock),
`tests/test_lifecycle.py` (fake ds4-server incl. swaps/watchdog). Pipe test
output to a file — spawned gateways inherit stdout and can hang a pipe.
