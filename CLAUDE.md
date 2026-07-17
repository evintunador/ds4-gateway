# Working in this repo

- Read `docs/DESIGN.md` first: it records the hardware/engine constraints and
  policy decisions that are NOT visible in the code. Most "why is it built
  this way" questions are answered there.
- `docs/OPERATIONS.md` is the runbook for the live deployment.
- This gateway fronts a LIVE service. Before restarting anything, run
  `bin/ds4ctl status` — real users may have requests in flight.
- Never modify `~/dev/ds4` (the inference engine). It is a pristine upstream
  checkout of antirez/ds4; engine changes belong in a separate fork (v2 plan).
- Test without loading the 81GB model: `uv run python tests/test_e2e.py`
  (mock backend). Live traffic simulation: `tools/simulate.py`.
