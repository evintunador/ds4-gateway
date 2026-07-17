"""Runaway-memory watchdog.

Two ceilings, checked every interval:
- model phys footprint over its limit: stop the model and mark it DISABLED
  (persistent, like `ds4ctl off`); the owner investigates and runs
  `ds4ctl on`. Footprint (not ps rss!) is the right metric on Apple
  Silicon: the 81GB of mmap'd weights are clean reclaimable file pages that
  never count against the process, while KV cache, Metal buffers, and any
  actual leak show up as dirty footprint (~6GB healthy).
- gateway's own RSS over its limit: exit the process. Under the LaunchDaemon
  this restarts throttled (ThrottleInterval); under manual/nohup operation it
  stays down — a leaking gateway serving traffic is worse than a dead one.

Limits live in config [watchdog]; no extra deps (`footprint`/`ps`).
"""

import asyncio
import os
import re
import subprocess
import sys
import time

_FOOTPRINT_RE = re.compile(r"Footprint:\s+([\d.]+)\s+(KB|MB|GB)")
_UNIT = {"KB": 1 / 1024, "MB": 1, "GB": 1024}


def rss_mb(pid: int) -> float | None:
    try:
        out = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)],
                             capture_output=True, text=True, timeout=10).stdout.strip()
        return int(out) / 1024 if out else None
    except (ValueError, OSError, subprocess.TimeoutExpired):
        return None


def footprint_mb(pid: int) -> float | None:
    """Physical (dirty) footprint incl. GPU allocations; falls back to rss."""
    try:
        out = subprocess.run(["footprint", str(pid)], capture_output=True,
                             text=True, timeout=15).stdout
        m = _FOOTPRINT_RE.search(out)
        if m:
            return float(m.group(1)) * _UNIT[m.group(2)]
    except (OSError, subprocess.TimeoutExpired):
        pass
    return rss_mb(pid)


class Watchdog:
    def __init__(self, gateway, interval_s: float = 30,
                 gateway_rss_mb: float = 2048, model_footprint_mb: float = 60000):
        self.gw = gateway
        self.interval = interval_s
        self.gateway_limit = gateway_rss_mb
        self.model_limit = model_footprint_mb
        self.events: list[dict] = []

    def _note(self, msg: str):
        print(f"[watchdog] {msg}")
        self.events.append({"t": time.time(), "msg": msg})
        del self.events[:-10]

    async def run(self):
        while True:
            await asyncio.sleep(self.interval)
            try:
                await self._check()
            except Exception as e:
                self._note(f"check error: {e}")

    async def _check(self):
        model = self.gw.model
        if model.managed and model.active.pid and not model.swap_state:
            m = await asyncio.to_thread(footprint_mb, model.active.pid)
            if m is not None and m > self.model_limit:
                self._note(f"model footprint {m:.0f}MB > limit "
                           f"{self.model_limit:.0f}MB; stopping and disabling "
                           "model (ds4ctl on to recover)")
                await model.disable(None)
        g = rss_mb(os.getpid())
        if g is not None and g > self.gateway_limit:
            self._note(f"gateway rss {g:.0f}MB > limit {self.gateway_limit:.0f}MB; exiting")
            sys.stdout.flush()
            os._exit(70)

    def info(self) -> dict:
        return {"interval_s": self.interval,
                "gateway_rss_limit_mb": self.gateway_limit,
                "model_footprint_limit_mb": self.model_limit,
                "events": self.events[-5:]}
