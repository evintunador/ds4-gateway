"""Runaway-memory watchdog.

Two ceilings, checked every interval:
- model RSS over its limit: stop the model and mark it DISABLED (persistent,
  like `ds4ctl off`) so a leaking engine can't wire the whole machine; the
  owner investigates and runs `ds4ctl on`.
- gateway's own RSS over its limit: exit the process. Under the LaunchDaemon
  this restarts throttled (ThrottleInterval); under manual/nohup operation it
  stays down — a leaking gateway serving traffic is worse than a dead one.

Limits live in config [watchdog]; RSS read via `ps` (no extra deps).
"""

import asyncio
import os
import subprocess
import sys
import time


def rss_mb(pid: int) -> float | None:
    try:
        out = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)],
                             capture_output=True, text=True, timeout=10).stdout.strip()
        return int(out) / 1024 if out else None
    except (ValueError, OSError, subprocess.TimeoutExpired):
        return None


class Watchdog:
    def __init__(self, gateway, interval_s: float = 30,
                 gateway_rss_mb: float = 2048, model_rss_mb: float = 115000):
        self.gw = gateway
        self.interval = interval_s
        self.gateway_limit = gateway_rss_mb
        self.model_limit = model_rss_mb
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
            m = rss_mb(model.active.pid)
            if m is not None and m > self.model_limit:
                self._note(f"model rss {m:.0f}MB > limit {self.model_limit:.0f}MB; "
                           "stopping and disabling model (ds4ctl on to recover)")
                await model.disable(None)
        g = rss_mb(os.getpid())
        if g is not None and g > self.gateway_limit:
            self._note(f"gateway rss {g:.0f}MB > limit {self.gateway_limit:.0f}MB; exiting")
            sys.stdout.flush()
            os._exit(70)

    def info(self) -> dict:
        return {"interval_s": self.interval,
                "gateway_rss_limit_mb": self.gateway_limit,
                "model_rss_limit_mb": self.model_limit,
                "events": self.events[-5:]}
