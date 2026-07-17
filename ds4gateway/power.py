"""Battery / AC state via pmset, with an override hook for testing."""

import asyncio
import re
import subprocess
import time
from dataclasses import dataclass

_SOURCE_RE = re.compile(r"drawing from '([^']+)'")
_PERCENT_RE = re.compile(r"(\d{1,3})%")


@dataclass
class PowerState:
    on_ac: bool
    percent: int
    read_at: float


class PowerMonitor:
    def __init__(self, min_battery_percent: int = 80, poll_interval_s: int = 10):
        self.min_percent = min_battery_percent
        self.poll_interval = poll_interval_s
        self.override: PowerState | None = None
        self.state = self._read()

    @staticmethod
    def _read() -> PowerState:
        out = subprocess.run(
            ["pmset", "-g", "batt"], capture_output=True, text=True, timeout=10
        ).stdout
        src = _SOURCE_RE.search(out)
        on_ac = bool(src and "AC" in src.group(1))
        pct = _PERCENT_RE.search(out)
        # No battery line (desktop Mac / parse failure): treat as fully charged.
        percent = int(pct.group(1)) if pct else 100
        return PowerState(on_ac=on_ac, percent=percent, read_at=time.time())

    async def run(self):
        while True:
            await asyncio.sleep(self.poll_interval)
            try:
                self.state = await asyncio.to_thread(self._read)
            except Exception:
                pass  # keep last known state; next poll retries

    def effective(self) -> PowerState:
        return self.override or self.state

    @property
    def serving_allowed(self) -> bool:
        s = self.effective()
        return s.on_ac and s.percent >= self.min_percent
