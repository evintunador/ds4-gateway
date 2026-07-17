"""ds4-server process lifecycle: start, stop, disable (with optional timer).

Disable semantics per design: manual disable fully stops ds4-server to free
the ~81GB of wired model memory; the power gate elsewhere never unloads.
Disabled state (including the resume deadline) persists to a state file so a
gateway restart mid-disable doesn't surprise-reload the model.
"""

import asyncio
import json
import os
import time

import aiohttp


class ModelBackend:
    def __init__(self, ds4_dir: str, server_bin: str, model_file: str,
                 host: str, port: int, args: list[str], managed: bool,
                 health_timeout_s: float, log_file: str, state_file: str):
        self.ds4_dir = ds4_dir
        self.server_bin = server_bin
        self.model_file = model_file
        self.host = host
        self.port = port
        self.args = list(args)
        self.managed = managed
        self.health_timeout = health_timeout_s
        self.log_file = log_file
        self.state_file = state_file
        self.proc: asyncio.subprocess.Process | None = None
        self.state = "stopped"  # stopped | starting | running
        self.disabled = False
        self.disabled_until: float | None = None
        self._resume_task: asyncio.Task | None = None

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    # ---- persistence ----------------------------------------------------

    def _save_state(self):
        try:
            with open(self.state_file, "w") as f:
                json.dump({"disabled": self.disabled,
                           "disabled_until": self.disabled_until}, f)
        except OSError:
            pass

    def load_state(self):
        try:
            with open(self.state_file) as f:
                data = json.load(f)
        except (OSError, ValueError):
            return
        until = data.get("disabled_until")
        if data.get("disabled"):
            if until and until <= time.time():
                return  # timer expired while gateway was down
            self.disabled = True
            self.disabled_until = until
            if until:
                self._resume_task = asyncio.create_task(
                    self._resume_after(until - time.time()))

    # ---- health ----------------------------------------------------------

    async def health(self) -> bool:
        try:
            timeout = aiohttp.ClientTimeout(total=5)
            async with aiohttp.ClientSession(timeout=timeout) as s:
                async with s.get(f"{self.base_url}/v1/models") as r:
                    return r.status == 200
        except Exception:
            return False

    # ---- lifecycle -------------------------------------------------------

    async def start(self):
        if self.disabled:
            return
        if not self.managed:
            self.state = "running"
            return
        if self.proc and self.proc.returncode is None:
            return
        self.state = "starting"
        os.makedirs(os.path.dirname(self.log_file) or ".", exist_ok=True)
        log = open(self.log_file, "ab")
        try:
            self.proc = await asyncio.create_subprocess_exec(
                self.server_bin, "-m", self.model_file, *self.args,
                "--host", self.host, "--port", str(self.port),
                cwd=self.ds4_dir, stdout=log, stderr=asyncio.subprocess.STDOUT,
            )
        finally:
            log.close()
        deadline = time.time() + self.health_timeout
        while time.time() < deadline:
            if self.proc.returncode is not None:
                self.state = "stopped"
                raise RuntimeError(
                    f"ds4-server exited with code {self.proc.returncode} during startup"
                    f" (see {self.log_file})")
            if await self.health():
                self.state = "running"
                return
            await asyncio.sleep(3)
        await self.stop()
        raise RuntimeError("ds4-server did not become healthy before timeout")

    async def stop(self):
        self.state = "stopped"
        if not self.managed or not self.proc or self.proc.returncode is not None:
            return
        self.proc.terminate()
        try:
            await asyncio.wait_for(self.proc.wait(), timeout=30)
        except asyncio.TimeoutError:
            self.proc.kill()
            await self.proc.wait()

    async def disable(self, duration_s: float | None = None):
        self.disabled = True
        self.disabled_until = time.time() + duration_s if duration_s else None
        if self._resume_task:
            self._resume_task.cancel()
            self._resume_task = None
        self._save_state()
        await self.stop()
        if duration_s:
            self._resume_task = asyncio.create_task(self._resume_after(duration_s))

    async def _resume_after(self, delay_s: float):
        await asyncio.sleep(max(delay_s, 0))
        await self.enable()

    async def enable(self):
        self.disabled = False
        self.disabled_until = None
        if self._resume_task and self._resume_task is not asyncio.current_task():
            self._resume_task.cancel()
        self._resume_task = None
        self._save_state()
        await self.start()

    def info(self) -> dict:
        return {
            "state": self.state,
            "disabled": self.disabled,
            "disabled_until": self.disabled_until,
            "managed": self.managed,
            "pid": self.proc.pid if self.proc and self.proc.returncode is None else None,
            "base_url": self.base_url,
        }
