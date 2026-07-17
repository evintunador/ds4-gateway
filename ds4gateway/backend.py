"""Model process lifecycle: spawn/adopt/stop ds4-server, disable timers, swaps.

Key property for blue/green gateway deploys: ds4-server must SURVIVE gateway
restarts. It is spawned in its own session with a pidfile under the shared
state dir, so a newly started gateway adopts the already-running model
(ModelProcess.adopt) instead of spawning a second 81GB copy, and an outgoing
gateway can release management without killing it.

Disable semantics per design: manual disable fully stops ds4-server to free
the wired model memory; the power gate elsewhere never unloads. Disabled
state (with any resume deadline) and the active model port persist in
state.json so restarts don't surprise-reload or lose track of the live port.
"""

import asyncio
import json
import os
import signal
import subprocess
import time
from pathlib import Path

import aiohttp


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError):
        return False
    except OSError:
        return False


class ModelProcess:
    """One ds4-server instance on one port: spawn, adopt, health, stop."""

    def __init__(self, ds4_dir: str, server_bin: str, model_file: str,
                 host: str, port: int, args: list[str],
                 health_timeout_s: float, log_file: str, run_dir: Path):
        self.ds4_dir = ds4_dir
        self.server_bin = server_bin
        self.model_file = model_file
        self.host = host
        self.port = port
        self.args = list(args)
        self.health_timeout = health_timeout_s
        self.log_file = log_file
        self.run_dir = run_dir
        self.proc: asyncio.subprocess.Process | None = None
        self.pid: int | None = None
        self.state = "stopped"  # stopped | starting | running

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    @property
    def pidfile(self) -> Path:
        return self.run_dir / f"model-{self.port}.pid"

    async def health(self) -> bool:
        try:
            timeout = aiohttp.ClientTimeout(total=5)
            async with aiohttp.ClientSession(timeout=timeout) as s:
                async with s.get(f"{self.base_url}/v1/models") as r:
                    return r.status == 200
        except Exception:
            return False

    async def adopt(self) -> bool:
        """Attach to an already-running ds4-server on this port, if any."""
        pid = None
        try:
            pid = int(self.pidfile.read_text().strip())
        except (OSError, ValueError):
            # no/garbage pidfile; maybe an instance predating pidfiles (or
            # orphaned) is still serving — recover its pid from the process table
            out = subprocess.run(["pgrep", "-f", f"ds4-server.*--port {self.port}"],
                                 capture_output=True, text=True).stdout.split()
            pid = int(out[0]) if out else None
        if pid and _pid_alive(pid) and await self.health():
            self.pid = pid
            self.pidfile.write_text(str(pid))
            self.state = "running"
            return True
        return False

    async def start(self):
        if self.state == "running":
            return
        self.state = "starting"
        os.makedirs(os.path.dirname(self.log_file) or ".", exist_ok=True)
        log = open(self.log_file, "ab")
        try:
            self.proc = await asyncio.create_subprocess_exec(
                self.server_bin, "-m", self.model_file, *self.args,
                "--host", self.host, "--port", str(self.port),
                cwd=self.ds4_dir, stdout=log, stderr=asyncio.subprocess.STDOUT,
                start_new_session=True,  # survive gateway death
            )
        finally:
            log.close()
        self.pid = self.proc.pid
        self.pidfile.write_text(str(self.pid))
        deadline = time.time() + self.health_timeout
        while time.time() < deadline:
            if self.proc.returncode is not None:
                self.state = "stopped"
                self._clear_pidfile()
                raise RuntimeError(
                    f"ds4-server (port {self.port}) exited with code "
                    f"{self.proc.returncode} during startup (see {self.log_file})")
            if await self.health():
                self.state = "running"
                return
            await asyncio.sleep(3)
        await self.stop()
        raise RuntimeError(f"ds4-server (port {self.port}) not healthy before timeout")

    async def stop(self):
        self.state = "stopped"
        if self.proc and self.proc.returncode is None:
            self.proc.terminate()
            try:
                await asyncio.wait_for(self.proc.wait(), timeout=30)
            except asyncio.TimeoutError:
                self.proc.kill()
                await self.proc.wait()
        elif self.pid and _pid_alive(self.pid):
            os.kill(self.pid, signal.SIGTERM)
            for _ in range(30):
                if not _pid_alive(self.pid):
                    break
                await asyncio.sleep(1)
            else:
                os.kill(self.pid, signal.SIGKILL)
        self._clear_pidfile()
        self.proc = None
        self.pid = None

    def _clear_pidfile(self):
        try:
            self.pidfile.unlink()
        except OSError:
            pass


class ModelManager:
    """Owns the active ModelProcess, disable state, and red/yellow swaps."""

    def __init__(self, cfg, run_dir: Path):
        self.cfg = cfg
        self.run_dir = run_dir
        self.managed = cfg.get("model", "managed", default=True)
        self.disabled = False
        self.disabled_until: float | None = None
        self.swap_state: str | None = None
        self._resume_task: asyncio.Task | None = None
        self.active = self._make(cfg, cfg.get("model", "port", default=8001))

    def _make(self, cfg, port: int, extra_args: list[str] | None = None) -> ModelProcess:
        return ModelProcess(
            ds4_dir=cfg.get("model", "ds4_dir"),
            server_bin=cfg.get("model", "server_bin", default="./ds4-server"),
            model_file=cfg.get("model", "model_file", default="ds4flash.gguf"),
            host=cfg.get("model", "host", default="127.0.0.1"),
            port=port,
            args=cfg.get("model", "args", default=[]) + (extra_args or []),
            health_timeout_s=cfg.get("model", "health_timeout_s", default=1800),
            log_file=str(cfg.resolve_path(cfg.get("model", "log_file",
                                                  default="logs/ds4-server.log"))),
            run_dir=self.run_dir,
        )

    def _alt_port(self, port: int) -> int:
        a = self.cfg.get("model", "port", default=8001)
        b = self.cfg.get("model", "alt_port", default=a + 1)
        return b if port == a else a

    # ---- persistence ------------------------------------------------------

    @property
    def state_file(self) -> Path:
        return self.run_dir / "state.json"

    def _save_state(self):
        try:
            self.state_file.write_text(json.dumps({
                "disabled": self.disabled,
                "disabled_until": self.disabled_until,
                "model_port": self.active.port,
            }))
        except OSError:
            pass

    # ---- gateway startup ---------------------------------------------------

    async def startup(self, autostart: bool):
        saved = {}
        try:
            saved = json.loads(self.state_file.read_text())
        except (OSError, ValueError):
            pass
        until = saved.get("disabled_until")
        if saved.get("disabled") and not (until and until <= time.time()):
            self.disabled = True
            self.disabled_until = until
            if until:
                self._resume_task = asyncio.create_task(
                    self._resume_after(until - time.time()))
            return
        if not self.managed:
            self.active.state = "running"
            return
        # adopt a survivor: last known port first, then both configured ports
        cfg_port = self.cfg.get("model", "port", default=8001)
        for port in dict.fromkeys(
                [saved.get("model_port"), cfg_port, self._alt_port(cfg_port)]):
            if port is None:
                continue
            candidate = self._make(self.cfg, port)
            if await candidate.adopt():
                self.active = candidate
                self._save_state()
                return
        if autostart:
            await self.active.start()
            self._save_state()

    # ---- enable / disable --------------------------------------------------

    async def disable(self, duration_s: float | None = None):
        if self.swap_state:
            raise RuntimeError("model swap in progress; try again when it finishes")
        self.disabled = True
        self.disabled_until = time.time() + duration_s if duration_s else None
        if self._resume_task and self._resume_task is not asyncio.current_task():
            self._resume_task.cancel()
        self._resume_task = None
        self._save_state()
        if self.managed:
            await self.active.stop()
        else:
            self.active.state = "stopped"
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
        if self.managed:
            if not await self.active.adopt():
                await self.active.start()
        else:
            self.active.state = "running"

    def release(self):
        """Stop managing the model without stopping it (blue/green handoff)."""
        self.managed = False

    async def shutdown_cleanup(self):
        if self.managed:
            await self.active.stop()

    # ---- red/yellow swap ----------------------------------------------------

    async def swap(self, sched):
        """Two-phase zero-hard-downtime model swap. See docs/DESIGN.md.

        Phase 1: bring up the other port with --ssd-streaming and a small
        expert cache (fits beside the resident model), flip, stop old.
        Phase 2: bring up a fresh fully-resident instance on the vacated
        port, flip back, stop the streaming stopgap.
        Flips take a scheduler-exclusive turn so no request straddles them.
        Config is re-read at the start, so edit config.toml (new model file,
        args, binary) before running a swap.
        """
        if not self.managed:
            raise RuntimeError("model is not managed by this gateway")
        if self.disabled:
            raise RuntimeError("model is disabled; enable it first")
        if self.swap_state:
            raise RuntimeError("swap already in progress")
        from .config import Config
        cfg = Config.load(self.cfg.path)
        streaming_args = cfg.get("model", "swap_streaming_args",
                                 default=["--ssd-streaming",
                                          "--ssd-streaming-cache-experts", "24GB"])
        old = self.active
        other = self._alt_port(old.port)
        try:
            self.swap_state = f"phase1: starting streaming instance on :{other}"
            yellow = self._make(cfg, other, extra_args=streaming_args)
            await yellow.start()
            self.swap_state = f"phase1: flipping traffic to :{other}"
            await sched.acquire("__swap__")
            try:
                self.active = yellow
                self._save_state()
            finally:
                sched.release("__swap__")
            self.swap_state = f"phase1: stopping old instance on :{old.port}"
            await old.stop()
            self.swap_state = f"phase2: starting resident instance on :{old.port}"
            resident = self._make(cfg, old.port)
            await resident.start()
            self.swap_state = f"phase2: flipping traffic to :{old.port}"
            await sched.acquire("__swap__")
            try:
                self.active = resident
                self._save_state()
            finally:
                sched.release("__swap__")
            self.swap_state = f"phase2: stopping streaming instance on :{other}"
            await yellow.stop()
            self.cfg = cfg
            self.swap_state = None
        except Exception as e:
            self.swap_state = f"FAILED: {e}"
            raise

    def info(self) -> dict:
        return {
            "state": self.active.state,
            "port": self.active.port,
            "disabled": self.disabled,
            "disabled_until": self.disabled_until,
            "managed": self.managed,
            "pid": self.active.pid,
            "base_url": self.active.base_url,
            "swap_state": self.swap_state,
        }
