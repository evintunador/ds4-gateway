"""The gateway HTTP server: gating, fairness, and streaming proxy to ds4-server.

Request flow for /v1/*:
  identify user -> manual-disable gate -> power gate (owner bypasses)
  -> fair-scheduler turn -> proxy (streaming) to the active ds4-server.

Admin API (/admin/*) is allowed for the owner's tailscale login, or for bare
loopback connections (ds4ctl, local curl). Non-owner tailnet users are denied.
"""

import asyncio
import json
import os
import signal
import time
from pathlib import Path

import aiohttp
from aiohttp import web

from .backend import ModelManager
from .identity import TS_LOGIN_HEADER, identify
from .metrics import INFERENCE_PATHS, UsageLog
from .power import PowerMonitor, PowerState
from .scheduler import FairScheduler, QueueFull, QueueTimeout
from .watchdog import Watchdog

_FORWARD_REQ_HEADERS = {"content-type", "accept"}
_SKIP_RESP_HEADERS = {"transfer-encoding", "connection", "content-length", "content-encoding"}


def _error(status: int, code: str, message: str, **extra) -> web.Response:
    body = {"error": {"message": message, "type": "gateway_error", "code": code, **extra}}
    return web.json_response(body, status=status)


class Gateway:
    def __init__(self, cfg):
        self.cfg = cfg
        self.owner = cfg.get("owner", "login")
        self.color = cfg.get("gateway", "color", default="blue")
        run_dir = Path(cfg.get("gateway", "state_dir",
                               default="~/.local/state/ds4-gateway")).expanduser()
        run_dir.mkdir(parents=True, exist_ok=True)
        self.power = PowerMonitor(
            min_battery_percent=cfg.get("power", "min_battery_percent", default=80),
            poll_interval_s=cfg.get("power", "poll_interval_s", default=10),
        )
        self.model = ModelManager(cfg, run_dir)
        self.sched = FairScheduler(
            weights=cfg.get("scheduler", "weights", default={}),
            default_weight=cfg.get("scheduler", "default_weight", default=1),
            sticky_extra_turns=cfg.get("scheduler", "sticky_extra_turns", default=2),
            max_queued_per_user=cfg.get("scheduler", "max_queued_per_user", default=4),
            queue_timeout_s=cfg.get("scheduler", "queue_timeout_s", default=600),
        )
        self.watchdog = Watchdog(
            self,
            interval_s=cfg.get("watchdog", "interval_s", default=30),
            gateway_rss_mb=cfg.get("watchdog", "gateway_rss_mb", default=2048),
            model_rss_mb=cfg.get("watchdog", "model_rss_mb", default=115000),
        )
        self.usage = UsageLog(
            run_dir / "usage.jsonl",
            enabled=cfg.get("metrics", "enabled", default=True))
        self.run_dir = run_dir
        self._load_weight_overrides()
        self.session: aiohttp.ClientSession | None = None
        self.started_at = time.time()
        self._bg: list[asyncio.Task] = []
        self._swap_task: asyncio.Task | None = None

    def _load_weight_overrides(self):
        self._weight_overrides = {}
        try:
            self._weight_overrides = json.loads(
                (self.run_dir / "weights.json").read_text())
            self.sched.weights.update(self._weight_overrides)
        except (OSError, ValueError):
            pass

    # ---- app wiring ------------------------------------------------------

    def build_app(self) -> web.Application:
        app = web.Application()
        app.router.add_get("/admin/status", self.h_status)
        app.router.add_post("/admin/on", self.h_on)
        app.router.add_post("/admin/off", self.h_off)
        app.router.add_post("/admin/power_override", self.h_power_override)
        app.router.add_post("/admin/model_swap", self.h_model_swap)
        app.router.add_post("/admin/shutdown", self.h_shutdown)
        app.router.add_get("/admin/weights", self.h_weights)
        app.router.add_post("/admin/weights", self.h_weights)
        app.router.add_route("*", "/{path:.*}", self.h_proxy)
        app.on_startup.append(self._startup)
        app.on_cleanup.append(self._cleanup)
        return app

    async def _startup(self, app):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=None, sock_read=600))
        self._bg.append(asyncio.create_task(self.power.run()))
        self._bg.append(asyncio.create_task(self.watchdog.run()))
        self._bg.append(asyncio.create_task(self._model_startup()))

    async def _model_startup(self):
        try:
            await self.model.startup(
                autostart=self.cfg.get("model", "autostart", default=True))
        except Exception as e:
            print(f"[gateway] model startup failed: {e}")

    async def _cleanup(self, app):
        for t in self._bg:
            t.cancel()
        if self.session:
            await self.session.close()
        await self.model.shutdown_cleanup()

    # ---- admin -----------------------------------------------------------

    def _admin_allowed(self, request: web.Request) -> bool:
        if request.headers.get(TS_LOGIN_HEADER) == self.owner:
            return True
        return (request.remote in ("127.0.0.1", "::1")
                and TS_LOGIN_HEADER not in request.headers
                and "X-Forwarded-For" not in request.headers)

    async def h_status(self, request):
        if not self._admin_allowed(request):
            return _error(403, "forbidden", "admin access is owner-only")
        p = self.power.effective()
        return web.json_response({
            "color": self.color,
            "port": self.cfg.get("gateway", "port", default=9001),
            "uptime_s": round(time.time() - self.started_at, 1),
            "power": {"on_ac": p.on_ac, "percent": p.percent,
                      "serving_allowed": self.power.serving_allowed,
                      "override_active": self.power.override is not None,
                      "min_battery_percent": self.power.min_percent},
            "backend": self.model.info(),
            "scheduler": self.sched.status(),
            "watchdog": self.watchdog.info(),
            "owner": self.owner,
            "state_dir": str(self.run_dir),
            "usage_log": str(self.usage.path),
        })

    async def h_weights(self, request):
        if not self._admin_allowed(request):
            return _error(403, "forbidden", "admin access is owner-only")
        if request.method == "POST":
            data = await request.json()
            login = data["login"]
            if data.get("clear"):
                self._weight_overrides.pop(login, None)
                cfg_w = self.cfg.get("scheduler", "weights", default={}).get(login)
                if cfg_w is not None:
                    self.sched.weights[login] = cfg_w
                else:
                    self.sched.weights.pop(login, None)
            else:
                w = float(data["weight"])
                self._weight_overrides[login] = w
                self.sched.weights[login] = w
            try:
                (self.run_dir / "weights.json").write_text(
                    json.dumps(self._weight_overrides))
            except OSError:
                pass
        return web.json_response({"weights": self.sched.weights,
                                  "overrides": self._weight_overrides,
                                  "default": self.sched.default_weight})

    async def h_on(self, request):
        if not self._admin_allowed(request):
            return _error(403, "forbidden", "admin access is owner-only")
        try:
            await self.model.enable()
        except Exception as e:
            return _error(500, "start_failed", str(e))
        return web.json_response({"ok": True, "backend": self.model.info()})

    async def h_off(self, request):
        if not self._admin_allowed(request):
            return _error(403, "forbidden", "admin access is owner-only")
        duration = None
        if request.can_read_body:
            try:
                duration = (await request.json()).get("duration_s")
            except ValueError:
                pass
        try:
            await self.model.disable(duration)
        except RuntimeError as e:
            return _error(409, "busy", str(e))
        return web.json_response({"ok": True, "backend": self.model.info()})

    async def h_power_override(self, request):
        if not self._admin_allowed(request):
            return _error(403, "forbidden", "admin access is owner-only")
        data = await request.json()
        if data.get("clear"):
            self.power.override = None
        else:
            self.power.override = PowerState(
                on_ac=bool(data["on_ac"]), percent=int(data["percent"]),
                read_at=time.time())
        return web.json_response({"ok": True, "serving_allowed": self.power.serving_allowed})

    async def h_model_swap(self, request):
        if not self._admin_allowed(request):
            return _error(403, "forbidden", "admin access is owner-only")
        if self._swap_task and not self._swap_task.done():
            return _error(409, "swap_in_progress", self.model.swap_state or "swap running")
        if self.model.swap_state and self.model.swap_state.startswith("FAILED"):
            self.model.swap_state = None  # allow retry after a failed swap
        try:
            self._swap_task = asyncio.create_task(self.model.swap(self.sched))
        except RuntimeError as e:
            return _error(409, "swap_rejected", str(e))
        return web.json_response({"ok": True,
                                  "note": "swap started; poll /admin/status"})

    async def h_shutdown(self, request):
        """Graceful exit for blue/green handoff: drain, optionally keep the model."""
        if not self._admin_allowed(request):
            return _error(403, "forbidden", "admin access is owner-only")
        keep_model = True
        if request.can_read_body:
            try:
                keep_model = (await request.json()).get("keep_model", True)
            except ValueError:
                pass
        if keep_model:
            self.model.release()
        asyncio.create_task(self._exit_soon())
        return web.json_response({"ok": True, "keep_model": keep_model})

    async def _exit_soon(self):
        deadline = time.time() + 60
        while time.time() < deadline:
            s = self.sched.status()
            if s["busy_user"] is None and not s["queued"]:
                break
            await asyncio.sleep(0.5)
        print(f"[gateway] {self.color} exiting after graceful drain")
        os.kill(os.getpid(), signal.SIGTERM)

    # ---- proxy -----------------------------------------------------------

    async def h_proxy(self, request: web.Request):
        arrived = time.time()
        body = await request.read()
        body_user = None
        if body:
            try:
                parsed = json.loads(body)
                if isinstance(parsed, dict):
                    body_user = parsed.get("user")
            except ValueError:
                pass
        login, source = await identify(request, body_user)
        is_owner = login == self.owner or source == "loopback"
        is_inference = request.path in INFERENCE_PATHS and request.method == "POST"

        def rejected(status, code, message, **extra):
            if is_inference:
                self.usage.record(user=login, path=request.path,
                                  status=status, code=code)
            return _error(status, code, message, **extra)

        if self.model.disabled:
            extra = {}
            if self.model.disabled_until:
                extra["resumes_at_epoch"] = self.model.disabled_until
            return rejected(503, "manually_disabled",
                            "the model server is manually disabled", **extra)
        if not is_owner and not self.power.serving_allowed:
            p = self.power.effective()
            return rejected(
                503, "battery_gated",
                f"server is available only on AC power with battery >= "
                f"{self.power.min_percent}% (currently "
                f"{'AC' if p.on_ac else 'battery'}, {p.percent}%); try again later")
        if self.model.active.state == "starting":
            return rejected(503, "model_loading", "model is loading; try again shortly")
        if self.model.active.state != "running":
            return rejected(503, "model_stopped", "model server is not running")

        try:
            await self.sched.acquire(login)
        except QueueFull:
            return rejected(429, "queue_full",
                            f"user '{login}' already has the maximum number of queued requests")
        except QueueTimeout:
            return rejected(504, "queue_timeout", "request timed out waiting for a turn")
        wait_s = time.time() - arrived
        try:
            return await self._forward(request, body, login, arrived, wait_s)
        finally:
            self.sched.release(login)

    async def _forward(self, request: web.Request, body: bytes,
                       login: str = "?", arrived: float | None = None,
                       wait_s: float = 0.0):
        # resolve the active backend at dispatch time — a red/yellow swap may
        # have flipped the pointer while this request was queued
        url = self.model.active.base_url + request.path_qs
        headers = {k: v for k, v in request.headers.items()
                   if k.lower() in _FORWARD_REQ_HEADERS}
        is_inference = request.path in INFERENCE_PATHS and request.method == "POST"
        arrived = arrived or time.time()
        t0 = time.time()
        ttft = None
        buf = b""
        sse_chunks = 0
        is_sse = False
        try:
            async with self.session.request(
                    request.method, url, headers=headers,
                    data=body if body else None) as upstream:
                is_sse = "text/event-stream" in upstream.headers.get("Content-Type", "")
                resp = web.StreamResponse(status=upstream.status)
                for k, v in upstream.headers.items():
                    if k.lower() not in _SKIP_RESP_HEADERS:
                        resp.headers[k] = v
                await resp.prepare(request)
                async for chunk in upstream.content.iter_any():
                    if ttft is None:
                        ttft = time.time() - t0
                    if is_inference:
                        if is_sse:
                            sse_chunks += chunk.count(b'"chat.completion.chunk"')
                        elif len(buf) < 262144:
                            buf += chunk
                    await resp.write(chunk)
                await resp.write_eof()
                if is_inference:
                    self._record_usage(request.path, login, upstream.status,
                                       arrived, wait_s, ttft, is_sse, buf, sse_chunks)
                return resp
        except aiohttp.ClientError as e:
            if is_inference:
                self.usage.record(user=login, path=request.path, status=502,
                                  code="upstream_error",
                                  latency_s=round(time.time() - arrived, 3))
            return _error(502, "upstream_error", f"ds4-server request failed: {e}")

    def _record_usage(self, path, login, status, arrived, wait_s, ttft,
                      is_sse, buf, sse_chunks):
        pt = ct = None
        estimated = False
        if not is_sse and buf:
            try:
                u = json.loads(buf).get("usage") or {}
                pt = u.get("prompt_tokens", u.get("input_tokens"))
                ct = u.get("completion_tokens", u.get("output_tokens"))
            except (ValueError, AttributeError):
                pass
        elif is_sse and sse_chunks:
            ct = max(sse_chunks - 1, 0)  # final chunk carries no token
            estimated = True
        self.usage.record(
            user=login, path=path, status=status,
            latency_s=round(time.time() - arrived, 3),
            wait_s=round(wait_s, 3),
            ttft_s=round(ttft, 3) if ttft is not None else None,
            stream=is_sse, prompt_tokens=pt, completion_tokens=ct,
            estimated=estimated)
