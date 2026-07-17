"""The gateway HTTP server: gating, fairness, and streaming proxy to ds4-server.

Request flow for /v1/*:
  identify user -> manual-disable gate -> power gate (owner bypasses)
  -> fair-scheduler turn -> proxy (streaming) to ds4-server.

Admin API (/admin/*) is allowed for the owner's tailscale login, or for bare
loopback connections (ds4ctl, local curl). Non-owner tailnet users are denied.
"""

import asyncio
import json
import time

import aiohttp
from aiohttp import web

from .backend import ModelBackend
from .identity import TS_LOGIN_HEADER, identify
from .power import PowerMonitor, PowerState
from .scheduler import FairScheduler, QueueFull, QueueTimeout

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
        self.power = PowerMonitor(
            min_battery_percent=cfg.get("power", "min_battery_percent", default=80),
            poll_interval_s=cfg.get("power", "poll_interval_s", default=10),
        )
        self.backend = ModelBackend(
            ds4_dir=cfg.get("model", "ds4_dir"),
            server_bin=cfg.get("model", "server_bin", default="./ds4-server"),
            model_file=cfg.get("model", "model_file", default="ds4flash.gguf"),
            host=cfg.get("model", "host", default="127.0.0.1"),
            port=cfg.get("model", "port", default=8001),
            args=cfg.get("model", "args", default=[]),
            managed=cfg.get("model", "managed", default=True),
            health_timeout_s=cfg.get("model", "health_timeout_s", default=1800),
            log_file=str(cfg.resolve_path(cfg.get("model", "log_file", default="logs/ds4-server.log"))),
            state_file=str(cfg.resolve_path(cfg.get("gateway", "state_file", default="state.json"))),
        )
        self.sched = FairScheduler(
            weights=cfg.get("scheduler", "weights", default={}),
            default_weight=cfg.get("scheduler", "default_weight", default=1),
            sticky_extra_turns=cfg.get("scheduler", "sticky_extra_turns", default=2),
            max_queued_per_user=cfg.get("scheduler", "max_queued_per_user", default=4),
            queue_timeout_s=cfg.get("scheduler", "queue_timeout_s", default=600),
        )
        self.session: aiohttp.ClientSession | None = None
        self.started_at = time.time()
        self._bg: list[asyncio.Task] = []

    # ---- app wiring ------------------------------------------------------

    def build_app(self) -> web.Application:
        app = web.Application()
        app.router.add_get("/admin/status", self.h_status)
        app.router.add_post("/admin/on", self.h_on)
        app.router.add_post("/admin/off", self.h_off)
        app.router.add_post("/admin/power_override", self.h_power_override)
        app.router.add_route("*", "/{path:.*}", self.h_proxy)
        app.on_startup.append(self._startup)
        app.on_cleanup.append(self._cleanup)
        return app

    async def _startup(self, app):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=None, sock_read=600))
        self.backend.load_state()
        self._bg.append(asyncio.create_task(self.power.run()))
        if self.cfg.get("model", "autostart", default=True) and not self.backend.disabled:
            self._bg.append(asyncio.create_task(self._autostart()))

    async def _autostart(self):
        try:
            await self.backend.start()
        except Exception as e:
            print(f"[gateway] model autostart failed: {e}")

    async def _cleanup(self, app):
        for t in self._bg:
            t.cancel()
        if self.session:
            await self.session.close()
        await self.backend.stop()

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
            "uptime_s": round(time.time() - self.started_at, 1),
            "power": {"on_ac": p.on_ac, "percent": p.percent,
                      "serving_allowed": self.power.serving_allowed,
                      "override_active": self.power.override is not None,
                      "min_battery_percent": self.power.min_percent},
            "backend": self.backend.info(),
            "scheduler": self.sched.status(),
            "owner": self.owner,
        })

    async def h_on(self, request):
        if not self._admin_allowed(request):
            return _error(403, "forbidden", "admin access is owner-only")
        try:
            await self.backend.enable()
        except Exception as e:
            return _error(500, "start_failed", str(e))
        return web.json_response({"ok": True, "backend": self.backend.info()})

    async def h_off(self, request):
        if not self._admin_allowed(request):
            return _error(403, "forbidden", "admin access is owner-only")
        duration = None
        if request.can_read_body:
            try:
                duration = (await request.json()).get("duration_s")
            except ValueError:
                pass
        await self.backend.disable(duration)
        return web.json_response({"ok": True, "backend": self.backend.info()})

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

    # ---- proxy -----------------------------------------------------------

    async def h_proxy(self, request: web.Request):
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

        if self.backend.disabled:
            extra = {}
            if self.backend.disabled_until:
                extra["resumes_at_epoch"] = self.backend.disabled_until
            return _error(503, "manually_disabled",
                          "the model server is manually disabled", **extra)
        if not is_owner and not self.power.serving_allowed:
            p = self.power.effective()
            return _error(
                503, "battery_gated",
                f"server is available only on AC power with battery >= "
                f"{self.power.min_percent}% (currently "
                f"{'AC' if p.on_ac else 'battery'}, {p.percent}%); try again later")
        if self.backend.state == "starting":
            return _error(503, "model_loading", "model is loading; try again shortly")
        if self.backend.state != "running":
            return _error(503, "model_stopped", "model server is not running")

        try:
            await self.sched.acquire(login)
        except QueueFull:
            return _error(429, "queue_full",
                          f"user '{login}' already has the maximum number of queued requests")
        except QueueTimeout:
            return _error(504, "queue_timeout", "request timed out waiting for a turn")
        try:
            return await self._forward(request, body)
        finally:
            self.sched.release(login)

    async def _forward(self, request: web.Request, body: bytes):
        url = self.backend.base_url + request.path_qs
        headers = {k: v for k, v in request.headers.items()
                   if k.lower() in _FORWARD_REQ_HEADERS}
        try:
            async with self.session.request(
                    request.method, url, headers=headers,
                    data=body if body else None) as upstream:
                resp = web.StreamResponse(status=upstream.status)
                for k, v in upstream.headers.items():
                    if k.lower() not in _SKIP_RESP_HEADERS:
                        resp.headers[k] = v
                await resp.prepare(request)
                async for chunk in upstream.content.iter_any():
                    await resp.write(chunk)
                await resp.write_eof()
                return resp
        except aiohttp.ClientError as e:
            return _error(502, "upstream_error", f"ds4-server request failed: {e}")
