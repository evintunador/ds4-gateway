"""Resolve who is making a request.

Priority order:
1. Tailscale-User-Login header — injected by `tailscale serve`, unspoofable
   (the gateway binds 127.0.0.1, so only the local tailscaled proxy or local
   processes can connect).
2. `tailscale whois` on the X-Forwarded-For address — authoritative fallback.
3. Bare loopback connection with no forwarding header — a local process
   (ds4ctl, local curl): treated as the machine owner.
4. Self-reported "user" field from the request body — a label only, prefixed
   so it can never collide with a real tailscale login.
"""

import asyncio
import json
import time

TS_LOGIN_HEADER = "Tailscale-User-Login"
LOCAL_USER = "local"

_whois_cache: dict[str, tuple[float, str | None]] = {}
_WHOIS_TTL_S = 300


async def _whois(ip: str) -> str | None:
    hit = _whois_cache.get(ip)
    if hit and time.time() - hit[0] < _WHOIS_TTL_S:
        return hit[1]
    login = None
    try:
        proc = await asyncio.create_subprocess_exec(
            "tailscale", "whois", "--json", ip,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
        )
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=5)
        login = json.loads(out).get("UserProfile", {}).get("LoginName")
    except Exception:
        pass
    _whois_cache[ip] = (time.time(), login)
    return login


async def identify(request, body_user: str | None = None) -> tuple[str, str]:
    """Return (login, source)."""
    login = request.headers.get(TS_LOGIN_HEADER)
    if login:
        return login, "header"
    fwd = request.headers.get("X-Forwarded-For")
    if fwd:
        ip = fwd.split(",")[0].strip()
        login = await _whois(ip)
        if login:
            return login, "whois"
    elif request.remote in ("127.0.0.1", "::1"):
        return LOCAL_USER, "loopback"
    if body_user:
        return f"claimed:{body_user}", "self-reported"
    return f"ip:{request.remote}", "peer-ip"
