"""End-to-end test: mock ds4-server + real gateway, no model weights involved.

Run:  uv run python tests/test_e2e.py
"""

import asyncio
import json
import os
import subprocess
import sys
import tempfile
import time

import aiohttp

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
MOCK_PORT = 18001
GW_PORT = 19001
GW = f"http://127.0.0.1:{GW_PORT}"
OWNER = "evintunador@github"
FRIEND = "friend@github"
FRIEND2 = "pal@github"

CONFIG = f"""
[gateway]
host = "127.0.0.1"
port = {GW_PORT}
state_file = "{{state_file}}"

[owner]
login = "{OWNER}"

[power]
min_battery_percent = 80
poll_interval_s = 3600

[model]
ds4_dir = "{ROOT}"
host = "127.0.0.1"
port = {MOCK_PORT}
managed = false
autostart = true
log_file = "{{log_file}}"

[scheduler]
default_weight = 1
max_queued_per_user = 3
queue_timeout_s = 30

[scheduler.weights]
"{OWNER}" = 4
"""

PASS = 0
FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ok    {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


def as_user(login):
    # gateway binds localhost; simulate what tailscale serve injects
    return {"Tailscale-User-Login": login, "X-Forwarded-For": "100.64.0.99"}


async def chat(s, headers, user_field=None, **kw):
    payload = {"model": "deepseek-v4-flash",
               "messages": [{"role": "user", "content": "hi"}]}
    if user_field:
        payload["user"] = user_field
    async with s.post(f"{GW}/v1/chat/completions", json=payload,
                      headers=headers, **kw) as r:
        return r.status, await r.json()


async def admin(s, method, path, payload=None):
    async with s.request(method, f"{GW}{path}", json=payload) as r:
        return r.status, await r.json()


async def run_tests():
    async with aiohttp.ClientSession() as s:
        print("== basic ==")
        st, body = await admin(s, "GET", "/admin/status")
        check("status endpoint", st == 200 and body["backend"]["state"] == "running", body)

        st, body = await chat(s, as_user(OWNER), user_field="owner-laptop")
        check("owner chat passthrough", st == 200 and "mock reply" in json.dumps(body), (st, body))

        st, body = await chat(s, as_user(FRIEND), user_field="friend")
        check("friend chat passthrough (on AC)", st == 200, (st, body))

        st, _ = await admin(s, "GET", "/v1/models")  # no admin headers -> loopback owner path
        check("GET /v1/models proxied", st == 200)

        print("== power gate ==")
        await admin(s, "POST", "/admin/power_override", {"on_ac": False, "percent": 50})
        st, body = await chat(s, as_user(FRIEND))
        check("friend gated on battery", st == 503
              and body["error"]["code"] == "battery_gated", (st, body))
        st, _ = await chat(s, as_user(OWNER))
        check("owner bypasses power gate", st == 200, st)
        await admin(s, "POST", "/admin/power_override", {"on_ac": True, "percent": 79})
        st, body = await chat(s, as_user(FRIEND))
        check("friend gated below 80% even on AC", st == 503, (st, body))
        await admin(s, "POST", "/admin/power_override", {"clear": True})
        st, _ = await chat(s, as_user(FRIEND))
        check("friend restored after override cleared", st == 200, st)

        print("== manual disable ==")
        st, _ = await admin(s, "POST", "/admin/off")
        check("ds4ctl off accepted", st == 200)
        st, body = await chat(s, as_user(OWNER))
        check("owner also blocked when disabled", st == 503
              and body["error"]["code"] == "manually_disabled", (st, body))
        st, _ = await admin(s, "POST", "/admin/on")
        check("ds4ctl on accepted", st == 200)
        st, _ = await chat(s, as_user(OWNER))
        check("recovered after on", st == 200, st)

        print("== disable timer ==")
        st, body = await admin(s, "POST", "/admin/off", {"duration_s": 3})
        check("off --for accepted", st == 200 and body["backend"]["disabled_until"], body)
        st, _ = await chat(s, as_user(FRIEND))
        check("blocked during timer", st == 503, st)
        await asyncio.sleep(4.5)
        st, _ = await chat(s, as_user(FRIEND))
        check("auto re-enabled after timer", st == 200, st)

        print("== admin auth ==")
        async with s.post(f"{GW}/admin/off", headers=as_user(FRIEND)) as r:
            check("friend denied admin", r.status == 403, r.status)
        async with s.get(f"{GW}/admin/status", headers=as_user(OWNER)) as r:
            check("remote owner allowed admin", r.status == 200, r.status)

        print("== serialization + fairness ==")
        tasks = []
        for i in range(3):
            tasks.append(chat(s, as_user(OWNER), user_field="owner"))
            tasks.append(chat(s, as_user(FRIEND), user_field="friend"))
            tasks.append(chat(s, as_user(FRIEND2), user_field="pal"))
        results = await asyncio.gather(*tasks)
        check("all concurrent requests served", all(st == 200 for st, _ in results),
              [st for st, _ in results])
        async with s.get(f"http://127.0.0.1:{MOCK_PORT}/mock/stats") as r:
            stats = await r.json()
        check("backend saw no concurrency", stats["max_concurrent"] == 1, stats)

        print("== queue cap ==")
        burst = [chat(s, as_user(FRIEND)) for _ in range(6)]
        statuses = [st for st, _ in await asyncio.gather(*burst)]
        check("burst above cap gets 429s", statuses.count(429) >= 1 and statuses.count(200) >= 3,
              statuses)

        print("== streaming ==")
        async with s.post(f"{GW}/v1/chat/completions", headers=as_user(FRIEND),
                          json={"model": "x", "stream": True,
                                "messages": [{"role": "user", "content": "hi"}]}) as r:
            text = (await r.read()).decode()
            check("SSE stream proxied", r.status == 200 and "data: [DONE]" in text,
                  (r.status, text[:120]))


def wait_http(url, timeout=15):
    import urllib.request
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urllib.request.urlopen(url, timeout=2)
            return
        except Exception:
            time.sleep(0.3)
    raise RuntimeError(f"{url} never came up")


def main():
    tmp = tempfile.mkdtemp(prefix="ds4gw-test-")
    cfg_path = os.path.join(tmp, "config.toml")
    with open(cfg_path, "w") as f:
        f.write(CONFIG.format(state_file=os.path.join(tmp, "state.json"),
                              log_file=os.path.join(tmp, "ds4.log")))
    mock = subprocess.Popen([sys.executable, os.path.join(HERE, "mock_backend.py"),
                             "--port", str(MOCK_PORT)])
    gw = None
    try:
        wait_http(f"http://127.0.0.1:{MOCK_PORT}/v1/models")
        gw = subprocess.Popen([sys.executable, "-m", "ds4gateway", "--config", cfg_path],
                              cwd=ROOT)
        wait_http(f"{GW}/admin/status")
        asyncio.run(run_tests())
    finally:
        for p in (gw, mock):
            if p:
                p.terminate()
                p.wait(timeout=10)
    print(f"\n{PASS} passed, {FAIL} failed")
    sys.exit(1 if FAIL else 0)


if __name__ == "__main__":
    main()
