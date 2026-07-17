"""Managed-lifecycle tests: spawn/adopt/swap/handoff using fake-ds4-server.

Covers what test_e2e.py (managed=false) cannot: the gateway actually owning
model processes, the two-phase red/yellow swap with zero dropped requests,
and the blue/green handoff where a second gateway adopts the model and the
first exits without killing it.

Run:  uv run python tests/test_lifecycle.py
"""

import asyncio
import json
import os
import signal
import subprocess
import sys
import tempfile
import time

import aiohttp

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
GW1, GW2, GW3 = 19003, 19004, 19005
M_RED, M_YEL, M_WD = 18011, 18012, 18021

CONFIG = f"""
[gateway]
host = "127.0.0.1"
port = {GW1}
alt_port = {GW2}
state_dir = "{{state_dir}}"

[owner]
login = "evintunador@github"

[power]
poll_interval_s = 3600

[model]
ds4_dir = "{HERE}"
server_bin = "{HERE}/fake-ds4-server"
model_file = "fake.gguf"
port = {M_RED}
alt_port = {M_YEL}
args = []
managed = true
autostart = true
health_timeout_s = 30
log_file = "{{state_dir}}/fake-ds4.log"
swap_streaming_args = ["--ssd-streaming"]

[scheduler]
queue_timeout_s = 30
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


async def status(s, port=GW1):
    async with s.get(f"http://127.0.0.1:{port}/admin/status") as r:
        return await r.json()


async def chat(s, port=GW1):
    async with s.post(f"http://127.0.0.1:{port}/v1/chat/completions",
                      json={"messages": [{"role": "user", "content": "hi"}]}) as r:
        return r.status, await r.json()


async def wait_backend_running(s, port=GW1, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            st = await status(s, port)
            if st["backend"]["state"] == "running":
                return st
        except aiohttp.ClientError:
            pass
        await asyncio.sleep(0.5)
    raise RuntimeError(f"gateway :{port} backend never running")


async def run_tests(cfg_path, state_dir, procs):
    async with aiohttp.ClientSession() as s:
        print("== managed spawn ==")
        st = await wait_backend_running(s)
        pid1 = st["backend"]["pid"]
        check("gateway spawned fake model", st["backend"]["port"] == M_RED and pid1, st["backend"])
        check("pidfile written", os.path.exists(f"{state_dir}/model-{M_RED}.pid"))
        code, body = await chat(s)
        check("chat via managed model", code == 200 and body["served_by_port"] == M_RED,
              (code, body))

        print("== red/yellow swap under load ==")
        stop_load = asyncio.Event()
        load_results = []

        async def load_loop():
            while not stop_load.is_set():
                try:
                    code, body = await chat(s)
                    load_results.append((code, body.get("served_by_port")))
                except Exception as e:
                    load_results.append((-1, str(e)))
                await asyncio.sleep(0.1)

        loader = asyncio.create_task(load_loop())
        async with s.post(f"http://127.0.0.1:{GW1}/admin/model_swap") as r:
            check("swap accepted", r.status == 200, r.status)
        deadline = time.time() + 60
        while time.time() < deadline:
            st = await status(s)
            if st["backend"]["swap_state"] is None:
                break
            await asyncio.sleep(0.5)
        stop_load.set()
        await loader
        st = await status(s)
        check("swap completed", st["backend"]["swap_state"] is None, st["backend"])
        check("model back on red port with new pid",
              st["backend"]["port"] == M_RED and st["backend"]["pid"] != pid1, st["backend"])
        codes = [c for c, _ in load_results]
        check(f"zero dropped requests during swap ({len(codes)} sent)",
              codes and all(c == 200 for c in codes),
              [r for r in load_results if r[0] != 200][:5])
        ports_seen = {p for c, p in load_results if c == 200}
        check("traffic actually flowed through yellow", M_YEL in ports_seen or len(codes) < 5,
              ports_seen)
        async with s.get(f"http://127.0.0.1:{GW1}/v1/models") as r:
            models = await r.json()
        check("final instance is resident (no --ssd-streaming)",
              models["streaming_mode"] is False, models)

        print("== blue/green handoff ==")
        pid_before = st["backend"]["pid"]
        gw2 = subprocess.Popen(
            [sys.executable, "-m", "ds4gateway", "--config", cfg_path,
             "--port", str(GW2), "--color", "green"], cwd=ROOT)
        procs.append(gw2)
        st2 = await wait_backend_running(s, GW2)
        check("green adopted (same model pid, no respawn)",
              st2["backend"]["pid"] == pid_before and st2["color"] == "green", st2["backend"])
        code, body = await chat(s, GW2)
        check("chat via green", code == 200, (code, body))
        async with s.post(f"http://127.0.0.1:{GW1}/admin/shutdown",
                          json={"keep_model": True}) as r:
            check("blue shutdown accepted", r.status == 200, r.status)
        deadline = time.time() + 30
        blue_dead = False
        while time.time() < deadline:
            try:
                await status(s, GW1)
            except aiohttp.ClientError:
                blue_dead = True
                break
            await asyncio.sleep(0.5)
        check("blue exited", blue_dead)
        st2 = await status(s, GW2)
        code, _ = await chat(s, GW2)
        check("model survived handoff",
              code == 200 and st2["backend"]["pid"] == pid_before, st2["backend"])

        print("== watchdog ==")
        wd_dir = tempfile.mkdtemp(prefix="ds4gw-watchdog-")
        wd_cfg = os.path.join(wd_dir, "config.toml")
        with open(wd_cfg, "w") as f:
            f.write(CONFIG.format(state_dir=wd_dir)
                    .replace(f"port = {GW1}", f"port = {GW3}", 1)
                    .replace(f"port = {M_RED}", f"port = {M_WD}", 1)
                    .replace(f"alt_port = {M_YEL}", f"alt_port = {M_WD + 1}", 1)
                    + "\n[watchdog]\ninterval_s = 1\nmodel_footprint_mb = 1\n")
        gw3 = subprocess.Popen([sys.executable, "-m", "ds4gateway",
                                "--config", wd_cfg], cwd=ROOT)
        procs.append(gw3)
        # the watchdog may trip while the model is still "starting" (any RSS
        # beats a 1MB limit), so wait directly for the disabled outcome
        st3 = None
        deadline = time.time() + 25
        tripped = False
        while time.time() < deadline:
            try:
                st3 = await status(s, GW3)
                if st3["backend"]["disabled"]:
                    tripped = True
                    break
            except aiohttp.ClientError:
                pass
            await asyncio.sleep(0.5)
        check("watchdog disabled over-limit model", tripped, st3)
        check("watchdog event recorded",
              tripped and any("rss" in e["msg"] for e in st3["watchdog"]["events"]),
              st3 and st3.get("watchdog"))
        await asyncio.sleep(1.5)
        model_up = True
        try:
            async with s.get(f"http://127.0.0.1:{M_WD}/v1/models"):
                pass
        except aiohttp.ClientError:
            model_up = False
        check("over-limit model actually stopped", not model_up)

        print("== off/on through adopting gateway ==")
        async with s.post(f"http://127.0.0.1:{GW2}/admin/off") as r:
            check("off via green", r.status == 200, r.status)
        await asyncio.sleep(1)
        check("adopted model actually stopped", not pid_alive(pid_before), pid_before)
        async with s.post(f"http://127.0.0.1:{GW2}/admin/on") as r:
            check("on via green", r.status == 200, r.status)
        code, _ = await chat(s, GW2)
        check("respawned and serving", code == 200, code)


def pid_alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def cleanup(state_dir, procs):
    for p in procs:
        if p and p.poll() is None:
            p.terminate()
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
    for f in os.listdir(state_dir):
        if f.startswith("model-") and f.endswith(".pid"):
            try:
                os.kill(int(open(os.path.join(state_dir, f)).read()), signal.SIGKILL)
            except (OSError, ValueError):
                pass


def main():
    state_dir = tempfile.mkdtemp(prefix="ds4gw-lifecycle-")
    cfg_path = os.path.join(state_dir, "config.toml")
    with open(cfg_path, "w") as f:
        f.write(CONFIG.format(state_dir=state_dir))
    # slow fake startup so the swap provably serves traffic from the yellow
    # stopgap while the phase-2 resident instance loads
    env = {**os.environ, "FAKE_DS4_STARTUP_DELAY": "0.5"}
    gw1 = subprocess.Popen([sys.executable, "-m", "ds4gateway", "--config", cfg_path],
                           cwd=ROOT, env=env)
    procs = [gw1]
    try:
        asyncio.run(run_tests(cfg_path, state_dir, procs))
    finally:
        cleanup(state_dir, procs)
    print(f"\n{PASS} passed, {FAIL} failed")
    sys.exit(1 if FAIL else 0)


if __name__ == "__main__":
    main()
