"""Blue/green gateway deploy orchestrator.

  uv run python tools/deploy.py              # deploy repo HEAD to inactive color
  uv run python tools/deploy.py --release releases/<dir>   # redeploy/rollback
  uv run python tools/deploy.py --promote    # point boot symlink at live release

Deploy sequence: archive HEAD into a versioned release dir -> uv sync ->
start gateway on the INACTIVE color port (it adopts the running model via
pidfile; the model is never touched) -> health check -> re-point
`tailscale serve` (this is the traffic flip) -> ask old gateway to drain and
exit WITHOUT killing the model -> update the `live` symlink.

`current` (what the future LaunchDaemon boots) only moves on --promote,
never as part of a deploy — a bad flip must not become the boot default.
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import time
import tomllib
import urllib.error
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


def sh(*cmd, **kw):
    return subprocess.run(cmd, check=True, capture_output=True, text=True, **kw)


def status_of(port):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/admin/status", timeout=5) as r:
            return json.loads(r.read())
    except (urllib.error.URLError, OSError):
        return None


def post(port, path, payload=None):
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}", method="POST",
        data=json.dumps(payload or {}).encode(),
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--release", default=None,
                    help="existing release dir to (re)deploy instead of archiving HEAD")
    ap.add_argument("--promote", action="store_true",
                    help="point the boot symlink (current) at the live release and exit")
    ap.add_argument("--no-serve", action="store_true",
                    help="skip the tailscale serve flip (testing)")
    ap.add_argument("--start-timeout", type=float, default=300)
    args = ap.parse_args()

    cfg = tomllib.loads((REPO / "config.toml").read_text())
    deploy_root = Path(cfg.get("deploy", {}).get("root", "~/dev/ds4-gateway-deploy")).expanduser()
    ports = {cfg["gateway"]["port"]: "blue", cfg["gateway"]["alt_port"]: "green"}

    if args.promote:
        live = deploy_root / "live"
        if not live.is_symlink():
            sys.exit("promote: no live release (deploy first)")
        current = deploy_root / "current"
        if current.is_symlink():
            current.unlink()
        current.symlink_to(live.resolve())
        print(f"current -> {live.resolve()}")
        print("(the LaunchDaemon, once enabled, will boot this version)")
        return

    # 1. find active/inactive color
    alive = {p: status_of(p) for p in ports}
    active = [p for p, s in alive.items() if s]
    if len(active) == 2:
        sys.exit("both gateway ports are up — finish/clean up the previous deploy first")
    old_port = active[0] if active else None
    new_port = next(p for p in ports if p != old_port) if old_port else list(ports)[0]
    print(f"active: {ports[old_port] if old_port else 'none'}"
          f"  ->  deploying {ports[new_port]} on :{new_port}")

    # 2. build release dir
    if args.release:
        rel = Path(args.release).expanduser().resolve()
        if not rel.is_dir():
            sys.exit(f"no such release dir: {rel}")
    else:
        dirty = sh("git", "-C", str(REPO), "status", "--porcelain").stdout.strip()
        if dirty:
            print("WARNING: working tree dirty; deploying committed HEAD only:\n" + dirty)
        sha = sh("git", "-C", str(REPO), "rev-parse", "--short", "HEAD").stdout.strip()
        rel = deploy_root / "releases" / f"{time.strftime('%Y%m%d-%H%M%S')}-{sha}"
        rel.mkdir(parents=True)
        tar = subprocess.Popen(["git", "-C", str(REPO), "archive", "HEAD"],
                               stdout=subprocess.PIPE)
        sh("tar", "-x", "-C", str(rel), stdin=tar.stdout)
        tar.wait()
        print(f"release: {rel}")
        sh("uv", "sync", cwd=str(rel))

    # 3. start new gateway (adopts the running model; never spawns a second one
    #    while one is healthy)
    (rel / "logs").mkdir(exist_ok=True)
    with open(rel / "logs" / "gateway.log", "ab") as log:
        subprocess.Popen(
            ["uv", "run", "python", "-m", "ds4gateway", "--config", "config.toml",
             "--port", str(new_port), "--color", ports[new_port]],
            cwd=str(rel), stdout=log, stderr=subprocess.STDOUT,
            start_new_session=True)
    deadline = time.time() + args.start_timeout
    while time.time() < deadline:
        s = status_of(new_port)
        if s and s["backend"]["state"] == "running":
            break
        if s and s["backend"]["disabled"]:
            break  # disabled state honored; gateway is still healthy
        time.sleep(2)
    else:
        sys.exit(f"new gateway on :{new_port} did not become healthy; "
                 f"see {rel}/logs/gateway.log (old gateway untouched)")
    print(f"{ports[new_port]} gateway healthy, backend: {status_of(new_port)['backend']['state']}")

    # 4. flip traffic
    if not args.no_serve:
        sh("tailscale", "serve", "--bg", str(new_port))
        out = sh("tailscale", "serve", "status").stdout
        if f":{new_port}" not in out:
            sys.exit(f"serve flip did not take; serve status:\n{out}")
        print(f"tailscale serve -> :{new_port}")

    # 5. retire old gateway (keeps the model running)
    if old_port:
        try:
            post(old_port, "/admin/shutdown", {"keep_model": True})
        except (urllib.error.URLError, OSError) as e:
            # pre-stage2 gateway or wedged process: SIGKILL by port so its
            # cleanup can't take the model down with it
            print(f"graceful shutdown refused ({e}); force-killing :{old_port}")
            pids = subprocess.run(["lsof", "-ti", f"tcp:{old_port}"],
                                  capture_output=True, text=True).stdout.split()
            for pid in pids:
                os.kill(int(pid), signal.SIGKILL)
        deadline = time.time() + 90
        while time.time() < deadline and status_of(old_port):
            time.sleep(1)
        if status_of(old_port):
            sys.exit(f"old gateway on :{old_port} did not exit; investigate")
        print(f"old {ports[old_port]} gateway drained and exited")

    # 6. mark live
    live = deploy_root / "live"
    if live.is_symlink():
        live.unlink()
    live.symlink_to(rel)
    print(f"live -> {rel}")
    print("boot version unchanged; run `ds4ctl promote` when satisfied")


if __name__ == "__main__":
    main()
