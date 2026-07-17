"""Live traffic simulator: virtual tailnet users generating realistic load.

Run ON the gateway machine against 127.0.0.1 — simulated identities work by
setting the Tailscale-User-Login header, which the gateway only trusts
because the connection arrives via loopback (see docs/OPERATIONS.md).

Each virtual user runs an independent loop: think (exponential), then send a
chat turn; conversations carry history across turns (~60% continue chance),
which exercises the scheduler's stickiness and ds4-server's KV prefix cache.
A fraction of requests stream, so TTFT (time to first byte) is measured.

Usage:
  uv run python tools/simulate.py --duration 60 --users 3
  uv run python tools/simulate.py --base-url http://127.0.0.1:19001 \
      --users 5 --think-mean 1 --duration 30          # against the mock
  ... --fail-on-errors    # exit 1 on any non-200 (for switchover tests)
"""

import argparse
import asyncio
import json
import random
import time

import aiohttp

PROMPTS = [
    "Summarize the plot of a heist movie in two sentences.",
    "What's a good name for a boat owned by a mathematician?",
    "Explain mmap to a five year old.",
    "Write a haiku about batteries.",
    "Give me one surprising fact about the ocean.",
]
FOLLOWUPS = ["shorter please", "now make it funnier", "why?", "give another example"]


class Recorder:
    def __init__(self, log_path=None):
        self.events = []
        self.window = []
        self.in_flight = 0
        self.log = open(log_path, "a") if log_path else None

    def record(self, ev):
        self.events.append(ev)
        self.window.append(ev)
        if self.log:
            self.log.write(json.dumps(ev) + "\n")
            self.log.flush()

    @staticmethod
    def _pct(vals, p):
        if not vals:
            return None
        vals = sorted(vals)
        return vals[min(int(len(vals) * p / 100), len(vals) - 1)]

    def summarize(self, evs):
        ok = [e for e in evs if e["status"] == 200]
        by_code = {}
        for e in evs:
            key = f"{e['status']}:{e.get('code', '')}".rstrip(":")
            by_code[key] = by_code.get(key, 0) + 1
        lat = [e["latency_s"] for e in ok]
        ttft = [e["ttft_s"] for e in ok if e.get("ttft_s") is not None]
        return {
            "requests": len(evs), "by_status": by_code,
            "latency_p50": self._pct(lat, 50), "latency_p95": self._pct(lat, 95),
            "ttft_p50": self._pct(ttft, 50), "ttft_p95": self._pct(ttft, 95),
        }

    def report_window(self, elapsed):
        s = self.summarize(self.window)
        self.window = []
        lat = (f"lat p50/p95 {s['latency_p50']:.2f}/{s['latency_p95']:.2f}s"
               if s["latency_p50"] is not None else "lat -")
        ttft = (f"  ttft p50 {s['ttft_p50']:.2f}s" if s["ttft_p50"] is not None else "")
        print(f"[{elapsed:5.0f}s] {s['requests']:3d} reqs  {s['by_status']}  "
              f"{lat}{ttft}  in-flight {self.in_flight}")


async def vuser(name, login, args, rec, deadline):
    headers = {"Tailscale-User-Login": login, "X-Forwarded-For": "100.64.0.99"}
    messages = []
    async with aiohttp.ClientSession() as s:
        while time.time() < deadline:
            await asyncio.sleep(random.expovariate(1 / args.think_mean))
            if time.time() >= deadline:
                break
            if not messages or random.random() > 0.6 or len(messages) > 10:
                messages = [{"role": "user", "content": random.choice(PROMPTS)}]
            else:
                messages.append({"role": "user", "content": random.choice(FOLLOWUPS)})
            stream = random.random() < args.stream_fraction
            payload = {"model": "deepseek-v4-flash", "messages": messages,
                       "max_tokens": args.max_tokens, "stream": stream, "user": name}
            t0 = time.time()
            ev = {"t": t0, "user": name, "stream": stream, "turn": len(messages)}
            rec.in_flight += 1
            try:
                async with s.post(f"{args.base_url}/v1/chat/completions",
                                  json=payload, headers=headers,
                                  timeout=aiohttp.ClientTimeout(total=args.timeout)) as r:
                    ev["status"] = r.status
                    ttft = None
                    body = b""
                    async for chunk in r.content.iter_any():
                        if ttft is None:
                            ttft = time.time() - t0
                        body += chunk
                    ev["ttft_s"] = ttft if stream else None
                    if r.status == 200:
                        reply = _extract_reply(body, stream)
                        messages.append({"role": "assistant", "content": reply})
                    else:
                        try:
                            ev["code"] = json.loads(body)["error"]["code"]
                        except Exception:
                            pass
                        messages = []
            except Exception as e:
                ev["status"] = -1
                ev["code"] = type(e).__name__
                messages = []
            finally:
                rec.in_flight -= 1
            ev["latency_s"] = time.time() - t0
            rec.record(ev)


def _extract_reply(body: bytes, stream: bool) -> str:
    try:
        if not stream:
            return json.loads(body)["choices"][0]["message"]["content"]
        parts = []
        for line in body.decode().splitlines():
            if line.startswith("data: ") and line != "data: [DONE]":
                delta = json.loads(line[6:])["choices"][0].get("delta", {})
                parts.append(delta.get("content") or "")
        return "".join(parts)
    except Exception:
        return "(unparseable)"


async def main_async(args):
    rec = Recorder(args.log)
    deadline = time.time() + args.duration
    start = time.time()
    users = [(f"sim-{i+1}", f"sim-{i+1}@loadtest") for i in range(args.users)]
    if args.owner:
        users.append(("owner", args.owner))
    tasks = [asyncio.create_task(vuser(n, l, args, rec, deadline)) for n, l in users]

    async def reporter():
        while time.time() < deadline:
            await asyncio.sleep(args.report_every)
            rec.report_window(time.time() - start)

    rep = asyncio.create_task(reporter())
    await asyncio.gather(*tasks)
    rep.cancel()

    s = rec.summarize(rec.events)
    print("\n== totals ==")
    print(json.dumps(s, indent=2))
    errors = sum(n for k, n in s["by_status"].items() if not k.startswith("200"))
    if args.fail_on_errors and errors:
        print(f"FAIL: {errors} non-200 responses")
        return 1
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--base-url", default="http://127.0.0.1:9001")
    ap.add_argument("--duration", type=float, default=60)
    ap.add_argument("--users", type=int, default=3)
    ap.add_argument("--owner", default=None,
                    help="also run a virtual user with this tailscale login (gate-bypass traffic)")
    ap.add_argument("--think-mean", type=float, default=5.0,
                    help="mean seconds between a user's requests")
    ap.add_argument("--stream-fraction", type=float, default=0.3)
    ap.add_argument("--max-tokens", type=int, default=48)
    ap.add_argument("--timeout", type=float, default=300)
    ap.add_argument("--report-every", type=float, default=10)
    ap.add_argument("--log", default=None, help="append per-request JSONL here")
    ap.add_argument("--fail-on-errors", action="store_true")
    args = ap.parse_args()
    raise SystemExit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    main()
