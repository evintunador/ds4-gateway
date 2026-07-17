"""Runtime & memory benchmark for the gateway + ds4-server stack.

Measures, per scenario: exact token counts and total latency (one
non-streamed request) plus TTFT and decode rate (one streamed request).
The cached scenario re-sends the previous prompt to show the KV
prefix-cache benefit; uncached prompts start with a unique nonce so the
shared prefix cache can't help. Model/gateway RSS sampled before and after.

Results print as a table and are saved (JSON) under <state_dir>/benchmarks/
for tracking regressions across model or engine versions.

Run on the gateway machine:  ds4ctl bench   (or uv run python tools/bench.py)
"""

import argparse
import asyncio
import json
import random
import subprocess
import time
from pathlib import Path

import aiohttp

WORDS = ("ocean gradient harbor lantern quantum meadow copper drift signal "
         "timber orbit velvet canyon ember glacier prism").split()


def make_prompt(n_tokens: int, nonce: str) -> str:
    # ~1 token per common word; nonce first so the shared KV prefix cache
    # cannot reuse anything across "uncached" runs
    words = [nonce] + [WORDS[i % len(WORDS)] for i in range(max(n_tokens - 20, 1))]
    return ("[" + " ".join(words) + "] Summarize the word list above in one "
            "short sentence.")


import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ds4gateway.watchdog import footprint_mb  # noqa: E402


async def run_once(s, base, prompt, max_tokens, stream):
    payload = {"model": "deepseek-v4-flash", "max_tokens": max_tokens,
               "stream": stream,
               "messages": [{"role": "user", "content": prompt}]}
    t0 = time.time()
    async with s.post(f"{base}/v1/chat/completions", json=payload) as r:
        if r.status != 200:
            raise RuntimeError(f"status {r.status}: {await r.text()}")
        if not stream:
            body = await r.json()
            u = body.get("usage", {})
            return {"latency_s": time.time() - t0,
                    "prompt_tokens": u.get("prompt_tokens"),
                    "completion_tokens": u.get("completion_tokens")}
        ttft = None
        chunks = 0
        async for chunk in r.content.iter_any():
            if ttft is None:
                ttft = time.time() - t0
            chunks += chunk.count(b'"chat.completion.chunk"')
        total = time.time() - t0
        decode = (chunks - 1) / (total - ttft) if chunks > 1 and total > ttft else None
        return {"latency_s": total, "ttft_s": ttft, "chunks": chunks,
                "decode_tok_s": decode}


async def main_async(args):
    # cached runs immediately after medium: ds4-server keeps ONE KV prefix
    # checkpoint, so any scenario in between would evict it
    scenarios = [
        ("short 64/64", 64, 64),
        ("medium 1k/128", 1000, 128),
    ]
    late_scenarios = [("long 8k/128", 8000, 128)]
    results = {"t": time.time(), "scenarios": {}}
    timeout = aiohttp.ClientTimeout(total=args.timeout)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        async with s.get(f"{args.base_url}/admin/status") as r:
            st = await r.json()
        model_pid = st["backend"]["pid"]
        state_dir = st.get("state_dir")
        results["footprint_before_mb"] = footprint_mb(model_pid)
        results["backend"] = st["backend"]["base_url"]

        async def run_scenario(name, ptok, mtok):
            prompt = make_prompt(ptok, nonce=f"bench{random.randrange(10**9)}")
            exact = await run_once(s, args.base_url, prompt, mtok, stream=False)
            streamed = await run_once(s, args.base_url, prompt + " (again)",
                                      mtok, stream=True)
            results["scenarios"][name] = {"exact": exact, "streamed": streamed}
            d = streamed["decode_tok_s"]
            decode = f"{d:.1f}" if d else "?"
            print(f"{name:<16} prompt={exact['prompt_tokens']:>5} tok  "
                  f"total={exact['latency_s']:.2f}s  "
                  f"ttft={streamed['ttft_s']:.2f}s  decode={decode} tok/s")
            return prompt

        for name, ptok, mtok in scenarios:
            cached_prompt = await run_scenario(name, ptok, mtok)

        # cached: the streamed "(again)" variant of the 1k prompt just ran, so
        # re-sending it exactly should hit the KV prefix checkpoint
        c = await run_once(s, args.base_url, cached_prompt + " (again)",
                           128, stream=True)
        results["scenarios"]["cached 1k/128"] = {"streamed": c}
        d = c["decode_tok_s"]
        decode = f"{d:.1f}" if d else "?"
        print(f"{'cached 1k/128':<16} {'(same prompt)':>16}  "
              f"total={c['latency_s']:.2f}s  ttft={c['ttft_s']:.2f}s  "
              f"decode={decode} tok/s")

        for name, ptok, mtok in late_scenarios:
            await run_scenario(name, ptok, mtok)

        results["footprint_after_mb"] = footprint_mb(model_pid)
        print(f"model footprint: {results['footprint_before_mb']:.0f} -> "
              f"{results['footprint_after_mb']:.0f} MB")

    if state_dir:
        out = Path(state_dir) / "benchmarks"
        out.mkdir(exist_ok=True)
        f = out / f"bench-{time.strftime('%Y%m%d-%H%M%S')}.json"
        f.write_text(json.dumps(results, indent=2))
        print(f"saved: {f}")


def find_gateway():
    import urllib.request
    for port in (9001, 9002):
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/admin/status", timeout=3)
            return f"http://127.0.0.1:{port}"
        except OSError:
            continue
    raise SystemExit("no gateway responding on 9001/9002; pass --base-url")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", default=None)
    ap.add_argument("--timeout", type=float, default=600)
    args = ap.parse_args()
    if args.base_url is None:
        args.base_url = find_gateway()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
