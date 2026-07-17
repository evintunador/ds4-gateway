"""Mock ds4-server for gateway testing.

Mimics the endpoints the gateway proxies, tracks concurrency so tests can
assert the gateway serializes requests (real ds4-server has one graph
worker), and records the order users were served for fairness checks.
"""

import argparse
import asyncio
import json

from aiohttp import web

state = {"active": 0, "max_concurrent": 0, "served_users": []}


async def models(request):
    return web.json_response({"object": "list", "data": [
        {"id": "deepseek-v4-flash", "object": "model"}]})


async def chat(request):
    body = await request.json()
    state["active"] += 1
    state["max_concurrent"] = max(state["max_concurrent"], state["active"])
    state["served_users"].append(body.get("user", "?"))
    try:
        await asyncio.sleep(float(request.headers.get("X-Mock-Delay", 0.2)))
        if body.get("stream"):
            resp = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
            await resp.prepare(request)
            for tok in ["Hello", " from", " mock"]:
                chunk = {"choices": [{"delta": {"content": tok}}]}
                await resp.write(f"data: {json.dumps(chunk)}\n\n".encode())
                await asyncio.sleep(0.05)
            await resp.write(b"data: [DONE]\n\n")
            await resp.write_eof()
            return resp
        return web.json_response({"choices": [{"message": {
            "role": "assistant", "content": f"mock reply to {body.get('user', '?')}"}}]})
    finally:
        state["active"] -= 1


async def stats(request):
    return web.json_response(state)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=18001)
    args = ap.parse_args()
    app = web.Application()
    app.router.add_get("/v1/models", models)
    app.router.add_post("/v1/chat/completions", chat)
    app.router.add_get("/mock/stats", stats)
    web.run_app(app, host="127.0.0.1", port=args.port, print=None)


if __name__ == "__main__":
    main()
