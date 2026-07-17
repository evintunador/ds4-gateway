import argparse

from aiohttp import web

from .config import Config
from .server import Gateway


def main():
    ap = argparse.ArgumentParser(description="ds4 gateway")
    ap.add_argument("--config", default="config.toml")
    ap.add_argument("--port", type=int, default=None,
                    help="override [gateway].port (blue/green deploys)")
    ap.add_argument("--color", default=None,
                    help="override [gateway].color (blue/green deploys)")
    args = ap.parse_args()
    cfg = Config.load(args.config)
    if args.port is not None:
        cfg.raw.setdefault("gateway", {})["port"] = args.port
    if args.color is not None:
        cfg.raw.setdefault("gateway", {})["color"] = args.color
    gw = Gateway(cfg)
    host = cfg.get("gateway", "host", default="127.0.0.1")
    port = cfg.get("gateway", "port", default=9001)
    print(f"[gateway] {gw.color} listening on {host}:{port}, owner={gw.owner}")
    web.run_app(gw.build_app(), host=host, port=port, print=None)


if __name__ == "__main__":
    main()
