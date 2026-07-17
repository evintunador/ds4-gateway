import argparse

from aiohttp import web

from .config import Config
from .server import Gateway


def main():
    ap = argparse.ArgumentParser(description="ds4 gateway")
    ap.add_argument("--config", default="config.toml")
    args = ap.parse_args()
    cfg = Config.load(args.config)
    gw = Gateway(cfg)
    host = cfg.get("gateway", "host", default="127.0.0.1")
    port = cfg.get("gateway", "port", default=9001)
    print(f"[gateway] {gw.color} listening on {host}:{port}, owner={gw.owner}")
    web.run_app(gw.build_app(), host=host, port=port, print=None)


if __name__ == "__main__":
    main()
