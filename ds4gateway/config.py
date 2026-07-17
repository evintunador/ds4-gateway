"""Load the gateway TOML config."""

import tomllib
from pathlib import Path


class Config:
    def __init__(self, raw: dict, path: Path):
        self.raw = raw
        self.path = path

    @classmethod
    def load(cls, path: str | Path) -> "Config":
        p = Path(path).expanduser().resolve()
        with open(p, "rb") as f:
            return cls(tomllib.load(f), p)

    def get(self, *keys, default=None):
        cur = self.raw
        for k in keys:
            if not isinstance(cur, dict) or k not in cur:
                return default
            cur = cur[k]
        return cur

    def resolve_path(self, value: str) -> Path:
        """Resolve a possibly-relative config path against the config file's directory."""
        p = Path(value).expanduser()
        return p if p.is_absolute() else self.path.parent / p
