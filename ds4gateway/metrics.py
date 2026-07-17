"""Content-free usage log.

One JSONL line per inference request: who, when, status, timings, token
counts. NEVER message text — the retention posture is that conversation
content exists only in ds4-server's in-RAM KV cache (see docs/DESIGN.md).

Token counts are exact for non-streamed responses (parsed from `usage`).
ds4-server omits usage from SSE streams, so streamed completion tokens are
estimated from chunk counts (~1 token/chunk) and flagged `"estimated": true`.
"""

import json
import time
from pathlib import Path

INFERENCE_PATHS = {"/v1/chat/completions", "/v1/completions",
                   "/v1/responses", "/v1/messages"}


class UsageLog:
    def __init__(self, path: Path, enabled: bool = True,
                 max_bytes: int = 50 * 1024 * 1024):
        self.path = Path(path)
        self.enabled = enabled
        self.max_bytes = max_bytes

    def record(self, **ev):
        if not self.enabled:
            return
        ev.setdefault("ts", time.time())
        try:
            if self.path.exists() and self.path.stat().st_size > self.max_bytes:
                self.path.replace(self.path.with_suffix(".jsonl.1"))
            with open(self.path, "a") as f:
                f.write(json.dumps(ev) + "\n")
        except OSError:
            pass  # metrics must never break serving
