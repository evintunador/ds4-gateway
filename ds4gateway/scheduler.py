"""Weighted fair queuing over a serial backend.

ds4-server executes one request at a time (single graph worker), so fairness
means deciding whose queued request goes next. Stride scheduling: each
dispatch charges the user pass-value inversely proportional to their weight,
and the lowest pass goes next. A bounded stickiness bonus lets the same user
take a couple of consecutive turns to exploit ds4-server's single shared KV
prefix cache before the scheduler rotates.
"""

import asyncio
from collections import deque

_STRIDE_BASE = 10000.0


class QueueFull(Exception):
    pass


class QueueTimeout(Exception):
    pass


class _Ticket:
    __slots__ = ("event", "abandoned")

    def __init__(self):
        self.event = asyncio.Event()
        self.abandoned = False


class FairScheduler:
    def __init__(self, weights: dict[str, float] | None = None, default_weight: float = 1,
                 sticky_extra_turns: int = 2, max_queued_per_user: int = 4,
                 queue_timeout_s: float = 600):
        self.weights = dict(weights or {})
        self.default_weight = default_weight
        self.sticky_extra = sticky_extra_turns
        self.max_queued = max_queued_per_user
        self.timeout = queue_timeout_s
        self._queues: dict[str, deque[_Ticket]] = {}
        self._passes: dict[str, float] = {}
        self._vtime = 0.0
        self._busy_user: str | None = None
        self._last_user: str | None = None
        self._sticky_used = 0

    def weight(self, user: str) -> float:
        return max(float(self.weights.get(user, self.default_weight)), 0.001)

    def _pick(self) -> str | None:
        pending = [u for u, q in self._queues.items() if q]
        if not pending:
            return None
        if (self._last_user in pending and len(pending) > 1
                and self._sticky_used < self.sticky_extra):
            return self._last_user
        return min(pending, key=lambda u: self._passes.get(u, self._vtime))

    def _maybe_dispatch(self):
        while self._busy_user is None:
            user = self._pick()
            if user is None:
                return
            self._sticky_used = self._sticky_used + 1 if user == self._last_user else 0
            self._vtime = self._passes.get(user, self._vtime)
            self._passes[user] = self._vtime + _STRIDE_BASE / self.weight(user)
            q = self._queues[user]
            while q:
                ticket = q.popleft()
                if not ticket.abandoned:
                    self._busy_user = user
                    self._last_user = user
                    ticket.event.set()
                    return
            # queue held only abandoned tickets; loop and pick again

    async def acquire(self, user: str):
        q = self._queues.setdefault(user, deque())
        if len(q) >= self.max_queued:
            raise QueueFull(user)
        if not q:
            # returning/new user starts at current virtual time, not zero,
            # so an idle user can't build up infinite priority
            self._passes[user] = max(self._passes.get(user, 0.0), self._vtime)
        ticket = _Ticket()
        q.append(ticket)
        self._maybe_dispatch()
        try:
            await asyncio.wait_for(ticket.event.wait(), self.timeout)
        except asyncio.TimeoutError:
            ticket.abandoned = True
            raise QueueTimeout(user)

    def release(self, user: str):
        if self._busy_user == user:
            self._busy_user = None
            self._maybe_dispatch()

    def status(self) -> dict:
        return {
            "busy_user": self._busy_user,
            "queued": {u: len(q) for u, q in self._queues.items() if q},
        }
