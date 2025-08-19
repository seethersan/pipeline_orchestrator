
from __future__ import annotations
import asyncio
from typing import Set, Dict, Any

class SSEBroadcaster:
    def __init__(self) -> None:
        self.subscribers: Set[asyncio.Queue] = set()

    async def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue()
        self.subscribers.add(q)
        return q

    async def unsubscribe(self, q: asyncio.Queue) -> None:
        self.subscribers.discard(q)

    def publish(self, event: Dict[str, Any]) -> None:
        for q in list(self.subscribers):
            try:
                q.put_nowait(event)
            except Exception:
                pass

broadcaster = SSEBroadcaster()
