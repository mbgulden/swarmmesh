from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

from .types import ChannelConfig, MeshError


class StreamChannel:
    def __init__(self, config: ChannelConfig):
        self.config = config
        # asyncio primitives are created lazily on first async use so a
        # channel can be constructed outside a running event loop. (On
        # Python 3.9, asyncio.Queue/Event bind to the loop at construction
        # time and raise RuntimeError when there is none.)
        self.queue: asyncio.Queue[bytes] | None = None
        self.closed_event: asyncio.Event | None = None
        self.is_closed = False

    def _ensure_primitives(self) -> None:
        if self.queue is None:
            self.queue = asyncio.Queue(maxsize=self.config.buffer_size)
            self.closed_event = asyncio.Event()

    async def send_token(self, token: bytes) -> None:
        if self.is_closed:
            raise MeshError("Channel is closed")

        self._ensure_primitives()
        assert self.queue is not None

        # This will block if queue is full, providing backpressure naturally
        try:
            await asyncio.wait_for(self.queue.put(token), timeout=self.config.timeout_seconds)
        except asyncio.TimeoutError:
            raise MeshError("Timeout writing to channel")

    async def _recv_token(self) -> bytes | None:
        self._ensure_primitives()
        assert self.queue is not None

        if self.queue.empty() and self.is_closed:
            return None

        try:
            token = await self.queue.get()
            return token
        except asyncio.CancelledError:
            return None

    def __aiter__(self) -> AsyncIterator[bytes]:
        return self

    async def __anext__(self) -> bytes:
        self._ensure_primitives()
        assert self.queue is not None
        assert self.closed_event is not None

        while not (self.is_closed and self.queue.empty()):
            # Use wait to wake up either when closed_event is set or a token is available
            get_task = asyncio.create_task(self.queue.get())
            closed_task = asyncio.create_task(self.closed_event.wait())

            done, pending = await asyncio.wait(
                [get_task, closed_task],
                return_when=asyncio.FIRST_COMPLETED
            )

            for task in pending:
                task.cancel()

            if get_task in done:
                try:
                    return get_task.result()
                except asyncio.CancelledError:
                    pass

            if closed_task in done and self.queue.empty():
                break

        raise StopAsyncIteration

    async def close(self) -> None:
        self.is_closed = True
        self._ensure_primitives()
        assert self.closed_event is not None
        self.closed_event.set()
