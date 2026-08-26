import asyncio
from typing import AsyncIterator, Optional
from .types import Message, ChannelConfig, MeshError

class StreamChannel:
    def __init__(self, config: ChannelConfig):
        self.config = config
        self.queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=config.buffer_size)
        self.is_closed = False
        self.closed_event = asyncio.Event()

    async def send_token(self, token: bytes) -> None:
        if self.is_closed:
            raise MeshError("Channel is closed")
        
        # This will block if queue is full, providing backpressure naturally
        try:
            await asyncio.wait_for(self.queue.put(token), timeout=self.config.timeout_seconds)
        except asyncio.TimeoutError:
            raise MeshError("Timeout writing to channel")

    async def _recv_token(self) -> Optional[bytes]:
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
        self.closed_event.set()
