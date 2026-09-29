from __future__ import annotations

import asyncio
import json
from abc import ABC, abstractmethod

from .types import MeshError, Message


class Transport(ABC):
    @abstractmethod
    async def connect(self) -> None:
        pass

    @abstractmethod
    async def send(self, message: Message) -> None:
        pass

    @abstractmethod
    async def recv(self) -> Message:
        pass

    @abstractmethod
    async def close(self) -> None:
        pass

class InProcessTransport(Transport):
    def __init__(self) -> None:
        # Queue is created in connect() so the transport can be constructed
        # outside a running event loop (Python 3.9 binds asyncio.Queue to
        # the loop at construction time).
        self.queue: asyncio.Queue[Message] | None = None
        self.is_connected: bool = False

    async def connect(self) -> None:
        self.queue = asyncio.Queue()
        self.is_connected = True

    async def send(self, message: Message) -> None:
        if not self.is_connected or self.queue is None:
            raise MeshError("Transport not connected")
        await self.queue.put(message)

    async def recv(self) -> Message:
        if not self.is_connected or self.queue is None:
            raise MeshError("Transport not connected")
        return await self.queue.get()

    async def close(self) -> None:
        self.is_connected = False

class TCPTransport(Transport):
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None

    async def connect(self) -> None:
        try:
            self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        except Exception as e:  # noqa: BLE001
            # Wrap any connection failure in the domain error type
            raise MeshError(f"Failed to connect to {self.host}:{self.port}: {e}")

    async def send(self, message: Message) -> None:
        if not self.writer:
            raise MeshError("Not connected")
        
        data = {
            "topic": message.topic,
            "payload": message.payload.hex(),
            "sender_id": message.sender_id,
            "timestamp": message.timestamp,
            "msg_id": message.msg_id
        }
        encoded = json.dumps(data).encode('utf-8')
        length_prefix = len(encoded).to_bytes(4, byteorder='big')
        self.writer.write(length_prefix + encoded)
        await self.writer.drain()

    async def recv(self) -> Message:
        if not self.reader:
            raise MeshError("Not connected")
        
        length_bytes = await self.reader.readexactly(4)
        length = int.from_bytes(length_bytes, byteorder='big')
        data = await self.reader.readexactly(length)
        
        parsed = json.loads(data.decode('utf-8'))
        return Message(
            topic=parsed["topic"],
            payload=bytes.fromhex(parsed["payload"]),
            sender_id=parsed["sender_id"],
            timestamp=parsed["timestamp"],
            msg_id=parsed["msg_id"]
        )

    async def close(self) -> None:
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
        self.reader = None
        self.writer = None
