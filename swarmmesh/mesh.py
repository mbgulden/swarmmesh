import asyncio
import time
from typing import Callable, Any
from .types import Message, ChannelConfig, TopicSubscription, PeerInfo
from .broker import PeerBroker
from .discovery import PeerDiscovery
from .channel import StreamChannel
from .transport import InProcessTransport

class EventMesh:
    def __init__(self, node_id: str, port: int = 0):
        self.node_id = node_id
        self.broker = PeerBroker(node_id)
        self.discovery = PeerDiscovery(node_id, port)
        self.running = False
        
        self.discovery.register_callback(self._on_peer_discovered)

    def _on_peer_discovered(self, peer: PeerInfo) -> None:
        # Connect to peer (simplified using InProcessTransport for demo)
        transport = InProcessTransport()
        asyncio.create_task(transport.connect())
        self.broker.add_peer(peer, transport)

    async def start(self) -> None:
        self.running = True
        await self.broker.start()
        await self.discovery.start()

    async def stop(self) -> None:
        self.running = False
        await self.discovery.stop()
        await self.broker.stop()

    async def publish(self, topic: str, payload: bytes) -> None:
        msg = Message(
            topic=topic,
            payload=payload,
            sender_id=self.node_id,
            timestamp=time.time()
        )
        await self.broker.fanout(msg)

    async def subscribe(self, topic: str, handler: Callable[[Message], Any]) -> None:
        sub = TopicSubscription(topic=topic, handler=handler)
        self.broker.subscribe(sub)

    def create_channel(self, config: ChannelConfig = None) -> StreamChannel:
        if config is None:
            config = ChannelConfig()
        return StreamChannel(config)
