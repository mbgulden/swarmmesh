import asyncio
import time
from typing import Dict, List, Optional, Callable, Any

from .types import Message, PeerInfo, TopicSubscription, MeshError
from .transport import Transport

class PeerBroker:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.peers: Dict[str, PeerInfo] = {}
        self.connections: Dict[str, Transport] = {}
        self.subscriptions: Dict[str, List[TopicSubscription]] = {}
        self.running = False
        self.dead_peer_timeout = 60.0

    async def start(self) -> None:
        self.running = True
        asyncio.create_task(self._health_check_loop())

    async def stop(self) -> None:
        self.running = False
        for peer_id, conn in self.connections.items():
            await conn.close()
        self.connections.clear()
        self.peers.clear()

    def add_peer(self, peer: PeerInfo, transport: Transport) -> None:
        self.peers[peer.peer_id] = peer
        self.connections[peer.peer_id] = transport

    def remove_peer(self, peer_id: str) -> None:
        if peer_id in self.peers:
            del self.peers[peer_id]
        if peer_id in self.connections:
            asyncio.create_task(self.connections[peer_id].close())
            del self.connections[peer_id]

    def subscribe(self, sub: TopicSubscription) -> None:
        if sub.topic not in self.subscriptions:
            self.subscriptions[sub.topic] = []
        self.subscriptions[sub.topic].append(sub)

    def unsubscribe(self, topic: str, handler: Callable) -> None:
        if topic in self.subscriptions:
            self.subscriptions[topic] = [
                s for s in self.subscriptions[topic] if s.handler != handler
            ]

    async def fanout(self, message: Message) -> None:
        # Deliver locally
        if message.topic in self.subscriptions:
            for sub in self.subscriptions[message.topic]:
                if sub.filter_fn is None or sub.filter_fn(message):
                    try:
                        res = sub.handler(message)
                        if asyncio.iscoroutine(res):
                            await res
                    except Exception:
                        pass # Ignore handler errors
        
        # Send to peers (very basic routing: send to all)
        # In a real mesh this would be smarter
        for peer_id, conn in self.connections.items():
            if peer_id != message.sender_id:
                try:
                    await conn.send(message)
                except Exception:
                    # Ignore send errors, health check will catch it
                    pass

    async def _health_check_loop(self) -> None:
        while self.running:
            now = time.time()
            dead_peers = []
            for peer_id, info in self.peers.items():
                if now - info.last_seen > self.dead_peer_timeout:
                    dead_peers.append(peer_id)
            
            for peer_id in dead_peers:
                self.remove_peer(peer_id)
                
            await asyncio.sleep(5.0)

    def record_heartbeat(self, peer_id: str) -> None:
        if peer_id in self.peers:
            self.peers[peer_id].last_seen = time.time()
