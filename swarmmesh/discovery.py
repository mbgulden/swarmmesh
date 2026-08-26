import asyncio
import socket
import json
import time
from typing import List, Callable, Optional
from .types import PeerInfo

class PeerDiscovery:
    def __init__(self, node_id: str, port: int):
        self.node_id = node_id
        self.port = port
        self.known_peers: List[PeerInfo] = []
        self.on_peer_discovered: Optional[Callable[[PeerInfo], None]] = None
        self.running = False
        
    def add_static_peer(self, host: str, port: int) -> None:
        # Dummy ID for static peer, real ID negotiated later
        info = PeerInfo(
            peer_id=f"static_{host}_{port}",
            host=host,
            port=port,
            last_seen=time.time()
        )
        self.known_peers.append(info)
        if self.on_peer_discovered:
            self.on_peer_discovered(info)

    def register_callback(self, cb: Callable[[PeerInfo], None]) -> None:
        self.on_peer_discovered = cb
        for peer in self.known_peers:
            cb(peer)

    async def start(self) -> None:
        self.running = True
        # In a real implementation, this would bind a UDP socket and broadcast
        # For this prototype, we'll just simulate by doing nothing active
        pass

    async def stop(self) -> None:
        self.running = False
