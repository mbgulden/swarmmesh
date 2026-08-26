from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Any, Optional, Dict, List
import uuid

class MeshError(Exception):
    """Base exception for swarmmesh errors."""
    pass

class BackpressurePolicy(Enum):
    DROP_OLDEST = "drop_oldest"
    DROP_NEWEST = "drop_newest"
    BLOCK = "block"
    REJECT = "reject"

class TransportProtocol(Enum):
    TCP = "tcp"
    UNIX_SOCKET = "unix_socket"
    IN_PROCESS = "in_process"

@dataclass
class PeerInfo:
    peer_id: str
    host: str
    port: int
    last_seen: float
    capabilities: List[str] = field(default_factory=list)

@dataclass
class Message:
    topic: str
    payload: bytes
    sender_id: str
    timestamp: float
    msg_id: str = field(default_factory=lambda: str(uuid.uuid4()))

@dataclass
class ChannelConfig:
    buffer_size: int = 1000
    backpressure_threshold: int = 800
    timeout_seconds: float = 30.0

@dataclass
class TopicSubscription:
    topic: str
    handler: Callable[[Message], Any]
    filter_fn: Optional[Callable[[Message], bool]] = None
