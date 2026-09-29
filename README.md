# 🕸️ SwarmMesh

[![CI](https://github.com/mbgulden/swarmmesh/actions/workflows/ci.yml/badge.svg)](https://github.com/mbgulden/swarmmesh/actions)
[![PyPI version](https://img.shields.io/badge/pypi-v0.1.0-blue.svg)](https://pypi.org/project/swarmmesh/)
[![Python 3.9+](https://img.shields.io/badge/python-3.9%20%7C%203.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> **Zero-dependency distributed event mesh and peer-to-peer agent RPC system**  
> *Topic-based pub/sub, streaming channels, and backpressure for multi-agent swarms — pure Python standard library, no runtime dependencies.*

---

## 💡 Why SwarmMesh?

Multi-agent swarms need a communication layer: agents must publish events, subscribe to topics, stream tokens to each other, and survive slow consumers without bringing down the mesh. The usual answers are heavyweight brokers (Kafka, RabbitMQ, Redis) or hand-rolled asyncio plumbing in every project.

**SwarmMesh** is the lightweight middle ground:
- **Topic-based pub/sub** — `publish(topic, payload)` fans out to local subscribers and connected peers.
- **Stream channels** — async-iterable token channels for streaming workloads (e.g. LLM token streams between agents).
- **Backpressure policies** — drop-oldest, drop-newest, block, or reject when consumers can't keep up.
- **Pluggable transports** — in-process for tests and single-process swarms, TCP with length-prefixed JSON framing for real peers.
- **Zero runtime dependencies** — `asyncio`, `dataclasses`, `json`, `socket`. Nothing else. `pip install swarmmesh` and you're done.

---

## 🏛️ Architecture

```
                    ┌──────────────────────────────────────────────┐
                    │                   EventMesh                  │
                    │   node_id · publish() · subscribe()          │
                    │   create_channel() · start() / stop()        │
                    └──────┬───────────────┬───────────────┬───────┘
                           │               │               │
                ┌──────────▼──────┐ ┌──────▼────────┐ ┌────▼──────────┐
                │   PeerBroker    │ │ StreamChannel │ │ PeerDiscovery │
                │  topic fanout   │ │  token stream │ │ static peers  │
                │  subscriptions  │ │  backpressure │ │  callbacks    │
                │  dead-peer GC   │ │  async-iter   │ │               │
                └──────────┬──────┘ └───────────────┘ └───────────────┘
                           │
                ┌──────────▼──────────────────────────────────────┐
                │                 Transports                      │
                │  InProcessTransport (asyncio.Queue)             │
                │  TCPTransport (length-prefixed JSON frames)     │
                └─────────────────────────────────────────────────┘
```

### Core components

| Component | Module | What it does |
|---|---|---|
| `EventMesh` | `swarmmesh.mesh` | Top-level node: owns a broker, discovery, and channel factory. `publish(topic, payload)`, `subscribe(topic, handler)`, `create_channel(config)`. |
| `PeerBroker` | `swarmmesh.broker` | Topic fanout to local subscribers and all connected peers; health-check loop evicts peers unseen for `dead_peer_timeout` (default 60s). |
| `StreamChannel` | `swarmmesh.channel` | Bounded `asyncio.Queue` token channel; `send_token(bytes)` blocks when full (natural backpressure), `async for` consumption, `close()`. |
| `BackpressureController` | `swarmmesh.backpressure` | Token-bucket rate limiter (`acquire()`) plus queue-depth policy (`handle_queue_depth()`): `DROP_OLDEST`, `DROP_NEWEST`, `BLOCK`, `REJECT`. |
| `PeerDiscovery` | `swarmmesh.discovery` | Peer registry with discovery callbacks; static peer registration via `add_static_peer(host, port)`. |
| Transports | `swarmmesh.transport` | `Transport` ABC (`connect`/`send`/`recv`/`close`); `InProcessTransport` (in-memory queue) and `TCPTransport` (asyncio streams, length-prefixed JSON). |

---

## 📦 Installation

```bash
pip install swarmmesh
```

*Pure Python standard library. Zero runtime dependencies. Python 3.9+.*

From source:

```bash
git clone https://github.com/mbgulden/swarmmesh.git
cd swarmmesh
pip install -e ".[test]"   # test extra: pytest, pytest-asyncio, mypy
```

---

## 🚀 Quick Start (< 5 minutes)

Publish and subscribe between two coroutines on one mesh node:

```python
import asyncio
from swarmmesh import EventMesh

async def main():
    mesh = EventMesh("agent-1")
    await mesh.start()

    async def on_task(msg):
        print(f"[{msg.topic}] from {msg.sender_id}: {msg.payload!r}")

    await mesh.subscribe("tasks", on_task)
    await mesh.publish("tasks", b'{"action": "summarize", "doc": "q3-report"}')

    await asyncio.sleep(0.1)  # let the handler run
    await mesh.stop()

asyncio.run(main())
```

Stream tokens through a backpressured channel:

```python
import asyncio
from swarmmesh import EventMesh, ChannelConfig

async def main():
    mesh = EventMesh("agent-1")
    ch = mesh.create_channel(ChannelConfig(buffer_size=100, timeout_seconds=5.0))

    async def producer():
        for token in [b"Hello", b" ", b"swarm"]:
            await ch.send_token(token)   # blocks when the buffer is full
        await ch.close()

    asyncio.create_task(producer())

    async for token in ch:               # async-iterable consumer
        print(token.decode(), end="")
    print()

asyncio.run(main())
```

Register peers and react to discovery:

```python
from swarmmesh import PeerDiscovery

discovery = PeerDiscovery("agent-1", 9000)
discovery.register_callback(lambda peer: print("discovered:", peer.peer_id))
discovery.add_static_peer("192.168.1.10", 9000)   # fires the callback
```

Apply backpressure policy to a queue:

```python
from swarmmesh import BackpressureController, BackpressurePolicy

ctrl = BackpressureController(BackpressurePolicy.DROP_NEWEST,
                              max_tokens_per_second=50.0, queue_limit=500)
if ctrl.acquire():          # token-bucket rate limit
    ...  # send

if ctrl.handle_queue_depth(current_depth):   # True = enqueue, False = drop
    ...  # enqueue
```

---

## ⌨️ CLI

The `swarmmesh` console script ships with the package:

```bash
swarmmesh status                          # Mesh status: OK
swarmmesh peers                           # Known peers: []
swarmmesh topics                          # Active topics: []
swarmmesh publish tasks '{"action":"go"}' # Published to tasks: {"action":"go"}
```

---

## 🐍 Python API Overview

Everything is importable from the top-level package:

```python
from swarmmesh import (
    EventMesh,            # node facade: publish / subscribe / channels
    PeerBroker,           # topic fanout, subscriptions, dead-peer GC
    StreamChannel,        # async-iterable token channel
    BackpressureController,
    PeerDiscovery,        # peer registry + discovery callbacks
    Transport, InProcessTransport,
    Message, PeerInfo, ChannelConfig, TopicSubscription,
    BackpressurePolicy, TransportProtocol, MeshError,
)
from swarmmesh.transport import TCPTransport   # TCP peer transport
```

**Message model** — `Message(topic, payload: bytes, sender_id, timestamp, msg_id)`; `msg_id` is a UUID assigned automatically. Payloads are raw bytes — serialize however you like (JSON, msgpack, protobuf).

**Subscriptions** — `TopicSubscription(topic, handler, filter_fn=None)`; handlers may be sync or async, and handler exceptions are isolated per-subscriber so one bad subscriber can't break fanout.

**Transports** — `TransportProtocol` enumerates `TCP`, `UNIX_SOCKET`, `IN_PROCESS`. `InProcessTransport` is fully working (queue-based); `TCPTransport` implements length-prefixed JSON framing over `asyncio` streams.

---

## 🗺️ Swarm Ecosystem

SwarmMesh is part of the **Swarm Primitives Ecosystem** for autonomous agent swarms:

- 🕸️ **SwarmMesh**: Distributed event mesh and peer-to-peer agent RPC — the communication primitive.
- 🔒 **SwarmLock**: Tokenized, non-blocking distributed advisory locks.
- ⏱️ **SwarmCron**: Native high-precision background cron scheduling.
- 🛡️ **SwarmProof**: Truth Oracle, evidence ledgers, and anti-hallucination gates.
- 🔀 **SwarmRouter**: Intelligent query routing and model cascading *(coming soon)*.
- 🧠 **SwarmCurator**: Long-term memory distillation and context compaction *(coming soon)*.

---

## 📄 License

MIT © GrowthWebDev
