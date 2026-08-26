import asyncio
import time
from swarmmesh.broker import PeerBroker
from swarmmesh.types import Message, TopicSubscription, PeerInfo
from swarmmesh.transport import InProcessTransport

def test_broker_subscribe_fanout():
    async def run_test():
        broker = PeerBroker("node1")
        received = []
        
        def handler(msg):
            received.append(msg)
            
        sub = TopicSubscription("topic1", handler)
        broker.subscribe(sub)
        
        msg = Message("topic1", b"data", "node2", time.time())
        await broker.fanout(msg)
        
        assert len(received) == 1
        assert received[0].payload == b"data"
        
    asyncio.run(run_test())

def test_broker_dead_peer():
    async def run_test():
        broker = PeerBroker("node1")
        broker.dead_peer_timeout = 0.1
        await broker.start()
        
        peer = PeerInfo("peer1", "localhost", 8080, time.time() - 1.0)
        t = InProcessTransport()
        broker.add_peer(peer, t)
        
        assert "peer1" in broker.peers
        
        await asyncio.sleep(0.2) # Let health check run
        
        assert "peer1" not in broker.peers
        
        await broker.stop()

    asyncio.run(run_test())

def test_broker_unsubscribe():
    broker = PeerBroker("node1")
    
    def handler(msg):
        pass
        
    sub = TopicSubscription("topic1", handler)
    broker.subscribe(sub)
    assert len(broker.subscriptions["topic1"]) == 1
    
    broker.unsubscribe("topic1", handler)
    assert len(broker.subscriptions["topic1"]) == 0
