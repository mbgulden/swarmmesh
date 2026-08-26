import asyncio
from swarmmesh.discovery import PeerDiscovery

def test_discovery_static_peer():
    async def run_test():
        d = PeerDiscovery("node1", 8080)
        
        discovered = []
        def on_discover(peer):
            discovered.append(peer)
            
        d.register_callback(on_discover)
        
        d.add_static_peer("localhost", 9090)
        
        assert len(discovered) == 1
        assert discovered[0].host == "localhost"
        assert discovered[0].port == 9090

    asyncio.run(run_test())

def test_discovery_start_stop():
    async def run_test():
        d = PeerDiscovery("node1", 8080)
        assert not d.running
        await d.start()
        assert d.running
        await d.stop()
        assert not d.running

    asyncio.run(run_test())
