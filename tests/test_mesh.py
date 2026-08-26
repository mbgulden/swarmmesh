import asyncio
from swarmmesh.mesh import EventMesh
from swarmmesh.types import ChannelConfig

def test_mesh_publish_subscribe():
    async def run_test():
        mesh = EventMesh("node1")
        await mesh.start()
        
        received = []
        async def handler(msg):
            received.append(msg)
            
        await mesh.subscribe("test_topic", handler)
        
        await mesh.publish("test_topic", b"hello mesh")
        
        # Give handler time to run
        await asyncio.sleep(0.01)
        
        assert len(received) == 1
        assert received[0].payload == b"hello mesh"
        
        await mesh.stop()

    asyncio.run(run_test())

def test_mesh_create_channel():
    mesh = EventMesh("node1")
    ch = mesh.create_channel(ChannelConfig(buffer_size=5))
    assert ch.config.buffer_size == 5
