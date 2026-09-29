import asyncio
import time

from swarmmesh.transport import InProcessTransport
from swarmmesh.types import MeshError, Message


def test_inprocess_transport_send_recv():
    async def run_test():
        t = InProcessTransport()
        await t.connect()
        msg = Message("test", b"hello", "sender", time.time())
        await t.send(msg)
        recv_msg = await t.recv()
        assert recv_msg.payload == b"hello"
        await t.close()

    asyncio.run(run_test())

def test_inprocess_transport_not_connected():
    async def run_test():
        t = InProcessTransport()
        msg = Message("test", b"hello", "sender", time.time())
        try:
            await t.send(msg)
            assert False, "Should have raised MeshError"
        except MeshError:
            pass
            
        try:
            await t.recv()
            assert False, "Should have raised MeshError"
        except MeshError:
            pass

    asyncio.run(run_test())

def test_inprocess_transport_construct_without_running_loop():
    # Regression test for Python 3.9: constructing a transport must not touch
    # the event loop (asyncio.Queue binds to the loop at construction time
    # there and raises RuntimeError when there is none).
    t = InProcessTransport()
    assert t.queue is None
    assert not t.is_connected
