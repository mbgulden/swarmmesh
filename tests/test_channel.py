import asyncio
from swarmmesh.channel import StreamChannel
from swarmmesh.types import ChannelConfig, MeshError

def test_channel_send_recv():
    async def run_test():
        config = ChannelConfig(buffer_size=10)
        ch = StreamChannel(config)
        
        await ch.send_token(b"token1")
        await ch.send_token(b"token2")
        
        assert await ch._recv_token() == b"token1"
        assert await ch._recv_token() == b"token2"

    asyncio.run(run_test())

def test_channel_async_iter():
    async def run_test():
        config = ChannelConfig(buffer_size=10)
        ch = StreamChannel(config)
        
        async def producer():
            await ch.send_token(b"A")
            await ch.send_token(b"B")
            await ch.close()
            
        asyncio.create_task(producer())
        
        tokens = []
        async for token in ch:
            tokens.append(token)
            
        assert tokens == [b"A", b"B"]

    asyncio.run(run_test())

def test_channel_send_closed():
    async def run_test():
        config = ChannelConfig(buffer_size=10)
        ch = StreamChannel(config)
        await ch.close()
        
        try:
            await ch.send_token(b"fail")
            assert False, "Should raise MeshError"
        except MeshError:
            pass

    asyncio.run(run_test())
