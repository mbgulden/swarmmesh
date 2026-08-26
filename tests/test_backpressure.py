import time
from swarmmesh.backpressure import BackpressureController
from swarmmesh.types import BackpressurePolicy, MeshError

def test_backpressure_rate_limit():
    # 10 tokens per second
    ctrl = BackpressureController(BackpressurePolicy.REJECT, 10.0, 100)
    
    # Should be able to acquire 10
    for _ in range(10):
        assert ctrl.acquire(1) == True
        
    # 11th should fail
    assert ctrl.acquire(1) == False

def test_backpressure_queue_reject():
    ctrl = BackpressureController(BackpressurePolicy.REJECT, 100.0, 10)
    
    assert ctrl.handle_queue_depth(5) == True
    
    try:
        ctrl.handle_queue_depth(10)
        assert False, "Should raise MeshError"
    except MeshError:
        pass

def test_backpressure_queue_drop_newest():
    ctrl = BackpressureController(BackpressurePolicy.DROP_NEWEST, 100.0, 10)
    
    assert ctrl.handle_queue_depth(9) == True
    assert ctrl.handle_queue_depth(10) == False
