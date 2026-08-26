import time
from typing import Deque
from collections import deque
from .types import BackpressurePolicy, MeshError

class BackpressureController:
    def __init__(self, policy: BackpressurePolicy, max_tokens_per_second: float = 100.0, queue_limit: int = 1000):
        self.policy = policy
        self.max_tokens_per_second = max_tokens_per_second
        self.queue_limit = queue_limit
        
        # Token bucket for rate limiting
        self.tokens = max_tokens_per_second
        self.last_update = time.time()
        
        # Track queue depth mock (since we are a controller)
        self.current_queue_depth = 0

    def _update_tokens(self) -> None:
        now = time.time()
        elapsed = now - self.last_update
        self.tokens = min(self.max_tokens_per_second, self.tokens + elapsed * self.max_tokens_per_second)
        self.last_update = now

    def acquire(self, amount: int = 1) -> bool:
        """Attempt to acquire permission to send."""
        self._update_tokens()
        
        if self.tokens >= amount:
            self.tokens -= amount
            return True
        return False

    def handle_queue_depth(self, current_depth: int) -> bool:
        """
        Called when items are being enqueued.
        Returns True if item should be enqueued, False if it should be rejected.
        Raises MeshError if policy dictates.
        """
        if current_depth < self.queue_limit:
            return True
            
        if self.policy == BackpressurePolicy.REJECT:
            raise MeshError("Queue full, rejecting")
        elif self.policy == BackpressurePolicy.DROP_OLDEST:
            # Caller must pop left before enqueueing
            return True 
        elif self.policy == BackpressurePolicy.DROP_NEWEST:
            return False
        elif self.policy == BackpressurePolicy.BLOCK:
            # Caller should block
            return False
        return True
