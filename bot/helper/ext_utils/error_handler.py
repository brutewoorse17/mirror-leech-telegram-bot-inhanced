"""
Optimized error handling utility with retry mechanisms and circuit breakers.
"""

import asyncio
import functools
import logging
from typing import Any, Callable, Optional, Type, Union, Tuple
from time import time
from collections import defaultdict

logger = logging.getLogger(__name__)


class CircuitBreaker:
    """Circuit breaker pattern implementation for better error handling."""
    
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60, 
                 expected_exception: Type[Exception] = Exception):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'CLOSED'  # CLOSED, OPEN, HALF_OPEN
    
    def __call__(self, func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            if self.state == 'OPEN':
                if time() - self.last_failure_time < self.recovery_timeout:
                    raise Exception("Circuit breaker is OPEN")
                else:
                    self.state = 'HALF_OPEN'
            
            try:
                result = await func(*args, **kwargs)
                self._on_success()
                return result
            except self.expected_exception as e:
                self._on_failure()
                raise e
        
        return wrapper
    
    def _on_success(self):
        """Reset failure count on success."""
        self.failure_count = 0
        self.state = 'CLOSED'
    
    def _on_failure(self):
        """Increment failure count and potentially open circuit."""
        self.failure_count += 1
        self.last_failure_time = time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = 'OPEN'


class RetryConfig:
    """Configuration for retry mechanism."""
    
    def __init__(self, max_attempts: int = 3, base_delay: float = 1.0, 
                 max_delay: float = 60.0, exponential_base: float = 2.0,
                 jitter: bool = True):
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter


def retry_async(config: Optional[RetryConfig] = None, 
                exceptions: Tuple[Type[Exception], ...] = (Exception,),
                on_retry: Optional[Callable] = None):
    """
    Async retry decorator with exponential backoff and jitter.
    """
    if config is None:
        config = RetryConfig()
    
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(config.max_attempts):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    
                    if attempt == config.max_attempts - 1:
                        # Last attempt, don't wait
                        break
                    
                    # Calculate delay with exponential backoff
                    delay = min(
                        config.base_delay * (config.exponential_base ** attempt),
                        config.max_delay
                    )
                    
                    # Add jitter to prevent thundering herd
                    if config.jitter:
                        import random
                        delay *= (0.5 + random.random() * 0.5)
                    
                    logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}: {e}. "
                                 f"Retrying in {delay:.2f}s...")
                    
                    if on_retry:
                        await on_retry(attempt, e, delay)
                    
                    await asyncio.sleep(delay)
            
            # All attempts failed
            logger.error(f"All {config.max_attempts} attempts failed for {func.__name__}")
            raise last_exception
        
        return wrapper
    return decorator


class ErrorTracker:
    """Track errors for monitoring and alerting."""
    
    def __init__(self):
        self.error_counts = defaultdict(int)
        self.error_details = defaultdict(list)
        self.last_reset = time()
        self.reset_interval = 3600  # 1 hour
    
    def record_error(self, error_type: str, details: str = ""):
        """Record an error occurrence."""
        current_time = time()
        
        # Reset counters if interval passed
        if current_time - self.last_reset > self.reset_interval:
            self.reset_counters()
        
        self.error_counts[error_type] += 1
        self.error_details[error_type].append({
            'timestamp': current_time,
            'details': details
        })
        
        # Keep only last 10 error details per type
        if len(self.error_details[error_type]) > 10:
            self.error_details[error_type] = self.error_details[error_type][-10:]
    
    def get_error_summary(self) -> dict:
        """Get summary of recorded errors."""
        return {
            'counts': dict(self.error_counts),
            'total_errors': sum(self.error_counts.values()),
            'reset_time': self.last_reset
        }
    
    def reset_counters(self):
        """Reset error counters."""
        self.error_counts.clear()
        self.error_details.clear()
        self.last_reset = time()


# Global error tracker instance
error_tracker = ErrorTracker()


def handle_errors(error_types: Tuple[Type[Exception], ...] = (Exception,),
                 default_return: Any = None,
                 log_errors: bool = True):
    """
    Decorator to handle specific errors gracefully.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except error_types as e:
                if log_errors:
                    logger.error(f"Error in {func.__name__}: {str(e)}")
                
                error_tracker.record_error(
                    error_type=type(e).__name__,
                    details=f"{func.__name__}: {str(e)}"
                )
                
                return default_return
        
        return wrapper
    return decorator


async def safe_execute(coro, default_return: Any = None, 
                      log_errors: bool = True) -> Any:
    """
    Safely execute a coroutine with error handling.
    """
    try:
        return await coro
    except Exception as e:
        if log_errors:
            logger.error(f"Error in safe_execute: {str(e)}")
        
        error_tracker.record_error(
            error_type=type(e).__name__,
            details=str(e)
        )
        
        return default_return


class RateLimiter:
    """Simple rate limiter to prevent API abuse."""
    
    def __init__(self, max_calls: int, time_window: int):
        self.max_calls = max_calls
        self.time_window = time_window
        self.calls = []
    
    async def acquire(self):
        """Acquire permission to make a call."""
        now = time()
        
        # Remove old calls outside time window
        self.calls = [call_time for call_time in self.calls 
                     if now - call_time < self.time_window]
        
        if len(self.calls) >= self.max_calls:
            # Calculate wait time
            oldest_call = min(self.calls)
            wait_time = self.time_window - (now - oldest_call)
            
            if wait_time > 0:
                await asyncio.sleep(wait_time)
        
        self.calls.append(now)


def rate_limited(max_calls: int, time_window: int):
    """
    Decorator to rate limit function calls.
    """
    limiter = RateLimiter(max_calls, time_window)
    
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            await limiter.acquire()
            return await func(*args, **kwargs)
        
        return wrapper
    return decorator