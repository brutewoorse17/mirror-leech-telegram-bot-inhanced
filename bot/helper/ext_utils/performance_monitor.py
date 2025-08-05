"""
Performance monitoring utility for tracking bot performance and identifying bottlenecks.
"""

import asyncio
import functools
import psutil
import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)


class PerformanceMetrics:
    """Track performance metrics for the bot."""
    
    def __init__(self, max_history: int = 1000):
        self.max_history = max_history
        self.function_metrics = defaultdict(lambda: {
            'call_count': 0,
            'total_time': 0.0,
            'avg_time': 0.0,
            'min_time': float('inf'),
            'max_time': 0.0,
            'recent_times': deque(maxlen=100)
        })
        self.system_metrics = deque(maxlen=max_history)
        self.error_counts = defaultdict(int)
        self.start_time = time.time()
    
    def record_function_call(self, func_name: str, execution_time: float):
        """Record metrics for a function call."""
        metrics = self.function_metrics[func_name]
        metrics['call_count'] += 1
        metrics['total_time'] += execution_time
        metrics['avg_time'] = metrics['total_time'] / metrics['call_count']
        metrics['min_time'] = min(metrics['min_time'], execution_time)
        metrics['max_time'] = max(metrics['max_time'], execution_time)
        metrics['recent_times'].append(execution_time)
    
    def record_system_metrics(self):
        """Record current system metrics."""
        try:
            cpu_percent = psutil.cpu_percent()
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            self.system_metrics.append({
                'timestamp': time.time(),
                'cpu_percent': cpu_percent,
                'memory_percent': memory.percent,
                'memory_used_mb': memory.used / (1024 * 1024),
                'disk_percent': disk.percent,
                'disk_used_gb': disk.used / (1024 * 1024 * 1024)
            })
        except Exception as e:
            logger.error(f"Error recording system metrics: {e}")
    
    def get_function_stats(self, func_name: str) -> Dict:
        """Get statistics for a specific function."""
        return dict(self.function_metrics.get(func_name, {}))
    
    def get_top_functions(self, limit: int = 10, sort_by: str = 'avg_time') -> List[Dict]:
        """Get top functions by specified metric."""
        functions = []
        for func_name, metrics in self.function_metrics.items():
            functions.append({
                'name': func_name,
                **metrics
            })
        
        return sorted(functions, key=lambda x: x.get(sort_by, 0), reverse=True)[:limit]
    
    def get_system_summary(self) -> Dict:
        """Get summary of system metrics."""
        if not self.system_metrics:
            return {}
        
        recent_metrics = list(self.system_metrics)[-10:]  # Last 10 readings
        
        return {
            'current': recent_metrics[-1] if recent_metrics else {},
            'avg_cpu': sum(m['cpu_percent'] for m in recent_metrics) / len(recent_metrics),
            'avg_memory': sum(m['memory_percent'] for m in recent_metrics) / len(recent_metrics),
            'uptime_seconds': time.time() - self.start_time
        }
    
    def record_error(self, error_type: str):
        """Record an error occurrence."""
        self.error_counts[error_type] += 1
    
    def get_error_summary(self) -> Dict:
        """Get error summary."""
        return dict(self.error_counts)


# Global performance metrics instance
performance_metrics = PerformanceMetrics()


def monitor_performance(func_name: Optional[str] = None):
    """Decorator to monitor function performance."""
    def decorator(func):
        name = func_name or f"{func.__module__}.{func.__name__}"
        
        if asyncio.iscoroutinefunction(func):
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                start_time = time.time()
                try:
                    result = await func(*args, **kwargs)
                    return result
                except Exception as e:
                    performance_metrics.record_error(type(e).__name__)
                    raise
                finally:
                    execution_time = time.time() - start_time
                    performance_metrics.record_function_call(name, execution_time)
            return async_wrapper
        else:
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                start_time = time.time()
                try:
                    result = func(*args, **kwargs)
                    return result
                except Exception as e:
                    performance_metrics.record_error(type(e).__name__)
                    raise
                finally:
                    execution_time = time.time() - start_time
                    performance_metrics.record_function_call(name, execution_time)
            return sync_wrapper
    
    return decorator


@asynccontextmanager
async def performance_context(operation_name: str):
    """Context manager for monitoring performance of code blocks."""
    start_time = time.time()
    try:
        yield
    except Exception as e:
        performance_metrics.record_error(type(e).__name__)
        raise
    finally:
        execution_time = time.time() - start_time
        performance_metrics.record_function_call(operation_name, execution_time)


class PerformanceReporter:
    """Generate performance reports."""
    
    @staticmethod
    def generate_report() -> Dict[str, Any]:
        """Generate comprehensive performance report."""
        return {
            'system_summary': performance_metrics.get_system_summary(),
            'top_functions_by_time': performance_metrics.get_top_functions(10, 'avg_time'),
            'top_functions_by_calls': performance_metrics.get_top_functions(10, 'call_count'),
            'error_summary': performance_metrics.get_error_summary(),
            'total_functions_monitored': len(performance_metrics.function_metrics)
        }
    
    @staticmethod
    def log_performance_summary():
        """Log a performance summary."""
        report = PerformanceReporter.generate_report()
        
        logger.info("=== Performance Summary ===")
        
        system = report['system_summary']
        if system:
            logger.info(f"Uptime: {system.get('uptime_seconds', 0):.1f}s")
            logger.info(f"CPU: {system.get('avg_cpu', 0):.1f}%")
            logger.info(f"Memory: {system.get('avg_memory', 0):.1f}%")
        
        logger.info(f"Functions monitored: {report['total_functions_monitored']}")
        
        if report['top_functions_by_time']:
            logger.info("Slowest functions:")
            for func in report['top_functions_by_time'][:5]:
                logger.info(f"  {func['name']}: {func['avg_time']:.3f}s avg "
                          f"({func['call_count']} calls)")
        
        errors = report['error_summary']
        if errors:
            logger.info(f"Errors: {sum(errors.values())} total")
            for error_type, count in sorted(errors.items(), key=lambda x: x[1], reverse=True)[:3]:
                logger.info(f"  {error_type}: {count}")


class SystemMonitor:
    """Monitor system resources periodically."""
    
    def __init__(self, interval: int = 60):
        self.interval = interval
        self.monitoring = False
        self.task = None
    
    async def start_monitoring(self):
        """Start system monitoring."""
        if self.monitoring:
            return
        
        self.monitoring = True
        self.task = asyncio.create_task(self._monitor_loop())
        logger.info("System monitoring started")
    
    async def stop_monitoring(self):
        """Stop system monitoring."""
        self.monitoring = False
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        logger.info("System monitoring stopped")
    
    async def _monitor_loop(self):
        """Main monitoring loop."""
        while self.monitoring:
            try:
                performance_metrics.record_system_metrics()
                await asyncio.sleep(self.interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(self.interval)


# Global system monitor instance
system_monitor = SystemMonitor()


async def start_performance_monitoring():
    """Start all performance monitoring."""
    await system_monitor.start_monitoring()
    
    # Log performance summary every 30 minutes
    async def periodic_report():
        while True:
            await asyncio.sleep(1800)  # 30 minutes
            PerformanceReporter.log_performance_summary()
    
    asyncio.create_task(periodic_report())
    logger.info("Performance monitoring initialized")


async def stop_performance_monitoring():
    """Stop all performance monitoring."""
    await system_monitor.stop_monitoring()
    PerformanceReporter.log_performance_summary()  # Final report