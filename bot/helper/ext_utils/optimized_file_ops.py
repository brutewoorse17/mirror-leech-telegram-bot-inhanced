"""
Optimized file operations utility for better performance and memory efficiency.
"""

import aiofiles
from asyncio import gather, create_subprocess_exec, PIPE
from pathlib import Path
from typing import AsyncGenerator, Optional, Union
import os

# Optimized buffer sizes for different operations
BUFFER_SIZE_SMALL = 64 * 1024  # 64KB for small files
BUFFER_SIZE_MEDIUM = 1024 * 1024  # 1MB for medium files  
BUFFER_SIZE_LARGE = 8 * 1024 * 1024  # 8MB for large files

class OptimizedFileOps:
    """Optimized file operations with better buffering and async I/O."""
    
    @staticmethod
    def get_optimal_buffer_size(file_size: int) -> int:
        """Get optimal buffer size based on file size."""
        if file_size < 1024 * 1024:  # < 1MB
            return BUFFER_SIZE_SMALL
        elif file_size < 100 * 1024 * 1024:  # < 100MB
            return BUFFER_SIZE_MEDIUM
        else:
            return BUFFER_SIZE_LARGE
    
    @staticmethod
    async def copy_file_optimized(src: Union[str, Path], dst: Union[str, Path], 
                                 file_size: Optional[int] = None) -> None:
        """
        Copy file with optimized buffering based on file size.
        """
        src_path = Path(src)
        dst_path = Path(dst)
        
        if file_size is None:
            file_size = src_path.stat().st_size
            
        buffer_size = OptimizedFileOps.get_optimal_buffer_size(file_size)
        
        async with aiofiles.open(src_path, 'rb') as src_file:
            async with aiofiles.open(dst_path, 'wb') as dst_file:
                while True:
                    chunk = await src_file.read(buffer_size)
                    if not chunk:
                        break
                    await dst_file.write(chunk)
    
    @staticmethod
    async def read_file_chunked(file_path: Union[str, Path], 
                               chunk_size: Optional[int] = None) -> AsyncGenerator[bytes, None]:
        """
        Read file in optimized chunks.
        """
        path = Path(file_path)
        file_size = path.stat().st_size
        
        if chunk_size is None:
            chunk_size = OptimizedFileOps.get_optimal_buffer_size(file_size)
        
        async with aiofiles.open(path, 'rb') as file:
            while True:
                chunk = await file.read(chunk_size)
                if not chunk:
                    break
                yield chunk
    
    @staticmethod
    async def get_file_size_async(file_path: Union[str, Path]) -> int:
        """Get file size asynchronously."""
        return (await aiofiles.os.stat(file_path)).st_size
    
    @staticmethod
    async def create_directory_tree(path: Union[str, Path]) -> None:
        """Create directory tree if it doesn't exist."""
        path = Path(path)
        if not path.exists():
            await aiofiles.os.makedirs(path, exist_ok=True)
    
    @staticmethod
    async def cleanup_empty_dirs(root_path: Union[str, Path]) -> None:
        """Remove empty directories recursively."""
        root = Path(root_path)
        
        # Get all subdirectories
        subdirs = [p for p in root.rglob('*') if p.is_dir()]
        
        # Sort by depth (deepest first) to remove from bottom up
        subdirs.sort(key=lambda p: len(p.parts), reverse=True)
        
        for subdir in subdirs:
            try:
                if not any(subdir.iterdir()):  # Directory is empty
                    await aiofiles.os.rmdir(subdir)
            except OSError:
                # Directory not empty or other error, skip
                pass
    
    @staticmethod
    async def move_file_optimized(src: Union[str, Path], dst: Union[str, Path]) -> None:
        """
        Move file with fallback to copy+delete for cross-device moves.
        """
        src_path = Path(src)
        dst_path = Path(dst)
        
        try:
            # Try atomic move first
            await aiofiles.os.rename(src_path, dst_path)
        except OSError:
            # Cross-device move, use copy+delete
            file_size = await OptimizedFileOps.get_file_size_async(src_path)
            await OptimizedFileOps.copy_file_optimized(src_path, dst_path, file_size)
            await aiofiles.os.remove(src_path)
    
    @staticmethod
    async def batch_file_operations(operations: list) -> list:
        """
        Execute multiple file operations in parallel.
        Each operation should be a coroutine.
        """
        return await gather(*operations, return_exceptions=True)


class FileCache:
    """Simple file metadata cache to avoid repeated stat calls."""
    
    def __init__(self, max_size: int = 1000):
        self.cache = {}
        self.max_size = max_size
    
    async def get_file_size(self, file_path: Union[str, Path]) -> int:
        """Get file size with caching."""
        path_str = str(file_path)
        
        if path_str not in self.cache:
            if len(self.cache) >= self.max_size:
                # Simple LRU: remove oldest entry
                oldest_key = next(iter(self.cache))
                del self.cache[oldest_key]
            
            size = await OptimizedFileOps.get_file_size_async(file_path)
            self.cache[path_str] = size
        
        return self.cache[path_str]
    
    def clear(self):
        """Clear the cache."""
        self.cache.clear()


# Global file cache instance
file_cache = FileCache()