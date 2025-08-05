import sqlite3
import os
import hashlib
from logging import getLogger
from datetime import datetime
from typing import Optional, List, Dict, Tuple

LOGGER = getLogger(__name__)

class HashDatabase:
    def __init__(self, db_path: str = "file_hashes.db"):
        self.db_path = db_path
        self._init_database()
    
    def _init_database(self):
        """Initialize the hash database with required tables"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS file_hashes (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        file_id TEXT UNIQUE,
                        file_name TEXT,
                        file_size INTEGER,
                        md5_hash TEXT,
                        sha1_hash TEXT,
                        drive_id TEXT,
                        mime_type TEXT,
                        download_date TIMESTAMP,
                        file_path TEXT,
                        INDEX(md5_hash),
                        INDEX(sha1_hash),
                        INDEX(file_id)
                    )
                ''')
                
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS duplicate_groups (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        hash_value TEXT,
                        hash_type TEXT,
                        file_count INTEGER,
                        total_size INTEGER,
                        created_date TIMESTAMP,
                        INDEX(hash_value, hash_type)
                    )
                ''')
                
                conn.commit()
                LOGGER.info("Hash database initialized successfully")
        except Exception as e:
            LOGGER.error(f"Failed to initialize hash database: {e}")
            raise
    
    def add_file_hash(self, file_id: str, file_name: str, file_size: int, 
                      md5_hash: str = None, sha1_hash: str = None, 
                      drive_id: str = None, mime_type: str = None, 
                      file_path: str = None) -> bool:
        """Add or update file hash information"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO file_hashes 
                    (file_id, file_name, file_size, md5_hash, sha1_hash, 
                     drive_id, mime_type, download_date, file_path)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (file_id, file_name, file_size, md5_hash, sha1_hash, 
                      drive_id, mime_type, datetime.now(), file_path))
                conn.commit()
                
                # Update duplicate groups
                if md5_hash:
                    self._update_duplicate_group(md5_hash, 'md5', file_size)
                if sha1_hash:
                    self._update_duplicate_group(sha1_hash, 'sha1', file_size)
                
                return True
        except Exception as e:
            LOGGER.error(f"Failed to add file hash: {e}")
            return False
    
    def _update_duplicate_group(self, hash_value: str, hash_type: str, file_size: int):
        """Update duplicate group statistics"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Count files with this hash
                if hash_type == 'md5':
                    cursor.execute('SELECT COUNT(*) FROM file_hashes WHERE md5_hash = ?', (hash_value,))
                else:
                    cursor.execute('SELECT COUNT(*) FROM file_hashes WHERE sha1_hash = ?', (hash_value,))
                
                file_count = cursor.fetchone()[0]
                
                cursor.execute('''
                    INSERT OR REPLACE INTO duplicate_groups 
                    (hash_value, hash_type, file_count, total_size, created_date)
                    VALUES (?, ?, ?, ?, ?)
                ''', (hash_value, hash_type, file_count, file_count * file_size, datetime.now()))
                
                conn.commit()
        except Exception as e:
            LOGGER.error(f"Failed to update duplicate group: {e}")
    
    def check_duplicate_by_hash(self, md5_hash: str = None, sha1_hash: str = None) -> List[Dict]:
        """Check if file with given hash already exists"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                if md5_hash:
                    cursor.execute('''
                        SELECT file_id, file_name, file_size, drive_id, mime_type, 
                               download_date, file_path 
                        FROM file_hashes 
                        WHERE md5_hash = ?
                    ''', (md5_hash,))
                elif sha1_hash:
                    cursor.execute('''
                        SELECT file_id, file_name, file_size, drive_id, mime_type, 
                               download_date, file_path 
                        FROM file_hashes 
                        WHERE sha1_hash = ?
                    ''', (sha1_hash,))
                else:
                    return []
                
                results = cursor.fetchall()
                return [
                    {
                        'file_id': row[0],
                        'file_name': row[1],
                        'file_size': row[2],
                        'drive_id': row[3],
                        'mime_type': row[4],
                        'download_date': row[5],
                        'file_path': row[6]
                    }
                    for row in results
                ]
        except Exception as e:
            LOGGER.error(f"Failed to check duplicate by hash: {e}")
            return []
    
    def check_duplicate_by_file_id(self, file_id: str) -> Optional[Dict]:
        """Check if file ID already exists in database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT file_id, file_name, file_size, md5_hash, sha1_hash,
                           drive_id, mime_type, download_date, file_path 
                    FROM file_hashes 
                    WHERE file_id = ?
                ''', (file_id,))
                
                row = cursor.fetchone()
                if row:
                    return {
                        'file_id': row[0],
                        'file_name': row[1],
                        'file_size': row[2],
                        'md5_hash': row[3],
                        'sha1_hash': row[4],
                        'drive_id': row[5],
                        'mime_type': row[6],
                        'download_date': row[7],
                        'file_path': row[8]
                    }
                return None
        except Exception as e:
            LOGGER.error(f"Failed to check duplicate by file ID: {e}")
            return None
    
    def get_duplicate_groups(self, hash_type: str = 'md5', min_files: int = 2) -> List[Dict]:
        """Get groups of duplicate files"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT hash_value, hash_type, file_count, total_size, created_date
                    FROM duplicate_groups 
                    WHERE hash_type = ? AND file_count >= ?
                    ORDER BY file_count DESC, total_size DESC
                ''', (hash_type, min_files))
                
                results = cursor.fetchall()
                return [
                    {
                        'hash_value': row[0],
                        'hash_type': row[1],
                        'file_count': row[2],
                        'total_size': row[3],
                        'created_date': row[4]
                    }
                    for row in results
                ]
        except Exception as e:
            LOGGER.error(f"Failed to get duplicate groups: {e}")
            return []
    
    def get_files_by_hash(self, hash_value: str, hash_type: str = 'md5') -> List[Dict]:
        """Get all files with specific hash"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                if hash_type == 'md5':
                    cursor.execute('''
                        SELECT file_id, file_name, file_size, drive_id, mime_type, 
                               download_date, file_path 
                        FROM file_hashes 
                        WHERE md5_hash = ?
                        ORDER BY download_date DESC
                    ''', (hash_value,))
                else:
                    cursor.execute('''
                        SELECT file_id, file_name, file_size, drive_id, mime_type, 
                               download_date, file_path 
                        FROM file_hashes 
                        WHERE sha1_hash = ?
                        ORDER BY download_date DESC
                    ''', (hash_value,))
                
                results = cursor.fetchall()
                return [
                    {
                        'file_id': row[0],
                        'file_name': row[1],
                        'file_size': row[2],
                        'drive_id': row[3],
                        'mime_type': row[4],
                        'download_date': row[5],
                        'file_path': row[6]
                    }
                    for row in results
                ]
        except Exception as e:
            LOGGER.error(f"Failed to get files by hash: {e}")
            return []
    
    def remove_file_hash(self, file_id: str) -> bool:
        """Remove file hash from database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Get hash values before deletion for group update
                cursor.execute('SELECT md5_hash, sha1_hash FROM file_hashes WHERE file_id = ?', (file_id,))
                row = cursor.fetchone()
                
                if row:
                    md5_hash, sha1_hash = row
                    
                    # Delete the file record
                    cursor.execute('DELETE FROM file_hashes WHERE file_id = ?', (file_id,))
                    
                    # Update duplicate groups
                    if md5_hash:
                        cursor.execute('SELECT COUNT(*) FROM file_hashes WHERE md5_hash = ?', (md5_hash,))
                        count = cursor.fetchone()[0]
                        if count == 0:
                            cursor.execute('DELETE FROM duplicate_groups WHERE hash_value = ? AND hash_type = ?', 
                                         (md5_hash, 'md5'))
                        else:
                            cursor.execute('''
                                UPDATE duplicate_groups 
                                SET file_count = ?, created_date = ?
                                WHERE hash_value = ? AND hash_type = ?
                            ''', (count, datetime.now(), md5_hash, 'md5'))
                    
                    if sha1_hash:
                        cursor.execute('SELECT COUNT(*) FROM file_hashes WHERE sha1_hash = ?', (sha1_hash,))
                        count = cursor.fetchone()[0]
                        if count == 0:
                            cursor.execute('DELETE FROM duplicate_groups WHERE hash_value = ? AND hash_type = ?', 
                                         (sha1_hash, 'sha1'))
                        else:
                            cursor.execute('''
                                UPDATE duplicate_groups 
                                SET file_count = ?, created_date = ?
                                WHERE hash_value = ? AND hash_type = ?
                            ''', (count, datetime.now(), sha1_hash, 'sha1'))
                    
                    conn.commit()
                    return True
                
                return False
        except Exception as e:
            LOGGER.error(f"Failed to remove file hash: {e}")
            return False
    
    def get_database_stats(self) -> Dict:
        """Get database statistics"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Total files
                cursor.execute('SELECT COUNT(*) FROM file_hashes')
                total_files = cursor.fetchone()[0]
                
                # Total size
                cursor.execute('SELECT SUM(file_size) FROM file_hashes')
                total_size = cursor.fetchone()[0] or 0
                
                # Duplicate groups
                cursor.execute('SELECT COUNT(*) FROM duplicate_groups WHERE file_count > 1')
                duplicate_groups = cursor.fetchone()[0]
                
                # Files in duplicate groups
                cursor.execute('''
                    SELECT SUM(file_count) FROM duplicate_groups WHERE file_count > 1
                ''')
                duplicate_files = cursor.fetchone()[0] or 0
                
                # Wasted space (duplicate files beyond the first)
                cursor.execute('''
                    SELECT SUM((file_count - 1) * (total_size / file_count))
                    FROM duplicate_groups WHERE file_count > 1
                ''')
                wasted_space = cursor.fetchone()[0] or 0
                
                return {
                    'total_files': total_files,
                    'total_size': total_size,
                    'duplicate_groups': duplicate_groups,
                    'duplicate_files': duplicate_files,
                    'wasted_space': int(wasted_space),
                    'efficiency': (1 - (wasted_space / total_size)) * 100 if total_size > 0 else 100
                }
        except Exception as e:
            LOGGER.error(f"Failed to get database stats: {e}")
            return {}
    
    def cleanup_orphaned_records(self) -> int:
        """Clean up orphaned records and rebuild duplicate groups"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Clear duplicate groups table
                cursor.execute('DELETE FROM duplicate_groups')
                
                # Rebuild duplicate groups from file_hashes
                cursor.execute('''
                    INSERT INTO duplicate_groups (hash_value, hash_type, file_count, total_size, created_date)
                    SELECT md5_hash, 'md5', COUNT(*), SUM(file_size), MAX(download_date)
                    FROM file_hashes 
                    WHERE md5_hash IS NOT NULL 
                    GROUP BY md5_hash
                    HAVING COUNT(*) > 1
                ''')
                
                cursor.execute('''
                    INSERT INTO duplicate_groups (hash_value, hash_type, file_count, total_size, created_date)
                    SELECT sha1_hash, 'sha1', COUNT(*), SUM(file_size), MAX(download_date)
                    FROM file_hashes 
                    WHERE sha1_hash IS NOT NULL 
                    GROUP BY sha1_hash
                    HAVING COUNT(*) > 1
                ''')
                
                cleaned = cursor.rowcount
                conn.commit()
                
                LOGGER.info(f"Cleaned up database, rebuilt {cleaned} duplicate groups")
                return cleaned
                
        except Exception as e:
            LOGGER.error(f"Failed to cleanup database: {e}")
            return 0

# Global hash database instance
hash_db = HashDatabase()