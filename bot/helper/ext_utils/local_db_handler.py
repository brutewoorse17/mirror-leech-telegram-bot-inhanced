import sqlite3
import json
import os
from aiofiles import open as aiopen
from aiofiles.os import path as aiopath
from typing import Dict, Any, Optional, List
from logging import getLogger
from datetime import datetime

LOGGER = getLogger(__name__)

class LocalDbManager:
    """Local SQLite database manager that mirrors MongoDB functionality"""
    
    def __init__(self, db_path: str = "local_mltb.db"):
        self.db_path = db_path
        self._return = True
        self.db = None
        
    async def connect(self):
        """Initialize local SQLite database"""
        try:
            # Create database directory if it doesn't exist
            db_dir = os.path.dirname(self.db_path)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir)
                
            # Initialize database tables
            self._init_tables()
            self.db = self.db_path  # Set db to indicate successful connection
            self._return = False
            LOGGER.info(f"Connected to local SQLite database: {self.db_path}")
        except Exception as e:
            LOGGER.error(f"Error connecting to local database: {e}")
            self.db = None
            self._return = True
            
    def _init_tables(self):
        """Initialize all required tables"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Settings tables
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS deploy_config (
                    bot_id TEXT PRIMARY KEY,
                    config_data TEXT
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS config (
                    bot_id TEXT PRIMARY KEY,
                    config_data TEXT
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS aria2c (
                    bot_id TEXT PRIMARY KEY,
                    config_data TEXT
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS qbittorrent (
                    bot_id TEXT PRIMARY KEY,
                    config_data TEXT
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS files (
                    bot_id TEXT,
                    file_path TEXT,
                    file_data BLOB,
                    PRIMARY KEY (bot_id, file_path)
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS nzb (
                    bot_id TEXT PRIMARY KEY,
                    config_data BLOB
                )
            ''')
            
            # User data table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    user_data TEXT,
                    thumbnail BLOB,
                    rclone_config BLOB,
                    token_pickle BLOB
                )
            ''')
            
            # RSS table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS rss (
                    bot_id TEXT,
                    user_id INTEGER,
                    rss_data TEXT,
                    PRIMARY KEY (bot_id, user_id)
                )
            ''')
            
            # Tasks table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS tasks (
                    bot_id TEXT,
                    link TEXT,
                    cid INTEGER,
                    tag TEXT,
                    PRIMARY KEY (bot_id, link)
                )
            ''')
            
            conn.commit()
            
    async def disconnect(self):
        """Disconnect from local database"""
        self._return = True
        self.db = None
        
    async def update_deploy_config(self):
        """Update deployment configuration"""
        if self._return:
            return
            
        try:
            from importlib import import_module
            from ...core.mltb_client import TgClient
            
            settings = import_module("config")
            config_file = {
                key: value.strip() if isinstance(value, str) else value
                for key, value in vars(settings).items()
                if not key.startswith("__")
            }
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR REPLACE INTO deploy_config (bot_id, config_data) VALUES (?, ?)",
                    (str(TgClient.ID), json.dumps(config_file))
                )
                conn.commit()
        except Exception as e:
            LOGGER.error(f"Error updating deploy config: {e}")
            
    async def update_config(self, dict_):
        """Update configuration"""
        if self._return:
            return
            
        try:
            from ...core.mltb_client import TgClient
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                # Get existing config
                cursor.execute("SELECT config_data FROM config WHERE bot_id = ?", (str(TgClient.ID),))
                result = cursor.fetchone()
                
                if result:
                    existing_config = json.loads(result[0])
                    existing_config.update(dict_)
                else:
                    existing_config = dict_
                    
                cursor.execute(
                    "INSERT OR REPLACE INTO config (bot_id, config_data) VALUES (?, ?)",
                    (str(TgClient.ID), json.dumps(existing_config))
                )
                conn.commit()
        except Exception as e:
            LOGGER.error(f"Error updating config: {e}")
            
    async def update_aria2(self, key, value):
        """Update aria2 configuration"""
        if self._return:
            return
            
        try:
            from ...core.mltb_client import TgClient
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT config_data FROM aria2c WHERE bot_id = ?", (str(TgClient.ID),))
                result = cursor.fetchone()
                
                if result:
                    config = json.loads(result[0])
                else:
                    config = {}
                    
                config[key] = value
                
                cursor.execute(
                    "INSERT OR REPLACE INTO aria2c (bot_id, config_data) VALUES (?, ?)",
                    (str(TgClient.ID), json.dumps(config))
                )
                conn.commit()
        except Exception as e:
            LOGGER.error(f"Error updating aria2 config: {e}")
            
    async def update_qbittorrent(self, key, value):
        """Update qBittorrent configuration"""
        if self._return:
            return
            
        try:
            from ...core.mltb_client import TgClient
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT config_data FROM qbittorrent WHERE bot_id = ?", (str(TgClient.ID),))
                result = cursor.fetchone()
                
                if result:
                    config = json.loads(result[0])
                else:
                    config = {}
                    
                config[key] = value
                
                cursor.execute(
                    "INSERT OR REPLACE INTO qbittorrent (bot_id, config_data) VALUES (?, ?)",
                    (str(TgClient.ID), json.dumps(config))
                )
                conn.commit()
        except Exception as e:
            LOGGER.error(f"Error updating qbittorrent config: {e}")
            
    async def save_qbit_settings(self):
        """Save qBittorrent settings"""
        if self._return:
            return
            
        try:
            from ...core.mltb_client import TgClient
            from ... import qbit_options
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR REPLACE INTO qbittorrent (bot_id, config_data) VALUES (?, ?)",
                    (str(TgClient.ID), json.dumps(qbit_options))
                )
                conn.commit()
        except Exception as e:
            LOGGER.error(f"Error saving qbit settings: {e}")
            
    async def update_private_file(self, path):
        """Update private file"""
        if self._return:
            return
            
        try:
            from ...core.mltb_client import TgClient
            
            db_path = path.replace(".", "__")
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                if await aiopath.exists(path):
                    async with aiopen(path, "rb") as pf:
                        pf_bin = await pf.read()
                    cursor.execute(
                        "INSERT OR REPLACE INTO files (bot_id, file_path, file_data) VALUES (?, ?, ?)",
                        (str(TgClient.ID), db_path, pf_bin)
                    )
                else:
                    cursor.execute(
                        "DELETE FROM files WHERE bot_id = ? AND file_path = ?",
                        (str(TgClient.ID), db_path)
                    )
                    
                conn.commit()
                
            if path == "config.py":
                await self.update_deploy_config()
        except Exception as e:
            LOGGER.error(f"Error updating private file: {e}")
            
    async def update_nzb_config(self):
        """Update NZB configuration"""
        if self._return:
            return
            
        try:
            from ...core.mltb_client import TgClient
            
            async with aiopen("sabnzbd/SABnzbd.ini", "rb") as pf:
                nzb_conf = await pf.read()
                
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR REPLACE INTO nzb (bot_id, config_data) VALUES (?, ?)",
                    (str(TgClient.ID), nzb_conf)
                )
                conn.commit()
        except Exception as e:
            LOGGER.error(f"Error updating NZB config: {e}")
            
    async def update_user_data(self, user_id):
        """Update user data"""
        if self._return:
            return
            
        try:
            from ... import user_data
            
            data = user_data.get(user_id, {})
            data = data.copy()
            
            # Extract binary data
            thumbnail = data.pop("THUMBNAIL", None)
            rclone_config = data.pop("RCLONE_CONFIG", None)
            token_pickle = data.pop("TOKEN_PICKLE", None)
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR REPLACE INTO users (user_id, user_data, thumbnail, rclone_config, token_pickle) VALUES (?, ?, ?, ?, ?)",
                    (user_id, json.dumps(data), thumbnail, rclone_config, token_pickle)
                )
                conn.commit()
        except Exception as e:
            LOGGER.error(f"Error updating user data: {e}")
            
    async def update_user_doc(self, user_id, key, path=""):
        """Update user document"""
        if self._return:
            return
            
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                if path:
                    async with aiopen(path, "rb") as doc:
                        doc_bin = await doc.read()
                    cursor.execute(
                        f"UPDATE users SET {key} = ? WHERE user_id = ?",
                        (doc_bin, user_id)
                    )
                else:
                    cursor.execute(
                        f"UPDATE users SET {key} = NULL WHERE user_id = ?",
                        (user_id,)
                    )
                    
                conn.commit()
        except Exception as e:
            LOGGER.error(f"Error updating user document: {e}")
            
    async def rss_update_all(self):
        """Update all RSS data"""
        if self._return:
            return
            
        try:
            from ... import rss_dict
            from ...core.mltb_client import TgClient
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                for user_id, rss_data in rss_dict.items():
                    cursor.execute(
                        "INSERT OR REPLACE INTO rss (bot_id, user_id, rss_data) VALUES (?, ?, ?)",
                        (str(TgClient.ID), user_id, json.dumps(rss_data))
                    )
                conn.commit()
        except Exception as e:
            LOGGER.error(f"Error updating all RSS data: {e}")
            
    async def rss_update(self, user_id):
        """Update RSS data for specific user"""
        if self._return:
            return
            
        try:
            from ... import rss_dict
            from ...core.mltb_client import TgClient
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR REPLACE INTO rss (bot_id, user_id, rss_data) VALUES (?, ?, ?)",
                    (str(TgClient.ID), user_id, json.dumps(rss_dict[user_id]))
                )
                conn.commit()
        except Exception as e:
            LOGGER.error(f"Error updating RSS data: {e}")
            
    async def rss_delete(self, user_id):
        """Delete RSS data for specific user"""
        if self._return:
            return
            
        try:
            from ...core.mltb_client import TgClient
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "DELETE FROM rss WHERE bot_id = ? AND user_id = ?",
                    (str(TgClient.ID), user_id)
                )
                conn.commit()
        except Exception as e:
            LOGGER.error(f"Error deleting RSS data: {e}")
            
    async def add_incomplete_task(self, cid, link, tag):
        """Add incomplete task"""
        if self._return:
            return
            
        try:
            from ...core.mltb_client import TgClient
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR REPLACE INTO tasks (bot_id, link, cid, tag) VALUES (?, ?, ?, ?)",
                    (str(TgClient.ID), link, cid, tag)
                )
                conn.commit()
        except Exception as e:
            LOGGER.error(f"Error adding incomplete task: {e}")
            
    async def rm_complete_task(self, link):
        """Remove completed task"""
        if self._return:
            return
            
        try:
            from ...core.mltb_client import TgClient
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "DELETE FROM tasks WHERE bot_id = ? AND link = ?",
                    (str(TgClient.ID), link)
                )
                conn.commit()
        except Exception as e:
            LOGGER.error(f"Error removing completed task: {e}")
            
    async def get_incomplete_tasks(self):
        """Get all incomplete tasks"""
        notifier_dict = {}
        if self._return:
            return notifier_dict
            
        try:
            from ...core.mltb_client import TgClient
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT link, cid, tag FROM tasks WHERE bot_id = ?",
                    (str(TgClient.ID),)
                )
                
                for link, cid, tag in cursor.fetchall():
                    if cid in notifier_dict:
                        if tag in notifier_dict[cid]:
                            notifier_dict[cid][tag].append(link)
                        else:
                            notifier_dict[cid][tag] = [link]
                    else:
                        notifier_dict[cid] = {tag: [link]}
                        
                # Clear tasks after retrieving
                cursor.execute("DELETE FROM tasks WHERE bot_id = ?", (str(TgClient.ID),))
                conn.commit()
                
        except Exception as e:
            LOGGER.error(f"Error getting incomplete tasks: {e}")
            
        return notifier_dict
        
    async def trunc_table(self, name):
        """Truncate table equivalent"""
        if self._return:
            return
            
        try:
            from ...core.mltb_client import TgClient
            
            table_mapping = {
                "settings": ["deploy_config", "config", "aria2c", "qbittorrent", "files", "nzb"],
                "users": ["users"],
                "rss": ["rss"],
                "tasks": ["tasks"]
            }
            
            tables_to_clear = table_mapping.get(name, [name])
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                for table in tables_to_clear:
                    if table in ["deploy_config", "config", "aria2c", "qbittorrent", "nzb", "rss", "tasks"]:
                        cursor.execute(f"DELETE FROM {table} WHERE bot_id = ?", (str(TgClient.ID),))
                    elif table == "files":
                        cursor.execute(f"DELETE FROM {table} WHERE bot_id = ?", (str(TgClient.ID),))
                    elif table == "users":
                        cursor.execute(f"DELETE FROM {table}")
                conn.commit()
        except Exception as e:
            LOGGER.error(f"Error truncating table {name}: {e}")