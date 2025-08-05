from aiofiles import open as aiopen
from aiofiles.os import path as aiopath
from supabase import create_client, Client
import json
import asyncio
from functools import wraps

from ... import LOGGER, user_data, rss_dict, qbit_options
from ...core.mltb_client import TgClient
from ...core.config_manager import Config


def run_sync(func):
    """Decorator to run sync functions in async context"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, func, *args, **kwargs)
    return wrapper


class SupabaseDbManager:
    def __init__(self):
        self._return = True
        self.client: Client = None
        self.db = self
        self.bot_id = None

    async def connect(self):
        """Initialize connection to Supabase"""
        try:
            if not Config.SUPABASE_URL or not Config.SUPABASE_SERVICE_KEY:
                LOGGER.error("Supabase URL or Service Key not provided")
                return

            # Create Supabase client
            self.client = create_client(Config.SUPABASE_URL, Config.SUPABASE_SERVICE_KEY)
            self.bot_id = str(TgClient.ID)
            
            # Test connection by creating tables if they don't exist
            await self._ensure_tables()
            
            self._return = False
            LOGGER.info("Connected to Supabase database")
            
        except Exception as e:
            LOGGER.error(f"Error connecting to Supabase: {e}")
            self._return = True

    async def disconnect(self):
        """Close Supabase connection"""
        self._return = True
        self.client = None

    @run_sync
    def _ensure_tables(self):
        """Ensure all required tables exist"""
        try:
            # Create settings table for configuration
            self.client.table('mltb_settings').select('*').limit(1).execute()
        except:
            # Table doesn't exist, it will be created by first insert
            pass
        
        try:
            # Create users table
            self.client.table('mltb_users').select('*').limit(1).execute()
        except:
            pass
        
        try:
            # Create rss table
            self.client.table('mltb_rss').select('*').limit(1).execute()
        except:
            pass
            
        try:
            # Create tasks table
            self.client.table('mltb_tasks').select('*').limit(1).execute()
        except:
            pass

    @run_sync
    def _upsert_setting(self, category: str, data: dict):
        """Upsert data into settings table"""
        record = {
            'bot_id': self.bot_id,
            'category': category,
            'data': json.dumps(data)
        }
        
        # Try to update first, if no rows affected, insert
        result = self.client.table('mltb_settings').upsert(
            record, 
            on_conflict='bot_id,category'
        ).execute()
        return result

    @run_sync
    def _get_setting(self, category: str):
        """Get data from settings table"""
        result = self.client.table('mltb_settings').select('data').eq(
            'bot_id', self.bot_id
        ).eq('category', category).execute()
        
        if result.data:
            return json.loads(result.data[0]['data'])
        return {}

    async def update_deploy_config(self):
        """Update deployment configuration"""
        if self._return:
            return
        
        try:
            from importlib import import_module
            settings = import_module("config")
            config_file = {
                key: value.strip() if isinstance(value, str) else value
                for key, value in vars(settings).items()
                if not key.startswith("__")
            }
            await self._upsert_setting('deploy_config', config_file)
        except Exception as e:
            LOGGER.error(f"Error updating deploy config in Supabase: {e}")

    async def update_config(self, dict_):
        """Update configuration"""
        if self._return:
            return
        
        try:
            await self._upsert_setting('config', dict_)
        except Exception as e:
            LOGGER.error(f"Error updating config in Supabase: {e}")

    async def update_aria2(self, key, value):
        """Update aria2 configuration"""
        if self._return:
            return
        
        try:
            current_config = await self._get_setting('aria2c')
            current_config[key] = value
            await self._upsert_setting('aria2c', current_config)
        except Exception as e:
            LOGGER.error(f"Error updating aria2 config in Supabase: {e}")

    async def update_qbittorrent(self, key, value):
        """Update qBittorrent configuration"""
        if self._return:
            return
        
        try:
            current_config = await self._get_setting('qbittorrent')
            current_config[key] = value
            await self._upsert_setting('qbittorrent', current_config)
        except Exception as e:
            LOGGER.error(f"Error updating qBittorrent config in Supabase: {e}")

    async def save_qbit_settings(self):
        """Save qBittorrent settings"""
        if self._return:
            return
        
        try:
            await self._upsert_setting('qbittorrent', qbit_options)
        except Exception as e:
            LOGGER.error(f"Error saving qBittorrent settings in Supabase: {e}")

    async def update_private_file(self, path):
        """Update private file data"""
        if self._return:
            return
        
        try:
            current_files = await self._get_setting('files')
            db_path = path.replace(".", "__")
            
            if await aiopath.exists(path):
                async with aiopen(path, "rb") as pf:
                    pf_bin = await pf.read()
                    # Convert binary to base64 for JSON storage
                    import base64
                    current_files[db_path] = base64.b64encode(pf_bin).decode('utf-8')
            else:
                current_files.pop(db_path, None)
            
            await self._upsert_setting('files', current_files)
            
            if path == "config.py":
                await self.update_deploy_config()
                
        except Exception as e:
            LOGGER.error(f"Error updating private file in Supabase: {e}")

    async def update_nzb_config(self):
        """Update NZB configuration"""
        if self._return:
            return
        
        try:
            async with aiopen("sabnzbd/SABnzbd.ini", "rb") as pf:
                nzb_conf = await pf.read()
                import base64
                config_data = {"SABnzbd__ini": base64.b64encode(nzb_conf).decode('utf-8')}
                await self._upsert_setting('nzb', config_data)
        except Exception as e:
            LOGGER.error(f"Error updating NZB config in Supabase: {e}")

    @run_sync
    def _upsert_user(self, user_id: int, data: dict):
        """Upsert user data"""
        record = {
            'bot_id': self.bot_id,
            'user_id': str(user_id),
            'data': json.dumps(data)
        }
        
        result = self.client.table('mltb_users').upsert(
            record,
            on_conflict='bot_id,user_id'
        ).execute()
        return result

    async def update_user_data(self, user_id):
        """Update user data"""
        if self._return:
            return
        
        try:
            data = user_data.get(user_id, {}).copy()
            # Remove binary data that needs special handling
            for key in ("THUMBNAIL", "RCLONE_CONFIG", "TOKEN_PICKLE"):
                data.pop(key, None)
            
            await self._upsert_user(user_id, data)
        except Exception as e:
            LOGGER.error(f"Error updating user data in Supabase: {e}")

    async def update_user_doc(self, user_id, key, path=""):
        """Update user document"""
        if self._return:
            return
        
        try:
            # Get current user data
            result = await self._get_user_data(user_id)
            
            if path:
                async with aiopen(path, "rb") as doc:
                    doc_bin = await doc.read()
                    import base64
                    result[key] = base64.b64encode(doc_bin).decode('utf-8')
            else:
                result.pop(key, None)
            
            await self._upsert_user(user_id, result)
        except Exception as e:
            LOGGER.error(f"Error updating user document in Supabase: {e}")

    @run_sync
    def _get_user_data(self, user_id):
        """Get user data from database"""
        result = self.client.table('mltb_users').select('data').eq(
            'bot_id', self.bot_id
        ).eq('user_id', str(user_id)).execute()
        
        if result.data:
            return json.loads(result.data[0]['data'])
        return {}

    @run_sync
    def _upsert_rss(self, user_id: int, data: dict):
        """Upsert RSS data"""
        record = {
            'bot_id': self.bot_id,
            'user_id': str(user_id),
            'data': json.dumps(data)
        }
        
        result = self.client.table('mltb_rss').upsert(
            record,
            on_conflict='bot_id,user_id'
        ).execute()
        return result

    @run_sync
    def _delete_rss(self, user_id: int):
        """Delete RSS data"""
        result = self.client.table('mltb_rss').delete().eq(
            'bot_id', self.bot_id
        ).eq('user_id', str(user_id)).execute()
        return result

    async def rss_update_all(self):
        """Update all RSS data"""
        if self._return:
            return
        
        try:
            for user_id in list(rss_dict.keys()):
                await self._upsert_rss(user_id, rss_dict[user_id])
        except Exception as e:
            LOGGER.error(f"Error updating all RSS in Supabase: {e}")

    async def rss_update(self, user_id):
        """Update RSS data for specific user"""
        if self._return:
            return
        
        try:
            await self._upsert_rss(user_id, rss_dict[user_id])
        except Exception as e:
            LOGGER.error(f"Error updating RSS in Supabase: {e}")

    async def rss_delete(self, user_id):
        """Delete RSS data for specific user"""
        if self._return:
            return
        
        try:
            await self._delete_rss(user_id)
        except Exception as e:
            LOGGER.error(f"Error deleting RSS in Supabase: {e}")

    @run_sync
    def _insert_task(self, link: str, cid: int, tag: str):
        """Insert incomplete task"""
        record = {
            'bot_id': self.bot_id,
            'task_id': link,
            'cid': str(cid),
            'tag': tag
        }
        
        result = self.client.table('mltb_tasks').insert(record).execute()
        return result

    @run_sync
    def _delete_task(self, link: str):
        """Delete task"""
        result = self.client.table('mltb_tasks').delete().eq(
            'bot_id', self.bot_id
        ).eq('task_id', link).execute()
        return result

    @run_sync
    def _get_all_tasks(self):
        """Get all incomplete tasks"""
        result = self.client.table('mltb_tasks').select('*').eq(
            'bot_id', self.bot_id
        ).execute()
        return result.data

    @run_sync
    def _clear_all_tasks(self):
        """Clear all tasks"""
        result = self.client.table('mltb_tasks').delete().eq(
            'bot_id', self.bot_id
        ).execute()
        return result

    async def add_incomplete_task(self, cid, link, tag):
        """Add incomplete task"""
        if self._return:
            return
        
        try:
            await self._insert_task(link, cid, tag)
        except Exception as e:
            LOGGER.error(f"Error adding incomplete task in Supabase: {e}")

    async def rm_complete_task(self, link):
        """Remove completed task"""
        if self._return:
            return
        
        try:
            await self._delete_task(link)
        except Exception as e:
            LOGGER.error(f"Error removing complete task in Supabase: {e}")

    async def get_incomplete_tasks(self):
        """Get all incomplete tasks"""
        notifier_dict = {}
        if self._return:
            return notifier_dict
        
        try:
            tasks = await self._get_all_tasks()
            
            for task in tasks:
                cid = task['cid']
                tag = task['tag']
                task_id = task['task_id']
                
                if cid in notifier_dict:
                    if tag in notifier_dict[cid]:
                        notifier_dict[cid][tag].append(task_id)
                    else:
                        notifier_dict[cid][tag] = [task_id]
                else:
                    notifier_dict[cid] = {tag: [task_id]}
            
            # Clear all tasks after retrieving
            await self._clear_all_tasks()
            
        except Exception as e:
            LOGGER.error(f"Error getting incomplete tasks from Supabase: {e}")
        
        return notifier_dict

    async def trunc_table(self, name):
        """Truncate table (clear all data for this bot)"""
        if self._return:
            return
        
        try:
            table_map = {
                'settings': 'mltb_settings',
                'users': 'mltb_users', 
                'rss': 'mltb_rss',
                'tasks': 'mltb_tasks'
            }
            
            table_name = table_map.get(name, f'mltb_{name}')
            
            # Delete all records for this bot
            await self._delete_bot_data(table_name)
            
        except Exception as e:
            LOGGER.error(f"Error truncating table {name} in Supabase: {e}")

    @run_sync
    def _delete_bot_data(self, table_name: str):
        """Delete all data for this bot from specified table"""
        result = self.client.table(table_name).delete().eq(
            'bot_id', self.bot_id
        ).execute()
        return result