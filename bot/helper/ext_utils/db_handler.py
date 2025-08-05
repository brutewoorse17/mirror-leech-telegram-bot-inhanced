from aiofiles import open as aiopen
from aiofiles.os import path as aiopath
from importlib import import_module
from pymongo import AsyncMongoClient
from pymongo.server_api import ServerApi
from pymongo.errors import PyMongoError

from ... import LOGGER, user_data, rss_dict, qbit_options
from ...core.mltb_client import TgClient
from ...core.config_manager import Config
from .local_db_handler import LocalDbManager
from .supabase_db_handler import SupabaseDbManager


class DbManager:
    def __init__(self):
        self._return = True
        self._conn = None
        self.db = None
        self._local_db = None
        self._supabase_db = None
        self._use_local = False
        self._use_supabase = False

    async def connect(self):
        # Priority order: Supabase -> MongoDB -> SQLite
        
        # Try Supabase first if configured
        if Config.SUPABASE_URL and Config.SUPABASE_SERVICE_KEY:
            try:
                LOGGER.info("Attempting to connect to Supabase database")
                self._supabase_db = SupabaseDbManager()
                await self._supabase_db.connect()
                if not self._supabase_db._return:
                    self.db = self._supabase_db.db
                    self._return = False
                    self._use_supabase = True
                    LOGGER.info("Successfully connected to Supabase database")
                    return
                else:
                    LOGGER.warning("Supabase connection failed, trying next option")
            except Exception as e:
                LOGGER.error(f"Error connecting to Supabase: {e}")
                LOGGER.info("Falling back to next database option")

        # Try MongoDB if DATABASE_URL is provided
        if Config.DATABASE_URL and not self._use_supabase:
            try:
                LOGGER.info("Attempting to connect to MongoDB database")
                if self._conn is not None:
                    await self._conn.close()
                self._conn = AsyncMongoClient(
                    Config.DATABASE_URL, server_api=ServerApi("1")
                )
                self.db = self._conn.mltb
                self._return = False
                LOGGER.info("Successfully connected to MongoDB database")
                return
            except PyMongoError as e:
                LOGGER.error(f"Error in MongoDB connection: {e}")
                LOGGER.info("Falling back to local SQLite database")

        # Fall back to SQLite
        LOGGER.info("Using local SQLite database as fallback")
        self._use_local = True
        self._local_db = LocalDbManager()
        await self._local_db.connect()
        self.db = self._local_db.db
        self._return = self._local_db._return
        if self._conn:
            await self._conn.close()
            self._conn = None

    async def disconnect(self):
        self._return = True
        if self._use_supabase and self._supabase_db:
            await self._supabase_db.disconnect()
        elif self._use_local and self._local_db:
            await self._local_db.disconnect()
        elif self._conn is not None:
            await self._conn.close()
        self._conn = None

    async def update_deploy_config(self):
        if self._return:
            return
        if self._use_supabase:
            await self._supabase_db.update_deploy_config()
            return
        if self._use_local:
            await self._local_db.update_deploy_config()
            return
        settings = import_module("config")
        config_file = {
            key: value.strip() if isinstance(value, str) else value
            for key, value in vars(settings).items()
            if not key.startswith("__")
        }
        await self.db.settings.deployConfig.replace_one(
            {"_id": TgClient.ID}, config_file, upsert=True
        )

    async def update_config(self, dict_):
        if self._return:
            return
        if self._use_supabase:
            await self._supabase_db.update_config(dict_)
            return
        if self._use_local:
            await self._local_db.update_config(dict_)
            return
        await self.db.settings.config.update_one(
            {"_id": TgClient.ID}, {"$set": dict_}, upsert=True
        )

    async def update_aria2(self, key, value):
        if self._return:
            return
        if self._use_supabase:
            await self._supabase_db.update_aria2(key, value)
            return
        if self._use_local:
            await self._local_db.update_aria2(key, value)
            return
        await self.db.settings.aria2c.update_one(
            {"_id": TgClient.ID}, {"$set": {key: value}}, upsert=True
        )

    async def update_qbittorrent(self, key, value):
        if self._return:
            return
        if self._use_supabase:
            await self._supabase_db.update_qbittorrent(key, value)
            return
        if self._use_local:
            await self._local_db.update_qbittorrent(key, value)
            return
        await self.db.settings.qbittorrent.update_one(
            {"_id": TgClient.ID}, {"$set": {key: value}}, upsert=True
        )

    async def save_qbit_settings(self):
        if self._return:
            return
        if self._use_supabase:
            await self._supabase_db.save_qbit_settings()
            return
        if self._use_local:
            await self._local_db.save_qbit_settings()
            return
        await self.db.settings.qbittorrent.update_one(
            {"_id": TgClient.ID}, {"$set": qbit_options}, upsert=True
        )

    async def update_private_file(self, path):
        if self._return:
            return
        if self._use_supabase:
            await self._supabase_db.update_private_file(path)
            return
        if self._use_local:
            await self._local_db.update_private_file(path)
            return
        db_path = path.replace(".", "__")
        if await aiopath.exists(path):
            async with aiopen(path, "rb+") as pf:
                pf_bin = await pf.read()
            await self.db.settings.files.update_one(
                {"_id": TgClient.ID}, {"$set": {db_path: pf_bin}}, upsert=True
            )
            if path == "config.py":
                await self.update_deploy_config()
        else:
            await self.db.settings.files.update_one(
                {"_id": TgClient.ID}, {"$unset": {db_path: ""}}, upsert=True
            )

    async def update_nzb_config(self):
        if self._return:
            return
        if self._use_supabase:
            await self._supabase_db.update_nzb_config()
            return
        if self._use_local:
            await self._local_db.update_nzb_config()
            return
        async with aiopen("sabnzbd/SABnzbd.ini", "rb+") as pf:
            nzb_conf = await pf.read()
        await self.db.settings.nzb.replace_one(
            {"_id": TgClient.ID}, {"SABnzbd__ini": nzb_conf}, upsert=True
        )

    async def update_user_data(self, user_id):
        if self._return:
            return
        if self._use_supabase:
            await self._supabase_db.update_user_data(user_id)
            return
        if self._use_local:
            await self._local_db.update_user_data(user_id)
            return
        data = user_data.get(user_id, {})
        data = data.copy()
        for key in ("THUMBNAIL", "RCLONE_CONFIG", "TOKEN_PICKLE"):
            data.pop(key, None)
        pipeline = [
            {
                "$replaceRoot": {
                    "newRoot": {
                        "$mergeObjects": [
                            data,
                            {
                                "$arrayToObject": {
                                    "$filter": {
                                        "input": {"$objectToArray": "$$ROOT"},
                                        "as": "field",
                                        "cond": {
                                            "$in": [
                                                "$$field.k",
                                                [
                                                    "THUMBNAIL",
                                                    "RCLONE_CONFIG",
                                                    "TOKEN_PICKLE",
                                                ],
                                            ]
                                        },
                                    }
                                }
                            },
                        ]
                    }
                }
            }
        ]
        await self.db.users.update_one({"_id": user_id}, pipeline, upsert=True)

    async def update_user_doc(self, user_id, key, path=""):
        if self._return:
            return
        if self._use_supabase:
            await self._supabase_db.update_user_doc(user_id, key, path)
            return
        if self._use_local:
            await self._local_db.update_user_doc(user_id, key, path)
            return
        if path:
            async with aiopen(path, "rb+") as doc:
                doc_bin = await doc.read()
            await self.db.users.update_one(
                {"_id": user_id}, {"$set": {key: doc_bin}}, upsert=True
            )
        else:
            await self.db.users.update_one(
                {"_id": user_id}, {"$unset": {key: ""}}, upsert=True
            )

    async def rss_update_all(self):
        if self._return:
            return
        if self._use_supabase:
            await self._supabase_db.rss_update_all()
            return
        if self._use_local:
            await self._local_db.rss_update_all()
            return
        for user_id in list(rss_dict.keys()):
            await self.db.rss[TgClient.ID].replace_one(
                {"_id": user_id}, rss_dict[user_id], upsert=True
            )

    async def rss_update(self, user_id):
        if self._return:
            return
        if self._use_supabase:
            await self._supabase_db.rss_update(user_id)
            return
        if self._use_local:
            await self._local_db.rss_update(user_id)
            return
        await self.db.rss[TgClient.ID].replace_one(
            {"_id": user_id}, rss_dict[user_id], upsert=True
        )

    async def rss_delete(self, user_id):
        if self._return:
            return
        if self._use_supabase:
            await self._supabase_db.rss_delete(user_id)
            return
        if self._use_local:
            await self._local_db.rss_delete(user_id)
            return
        await self.db.rss[TgClient.ID].delete_one({"_id": user_id})

    async def add_incomplete_task(self, cid, link, tag):
        if self._return:
            return
        if self._use_supabase:
            await self._supabase_db.add_incomplete_task(cid, link, tag)
            return
        if self._use_local:
            await self._local_db.add_incomplete_task(cid, link, tag)
            return
        await self.db.tasks[TgClient.ID].insert_one(
            {"_id": link, "cid": cid, "tag": tag}
        )

    async def rm_complete_task(self, link):
        if self._return:
            return
        if self._use_supabase:
            await self._supabase_db.rm_complete_task(link)
            return
        if self._use_local:
            await self._local_db.rm_complete_task(link)
            return
        await self.db.tasks[TgClient.ID].delete_one({"_id": link})

    async def get_incomplete_tasks(self):
        notifier_dict = {}
        if self._return:
            return notifier_dict
        if self._use_supabase:
            return await self._supabase_db.get_incomplete_tasks()
        if self._use_local:
            return await self._local_db.get_incomplete_tasks()
        if await self.db.tasks[TgClient.ID].find_one():
            rows = self.db.tasks[TgClient.ID].find({})
            async for row in rows:
                if row["cid"] in list(notifier_dict.keys()):
                    if row["tag"] in list(notifier_dict[row["cid"]]):
                        notifier_dict[row["cid"]][row["tag"]].append(row["_id"])
                    else:
                        notifier_dict[row["cid"]][row["tag"]] = [row["_id"]]
                else:
                    notifier_dict[row["cid"]] = {row["tag"]: [row["_id"]]}
        await self.db.tasks[TgClient.ID].drop()
        return notifier_dict

    async def trunc_table(self, name):
        if self._return:
            return
        if self._use_supabase:
            await self._supabase_db.trunc_table(name)
            return
        if self._use_local:
            await self._local_db.trunc_table(name)
            return
        await self.db[name][TgClient.ID].drop()


database = DbManager()
