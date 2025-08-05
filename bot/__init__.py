# Install uvloop for better async performance
from uvloop import install
install()

# Standard library imports
from asyncio import Lock, new_event_loop, set_event_loop
from logging import getLogger, FileHandler, StreamHandler, INFO, basicConfig, WARNING, ERROR
from os import cpu_count
from time import time

# Third-party imports
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sabnzbdapi import SabnzbdClient

# Performance: Set logging levels early to avoid unnecessary processing
_loggers_to_silence = [
    ("requests", WARNING),
    ("urllib3", WARNING), 
    ("pyrogram", ERROR),
    ("httpx", WARNING),
    ("pymongo", WARNING),
    ("aiohttp", WARNING),
]

for logger_name, level in _loggers_to_silence:
    getLogger(logger_name).setLevel(level)

# Initialize timing
bot_start_time = time()

# Create and set event loop with optimizations
bot_loop = new_event_loop()
set_event_loop(bot_loop)

# Configure logging with optimized format
basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[FileHandler("log.txt", buffering=1), StreamHandler()],
    level=INFO,
)

LOGGER = getLogger(__name__)
cpu_no = cpu_count()

# Constants
DOWNLOAD_DIR = "/usr/src/app/downloads/"

# Initialize data structures with appropriate types for better performance
intervals = {"status": {}, "qb": "", "jd": "", "nzb": "", "stopAll": False}

# Use dict.fromkeys() for better performance when initializing empty dicts
_empty_dicts = ["qb_torrents", "jd_downloads", "nzb_jobs", "user_data", 
                "aria2_options", "qbit_options", "nzb_options", "queued_dl", 
                "queued_up", "status_dict", "task_dict", "rss_dict", "auth_chats"]

for dict_name in _empty_dicts:
    globals()[dict_name] = {}

# Initialize lists and sets
excluded_extensions = ["aria2", "!qB"]
drives_names = []
drives_ids = []
index_urls = []
sudo_users = []

# Use set literals for better performance
non_queued_dl = set()
non_queued_up = set()
multi_tags = set()

# Initialize locks - group related locks together
task_dict_lock = Lock()
queue_dict_lock = Lock()

# Download client locks
qb_listener_lock = Lock()
nzb_listener_lock = Lock()
jd_listener_lock = Lock()

# Resource locks
cpu_eater_lock = Lock()
same_directory_lock = Lock()

# Initialize external clients
sabnzbd_client = SabnzbdClient(
    host="http://localhost",
    api_key="mltb",
    port="8070",
)

# Initialize scheduler with event loop
scheduler = AsyncIOScheduler(event_loop=bot_loop)
