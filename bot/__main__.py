from asyncio import gather
from . import LOGGER, bot_loop
from .core.mltb_client import TgClient
from .core.config_manager import Config

# Load configuration early
Config.load()


async def main():
    """Optimized main startup function with better error handling and parallelization."""
    try:
        # Import startup functions
        from .core.startup import (
            load_settings,
            load_configurations,
            save_settings,
            update_aria2_options,
            update_nzb_options,
            update_qb_options,
            update_variables,
        )

        LOGGER.info("Loading initial settings...")
        await load_settings()

        LOGGER.info("Starting Telegram clients...")
        await gather(TgClient.start_bot(), TgClient.start_user())
        
        LOGGER.info("Loading configurations and updating variables...")
        await gather(load_configurations(), update_variables())

        # Initialize torrent manager
        LOGGER.info("Initializing torrent manager...")
        from .core.torrent_manager import TorrentManager
        await TorrentManager.initiate()
        
        LOGGER.info("Updating download client options...")
        await gather(
            update_qb_options(),
            update_aria2_options(),
            update_nzb_options(),
        )

        # Import additional services
        from .helper.ext_utils.files_utils import clean_all
        from .core.jdownloader_booter import jdownloader
        from .helper.ext_utils.telegraph_helper import telegraph
        from .helper.mirror_leech_utils.rclone_utils.serve import rclone_serve_booter
        from .modules import (
            initiate_search_tools,
            get_packages_version,
            restart_notification,
        )

        LOGGER.info("Starting additional services...")
        # Group related services for better parallelization
        core_services = [
            save_settings(),
            clean_all(),
            telegraph.create_account(),
        ]
        
        optional_services = [
            jdownloader.boot(),
            initiate_search_tools(),
            get_packages_version(),
            restart_notification(),
            rclone_serve_booter(),
        ]

        # Start core services first, then optional ones
        await gather(*core_services)
        await gather(*optional_services, return_exceptions=True)  # Don't fail if optional services fail

        LOGGER.info("All services initialized successfully!")

    except Exception as e:
        LOGGER.error(f"Error during startup: {e}")
        raise


# Run main startup
bot_loop.run_until_complete(main())

# Initialize handlers and callbacks
LOGGER.info("Setting up handlers and callbacks...")
from .helper.ext_utils.bot_utils import create_help_buttons
from .helper.listeners.aria2_listener import add_aria2_callbacks
from .core.handlers import add_handlers

# Initialize in logical order
add_aria2_callbacks()
create_help_buttons()
add_handlers()

LOGGER.info("Bot Started Successfully! Ready to handle requests.")
bot_loop.run_forever()
