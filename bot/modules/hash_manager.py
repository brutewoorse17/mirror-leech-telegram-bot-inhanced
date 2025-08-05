from asyncio import sleep
from pyrogram import filters
from pyrogram.handlers import MessageHandler

from ..core.config_manager import Config
from ..helper.ext_utils.bot_utils import new_task, sync_to_async
from ..helper.ext_utils.hash_utils import hash_db
from ..helper.ext_utils.status_utils import get_readable_file_size
from ..helper.telegram_helper.bot_commands import BotCommands
from ..helper.telegram_helper.filters import CustomFilters
from ..helper.telegram_helper.message_utils import send_message, edit_message, delete_message

@new_task
async def hash_stats(client, message):
    """Show hash database statistics"""
    user_id = message.from_user.id
    
    if user_id not in Config.SUDO_USERS and user_id != Config.OWNER_ID:
        await send_message(message, "❌ You don't have permission to use this command!")
        return
    
    try:
        stats = await sync_to_async(hash_db.get_database_stats)
        
        if not stats:
            await send_message(message, "❌ Failed to retrieve database statistics!")
            return
        
        msg = "📊 <b>Hash Database Statistics</b>\n\n"
        msg += f"📁 <b>Total Files:</b> {stats['total_files']:,}\n"
        msg += f"💾 <b>Total Size:</b> {get_readable_file_size(stats['total_size'])}\n"
        msg += f"🔄 <b>Duplicate Groups:</b> {stats['duplicate_groups']:,}\n"
        msg += f"📋 <b>Duplicate Files:</b> {stats['duplicate_files']:,}\n"
        msg += f"🗑️ <b>Wasted Space:</b> {get_readable_file_size(stats['wasted_space'])}\n"
        msg += f"⚡ <b>Storage Efficiency:</b> {stats['efficiency']:.1f}%\n\n"
        
        if stats['wasted_space'] > 0:
            msg += f"💡 <b>Space Savings:</b> {get_readable_file_size(stats['wasted_space'])} saved by avoiding duplicates!"
        else:
            msg += "✅ <b>No duplicates detected!</b> All files are unique."
        
        await send_message(message, msg)
        
    except Exception as e:
        await send_message(message, f"❌ Error retrieving statistics: {str(e)}")

@new_task
async def hash_duplicates(client, message):
    """Show duplicate file groups"""
    user_id = message.from_user.id
    
    if user_id not in Config.SUDO_USERS and user_id != Config.OWNER_ID:
        await send_message(message, "❌ You don't have permission to use this command!")
        return
    
    try:
        # Parse command arguments
        args = message.text.split()
        limit = 10  # Default limit
        hash_type = 'md5'  # Default hash type
        
        if len(args) > 1:
            try:
                limit = int(args[1])
                limit = min(max(limit, 1), 50)  # Limit between 1-50
            except ValueError:
                pass
        
        if len(args) > 2 and args[2].lower() in ['md5', 'sha1']:
            hash_type = args[2].lower()
        
        duplicate_groups = await sync_to_async(hash_db.get_duplicate_groups, hash_type, 2)
        
        if not duplicate_groups:
            await send_message(message, f"✅ No duplicate groups found using {hash_type.upper()} hashes!")
            return
        
        msg = f"🔄 <b>Duplicate Groups ({hash_type.upper()})</b>\n"
        msg += f"📊 Showing top {min(limit, len(duplicate_groups))} groups\n\n"
        
        for i, group in enumerate(duplicate_groups[:limit], 1):
            msg += f"<b>{i}. Hash:</b> <code>{group['hash_value'][:16]}...{group['hash_value'][-8:]}</code>\n"
            msg += f"   📁 Files: {group['file_count']}\n"
            msg += f"   💾 Total Size: {get_readable_file_size(group['total_size'])}\n"
            msg += f"   🗑️ Wasted: {get_readable_file_size(group['total_size'] - (group['total_size'] // group['file_count']))}\n\n"
        
        if len(duplicate_groups) > limit:
            msg += f"... and {len(duplicate_groups) - limit} more duplicate groups\n\n"
        
        msg += f"💡 <b>Usage:</b> <code>/{BotCommands.HashDuplicatesCommand} [limit] [hash_type]</code>\n"
        msg += f"📝 <b>Example:</b> <code>/{BotCommands.HashDuplicatesCommand} 20 sha1</code>"
        
        await send_message(message, msg)
        
    except Exception as e:
        await send_message(message, f"❌ Error retrieving duplicates: {str(e)}")

@new_task
async def hash_details(client, message):
    """Show details of files with specific hash"""
    user_id = message.from_user.id
    
    if user_id not in Config.SUDO_USERS and user_id != Config.OWNER_ID:
        await send_message(message, "❌ You don't have permission to use this command!")
        return
    
    try:
        args = message.text.split()
        if len(args) < 2:
            await send_message(message, f"❌ Usage: <code>/{BotCommands.HashDetailsCommand} [hash_value] [hash_type]</code>")
            return
        
        hash_value = args[1]
        hash_type = 'md5'
        
        if len(args) > 2 and args[2].lower() in ['md5', 'sha1']:
            hash_type = args[2].lower()
        
        files = await sync_to_async(hash_db.get_files_by_hash, hash_value, hash_type)
        
        if not files:
            await send_message(message, f"❌ No files found with {hash_type.upper()} hash: <code>{hash_value}</code>")
            return
        
        msg = f"🔍 <b>Files with {hash_type.upper()} Hash</b>\n"
        msg += f"🔐 <code>{hash_value}</code>\n\n"
        msg += f"📊 <b>Found {len(files)} file(s):</b>\n\n"
        
        for i, file in enumerate(files, 1):
            # Create Google Drive link
            drive_link = f"https://drive.google.com/uc?id={file['file_id']}&export=download"
            
            msg += f"<b>{i}. <a href='{drive_link}'>{file['file_name']}</a></b>\n"
            msg += f"   📁 ID: <code>{file['file_id']}</code>\n"
            msg += f"   💾 Size: {get_readable_file_size(file['file_size'])}\n"
            msg += f"   📅 Downloaded: {file['download_date']}\n"
            if file['file_path']:
                msg += f"   📂 Path: <code>{file['file_path']}</code>\n"
            msg += "\n"
        
        await send_message(message, msg)
        
    except Exception as e:
        await send_message(message, f"❌ Error retrieving hash details: {str(e)}")

@new_task
async def hash_cleanup(client, message):
    """Clean up hash database and rebuild duplicate groups"""
    user_id = message.from_user.id
    
    if user_id != Config.OWNER_ID:
        await send_message(message, "❌ Only the owner can use this command!")
        return
    
    try:
        sent_msg = await send_message(message, "🔄 <b>Cleaning up hash database...</b>")
        
        cleaned = await sync_to_async(hash_db.cleanup_orphaned_records)
        
        await edit_message(sent_msg, f"✅ <b>Database cleanup completed!</b>\n\n📊 Rebuilt {cleaned} duplicate groups.")
        
    except Exception as e:
        await send_message(message, f"❌ Error during cleanup: {str(e)}")

@new_task
async def hash_remove(client, message):
    """Remove specific file from hash database"""
    user_id = message.from_user.id
    
    if user_id not in Config.SUDO_USERS and user_id != Config.OWNER_ID:
        await send_message(message, "❌ You don't have permission to use this command!")
        return
    
    try:
        args = message.text.split()
        if len(args) < 2:
            await send_message(message, f"❌ Usage: <code>/{BotCommands.HashRemoveCommand} [file_id]</code>")
            return
        
        file_id = args[1]
        
        # Check if file exists
        existing_file = await sync_to_async(hash_db.check_duplicate_by_file_id, file_id)
        if not existing_file:
            await send_message(message, f"❌ File with ID <code>{file_id}</code> not found in database!")
            return
        
        # Remove the file
        success = await sync_to_async(hash_db.remove_file_hash, file_id)
        
        if success:
            msg = f"✅ <b>File removed from database!</b>\n\n"
            msg += f"📁 <b>File:</b> {existing_file['file_name']}\n"
            msg += f"🔍 <b>ID:</b> <code>{file_id}</code>\n"
            msg += f"💾 <b>Size:</b> {get_readable_file_size(existing_file['file_size'])}"
            await send_message(message, msg)
        else:
            await send_message(message, f"❌ Failed to remove file from database!")
        
    except Exception as e:
        await send_message(message, f"❌ Error removing file: {str(e)}")

@new_task
async def hash_links(client, message):
    """Get Google Drive links for duplicate files by hash"""
    user_id = message.from_user.id
    
    if user_id not in Config.SUDO_USERS and user_id != Config.OWNER_ID:
        await send_message(message, "❌ You don't have permission to use this command!")
        return
    
    try:
        args = message.text.split()
        if len(args) < 2:
            await send_message(message, f"❌ Usage: <code>/{BotCommands.HashLinksCommand} [hash_value] [hash_type]</code>")
            return
        
        hash_value = args[1]
        hash_type = 'md5'
        
        if len(args) > 2 and args[2].lower() in ['md5', 'sha1']:
            hash_type = args[2].lower()
        
        files = await sync_to_async(hash_db.get_files_by_hash, hash_value, hash_type)
        
        if not files:
            await send_message(message, f"❌ No files found with {hash_type.upper()} hash: <code>{hash_value}</code>")
            return
        
        msg = f"🔗 <b>Google Drive Links ({hash_type.upper()})</b>\n"
        msg += f"🔐 <code>{hash_value}</code>\n\n"
        
        if len(files) == 1:
            file = files[0]
            drive_link = f"https://drive.google.com/uc?id={file['file_id']}&export=download"
            msg += f"📁 <b><a href='{drive_link}'>{file['file_name']}</a></b>\n"
            msg += f"💾 Size: {get_readable_file_size(file['file_size'])}\n"
            msg += f"📅 Processed: {file['download_date']}"
        else:
            msg += f"📊 <b>Found {len(files)} duplicate files:</b>\n\n"
            
            for i, file in enumerate(files, 1):
                drive_link = f"https://drive.google.com/uc?id={file['file_id']}&export=download"
                msg += f"{i}. <b><a href='{drive_link}'>{file['file_name']}</a></b>\n"
                msg += f"   💾 {get_readable_file_size(file['file_size'])} | 📅 {file['download_date']}\n\n"
            
            msg += "💡 <b>Click any link above to download the file directly!</b>"
        
        await send_message(message, msg)
        
    except Exception as e:
        await send_message(message, f"❌ Error retrieving links: {str(e)}")

# Register handlers
hash_stats_handler = MessageHandler(hash_stats, filters=filters.command(BotCommands.HashStatsCommand) & CustomFilters.authorized)
hash_duplicates_handler = MessageHandler(hash_duplicates, filters=filters.command(BotCommands.HashDuplicatesCommand) & CustomFilters.authorized)
hash_details_handler = MessageHandler(hash_details, filters=filters.command(BotCommands.HashDetailsCommand) & CustomFilters.authorized)
hash_cleanup_handler = MessageHandler(hash_cleanup, filters=filters.command(BotCommands.HashCleanupCommand) & CustomFilters.authorized)
hash_remove_handler = MessageHandler(hash_remove, filters=filters.command(BotCommands.HashRemoveCommand) & CustomFilters.authorized)
hash_links_handler = MessageHandler(hash_links, filters=filters.command(BotCommands.HashLinksCommand) & CustomFilters.authorized)