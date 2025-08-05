from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from io import FileIO
from logging import getLogger
from os import makedirs, path as ospath
from tenacity import (
    retry,
    wait_exponential,
    stop_after_attempt,
    retry_if_exception_type,
    RetryError,
)

from ...ext_utils.bot_utils import async_to_sync
from ...ext_utils.bot_utils import SetInterval
from ...ext_utils.hash_utils import hash_db
from ...ext_utils.status_utils import get_readable_file_size
from ...mirror_leech_utils.gdrive_utils.helper import GoogleDriveHelper

LOGGER = getLogger(__name__)


class GoogleDriveDownload(GoogleDriveHelper):
    def __init__(self, listener, path):
        self.listener = listener
        self._updater = None
        self._path = path
        super().__init__()
        self.is_downloading = True

    def download(self):
        file_id = self.get_id_from_url(self.listener.link, self.listener.user_id)
        self.service = self.authorize()
        self._updater = SetInterval(self.update_interval, self.progress)
        try:
            meta = self.get_file_metadata_with_hash(file_id)
            
            # Check for hash-based duplicates before downloading
            if not meta.get("mimeType") == self.G_DRIVE_DIR_MIME_TYPE:
                duplicate_check = self._check_duplicate_before_download(file_id, meta)
                if duplicate_check:
                    return duplicate_check
            
            if meta.get("mimeType") == self.G_DRIVE_DIR_MIME_TYPE:
                self._download_folder(file_id, self._path, self.listener.name)
            else:
                makedirs(self._path, exist_ok=True)
                self._download_file(
                    file_id, self._path, self.listener.name, meta.get("mimeType")
                )
        except Exception as err:
            if isinstance(err, RetryError):
                LOGGER.info(f"Total Attempts: {err.last_attempt.attempt_number}")
                err = err.last_attempt.exception()
            err = str(err).replace(">", "").replace("<", "")
            if "downloadQuotaExceeded" in err:
                err = "Download Quota Exceeded."
            elif "File not found" in err:
                if not self.alt_auth and self.use_sa:
                    self.alt_auth = True
                    self.use_sa = False
                    LOGGER.error("File not found. Trying with token.pickle...")
                    self._updater.cancel()
                    return self.download()
                err = "File not found!"
            async_to_sync(self.listener.on_download_error, err)
            self.listener.is_cancelled = True
        finally:
            self._updater.cancel()
            if self.listener.is_cancelled:
                return
            async_to_sync(self.listener.on_download_complete)
            return

    def _download_folder(self, folder_id, path, folder_name):
        folder_name = folder_name.replace("/", "")
        if not ospath.exists(f"{path}/{folder_name}"):
            makedirs(f"{path}/{folder_name}")
        path += f"/{folder_name}"
        result = self.get_files_by_folder_id(folder_id)
        if len(result) == 0:
            return
        result = sorted(result, key=lambda k: k["name"])
        for item in result:
            file_id = item["id"]
            filename = item["name"]
            shortcut_details = item.get("shortcutDetails")
            if shortcut_details is not None:
                file_id = shortcut_details["targetId"]
                mime_type = shortcut_details["targetMimeType"]
            else:
                mime_type = item.get("mimeType")
            if mime_type == self.G_DRIVE_DIR_MIME_TYPE:
                self._download_folder(file_id, path, filename)
            elif not ospath.isfile(
                f"{path}/{filename}"
            ) and not filename.strip().lower().endswith(
                tuple(self.listener.excluded_extensions)
            ):
                # Check for duplicates before downloading individual files in folder
                if self._should_skip_file_in_folder(item):
                    LOGGER.info(f"Skipping duplicate file in folder: {filename}")
                    continue
                self._download_file(file_id, path, filename, mime_type)
            if self.listener.is_cancelled:
                break

    @retry(
        wait=wait_exponential(multiplier=2, min=3, max=6),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type(Exception),
    )
    def _download_file(self, file_id, path, filename, mime_type, export=False):
        if export:
            request = self.service.files().export_media(
                fileId=file_id, mimeType="application/pdf"
            )
        else:
            request = self.service.files().get_media(
                fileId=file_id, supportsAllDrives=True, acknowledgeAbuse=True
            )
        filename = filename.replace("/", "")
        if export:
            filename = f"{filename}.pdf"
        if len(filename.encode()) > 255:
            ext = ospath.splitext(filename)[1]
            filename = f"{filename[:245]}{ext}"

            if self.listener.name.strip().endswith(ext):
                self.listener.name = filename
        if self.listener.is_cancelled:
            return
        fh = FileIO(f"{path}/{filename}", "wb")
        downloader = MediaIoBaseDownload(fh, request, chunksize=100 * 1024 * 1024)
        done = False
        retries = 0
        while not done:
            if self.listener.is_cancelled:
                fh.close()
                break
            try:
                self.status, done = downloader.next_chunk()
            except HttpError as err:
                LOGGER.error(err)
                if err.resp.status in [500, 502, 503, 504, 429] and retries < 10:
                    retries += 1
                    continue
                if err.resp.get("content-type", "").startswith("application/json"):
                    reason = (
                        eval(err.content).get("error").get("errors")[0].get("reason")
                    )
                    if "fileNotDownloadable" in reason and "document" in mime_type:
                        return self._download_file(
                            file_id, path, filename, mime_type, True
                        )
                    if reason not in [
                        "downloadQuotaExceeded",
                        "dailyLimitExceeded",
                    ]:
                        raise err
                    if self.use_sa:
                        if self.sa_count >= self.sa_number:
                            LOGGER.info(
                                f"Reached maximum number of service accounts switching, which is {self.sa_count}"
                            )
                            raise err
                        else:
                            if self.listener.is_cancelled:
                                return
                            self.switch_service_account()
                            LOGGER.info(f"Got: {reason}, Trying Again...")
                            return self._download_file(
                                file_id, path, filename, mime_type
                            )
                    else:
                        LOGGER.error(f"Got: {reason}")
                        raise err
        
        # Add successfully downloaded file to hash database
        if not self.listener.is_cancelled:
            try:
                meta = self.get_file_metadata_with_hash(file_id)
                hash_db.add_file_hash(
                    file_id=file_id,
                    file_name=filename,
                    file_size=meta.get("size", 0),
                    md5_hash=meta.get("md5Checksum"),
                    sha1_hash=meta.get("sha1Checksum"),
                    mime_type=mime_type,
                    file_path=f"{path}/{filename}"
                )
                LOGGER.info(f"Added file to hash database: {filename}")
            except Exception as e:
                LOGGER.error(f"Failed to add file to hash database: {e}")
        
        self.file_processed_bytes = 0

    def _check_duplicate_before_download(self, file_id, meta):
        """Check if file is duplicate based on hash before starting download"""
        try:
            md5_hash = meta.get("md5Checksum")
            sha1_hash = meta.get("sha1Checksum")
            file_name = meta.get("name", "Unknown")
            file_size = int(meta.get("size", 0))
            
            # Check if this exact file ID was already processed
            existing_file = hash_db.check_duplicate_by_file_id(file_id)
            if existing_file:
                LOGGER.info(f"File already in database: {file_name}")
                existing_drive_link = self.G_DRIVE_BASE_DOWNLOAD_URL.format(file_id)
                
                msg = f"🔄 <b>File Already Processed!</b>\n\n"
                msg += f"📁 <b>File:</b> <a href='{existing_drive_link}'>{file_name}</a>\n"
                msg += f"💾 <b>Size:</b> {get_readable_file_size(file_size)}\n"
                msg += f"📅 <b>Previously processed:</b> {existing_file['download_date']}\n"
                
                if existing_file['file_path']:
                    msg += f"📂 <b>Local path:</b> <code>{existing_file['file_path']}</code>\n"
                
                msg += f"\n💡 <b>Click the link above to access your file directly!</b>\n"
                msg += "🚫 <b>Download cancelled - file already processed.</b>"
                
                async_to_sync(self.listener.on_download_error, msg)
                self.listener.is_cancelled = True
                return True
            
            # Check for hash-based duplicates
            duplicates = []
            if md5_hash:
                duplicates = hash_db.check_duplicate_by_hash(md5_hash=md5_hash)
            elif sha1_hash:
                duplicates = hash_db.check_duplicate_by_hash(sha1_hash=sha1_hash)
            
            if duplicates:
                LOGGER.info(f"Hash-based duplicate found for: {file_name}")
                
                # Get the most recent duplicate for the link
                most_recent_dup = max(duplicates, key=lambda x: x['download_date'])
                duplicate_drive_link = self.G_DRIVE_BASE_DOWNLOAD_URL.format(most_recent_dup['file_id'])
                
                msg = f"🔄 <b>Duplicate File Found!</b>\n\n"
                msg += f"📁 <b>Requested File:</b> {file_name}\n"
                msg += f"💾 <b>Size:</b> {get_readable_file_size(file_size)}\n\n"
                
                msg += f"✅ <b>Available Duplicate:</b> <a href='{duplicate_drive_link}'>{most_recent_dup['file_name']}</a>\n"
                msg += f"📅 <b>Previously processed:</b> {most_recent_dup['download_date']}\n"
                
                if md5_hash:
                    msg += f"🔐 <b>MD5:</b> <code>{md5_hash[:16]}...{md5_hash[-8:]}</code>\n"
                if sha1_hash:
                    msg += f"🔐 <b>SHA1:</b> <code>{sha1_hash[:16]}...{sha1_hash[-8:]}</code>\n"
                
                if len(duplicates) > 1:
                    msg += f"\n<b>📋 All {len(duplicates)} duplicate(s):</b>\n"
                    for i, dup in enumerate(duplicates, 1):
                        dup_link = self.G_DRIVE_BASE_DOWNLOAD_URL.format(dup['file_id'])
                        msg += f"\n{i}. <a href='{dup_link}'>{dup['file_name']}</a>\n"
                        msg += f"   📅 {dup['download_date']}\n"
                        if dup['file_path']:
                            msg += f"   📂 {dup['file_path']}\n"
                
                msg += f"\n💡 <b>Use the link above to access your file directly!</b>\n"
                msg += "🚫 <b>Download cancelled to prevent duplicate storage.</b>"
                
                # Add the file to database even though we're not downloading
                hash_db.add_file_hash(
                    file_id=file_id,
                    file_name=file_name,
                    file_size=file_size,
                    md5_hash=md5_hash,
                    sha1_hash=sha1_hash,
                    mime_type=meta.get("mimeType"),
                    file_path=None  # Not downloaded
                )
                
                async_to_sync(self.listener.on_download_error, msg)
                self.listener.is_cancelled = True
                return True
            
            return False
            
        except Exception as e:
            LOGGER.error(f"Error checking duplicates: {e}")
            return False

    def _should_skip_file_in_folder(self, item):
        """Check if file in folder should be skipped due to duplication"""
        try:
            file_id = item["id"]
            md5_hash = item.get("md5Checksum")
            sha1_hash = item.get("sha1Checksum")
            
            # Check if file ID already exists
            if hash_db.check_duplicate_by_file_id(file_id):
                return True
            
            # Check hash-based duplicates
            if md5_hash and hash_db.check_duplicate_by_hash(md5_hash=md5_hash):
                return True
            if sha1_hash and hash_db.check_duplicate_by_hash(sha1_hash=sha1_hash):
                return True
            
            return False
        except Exception as e:
            LOGGER.error(f"Error checking file in folder: {e}")
            return False
