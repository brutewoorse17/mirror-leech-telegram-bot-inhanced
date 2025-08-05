# Hash-Based Duplicate Detection System

This document describes the hash-based duplicate detection system implemented for Google Drive downloads to prevent downloading duplicate files based on their content hash rather than just filename.

## Features

### 🔍 **Pre-Download Detection**
- Checks file hashes (MD5/SHA1) before starting download
- Prevents downloading files that already exist in the database
- Shows detailed duplicate information when found
- Saves bandwidth and storage space

### 📊 **Hash Database Management**
- SQLite database storing file metadata and hashes
- Tracks MD5 and SHA1 checksums from Google Drive API
- Maintains duplicate group statistics
- Automatic cleanup and optimization

### 🛠️ **Management Commands**
- `/hashstats` - View database statistics and storage efficiency
- `/hashduplicates [limit] [hash_type]` - List duplicate file groups
- `/hashdetails [hash] [hash_type]` - Show files with specific hash
- `/hashlinks [hash] [hash_type]` - Get Google Drive links for duplicate files
- `/hashcleanup` - Clean and rebuild database (Owner only)
- `/hashremove [file_id]` - Remove specific file from database

### 🔄 **Search Integration**
- Search results show duplicate count for each file
- Visual indicators for files with existing duplicates
- Hash information included in search metadata

## How It Works

### 1. **Download Process**
```
User initiates download → Get file metadata with hashes → Check database for duplicates → 
If duplicate found: Show warning and cancel → If unique: Proceed with download → Add to database
```

### 2. **Duplicate Detection Logic**
- **File ID Check**: Exact same file already processed
- **MD5 Hash Check**: Same content with different file ID
- **SHA1 Hash Check**: Fallback if MD5 not available
- **Database Storage**: Track all processed files with metadata

### 3. **Search Enhancement**
- Retrieve hash information during search
- Display duplicate count in search results
- Prevent users from downloading known duplicates

## Database Schema

### `file_hashes` Table
- `file_id`: Google Drive file ID (primary key)
- `file_name`: Original filename
- `file_size`: File size in bytes
- `md5_hash`: MD5 checksum from Google Drive
- `sha1_hash`: SHA1 checksum from Google Drive
- `drive_id`: Source drive ID
- `mime_type`: File MIME type
- `download_date`: When file was processed
- `file_path`: Local path (if downloaded)

### `duplicate_groups` Table
- `hash_value`: The hash value
- `hash_type`: 'md5' or 'sha1'
- `file_count`: Number of files with this hash
- `total_size`: Combined size of all duplicates
- `created_date`: When group was created

## Usage Examples

### Check Database Statistics
```
/hashstats
```
**Output:**
```
📊 Hash Database Statistics

📁 Total Files: 1,234
💾 Total Size: 45.6 GB
🔄 Duplicate Groups: 23
📋 Duplicate Files: 67
🗑️ Wasted Space: 2.3 GB
⚡ Storage Efficiency: 94.9%

💡 Space Savings: 2.3 GB saved by avoiding duplicates!
```

### View Duplicate Groups
```
/hashduplicates 10 md5
```
**Output:**
```
🔄 Duplicate Groups (MD5)
📊 Showing top 10 groups

1. Hash: a1b2c3d4e5f6...12345678
   📁 Files: 3
   💾 Total Size: 1.2 GB
   🗑️ Wasted: 800 MB

2. Hash: f6e5d4c3b2a1...87654321
   📁 Files: 2
   💾 Total Size: 500 MB
   🗑️ Wasted: 250 MB
...
```

### Download Attempt with Duplicate
When trying to download a duplicate file, the bot automatically provides Google Drive links:
```
🔄 Duplicate File Found!

📁 Requested File: example_movie.mp4
💾 Size: 1.2 GB

✅ Available Duplicate: example_film.mp4
📅 Previously processed: 2024-01-15 10:30:45
🔐 MD5: a1b2c3d4e5f6...12345678

📋 All 2 duplicate(s):

1. example_film.mp4
   📅 2024-01-15 10:30:45
   📂 /downloads/movies/example_film.mp4

2. movie_copy.mp4
   📅 2024-01-10 15:22:10
   📂 /downloads/backup/movie_copy.mp4

💡 Use the link above to access your file directly!
🚫 Download cancelled to prevent duplicate storage.
```

### Get Direct Links for Duplicates
```
/hashlinks a1b2c3d4e5f6789012345678901234567890 md5
```
**Output:**
```
🔗 Google Drive Links (MD5)
🔐 a1b2c3d4e5f6789012345678901234567890

📊 Found 2 duplicate files:

1. example_film.mp4
   💾 1.2 GB | 📅 2024-01-15 10:30:45

2. movie_copy.mp4
   💾 1.2 GB | 📅 2024-01-10 15:22:10

💡 Click any link above to download the file directly!
```

## Configuration

The system is automatically enabled when the bot starts. The hash database (`file_hashes.db`) is created in the bot's root directory.

### Customization Options
- Database path can be changed in `hash_utils.py`
- Hash types can be prioritized (MD5 preferred over SHA1)
- Duplicate detection can be disabled per user/command
- Search result duplicate display can be toggled

## Performance Considerations

- **Database Indexing**: Indexes on hash columns for fast lookups
- **Memory Usage**: Efficient SQLite queries with limited result sets
- **API Calls**: Minimal additional Google Drive API requests
- **Storage**: Lightweight database with compressed hash storage

## Benefits

1. **Storage Efficiency**: Prevents downloading identical files multiple times
2. **Bandwidth Savings**: Avoids unnecessary network transfers
3. **Organization**: Better file management with duplicate awareness
4. **User Experience**: Clear feedback about duplicate files
5. **Analytics**: Detailed statistics about storage usage and efficiency

## Technical Implementation

- **Language**: Python 3.8+
- **Database**: SQLite3 with optimized schema
- **Integration**: Seamless with existing Google Drive utilities
- **Error Handling**: Graceful fallbacks if hash data unavailable
- **Logging**: Comprehensive logging for debugging and monitoring

This system significantly improves the bot's efficiency by preventing duplicate downloads while providing users with detailed information about file duplicates in their Google Drive storage.