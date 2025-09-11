import json
import asyncio
import os
import aiohttp
import aiofiles
from pyrogram import Client
from pymongo import MongoClient
from datetime import datetime
import logging
import math
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('uploader.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TelegramMovieUploader:
    def __init__(self, session_string, api_id, api_hash, group_id, mongodb_uri="mongodb://localhost:27017/", db_name="movie_uploader", max_file_size_mb=1900):
        self.session_string = session_string
        self.api_id = api_id
        self.api_hash = api_hash
        self.group_id = group_id
        self.max_file_size_bytes = max_file_size_mb * 1024 * 1024  # Convert MB to bytes
        
        # MongoDB setup
        self.mongo_client = MongoClient(mongodb_uri)
        self.db = self.mongo_client[db_name]
        self.collection = self.db.uploaded_movies
        
        # Pyrogram client
        self.app = Client(
            "uploader_session",
            session_string=session_string,
            api_id=api_id,
            api_hash=api_hash
        )
        
        # Create downloads directory
        self.downloads_dir = "downloads"
        os.makedirs(self.downloads_dir, exist_ok=True)
    
    def convert_gdrive_url(self, gdrive_url):
        """Convert Google Drive sharing URL to direct download URL"""
        try:
            # Extract file ID from the sharing URL
            file_id_match = re.search(r'/file/d/([a-zA-Z0-9_-]+)', gdrive_url)
            if file_id_match:
                file_id = file_id_match.group(1)
                # Convert to direct download URL
                direct_url = f"https://drive.google.com/uc?export=download&id={file_id}"
                logger.info(f"Converted Google Drive URL: {direct_url}")
                return direct_url
            else:
                logger.error(f"Could not extract file ID from URL: {gdrive_url}")
                return gdrive_url
        except Exception as e:
            logger.error(f"Error converting Google Drive URL: {str(e)}")
            return gdrive_url
    
    async def download_json_from_gdrive(self, gdrive_url, local_path="movies_data.json"):
        """Download JSON file from Google Drive"""
        try:
            direct_url = self.convert_gdrive_url(gdrive_url)
            logger.info(f"Downloading JSON from Google Drive...")
            
            async with aiohttp.ClientSession() as session:
                async with session.get(direct_url) as response:
                    if response.status == 200:
                        content = await response.text()
                        
                        # Check if we got the actual JSON or Google's download page
                        if content.strip().startswith('[') or content.strip().startswith('{'):
                            # It's JSON content
                            async with aiofiles.open(local_path, 'w', encoding='utf-8') as file:
                                await file.write(content)
                            logger.info(f"JSON file downloaded successfully: {local_path}")
                            return local_path
                        else:
                            # We might have hit Google's download confirmation page
                            # Try to extract the confirm URL
                            confirm_match = re.search(r'action="([^"]*)".*?name="confirm".*?value="([^"]*)"', content, re.DOTALL)
                            if confirm_match:
                                confirm_url = confirm_match.group(1).replace('&amp;', '&')
                                confirm_token = confirm_match.group(2)
                                
                                # Make confirmed download request
                                confirm_data = {
                                    'confirm': confirm_token,
                                    'uuid': file_id if 'file_id' in locals() else ''
                                }
                                
                                async with session.post(confirm_url, data=confirm_data) as confirm_response:
                                    if confirm_response.status == 200:
                                        json_content = await confirm_response.text()
                                        async with aiofiles.open(local_path, 'w', encoding='utf-8') as file:
                                            await file.write(json_content)
                                        logger.info(f"JSON file downloaded successfully (with confirmation): {local_path}")
                                        return local_path
                            
                            logger.error("Could not download JSON file - got HTML instead of JSON")
                            return None
                    else:
                        logger.error(f"Failed to download JSON: HTTP {response.status}")
                        return None
                        
        except Exception as e:
            logger.error(f"Error downloading JSON from Google Drive: {str(e)}")
            return None
    
    def is_already_uploaded(self, movie_id):
        """Check if movie is already uploaded to Telegram"""
        return self.collection.find_one({"movie_id": movie_id, "status": "uploaded"}) is not None
    
    def mark_as_uploaded(self, movie_id, movie_name, file_paths, message_ids=None, is_split=False, total_parts=1):
        """Mark movie as uploaded in database"""
        self.collection.update_one(
            {"movie_id": movie_id},
            {
                "$set": {
                    "movie_id": movie_id,
                    "movie_name": movie_name,
                    "file_paths": file_paths if isinstance(file_paths, list) else [file_paths],
                    "status": "uploaded",
                    "uploaded_at": datetime.now(),
                    "message_ids": message_ids if isinstance(message_ids, list) else [message_ids] if message_ids else [],
                    "is_split": is_split,
                    "total_parts": total_parts
                }
            },
            upsert=True
        )
    
    def mark_as_failed(self, movie_id, movie_name, error_message):
        """Mark movie as failed in database"""
        self.collection.update_one(
            {"movie_id": movie_id},
            {
                "$set": {
                    "movie_id": movie_id,
                    "movie_name": movie_name,
                    "status": "failed",
                    "error": error_message,
                    "failed_at": datetime.now()
                }
            },
            upsert=True
        )
    
    async def download_file(self, url, file_path, movie_name):
        """Download MP4 file from URL"""
        try:
            logger.info(f"Starting download: {movie_name}")
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        total_size = int(response.headers.get('content-length', 0))
                        downloaded = 0
                        
                        async with aiofiles.open(file_path, 'wb') as file:
                            async for chunk in response.content.iter_chunked(8192):
                                await file.write(chunk)
                                downloaded += len(chunk)
                                
                                if total_size > 0:
                                    progress = (downloaded / total_size) * 100
                                    if downloaded % (1024 * 1024 * 50) == 0:  # Log every 50MB
                                        logger.info(f"Download progress for {movie_name}: {progress:.1f}%")
                        
                        logger.info(f"Download completed: {movie_name}")
                        return True
                    else:
                        logger.error(f"Failed to download {movie_name}: HTTP {response.status}")
                        return False
                        
        except Exception as e:
            logger.error(f"Error downloading {movie_name}: {str(e)}")
            return False
    
    async def split_file(self, file_path, movie_name):
        """Split large file into smaller chunks"""
        try:
            file_size = os.path.getsize(file_path)
            
            if file_size <= self.max_file_size_bytes:
                return [file_path]  # No need to split
            
            logger.info(f"Splitting large file ({file_size / (1024*1024):.1f}MB): {movie_name}")
            
            # Calculate number of parts needed
            num_parts = math.ceil(file_size / self.max_file_size_bytes)
            part_size = math.ceil(file_size / num_parts)
            
            base_name = os.path.splitext(file_path)[0]
            split_files = []
            
            async with aiofiles.open(file_path, 'rb') as source_file:
                for part_num in range(num_parts):
                    part_filename = f"{base_name}.{part_num + 1:03d}.mp4"
                    split_files.append(part_filename)
                    
                    async with aiofiles.open(part_filename, 'wb') as part_file:
                        bytes_to_read = min(part_size, file_size - (part_num * part_size))
                        bytes_read = 0
                        
                        while bytes_read < bytes_to_read:
                            chunk_size = min(8192, bytes_to_read - bytes_read)
                            chunk = await source_file.read(chunk_size)
                            if not chunk:
                                break
                            await part_file.write(chunk)
                            bytes_read += len(chunk)
                    
                    logger.info(f"Created part {part_num + 1}/{num_parts}: {part_filename}")
            
            # Remove original file after splitting
            os.remove(file_path)
            logger.info(f"Split complete: {num_parts} parts created")
            
            return split_files
            
        except Exception as e:
            logger.error(f"Error splitting file {file_path}: {str(e)}")
            return [file_path]  # Return original file if splitting fails
    
    async def upload_to_telegram(self, file_path, caption, part_info=""):
        """Upload file to Telegram group"""
        try:
            full_caption = f"{caption}{part_info}" if part_info else caption
            logger.info(f"Uploading to Telegram: {full_caption}")
            
            message = await self.app.send_video(
                chat_id=self.group_id,
                video=file_path,
                caption=full_caption,
                supports_streaming=True
            )
            
            logger.info(f"Successfully uploaded to Telegram: {full_caption}")
            return message.id
            
        except Exception as e:
            logger.error(f"Error uploading to Telegram {caption}: {str(e)}")
            return None
    
    async def upload_files_to_telegram(self, file_paths, movie_name):
        """Upload single or multiple files to Telegram"""
        message_ids = []
        
        if len(file_paths) == 1:
            # Single file upload
            message_id = await self.upload_to_telegram(file_paths[0], movie_name)
            if message_id:
                message_ids.append(message_id)
        else:
            # Multiple parts upload
            for i, file_path in enumerate(file_paths, 1):
                part_info = f" [Part {i}/{len(file_paths)}]"
                message_id = await self.upload_to_telegram(file_path, movie_name, part_info)
                if message_id:
                    message_ids.append(message_id)
                else:
                    # If any part fails, consider the whole upload failed
                    return []
                    
                # Small delay between uploads to avoid rate limiting
                if i < len(file_paths):
                    await asyncio.sleep(3)  # Increased delay for GitHub Actions
        
        return message_ids
    
    def cleanup_files(self, file_paths):
        """Delete downloaded files after upload"""
        if isinstance(file_paths, str):
            file_paths = [file_paths]
            
        for file_path in file_paths:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    logger.info(f"Cleaned up file: {file_path}")
            except Exception as e:
                logger.error(f"Error cleaning up file {file_path}: {str(e)}")
    
    async def process_movie(self, movie_data):
        """Process a single movie: download and upload"""
        movie_id = movie_data["id"]
        movie_name = movie_data["name"]
        movie_url = movie_data["link"]
        
        # Check if already uploaded
        if self.is_already_uploaded(movie_id):
            logger.info(f"Skipping already uploaded movie: {movie_name}")
            return True
        
        # Generate file path
        safe_filename = "".join(c for c in movie_name if c.isalnum() or c in (' ', '-', '_', '.')).rstrip()
        file_path = os.path.join(self.downloads_dir, f"{movie_id}_{safe_filename}.mp4")
        
        try:
            # Download file
            if await self.download_file(movie_url, file_path, movie_name):
                # Check if file needs splitting
                file_size = os.path.getsize(file_path)
                logger.info(f"Downloaded file size: {file_size / (1024*1024):.1f}MB")
                
                # Split file if necessary
                files_to_upload = await self.split_file(file_path, movie_name)
                
                # Upload to Telegram
                message_ids = await self.upload_files_to_telegram(files_to_upload, movie_name)
                
                if message_ids:
                    # Mark as uploaded in database
                    is_split = len(files_to_upload) > 1
                    self.mark_as_uploaded(
                        movie_id, 
                        movie_name, 
                        files_to_upload, 
                        message_ids, 
                        is_split, 
                        len(files_to_upload)
                    )
                    logger.info(f"Successfully processed: {movie_name} ({'split into ' + str(len(files_to_upload)) + ' parts' if is_split else 'single file'})")
                    
                    # Cleanup downloaded files
                    self.cleanup_files(files_to_upload)
                    return True
                else:
                    self.mark_as_failed(movie_id, movie_name, "Failed to upload to Telegram")
                    self.cleanup_files(files_to_upload)
                    return False
            else:
                self.mark_as_failed(movie_id, movie_name, "Failed to download")
                return False
                
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            self.mark_as_failed(movie_id, movie_name, error_msg)
            
            # Cleanup any remaining files
            if os.path.exists(file_path):
                self.cleanup_files(file_path)
            
            logger.error(f"Error processing {movie_name}: {error_msg}")
            return False
    
    async def process_all_movies(self, json_source, max_concurrent=2):
        """Process all movies from JSON file or Google Drive URL"""
        try:
            local_json_path = None
            
            # Check if json_source is a Google Drive URL or local file path
            if json_source.startswith('https://drive.google.com'):
                # Download from Google Drive
                local_json_path = await self.download_json_from_gdrive(json_source)
                if not local_json_path:
                    logger.error("Failed to download JSON from Google Drive")
                    return
                json_file_path = local_json_path
            else:
                # Use as local file path
                json_file_path = json_source
            
            # Load JSON data
            with open(json_file_path, 'r', encoding='utf-8') as file:
                movies_data = json.load(file)
            
            logger.info(f"Loaded {len(movies_data)} movies from {json_file_path}")
            
            # Start Pyrogram client
            await self.app.start()
            logger.info("Pyrogram client started")
            
            # Process movies with limited concurrency
            semaphore = asyncio.Semaphore(max_concurrent)
            
            async def process_with_semaphore(movie_data):
                async with semaphore:
                    return await self.process_movie(movie_data)
            
            # Create tasks for all movies
            tasks = [process_with_semaphore(movie) for movie in movies_data]
            
            # Process all movies
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Count results
            successful = sum(1 for result in results if result is True)
            failed = len(results) - successful
            
            logger.info(f"Processing completed: {successful} successful, {failed} failed")
            
            # Clean up downloaded JSON file if it was from Google Drive
            if local_json_path and os.path.exists(local_json_path):
                try:
                    os.remove(local_json_path)
                    logger.info("Cleaned up downloaded JSON file")
                except Exception as e:
                    logger.warning(f"Could not clean up JSON file: {str(e)}")
            
        except Exception as e:
            logger.error(f"Error processing movies: {str(e)}")
        
        finally:
            # Stop Pyrogram client
            await self.app.stop()
            logger.info("Pyrogram client stopped")
    
    def get_upload_stats(self):
        """Get upload statistics from database"""
        total = self.collection.count_documents({})
        uploaded = self.collection.count_documents({"status": "uploaded"})
        failed = self.collection.count_documents({"status": "failed"})
        split_files = self.collection.count_documents({"status": "uploaded", "is_split": True})
        
        return {
            "total": total,
            "uploaded": uploaded,
            "failed": failed,
            "pending": total - uploaded - failed,
            "split_files": split_files
        }
    
    def close(self):
        """Close MongoDB connection"""
        self.mongo_client.close()

async def main():
    # Get configuration from environment variables
    SESSION_STRING = os.getenv('SESSION_STRING')
    API_ID = int(os.getenv('API_ID'))
    API_HASH = os.getenv('API_HASH')
    GROUP_ID = int(os.getenv('GROUP_ID'))
    MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
    GDRIVE_JSON_URL = os.getenv('GDRIVE_JSON_URL', 'https://drive.google.com/file/d/1VB9C9l38_PvxZAWs3fUtUw9bKKEavPBu/view?usp=sharing')
    MAX_CONCURRENT = int(os.getenv('MAX_CONCURRENT', '2'))
    
    # Validate required environment variables
    if not all([SESSION_STRING, API_ID, API_HASH, GROUP_ID]):
        logger.error("Missing required environment variables")
        return
    
    logger.info(f"Starting with concurrent downloads: {MAX_CONCURRENT}")
    
    # Create uploader instance
    uploader = TelegramMovieUploader(
        session_string=SESSION_STRING,
        api_id=API_ID,
        api_hash=API_HASH,
        group_id=GROUP_ID,
        mongodb_uri=MONGODB_URI
    )
    
    try:
        # Show current stats
        stats = uploader.get_upload_stats()
        logger.info(f"Current stats - Total: {stats['total']}, Uploaded: {stats['uploaded']}, Failed: {stats['failed']}, Pending: {stats['pending']}, Split files: {stats['split_files']}")
        
        # Process all movies using Google Drive URL
        await uploader.process_all_movies(GDRIVE_JSON_URL, max_concurrent=MAX_CONCURRENT)
        
        # Show final stats
        final_stats = uploader.get_upload_stats()
        logger.info(f"Final stats - Total: {final_stats['total']}, Uploaded: {final_stats['uploaded']}, Failed: {final_stats['failed']}, Split files: {final_stats['split_files']}")
        
    finally:
        uploader.close()

if __name__ == "__main__":
    asyncio.run(main())
