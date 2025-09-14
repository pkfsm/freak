
import requests
import json
import os
from urllib.parse import quote
import subprocess
import asyncio
from pyrogram import Client
import time
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('course_downloader.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class CourseDownloaderOptimized:
    def __init__(self):
        # Get configuration from environment variables
        self.access_token = "eyJhbGciOiJIUzM4NCIsInR5cCI6IkpXVCJ9.eyJpZCI6MTMxMjk1MDMzLCJvcmdJZCI6MzI1MjUxLCJ0eXBlIjoxLCJtb2JpbGUiOiI5MTkxOTk4MjA3ODEiLCJuYW1lIjoiRmFyaGFuYSBNdWtodGFyIiwiZW1haWwiOiJ0ZXNsYWZiZ0BnbWFpbC5jb20iLCJpc0ludGVybmF0aW9uYWwiOjAsImRlZmF1bHRMYW5ndWFnZSI6IkVOIiwiY291bnRyeUNvZGUiOiJJTiIsImNvdW50cnlJU08iOiI5MSIsInRpbWV6b25lIjoiR01UKzU6MzAiLCJpc0RpeSI6dHJ1ZSwib3JnQ29kZSI6ImN4YXpjIiwiaXNEaXlTdWJhZG1pbiI6MCwiZmluZ2VycHJpbnRJZCI6IjI1ZjYxMzYyMTFjYjU2ZDQ2MjYxNTNmNTZmOGI0NDA0IiwiaWF0IjoxNzU3ODU3NDIxLCJleHAiOjE3NTg0NjIyMjF9.c7i4nnctIjO8hWfPNsSD8fgVTKcDfg9KFdPcGgAKfi-1lvHtGVrSBKm1CQIlTZty"
        self.telegram_session_string = os.getenv('SESSION_STRING')
        self.course_id = os.getenv('COURSE_ID', '520227')
        self.folder_id = os.getenv('FOLDER_ID', '25558299')
        self.chat_id = os.getenv('GROUP_ID', -1002968554908)

        # Validate required environment variables
        if not self.access_token:
            raise ValueError("CLASSPLUS_ACCESS_TOKEN environment variable is required")
        if not self.telegram_session_string:
            raise ValueError("TELEGRAM_SESSION_STRING environment variable is required")

        logger.info(f"Initialized CourseDownloader for course {self.course_id}, folder {self.folder_id}")

        self.base_url = "https://api.classplusapp.com"
        self.video_url_endpoint = "https://api.classplusapp.com/cams/uploader/video/jw-signed-url"

        self.headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en',
            'api-version': '52',
            'device-id': '69',
            'origin': 'https://web.classplusapp.com',
            'priority': 'u=1, i',
            'referer': 'https://web.classplusapp.com/',
            'region': 'IN',
            'sec-ch-ua': '"Chromium";v="140", "Not=A?Brand";v="24", "Microsoft Edge";v="140"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36 Edg/140.0.0.0',
            'x-access-token': self.access_token
        }

        # Create downloads directory
        self.downloads_dir = "downloads"
        os.makedirs(self.downloads_dir, exist_ok=True)

        # Statistics tracking
        self.stats = {
            'files_processed': 0,
            'successful_downloads': 0,
            'successful_uploads': 0,
            'failed_downloads': 0,
            'failed_uploads': 0
        }

    def fetch_course_content(self, course_id, folder_id):
        """Fetch course content from the API"""
        url = f"{self.base_url}/v2/course/content/get?courseId={course_id}&folderId={folder_id}&storeContentEvent=false"

        try:
            logger.info(f"Fetching content from folder {folder_id}")
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Error fetching course content: {e}")
            return None

    def get_video_url(self, content_hash_id):
        """Get video streaming URL using content hash ID"""
        encoded_id = quote(content_hash_id, safe='')
        url = f"{self.video_url_endpoint}?contentId={encoded_id}"

        try:
            logger.info(f"Getting video URL for content: {content_hash_id[:20]}...")
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            data = response.json()

            if data.get('success') and data.get('url'):
                logger.info("Video URL obtained successfully")
                return data['url']
        except requests.RequestException as e:
            logger.error(f"Error getting video URL: {e}")
        return None

    def download_m3u8_video(self, m3u8_url, output_filename):
        """Download video from m3u8 URL using ffmpeg"""
        try:
            # Ensure safe filename
            safe_filename = "".join(c for c in output_filename if c.isalnum() or c in (' ', '-', '_', '.')).rstrip()
            if not safe_filename.endswith('.mp4'):
                safe_filename += '.mp4'

            # Save to downloads directory
            file_path = os.path.join(self.downloads_dir, safe_filename)

            logger.info(f"📥 Downloading video: {safe_filename}")

            cmd = [
                'ffmpeg', '-y',  # -y to overwrite existing files
                '-i', m3u8_url,
                '-c', 'copy',
                '-bsf:a', 'aac_adtstoasc',
                '-loglevel', 'error',  # Reduce ffmpeg output
                file_path
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)  # 1 hour timeout

            if result.returncode == 0 and os.path.exists(file_path):
                file_size_mb = os.path.getsize(file_path) / (1024*1024)
                logger.info(f"✅ Successfully downloaded: {safe_filename} ({file_size_mb:.1f} MB)")
                self.stats['successful_downloads'] += 1
                return file_path
            else:
                logger.error(f"❌ Error downloading video: {result.stderr}")
                self.stats['failed_downloads'] += 1
                return None
        except Exception as e:
            logger.error(f"❌ Error in download_m3u8_video: {e}")
            self.stats['failed_downloads'] += 1
            return None

    def download_pdf(self, pdf_url, filename):
        """Download PDF from URL"""
        try:
            logger.info(f"📄 Downloading PDF: {filename}")
            response = requests.get(pdf_url, timeout=300)  # 5 minute timeout
            response.raise_for_status()

            # Ensure safe filename
            safe_filename = "".join(c for c in filename if c.isalnum() or c in (' ', '-', '_', '.')).rstrip()
            if not safe_filename.endswith('.pdf'):
                safe_filename += '.pdf'

            # Save to downloads directory
            file_path = os.path.join(self.downloads_dir, safe_filename)

            with open(file_path, 'wb') as f:
                f.write(response.content)

            file_size_mb = os.path.getsize(file_path) / (1024*1024)
            logger.info(f"✅ Downloaded PDF: {safe_filename} ({file_size_mb:.1f} MB)")
            self.stats['successful_downloads'] += 1
            return file_path
        except Exception as e:
            logger.error(f"❌ Error downloading PDF: {e}")
            self.stats['failed_downloads'] += 1
            return None

    async def upload_to_telegram(self, file_path, chat_id="me"):
        """Upload file to Telegram using Pyrogram with session string"""
        try:
            logger.info(f"📤 Uploading to Telegram: {os.path.basename(file_path)}")
            API_ID = int(os.getenv('API_ID'))
            API_HASH = os.getenv('API_HASH')
            # Create client using session string
            app = Client(
                session_string=TELEGRAM_SESSION_STRING,
                api_id=API_ID,
                api_hash=API_HASH,
                name="uploader"
            )

            async with app:
                # Get file size
                file_size = os.path.getsize(file_path)
                file_size_mb = file_size / (1024 * 1024)

                # Check file size limits (2GB for videos)
                if file_size > 2 * 1024 * 1024 * 1024:  # 2GB limit
                    logger.error(f"❌ File too large: {file_size_mb:.2f} MB (max 2GB)")
                    self.stats['failed_uploads'] += 1
                    return None

                filename = os.path.basename(file_path)
                caption = f"📁 {filename}\n🗂 Size: {file_size_mb:.2f} MB"

                if file_path.endswith('.mp4'):
                    # Upload as video
                    message = await app.send_video(
                        chat_id=chat_id,
                        video=file_path,
                        caption=caption,
                        supports_streaming=True,
                        progress=self.progress_callback
                    )
                elif file_path.endswith('.pdf'):
                    # Upload as document
                    message = await app.send_document(
                        chat_id=chat_id,
                        document=file_path,
                        caption=caption,
                        progress=self.progress_callback
                    )
                else:
                    # Upload as generic document
                    message = await app.send_document(
                        chat_id=chat_id,
                        document=file_path,
                        progress=self.progress_callback
                    )

                logger.info(f"✅ Successfully uploaded to Telegram: {filename}")
                self.stats['successful_uploads'] += 1

                # Delete local file immediately after successful upload to save space
                try:
                    os.remove(file_path)
                    logger.info(f"🗑️ Deleted local file: {filename}")
                except Exception as e:
                    logger.warning(f"Could not delete local file {filename}: {e}")

                return message

        except Exception as e:
            logger.error(f"❌ Error uploading to Telegram: {e}")
            self.stats['failed_uploads'] += 1
            return None

    async def progress_callback(self, current, total):
        """Progress callback for file uploads"""
        percentage = (current / total) * 100
        if int(percentage) % 25 == 0:  # Print every 25%
            logger.info(f"📤 Upload progress: {percentage:.1f}% ({current}/{total})")

    async def process_and_upload_item(self, item, folder_name="", level=0):
        """Process a single content item - download and upload immediately"""
        content_type = item.get('contentType')
        item_name = item.get('name', 'Unknown')
        indent = "  " * level

        self.stats['files_processed'] += 1

        if content_type == 2:  # Video
            logger.info(f"{indent}🎬 Processing video: {item_name}")
            content_hash_id = item.get('contentHashId')
            if content_hash_id:
                # Get video URL
                video_url = self.get_video_url(content_hash_id)
                if video_url:
                    # Download video
                    filename = f"{folder_name}_{item_name}" if folder_name else item_name
                    downloaded_file = self.download_m3u8_video(video_url, filename)
                    if downloaded_file:
                        # Upload immediately after download
                        await self.upload_to_telegram(downloaded_file, self.chat_id)
                        # Small delay to avoid rate limiting
                        await asyncio.sleep(3)

        elif content_type == 3:  # Document/PDF
            logger.info(f"{indent}📄 Processing document: {item_name}")
            pdf_url = item.get('url')
            if pdf_url:
                # Download PDF
                filename = f"{folder_name}_{item_name}" if folder_name else item_name
                downloaded_file = self.download_pdf(pdf_url, filename)
                if downloaded_file:
                    # Upload immediately after download
                    await self.upload_to_telegram(downloaded_file, self.chat_id)
                    # Small delay to avoid rate limiting
                    await asyncio.sleep(2)

    async def process_content_recursively(self, course_id, folder_id, folder_name="Root", level=0):
        """Recursively process course content - download and upload each file immediately"""
        indent = "  " * level
        logger.info(f"{indent}📁 Processing folder: {folder_name}")

        content_data = self.fetch_course_content(course_id, folder_id)
        if not content_data or content_data.get('status') != 'success':
            logger.error(f"{indent}❌ Failed to fetch content for folder: {folder_name}")
            return

        course_content = content_data['data']['courseContent']

        for item in course_content:
            content_type = item.get('contentType')

            if content_type in [2, 3]:  # Video or Document
                # Process and upload immediately
                await self.process_and_upload_item(
                    item, 
                    folder_name if level > 0 else "", 
                    level
                )

            elif content_type == 1:  # Folder
                item_name = item.get('name', 'Unknown')
                logger.info(f"{indent}📁 Found subfolder: {item_name}")
                subfolder_id = item.get('id')
                if subfolder_id:
                    # Recursively process subfolder
                    await self.process_content_recursively(
                        course_id, subfolder_id, item_name, level + 1
                    )

    async def download_and_upload_course(self):
        """Main function - process items one by one (download + upload immediately)"""
        logger.info("🚀 Starting OPTIMIZED course download and upload process...")
        logger.info("📋 Strategy: Download each file → Upload immediately → Delete local file")
        logger.info(f"🎯 Course ID: {self.course_id}, Folder ID: {self.folder_id}")
        logger.info(f"📱 Target chat: {self.chat_id}")

        start_time = time.time()

        try:
            # Process all content recursively with immediate upload
            await self.process_content_recursively(self.course_id, self.folder_id)

            # Final statistics
            end_time = time.time()
            duration = end_time - start_time

            logger.info("🎉 Process completed!")
            logger.info("📊 FINAL STATISTICS:")
            logger.info(f"   ⏱️  Total time: {duration:.1f} seconds")
            logger.info(f"   📁 Files processed: {self.stats['files_processed']}")
            logger.info(f"   ⬇️  Successful downloads: {self.stats['successful_downloads']}")
            logger.info(f"   ❌ Failed downloads: {self.stats['failed_downloads']}")
            logger.info(f"   ⬆️  Successful uploads: {self.stats['successful_uploads']}")
            logger.info(f"   ❌ Failed uploads: {self.stats['failed_uploads']}")

            success_rate = (self.stats['successful_uploads'] / max(1, self.stats['files_processed'])) * 100
            logger.info(f"   📈 Overall success rate: {success_rate:.1f}%")

        except Exception as e:
            logger.error(f"❌ Error in main process: {e}")
            raise

if __name__ == "__main__":
    try:
        # Create and run optimized downloader
        downloader = CourseDownloaderOptimized()
        asyncio.run(downloader.download_and_upload_course())
        logger.info("✅ Course downloader completed successfully")
    except Exception as e:
        logger.error(f"❌ Course downloader failed: {e}")
        sys.exit(1)
