
import requests
import json
import os
from urllib.parse import quote, urljoin
import subprocess
import asyncio
from pyrogram import Client
import time
import logging
import sys
import re

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

        # Video quality preference (480p, 720p, 240p)
        self.preferred_quality = os.getenv('VIDEO_QUALITY', '480p')

        # Validate required environment variables
        if not self.access_token:
            raise ValueError("CLASSPLUS_ACCESS_TOKEN environment variable is required")
        if not self.telegram_session_string:
            raise ValueError("TELEGRAM_SESSION_STRING environment variable is required")

        logger.info(f"Initialized CourseDownloader for course {self.course_id}, folder {self.folder_id}")
        logger.info(f"Preferred video quality: {self.preferred_quality}")

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

    def parse_m3u8_master_playlist(self, master_playlist_content, base_url):
        """Parse master m3u8 playlist to extract available quality streams"""
        try:
            logger.info("📋 Parsing m3u8 master playlist for available qualities...")

            lines = master_playlist_content.strip().split('\n')
            streams = []

            i = 0
            while i < len(lines):
                line = lines[i].strip()

                # Look for stream info lines
                if line.startswith('#EXT-X-STREAM-INF:'):
                    # Parse stream info
                    stream_info = {}

                    # Extract bandwidth
                    bandwidth_match = re.search(r'BANDWIDTH=(\d+)', line)
                    if bandwidth_match:
                        stream_info['bandwidth'] = int(bandwidth_match.group(1))

                    # Extract resolution
                    resolution_match = re.search(r'RESOLUTION=(\d+x\d+)', line)
                    if resolution_match:
                        stream_info['resolution'] = resolution_match.group(1)

                    # Extract frame rate if available
                    framerate_match = re.search(r'FRAME-RATE=([\d.]+)', line)
                    if framerate_match:
                        stream_info['framerate'] = float(framerate_match.group(1))

                    # Get the next line which should be the playlist URL
                    if i + 1 < len(lines):
                        playlist_url = lines[i + 1].strip()
                        if not playlist_url.startswith('#'):
                            # Make absolute URL if it's relative
                            if not playlist_url.startswith('http'):
                                stream_info['url'] = urljoin(base_url, playlist_url)
                            else:
                                stream_info['url'] = playlist_url

                            # Determine quality from resolution or URL
                            quality = self.determine_quality_from_stream(stream_info, playlist_url)
                            stream_info['quality'] = quality

                            streams.append(stream_info)
                            logger.info(f"   📺 Found stream: {quality} ({stream_info.get('resolution', 'Unknown')})")

                i += 1

            return streams

        except Exception as e:
            logger.error(f"❌ Error parsing m3u8 playlist: {e}")
            return []

    def determine_quality_from_stream(self, stream_info, playlist_url):
        """Determine video quality from stream info or URL"""
        # First try to determine from resolution
        if 'resolution' in stream_info:
            resolution = stream_info['resolution']
            width, height = map(int, resolution.split('x'))

            if height >= 1080:
                return '1080p'
            elif height >= 720:
                return '720p'
            elif height >= 480:
                return '480p'
            elif height >= 360:
                return '360p'
            elif height >= 240:
                return '240p'

        # Try to determine from URL path
        if '720/' in playlist_url or '720p' in playlist_url:
            return '720p'
        elif '480/' in playlist_url or '480p' in playlist_url:
            return '480p'
        elif '360/' in playlist_url or '360p' in playlist_url:
            return '360p'
        elif '240/' in playlist_url or '240p' in playlist_url:
            return '240p'
        elif '1080/' in playlist_url or '1080p' in playlist_url:
            return '1080p'

        # Fallback to bandwidth-based estimation
        if 'bandwidth' in stream_info:
            bandwidth = stream_info['bandwidth']
            if bandwidth > 1500000:  # > 1.5 Mbps
                return '720p+'
            elif bandwidth > 600000:  # > 600 kbps
                return '480p'
            elif bandwidth > 300000:  # > 300 kbps
                return '360p'
            else:
                return '240p'

        return 'unknown'

    def select_best_quality_stream(self, streams, preferred_quality='480p'):
        """Select the best available stream based on preferred quality"""
        if not streams:
            return None

        logger.info(f"🎯 Looking for {preferred_quality} quality stream...")

        # Quality priority order (fallback sequence)
        quality_priority = {
            '480p': ['480p', '360p', '720p', '240p'],
            '720p': ['720p', '480p', '1080p', '360p', '240p'],
            '360p': ['360p', '480p', '240p', '720p'],
            '240p': ['240p', '360p', '480p'],
        }

        priority_list = quality_priority.get(preferred_quality, ['480p', '720p', '360p', '240p'])

        # Try to find exact match first
        for quality in priority_list:
            for stream in streams:
                if stream.get('quality') == quality:
                    logger.info(f"✅ Selected {quality} stream: {stream.get('resolution', 'Unknown resolution')}")
                    return stream

        # If no exact match, return the first available stream
        selected_stream = streams[0]
        logger.warning(f"⚠️ Preferred quality not found, using: {selected_stream.get('quality', 'unknown')}")
        return selected_stream

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

    def get_selected_quality_stream_url(self, master_m3u8_url):
        """Get the specific quality stream URL from master playlist"""
        try:
            logger.info(f"📡 Fetching master playlist from: {master_m3u8_url}")

            # Fetch master playlist
            response = requests.get(master_m3u8_url, timeout=30)
            response.raise_for_status()

            master_content = response.text
            logger.info("📋 Master playlist fetched successfully")
            logger.debug(f"Master playlist content:\n{master_content}")

            # Parse available streams
            base_url = master_m3u8_url.rsplit('/', 1)[0] + '/'
            streams = self.parse_m3u8_master_playlist(master_content, base_url)

            if not streams:
                logger.warning("⚠️ No streams found in master playlist, using original URL")
                return master_m3u8_url

            # Select best quality stream
            selected_stream = self.select_best_quality_stream(streams, self.preferred_quality)

            if selected_stream and 'url' in selected_stream:
                logger.info(f"🎯 Using {selected_stream.get('quality', 'unknown')} quality stream")
                return selected_stream['url']
            else:
                logger.warning("⚠️ Could not select quality stream, using original URL")
                return master_m3u8_url

        except Exception as e:
            logger.error(f"❌ Error processing master playlist: {e}")
            logger.info("📡 Falling back to original URL")
            return master_m3u8_url

    def download_m3u8_video(self, m3u8_url, output_filename):
        """Download video from m3u8 URL using ffmpeg with quality selection"""
        try:
            # Ensure safe filename
            safe_filename = "".join(c for c in output_filename if c.isalnum() or c in (' ', '-', '_', '.')).rstrip()
            if not safe_filename.endswith('.mp4'):
                safe_filename += '.mp4'

            # Save to downloads directory
            file_path = os.path.join(self.downloads_dir, safe_filename)

            logger.info(f"📥 Processing video download: {safe_filename}")

            # Get the specific quality stream URL
            selected_stream_url = self.get_selected_quality_stream_url(m3u8_url)

            logger.info(f"📥 Downloading video with FFmpeg: {safe_filename}")
            logger.info(f"🎯 Stream URL: {selected_stream_url}")

            cmd = [
                'ffmpeg', '-y',  # -y to overwrite existing files
                '-i', selected_stream_url,
                '-c', 'copy',  # Copy streams without re-encoding
                '-bsf:a', 'aac_adtstoasc',  # Fix AAC stream for MP4
                '-loglevel', 'error',  # Reduce ffmpeg output
                '-max_muxing_queue_size', '1024',  # Handle stream synchronization
                file_path
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)  # 1 hour timeout

            if result.returncode == 0 and os.path.exists(file_path):
                file_size_mb = os.path.getsize(file_path) / (1024*1024)
                logger.info(f"✅ Successfully downloaded: {safe_filename} ({file_size_mb:.1f} MB) [{self.preferred_quality}]")
                self.stats['successful_downloads'] += 1
                return file_path
            else:
                logger.error(f"❌ FFmpeg error: {result.stderr}")
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
            API_ID = 3477714
            API_HASH = "1264d2d7d397c4635147ee25ab5808d1"

            # Create client using session string
            app = Client(
                session_string=self.telegram_session_string,
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
                caption = f"📁 {filename}\n🗂 Size: {file_size_mb:.2f} MB\n🎯 Quality: {self.preferred_quality}"

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
        try:
            if current is None or total is None or total == 0:
                return

            percentage = (current / total) * 100
            if int(percentage) % 25 == 0:  # Print every 25%
                logger.info(f"📤 Upload progress: {percentage:.1f}% ({current}/{total})")
        except Exception as e:
            pass  # Ignore progress callback errors

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
                    # Download video with quality selection
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
        logger.info("🚀 Starting QUALITY-OPTIMIZED course download and upload process...")
        logger.info("📋 Strategy: Download each file → Upload immediately → Delete local file")
        logger.info(f"🎯 Course ID: {self.course_id}, Folder ID: {self.folder_id}")
        logger.info(f"📱 Target chat: {self.chat_id}")
        logger.info(f"🎬 Video quality preference: {self.preferred_quality}")

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
            logger.info(f"   🎬 Video quality used: {self.preferred_quality}")
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
