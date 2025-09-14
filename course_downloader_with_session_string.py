
import requests
import json
import os
from urllib.parse import quote
import subprocess
import asyncio
from pyrogram import Client
import time

class CourseDownloader:
    def __init__(self, access_token, telegram_session_string):
        self.access_token = access_token
        self.telegram_session_string = telegram_session_string

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

    def fetch_course_content(self, course_id, folder_id):
        """Fetch course content from the API"""
        url = f"{self.base_url}/v2/course/content/get?courseId={course_id}&folderId={folder_id}&storeContentEvent=false"

        try:
            print(f"📡 Fetching content from folder {folder_id}")
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"❌ Error fetching course content: {e}")
            return None

    def get_video_url(self, content_hash_id):
        """Get video streaming URL using content hash ID"""
        encoded_id = quote(content_hash_id, safe='')
        url = f"{self.video_url_endpoint}?contentId={encoded_id}"

        try:
            print(f"🎬 Getting video URL for content: {content_hash_id[:20]}...")
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            data = response.json()

            if data.get('success') and data.get('url'):
                print(f"✅ Video URL obtained successfully")
                return data['url']
        except requests.RequestException as e:
            print(f"❌ Error getting video URL: {e}")
        return None

    def download_m3u8_video(self, m3u8_url, output_filename):
        """Download video from m3u8 URL using ffmpeg"""
        try:
            # Ensure safe filename
            safe_filename = "".join(c for c in output_filename if c.isalnum() or c in (' ', '-', '_', '.')).rstrip()
            if not safe_filename.endswith('.mp4'):
                safe_filename += '.mp4'

            print(f"📥 Downloading video: {safe_filename}")

            cmd = [
                'ffmpeg', '-y',  # -y to overwrite existing files
                '-i', m3u8_url,
                '-c', 'copy',
                '-bsf:a', 'aac_adtstoasc',
                safe_filename
            ]

            result = subprocess.run(cmd, capture_output=True, text=True)

            if result.returncode == 0 and os.path.exists(safe_filename):
                print(f"✅ Successfully downloaded: {safe_filename}")
                return safe_filename
            else:
                print(f"❌ Error downloading video: {result.stderr}")
                return None
        except Exception as e:
            print(f"❌ Error in download_m3u8_video: {e}")
            return None

    def download_pdf(self, pdf_url, filename):
        """Download PDF from URL"""
        try:
            print(f"📄 Downloading PDF: {filename}")
            response = requests.get(pdf_url)
            response.raise_for_status()

            # Ensure safe filename
            safe_filename = "".join(c for c in filename if c.isalnum() or c in (' ', '-', '_', '.')).rstrip()
            if not safe_filename.endswith('.pdf'):
                safe_filename += '.pdf'

            with open(safe_filename, 'wb') as f:
                f.write(response.content)

            print(f"✅ Downloaded PDF: {safe_filename}")
            return safe_filename
        except Exception as e:
            print(f"❌ Error downloading PDF: {e}")
            return None

    async def upload_to_telegram(self, file_path, chat_id="me"):
        """Upload file to Telegram using Pyrogram with session string"""
        try:
            print(f"📤 Uploading to Telegram: {file_path}")

            # Create client using session string
            app = Client(
                name=":memory:",  # Use in-memory session
                session_string=self.telegram_session_string
            )

            async with app:
                # Get file size
                file_size = os.path.getsize(file_path)
                file_size_mb = file_size / (1024 * 1024)

                print(f"📊 File size: {file_size_mb:.2f} MB")

                if file_path.endswith('.mp4'):
                    # Upload as video with progress
                    message = await app.send_video(
                        chat_id=chat_id,
                        video=file_path,
                        caption=f"📹 {os.path.basename(file_path)}\n🗂 Size: {file_size_mb:.2f} MB",
                        supports_streaming=True,
                        progress=self.progress_callback
                    )
                elif file_path.endswith('.pdf'):
                    # Upload as document
                    message = await app.send_document(
                        chat_id=chat_id,
                        document=file_path,
                        caption=f"📄 {os.path.basename(file_path)}\n🗂 Size: {file_size_mb:.2f} MB",
                        progress=self.progress_callback
                    )
                else:
                    # Upload as generic document
                    message = await app.send_document(
                        chat_id=chat_id,
                        document=file_path,
                        progress=self.progress_callback
                    )

                print(f"✅ Successfully uploaded to Telegram: {file_path}")

                # Optional: Delete local file after upload
                # os.remove(file_path)
                # print(f"🗑️ Deleted local file: {file_path}")

                return message

        except Exception as e:
            print(f"❌ Error uploading to Telegram: {e}")
            return None

    async def progress_callback(self, current, total):
        """Progress callback for file uploads"""
        percentage = (current / total) * 100
        if int(percentage) % 10 == 0:  # Print every 10%
            print(f"📤 Upload progress: {percentage:.1f}% ({current}/{total})")

    def process_content_recursively(self, course_id, folder_id, folder_name="Root", level=0):
        """Recursively process course content"""
        indent = "  " * level
        print(f"{indent}📁 Processing folder: {folder_name}")

        content_data = self.fetch_course_content(course_id, folder_id)
        if not content_data or content_data.get('status') != 'success':
            print(f"{indent}❌ Failed to fetch content for folder: {folder_name}")
            return []

        course_content = content_data['data']['courseContent']
        all_files = []

        for item in course_content:
            content_type = item.get('contentType')
            item_name = item.get('name', 'Unknown')

            if content_type == 2:  # Video
                print(f"{indent}🎬 Found video: {item_name}")
                content_hash_id = item.get('contentHashId')
                if content_hash_id:
                    video_url = self.get_video_url(content_hash_id)
                    if video_url:
                        downloaded_file = self.download_m3u8_video(video_url, f"{folder_name}_{item_name}" if level > 0 else item_name)
                        if downloaded_file:
                            all_files.append(downloaded_file)

            elif content_type == 3:  # Document/PDF
                print(f"{indent}📄 Found document: {item_name}")
                pdf_url = item.get('url')
                if pdf_url:
                    downloaded_file = self.download_pdf(pdf_url, f"{folder_name}_{item_name}" if level > 0 else item_name)
                    if downloaded_file:
                        all_files.append(downloaded_file)

            elif content_type == 1:  # Folder
                print(f"{indent}📁 Found subfolder: {item_name}")
                subfolder_id = item.get('id')
                if subfolder_id:
                    # Recursively process subfolder
                    subfolder_files = self.process_content_recursively(
                        course_id, subfolder_id, item_name, level + 1
                    )
                    all_files.extend(subfolder_files)

        return all_files

    async def download_and_upload_course(self, course_id, folder_id, chat_id="me"):
        """Main function to download course and upload to Telegram"""
        print("🚀 Starting course download and upload process...")
        print(f"📋 Course ID: {course_id}, Folder ID: {folder_id}")
        print(f"🎯 Target chat: {chat_id}")

        # Process all content recursively
        downloaded_files = self.process_content_recursively(course_id, folder_id)

        if not downloaded_files:
            print("❌ No files were downloaded")
            return

        print(f"✅ Downloaded {len(downloaded_files)} files")
        print("📤 Starting Telegram upload...")

        # Upload all files to Telegram
        successful_uploads = 0
        for i, file_path in enumerate(downloaded_files, 1):
            print(f"\n📤 Uploading file {i}/{len(downloaded_files)}: {file_path}")
            result = await self.upload_to_telegram(file_path, chat_id)
            if result:
                successful_uploads += 1
            time.sleep(3)  # Delay between uploads to avoid rate limiting

        print(f"\n🎉 Process completed!")
        print(f"📊 Summary: {successful_uploads}/{len(downloaded_files)} files uploaded successfully")

# Session string generator function (optional utility)
async def generate_session_string(api_id, api_hash):
    """Generate Pyrogram session string for first-time setup"""
    print("🔐 Generating Pyrogram session string...")
    print("📱 You will need to enter your phone number and verification code.")

    async with Client(":memory:", api_id=api_id, api_hash=api_hash) as app:
        session_string = await app.export_session_string()
        print(f"✅ Session string generated successfully!")
        print(f"🔑 Your session string: {session_string}")
        return session_string

# Example usage with session string:
if __name__ == "__main__":
    # Configuration - YOU NEED TO FILL THESE
    ACCESS_TOKEN = "eyJhbGciOiJIUzM4NCIsInR5cCI6IkpXVCJ9.eyJpZCI6MTMxMjk1MDMzLCJvcmdJZCI6MzI1MjUxLCJ0eXBlIjoxLCJtb2JpbGUiOiI5MTkxOTk4MjA3ODEiLCJuYW1lIjoiRmFyaGFuYSBNdWtodGFyIiwiZW1haWwiOiJ0ZXNsYWZiZ0BnbWFpbC5jb20iLCJpc0ludGVybmF0aW9uYWwiOjAsImRlZmF1bHRMYW5ndWFnZSI6IkVOIiwiY291bnRyeUNvZGUiOiJJTiIsImNvdW50cnlJU08iOiI5MSIsInRpbWV6b25lIjoiR01UKzU6MzAiLCJpc0RpeSI6dHJ1ZSwib3JnQ29kZSI6ImN4YXpjIiwiaXNEaXlTdWJhZG1pbiI6MCwiZmluZ2VycHJpbnRJZCI6IjI1ZjYxMzYyMTFjYjU2ZDQ2MjYxNTNmNTZmOGI0NDA0IiwiaWF0IjoxNzU3ODU3NDIxLCJleHAiOjE3NTg0NjIyMjF9.c7i4nnctIjO8hWfPNsSD8fgVTKcDfg9KFdPcGgAKfi-1lvHtGVrSBKm1CQIlTZty"
    TELEGRAM_SESSION_STRING = "your_session_string_here"

    COURSE_ID = "520227"
    FOLDER_ID = "25558299"
    CHAT_ID = "me"  # or specific chat/channel ID or username like "@channel_username"

    # Create downloader instance
    downloader = CourseDownloader(ACCESS_TOKEN, TELEGRAM_SESSION_STRING)

    # Run the download and upload process
    asyncio.run(downloader.download_and_upload_course(COURSE_ID, FOLDER_ID, CHAT_ID))

# Optional: Generate session string (run this once to get your session string)
# Uncomment and run this section if you need to generate a session string:
"""
async def setup_session():
    API_ID = your_api_id_here
    API_HASH = "your_api_hash_here"

    session_string = await generate_session_string(API_ID, API_HASH)
    print("Save this session string and use it in the main script!")

# Run this to generate session string:
# asyncio.run(setup_session())
"""
