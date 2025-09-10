import os
import json
import requests
import nest_asyncio
import asyncio
import subprocess
from pymongo import MongoClient

# --- Config ---
json_file = "fixed.json"   # Input JSON
onedrive_path = "onedrive:/HindiUploads"  # OneDrive destination folder (rclone remote)
mongo_uri = "mongodb+srv://shikari:shikari@cluster0.30gdc.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
mongo_db = "hindi_uploads"
mongo_collection = "uploaded_files"
# --------------

# MongoDB client
mongo_client = MongoClient(mongo_uri)
db = mongo_client[mongo_db]
collection = db[mongo_collection]

# Load/Save JSON (pending list)
def load_json():
    with open(json_file, "r", encoding="utf-8") as f:
        return json.load(f)

def save_json(data):
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

# Upload to OneDrive using rclone & return shareable link
def upload_and_get_link(local_file, remote_dir, display_name):
    # Upload
    subprocess.run(["rclone", "copy", local_file, remote_dir], check=True)

    # Remote full path
    remote_path = f"{remote_dir}/{os.path.basename(local_file)}"

    # Get public shareable link
    result = subprocess.run(
        ["rclone", "link", remote_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    link = result.stdout.strip()
    if not link:
        link = f"FAILED_TO_GET_LINK_FOR_{display_name}"
    return link

async def main():
    data = load_json()
    total = len(data)
    print(f"🔄 Starting upload — {total} files left")

    counter = 1
    while data:
        entry = data[0]
        url = entry["url"]
        name = entry["name"]
        file_name = url.split("/")[-1]

        print(f"[{counter}/{total}] ⬇️ Downloading: {name}")
        try:
            r = requests.get(url, stream=True, timeout=60)
            with open(file_name, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            print(f"[{counter}/{total}] 📤 Uploading to OneDrive: {name}")
            link = upload_and_get_link(file_name, onedrive_path, name)

            # Save to MongoDB
            collection.insert_one({"name": name, "link": link})
            print(f"✅ Uploaded {name} → {link} (saved to MongoDB)")

        except Exception as e:
            print(f"❌ Failed {name}: {e}")
            continue

        finally:
            if os.path.exists(file_name):
                os.remove(file_name)

        # Remove uploaded entry & save JSON
        data.pop(0)
        save_json(data)
        print(f"[{counter}/{total}] ✅ Removed {name} from pending list")
        counter += 1

    print("🎉 All Hindi entries uploaded (or finished current batch)!")

# Patch Colab loop and run
nest_asyncio.apply()
await main()
