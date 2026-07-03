"""
Download Virginia campaign finance CSVs from SBE and upload to GCS.

This script scrapes the VA State Board of Elections CSV repository and
uploads files to a GCS bucket under the raw_data/ prefix.
"""

import os
import re
import time
from pathlib import Path
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from google.cloud import storage


# Configuration
BASE_URL = "https://apps.elections.virginia.gov/SBE_CSV/CF/"
BUCKET_NAME = "va-cf-local"
GCS_PREFIX = "raw_data"
LOCAL_TEMP_DIR = Path("./temp_csv_download")

# Request headers to avoid being blocked
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    "Referer": BASE_URL
}


def init_temp_dir():
    """Create temporary directory for downloads."""
    LOCAL_TEMP_DIR.mkdir(exist_ok=True)
    return LOCAL_TEMP_DIR


def fetch_directory_listing(url):
    """
    Fetch and parse the directory listing from the base URL.
    Returns a list of CSV file URLs found on the page.
    """
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")

        # Find all links that end with .csv or look like folders
        csv_files = []
        folder_links = []

        for link in soup.find_all("a", href=True):
            href = link.get("href")
            if href.endswith(".csv"):
                # Absolute URL if needed
                if href.startswith("http"):
                    csv_files.append(href)
                else:
                    csv_files.append(url.rstrip("/") + "/" + href.lstrip("/"))
            elif href and not href.endswith(".") and href != "/":
                # Potential folder (don't start with /, likely relative)
                folder_links.append(href)

        return csv_files, folder_links
    except Exception as e:
        print(f"Error fetching directory listing: {e}")
        return [], []


def download_file(url, save_path):
    """
    Download a file from url and save to save_path.
    Returns True if successful, False otherwise.
    """
    try:
        response = requests.get(url, headers=HEADERS, timeout=60)
        response.raise_for_status()

        content = response.content

        # Check if response is an error page
        if len(content) < 100:
            if any(
                marker in content.lower()
                for marker in [b"error", b"forbidden", b"<html"]
            ):
                print(f"   ❌ {Path(save_path).name} appears to be an error page, skipping")
                return False

        save_path.write_bytes(content)
        print(f"   ✅ Downloaded {Path(save_path).name} ({len(content):,} bytes)")
        return True
    except requests.exceptions.RequestException as e:
        print(f"   ❌ Failed to download {Path(save_path).name}: {e}")
        return False
    except Exception as e:
        print(f"   ❌ Unexpected error downloading {Path(save_path).name}: {e}")
        return False


def upload_to_gcs(file_path, bucket_name, gcs_prefix):
    """
    Upload a local file to GCS.
    Returns True if successful, False otherwise.
    """
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)

        # Construct the GCS path: raw_data/filename
        blob_name = f"{gcs_prefix}/{Path(file_path).name}"
        blob = bucket.blob(blob_name)

        blob.upload_from_filename(str(file_path))
        print(f"   ✅ Uploaded to gs://{bucket_name}/{blob_name}")
        return True
    except Exception as e:
        print(f"   ❌ Failed to upload {Path(file_path).name} to GCS: {e}")
        return False


def scrape_and_upload_recursive(url, bucket_name, gcs_prefix, depth=0, max_depth=3):
    """
    Recursively scrape directories and upload CSV files to GCS.
    Prevents infinite recursion with max_depth.
    """
    if depth > max_depth:
        print(f"Reached max depth ({max_depth}), stopping recursion")
        return

    indent = "  " * depth
    print(f"{indent}Scanning: {url}")

    csv_files, folder_links = fetch_directory_listing(url)

    # Download and upload CSV files at this level
    for csv_url in csv_files:
        filename = Path(csv_url).name
        print(f"{indent}Processing {filename}")

        # Add small delay to avoid overwhelming the server
        time.sleep(0.5)

        # Download locally
        temp_file = LOCAL_TEMP_DIR / filename
        if download_file(csv_url, temp_file):
            # Upload to GCS
            upload_to_gcs(temp_file, bucket_name, gcs_prefix)
            # Clean up local temp file
            temp_file.unlink()

    # Recursively scan subdirectories
    for folder in folder_links:
        if folder.startswith("http"):
            folder_url = folder
        else:
            folder_url = url.rstrip("/") + "/" + folder.lstrip("/")

        print(f"{indent}Found folder: {folder}")
        scrape_and_upload_recursive(folder_url, bucket_name, gcs_prefix, depth + 1, max_depth)


def main():
    """Main entry point."""
    print(f"Starting download from {BASE_URL}")
    print(f"Target GCS bucket: {BUCKET_NAME}/{GCS_PREFIX}")
    print(f"Started at: {datetime.now().isoformat()}\n")

    # Initialize temp directory
    init_temp_dir()

    try:
        # Start scraping and uploading
        scrape_and_upload_recursive(BASE_URL, BUCKET_NAME, GCS_PREFIX)
        print(f"\n✅ Completed at: {datetime.now().isoformat()}")
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        raise
    finally:
        # Clean up temp directory
        import shutil
        if LOCAL_TEMP_DIR.exists():
            shutil.rmtree(LOCAL_TEMP_DIR)
            print(f"Cleaned up temporary directory: {LOCAL_TEMP_DIR}")


if __name__ == "__main__":
    main()
