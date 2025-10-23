import os
import time
import requests
import boto3
import threading
import json
from botocore.exceptions import ClientError
from botocore.client import Config
import sys # Import sys for exiting

# --- Configuration ---
# Read credentials and IDs securely from environment variables

# Cloudflare R2 Configuration
# --- NEW: Get Account ID and Bucket Name from environment ---
R2_ACCOUNT_ID = os.environ.get('R2_ACCOUNT_ID')
R2_BUCKET_NAME = os.environ.get('R2_BUCKET_NAME', 'movies1') # Default if not set
R2_ENDPOINT_URL = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com" if R2_ACCOUNT_ID else None
# --- END NEW ---

R2_CONFIG = {
    'name': 'Cloudflare R2',
    'config': {
        'service_name': 's3',
        'endpoint_url': R2_ENDPOINT_URL, # <-- CHANGED
        'aws_access_key_id': os.environ.get('R2_ACCESS_KEY_ID'),
        'aws_secret_access_key': os.environ.get('R2_SECRET_ACCESS_KEY'),
        'region_name': 'auto',
        'config': Config(signature_version='s3v4')
    },
    'bucket_name': R2_BUCKET_NAME, # <-- CHANGED
    'max_size_gb': 9.5
}

# ImpossibleCloud Configuration
IMPOSSIBLE_BUCKET_NAME = os.environ.get('IMPOSSIBLE_BUCKET_NAME', 'vnrbnr') # Default if not set
IMPOSSIBLE_CONFIG = {
    'name': 'ImpossibleCloud',
    'config': {
        'service_name': 's3',
        'aws_access_key_id': os.environ.get('IMPOSSIBLE_ACCESS_KEY_ID'),
        'aws_secret_access_key': os.environ.get('IMPOSSIBLE_SECRET_ACCESS_KEY'),
        'endpoint_url': 'https://eu-central-2.storage.impossibleapi.net',
        'region_name': 'eu-central-2'
    },
    'bucket_name': IMPOSSIBLE_BUCKET_NAME, # <-- CHANGED
    'max_size_gb': None
}

# Wasabi Configuration
WASABI_BUCKET_NAME = os.environ.get('WASABI_BUCKET_NAME', 'thisismybuck') # Default if not set
WASABI_CONFIG = {
    'name': 'Wasabi',
    'config': {
        'service_name': 's3',
        'aws_access_key_id': os.environ.get('WASABI_ACCESS_KEY_ID'),
        'aws_secret_access_key': os.environ.get('WASABI_SECRET_ACCESS_KEY'),
        'endpoint_url': 'https://s3.ap-northeast-1.wasabisys.com',
        'region_name': 'ap-northeast-1'
    },
    'bucket_name': WASABI_BUCKET_NAME, # <-- CHANGED
    'max_size_gb': None
}

CLOUDS = [R2_CONFIG, IMPOSSIBLE_CONFIG, WASABI_CONFIG]
DOWNLOAD_DIR = 'temp_downloads'
STATUS_DIR = 'job_status'

# --- Check if any required environment variables are missing ---
missing_vars = []
# --- NEW: Added checks for R2 Account ID and Endpoint URL ---
if not R2_ACCOUNT_ID: missing_vars.append('R2_ACCOUNT_ID')
if not R2_ENDPOINT_URL: missing_vars.append('R2_ENDPOINT_URL (derived from R2_ACCOUNT_ID)')
# --- END NEW ---
if not R2_CONFIG['config']['aws_access_key_id']: missing_vars.append('R2_ACCESS_KEY_ID')
if not R2_CONFIG['config']['aws_secret_access_key']: missing_vars.append('R2_SECRET_ACCESS_KEY')
if not IMPOSSIBLE_CONFIG['config']['aws_access_key_id']: missing_vars.append('IMPOSSIBLE_ACCESS_KEY_ID')
if not IMPOSSIBLE_CONFIG['config']['aws_secret_access_key']: missing_vars.append('IMPOSSIBLE_SECRET_ACCESS_KEY')
if not WASABI_CONFIG['config']['aws_access_key_id']: missing_vars.append('WASABI_ACCESS_KEY_ID')
if not WASABI_CONFIG['config']['aws_secret_access_key']: missing_vars.append('WASABI_SECRET_ACCESS_KEY')

# Check bucket names (optional, as they have defaults)
# if not R2_BUCKET_NAME: missing_vars.append('R2_BUCKET_NAME')
# if not IMPOSSIBLE_BUCKET_NAME: missing_vars.append('IMPOSSIBLE_BUCKET_NAME')
# if not WASABI_BUCKET_NAME: missing_vars.append('WASABI_BUCKET_NAME')


if missing_vars:
    print(f"ERROR: Missing required environment variables: {', '.join(missing_vars)}")
    sys.exit(f"FATAL: Missing environment variables: {', '.join(missing_vars)}")

# --- Boto3 Helper Functions ---

def initialize_client(config_dict):
    """Initialize S3 client."""
    # Check if keys and endpoint are present before initializing
    if (not config_dict['config']['aws_access_key_id'] or
            not config_dict['config']['aws_secret_access_key'] or
            not config_dict['config']['endpoint_url']):
        print(f"Warning: Missing credentials or endpoint for {config_dict['name']}. Skipping client initialization.")
        return None
    try:
        # Pass only the necessary keys to boto3.client
        client_config = {
            'service_name': config_dict['config']['service_name'],
            'endpoint_url': config_dict['config']['endpoint_url'],
            'aws_access_key_id': config_dict['config']['aws_access_key_id'],
            'aws_secret_access_key': config_dict['config']['aws_secret_access_key'],
            'region_name': config_dict['config']['region_name']
        }
        # Add botocore config only if present (R2 needs it)
        if 'config' in config_dict['config']:
             client_config['config'] = config_dict['config']['config']

        return boto3.client(**client_config)
    except Exception as e:
        print(f"Error initializing client for {config_dict['name']}: {e}")
        return None


def get_bucket_size(client, bucket_name):
    """Calculate total size of all files in the bucket"""
    if not client: # Handle case where client initialization failed
        print(f"Skipping get_bucket_size for {bucket_name}: client not initialized.")
        return 0
    total_size = 0
    try:
        paginator = client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name)
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    total_size += obj['Size']
        return total_size
    except ClientError as e:
        print(f"Error getting bucket size for {bucket_name}: {e}")
        # If bucket doesn't exist, treat size as 0
        if e.response['Error']['Code'] == 'NoSuchBucket':
             print(f"Bucket {bucket_name} does not exist.")
             return 0
        # Re-raise other errors if needed, or return 0
        return 0 # Fail safe

def generate_presigned_url(client, bucket_name, file_name, expiration=3600):
    """Generate presigned URL for access"""
    if not client: # Handle case where client initialization failed
        print(f"Skipping generate_presigned_url for {file_name}: client not initialized.")
        return None
    try:
        return client.generate_presigned_url('get_object', Params={'Bucket': bucket_name, 'Key': file_name}, ExpiresIn=expiration)
    except ClientError as e:
        print(f"Error generating presigned URL for {file_name} in {bucket_name}: {e}")
        return None

# --- Progress Update Functions ---

def update_job_progress(job_id, progress_data):
    """Updates job progress in a JSON file for the UI to read."""
    try:
        status_file = os.path.join(STATUS_DIR, f"{job_id}.json")
        with open(status_file, 'w') as f:
            json.dump(progress_data, f)
    except Exception as e:
        print(f"Error updating progress for {job_id}: {e}")

class ProgressTracker:
    """Updates progress for a specific cloud during upload."""
    def __init__(self, job_id, cloud_name, total_size, progress_data):
        self.job_id = job_id
        self.cloud_name = cloud_name
        self.total_size = total_size
        self.bytes_transferred = 0
        self.progress_data = progress_data
        self._lock = threading.Lock()
        self.last_time = time.time()
        self.last_bytes = 0
        self.speed_str = "0 MB/s"

    def __call__(self, new_bytes):
        with self._lock:
            self.bytes_transferred += new_bytes
            percentage = min(int((self.bytes_transferred / self.total_size) * 100), 100)

            current_time = time.time()
            if current_time - self.last_time > 1.0:
                time_diff = current_time - self.last_time
                bytes_diff = self.bytes_transferred - self.last_bytes
                speed = (bytes_diff / time_diff) if time_diff > 0 else 0
                self.speed_str = f"{speed / (1024*1024):.2f} MB/s"
                self.last_time = current_time
                self.last_bytes = self.bytes_transferred

            size_str = f"{self.bytes_transferred / (1024*1024):.2f} MB / {self.total_size / (1024*1024):.2f} MB ({self.speed_str})"

            # Update the main progress dictionary safely
            if self.cloud_name in self.progress_data.get('clouds', {}):
                self.progress_data['clouds'][self.cloud_name].update({
                    'stage': 'uploading',
                    'percentage': percentage,
                    'message': size_str
                })
            else:
                 self.progress_data.setdefault('clouds', {})[self.cloud_name] = {
                    'stage': 'uploading',
                    'percentage': percentage,
                    'message': size_str
                }
            update_job_progress(self.job_id, self.progress_data)

# --- Main Task Functions ---

def download_file_with_progress_task(job_id, url, temp_filepath, progress_data):
    """Downloads file and updates progress."""
    progress_data['download'] = {'stage': 'downloading', 'percentage': 0, 'message': 'Starting...'}
    update_job_progress(job_id, progress_data)

    try:
        # Use a longer timeout for potentially large files
        with requests.get(url, stream=True, timeout=60) as r: # Increased timeout
            r.raise_for_status()
            total_size = int(r.headers.get('content-length', 0))
            downloaded_size = 0

            last_time = time.time()
            last_bytes = 0
            speed_str = "0 MB/s"

            with open(temp_filepath, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192 * 4): # Increased chunk size
                    if not chunk: # Handle empty chunks
                         continue
                    f.write(chunk)
                    downloaded_size += len(chunk)

                    if total_size > 0:
                        percentage = min(int((downloaded_size / total_size) * 100), 100)

                        current_time = time.time()
                        # Update progress less frequently if needed, e.g., every 2 seconds
                        if current_time - last_time > 1.0:
                            time_diff = current_time - last_time
                            bytes_diff = downloaded_size - last_bytes
                            speed = (bytes_diff / time_diff) if time_diff > 0 else 0
                            speed_str = f"{speed / (1024*1024):.2f} MB/s"
                            last_time = current_time
                            last_bytes = downloaded_size

                            # Update the json file only on interval to reduce I/O
                            size_str = f"{downloaded_size / (1024*1024):.2f} MB / {total_size / (1024*1024):.2f} MB ({speed_str})"
                            progress_data['download'] = {'stage': 'downloading', 'percentage': percentage, 'message': size_str}
                            update_job_progress(job_id, progress_data)

            msg = f"Complete: {downloaded_size / (1024*1024):.2f} MB"
            progress_data['download'] = {'stage': 'completed', 'percentage': 100, 'message': msg}
            update_job_progress(job_id, progress_data)
            return temp_filepath, downloaded_size

    except Exception as e:
        msg = f"Error: {e}"
        progress_data['download'] = {'stage': 'failed', 'percentage': 0, 'message': msg}
        progress_data['status'] = 'failed'
        update_job_progress(job_id, progress_data)
        raise # Reraise the exception so the worker knows the job failed

def upload_file_to_cloud_task(job_id, cloud, temp_filepath, progress_data):
    """Uploads a single file to a specific cloud with progress"""
    cloud_name = cloud['name']
    # Ensure clouds dict exists
    progress_data.setdefault('clouds', {})[cloud_name] = {'stage': 'pending', 'percentage': 0, 'message': 'Waiting...'}
    update_job_progress(job_id, progress_data)

    try:
        client = initialize_client(cloud)
        if not client:
            msg = "Skipped: Client initialization failed (check credentials/endpoint)."
            progress_data['clouds'][cloud_name] = {'stage': 'skipped', 'percentage': 0, 'message': msg}
            update_job_progress(job_id, progress_data)
            return # Skip upload if client failed

        # Cloudflare R2 size check
        if cloud_name == R2_CONFIG['name']:
            progress_data['clouds'][cloud_name]['stage'] = 'checking'
            progress_data['clouds'][cloud_name]['message'] = 'Checking bucket size...'
            update_job_progress(job_id, progress_data)

            if not os.path.exists(temp_filepath):
                 raise FileNotFoundError(f"Temporary file not found: {temp_filepath}")
            new_file_size = os.path.getsize(temp_filepath)
            max_bytes = R2_CONFIG['max_size_gb'] * 1024 ** 3
            existing_size = get_bucket_size(client, cloud['bucket_name'])

            if existing_size + new_file_size > max_bytes:
                excess_gb = ((existing_size + new_file_size) - max_bytes) / 1024**3
                msg = f"Skipped: Would exceed {R2_CONFIG['max_size_gb']}GB limit by {excess_gb:.2f} GB."
                progress_data['clouds'][cloud_name] = {'stage': 'skipped', 'percentage': 0, 'message': msg}
                update_job_progress(job_id, progress_data)
                return # Skip this specific upload

        # Start upload
        if not os.path.exists(temp_filepath):
             raise FileNotFoundError(f"Temporary file not found before upload: {temp_filepath}")
        file_size = os.path.getsize(temp_filepath)
        if file_size == 0:
            msg = "Skipped: File size is 0 bytes."
            progress_data['clouds'][cloud_name] = {'stage': 'skipped', 'percentage': 0, 'message': msg}
            update_job_progress(job_id, progress_data)
            return

        progress_callback = ProgressTracker(job_id, cloud_name, file_size, progress_data)

        client.upload_file(
            temp_filepath,
            cloud['bucket_name'],
            progress_data['filename'],
            Callback=progress_callback
        )

        url = generate_presigned_url(client, cloud['bucket_name'], progress_data['filename'])
        msg = f"Complete. Link: {url}" if url else "Complete (Link generation failed)."
        progress_data['clouds'][cloud_name] = {'stage': 'completed', 'percentage': 100, 'message': msg}
        update_job_progress(job_id, progress_data)

    except Exception as e:
        msg = f"Upload failed: {e}"
        progress_data['clouds'][cloud_name] = {'stage': 'failed', 'percentage': 0, 'message': msg}
        update_job_progress(job_id, progress_data)
        # Don't mark the whole job failed just because one cloud failed
        # Don't re-raise, let other clouds continue
        print(f"Upload to {cloud_name} failed for job {job_id}: {e}") # Log the error
