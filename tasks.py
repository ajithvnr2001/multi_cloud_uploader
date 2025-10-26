import os
import time
import requests
import boto3
import threading
import json
from botocore.exceptions import ClientError
from botocore.client import Config
from boto3.s3.transfer import TransferConfig # Required import
import sys
from urllib.parse import quote

# --- Configuration ---
# Read credentials and IDs securely from environment variables

# --- Cloudflare R2 Configuration ---
R2_ACCOUNT_ID = os.environ.get('R2_ACCOUNT_ID')
R2_BUCKET_NAME = os.environ.get('R2_BUCKET_NAME', 'r2-default-bucket')
R2_ENDPOINT_URL = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com" if R2_ACCOUNT_ID else None
R2_MAX_SIZE_GB = 19.5 # Defined limit

R2_CONFIG = {
    'name': 'Cloudflare R2',
    'config': {
        'service_name': 's3',
        'endpoint_url': R2_ENDPOINT_URL,
        'aws_access_key_id': os.environ.get('R2_ACCESS_KEY_ID'),
        'aws_secret_access_key': os.environ.get('R2_SECRET_ACCESS_KEY'),
        'region_name': 'auto',
        'config': Config(signature_version='s3v4') # Corrected signature version
    },
    'bucket_name': R2_BUCKET_NAME,
    'max_size_gb': R2_MAX_SIZE_GB # Reference the defined limit
}

# --- ImpossibleCloud Configuration ---
IMPOSSIBLE_BUCKET_NAME = os.environ.get('IMPOSSIBLE_BUCKET_NAME', 'impossible-default-bucket')
IMPOSSIBLE_CONFIG = {
    'name': 'ImpossibleCloud',
    'config': {
        'service_name': 's3',
        'aws_access_key_id': os.environ.get('IMPOSSIBLE_ACCESS_KEY_ID'),
        'aws_secret_access_key': os.environ.get('IMPOSSIBLE_SECRET_ACCESS_KEY'),
        'endpoint_url': 'https://eu-central-2.storage.impossibleapi.net',
        'region_name': 'eu-central-2'
    },
    'bucket_name': IMPOSSIBLE_BUCKET_NAME,
    'max_size_gb': None
}

# --- Wasabi Configuration ---
WASABI_BUCKET_NAME = os.environ.get('WASABI_BUCKET_NAME', 'wasabi-default-bucket')
WASABI_CONFIG = {
    'name': 'Wasabi',
    'config': {
        'service_name': 's3',
        'aws_access_key_id': os.environ.get('WASABI_ACCESS_KEY_ID'),
        'aws_secret_access_key': os.environ.get('WASABI_SECRET_ACCESS_KEY'),
        'endpoint_url': 'https://s3.ap-northeast-1.wasabisys.com',
        'region_name': 'ap-northeast-1'
    },
    'bucket_name': WASABI_BUCKET_NAME,
    'max_size_gb': None
}

# --- Oracle Cloud (OCI) Configuration ---
OCI_NAMESPACE = os.environ.get('OCI_NAMESPACE')
OCI_REGION = os.environ.get('OCI_REGION', 'ap-hyderabad-1')
OCI_BUCKET_NAME = os.environ.get('OCI_BUCKET_NAME', 'oci-default-bucket')
OCI_ENDPOINT_URL = f"https://{OCI_NAMESPACE}.compat.objectstorage.{OCI_REGION}.oraclecloud.com" if OCI_NAMESPACE and OCI_REGION else None

OCI_CONFIG = {
    'name': 'Oracle Cloud',
    'config': {
        'service_name': 's3',
        'aws_access_key_id': os.environ.get('OCI_ACCESS_KEY_ID'),
        'aws_secret_access_key': os.environ.get('OCI_SECRET_ACCESS_KEY'),
        'endpoint_url': OCI_ENDPOINT_URL,
        'region_name': OCI_REGION
    },
    'bucket_name': OCI_BUCKET_NAME,
    'max_size_gb': None, # We apply the limit logic externally now
    'oci_namespace': OCI_NAMESPACE,
    'oci_region': OCI_REGION
}

# Combine all cloud configs
CLOUDS = [R2_CONFIG, IMPOSSIBLE_CONFIG, WASABI_CONFIG, OCI_CONFIG]
DOWNLOAD_DIR = 'temp_downloads'
STATUS_DIR = 'job_status'

# --- Check if any required environment variables are missing ---
missing_vars = []
# R2
if not R2_ACCOUNT_ID: missing_vars.append('R2_ACCOUNT_ID')
if not R2_ENDPOINT_URL: missing_vars.append('R2_ENDPOINT_URL (derived from R2_ACCOUNT_ID)')
if not R2_CONFIG['config']['aws_access_key_id']: missing_vars.append('R2_ACCESS_KEY_ID')
if not R2_CONFIG['config']['aws_secret_access_key']: missing_vars.append('R2_SECRET_ACCESS_KEY')
# Impossible
if not IMPOSSIBLE_CONFIG['config']['aws_access_key_id']: missing_vars.append('IMPOSSIBLE_ACCESS_KEY_ID')
if not IMPOSSIBLE_CONFIG['config']['aws_secret_access_key']: missing_vars.append('IMPOSSIBLE_SECRET_ACCESS_KEY')
# Wasabi
if not WASABI_CONFIG['config']['aws_access_key_id']: missing_vars.append('WASABI_ACCESS_KEY_ID')
if not WASABI_CONFIG['config']['aws_secret_access_key']: missing_vars.append('WASABI_SECRET_ACCESS_KEY')
# OCI
if not OCI_NAMESPACE: missing_vars.append('OCI_NAMESPACE')
if not OCI_REGION: missing_vars.append('OCI_REGION')
if not OCI_ENDPOINT_URL: missing_vars.append('OCI_ENDPOINT_URL (derived from OCI_NAMESPACE/OCI_REGION)')
if not OCI_CONFIG['config']['aws_access_key_id']: missing_vars.append('OCI_ACCESS_KEY_ID')
if not OCI_CONFIG['config']['aws_secret_access_key']: missing_vars.append('OCI_SECRET_ACCESS_KEY')

if missing_vars:
    print(f"ERROR: Missing required environment variables: {', '.join(missing_vars)}")
    sys.exit(f"FATAL: Missing environment variables: {', '.join(missing_vars)}")


# --- Boto3 Helper Functions ---
def initialize_client(config_dict):
    """Initialize S3 client."""
    required_keys = ['aws_access_key_id', 'aws_secret_access_key', 'endpoint_url']
    if any(not config_dict['config'].get(key) for key in required_keys):
        print(f"Warning: Missing credentials or endpoint for {config_dict['name']}. Skipping client initialization.")
        return None
    try:
        client_config = {
            'service_name': config_dict['config']['service_name'],
            'endpoint_url': config_dict['config']['endpoint_url'],
            'aws_access_key_id': config_dict['config']['aws_access_key_id'],
            'aws_secret_access_key': config_dict['config']['aws_secret_access_key'],
            'region_name': config_dict['config']['region_name']
        }
        if 'config' in config_dict['config'] and isinstance(config_dict['config']['config'], Config):
             client_config['config'] = config_dict['config']['config']
        return boto3.client(**client_config)
    except Exception as e:
        print(f"Error initializing client for {config_dict['name']}: {e}")
        return None

def get_bucket_size(client, bucket_name):
    """Calculate total size of all files in the bucket"""
    if not client:
        print(f"Skipping get_bucket_size for {bucket_name}: client not initialized.")
        return 0
    total_size = 0
    try:
        paginator = client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket_name)
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    total_size += obj.get('Size', 0)
    except ClientError as e:
        print(f"Error getting bucket size for {bucket_name}: {e}")
        if e.response['Error']['Code'] == 'NoSuchBucket':
             print(f"Bucket {bucket_name} does not exist.")
             return 0
        return 0
    except Exception as e:
        print(f"Unexpected error getting bucket size for {bucket_name}: {e}")
        return 0
    return total_size

def generate_presigned_url(client, bucket_name, file_name, expiration=3600):
    """Generate presigned URL for access (standard S3/R2/Wasabi/Impossible)"""
    if not client:
        print(f"Skipping generate_presigned_url for {file_name}: client not initialized.")
        return None
    try:
        url = client.generate_presigned_url('get_object',
                                             Params={'Bucket': bucket_name, 'Key': file_name},
                                             ExpiresIn=expiration)
        return url
    except ClientError as e:
        print(f"Error generating presigned URL for {file_name} in {bucket_name}: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error generating presigned URL for {file_name} in {bucket_name}: {e}")
        return None

def generate_oci_public_url(oci_namespace, oci_region, bucket_name, file_name):
    """Generate public URLs for OCI Object Storage."""
    if not all([oci_namespace, oci_region, bucket_name, file_name]):
        print("Warning: Missing info for OCI public URL generation.")
        return None
    try:
        encoded_name = quote(file_name, safe='')
        url = f"https://objectstorage.{oci_region}.oraclecloud.com/n/{oci_namespace}/b/{bucket_name}/o/{encoded_name}"
        return url
    except Exception as e:
        print(f"Error generating OCI public URL for {file_name}: {e}")
        return None


# --- Progress Update Functions ---
def update_job_progress(job_id, progress_data):
    """Updates job progress in a JSON file."""
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

# --- UPDATED: download_file_with_progress_task ---
def download_file_with_progress_task(job_id, url, temp_filepath, progress_data):
    """Downloads file, updates progress, and checks for cancellation signal frequently."""
    progress_data['download'] = {'stage': 'downloading', 'percentage': 0, 'message': 'Starting...'}
    update_job_progress(job_id, progress_data)
    cancel_file = os.path.join(STATUS_DIR, f"{job_id}.cancel") # Define cancel file path
    start_time = time.time()
    last_cancel_check_time = time.time() # Track when we last checked cancel file

    try:
        print(f"DEBUG [{job_id}]: Starting download request for {url}") # Log start
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            total_size = int(r.headers.get('content-length', 0))
            downloaded_size = 0
            last_progress_update_time = time.time()
            last_bytes = 0
            speed_str = "0 MB/s"
            percentage = 0 # Initialize percentage

            print(f"DEBUG [{job_id}]: Opened connection. Total size: {total_size}") # Log connection open

            with open(temp_filepath, 'wb') as f:
                # Check cancellation more frequently
                for chunk_num, chunk in enumerate(r.iter_content(chunk_size=8192 * 4)): # Increased chunk size might help speed

                    # Check for cancellation signal roughly every second
                    current_time_for_cancel_check = time.time()
                    if current_time_for_cancel_check - last_cancel_check_time > 1.0:
                        # print(f"DEBUG [{job_id}]: Checking for cancel file...") # Can be noisy
                        if os.path.exists(cancel_file):
                            print(f"EVENT [{job_id}]: Cancellation signal file found. Stopping download.")
                            f.close()
                            if os.path.exists(temp_filepath): os.remove(temp_filepath)
                            progress_data['download'] = {'stage': 'cancelled', 'percentage': percentage, 'message': 'Download cancelled by user.'}
                            progress_data['status'] = 'cancelled'
                            update_job_progress(job_id, progress_data)
                            if os.path.exists(cancel_file): os.remove(cancel_file)
                            return None, -1 # Signal cancellation
                        last_cancel_check_time = current_time_for_cancel_check # Update check time

                    if not chunk: continue
                    f.write(chunk)
                    downloaded_size += len(chunk)

                    # Progress Update Logic
                    if total_size > 0:
                        percentage = min(int((downloaded_size / total_size) * 100), 100)
                        current_time_for_progress = time.time()
                        # Update progress status ~once per second
                        if current_time_for_progress - last_progress_update_time > 1.0:
                            time_diff = current_time_for_progress - last_progress_update_time
                            bytes_diff = downloaded_size - last_bytes
                            speed = (bytes_diff / time_diff) if time_diff > 0 else 0
                            speed_str = f"{speed / (1024*1024):.2f} MB/s"
                            last_progress_update_time = current_time_for_progress
                            last_bytes = downloaded_size
                            size_str = f"{downloaded_size / (1024*1024):.2f} MB / {total_size / (1024*1024):.2f} MB ({speed_str})"
                            progress_data['download'] = {'stage': 'downloading', 'percentage': percentage, 'message': size_str}
                            update_job_progress(job_id, progress_data)
                    # End Progress Update Logic

            # Download finished successfully
            print(f"DEBUG [{job_id}]: Download loop finished.") # Log loop end
            msg = f"Complete: {downloaded_size / (1024*1024):.2f} MB"
            progress_data['download'] = {'stage': 'completed', 'percentage': 100, 'message': msg}
            update_job_progress(job_id, progress_data)
            return temp_filepath, downloaded_size

    except Exception as e:
        print(f"ERROR [{job_id}]: Exception during download: {type(e).__name__} - {e}") # Log exception
        # Check if cancellation happened just before exception
        if os.path.exists(cancel_file):
             print(f"DEBUG [{job_id}]: Download exception likely due to cancellation.")
             if os.path.exists(temp_filepath): os.remove(temp_filepath)
             progress_data['download'] = {'stage': 'cancelled', 'percentage': 0, 'message': 'Download cancelled by user.'}
             progress_data['status'] = 'cancelled'
             update_job_progress(job_id, progress_data)
             if os.path.exists(cancel_file): os.remove(cancel_file)
             return None, -1 # Signal cancellation
        else:
             # Genuine download error
             msg = f"Error: {e}"
             progress_data['download'] = {'stage': 'failed', 'percentage': 0, 'message': msg}
             progress_data['status'] = 'failed'
             update_job_progress(job_id, progress_data)
             raise # Re-raise the exception for the worker
# --- END UPDATED download_file_with_progress_task ---


# --- upload_file_to_cloud_task ---
# (Keep this function exactly as the previous version - using upload_file for all, checking limits for R2/OCI)
def upload_file_to_cloud_task(job_id, cloud_config, temp_filepath, progress_data):
    """Uploads file & generates URL. Uses upload_file for ALL clouds. Checks size limit for R2 and OCI."""
    cloud_name = cloud_config['name']
    progress_data.setdefault('clouds', {})[cloud_name] = {'stage': 'pending', 'percentage': 0, 'message': 'Waiting...'}
    update_job_progress(job_id, progress_data)

    upload_success = False
    final_url = None
    error_message = None
    client = None

    try:
        client = initialize_client(cloud_config)
        if not client: raise ConnectionError("Client initialization failed (check credentials/endpoint).")

        bucket_name = cloud_config['bucket_name']
        filename = progress_data['filename']

        # --- Size Check for Cloudflare R2 AND Oracle Cloud ---
        SIZE_LIMIT_GB = 19.5
        if cloud_name == R2_CONFIG['name'] or cloud_name == OCI_CONFIG['name']:
            print(f"DEBUG: Performing size check for {cloud_name} (Limit: {SIZE_LIMIT_GB}GB).")
            progress_data['clouds'][cloud_name]['stage'] = 'checking'
            progress_data['clouds'][cloud_name]['message'] = f'Checking bucket size (Limit: {SIZE_LIMIT_GB}GB)...'
            update_job_progress(job_id, progress_data)
            if not os.path.exists(temp_filepath): raise FileNotFoundError(f"Temporary file not found: {temp_filepath}")
            new_file_size = os.path.getsize(temp_filepath)
            max_bytes = SIZE_LIMIT_GB * 1024 ** 3
            existing_size = get_bucket_size(client, bucket_name)
            print(f"DEBUG: {cloud_name} - Existing: {existing_size}, New: {new_file_size}, Total: {existing_size + new_file_size}, Max: {max_bytes}")
            if existing_size + new_file_size > max_bytes:
                excess_gb = ((existing_size + new_file_size) - max_bytes) / 1024**3
                msg = f"Skipped: Would exceed {SIZE_LIMIT_GB}GB limit by {excess_gb:.2f} GB."
                print(f"DEBUG: {cloud_name} - {msg}")
                progress_data['clouds'][cloud_name] = {'stage': 'skipped', 'percentage': 0, 'message': msg}
                update_job_progress(job_id, progress_data)
                return
            else: print(f"DEBUG: {cloud_name} - Size check passed.")
        # --- End Size Check ---

        # --- Check file existence and size ---
        if not os.path.exists(temp_filepath): raise FileNotFoundError(f"Temporary file not found before upload: {temp_filepath}")
        file_size = os.path.getsize(temp_filepath)
        if file_size == 0:
            msg = "Skipped: File size is 0 bytes."; progress_data['clouds'][cloud_name] = {'stage': 'skipped', 'percentage': 0, 'message': msg}; update_job_progress(job_id, progress_data); return

        # --- Start upload using upload_file for ALL clouds ---
        progress_callback = ProgressTracker(job_id, cloud_name, file_size, progress_data)
        transfer_config = TransferConfig(multipart_threshold=8*1024*1024, max_concurrency=10, multipart_chunksize=8*1024*1024, use_threads=True)
        progress_data['clouds'][cloud_name]['stage'] = 'uploading'
        progress_data['clouds'][cloud_name]['percentage'] = 0
        progress_data['clouds'][cloud_name]['message'] = f"0 MB / {file_size / (1024*1024):.2f} MB (Starting...)"
        update_job_progress(job_id, progress_data)
        print(f"DEBUG: Using upload_file for {cloud_name}.")
        upload_args = {'Filename': temp_filepath, 'Bucket': bucket_name, 'Key': filename, 'Callback': progress_callback, 'Config': transfer_config}
        client.upload_file(**upload_args)
        # --- End Upload Logic ---

        upload_success = True
        print(f"DEBUG: Upload seems successful for {cloud_name}, job {job_id}")

    except Exception as e:
        error_message = f"Upload failed: {e}"
        print(f"ERROR: Upload to {cloud_name} failed for job {job_id}: {e}")
        print(f"ERROR Type: {type(e).__name__}")

    # --- Update final status and generate URL ---
    if upload_success and client:
        progress_data['clouds'][cloud_name]['stage'] = 'generating_url'; progress_data['clouds'][cloud_name]['message'] = 'Generating URL...'; update_job_progress(job_id, progress_data)
        if cloud_name == 'Oracle Cloud':
            final_url = generate_oci_public_url(cloud_config.get('oci_namespace'), cloud_config.get('oci_region'), bucket_name, filename)
            if not final_url: final_url = generate_presigned_url(client, bucket_name, filename, expiration=604800); url_type = "Presigned (7 days)"
            else: url_type = "Public (Permanent*)"
        else: final_url = generate_presigned_url(client, bucket_name, filename, expiration=604800); url_type = "Presigned (7 days)"
        if final_url: msg = f"Complete. {url_type}: {final_url}"; progress_data['clouds'][cloud_name] = {'stage': 'completed', 'percentage': 100, 'message': msg}
        else: msg = "Complete (URL generation failed)."; progress_data['clouds'][cloud_name] = {'stage': 'completed', 'percentage': 100, 'message': msg}
    elif not upload_success: msg = error_message if error_message else "Upload failed: Unknown error"; progress_data['clouds'][cloud_name] = {'stage': 'failed', 'percentage': 0, 'message': msg}
    elif not client: msg = "Failed: Client initialization failed"; progress_data['clouds'][cloud_name] = {'stage': 'failed', 'percentage': 0, 'message': msg}
    update_job_progress(job_id, progress_data)
# --- END upload_file_to_cloud_task ---
