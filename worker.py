import sys
import os
import json
import threading
from tasks import (
    download_file_with_progress_task, 
    upload_file_to_cloud_task, 
    CLOUDS, 
    DOWNLOAD_DIR, 
    STATUS_DIR
)

def main(job_id, url, filename):
    
    # This is the master progress object for this job
    progress_data = {
        'job_id': job_id,
        'filename': filename,
        'url': url,
        'status': 'processing', # processing, completed, failed
        'download': {},
        'clouds': {}
    }
    
    # --- MODIFIED: Write initial "processing" status IMMEDIATELY ---
    status_file = os.path.join(STATUS_DIR, f"{job_id}.json")
    try:
        with open(status_file, 'w') as f:
            json.dump(progress_data, f)
    except Exception as e:
        print(f"Failed to write initial status for {job_id}: {e}")
        # If we can't even write the file, there's no point continuing.
        # We also need to log this error *to the file*
        try:
            with open(status_file, 'w') as f:
                json.dump({'status': 'failed', 'message': f'Failed to write initial status: {e}'}, f)
        except:
            pass # Total failure
        return 
    # --- End of fix ---
    
    temp_filepath = os.path.join(DOWNLOAD_DIR, f"{threading.get_ident()}_{filename}")
    
    try:
        # --- 1. Download File ---
        temp_filepath, new_file_size = download_file_with_progress_task(
            job_id, url, temp_filepath, progress_data
        )
        
        # --- 2. Upload to all clouds sequentially ---
        # (Using threads here is complex in a subprocess, sequential is safer)
        if new_file_size > 0:
            for cloud in CLOUDS:
                upload_file_to_cloud_task(
                    job_id, cloud, temp_filepath, progress_data
                )
        
        # --- 3. Final Status ---
        if progress_data['status'] != 'failed':
             progress_data['status'] = 'completed'
        
    except Exception as e:
        print(f"Job {job_id} failed: {e}")
        progress_data['status'] = 'failed'
        # Download task will set its own error
    
    finally:
        # --- 4. Cleanup ---
        try:
            if os.path.exists(temp_filepath):
                os.remove(temp_filepath)
        except Exception as e:
            print(f"Could not delete temp file: {e}")
            
        # Write final status
        with open(status_file, 'w') as f:
            json.dump(progress_data, f)

if __name__ == "__main__":
    # This script is called as: python worker.py <job_id> <url> <filename>
    if len(sys.argv) != 4:
        print("Usage: python worker.py <job_id> <url> <filename>")
        sys.exit(1)
        
    job_id = sys.argv[1]
    url = sys.argv[2]
    filename = sys.argv[3]
    
    main(job_id, url, filename)
