import sys
import os
import json
import threading
from tasks import (
    download_file_with_progress_task,
    upload_file_to_cloud_task,
    CLOUDS, # Import the updated list which includes OCI
    DOWNLOAD_DIR,
    STATUS_DIR,
    update_job_progress
)

def main(job_id, url, filename, selected_clouds_str):
    selected_clouds_list = []
    if selected_clouds_str:
        selected_clouds_list = selected_clouds_str.split(',')
    print(f"Job {job_id}: Received selected clouds: {selected_clouds_list}")

    progress_data = {
        'job_id': job_id,
        'filename': filename,
        'url': url,
        'status': 'processing',
        'download': {},
        'clouds': {cloud_name: {} for cloud_name in selected_clouds_list}
    }

    status_file = os.path.join(STATUS_DIR, f"{job_id}.json")
    try:
        os.makedirs(STATUS_DIR, exist_ok=True)
        update_job_progress(job_id, progress_data)
        print(f"Job {job_id}: Initial status file written.")
    except Exception as e:
        print(f"FATAL: Failed to write initial status for {job_id}: {e}")
        try:
            with open(status_file, 'w') as f:
                json.dump({'status': 'failed', 'message': f'Failed to write initial status: {e}'}, f)
        except: pass
        return

    # Use a unique temp file path including job_id
    temp_filepath = os.path.join(DOWNLOAD_DIR, f"{job_id}_{filename}")
    temp_filepath_actual = None # Variable to store actual path after download

    try:
        # --- 1. Download File ---
        print(f"Job {job_id}: Starting download from {url} to {temp_filepath}")
        temp_filepath_actual, new_file_size = download_file_with_progress_task(
            job_id, url, temp_filepath, progress_data
        )
        print(f"Job {job_id}: Download completed. Size: {new_file_size} bytes.")

        # --- 2. Upload to SELECTED clouds sequentially ---
        if new_file_size > 0 and selected_clouds_list:
             print(f"Job {job_id}: Starting uploads to: {', '.join(selected_clouds_list)}")
             clouds_to_upload = [cloud for cloud in CLOUDS if cloud['name'] in selected_clouds_list]

             for cloud_config in clouds_to_upload:
                 cloud_name = cloud_config['name']
                 print(f"Job {job_id}: Uploading to {cloud_name}...")
                 # Call the task function which now handles URL generation internally
                 upload_file_to_cloud_task(
                     job_id, cloud_config, temp_filepath_actual, progress_data
                 )
                 print(f"Job {job_id}: Upload attempt finished for {cloud_name}.")

        elif new_file_size <= 0: # Check for 0 or negative size
             print(f"Job {job_id}: Skipping uploads because downloaded file size is {new_file_size}.")
             progress_data['status'] = 'failed'
             if 'download' in progress_data and progress_data['download'].get('stage') != 'failed':
                progress_data['download']['message'] = "Download resulted in 0 bytes file."


        # --- 3. Final Status ---
        upload_failed = False
        all_skipped_or_completed = True
        for cloud_name in selected_clouds_list:
            cloud_status = progress_data.get('clouds', {}).get(cloud_name, {}).get('stage')
            if cloud_status == 'failed':
                upload_failed = True
                all_skipped_or_completed = False # If one failed, not all completed/skipped
                # break # We want to check all selected clouds
            elif cloud_status not in ['completed', 'skipped']:
                 all_skipped_or_completed = False

        # Mark completed only if download succeeded AND all selected uploads are completed or skipped
        if progress_data['status'] != 'failed' and all_skipped_or_completed:
             progress_data['status'] = 'completed'
             print(f"Job {job_id}: Marked as completed.")
        # Mark as failed if download failed OR any selected upload failed
        elif progress_data['status'] == 'failed' or upload_failed:
             progress_data['status'] = 'failed'
             print(f"Job {job_id}: Marked as failed due to download or upload error(s).")
        # Handle cases where processing might still be ongoing (shouldn't happen here but safe)
        else:
             print(f"Job {job_id}: Worker finished but final status is indeterminate (should be processing).")


    except Exception as e:
        print(f"Job {job_id}: Worker process failed: {e}")
        progress_data['status'] = 'failed'
        if 'download' in progress_data and progress_data['download'].get('stage') != 'failed':
             progress_data.setdefault('download', {})['stage'] = 'failed'
             progress_data.setdefault('download', {})['message'] = f"Worker failed: {e}"
        print(f"Job {job_id}: Marked as failed due to worker exception.")


    finally:
        # --- 4. Cleanup ---
        try:
            # Use temp_filepath_actual which holds the path returned by download task
            if temp_filepath_actual and os.path.exists(temp_filepath_actual):
                os.remove(temp_filepath_actual)
                print(f"Job {job_id}: Cleaned up temp file {temp_filepath_actual}")
        except Exception as e:
            print(f"Job {job_id}: Warning - Could not delete temp file {temp_filepath_actual}: {e}")

        # Write final status
        try:
            update_job_progress(job_id, progress_data)
            print(f"Job {job_id}: Final status '{progress_data['status']}' written.")
        except Exception as e:
             print(f"Job {job_id}: ERROR - Failed to write final status: {e}")

if __name__ == "__main__":
    print(f"Worker started with args: {sys.argv}")
    if len(sys.argv) != 5:
        print("Usage: python worker.py <job_id> <url> <filename> <selected_clouds_comma_separated>")
        sys.exit(1)

    job_id = sys.argv[1]
    url = sys.argv[2]
    filename = sys.argv[3]
    selected_clouds_str = sys.argv[4]

    main(job_id, url, filename, selected_clouds_str)
    print(f"Worker finished for job {job_id}.")
