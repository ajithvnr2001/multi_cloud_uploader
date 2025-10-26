import sys
import os
import json
import threading
from tasks import (
    download_file_with_progress_task,
    upload_file_to_cloud_task,
    CLOUDS,
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
    cancel_file = os.path.join(STATUS_DIR, f"{job_id}.cancel")

    try:
        os.makedirs(STATUS_DIR, exist_ok=True)
        if os.path.exists(cancel_file):
             print(f"Job {job_id}: Cancelled before worker started. Exiting.")
             progress_data['status'] = 'cancelled'; progress_data['download'] = {'stage': 'cancelled', 'message': 'Cancelled before start.'}
             update_job_progress(job_id, progress_data)
             os.remove(cancel_file)
             return
        update_job_progress(job_id, progress_data)
        print(f"Job {job_id}: Initial status file written.")
    except Exception as e:
        print(f"FATAL: Failed to write initial status for {job_id}: {e}")
        try: update_job_progress(job_id, {'status': 'failed', 'message': f'Worker init failed: {e}'})
        except: pass
        if os.path.exists(cancel_file): os.remove(cancel_file)
        return

    temp_filepath = os.path.join(DOWNLOAD_DIR, f"{job_id}_{filename}")
    temp_filepath_actual = None # Initialize
    download_cancelled = False

    try:
        # --- 1. Download File ---
        print(f"Job {job_id}: Starting download from {url} to {temp_filepath}")
        temp_filepath_actual, new_file_size = download_file_with_progress_task(
            job_id, url, temp_filepath, progress_data
        )
        if new_file_size == -1: # Cancellation signalled
            download_cancelled = True
            print(f"Job {job_id}: Download task signalled cancellation.")
        elif temp_filepath_actual:
            print(f"Job {job_id}: Download completed. Size: {new_file_size} bytes.")
        else:
             raise RuntimeError("Download task returned None without signalling cancellation.")

        # --- 2. Upload (Check cancellation again) ---
        if os.path.exists(cancel_file):
             print(f"Job {job_id}: Cancellation signal found after download, before uploads. Skipping uploads.")
             download_cancelled = True # Ensure flag is set even if download finished
             progress_data['status'] = 'cancelled'
             if progress_data.get('download', {}).get('stage') != 'cancelled':
                  progress_data['download'] = {'stage': 'cancelled', 'message': 'Cancelled after download.'}
             update_job_progress(job_id, progress_data)
             # Don't remove cancel file yet, let finally block handle it

        if not download_cancelled and new_file_size > 0 and selected_clouds_list:
             print(f"Job {job_id}: Starting uploads to: {', '.join(selected_clouds_list)}")
             clouds_to_upload = [cloud for cloud in CLOUDS if cloud['name'] in selected_clouds_list]
             for cloud_config in clouds_to_upload:
                 cloud_name = cloud_config['name']
                 print(f"Job {job_id}: Uploading to {cloud_name}...")
                 try:
                    upload_file_to_cloud_task(job_id, cloud_config, temp_filepath_actual, progress_data)
                    print(f"Job {job_id}: Upload attempt finished for {cloud_name}.")
                 except Exception as upload_err:
                      print(f"Job {job_id}: ERROR during upload to {cloud_name}: {upload_err}")
                      progress_data.setdefault('clouds', {}).setdefault(cloud_name, {}).update({'stage':'failed', 'message': f"Upload failed: {upload_err}"})
                      update_job_progress(job_id, progress_data)

        elif download_cancelled: print(f"Job {job_id}: Skipping uploads due to cancellation.")
        elif new_file_size <= 0:
             print(f"Job {job_id}: Skipping uploads because downloaded file size is {new_file_size}.")
             if progress_data['status'] != 'failed': progress_data['status'] = 'failed'
             if 'download' in progress_data and progress_data['download'].get('stage') != 'failed': progress_data['download']['message'] = "Download resulted in 0 bytes file."

        # --- 3. Final Status ---
        if download_cancelled: final_status = 'cancelled'
        elif progress_data['status'] == 'failed': final_status = 'failed'
        else:
            upload_failed = False; all_skipped_or_completed = True
            for cloud_name in selected_clouds_list:
                cloud_status = progress_data.get('clouds', {}).get(cloud_name, {}).get('stage')
                if cloud_status == 'failed': upload_failed = True; all_skipped_or_completed = False
                elif cloud_status not in ['completed', 'skipped']: all_skipped_or_completed = False
            if upload_failed: final_status = 'failed'
            elif all_skipped_or_completed: final_status = 'completed'
            else: final_status = 'processing'; print(f"WARN: Job {job_id} worker finished but status indeterminate.")
        progress_data['status'] = final_status
        print(f"Job {job_id}: Final status determined as '{final_status}'.")

    except Exception as e:
        print(f"Job {job_id}: Worker process failed with unhandled exception: {e}")
        if download_cancelled: progress_data['status'] = 'cancelled'
        else:
            progress_data['status'] = 'failed'
            if 'download' not in progress_data or progress_data.get('download', {}).get('stage') != 'failed':
                 progress_data.setdefault('download', {})['stage'] = 'failed'
                 progress_data.setdefault('download', {})['message'] = f"Worker failed: {e}"
        print(f"Job {job_id}: Marked as '{progress_data['status']}' due to worker exception.")

    finally:
        # --- 4. Cleanup ---
        # --- MODIFIED: Check temp_filepath_actual before using ---
        temp_file_to_remove = temp_filepath_actual if temp_filepath_actual else temp_filepath
        try:
            if os.path.exists(temp_file_to_remove):
                os.remove(temp_file_to_remove)
                print(f"Job {job_id}: Cleaned up temp file {temp_file_to_remove}")
        except Exception as e:
            print(f"Job {job_id}: Warning - Could not delete temp file {temp_file_to_remove}: {e}")
        # --- END MODIFIED ---

        # Clean up cancel file if it exists
        try:
            if os.path.exists(cancel_file):
                os.remove(cancel_file)
                print(f"Job {job_id}: Cleaned up cancel signal file.")
        except Exception as e:
            print(f"Job {job_id}: Warning - Could not delete cancel file {cancel_file}: {e}")

        # Write final status
        try:
            update_job_progress(job_id, progress_data)
            print(f"Job {job_id}: Final status '{progress_data['status']}' written.")
        except Exception as e:
             print(f"Job {job_id}: ERROR - Failed to write final status: {e}")

# --- Main execution block ---
# (Keep existing __main__ block)
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
