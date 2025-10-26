import streamlit as st
import os
import json
import time
import subprocess
import sys
from urllib.parse import urlparse

# --- State Management ---
STATUS_DIR = 'job_status'
MASTER_STATE_FILE = os.path.join(STATUS_DIR, 'master_job_list.json')
AVAILABLE_CLOUDS = ["Cloudflare R2", "ImpossibleCloud", "Wasabi", "Oracle Cloud"]
STATE_LOADED = False

# --- Added for Debugging ---
print("--- Script Start ---")

def save_state(job_list):
    """Saves the master job list to a file."""
    try:
        os.makedirs(STATUS_DIR, exist_ok=True)
        with open(MASTER_STATE_FILE, 'w') as f:
            json.dump(job_list, f, indent=4)
        print(f"DEBUG: Saved state with {len(job_list)} jobs.") # Debug print
    except Exception as e:
        print(f"ERROR: Error saving state: {e}") # Log error
        st.error(f"Failed to save job list: {e}")

def load_state():
    """Loads the master job list from a file."""
    global STATE_LOADED
    print("DEBUG: Entering load_state()") # Debug print
    if STATE_LOADED:
        print("DEBUG: State already loaded, returning session state.") # Debug print
        return st.session_state.get('jobs', [])

    if os.path.exists(MASTER_STATE_FILE):
        try:
            # Check file size before reading
            if os.path.getsize(MASTER_STATE_FILE) == 0:
                 print("DEBUG: Master state file is empty, returning empty list.") # Debug print
                 STATE_LOADED = True
                 return []
            with open(MASTER_STATE_FILE, 'r') as f:
                content = f.read() # Read entire file
                # Basic check if content looks like JSON start/end
                if not content.strip().startswith('[') or not content.strip().endswith(']'):
                     print("ERROR: Master state file content doesn't look like a JSON list.") # Debug print
                     st.warning("Job list file seems corrupted. Starting fresh.")
                     STATE_LOADED = True
                     return []
                loaded_jobs = json.loads(content)
                print(f"DEBUG: Loaded state with {len(loaded_jobs)} jobs.") # Debug print
                STATE_LOADED = True
                return loaded_jobs
        except json.JSONDecodeError as e:
             print(f"ERROR: Error decoding master state file: {e}. Starting fresh.")
             st.warning(f"Could not read job list file ({MASTER_STATE_FILE}): {e}. Starting fresh.")
             STATE_LOADED = True
             return []
        except Exception as e:
            print(f"ERROR: Error loading state: {e}")
            st.error(f"Failed to load job list ({MASTER_STATE_FILE}): {e}")
            STATE_LOADED = True
            return []
    else:
         print("DEBUG: Master state file not found, returning empty list.") # Debug print
         STATE_LOADED = True
         return []

def get_job_progress(job_id):
    """Loads a specific job's progress. Returns None on critical read errors."""
    status_file = os.path.join(STATUS_DIR, f"{job_id}.json")
    cancel_file = os.path.join(STATUS_DIR, f"{job_id}.cancel")

    if os.path.exists(cancel_file):
        print(f"DEBUG: Cancel file found for {job_id}, forcing 'cancelling' status.")
        return {'status': 'cancelling'}

    if os.path.exists(status_file):
        try:
            if os.path.getsize(status_file) == 0:
                 # print(f"DEBUG: Job progress file {job_id} is empty.")
                 return {'status': 'processing', 'message': 'Worker initializing...'}
            with open(status_file, 'r') as f:
                # print(f"DEBUG: Reading progress for {job_id}") # Debug print - can be noisy
                return json.load(f)
        except json.JSONDecodeError as e:
            print(f"ERROR: Error decoding job progress file {job_id}: {e}")
            return {'status': 'processing', 'message': f'Reading status failed: {e}'}
        except Exception as e:
            print(f"ERROR: Error loading job {job_id}: {e}")
            st.error(f"Cannot read status for Job ID {job_id}: {e}")
            return None # Critical error
    else:
        # print(f"DEBUG: Job progress file {job_id} not found.") # Debug print - can be noisy
        # Status file doesn't exist, check master list status
        master_status = 'pending'
        if 'jobs' in st.session_state:
            for job in st.session_state.jobs:
                if job.get('job_id') == job_id:
                    master_status = job.get('status', 'pending')
                    break
        # If master list thinks it's processing, show processing briefly, otherwise pending
        return {'status': 'processing' if master_status == 'processing' else 'pending'}


# --- Streamlit UI ---

st.set_page_config(page_title="Multi-Cloud Uploader", layout="wide")
print("DEBUG: Set page config") # Debug print
st.title("🚀 Multi-Cloud Remote URL Uploader")
print("DEBUG: Set title") # Debug print

# Initialize session state for job queue ONCE using load_state
if 'jobs' not in st.session_state:
    print("DEBUG: Initializing session state['jobs']") # Debug print
    st.session_state.jobs = load_state()
    print(f"DEBUG: Session state['jobs'] initialized with {len(st.session_state.jobs)} items.") # Debug print


# --- Sidebar ---
with st.sidebar:
    st.header("Add New Job")
    # (Sidebar form remains the same)
    with st.form("new_job_form", clear_on_submit=True):
        url = st.text_input("File URL *", placeholder="https.example.com/file.zip")
        custom_filename = st.text_input("Custom Filename (optional)", placeholder="Leave empty to use original filename")
        selected_clouds = st.multiselect(
            "Select Clouds to Upload To *",
            options=AVAILABLE_CLOUDS,
            default=AVAILABLE_CLOUDS
        )
        add_job_submitted = st.form_submit_button("Add to Queue")
        if add_job_submitted:
            if not url: st.error("File URL is required.")
            elif not selected_clouds: st.error("Please select at least one cloud provider.")
            else:
                filename = custom_filename or os.path.basename(urlparse(url).path) or f"file_{hash(url)}"
                job_id = f"job_{int(time.time())}_{hash(filename) & 0xffff}"
                new_job = { 'job_id': job_id, 'filename': filename, 'url': url, 'status': 'pending', 'selected_clouds': selected_clouds }
                if 'jobs' not in st.session_state: st.session_state.jobs = []
                st.session_state.jobs.append(new_job)
                save_state(st.session_state.jobs)
                st.success(f"Added '{filename}' to queue for {', '.join(selected_clouds)}.")
                st.rerun()

print("DEBUG: Finished sidebar rendering") # Debug print

# --- Main Page ---
st.header("Job Queue")
print("DEBUG: Set main page header") # Debug print

# --- Buttons ---
col_b1, col_b2 = st.columns(2)
with col_b1:
    # (Process button logic remains the same)
    pending_jobs_exist = any(j.get('status') == 'pending' for j in st.session_state.get('jobs', []))
    if st.button("Process All Pending Jobs", type="primary", use_container_width=True, disabled=not pending_jobs_exist):
        launched_jobs = 0
        jobs_in_state = st.session_state.get('jobs', [])
        if not jobs_in_state: st.warning("Job list is empty in session state.")
        for i, job in enumerate(jobs_in_state):
             if job.get('status') == 'pending':
                job_id = job['job_id']
                selected_clouds_str = ",".join(job.get('selected_clouds', AVAILABLE_CLOUDS))
                print(f"DEBUG: Launching worker for job: {job_id}, Clouds: {selected_clouds_str}")
                command = [ sys.executable, "worker.py", job_id, job['url'], job['filename'], selected_clouds_str]
                log_dir = "/app/job_status"
                stdout_log = os.path.join(log_dir, f"{job_id}.out.log")
                stderr_log = os.path.join(log_dir, f"{job_id}.err.log")
                try:
                    os.makedirs(log_dir, exist_ok=True)
                    initial_status = { 'job_id': job_id, 'filename': job['filename'], 'url': job['url'], 'status': 'processing', 'download': {'stage':'pending'}, 'clouds': {} }
                    with open(os.path.join(STATUS_DIR, f"{job_id}.json"), 'w') as f_init: json.dump(initial_status, f_init)
                    with open(stdout_log, 'wb') as out, open(stderr_log, 'wb') as err: subprocess.Popen(command, stdout=out, stderr=err)
                    if i < len(st.session_state.jobs): st.session_state.jobs[i]['status'] = 'processing'
                    print(f"DEBUG: Marked job {job_id} as processing in session state.")
                    launched_jobs += 1
                except Exception as e:
                     st.error(f"Failed to launch worker for job {job['filename']}: {e}")
                     print(f"ERROR: Failed to launch worker for job {job['filename']}: {e}")
        if launched_jobs > 0:
            print(f"DEBUG: Launched {launched_jobs} jobs. Saving state and rerunning.")
            save_state(st.session_state.jobs)
            st.rerun()
        elif pending_jobs_exist:
             st.warning("Could not launch any pending jobs. Check logs.")
             print("WARNING: Could not launch any pending jobs.")

with col_b2:
    # (Clear button logic remains the same)
    clearable_jobs_exist = any(j.get('status') in ['completed', 'failed', 'cancelled'] for j in st.session_state.get('jobs', []))
    if st.button("Clear Finished/Failed/Cancelled Jobs", use_container_width=True, disabled=not clearable_jobs_exist):
        initial_count = len(st.session_state.get('jobs', []))
        st.session_state.jobs = [j for j in st.session_state.get('jobs', []) if j.get('status') in ['pending', 'processing', 'cancelling']]
        cleared_count = initial_count - len(st.session_state.jobs)
        if cleared_count > 0:
            save_state(st.session_state.jobs)
            st.toast(f"Cleared {cleared_count} finished jobs.")
            st.rerun()
        else: st.toast("No finished jobs to clear.")

st.markdown("---")
print("DEBUG: Finished rendering buttons") # Debug print

# --- Display Jobs ---
if not st.session_state.get('jobs'):
    st.info("No jobs in queue. Add a job using the sidebar.")
    print("DEBUG: No jobs in queue to display.") # Debug print
else:
    print(f"DEBUG: Starting to display {len(st.session_state.jobs)} jobs.") # Debug print
    needs_rerun = False
    jobs_to_remove_indices = []

    jobs_list_snapshot = list(st.session_state.jobs)

    for i, job_summary in enumerate(jobs_list_snapshot):
        job_id = job_summary.get('job_id')
        print(f"DEBUG: Processing display for job index {i}, ID: {job_id}") # Debug print
        if not job_id:
             st.error(f"Job at index {i} is missing an ID. Skipping.")
             print(f"ERROR: Job at index {i} is missing an ID.") # Debug print
             continue

        filename = job_summary.get('filename', f'Unknown Job {job_id}')
        url = job_summary.get('url', 'N/A')
        selected_clouds_display = job_summary.get('selected_clouds', AVAILABLE_CLOUDS)

        job_progress = get_job_progress(job_id)

        if job_progress is None:
             status = 'error (read failed)'
             print(f"ERROR: get_job_progress returned None for {job_id}") # Debug print
             if i < len(st.session_state.jobs): st.session_state.jobs[i]['status'] = status
             needs_rerun = False
             with st.expander(f"**{filename}** (Status: {status})", expanded=True):
                  st.error(f"Could not read progress file for Job ID {job_id}. Check permissions or logs.")
             continue

        status_from_master_list = job_summary.get('status')
        status_from_file = job_progress.get('status')

        if status_from_file == 'cancelling': status = 'cancelling'
        elif status_from_master_list == 'processing' and (not status_from_file or status_from_file == 'pending'): status = 'processing'
        elif status_from_file: status = status_from_file
        else: status = status_from_master_list if status_from_master_list else 'pending'

        if i < len(st.session_state.jobs) and st.session_state.jobs[i].get('status') != status:
             st.session_state.jobs[i]['status'] = status
             if status in ['completed', 'failed', 'cancelled']: save_state(st.session_state.jobs)

        if status in ['processing', 'cancelling']: needs_rerun = True

        print(f"DEBUG: Rendering expander for job {job_id} with status {status}") # Debug print
        with st.expander(f"**{filename}** (Status: {status})", expanded=(status not in ['completed', 'failed', 'cancelled'])):
            st.caption(f"URL: {url} | Job ID: {job_id}")
            st.caption(f"Target Clouds: {', '.join(selected_clouds_display)}")

            dl_progress = job_progress.get('download', {})
            dl_stage = dl_progress.get('stage', 'pending')
            dl_perc = dl_progress.get('percentage', 0)
            dl_msg = dl_progress.get('message', job_progress.get('message', '') if status == 'processing' else '')

            cancel_col1, cancel_col2 = st.columns([0.8, 0.2])
            with cancel_col1: st.text("📥 Download")
            with cancel_col2:
                if status == 'processing' and dl_stage == 'downloading':
                    if st.button("Cancel ❌", key=f"cancel_{job_id}"):
                        cancel_file_path = os.path.join(STATUS_DIR, f"{job_id}.cancel")
                        try:
                            with open(cancel_file_path, 'w') as f_cancel: f_cancel.write(str(time.time()))
                            print(f"DEBUG: Created cancel file for job {job_id}")
                            if i < len(st.session_state.jobs): st.session_state.jobs[i]['status'] = 'cancelling'
                            save_state(st.session_state.jobs)
                            st.toast(f"Cancellation requested for {filename}...")
                            st.rerun()
                        except Exception as e: st.error(f"Could not request cancellation: {e}")
                elif status == 'pending':
                     if st.button("Remove ✂️", key=f"remove_{job_id}"):
                         jobs_to_remove_indices.append(i)

            st.progress(dl_perc)
            st.caption(f"Status: {dl_stage} | {dl_msg}")

            st.text("☁️ Cloud Uploads")
            cloud_progress = job_progress.get('clouds', {})

            for cloud_name in selected_clouds_display:
                prog = cloud_progress.get(cloud_name, {})
                stage = prog.get('stage', 'waiting')
                if status in ['processing', 'cancelling'] and stage == 'waiting': stage = 'pending'
                perc = prog.get('percentage', 0)
                msg = prog.get('message', '')
                st.markdown(f"**{cloud_name}** (Status: *{stage}*)")
                st.progress(perc)
                if msg: st.caption(msg)

            if status == 'failed':
                st.error(f"Job Failed. Check download or upload status above for details.")
                error_log_path = os.path.join(STATUS_DIR, f"{job_id}.err.log")
                if os.path.exists(error_log_path):
                     try:
                          with open(error_log_path, 'r') as f_err:
                               error_content = f_err.read().strip()
                               if error_content: st.code(error_content, language='log')
                               else: st.caption("Error log is empty.")
                     except Exception as log_e: st.caption(f"Could not read error log: {log_e}")
            elif status == 'cancelled':
                 st.info("Job was cancelled by user.")
        print(f"DEBUG: Finished rendering expander for job {job_id}") # Debug print

    # --- Process Job Removals ---
    if jobs_to_remove_indices:
        print(f"DEBUG: Removing job indices: {jobs_to_remove_indices}") # Debug print
        indices_to_remove_set = set(jobs_to_remove_indices)
        original_jobs = st.session_state.get('jobs', [])
        new_jobs_list = []
        for i, job in enumerate(original_jobs):
             if i not in indices_to_remove_set: new_jobs_list.append(job)
             else:
                 st.toast(f"Removed pending job: {job.get('filename', 'Unknown')}")
                 try: # Clean up potential status file
                      status_file = os.path.join(STATUS_DIR, f"{job.get('job_id')}.json")
                      if os.path.exists(status_file): os.remove(status_file)
                 except Exception as e: print(f"Could not remove status file for removed job {job.get('job_id')}: {e}")
        st.session_state.jobs = new_jobs_list
        save_state(st.session_state.jobs)
        st.rerun()

    # --- Auto-refresh logic ---
    if needs_rerun:
        print("DEBUG: Scheduling rerun for progress update...") # Debug print
        time.sleep(2)
        st.rerun()

print("--- Script End ---") # Debug print
