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
# --- Define available clouds for UI ---
AVAILABLE_CLOUDS = ["Cloudflare R2", "ImpossibleCloud", "Wasabi"]

# --- NEW: Flag to ensure state is loaded only once ---
STATE_LOADED = False

def save_state(job_list):
    """Saves the master job list to a file."""
    try:
        # Ensure directory exists
        os.makedirs(STATUS_DIR, exist_ok=True)
        with open(MASTER_STATE_FILE, 'w') as f:
            json.dump(job_list, f, indent=4)
        # print(f"DEBUG: Saved state with {len(job_list)} jobs.") # Optional debug print
    except Exception as e:
        print(f"Error saving state: {e}")
        st.error(f"Failed to save job list: {e}") # Show error in UI

def load_state():
    """Loads the master job list from a file."""
    global STATE_LOADED
    if STATE_LOADED: # Avoid reloading if already loaded in this run
        return st.session_state.get('jobs', [])

    if os.path.exists(MASTER_STATE_FILE):
        try:
            with open(MASTER_STATE_FILE, 'r') as f:
                content = f.read()
                if not content:
                    print("DEBUG: Master state file is empty.")
                    STATE_LOADED = True
                    return []
                loaded_jobs = json.loads(content)
                print(f"DEBUG: Loaded state with {len(loaded_jobs)} jobs.") # Optional debug print
                STATE_LOADED = True
                return loaded_jobs
        except json.JSONDecodeError as e:
             print(f"Error decoding master state file: {e}. Starting fresh.")
             st.warning(f"Could not read job list file ({MASTER_STATE_FILE}): {e}. Starting fresh.")
             STATE_LOADED = True
             return [] # Return empty list on corruption
        except Exception as e:
            print(f"Error loading state: {e}")
            st.error(f"Failed to load job list ({MASTER_STATE_FILE}): {e}")
            STATE_LOADED = True
            return [] # Return empty list on other errors
    else:
         print("DEBUG: Master state file not found.")
         STATE_LOADED = True
         return [] # File doesn't exist yet

def get_job_progress(job_id):
    """Loads a specific job's progress. Returns None on critical read errors."""
    status_file = os.path.join(STATUS_DIR, f"{job_id}.json")
    if os.path.exists(status_file):
        try:
            # Check file size first - empty file causes JSONDecodeError
            if os.path.getsize(status_file) == 0:
                 # print(f"DEBUG: Job progress file {job_id} is empty.") # Optional debug
                 # File exists but is empty, likely being written. Treat as processing.
                 return {'status': 'processing', 'message': 'Worker initializing...'}
            with open(status_file, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            print(f"Error decoding job progress file {job_id}: {e}")
            # File exists but is corrupted or partially written. Treat as processing.
            return {'status': 'processing', 'message': f'Reading status failed: {e}'}
        except Exception as e:
            print(f"Error loading job {job_id}: {e}")
            # More serious error (e.g., permissions). Return None to signal failure.
            st.error(f"Cannot read status for Job ID {job_id}: {e}")
            return None # Indicate a critical read error
    else:
        # Job file not created yet. Status depends on master list.
        # print(f"DEBUG: Job progress file {job_id} not found.") # Optional debug
        return {'status': 'pending'}


# --- Streamlit UI ---

st.set_page_config(page_title="Multi-Cloud Uploader", layout="wide")
st.title("🚀 Multi-Cloud Remote URL Uploader")

# Initialize session state for job queue ONCE using load_state
if 'jobs' not in st.session_state:
    st.session_state.jobs = load_state()

# --- Sidebar for Adding Jobs ---
# (No changes needed in the sidebar form itself)
with st.sidebar:
    st.header("Add New Job")
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
            if not url:
                st.error("File URL is required.")
            elif not selected_clouds:
                 st.error("Please select at least one cloud provider.")
            else:
                filename = custom_filename or os.path.basename(urlparse(url).path) or f"file_{hash(url)}"
                job_id = f"job_{int(time.time())}_{hash(filename) & 0xffff}"

                new_job = {
                    'job_id': job_id,
                    'filename': filename,
                    'url': url,
                    'status': 'pending',
                    'selected_clouds': selected_clouds
                }

                # Ensure jobs list exists before appending
                if 'jobs' not in st.session_state:
                     st.session_state.jobs = []
                st.session_state.jobs.append(new_job)
                save_state(st.session_state.jobs)

                st.success(f"Added '{filename}' to queue for {', '.join(selected_clouds)}.")
                st.rerun()

# --- Main Page for Job Queue and Processing ---
st.header("Job Queue")

# --- Queue Management Buttons ---
# (No significant changes needed, added some print/toast messages)
col_b1, col_b2 = st.columns(2)
with col_b1:
    # Check jobs directly from session state
    pending_jobs_exist = any(j.get('status') == 'pending' for j in st.session_state.get('jobs', []))
    if st.button("Process All Pending Jobs", type="primary", use_container_width=True, disabled=not pending_jobs_exist):
        launched_jobs = 0
        jobs_in_state = st.session_state.get('jobs', []) # Get current list
        if not jobs_in_state:
             st.warning("Job list is empty in session state.")

        for i, job in enumerate(jobs_in_state):
            # Double check status before launching
            if job.get('status') == 'pending':
                job_id = job['job_id']
                selected_clouds_str = ",".join(job.get('selected_clouds', AVAILABLE_CLOUDS))
                print(f"DEBUG: Launching worker for job: {job_id}, Clouds: {selected_clouds_str}") # Debug print

                command = [
                    sys.executable,
                    "worker.py",
                    job_id,
                    job['url'],
                    job['filename'],
                    selected_clouds_str
                ]

                log_dir = "/app/job_status"
                stdout_log = os.path.join(log_dir, f"{job_id}.out.log")
                stderr_log = os.path.join(log_dir, f"{job_id}.err.log")

                try:
                    os.makedirs(log_dir, exist_ok=True)
                    with open(stdout_log, 'wb') as out, open(stderr_log, 'wb') as err:
                        subprocess.Popen(command, stdout=out, stderr=err)
                    # Update status ONLY in the session state list
                    st.session_state.jobs[i]['status'] = 'processing'
                    print(f"DEBUG: Marked job {job_id} as processing in session state.") # Debug print
                    launched_jobs += 1
                except Exception as e:
                     st.error(f"Failed to launch worker for job {job['filename']}: {e}")
                     print(f"ERROR: Failed to launch worker for job {job['filename']}: {e}") # Ensure error logged

        if launched_jobs > 0:
            print(f"DEBUG: Launched {launched_jobs} jobs. Saving state and rerunning.") # Debug print
            save_state(st.session_state.jobs) # Save updated statuses
            st.rerun()
        elif pending_jobs_exist: # Only warn if there were pending jobs but none launched
             st.warning("Could not launch any pending jobs. Check logs.")
             print("WARNING: Could not launch any pending jobs.") # Debug print


with col_b2:
    clearable_jobs_exist = any(j.get('status') in ['completed', 'failed'] for j in st.session_state.get('jobs', []))
    if st.button("Clear Completed/Failed Jobs", use_container_width=True, disabled=not clearable_jobs_exist):
        initial_count = len(st.session_state.get('jobs', []))
        # Keep only pending or processing jobs
        st.session_state.jobs = [j for j in st.session_state.get('jobs', []) if j.get('status') in ['pending', 'processing']]
        cleared_count = initial_count - len(st.session_state.jobs)
        if cleared_count > 0:
            save_state(st.session_state.jobs)
            st.toast(f"Cleared {cleared_count} finished jobs.")
            st.rerun()
        else:
             st.toast("No completed or failed jobs to clear.")


st.markdown("---")

# --- Display Individual Jobs ---
if not st.session_state.get('jobs'): # Check if list exists and has items
    st.info("No jobs in queue. Add a job using the sidebar.")
else:
    needs_rerun = False
    jobs_to_remove_indices = [] # Store indices instead of jobs

    # Iterate safely over a copy if modifying the list, otherwise iterate directly
    jobs_list_snapshot = list(st.session_state.jobs) # Create snapshot for iteration

    for i, job_summary in enumerate(jobs_list_snapshot):
        job_id = job_summary.get('job_id')
        if not job_id:
             st.error(f"Job at index {i} is missing an ID. Skipping.")
             continue # Skip malformed job entry

        filename = job_summary.get('filename', f'Unknown Job {job_id}')
        url = job_summary.get('url', 'N/A')
        selected_clouds_display = job_summary.get('selected_clouds', AVAILABLE_CLOUDS)

        # Get the latest progress from the job's JSON file
        job_progress = get_job_progress(job_id)

        # --- MODIFIED: Handle case where get_job_progress returns None ---
        if job_progress is None:
             # Critical error reading status file, update status and show error
             status = 'error (read failed)'
             if i < len(st.session_state.jobs): # Check index validity
                 st.session_state.jobs[i]['status'] = status
             # No automatic rerun for read errors
             needs_rerun = False # Override potential previous True value

             with st.expander(f"**{filename}** (Status: {status})", expanded=True):
                  st.error(f"Could not read progress file for Job ID {job_id}. Check permissions or logs.")
             continue # Skip rest of rendering for this job
        # --- END MODIFIED ---

        # Determine current status (Trust master list for initial 'processing' state)
        status_from_master_list = job_summary.get('status')
        status_from_file = job_progress.get('status')

        if status_from_master_list == 'processing' and (not status_from_file or status_from_file == 'pending'):
            status = 'processing'
        elif status_from_file:
            status = status_from_file
        else:
            status = status_from_master_list if status_from_master_list else 'pending'

        # Update session state if necessary
        if i < len(st.session_state.jobs) and st.session_state.jobs[i].get('status') != status:
             st.session_state.jobs[i]['status'] = status
             # Consider saving state here if status changes significantly,
             # but might cause too many writes. Let's rely on button clicks for saves.
             # save_state(st.session_state.jobs)

        if status == 'processing':
            needs_rerun = True

        # Render job details
        with st.expander(f"**{filename}** (Status: {status})", expanded=(status not in ['completed', 'failed'])):
            st.caption(f"URL: {url} | Job ID: {job_id}")
            st.caption(f"Target Clouds: {', '.join(selected_clouds_display)}")

            if status == 'pending':
                if st.button("Cancel Job ❌", key=f"cancel_{job_id}"):
                    jobs_to_remove_indices.append(i) # Add index to remove list
                    # Remove status/log files immediately
                    try:
                        for ext in ['.json', '.out.log', '.err.log']:
                             filepath = os.path.join(STATUS_DIR, f"{job_id}{ext}")
                             if os.path.exists(filepath):
                                 os.remove(filepath)
                    except OSError as e:
                        st.warning(f"Could not remove status/log file for cancelled job {job_id}: {e}")

            # --- Show Progress Bars ---
            dl_progress = job_progress.get('download', {})
            dl_stage = dl_progress.get('stage', 'pending')
            dl_perc = dl_progress.get('percentage', 0)
            dl_msg = dl_progress.get('message', job_progress.get('message', '') if status == 'processing' else '') # Show init message

            st.text("📥 Download")
            st.progress(dl_perc)
            st.caption(f"Status: {dl_stage} | {dl_msg}")

            st.text("☁️ Cloud Uploads")
            cloud_progress = job_progress.get('clouds', {})

            for cloud_name in selected_clouds_display:
                prog = cloud_progress.get(cloud_name, {})
                stage = prog.get('stage', 'waiting') # Default to waiting if not processing/pending
                if status == 'processing' and stage == 'waiting':
                     stage = 'pending' # Show pending only if main job is processing

                perc = prog.get('percentage', 0)
                msg = prog.get('message', '')

                st.markdown(f"**{cloud_name}** (Status: *{stage}*)")
                st.progress(perc)
                if msg:
                    st.caption(msg)

            if status == 'failed':
                st.error(f"Job Failed. Check download or upload status above for details.")
                error_log_path = os.path.join(STATUS_DIR, f"{job_id}.err.log")
                if os.path.exists(error_log_path):
                     try:
                          with open(error_log_path, 'r') as f_err:
                               error_content = f_err.read().strip()
                               if error_content:
                                    st.code(error_content, language='log')
                               else:
                                     st.caption("Error log is empty.")
                     except Exception as log_e:
                          st.caption(f"Could not read error log: {log_e}")


    # --- Process Job Removals (outside the main loop) ---
    if jobs_to_remove_indices:
        # Create a new list excluding the indices to remove
        new_jobs_list = []
        indices_to_remove_set = set(jobs_to_remove_indices)
        original_jobs = st.session_state.get('jobs', []) # Get current list

        for i, job in enumerate(original_jobs):
             if i not in indices_to_remove_set:
                 new_jobs_list.append(job)
             else:
                 # Show toast for the job being removed
                 st.toast(f"Cancelled job: {job.get('filename', 'Unknown')}")

        st.session_state.jobs = new_jobs_list # Update session state
        save_state(st.session_state.jobs) # Save the modified list
        st.rerun()

    # --- Auto-refresh logic ---
    if needs_rerun:
        time.sleep(2) # Poll every 2 seconds
        # print("DEBUG: Rerunning for progress update...") # Optional debug print
        st.rerun()

# --- Fallback if script somehow finishes without rendering ---
# This shouldn't normally be reached with st.rerun() but added as safety
# print("DEBUG: End of script reached.")
