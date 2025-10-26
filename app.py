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

def save_state(job_list):
    """Saves the master job list to a file."""
    try:
        os.makedirs(STATUS_DIR, exist_ok=True)
        with open(MASTER_STATE_FILE, 'w') as f:
            json.dump(job_list, f, indent=4)
    except Exception as e:
        print(f"Error saving state: {e}")
        st.error(f"Failed to save job list: {e}")

def load_state():
    """Loads the master job list from a file."""
    global STATE_LOADED
    if STATE_LOADED:
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
                print(f"DEBUG: Loaded state with {len(loaded_jobs)} jobs.")
                STATE_LOADED = True
                return loaded_jobs
        except json.JSONDecodeError as e:
             print(f"Error decoding master state file: {e}. Starting fresh.")
             st.warning(f"Could not read job list file ({MASTER_STATE_FILE}): {e}. Starting fresh.")
             STATE_LOADED = True
             return []
        except Exception as e:
            print(f"Error loading state: {e}")
            st.error(f"Failed to load job list ({MASTER_STATE_FILE}): {e}")
            STATE_LOADED = True
            return []
    else:
         print("DEBUG: Master state file not found.")
         STATE_LOADED = True
         return []

def get_job_progress(job_id):
    """Loads a specific job's progress. Returns None on critical read errors."""
    status_file = os.path.join(STATUS_DIR, f"{job_id}.json")
    if os.path.exists(status_file):
        try:
            if os.path.getsize(status_file) == 0:
                 return {'status': 'processing', 'message': 'Worker initializing...'}
            with open(status_file, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            print(f"Error decoding job progress file {job_id}: {e}")
            return {'status': 'processing', 'message': f'Reading status failed: {e}'}
        except Exception as e:
            print(f"Error loading job {job_id}: {e}")
            st.error(f"Cannot read status for Job ID {job_id}: {e}")
            return None
    else:
        return {'status': 'pending'}

# --- Streamlit UI ---

st.set_page_config(page_title="Multi-Cloud Uploader", layout="wide")
st.title("🚀 Multi-Cloud Remote URL Uploader")

if 'jobs' not in st.session_state:
    st.session_state.jobs = load_state()

# --- Sidebar for Adding Jobs ---
# (No changes needed here)
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
                if 'jobs' not in st.session_state:
                     st.session_state.jobs = []
                st.session_state.jobs.append(new_job)
                save_state(st.session_state.jobs)
                st.success(f"Added '{filename}' to queue for {', '.join(selected_clouds)}.")
                st.rerun()

# --- Main Page for Job Queue and Processing ---
st.header("Job Queue")

# --- Queue Management Buttons ---
# (No changes needed here)
col_b1, col_b2 = st.columns(2)
with col_b1:
    pending_jobs_exist = any(j.get('status') == 'pending' for j in st.session_state.get('jobs', []))
    if st.button("Process All Pending Jobs", type="primary", use_container_width=True, disabled=not pending_jobs_exist):
        launched_jobs = 0
        jobs_in_state = st.session_state.get('jobs', [])
        if not jobs_in_state:
             st.warning("Job list is empty in session state.")
        for i, job in enumerate(jobs_in_state):
            if job.get('status') == 'pending':
                job_id = job['job_id']
                selected_clouds_str = ",".join(job.get('selected_clouds', AVAILABLE_CLOUDS))
                print(f"DEBUG: Launching worker for job: {job_id}, Clouds: {selected_clouds_str}")
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
                    if i < len(st.session_state.jobs):
                         st.session_state.jobs[i]['status'] = 'processing'
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
    clearable_jobs_exist = any(j.get('status') in ['completed', 'failed'] for j in st.session_state.get('jobs', []))
    if st.button("Clear Completed/Failed Jobs", use_container_width=True, disabled=not clearable_jobs_exist):
        initial_count = len(st.session_state.get('jobs', []))
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
if not st.session_state.get('jobs'):
    st.info("No jobs in queue. Add a job using the sidebar.")
else:
    needs_rerun = False
    jobs_to_remove_indices = []

    jobs_list_snapshot = list(st.session_state.jobs)

    for i, job_summary in enumerate(jobs_list_snapshot):
        job_id = job_summary.get('job_id')
        if not job_id:
             st.error(f"Job at index {i} is missing an ID. Skipping.")
             continue

        filename = job_summary.get('filename', f'Unknown Job {job_id}')
        url = job_summary.get('url', 'N/A')
        selected_clouds_display = job_summary.get('selected_clouds', AVAILABLE_CLOUDS)

        job_progress = get_job_progress(job_id)

        if job_progress is None:
             status = 'error (read failed)'
             if i < len(st.session_state.jobs):
                 st.session_state.jobs[i]['status'] = status
             needs_rerun = False
             with st.expander(f"**{filename}** (Status: {status})", expanded=True):
                  st.error(f"Could not read progress file for Job ID {job_id}. Check permissions or logs.")
             continue

        status_from_master_list = job_summary.get('status')
        status_from_file = job_progress.get('status')

        if status_from_master_list == 'processing' and (not status_from_file or status_from_file == 'pending'):
            status = 'processing'
        elif status_from_file:
            status = status_from_file
        else:
            status = status_from_master_list if status_from_master_list else 'pending'

        if i < len(st.session_state.jobs) and st.session_state.jobs[i].get('status') != status:
             st.session_state.jobs[i]['status'] = status

        if status == 'processing':
            needs_rerun = True

        with st.expander(f"**{filename}** (Status: {status})", expanded=(status not in ['completed', 'failed'])):
            st.caption(f"URL: {url} | Job ID: {job_id}")
            st.caption(f"Target Clouds: {', '.join(selected_clouds_display)}")

            if status == 'pending':
                if st.button("Cancel Job ❌", key=f"cancel_{job_id}"):
                    jobs_to_remove_indices.append(i)
                    try:
                        for ext in ['.json', '.out.log', '.err.log']:
                             filepath = os.path.join(STATUS_DIR, f"{job_id}{ext}")
                             if os.path.exists(filepath):
                                 os.remove(filepath)
                    except OSError as e:
                        st.warning(f"Could not remove status/log file for cancelled job {job_id}: {e}")

            dl_progress = job_progress.get('download', {})
            dl_stage = dl_progress.get('stage', 'pending')
            dl_perc = dl_progress.get('percentage', 0)
            dl_msg = dl_progress.get('message', job_progress.get('message', '') if status == 'processing' else '')

            st.text("📥 Download")
            st.progress(dl_perc)
            st.caption(f"Status: {dl_stage} | {dl_msg}")

            st.text("☁️ Cloud Uploads")
            cloud_progress = job_progress.get('clouds', {})

            for cloud_name in selected_clouds_display:
                prog = cloud_progress.get(cloud_name, {})
                stage = prog.get('stage', 'waiting')
                if status == 'processing' and stage == 'waiting':
                     stage = 'pending'

                perc = prog.get('percentage', 0)
                msg = prog.get('message', '')

                st.markdown(f"**{cloud_name}** (Status: *{stage}*)")
                st.progress(perc)
                # --- CORRECTED: Simplified caption display ---
                if msg:
                    st.caption(msg)
                # --- END CORRECTION ---

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
        indices_to_remove_set = set(jobs_to_remove_indices)
        original_jobs = st.session_state.get('jobs', [])
        new_jobs_list = []

        for i, job in enumerate(original_jobs):
             if i not in indices_to_remove_set:
                 new_jobs_list.append(job)
             else:
                 st.toast(f"Cancelled job: {job.get('filename', 'Unknown')}")

        st.session_state.jobs = new_jobs_list
        save_state(st.session_state.jobs)
        st.rerun()

    # --- Auto-refresh logic ---
    if needs_rerun:
        time.sleep(2)
        st.rerun()
