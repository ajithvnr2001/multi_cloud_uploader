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

def save_state(job_list):
    """Saves the master job list to a file."""
    try:
        with open(MASTER_STATE_FILE, 'w') as f:
            json.dump(job_list, f, indent=4)
    except Exception as e:
        print(f"Error saving state: {e}")

def load_state():
    """Loads the master job list from a file."""
    if os.path.exists(MASTER_STATE_FILE):
        try:
            with open(MASTER_STATE_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading state: {e}")
            return []
    return []

def get_job_progress(job_id):
    """Loads a specific job's progress from its JSON file."""
    status_file = os.path.join(STATUS_DIR, f"{job_id}.json")
    if os.path.exists(status_file):
        try:
            with open(status_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            # File might be being written, just return a pending state
            print(f"Error loading job {job_id}: {e}")
            # Return a minimal processing state to avoid flicker
            return {'status': 'processing'}
    # Job file not created yet, so it's pending
    return {'status': 'pending'} 

# --- Streamlit UI ---

st.set_page_config(page_title="Multi-Cloud Uploader", layout="wide")
st.title("🚀 Multi-Cloud Remote URL Uploader")

# Initialize session state for job queue
if 'jobs' not in st.session_state:
    st.session_state.jobs = load_state()

# --- Sidebar for Adding Jobs ---
with st.sidebar:
    st.header("Add New Job")
    with st.form("new_job_form", clear_on_submit=True):
        url = st.text_input("File URL *", placeholder="https.example.com/file.zip")
        custom_filename = st.text_input("Custom Filename (optional)", placeholder="Leave empty to use original filename")
        add_job_submitted = st.form_submit_button("Add to Queue")
        
        if add_job_submitted and url:
            filename = custom_filename or os.path.basename(urlparse(url).path) or f"file_{hash(url)}"
            # Create a unique job ID based on time
            job_id = f"job_{int(time.time())}_{hash(filename) & 0xffff}"
            
            new_job = {
                'job_id': job_id,
                'filename': filename,
                'url': url,
                'status': 'pending' # pending, processing, completed, failed
            }
            
            st.session_state.jobs.append(new_job)
            save_state(st.session_state.jobs) # Save the master list
            
            st.success(f"Added '{filename}' to queue.")
            st.rerun()

# --- Main Page for Job Queue and Processing ---
st.header("Job Queue")

# --- Queue Management Buttons ---
col_b1, col_b2 = st.columns(2)
with col_b1:
    no_pending_jobs = not any(j['status'] == 'pending' for j in st.session_state.jobs)
    if st.button("Process All Pending Jobs", type="primary", use_container_width=True, disabled=no_pending_jobs):
        for i, job in enumerate(st.session_state.jobs):
            if job['status'] == 'pending':
                # --- THIS IS THE KEY ---
                # Launch worker.py as a detached background process
                
                # Use sys.executable to ensure we're using the same Python
                command = [
                    sys.executable, 
                    "worker.py", 
                    job['job_id'], 
                    job['url'], 
                    job['filename']
                ]
                
                # Create log files for stdout and stderr
                log_dir = "/app/job_status" # Write logs to the persistent volume
                job_id = job['job_id']
                stdout_log = os.path.join(log_dir, f"{job_id}.out.log")
                stderr_log = os.path.join(log_dir, f"{job_id}.err.log")

                with open(stdout_log, 'wb') as out, open(stderr_log, 'wb') as err:
                    subprocess.Popen(command, stdout=out, stderr=err)
                
                # Update the status in our session state
                st.session_state.jobs[i]['status'] = 'processing'
        
        # Save this new "processing" state to the master list
        save_state(st.session_state.jobs)
        st.rerun()

with col_b2:
    no_clearable_jobs = not any(j['status'] in ['completed', 'failed'] for j in st.session_state.jobs)
    if st.button("Clear Completed/Failed Jobs", use_container_width=True, disabled=no_clearable_jobs):
        st.session_state.jobs = [j for j in st.session_state.jobs if j['status'] == 'pending' or j['status'] == 'processing']
        save_state(st.session_state.jobs)
        st.rerun()

st.markdown("---")

# --- Display Individual Jobs ---
if not st.session_state.jobs:
    st.info("No jobs in queue. Add a job using the sidebar.")

needs_rerun = False
jobs_to_remove = []

for i, job_summary in enumerate(st.session_state.jobs):
    job_id = job_summary['job_id']
    
    # Get the latest progress from the job's JSON file
    job_progress = get_job_progress(job_id)
    
    # --- MODIFIED LOGIC ---
    # Trust the master list if it says "processing" but the file isn't created yet.
    # Otherwise, trust the file.
    status_from_master_list = job_summary['status']
    status_from_file = job_progress.get('status') # This will be 'pending' if file doesn't exist

    if status_from_master_list == 'processing' and status_from_file == 'pending':
        status = 'processing'
    else:
        status = status_from_file

    st.session_state.jobs[i]['status'] = status # Update session state
    # --- END OF MODIFIED LOGIC ---
    
    filename = job_summary['filename']
    url = job_summary['url']

    if status == 'processing':
        needs_rerun = True

    with st.expander(f"**{filename}** (Status: {status})", expanded=True):
        st.caption(f"URL: {url} | Job ID: {job_id}")
        
        if status == 'pending':
            if st.button("Cancel Job ❌", key=f"cancel_{job_id}"):
                jobs_to_remove.append(i)
                # Also remove the status file
                try:
                    os.remove(os.path.join(STATUS_DIR, f"{job_id}.json"))
                except OSError:
                    pass
        
        # --- Show Progress Bars ---
        dl_progress = job_progress.get('download', {})
        dl_stage = dl_progress.get('stage', 'pending')
        dl_perc = dl_progress.get('percentage', 0)
        dl_msg = dl_progress.get('message', '')
        
        st.text("📥 Download")
        st.progress(dl_perc)
        st.caption(f"Status: {dl_stage} | {dl_msg}")

        st.text("☁️ Cloud Uploads")
        cloud_progress = job_progress.get('clouds', {})
        
        # Default to all clouds if none have reported yet
        cloud_names = cloud_progress.keys() if cloud_progress else ["Cloudflare R2", "ImpossibleCloud", "Wasabi"]
        
        for cloud_name in cloud_names:
            prog = cloud_progress.get(cloud_name, {})
            stage = prog.get('stage', 'pending')
            perc = prog.get('percentage', 0)
            msg = prog.get('message', '')

            st.markdown(f"**{cloud_name}** (Status: *{stage}*)")
            st.progress(perc)
            if msg:
                st.caption(msg)
                
        if status == 'failed':
            st.error(f"Job Failed. Check download or upload status for details.")

# --- Process Job Removals (outside the main loop) ---
if jobs_to_remove:
    for index in sorted(jobs_to_remove, reverse=True):
        st.toast(f"Cancelled job: {st.session_state.jobs[index]['filename']}")
        del st.session_state.jobs[index]
    save_state(st.session_state.jobs)
    st.rerun()

# --- Auto-refresh logic ---
if needs_rerun:
    time.sleep(2) # Poll every 2 seconds
    st.rerun()
