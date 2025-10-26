# Multi-Cloud Remote URL Uploader 🚀☁️

This application provides a web interface (built with Streamlit) to download files from URLs and upload them to multiple S3-compatible cloud storage providers simultaneously (Cloudflare R2, Wasabi, Impossible Cloud, **Oracle Cloud**). It's designed to run inside a Docker container using a detached background process for downloads/uploads, ensuring persistence even if the browser is refreshed.

## Features ✨

* **Web Interface:** Simple UI to add download jobs via URL.
* **Custom Filenames:** Option to specify a custom filename for the uploaded file.
* **Multi-Cloud Upload:** Uploads to Cloudflare R2, Wasabi, Impossible Cloud, and **Oracle Cloud**.
* **Selective Upload:** Choose which cloud providers to upload to for each job.
* **Cloudflare R2 Limit Check:** Automatically checks if the upload will exceed the specified Cloudflare R2 size limit (default **19.5GB**) before uploading to R2.
* **Job Queue:** Add multiple download URLs; they will be processed sequentially when you click "Process".
* **Progress Tracking:** Real-time progress bars for downloads and uploads, including size and speed.
* **Background Processing:** Downloads and uploads run as separate background processes using `subprocess.Popen`, allowing the UI to remain responsive and preserving progress even if the browser is refreshed or closed.
* **Persistence:** The job queue and individual job statuses are saved to disk, so jobs are not lost if the container restarts.
* **Secure Credential Handling:** Cloud credentials and configuration are provided via environment variables, not hardcoded in the image.

---

## Required Files (for Building Locally) 📂

If you choose to build the image yourself, make sure you have the following files in your project directory:

1.  `Dockerfile`: Defines how to build the Docker image.
2.  `requirements.txt`: Lists the required Python libraries.
3.  `app.py`: The main Streamlit web application script.
4.  `worker.py`: The script that runs in the background to perform downloads/uploads.
5.  `tasks.py`: Contains the core logic for downloading, uploading, and interacting with cloud providers.
6.  `.dockerignore`: Tells Docker which files/folders to exclude from the build context (important!).
7.  `.gitignore`: Tells Git which files/folders to ignore (e.g., `job_status`, `temp_downloads`).

---

## Option 1: Build and Run Locally 🛠️

Use this option if you want to build the image from the source code.

### 1. Prerequisites

* Docker installed on your machine/server.

### 2. Build the Image

Clone the repository (or ensure all required files are present) and navigate into the project directory. Then, build the Docker image:

```bash
# Make sure you are in the directory containing the Dockerfile
docker build -t multi-cloud-uploader .
