-----

````markdown
# Multi-Cloud Remote URL Uploader 🚀☁️

This application provides a web interface (built with Streamlit) to download files from URLs and upload them to multiple S3-compatible cloud storage providers simultaneously (Cloudflare R2, Wasabi, Impossible Cloud). It's designed to run inside a Docker container using a detached background process for downloads/uploads, ensuring persistence even if the browser is refreshed.

## Features ✨

* **Web Interface:** Simple UI to add download jobs via URL.
* **Custom Filenames:** Option to specify a custom filename for the uploaded file.
* **Multi-Cloud Upload:** Uploads to Cloudflare R2, Wasabi, and Impossible Cloud.
* **Cloudflare R2 Limit Check:** Automatically checks if the upload will exceed the specified Cloudflare R2 size limit (default 9.5GB) before uploading to R2.
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
````

*(You can replace `multi-cloud-uploader` with your preferred image name/tag)*

### 3\. Run the Container

Proceed to the **"Run the Container"** section below (it's the same command whether you build locally or pull).

-----

## Option 2: Pull and Run Directly from Docker Hub  PULL⬇️

Use this option to run the pre-built image without needing the source code locally.

### 1\. Prerequisites

  * Docker installed on your machine/server.

### 2\. Pull the Image

Download the latest pre-built image from Docker Hub:

```bash
docker pull ajithvnr2001/multi-cloud-uploader:v1
```

*(Make sure you have specified the correct tag, e.g., `:v1` or `:latest` if you pushed with that tag)*

### 3\. Run the Container

Proceed to the **"Run the Container"** section below.

-----

## Run the Container (Common Step) ▶️

Whether you built the image locally or pulled it from Docker Hub, you use the following command to run it.

You **must** provide your cloud storage credentials and configuration as environment variables using the `-e` flag.

```bash
docker run -d \
  -p 3001:3001 \
  -v $(pwd)/job_status:/app/job_status \
  -v $(pwd)/temp_downloads:/app/temp_downloads \
  --restart unless-stopped \
  -e R2_ACCOUNT_ID='YOUR_R2_ACCOUNT_ID' \
  -e R2_BUCKET_NAME='YOUR_R2_BUCKET_NAME' \
  -e R2_ACCESS_KEY_ID='YOUR_R2_KEY_ID' \
  -e R2_SECRET_ACCESS_KEY='YOUR_R2_SECRET' \
  -e IMPOSSIBLE_BUCKET_NAME='YOUR_IMPOSSIBLE_BUCKET_NAME' \
  -e IMPOSSIBLE_ACCESS_KEY_ID='YOUR_IMPOSSIBLE_KEY_ID' \
  -e IMPOSSIBLE_SECRET_ACCESS_KEY='YOUR_IMPOSSIBLE_SECRET' \
  -e WASABI_BUCKET_NAME='YOUR_WASABI_BUCKET_NAME' \
  -e WASABI_ACCESS_KEY_ID='YOUR_WASABI_KEY_ID' \
  -e WASABI_SECRET_ACCESS_KEY='YOUR_WASABI_SECRET' \
  --name uploader-app \
  ajithvnr2001/multi-cloud-uploader:v1 # Use this name if you pulled the image
# Or use 'multi-cloud-uploader' if you built it locally with that tag
```

**Explanation of Flags:**

  * `-d`: Run the container in detached mode (in the background).
  * `-p 3001:3001`: Maps port 3001 on your host to port 3001 in the container.
  * `-v $(pwd)/job_status:/app/job_status`: **(Important for Persistence)** Mounts a `job_status` folder from your current directory on the host into the container. This saves the job list and progress files. **Crucial for resuming after restarts.** Create this folder (`mkdir job_status`) on your host if it doesn't exist before running.
  * `-v $(pwd)/temp_downloads:/app/temp_downloads`: Mounts a `temp_downloads` folder. Useful for debugging or potentially resuming uploads later (though current logic doesn't explicitly support resuming partial uploads). Create this folder (`mkdir temp_downloads`) on your host if it doesn't exist. Can be removed if host storage is very limited, but temporary files will be lost if the container is removed.
  * `--restart unless-stopped`: Automatically restarts the container if it crashes or the server reboots, unless you manually stop it via `docker stop uploader-app`.
  * `-e VARIABLE_NAME='VALUE'`: Sets environment variables inside the container for credentials and configuration. **Replace ALL placeholder values\!**
  * `--name uploader-app`: Assigns a convenient name (`uploader-app`) to the running container, making it easier to manage.
  * `ajithvnr2001/multi-cloud-uploader:v1`: **The name of the image to run**. Use the Docker Hub name if you pulled it, or the local name (e.g., `multi-cloud-uploader`) if you built it yourself.

-----

## Required Environment Variables 🔑

You need to set the following environment variables when running the container:

| Variable                     | Description                                    | Example                                           |
| :--------------------------- | :--------------------------------------------- | :------------------------------------------------ |
| `R2_ACCOUNT_ID`              | Your Cloudflare R2 Account ID                  | `7e8efdbjkbshjb84dc56e32`                |
| `R2_BUCKET_NAME`             | The R2 bucket name to upload to                | `my-r2-bucket`                                    |
| `R2_ACCESS_KEY_ID`           | Your R2 Access Key ID                          | `44c2hbshjb8441138364e0efbab`                |
| `R2_SECRET_ACCESS_KEY`       | Your R2 Secret Access Key                      | `6491f7c6...c771c880`                             |
| `IMPOSSIBLE_BUCKET_NAME`     | The Impossible Cloud bucket name (optional\*)  | `my-impossible-bucket`                            |
| `IMPOSSIBLE_ACCESS_KEY_ID`   | Your Impossible Cloud Access Key ID            | `E66CAKNFJN3EEF92A`                            |
| `IMPOSSIBLE_SECRET_ACCESS_KEY`| Your Impossible Cloud Secret Access Key        | `ad8fb4SJNSJK..f51b701`                       |
| `WASABI_BUCKET_NAME`         | The Wasabi bucket name (optional\*)            | `my-wasabi-bucket`                                |
| `WASABI_ACCESS_KEY_ID`       | Your Wasabi Access Key ID                      | `AZLR3FJNSJNFDJK8BO`                            |
| `WASABI_SECRET_ACCESS_KEY`   | Your Wasabi Secret Access Key                  | `CJ2twkjnsdjkbnf8R...85BTMJD`                       |

*\* If bucket name variables are omitted, the application uses default names (`vnrbnr` for Impossible, `thisismybuck` for Wasabi).*

-----

## Usage 🖱️

1.  Open your web browser and navigate to `http://<your_server_ip>:3001`.
2.  Use the sidebar to enter the **File URL** and an optional **Custom Filename**.
3.  Click **Add to Queue**.
4.  The job will appear in the main **Job Queue** area with a "pending" status.
5.  Add more jobs as needed.
6.  Click **Process All Pending Jobs** to start the downloads and uploads sequentially. The background worker script (`worker.py`) will be launched for each job.
7.  Monitor the progress bars and status messages. The UI refreshes automatically every few seconds while jobs are processing.
8.  You can **safely refresh the browser or close it**; the background `worker.py` process for the currently active job will continue running. When you reopen the app, it will read the latest status from the saved files.
9.  Use **Cancel Job ❌** to remove a job *before* it starts processing (`pending` status). This deletes the job from the list.
10. Use **Clear Completed/Failed Jobs** to remove finished or failed jobs from the UI list and the saved state file.

-----

## Managing the Container ⚙️

Use these commands on your server where you ran `docker run`:

  * **View Logs:** See the output from the Streamlit application (useful for UI errors).

    ```bash
    docker logs uploader-app
    ```

    *(Use `docker logs -f uploader-app` to follow the logs in real-time.)*

  * **Stop the Container:** Gracefully stops the application. Background jobs currently processing *may* be interrupted depending on how quickly the container stops. Jobs will remain in the queue thanks to persistence.

    ```bash
    docker stop uploader-app
    ```

  * **Start the Container:** Restarts a previously stopped container. It will reload the job queue from the `job_status` volume.

    ```bash
    docker start uploader-app
    ```

  * **Remove the Container:** Stops and deletes the container instance. **Your job queue data is safe** because it's stored in the host volume (`job_status`).

    ```bash
    docker stop uploader-app # Stop it first (if running)
    docker rm uploader-app
    ```

    *(After removing, you'll need to use `docker run` again to recreate it.)*

-----

## Clearing Old Job Queues / Starting Fresh 🧹

Because the job list and status files are saved in the mounted host volume (`$(pwd)/job_status`), they persist even if you remove and recreate the container. To completely clear the queue and start fresh:

1.  **Stop the running container:**
    ```bash
    docker stop uploader-app
    ```
2.  **Delete the job status folder on your HOST machine:**
    *(Make sure you are in the parent directory where you created the `job_status` folder, e.g., `/home/opc/multi_up_docker`)*
    ```bash
    rm -rf ./job_status
    ```
    **Warning:** This permanently deletes all job history and progress.
3.  **Start the container again:**
    ```bash
    docker start uploader-app
    ```
    *(Or use `docker run...` if you had removed the container).* The app will create a new, empty `job_status` directory inside the volume mount and start with no jobs.

-----

## Troubleshooting 🪵

  * **UI Errors:** Check the container logs: `docker logs uploader-app`.
  * **Download/Upload Failures:** Check the individual job logs stored in the mounted `job_status` volume on your host machine (the folder where you ran `docker run`). Look for files named `job_<id>.out.log` (standard output) and `job_<id>.err.log` (error output) for the specific job ID. These contain the direct output from the background `worker.py` script.
  * **Container Stops Unexpectedly:** This might be due to running out of memory (OOM Killer) on low-resource VPS. The `--restart unless-stopped` flag helps mitigate this by automatically restarting. Check system logs (`dmesg` or `/var/log/syslog`) on the host for OOM messages.
  * **Permission Denied Errors:** Ensure the folders you are mounting (`job_status`, `temp_downloads`) on the host have the correct permissions for the Docker process to write to them. Sometimes running `chmod -R 777 ./job_status ./temp_downloads` might be necessary, although this is overly permissive for production.

<!-- end list -->

```
```
