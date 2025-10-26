# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy all the python code into the container
COPY . .

# --- NEW: Explicitly copy worker.py again ---
COPY worker.py .
# --- END NEW ---

# Create directories for job state and temporary downloads
RUN mkdir -p /app/job_status
RUN mkdir -p /app/temp_downloads

# Make port 3001 available
EXPOSE 3001

# Define environment variable for Streamlit
ENV STREAMLIT_SERVER_PORT=3001
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0

# Run app.py when the container launches
CMD ["streamlit", "run", "app.py"]
