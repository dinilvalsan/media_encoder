# Use a minimal Python base image
FROM python:3.10-slim

# Install ffmpeg
RUN apt-get update && \
    apt-get install -y ffmpeg && \
    rm -rf /var/lib/apt/lists/*

# Set up the working directory
WORKDIR /app

# Copy and install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your handler code
COPY handler.py .

# Command to run the RunPod worker
CMD ["python", "-u", "handler.py"]