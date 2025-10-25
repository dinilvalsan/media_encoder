import os
import subprocess
import uuid
import boto3
import runpod
from glob import glob  # To find thumbnail files

# --- Configuration for Cloudflare R2 ---
try:
    CLOUDFLARE_ACCOUNT_ID = os.environ['CLOUDFLARE_ACCOUNT_ID']
    S3_ACCESS_KEY = os.environ['CLOUDFLARE_R2_ACCESS_KEY_ID']
    S3_SECRET_KEY = os.environ['CLOUDFLARE_R2_SECRET_ACCESS_KEY']
    S3_BUCKET_NAME = os.environ['CLOUDFLARE_R2_BUCKET_NAME']

    # Construct the R2 S3-compatible endpoint URL
    S3_ENDPOINT_URL = f"https{':'}//{CLOUDFLARE_ACCOUNT_ID}.r2.cloudflarestorage.com"

except KeyError as e:
    print(f"Fatal: Missing environment variable: {e.args[0]}")
    print(
        "Please set CLOUDFLARE_ACCOUNT_ID, CLOUDFLARE_R2_ACCESS_KEY_ID, CLOUDFLARE_R2_SECRET_ACCESS_KEY, and CLOUDFLARE_R2_BUCKET_NAME")
    # We'll let it fail later if handler is called, but this alerts on startup
    S3_ENDPOINT_URL = None  # Set to None to ensure failure if env vars are missing

# Initialize S3 client
if S3_ENDPOINT_URL:
    s3 = boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name='auto'  # R2 specific
    )


# ---------------------


def transcode_to_mp4(input_path, output_path):
    """
    Task 1: Transcodes a video to a web-ready MP4 using H.264 and AAC.
    """
    print(f"Transcoding {input_path} to {output_path}...")
    command = [
        'ffmpeg',
        '-i', input_path,
        '-c:v', 'libx264',  # H.264 video codec
        '-preset', 'fast',  # Good balance of speed and quality
        '-c:a', 'aac',  # AAC audio codec
        '-b:a', '128k',  # 128 kbps audio bitrate
        '-movflags', '+faststart',  # Optimizes for web streaming
        output_path
    ]
    subprocess.run(command, check=True)
    print("Transcoding complete.")


def generate_thumbnails(input_path, output_dir):
    """
    Task 2: Generates thumbnails every 10 seconds.
    """
    print(f"Generating thumbnails for {input_path}...")
    output_pattern = os.path.join(output_dir, 'thumb_%03d.jpg')
    command = [
        'ffmpeg',
        '-i', input_path,
        '-vf', 'fps=1/10',  # 1 frame every 10 seconds
        '-q:v', '3',  # Output quality (1-5, 2-3 is good)
        output_pattern
    ]
    subprocess.run(command, check=True)

    # Find all generated thumbnails
    thumbnail_files = sorted(glob(os.path.join(output_dir, 'thumb_*.jpg')))
    print(f"Generated {len(thumbnail_files)} thumbnails.")
    return thumbnail_files


def analyze_thumbnails_ai(thumbnail_paths):
    """
    Task 3: Placeholder for future AI analysis.
    """
    print("AI analysis placeholder: Not implemented yet.")
    # In the future, you would load a model and process each thumbnail
    #
    # for path in thumbnail_paths:
    #     image = load_image(path)
    #     result = ai_model.predict(image)
    #     print(f"AI result for {path}: {result}")

    ai_metadata = {
        "status": "pending",
        "message": "AI analysis will be implemented in the future."
    }
    return ai_metadata


def handler(job):
    """
    The main handler function for the RunPod Serverless worker.
    """
    if not S3_ENDPOINT_URL or not s3:
        return {"error": "Server is misconfigured. Missing R2 environment variables."}

    job_input = job['input']

    # --- 1. Get Job Details ---
    # Expecting an S3 key for the uploaded video
    try:
        source_video_key = job_input['source_video_key']
    except KeyError:
        return {"error": "Missing 'source_video_key' in job input."}

    # Generate unique IDs for this job
    job_id = str(uuid.uuid4())
    local_input_dir = f"/tmp/{job_id}_input"
    local_output_dir = f"/tmp/{job_id}_output"

    os.makedirs(local_input_dir, exist_ok=True)
    os.makedirs(local_output_dir, exist_ok=True)

    local_input_path = os.path.join(local_input_dir, os.path.basename(source_video_key))
    transcoded_filename = f"{os.path.splitext(os.path.basename(source_video_key))[0]}_processed.mp4"
    local_transcoded_path = os.path.join(local_output_dir, transcoded_filename)

    try:
        # --- 2. Download Source File from R2 ---
        print(f"Downloading {source_video_key} from R2 bucket {S3_BUCKET_NAME}...")
        s3.download_file(S3_BUCKET_NAME, source_video_key, local_input_path)
        print("Download complete.")

        # --- 3. Run Processing Tasks ---

        # Task 1: Transcode
        transcode_to_mp4(local_input_path, local_transcoded_path)

        # Task 2: Generate Thumbnails
        thumbnail_paths = generate_thumbnails(local_transcoded_path, local_output_dir)

        # Task 3: AI Placeholder
        ai_results = analyze_thumbnails_ai(thumbnail_paths)

        # --- 4. Upload Results back to R2 ---
        s3_output_prefix = f"processed/{job_id}"

        # Upload transcoded video
        s3_transcoded_key = f"{s3_output_prefix}/{transcoded_filename}"
        print(f"Uploading transcoded video to {s3_transcoded_key}...")
        s3.upload_file(local_transcoded_path, S3_BUCKET_NAME, s3_transcoded_key)

        # Upload thumbnails
        s3_thumbnail_keys = []
        for thumb_path in thumbnail_paths:
            thumb_filename = os.path.basename(thumb_path)
            s3_thumb_key = f"{s3_output_prefix}/thumbnails/{thumb_filename}"
            print(f"Uploading thumbnail {s3_thumb_key}...")
            s3.upload_file(thumb_path, S3_BUCKET_NAME, s3_thumb_key)
            s3_thumbnail_keys.append(s3_thumb_key)

        print("All uploads complete.")

        # --- 5. Return Success Response ---
        # Note: These keys are not public URLs. Your application will need
        # to either generate presigned URLs or serve them via your R2.dev / public domain.
        return {
            "status": "success",
            "transcoded_video_key": s3_transcoded_key,
            "thumbnail_keys": s3_thumbnail_keys,
            "ai_analysis": ai_results,
            "public_base_url": f"{os.environ.get('CLOUDFLARE_R2_PUBLIC_URL', 'https://YOUR-PUBLIC-URL.dev')}/{s3_output_prefix}"
        }

    except Exception as e:
        print(f"Job failed: {e}")
        return {"error": str(e)}

    finally:
        # --- 6. Cleanup ---
        # Clean up local temp files to free space for the next job
        print("Cleaning up local files...")
        for dir_path in [local_input_dir, local_output_dir]:
            if os.path.exists(dir_path):
                for f in glob(f"{dir_path}/*"):
                    os.remove(f)
                os.rmdir(dir_path)
        print("Cleanup complete.")


# Start the RunPod worker
runpod.serverless.start({"handler": handler})