import sys
import os
import boto3

# Add project root to path to import api_aws
sys.path.append(os.getcwd())

from api_aws.s3_helpers import list_runs, list_steps, build_nc_key, BUCKET

def download_sample():
    print(f"Connecting to S3 bucket: {BUCKET}...")
    s3 = boto3.client("s3")
    
    print("Fetching available runs...")
    runs = list_runs()
    if not runs:
        print("No runs found.")
        return

    latest_run = runs[-1]
    print(f"Selected Run: {latest_run}")

    print(f"Fetching steps for run {latest_run}...")
    steps = list_steps(latest_run)
    if not steps:
        print("No steps found.")
        return
        
    first_step = steps[0]
    print(f"Selected Step: {first_step}")

    key = build_nc_key(latest_run, first_step)
    filename = os.path.basename(key)
    
    print(f"Downloading {key} to {filename}...")
    try:
        s3.download_file(BUCKET, key, filename)
        print(f"✅ Success! File saved as: {filename}")
        print("You can now upload this file in the 'Local File' tab of the frontend.")
    except Exception as e:
        print(f"❌ Error downloading file: {e}")

if __name__ == "__main__":
    download_sample()
