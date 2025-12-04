import boto3
from botocore import UNSIGNED
from botocore.config import Config
import os
from pathlib import Path

BUCKET_NAME = 'noaa-oar-mlwp-data'
MODEL_PREFIX = 'PANG_v100_GFS' # Pangu-Weather

def get_s3_client():
    """Create an unsigned S3 client for public bucket access."""
    return boto3.client('s3', config=Config(signature_version=UNSIGNED))

def list_common_prefixes(client, prefix):
    """List 'subdirectories' (common prefixes) under a given prefix."""
    response = client.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=prefix,
        Delimiter='/'
    )
    if 'CommonPrefixes' not in response:
        return []
    return [obj['Prefix'] for obj in response['CommonPrefixes']]

def get_latest_year(client, model_prefix):
    """Find the latest available year for the model."""
    # Ensure prefix ends with /
    if not model_prefix.endswith('/'):
        model_prefix += '/'
        
    years = list_common_prefixes(client, model_prefix)
    if not years:
        return None
    
    # Sort and pick the last one (latest year)
    years.sort()
    return years[-1]

def get_latest_date(client, year_prefix):
    """Find the latest available date (MMDD) for the given year."""
    dates = list_common_prefixes(client, year_prefix)
    if not dates:
        return None
    
    dates.sort()
    return dates[-1]

def get_files_in_date(client, date_prefix):
    """List all files in the specific date directory."""
    response = client.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=date_prefix
    )
    if 'Contents' not in response:
        return []
    return [obj['Key'] for obj in response['Contents']]

def download_file(client, key, local_dir):
    """Download the first 10MB of a file from S3 to the local directory."""
    filename = os.path.basename(key)
    local_path = os.path.join(local_dir, filename)
    
    print(f"Downloading first 10MB of {key} to {local_path}...", flush=True)
    
    try:
        # Use get_object with Range to download only first 10MB
        response = client.get_object(
            Bucket=BUCKET_NAME, 
            Key=key, 
            Range='bytes=0-10485760' # 10MB
        )
        
        with open(local_path, 'wb') as f:
            for chunk in response['Body'].iter_chunks(chunk_size=1024*1024):
                f.write(chunk)
                
        size = os.path.getsize(local_path)
        print(f"[SUCCESS] Download successful (partial)! File size: {size / 1024 / 1024:.2f} MB", flush=True)
        return local_path
    except Exception as e:
        print(f"[ERROR] Error downloading file: {e}", flush=True)
        return None

def main():
    print(f"--- NOAA S3 Connection PoC ---", flush=True)
    print(f"Bucket: {BUCKET_NAME}", flush=True)
    print(f"Target Model: {MODEL_PREFIX}", flush=True)
    
    print("Creating S3 client...", flush=True)
    client = get_s3_client()
    print("S3 client created.", flush=True)
    
    # 1. Find latest year
    print(f"\n1. Searching for latest year...", flush=True)
    latest_year_prefix = get_latest_year(client, MODEL_PREFIX)
    if not latest_year_prefix:
        print("[ERROR] No years found.", flush=True)
        return
    print(f"   Found: {latest_year_prefix}", flush=True)
    
    # 2. Find latest date
    print(f"\n2. Searching for latest date in {latest_year_prefix}...", flush=True)
    latest_date_prefix = get_latest_date(client, latest_year_prefix)
    if not latest_date_prefix:
        print("[ERROR] No dates found.", flush=True)
        return
    print(f"   Found: {latest_date_prefix}", flush=True)
    
    # 3. List files and pick one
    print(f"\n3. Listing files in {latest_date_prefix}...", flush=True)
    files = get_files_in_date(client, latest_date_prefix)
    if not files:
        print("[ERROR] No files found.", flush=True)
        return
    
    # Filter for .nc files just in case
    nc_files = [f for f in files if f.endswith('.nc')]
    if not nc_files:
        print("[ERROR] No .nc files found.", flush=True)
        return
        
    print(f"   Found {len(nc_files)} files. Picking the first one for test.", flush=True)
    target_file = nc_files[0]
    
    # 4. Download
    download_dir = os.path.join(os.path.dirname(__file__), 'downloads')
    os.makedirs(download_dir, exist_ok=True)
    
    download_file(client, target_file, download_dir)

if __name__ == "__main__":
    main()
