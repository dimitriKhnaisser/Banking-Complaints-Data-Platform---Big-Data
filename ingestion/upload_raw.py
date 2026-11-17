"""Piece C: upload the raw full-load files to MinIO (S3-compatible storage)."""
import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.client import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()  # reads the .env file in the project folder

ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
BUCKET = os.getenv("S3_BUCKET", "raw")
ACCESS_KEY = os.environ["MINIO_ROOT_USER"]
SECRET_KEY = os.environ["MINIO_ROOT_PASSWORD"]

FILES = ["complaints.csv.zip", "complaints.csv", "manifest.json"]


def get_client():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),  # needed for local S3 stores
    )


def ensure_bucket(s3):
    try:
        s3.head_bucket(Bucket=BUCKET)
    except ClientError:
        s3.create_bucket(Bucket=BUCKET)
        print(f"Created bucket: {BUCKET}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingest-date", help="YYYY-MM-DD folder to upload (default: today, UTC)")
    args = parser.parse_args()
    ingest_date = args.ingest_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    local_dir = Path(f"data/raw/cfpb/full_load/ingest_date={ingest_date}")
    prefix = f"cfpb/full_load/ingest_date={ingest_date}"

    s3 = get_client()
    ensure_bucket(s3)

    # Multipart upload for big files, in 64 MB parts
    config = TransferConfig(multipart_threshold=64 * 1024 * 1024,
                            multipart_chunksize=64 * 1024 * 1024)

    for name in FILES:
        local_file = local_dir / name
        if not local_file.exists():
            sys.exit(f"Missing local file: {local_file}")
        key = f"{prefix}/{name}"
        print(f"Uploading {name} ...")
        s3.upload_file(str(local_file), BUCKET, key, Config=config)

        # Verify: the stored size must equal the local size
        stored = s3.head_object(Bucket=BUCKET, Key=key)["ContentLength"]
        local = local_file.stat().st_size
        status = "OK" if stored == local else "SIZE MISMATCH"
        print(f"  {BUCKET}/{key}  local={local}  stored={stored}  {status}")
        if stored != local:
            sys.exit("Upload verification failed.")

    print("Upload complete.")


if __name__ == "__main__":
    main()