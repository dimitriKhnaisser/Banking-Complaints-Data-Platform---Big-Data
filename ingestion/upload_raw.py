"""Piece C: upload the raw full-load files to MinIO (S3-compatible storage)."""
import argparse
import hashlib
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

FILES = ["complaints.csv.zip", "complaints.csv", "manifest.json"]


def get_client():
    access_key = os.getenv("MINIO_ROOT_USER")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD")
    if not access_key or not secret_key:
        raise RuntimeError("Set MINIO_ROOT_USER and MINIO_ROOT_PASSWORD in the local .env file")
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),  # needed for local S3 stores
    )


def ensure_bucket(s3):
    try:
        s3.head_bucket(Bucket=BUCKET)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status != 404 and code not in {"404", "NoSuchBucket", "NotFound"}:
            raise
        s3.create_bucket(Bucket=BUCKET)
        print(f"Created bucket: {BUCKET}")


def sha256_of(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def upload_files(local_files, prefix, force=False):
    s3 = get_client()
    ensure_bucket(s3)
    config = TransferConfig(multipart_threshold=64 * 1024 * 1024,
                            multipart_chunksize=64 * 1024 * 1024,
                            max_concurrency=2)
    for local_file, name in local_files:
        if not local_file.exists():
            sys.exit(f"Missing local file: {local_file}")
        key = f"{prefix}/{name}"
        local_hash = sha256_of(local_file)
        try:
            existing = s3.head_object(Bucket=BUCKET, Key=key)
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            status_code = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status_code != 404 and code not in {"404", "NoSuchKey", "NotFound"}:
                raise
            existing = None
        if existing and existing.get("ContentLength") == local_file.stat().st_size and existing.get("Metadata", {}).get("sha256") == local_hash:
            print(f"Already present and verified; skipping {BUCKET}/{key}")
            continue
        if existing and not force:
            raise RuntimeError(
                f"Object already exists and does not have a matching SHA-256: {BUCKET}/{key}. "
                "Use --force only if replacing that object is intended."
            )
        print(f"Uploading {name} ...")
        s3.upload_file(str(local_file), BUCKET, key, ExtraArgs={"Metadata": {"sha256": local_hash}}, Config=config)
        uploaded = s3.head_object(Bucket=BUCKET, Key=key)
        stored = uploaded["ContentLength"]
        local = local_file.stat().st_size
        verified_hash = uploaded.get("Metadata", {}).get("sha256") == local_hash
        status = "OK" if stored == local and verified_hash else "MISMATCH"
        print(f"  {BUCKET}/{key}  local={local}  stored={stored}  {status}")
        if stored != local or not verified_hash:
            sys.exit("Upload size/checksum metadata verification failed.")


def upload_incremental_run(ingest_date, run_id, force=False):
    local_dir = Path(f"data/raw/cfpb/api/ingest_date={ingest_date}/run_id={run_id}")
    if not local_dir.is_dir():
        raise FileNotFoundError(f"Incremental run directory not found: {local_dir}")
    files = [(path, path.relative_to(local_dir).as_posix())
             for path in sorted(local_dir.rglob("*")) if path.is_file()]
    if not any(name == "manifest.json" for _, name in files):
        raise FileNotFoundError(f"Incremental run has no manifest: {local_dir}")
    upload_files(files, f"cfpb/api/ingest_date={ingest_date}/run_id={run_id}", force=force)


def upload_high_water_mark(force=False):
    path = Path("data/raw/cfpb/api/high_water_mark.json")
    if path.exists():
        upload_files([(path, "high_water_mark.json")], "cfpb/api", force=force)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingest-date", help="YYYY-MM-DD folder to upload (default: today, UTC)")
    parser.add_argument("--source", choices=("full", "incremental"), default="full")
    parser.add_argument("--run-id", help="Required for --source incremental")
    parser.add_argument("--force", action="store_true", help="Replace existing objects whose SHA-256 differs")
    args = parser.parse_args()
    ingest_date = args.ingest_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    if args.source == "full":
        local_dir = Path(f"data/raw/cfpb/full_load/ingest_date={ingest_date}")
        prefix = f"cfpb/full_load/ingest_date={ingest_date}"
        local_files = [(local_dir / name, name) for name in FILES]
        metadata_path = local_dir / "download_metadata.json"
        if metadata_path.exists():
            local_files.append((metadata_path, metadata_path.name))
    else:
        if not args.run_id:
            parser.error("--run-id is required with --source incremental")
        local_dir = Path(f"data/raw/cfpb/api/ingest_date={ingest_date}/run_id={args.run_id}")
        prefix = f"cfpb/api/ingest_date={ingest_date}/run_id={args.run_id}"
        if not local_dir.is_dir():
            sys.exit(f"Incremental run directory not found: {local_dir}")
        local_files = [(path, path.relative_to(local_dir).as_posix()) for path in sorted(local_dir.rglob("*")) if path.is_file()]
        if not any(key == "manifest.json" for _, key in local_files):
            sys.exit(f"Incremental run has no manifest: {local_dir}")

    upload_files(local_files, prefix, force=args.force)
    print("Upload complete.")


if __name__ == "__main__":
    main()
