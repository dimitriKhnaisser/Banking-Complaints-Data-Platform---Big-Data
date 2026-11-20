"""Extract the historical CFPB CSV as a stream and write its manifest."""
import argparse
import csv
import hashlib
import json
import os
import sys
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path

URL = "https://files.consumerfinance.gov/ccdb/complaints.csv.zip"
CHUNK_SIZE = 1024 * 1024
csv.field_size_limit(2**31 - 1)


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(CHUNK_SIZE), b""):
            digest.update(chunk)
    return digest.hexdigest()


def extract(zip_path: Path, folder: Path, ingest_date: str, force: bool = False) -> dict:
    csv_path = folder / "complaints.csv"
    manifest_path = folder / "manifest.json"
    if not zip_path.exists():
        raise FileNotFoundError(f"ZIP not found: {zip_path}. Run full_load.py first.")
    if not force and (csv_path.exists() or manifest_path.exists()):
        raise FileExistsError(f"Extracted output already exists in {folder}; use --force to replace it")

    part_path = folder / "complaints.csv.part"
    try:
        with zipfile.ZipFile(zip_path) as archive:
            csv_members = [name for name in archive.namelist() if name.lower().endswith(".csv")]
            if not csv_members:
                raise ValueError("No CSV found inside the ZIP")
            with archive.open(csv_members[0]) as source, part_path.open("wb") as target:
                while chunk := source.read(CHUNK_SIZE):
                    target.write(chunk)
                target.flush()
                os.fsync(target.fileno())
        with part_path.open(newline="", encoding="utf-8-sig") as stream:
            reader = csv.reader(stream)
            header = next(reader, None)
            if header is None:
                raise ValueError("Extracted CSV is empty")
            record_count = sum(1 for _ in reader)
        extracted_size = part_path.stat().st_size
        extracted_sha256 = sha256_of(part_path)
    except Exception:
        part_path.unlink(missing_ok=True)
        raise

    download_metadata_path = folder / "download_metadata.json"
    download_metadata = json.loads(download_metadata_path.read_text(encoding="utf-8")) if download_metadata_path.exists() else {}
    manifest = {
        "run_id": str(uuid.uuid4()),
        "source_url": URL,
        "ingest_date": ingest_date,
        "downloaded_at_utc": download_metadata.get("downloaded_at_utc"),
        "http_status": download_metadata.get("http_status"),
        "download_attempt_count": download_metadata.get("attempt_count"),
        "download_retry_count": download_metadata.get("retry_count"),
        "manifest_created_at_utc": datetime.now(timezone.utc).isoformat(),
        "zip_file": zip_path.name,
        "zip_size_bytes": zip_path.stat().st_size,
        "zip_sha256": sha256_of(zip_path),
        "csv_file": csv_path.name,
        "csv_size_bytes": extracted_size,
        "csv_sha256": extracted_sha256,
        "record_count": record_count,
        "columns": header,
        "outcome": "success",
    }
    temp_manifest = manifest_path.with_suffix(".json.part")
    temp_manifest.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    os.replace(part_path, csv_path)
    os.replace(temp_manifest, manifest_path)
    return manifest


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ingest-date", help="Partition date YYYY-MM-DD (default: today UTC)")
    parser.add_argument("--force", action="store_true", help="Replace extracted CSV and manifest")
    args = parser.parse_args(argv)
    ingest_date = args.ingest_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    folder = Path("data/raw/cfpb/full_load") / f"ingest_date={ingest_date}"
    try:
        manifest = extract(folder / "complaints.csv.zip", folder, ingest_date, force=args.force)
    except (OSError, ValueError, zipfile.BadZipFile) as exc:
        sys.exit(str(exc))
    print(f"Records: {manifest['record_count']}")
    print(f"Manifest saved to {folder / 'manifest.json'}")


if __name__ == "__main__":
    main()
