"""Piece B: extract the CSV from the ZIP, count records, write manifest.json."""
import csv
import hashlib
import json
import sys
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path

URL = "https://files.consumerfinance.gov/ccdb/complaints.csv.zip"
csv.field_size_limit(2**31 - 1)  # large limit that also works on Windows

def sha256_of(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def main():
    ingest_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    folder = Path(f"data/raw/cfpb/full_load/ingest_date={ingest_date}")
    zip_path = folder / "complaints.csv.zip"
    csv_path = folder / "complaints.csv"

    if not zip_path.exists():
        sys.exit(f"ZIP not found: {zip_path}. Run full_load.py first.")

    # 1. Extract (the ZIP itself is only read, never modified)
    print("Extracting CSV...")
    with zipfile.ZipFile(zip_path) as z:
        names = [n for n in z.namelist() if n.lower().endswith(".csv")]
        if not names:
            sys.exit("No CSV found inside the ZIP.")
        with z.open(names[0]) as src, open(csv_path, "wb") as dst:
            while chunk := src.read(1024 * 1024):
                dst.write(chunk)

    # 2. Count records with the csv module (safe if a field contains a newline)
    print("Counting records...")
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        record_count = sum(1 for _ in reader)

    # 3. Manifest
    manifest = {
        "run_id": str(uuid.uuid4()),
        "source_url": URL,
        "ingest_date": ingest_date,
        "file_downloaded_at_utc": datetime.fromtimestamp(
            zip_path.stat().st_mtime, tz=timezone.utc
        ).isoformat(),
        "manifest_created_at_utc": datetime.now(timezone.utc).isoformat(),
        "zip_file": zip_path.name,
        "zip_size_bytes": zip_path.stat().st_size,
        "zip_sha256": sha256_of(zip_path),
        "csv_file": csv_path.name,
        "csv_size_bytes": csv_path.stat().st_size,
        "record_count": record_count,
        "columns": header,
        "outcome": "success",
    }
    with open(folder / "manifest.json", "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    print(f"Records: {record_count}")
    print(f"Columns ({len(header)}): {header}")
    print(f"Manifest saved to {folder / 'manifest.json'}")


if __name__ == "__main__":
    main()