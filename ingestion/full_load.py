"""Piece A: download the full CFPB complaints ZIP, then report size and checksum."""
import hashlib
from datetime import datetime, timezone
from pathlib import Path

import requests

URL = "https://files.consumerfinance.gov/ccdb/complaints.csv.zip"
TIMEOUT = 60  # seconds to wait for the server


def main():
    ingest_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_dir = Path(f"data/raw/cfpb/full_load/ingest_date={ingest_date}")
    out_dir.mkdir(parents=True, exist_ok=True)
    final_path = out_dir / "complaints.csv.zip"
    temp_path = out_dir / "complaints.csv.zip.part"

    sha256 = hashlib.sha256()
    size = 0

    print(f"Downloading {URL}")
    with requests.get(URL, stream=True, timeout=TIMEOUT) as response:
        response.raise_for_status()  # stops on HTTP errors (404, 500...)
        print("HTTP status:", response.status_code)
        with open(temp_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):  # 1 MB
                f.write(chunk)
                sha256.update(chunk)
                size += len(chunk)
                print(f"\r{size / 1_000_000:.0f} MB downloaded", end="", flush=True)

    temp_path.replace(final_path)  # replace() also works when the target exists (Windows)
    print(f"\nSaved to: {final_path}")
    print(f"Size: {size} bytes")
    print(f"SHA-256: {sha256.hexdigest()}")


if __name__ == "__main__":
    main()