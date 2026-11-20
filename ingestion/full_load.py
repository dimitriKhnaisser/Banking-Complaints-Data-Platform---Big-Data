"""Stream the complete CFPB historical ZIP to a date-partitioned raw folder."""
import argparse
import hashlib
import json
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

URL = "https://files.consumerfinance.gov/ccdb/complaints.csv.zip"
CHUNK_SIZE = 1024 * 1024
MAX_ATTEMPTS = 3
CONNECT_TIMEOUT = 15
READ_TIMEOUT = 120
RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504}


def download(output_dir: Path, force: bool = False, session=requests) -> dict:
    """Download atomically; existing complete archives are preserved by default."""
    output_dir.mkdir(parents=True, exist_ok=True)
    final_path = output_dir / "complaints.csv.zip"
    metadata_path = output_dir / "download_metadata.json"
    if final_path.exists() and not force:
        raise FileExistsError(f"Archive already exists; use --force to replace it: {final_path}")

    last_error = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        part_path = output_dir / "complaints.csv.zip.part"
        sha256 = hashlib.sha256()
        size = 0
        status = None
        try:
            with session.get(URL, stream=True, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)) as response:
                status = response.status_code
                if status in RETRYABLE_STATUS:
                    response.raise_for_status()
                response.raise_for_status()
                with part_path.open("wb") as stream:
                    for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk:
                            stream.write(chunk)
                            sha256.update(chunk)
                            size += len(chunk)
                    stream.flush()
                    os.fsync(stream.fileno())
            if size == 0:
                raise requests.RequestException("The server returned an empty archive")
            downloaded_at = datetime.now(timezone.utc).isoformat()
            metadata = {
                "source_url": URL,
                "http_status": status,
                "attempt_count": attempt,
                "retry_count": attempt - 1,
                "downloaded_at_utc": downloaded_at,
                "size_bytes": size,
                "sha256": sha256.hexdigest(),
            }
            # Keep the previous final archive intact until the new transfer is complete.
            os.replace(part_path, final_path)
            temp_metadata = metadata_path.with_suffix(".json.part")
            temp_metadata.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
            os.replace(temp_metadata, metadata_path)
            return metadata
        except requests.HTTPError as exc:
            last_error = exc
            response_status = getattr(exc.response, "status_code", status)
            if response_status not in RETRYABLE_STATUS or attempt == MAX_ATTEMPTS:
                break
        except (requests.Timeout, requests.ConnectionError, requests.RequestException, OSError) as exc:
            last_error = exc
            if attempt == MAX_ATTEMPTS:
                break
        finally:
            part_path.unlink(missing_ok=True)
        delay = min(2 ** (attempt - 1), 8) + random.uniform(0, 0.25)
        time.sleep(delay)
    raise RuntimeError(f"Download failed after {attempt} attempt(s): {last_error}") from last_error


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ingest-date", help="Partition date YYYY-MM-DD (default: today UTC)")
    parser.add_argument("--force", action="store_true", help="Replace an existing archive after a successful download")
    args = parser.parse_args(argv)
    ingest_date = args.ingest_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    output_dir = Path("data/raw/cfpb/full_load") / f"ingest_date={ingest_date}"
    if (output_dir / "complaints.csv.zip").exists() and not args.force:
        print(f"Archive already exists; preserving it: {output_dir / 'complaints.csv.zip'}")
        print("Use --force to download a replacement.")
        return
    info = download(output_dir, force=args.force)
    print(f"Saved to: {output_dir / 'complaints.csv.zip'}")
    print(f"HTTP status: {info['http_status']}; retries: {info['retry_count']}")
    print(f"Size: {info['size_bytes']} bytes; SHA-256: {info['sha256']}")


if __name__ == "__main__":
    main()
