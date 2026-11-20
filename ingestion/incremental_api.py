"""Collect a bounded CFPB API date window into immutable raw JSON pages."""
import argparse
import hashlib
import json
import os
import random
import sys
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests

API_URL = "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/"
ROOT = Path("data/raw/cfpb/api")
STATE_PATH = ROOT / "high_water_mark.json"
MAX_ATTEMPTS = 3
MAX_FROM = 100_000
DEFAULT_PAGE_SIZE = 1000
MAX_PAGE_SIZE = 1000
RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504}


def get_page(session, params):
    last_error = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            with session.get(API_URL, params=params, timeout=(15, 60)) as response:
                response.raise_for_status()
                return response, attempt
        except requests.HTTPError as exc:
            last_error = exc
            status = getattr(exc.response, "status_code", None)
            if status not in RETRYABLE_STATUS or attempt == MAX_ATTEMPTS:
                break
        except (requests.Timeout, requests.ConnectionError, requests.RequestException, ValueError) as exc:
            last_error = exc
            if attempt == MAX_ATTEMPTS:
                break
        time.sleep(min(2 ** (attempt - 1), 8) + random.uniform(0, 0.25))
    raise RuntimeError(f"API request failed after {attempt} attempt(s): {last_error}") from last_error


def extract_hits(payload):
    try:
        return payload["hits"]["hits"], int(payload["hits"]["total"]["value"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("Unexpected CFPB API JSON shape; expected hits.hits and hits.total.value") from exc


def next_cursor(payload, next_page):
    breakpoints = payload.get("_meta", {}).get("break_points", {})
    point = breakpoints.get(str(next_page))
    if point and len(point) >= 2:
        return f"{point[0]}_{point[1]}"
    # Some CFPB responses omit break_points for larger page sizes but include
    # the OpenSearch sort values on every hit. The last hit is a valid cursor.
    hits = payload.get("hits", {}).get("hits", [])
    sort_values = hits[-1].get("sort") if hits else None
    if sort_values and len(sort_values) >= 2:
        return "_".join(map(str, sort_values))
    return None


def load_high_water_mark(path=STATE_PATH):
    if not path.exists():
        return None
    value = json.loads(path.read_text(encoding="utf-8"))
    return date.fromisoformat(value["high_water_mark"])


def write_json_atomic(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".part")
    temp.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    os.replace(temp, path)


def collect(start_date: date, end_date: date, overlap_days=0, page_size=1000, session=requests, root=ROOT,
            advance_watermark=True):
    """Collect inclusive date bounds; state advances only after every page is saved."""
    if end_date < start_date:
        raise ValueError("end-date must be on or after start-date")
    if not 1 <= page_size <= MAX_PAGE_SIZE:
        raise ValueError(f"page-size must be between 1 and {MAX_PAGE_SIZE}; CFPB cursor metadata is returned up to this size")
    if overlap_days < 0:
        raise ValueError("overlap-days cannot be negative")

    started_at = datetime.now(timezone.utc)
    run_id = f"{started_at.strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"
    run_dir = root / f"ingest_date={started_at.date().isoformat()}" / f"run_id={run_id}"
    run_dir.mkdir(parents=True, exist_ok=False)
    page_records = []
    page_number = 1
    cursor = None
    frm = 0
    total_expected = None

    while True:
        params = {
            "date_received_min": start_date.isoformat(),
            "date_received_max": (end_date + timedelta(days=1)).isoformat(),
            "size": page_size,
            "frm": frm,
            "page": page_number,
            "sort": "created_date_desc",
            "no_aggs": "true",
            "no_highlight": "true",
        }
        if cursor:
            params["search_after"] = cursor
        response, attempts = get_page(session, params)
        body = response.content
        page_path = run_dir / f"response_{page_number:05d}.json"
        page_part = page_path.with_suffix(".json.part")
        page_part.write_bytes(body)
        os.replace(page_part, page_path)
        # Preserve the exact response body before parsing or applying pagination logic.
        payload = response.json()
        hits, total = extract_hits(payload)
        if total_expected is None:
            total_expected = total
            if total > MAX_FROM:
                raise RuntimeError(
                    f"API window contains {total} records, above the {MAX_FROM} offset limit; "
                    "split the date window before collecting"
                )
        elif total != total_expected:
            raise RuntimeError(f"API total changed while paging ({total_expected} to {total}); rerun this date window")
        page_records.append({
            "file": page_path.name,
            "request_params": params,
            "http_status": response.status_code,
            "attempt_count": attempts,
            "retry_count": attempts - 1,
            "size_bytes": len(body),
            "sha256": hashlib.sha256(body).hexdigest(),
            "record_count": len(hits),
        })

        seen = sum(item["record_count"] for item in page_records)
        if seen >= total or not hits:
            break
        if frm + page_size >= MAX_FROM:
            raise RuntimeError("CFPB API offset limit reached before all results were retrieved; split the date range")
        next_page = page_number + 1
        cursor = next_cursor(payload, next_page)
        if cursor is None:
            raise RuntimeError(f"API omitted _meta.break_points[{next_page}]; refusing to advance an incomplete run")
        page_number = next_page
        frm += page_size

    completed_at = datetime.now(timezone.utc).isoformat()
    manifest = {
        "run_id": run_id,
        "source_url": API_URL,
        "ingest_type": "incremental_api",
        "ingest_date": started_at.date().isoformat(),
        "requested_date_min": start_date.isoformat(),
        "requested_date_max_inclusive": end_date.isoformat(),
        "api_date_received_max_exclusive": (end_date + timedelta(days=1)).isoformat(),
        "started_at_utc": started_at.isoformat(),
        "completed_at_utc": completed_at,
        "page_size": page_size,
        "page_count": len(page_records),
        "record_count": sum(item["record_count"] for item in page_records),
        "api_total": total_expected or 0,
        "pages": page_records,
        "outcome": "success",
    }
    write_json_atomic(run_dir / "manifest.json", manifest)

    if advance_watermark:
        advance_high_water_mark(manifest, run_dir, overlap_days, root)
    return manifest, run_dir


def advance_high_water_mark(manifest, run_dir, overlap_days=0, root=ROOT):
    state_path = root / "high_water_mark.json"
    previous = load_high_water_mark(state_path)
    end_date = date.fromisoformat(manifest["requested_date_max_inclusive"])
    new_mark = max(previous, end_date) if previous else end_date
    write_json_atomic(state_path, {
        "high_water_mark": new_mark.isoformat(),
        "last_api_call_date_utc": manifest["started_at_utc"][:10],
        "last_api_call_at_utc": manifest["started_at_utc"],
        "last_requested_date_min": manifest["requested_date_min"],
        "last_requested_date_max_inclusive": manifest["requested_date_max_inclusive"],
        "updated_at_utc": manifest["completed_at_utc"],
        "last_successful_run_id": manifest["run_id"],
        "last_successful_run_manifest": str((run_dir / "manifest.json").as_posix()),
        "overlap_days": overlap_days,
    })
    return state_path


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", help="Inclusive YYYY-MM-DD; required for first run")
    parser.add_argument("--end-date", default=datetime.now(timezone.utc).date().isoformat(), help="Inclusive YYYY-MM-DD (default: today UTC)")
    parser.add_argument("--overlap-days", type=int, default=0,
                        help="Replay this many days before previous high-water mark (default: 0; date boundary is inclusive)")
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    args = parser.parse_args(argv)
    try:
        end_date = date.fromisoformat(args.end_date)
        high_water = load_high_water_mark()
        if args.start_date:
            start_date = date.fromisoformat(args.start_date)
        elif high_water:
            start_date = high_water - timedelta(days=args.overlap_days)
        else:
            raise ValueError("First run requires --start-date")
        manifest, run_dir = collect(start_date, end_date, args.overlap_days, args.page_size)
    except (ValueError, OSError, RuntimeError, requests.RequestException) as exc:
        sys.exit(str(exc))
    print(f"Run {manifest['run_id']} saved to {run_dir}")
    print(f"Records: {manifest['record_count']} in {manifest['page_count']} page(s)")
    print(f"High-water mark: {manifest['requested_date_max_inclusive']}")


if __name__ == "__main__":
    main()
