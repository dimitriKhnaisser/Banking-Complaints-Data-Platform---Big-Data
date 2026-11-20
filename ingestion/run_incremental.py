"""Collect one incremental window, upload it, then publish the high-water mark."""
import argparse
import sys
from datetime import date, datetime, timedelta, timezone

if __package__:
    from . import incremental_api, upload_raw
else:  # Support the documented `python ingestion\run_incremental.py` command.
    import incremental_api
    import upload_raw


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", help="Inclusive YYYY-MM-DD; required for the first run")
    parser.add_argument("--end-date", default=datetime.now(timezone.utc).date().isoformat(),
                        help="Inclusive YYYY-MM-DD (default: today UTC)")
    parser.add_argument("--overlap-days", type=int, default=0,
                        help="Replay this many days before the saved date (default: 0; the saved date is included)")
    parser.add_argument("--page-size", type=int, default=incremental_api.DEFAULT_PAGE_SIZE)
    parser.add_argument("--force", action="store_true", help="Replace existing MinIO objects with different checksums")
    args = parser.parse_args(argv)
    try:
        end_date = date.fromisoformat(args.end_date)
        mark = incremental_api.load_high_water_mark()
        if args.start_date:
            start_date = date.fromisoformat(args.start_date)
        elif mark:
            start_date = mark - timedelta(days=args.overlap_days)
        else:
            raise ValueError("First run requires --start-date")

        manifest, run_dir = incremental_api.collect(
            start_date, end_date, args.overlap_days, args.page_size, advance_watermark=False
        )
        print(f"Collected run {manifest['run_id']} to {run_dir}")
        # Publish the complete raw run first; only then mark its end date as successful.
        upload_raw.upload_incremental_run(manifest["ingest_date"], manifest["run_id"], force=args.force)
        state_path = incremental_api.advance_high_water_mark(
            manifest, run_dir, args.overlap_days
        )
        # This single state object is intentionally replaced after the immutable run upload.
        upload_raw.upload_high_water_mark(force=True)
        print(f"Uploaded raw run and high-water mark ({state_path})")
    except (ValueError, OSError, RuntimeError) as exc:
        sys.exit(str(exc))


if __name__ == "__main__":
    main()
