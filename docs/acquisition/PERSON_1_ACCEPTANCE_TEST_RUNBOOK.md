# Person 1 Acquisition Acceptance Test Runbook

Use this runbook to review and verify the Data Acquisition and Raw Storage component on the copied project. Run commands from the repository root:

```text
C:\Users\User\Documents\Codex\2026-11-20\ca\project-to-work-on\banking-complaints-platform
```

This runbook covers the acquisition role only. It does not test Spark transformations, MongoDB collections, analytics or the dashboard.

## Handover summary - read this first

This copy contains the first project part: CFPB data acquisition and raw storage. It prepares the historical download/extraction flow and the incremental API flow. It does not finish the Spark, MongoDB, analysis or dashboard parts of the platform.

The historical CFPB snapshot is already present locally and in MinIO. Routine API runs do not download the large historical archive again. The incremental collector saves each complete API response as raw JSON, writes a manifest, uploads the run to MinIO, and then updates the checkpoint.

### Run the API manually in Git Bash

Open Git Bash in the copied project root and run:

```bash
./.venv/Scripts/python.exe ingestion/run_incremental.py --page-size 1000
```

This command runs once. It reads `data/raw/cfpb/api/high_water_mark.json`, starts at the saved date by default, and ends on the current UTC date. It uploads the raw run to MinIO and updates the checkpoint after upload. It does not keep running in the background. Keep MinIO available and keep the project's `.env` file in place; never share that file.

The same command from PowerShell is:

```powershell
.venv\Scripts\python.exe ingestion\run_incremental.py --page-size 1000
```

For the first run in a fresh copy, provide an explicit bounded date window because no checkpoint exists yet:

```bash
./.venv/Scripts/python.exe ingestion/run_incremental.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD --overlap-days 0 --page-size 1000
```

Keep the date range small. CFPB rejects windows above 100,000 results; split a large range into separate, smaller date-window commands. Each successful command creates one new `run_id` folder, even if its results match an earlier run.

### How the checkpoint works

The existing `high_water_mark.json` is the checkpoint; a second checkpoint file is not needed. It records the last successfully queried date, the API call's UTC date/time, the last run ID and the requested date window. The next normal command starts from that saved date. It updates the checkpoint only after the complete raw run uploads to MinIO.

CFPB's request filter accepts calendar dates, not an exact last-call time. A read-only request using a timestamp filter returned HTTP 400. Therefore, if you run twice on the same day, the API can return that calendar day's records again. The default overlap is zero to avoid refetching a whole week. Use `--overlap-days 7` only when you need a wider check for late arrivals; that can return many more repeated records and can exceed the API result limit. Spark must deduplicate curated records by `complaint_id`.

The checkpoint currently says the last successful call was `2026-11-20T16:40:36Z` (run `20261120T164036Z-1b10f06f`). That run succeeded for the November 20 date window and returned zero records. The prior successful November 19 runs returned 5,046 records across six pages each. In MinIO, the latest files are under `raw/cfpb/api/ingest_date=2026-11-20/run_id=20261120T164036Z-1b10f06f/`; the checkpoint is `raw/cfpb/api/high_water_mark.json`.

### What was completed in this first project part

- Streamed CFPB historical ZIP download, extraction/counting, manifests and checksums.
- Incremental CFPB API pagination, retries, bounded date windows, raw response preservation and run manifests.
- Checksum-verified MinIO uploads and the persisted high-water/checkpoint JSON.
- Offline tests for success, empty results, pagination, retries, failures, replay and safe upload behavior.
- Setup and handoff instructions in this runbook and the implementation guide.

The main component files are `ingestion/full_load.py` (historical ZIP download), `ingestion/extract_manifest.py` (stream extraction and counts), `ingestion/incremental_api.py` (API paging and local checkpoint), `ingestion/run_incremental.py` (API run, MinIO upload and checkpoint publication) and `ingestion/upload_raw.py` (MinIO uploads). `tests/test_acquisition.py` contains the offline checks.

The copied project has not been committed or pushed. The historical dataset was not downloaded again. No background collector is running, and no Windows scheduled task was registered. The Docker services, MongoDB, Spark processing and dashboard were not tested as part of this acquisition handoff.

## Safety rules

- Do not print or share `.env`; it contains local credentials.
- Do not repeat the full historical download just to test the scripts. The existing ZIP is about 366 MB and the extracted CSV about 5.8 GB.
- Do not use `--force` during routine verification. The uploader stops if an existing MinIO object does not match the local SHA-256 metadata.
- Never run `docker compose down -v`. That removes persistent MinIO and MongoDB volumes.
- A live incremental run contacts CFPB and writes a new local run plus objects in MinIO. Use a bounded date window.

## Current evidence already checked

- The copied project contains the historical partition `data/raw/cfpb/full_load/ingest_date=2026-11-12/`.
- Its manifest reports 19,112,942 records, ZIP size 366,121,838 bytes and CSV size 5,797,256,221 bytes.
- A read-only MinIO listing showed these existing objects in bucket `raw`:
  - `cfpb/full_load/ingest_date=2026-11-12/complaints.csv`
  - `cfpb/full_load/ingest_date=2026-11-12/complaints.csv.zip`
  - `cfpb/full_load/ingest_date=2026-11-12/manifest.json`
- MinIO contains the historical objects above and the successful API run prefixes listed below.
- The 19 offline acquisition tests passed.
- Successful November 19 runs: `20261120T153006Z-296fc3a9` and replay `20261120T153053Z-b0bcf8f9`; each returned 5,046 records in six pages. The local files and MinIO sizes/checksum metadata matched.
- Successful November 20 empty-result runs: `20261120T161834Z-315a1b39` and `20261120T164036Z-1b10f06f`; each returned zero records in one page. The latest run manifest and checkpoint were verified in MinIO.
- Incomplete local-only attempts have no success manifest and were not uploaded: `20261120T152327Z-bb47a618` (API pagination cursor issue), `20261120T161635Z-240d2b3e` (network proxy failure), and `20261120T161716Z-d8930603` (date window exceeded the 100,000-result API limit). Do not treat these folders as completed runs.

## 1. Check the Python environment

In Windows CMD:

```bat
cd /d C:\Users\User\Documents\Codex\2026-11-20\ca\project-to-work-on\banking-complaints-platform
.venv\Scripts\python.exe --version
.venv\Scripts\python.exe -m pip show requests boto3 python-dotenv
```

The project targets Python 3.10. `pytest` is pinned in `requirements.txt`, but the existing environment did not have it installed. The tests use Python's built-in `unittest`, so no package installation is needed to run the verified suite.

For a fresh clone in Windows CMD, create and install a new environment:

```bat
py -3.10 -m venv .venv
.venv\Scripts\activate
python -m pip install -r requirements.txt
```

For Git Bash, run from the project root:

```bash
py -3.10 -m venv .venv
./.venv/Scripts/python.exe -m pip install -r requirements.txt
test -e .env || cp .env.example .env
```

If `.env` is missing, copy `.env.example` to `.env` and fill in local values. Never overwrite an existing `.env` blindly.

## 2. Run all offline acquisition tests

```bat
.venv\Scripts\python.exe -m unittest discover -s tests -v
```

Expected result: **19 tests, `OK`**. These tests use temporary tiny archives and mocked CFPB/MinIO responses. They do not download the CFPB dataset, call the real API, or write to MinIO.

Coverage includes:

- Historical streaming, retry and no-overwrite behavior.
- ZIP extraction, CSV record counting, ingest-date handling and safe forced-extraction failure.
- API pagination/cursor handling, timeout and HTTP retries, successful empty responses, repeated windows, and a high-water mark that does not advance on incomplete pagination.
- Preserving a malformed raw API response before parsing fails.
- Checksum-aware MinIO rerun behavior using a fake S3 client.
- Parsing the synthetic sample at `tests/fixtures/sample_api_response.json`.

The intentional incomplete-pagination test sends one hit while reporting two and omits the next-page cursor. It must fail the run and leave the high-water mark unchanged. A timeout test initially exposed an error in the fake session; that test harness was corrected, and the complete suite passed.

## 3. Verify the existing historical files without re-ingesting

Use PowerShell from the repository root:

```powershell
$histDir = 'data\raw\cfpb\full_load\ingest_date=2026-11-12'
Get-Item "$histDir\complaints.csv.zip", "$histDir\complaints.csv", "$histDir\manifest.json" |
  Select-Object Name, Length
```

Expected sizes:

| File | Expected bytes |
|---|---:|
| `complaints.csv.zip` | 366121838 |
| `complaints.csv` | 5797256221 |
| `manifest.json` | 908 |

Read the manifest summary without printing file contents or credentials:

```bat
.venv\Scripts\python.exe -c "import json; p='data/raw/cfpb/full_load/ingest_date=2026-11-12/manifest.json'; m=json.load(open(p,encoding='utf-8')); print({k:m.get(k) for k in ('ingest_date','record_count','zip_size_bytes','zip_sha256','csv_size_bytes','outcome')})"
```

This confirms the existing recorded evidence. It does not recompute the checksum across the 5.8 GB CSV. If an independent ZIP checksum is needed, `Get-FileHash "$histDir\complaints.csv.zip" -Algorithm SHA256` reads the 366 MB ZIP once; compare the result with `zip_sha256` in the manifest. Do not rerun extraction just for this check.

## 4. Verify MinIO connectivity and existing raw objects

The commands below are read-only. They do not upload or overwrite objects.

```bat
curl.exe -fsS http://localhost:9000/minio/health/live
.venv\Scripts\python.exe -c "from ingestion.upload_raw import get_client, BUCKET; r=get_client().list_objects_v2(Bucket=BUCKET,Prefix='cfpb/',MaxKeys=100); print('bucket:',r.get('Name')); [print(o['Key'],o['Size']) for o in r.get('Contents',[])]"
```

Expected: MinIO health returns successfully and the existing historical keys listed above appear under `cfpb/full_load/`. If MinIO is stopped, first inspect the Compose configuration and service status. Starting only MinIO with `docker compose up -d minio` may start or recreate its container and attach the existing `minio_data` volume; it does not delete that volume. Do not start MongoDB for this acquisition check.

If `docker compose ps` reports a Docker permission/connection error but the health URL and S3 listing work, record that Docker CLI status could not be checked; do not reset services or volumes to fix the CLI issue.

## 5. Run one bounded live incremental collection and upload

This is the first step that contacts CFPB and writes new data. Confirm that MinIO is reachable and `.env` is configured first. In PowerShell, run a bounded one-day window to avoid the CFPB 100,000-result limit:

```powershell
$endDate = [DateTime]::UtcNow.ToString('yyyy-MM-dd')
$startDate = $endDate
.venv\Scripts\python.exe ingestion\run_incremental.py --start-date $startDate --end-date $endDate --overlap-days 0 --page-size 1000
```

Expected on success:

1. The console prints a unique run ID and local run directory.
2. A manifest and one or more `response_00001.json` files appear under `data/raw/cfpb/api/ingest_date=<UTC date>/run_id=<run ID>/`.
3. The manifest records the date window, page count, record count, per-page request parameters, HTTP status, retry count, size and SHA-256.
4. The same run objects appear in MinIO under `raw/cfpb/api/ingest_date=<UTC date>/run_id=<run ID>/`.
5. `data/raw/cfpb/api/high_water_mark.json` and `raw/cfpb/api/high_water_mark.json` contain the successful end date.

The collector requests all products. It does not filter to student loans. The exact CFPB response count can change over time; verify that the run is complete and the manifest's `record_count` equals the sum of its per-page counts. If `page_count` is 1, pagination was not exercised by this live window; the mocked multi-page test remains the pagination test evidence. A successful response with `record_count: 0` is still a valid empty run; it means CFPB returned no records for that date window at that time.

To inspect a local manifest after copying the run ID from the console:

```powershell
$runId = 'PASTE_THE_PRINTED_RUN_ID_HERE'
$runDir = "data\raw\cfpb\api\ingest_date=$endDate\run_id=$runId"
.venv\Scripts\python.exe -c "import json,sys; m=json.load(open(sys.argv[1],encoding='utf-8')); print({k:m.get(k) for k in ('run_id','requested_date_min','requested_date_max_inclusive','page_count','record_count','api_total','outcome')})" "$runDir\manifest.json"
```

## 6. Test replay and the overlap

Replay the same bounded dates once. This creates a second immutable raw run with a different run ID. The existing run files are not replaced.

```powershell
.venv\Scripts\python.exe ingestion\run_incremental.py --start-date $startDate --end-date $endDate --overlap-days 0 --page-size 1000
```

Check that both run IDs have separate local and MinIO prefixes. The saved high-water date is the next run checkpoint. The date boundary is inclusive, so that one calendar day can be fetched again. This tests raw replay safety; the Spark teammate must test that deduplication by `complaint_id` prevents curated counts and dashboard measures from inflating. Do not present raw duplicate runs as proof of curated replay safety.

For the normal rolling invocation after a successful first run:

```powershell
.venv\Scripts\python.exe ingestion\run_incremental.py --overlap-days 0 --page-size 1000
```

It starts on the local high-water date by default and ends today UTC. Use `--overlap-days 7` only when you want to recheck a wider window for late arrivals; the wider query may exceed the API's 100,000-result limit and create more repeated raw records.

## 7. Verify MinIO objects and upload idempotency

The `run_incremental.py` success message confirms that the run and state upload completed. To list the API prefix without showing credentials:

```bat
.venv\Scripts\python.exe -c "from ingestion.upload_raw import get_client, BUCKET; r=get_client().list_objects_v2(Bucket=BUCKET,Prefix='cfpb/api/',MaxKeys=1000); [print(o['Key'],o['Size']) for o in r.get('Contents',[])]"
```

The uploader verifies each object's byte size and SHA-256 metadata after upload. Re-running the uploader for a key whose size and checksum match should print that it is already present and skip it. If a pre-existing object has no checksum metadata or differs, the uploader stops. Do not add `--force` unless a reviewer has confirmed that replacing that exact key is intended. The high-water mark is deliberately updated after a complete uploaded run; run folders remain immutable.

## 8. Historical full-load reproduction (optional, not routine)

The copied project already has the verified historical partition, so do not run this section for normal acceptance. A fresh-clone reproduction downloads about 366 MB and extracts a CSV of about 5.8 GB. Check free disk space first and run it only once in a clean environment or a new unused ingest-date partition:

```bat
.venv\Scripts\python.exe ingestion\full_load.py --ingest-date YYYY-MM-DD
.venv\Scripts\python.exe ingestion\extract_manifest.py --ingest-date YYYY-MM-DD
.venv\Scripts\python.exe ingestion\upload_raw.py --ingest-date YYYY-MM-DD
```

Replace `YYYY-MM-DD` with a real, unused UTC date. Expected evidence is the ZIP, extracted CSV and manifest locally; matching keys in MinIO; a successful HTTP status; nonzero file sizes/checksums; and a CSV logical-record count in the manifest. Never use this step just to rerun unit tests.

## 9. Schedule test

No Windows scheduled task is currently registered. To set one up later, the team can configure Windows Task Scheduler to run every 12 or 24 hours:

- Program: `<project>\.venv\Scripts\python.exe`
- Arguments: `ingestion\run_incremental.py --overlap-days 0 --page-size 1000`
- Start in: the project root
- Account: the same Windows account that can read `.env` and reach MinIO

The PDF allows a manual trigger of this same collector during the live demonstration. Record the configured interval or manual trigger in the handoff notes. Do not create a second collector command with different logic.

## Pass/fail checklist for handoff

- [ ] Python version and required packages recorded; `.env` kept private.
- [x] Offline test suite reports 19 passing tests.
- [ ] Existing historical manifest, file sizes and MinIO keys checked without re-ingestion.
- [x] Bounded live CFPB run completes; manifest and raw responses are present locally.
- [x] Live run and high-water state are present in MinIO with matching byte sizes/checksum metadata.
- [x] Replay creates a separate run ID and does not move the high-water mark backward.
- [x] Live pagination returned six pages; replay returned six pages as well.
- [x] Manual trigger is documented and has been demonstrated; no background task is registered.
- [x] No volume reset, secret exposure, full dataset rerun, commit or push occurred.
- [ ] Spark owner separately demonstrates bad-record quality checks and replay-safe curated counts.

## Results recorded for this copy

- Offline: `python -m unittest discover -s tests -v` - 19 tests passed.
- Live window 2026-11-19: six pages and 5,046 records per run, with a successful replay into a separate run ID.
- Live window 2026-11-20: two successful one-page runs, each with zero records.
- First successful run: `20261120T153006Z-296fc3a9`; MinIO object sizes and SHA-256 metadata matched local files.
- Replay: `20261120T153053Z-b0bcf8f9`; separate run prefix, six pages, 5,046 records.
- Latest checkpoint: `2026-11-20T16:40:36Z`; run `20261120T164036Z-1b10f06f`; local and MinIO manifest/checkpoint sizes and checksums match.
- The incomplete local-only attempts are listed above. They have no manifest and are not part of the successful Silo data.
- Live runs only wrote new API run prefixes and the acquisition high-water state; the historical partition was not re-ingested. Docker and MongoDB were not tested.

## Record results for the next person

```text
Tester / date:
Python version:
Offline test result:
Historical manifest count and sizes:
MinIO health and existing-key result:
Incremental run ID:
Incremental window / pages / records:
Local manifest path:
MinIO run prefix:
High-water date:
Replay run ID / result:
Schedule interval or demo trigger:
Failures and recovery:
```

## Scope boundary

This runbook verifies acquisition and raw storage. It cannot certify the entire platform. The project's Spark bad-date/duplicate-record checks, MongoDB upserts/indexes, curated reconciliation, dashboard behavior and end-to-end replay counts must be tested with their respective components before the team claims the whole platform is complete.
