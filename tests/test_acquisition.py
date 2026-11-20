import json
import subprocess
import sys
import tempfile
import unittest
import zipfile
from datetime import date
from pathlib import Path
from unittest.mock import patch

from botocore.exceptions import ClientError
import requests

from ingestion import extract_manifest, full_load, incremental_api, run_incremental, upload_raw


class FakeResponse:
    def __init__(self, payload=None, body=b"archive", status=200, json_error=None):
        self.status_code = status
        self.content = body
        self._payload = payload or {}
        self._json_error = json_error

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests
            error = requests.HTTPError(f"HTTP {self.status_code}")
            error.response = self
            raise error

    def iter_content(self, chunk_size):
        yield self.content

    def json(self):
        if self._json_error:
            raise self._json_error
        return self._payload


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response


class FakeS3:
    def __init__(self):
        self.objects = {}
        self.upload_count = 0

    def head_bucket(self, **_kwargs):
        return None

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}, "ResponseMetadata": {"HTTPStatusCode": 404}}, "HeadObject")
        return self.objects[Key]

    def upload_file(self, filename, _bucket, key, ExtraArgs, Config):
        self.upload_count += 1
        self.objects[key] = {
            "ContentLength": Path(filename).stat().st_size,
            "Metadata": ExtraArgs["Metadata"],
        }


def payload(hits, total, breakpoints=None):
    return {"hits": {"hits": hits, "total": {"value": total}}, "_meta": {"break_points": breakpoints or {}}}


class AcquisitionTests(unittest.TestCase):
    def test_documented_incremental_runner_command_starts(self):
        repo_root = Path(__file__).resolve().parents[1]
        script = repo_root / "ingestion" / "run_incremental.py"
        result = subprocess.run(
            [sys.executable, str(script), "--help"], cwd=repo_root,
            capture_output=True, text=True, check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("--start-date", result.stdout)

    def test_incremental_runner_starts_from_saved_date_by_default(self):
        manifest = {
            "run_id": "test-run",
            "ingest_date": "2026-11-20",
            "requested_date_min": "2026-11-20",
            "requested_date_max_inclusive": "2026-11-20",
        }
        with patch.object(incremental_api, "load_high_water_mark", return_value=date(2026, 11, 20)), \
                patch.object(incremental_api, "collect", return_value=(manifest, Path("run-dir"))) as collect, \
                patch.object(upload_raw, "upload_incremental_run"), \
                patch.object(incremental_api, "advance_high_water_mark", return_value=Path("state.json")), \
                patch.object(upload_raw, "upload_high_water_mark"):
            run_incremental.main(["--end-date", "2026-11-20"])

        self.assertEqual(collect.call_args.args[:3], (date(2026, 11, 20), date(2026, 11, 20), 0))
        self.assertEqual(collect.call_args.kwargs, {"advance_watermark": False})

    def test_incremental_runner_does_not_advance_checkpoint_when_upload_fails(self):
        manifest = {"run_id": "test-run", "ingest_date": "2026-11-20"}
        with patch.object(incremental_api, "load_high_water_mark", return_value=date(2026, 11, 20)), \
                patch.object(incremental_api, "collect", return_value=(manifest, Path("run-dir"))), \
                patch.object(upload_raw, "upload_incremental_run", side_effect=RuntimeError("upload failed")), \
                patch.object(incremental_api, "advance_high_water_mark") as advance, \
                patch.object(upload_raw, "upload_high_water_mark") as upload_state:
            with self.assertRaises(SystemExit):
                run_incremental.main(["--end-date", "2026-11-20"])

        advance.assert_not_called()
        upload_state.assert_not_called()

    def test_historical_download_is_streamed_and_records_metadata(self):
        with tempfile.TemporaryDirectory() as temporary:
            session = FakeSession([FakeResponse(body=b"small zip bytes")])
            info = full_load.download(Path(temporary), session=session)
            self.assertEqual((Path(temporary) / "complaints.csv.zip").read_bytes(), b"small zip bytes")
            saved = json.loads((Path(temporary) / "download_metadata.json").read_text())
            self.assertEqual(saved["retry_count"], 0)
            self.assertEqual(info["size_bytes"], len(b"small zip bytes"))
            self.assertEqual(session.calls[0][1]["stream"], True)

    def test_historical_download_preserves_existing_archive_without_force(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "complaints.csv.zip"
            path.write_bytes(b"original")
            with self.assertRaises(FileExistsError):
                full_load.download(Path(temporary), session=FakeSession([]))
            self.assertEqual(path.read_bytes(), b"original")

    def test_historical_download_retries_temporary_http_error(self):
        session = FakeSession([FakeResponse(status=503), FakeResponse(body=b"retried")])
        with tempfile.TemporaryDirectory() as temporary, patch("ingestion.full_load.time.sleep"):
            info = full_load.download(Path(temporary), session=session)
            self.assertEqual(info["retry_count"], 1)
            self.assertEqual((Path(temporary) / "complaints.csv.zip").read_bytes(), b"retried")

    def test_extract_counts_records_and_uses_ingest_date(self):
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            archive_path = folder / "complaints.csv.zip"
            with zipfile.ZipFile(archive_path, "w") as archive:
                archive.writestr("complaints.csv", "id,text\n1,hello\n2,world\n")
            (folder / "download_metadata.json").write_text(json.dumps({
                "downloaded_at_utc": "2026-11-12T00:00:00+00:00", "http_status": 200,
                "attempt_count": 1, "retry_count": 0,
            }))
            manifest = extract_manifest.extract(archive_path, folder, "2026-11-12")
            self.assertEqual(manifest["record_count"], 2)
            self.assertEqual(manifest["ingest_date"], "2026-11-12")
            self.assertEqual(manifest["downloaded_at_utc"], "2026-11-12T00:00:00+00:00")

    def test_failed_forced_extract_preserves_previous_csv(self):
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            zip_path = folder / "complaints.csv.zip"
            zip_path.write_bytes(b"not a zip")
            csv_path = folder / "complaints.csv"
            csv_path.write_bytes(b"existing")
            with self.assertRaises(zipfile.BadZipFile):
                extract_manifest.extract(zip_path, folder, "2026-11-12", force=True)
            self.assertEqual(csv_path.read_bytes(), b"existing")

    def test_incremental_paginates_saves_raw_and_advances_watermark(self):
        first = payload([{"_source": {"complaint_id": 1}}, {"_source": {"complaint_id": 2}}], 3,
                        {"2": ["cursor", "2"]})
        second = payload([{"_source": {"complaint_id": 3}}], 3)
        session = FakeSession([FakeResponse(first, body=b'{"page":1}'), FakeResponse(second, body=b'{"page":2}')])
        with tempfile.TemporaryDirectory() as temporary:
            manifest, run_dir = incremental_api.collect(date(2026, 11, 1), date(2026, 11, 2), session=session,
                                                        root=Path(temporary))
            self.assertEqual(manifest["record_count"], 3)
            self.assertEqual(manifest["page_count"], 2)
            self.assertEqual((run_dir / "response_00002.json").read_bytes(), b'{"page":2}')
            state = json.loads((Path(temporary) / "high_water_mark.json").read_text())
            self.assertEqual(state["high_water_mark"], "2026-11-02")
            self.assertEqual(state["last_api_call_date_utc"], manifest["started_at_utc"][:10])
            self.assertEqual(state["last_api_call_at_utc"], manifest["started_at_utc"])
            self.assertEqual(state["last_requested_date_min"], "2026-11-01")
            self.assertEqual(state["last_requested_date_max_inclusive"], "2026-11-02")
            second_request_params = session.calls[1][1]["params"]
            self.assertEqual(second_request_params["search_after"], "cursor_2")

    def test_incremental_retries_temporary_http_error(self):
        session = FakeSession([FakeResponse(status=503), FakeResponse(payload=payload([], 0), body=b'{"empty":true}')])
        with patch("ingestion.incremental_api.time.sleep"):
            response, attempts = incremental_api.get_page(session, {})
            parsed = response.json()
        self.assertEqual(attempts, 2)
        self.assertEqual(parsed["hits"]["total"]["value"], 0)
        self.assertEqual(response.status_code, 200)

    def test_incremental_retries_timeout(self):
        session = FakeSession([requests.Timeout("temporary"), FakeResponse(payload=payload([], 0))])
        with patch("ingestion.incremental_api.time.sleep"):
            response, attempts = incremental_api.get_page(session, {})
            parsed = response.json()
        self.assertEqual(attempts, 2)
        self.assertEqual(parsed["hits"]["total"]["value"], 0)

    def test_incremental_uses_hit_sort_values_when_breakpoints_are_empty(self):
        page = payload([{"sort": [111, "one"]}, {"sort": [222, "two"]}], 3)
        self.assertEqual(incremental_api.next_cursor(page, 2), "222_two")

    def test_empty_api_response_is_success_and_window_replay_is_separate(self):
        empty_json = b'{"hits":{"hits":[],"total":{"value":0}},"_meta":{"break_points":{}}}'
        first_session = FakeSession([FakeResponse(payload([], 0), body=empty_json)])
        second_session = FakeSession([FakeResponse(payload([], 0), body=empty_json)])
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first_manifest, first_dir = incremental_api.collect(
                date(2026, 11, 20), date(2026, 11, 20), session=first_session, root=root
            )
            second_manifest, second_dir = incremental_api.collect(
                date(2026, 11, 20), date(2026, 11, 20), session=second_session, root=root
            )
            self.assertEqual(first_manifest["outcome"], "success")
            self.assertEqual(first_manifest["record_count"], 0)
            self.assertTrue((first_dir / "response_00001.json").is_file())
            self.assertNotEqual(first_manifest["run_id"], second_manifest["run_id"])
            self.assertNotEqual(first_dir, second_dir)

    def test_small_api_fixture_is_parseable(self):
        fixture = Path(__file__).parent / "fixtures" / "sample_api_response.json"
        response = json.loads(fixture.read_text(encoding="utf-8"))
        hits, total = incremental_api.extract_hits(response)
        self.assertEqual(total, 1)
        self.assertEqual(hits[0]["_source"]["complaint_id"], 9000000001)

    def test_minio_upload_skips_identical_object_on_rerun(self):
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "raw.json"
            source.write_bytes(b"tiny raw payload")
            s3 = FakeS3()
            with patch("ingestion.upload_raw.get_client", return_value=s3):
                upload_raw.upload_files([(source, "raw.json")], "cfpb/test")
                upload_raw.upload_files([(source, "raw.json")], "cfpb/test")
            self.assertEqual(s3.upload_count, 1)

    def test_minio_upload_refuses_unverified_existing_object(self):
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "raw.json"
            source.write_bytes(b"tiny raw payload")
            s3 = FakeS3()
            s3.objects["cfpb/test/raw.json"] = {"ContentLength": source.stat().st_size, "Metadata": {}}
            with patch("ingestion.upload_raw.get_client", return_value=s3):
                with self.assertRaisesRegex(RuntimeError, "Use --force"):
                    upload_raw.upload_files([(source, "raw.json")], "cfpb/test")
            self.assertEqual(s3.upload_count, 0)

    def test_incremental_does_not_advance_without_pagination_cursor(self):
        response = FakeResponse(payload([{"_source": {"complaint_id": 1}}], 2))
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(RuntimeError, "break_points"):
                incremental_api.collect(date(2026, 11, 1), date(2026, 11, 2),
                                        session=FakeSession([response]), root=Path(temporary))
            self.assertFalse((Path(temporary) / "high_water_mark.json").exists())

    def test_incremental_rejects_window_over_api_offset_limit_early(self):
        response = FakeResponse(payload([{"_source": {"complaint_id": 1}}], 100_001), body=b'{"oversized":true}')
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with self.assertRaisesRegex(RuntimeError, "above the 100000 offset limit"):
                incremental_api.collect(date(2026, 11, 1), date(2026, 11, 20),
                                        session=FakeSession([response]), root=root)
            run_dirs = list(root.glob("ingest_date=*/run_id=*"))
            self.assertEqual(len(run_dirs), 1)
            self.assertTrue((run_dirs[0] / "response_00001.json").is_file())
            self.assertFalse((root / "high_water_mark.json").exists())

    def test_invalid_api_json_is_preserved_before_processing_fails(self):
        response = FakeResponse(body=b"not json", json_error=ValueError("invalid JSON"))
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with self.assertRaisesRegex(ValueError, "invalid JSON"):
                incremental_api.collect(date(2026, 11, 20), date(2026, 11, 20),
                                        session=FakeSession([response]), root=root)
            run_dirs = list(root.glob("ingest_date=*/run_id=*"))
            self.assertEqual(len(run_dirs), 1)
            self.assertEqual((run_dirs[0] / "response_00001.json").read_bytes(), b"not json")
            self.assertFalse((root / "high_water_mark.json").exists())


if __name__ == "__main__":
    unittest.main()
