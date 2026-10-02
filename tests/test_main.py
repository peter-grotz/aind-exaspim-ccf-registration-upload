"""The capsule end to end, against a local stand-in for S3."""

import json

import main
import pytest
from conftest import BUCKET, make_asset, make_data
from upload_capsule.metadata import MetadataError

NAME = "exaSPIM_841260_2026-07-07_15-13-51_processed_2026-07-26_09-25-11"


def _args(tmp_path, *extra):
    return main.parse_args(
        ["--data-dir", str(tmp_path / "data"), "--results-dir", str(tmp_path / "results"), *extra]
    )


def test_a_dry_run_writes_nothing_to_the_asset(s3, tmp_path, monkeypatch):
    monkeypatch.delenv("SMARTSHEET_TOKEN", raising=False)
    make_asset(s3, NAME)
    make_data(tmp_path / "data", NAME)
    before = sorted(s3.find(f"{BUCKET}/{NAME}"))
    assert main.run(_args(tmp_path, "--dry-run"), s3) == 0
    assert sorted(s3.find(f"{BUCKET}/{NAME}")) == before
    root = json.loads((tmp_path / "results" / "_publish" / "processing.json").read_text())
    assert len(root["data_processes"]) == 13


@pytest.mark.parametrize("name", [NAME, "823507_2026-06-30_16-49-27_processed_2026-08-31_10-32-14"])
def test_a_run_publishes(s3, tmp_path, monkeypatch, name):
    monkeypatch.delenv("SMARTSHEET_TOKEN", raising=False)
    make_asset(s3, name)
    make_data(tmp_path / "data", name)
    assert main.run(_args(tmp_path), s3) == 0
    base = f"{BUCKET}/{name}"
    assert s3.isfile(f"{base}/ccf_alignment/ccf_aligned.zarr/0/chunk")
    assert s3.isfile(f"{base}/ccf_alignment/processing.json")
    assert s3.isfile(f"{base}/soma_detection/soma_locations.csv")
    assert s3.isfile(f"{base}/soma_detection/processing.json")
    assert not s3.exists(f"{base}/ccf_fusion")
    assert json.loads(s3.cat(f"{base}/processing.json"))["dependency_graph"]
    assert not s3.exists(f"{base}/original_metadata")


def test_missing_registration_records_publish_nothing(s3, tmp_path, monkeypatch):
    monkeypatch.delenv("SMARTSHEET_TOKEN", raising=False)
    make_asset(s3, NAME)
    make_data(tmp_path / "data", NAME, records=False)
    before = sorted(s3.find(f"{BUCKET}/{NAME}"))
    with pytest.raises(MetadataError, match="Required processes missing"):
        main.run(_args(tmp_path), s3)
    assert sorted(s3.find(f"{BUCKET}/{NAME}")) == before


def test_a_missing_subject_fails_before_publishing(s3, tmp_path, monkeypatch):
    monkeypatch.setenv("SMARTSHEET_TOKEN", "test-token")
    make_asset(s3, NAME)
    s3.rm(f"{BUCKET}/{NAME}/subject.json")
    make_data(tmp_path / "data", NAME)
    before = sorted(s3.find(f"{BUCKET}/{NAME}"))
    with pytest.raises(main.InputError):
        main.run(_args(tmp_path), s3)
    assert sorted(s3.find(f"{BUCKET}/{NAME}")) == before
