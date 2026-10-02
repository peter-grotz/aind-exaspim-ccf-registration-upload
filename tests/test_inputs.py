"""The asset is identified from the manifest and its own metadata, not its name."""

import json

import pytest
from conftest import BUCKET, make_asset
from upload_capsule.inputs import InputError, asset_from_manifest, subject_id


def _manifest(path, uri):
    path.write_text(json.dumps({"zarr_multiscale": {"input_uri": uri}}))
    return path


def test_a_missing_manifest_is_named(tmp_path):
    with pytest.raises(InputError, match="No manifest"):
        asset_from_manifest(tmp_path / "exaspim_manifest.json")


def test_a_manifest_without_an_input_uri_is_refused(tmp_path):
    (tmp_path / "m.json").write_text('{"zarr_multiscale": {}}')
    with pytest.raises(InputError, match="input_uri"):
        asset_from_manifest(tmp_path / "m.json")


@pytest.mark.parametrize(
    "name",
    [
        "exaSPIM_841260_2026-07-07_15-13-51_processed_2026-07-26_09-25-11",
        "823507_2026-06-30_16-49-27_processed_2026-08-31_10-32-14",
    ],
)
def test_the_asset_is_the_first_key_segment_whatever_its_name(tmp_path, name):
    manifest = _manifest(tmp_path / "m.json", f"s3://{BUCKET}/{name}/fusion/fused.zarr")
    asset = asset_from_manifest(manifest)
    assert (asset.bucket, asset.name) == (BUCKET, name)
    assert asset.uri == f"s3://{BUCKET}/{name}/"


@pytest.mark.parametrize("uri", ["/local/path", "s3://bucket-only", "gs://b/d"])
def test_a_uri_outside_a_dataset_is_refused(tmp_path, uri):
    with pytest.raises(InputError):
        asset_from_manifest(_manifest(tmp_path / "m.json", uri))


def test_the_subject_comes_from_subject_json(s3, tmp_path):
    make_asset(s3, "823507_2026-06-30_16-49-27", subject="823507", upstream=False)
    asset = asset_from_manifest(
        _manifest(tmp_path / "m.json", f"s3://{BUCKET}/823507_2026-06-30_16-49-27/x")
    )
    assert subject_id(s3, asset) == "823507"


def test_the_subject_falls_back_to_data_description(s3, tmp_path):
    s3.pipe(f"{BUCKET}/d/data_description.json", b'{"subject_id": "123456"}')
    asset = asset_from_manifest(_manifest(tmp_path / "m.json", f"s3://{BUCKET}/d/x"))
    assert subject_id(s3, asset) == "123456"


def test_no_subject_is_refused(s3, tmp_path):
    s3.pipe(f"{BUCKET}/d/subject.json", b"{}")
    asset = asset_from_manifest(_manifest(tmp_path / "m.json", f"s3://{BUCKET}/d/x"))
    with pytest.raises(InputError, match="No subject_id"):
        subject_id(s3, asset)
