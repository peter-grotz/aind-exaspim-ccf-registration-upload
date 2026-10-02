"""Shared fixtures: an in-memory stand-in for S3, and a pipeline run's /data."""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

import pytest
from fsspec.implementations.dirfs import DirFileSystem
from fsspec.implementations.local import LocalFileSystem

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "code"))

RESOURCES = Path(__file__).parent / "resources" / "841260"
BUCKET = "aind-open-data"


@pytest.fixture
def s3(tmp_path):
    """A filesystem rooted in a temporary directory, addressed like s3fs: ``bucket/key``.

    ``auto_mkdir`` mirrors S3, where writing a key needs no parent "directory".
    """
    root = tmp_path / "s3"
    root.mkdir()
    return DirFileSystem(path=str(root), fs=LocalFileSystem(auto_mkdir=True))


def make_asset(fs, name: str, subject: str = "841260", upstream: bool = True) -> None:
    """Write a processed dataset with its core metadata and upstream stage records."""
    fs.makedirs(f"{BUCKET}/{name}", exist_ok=True)
    fs.pipe(f"{BUCKET}/{name}/subject.json", json.dumps({"subject_id": subject}).encode())
    fs.pipe(f"{BUCKET}/{name}/processing.json", b'{"previous": true}')
    if upstream:
        for sub in ("fusion", "flatfield_correction", "denoised"):
            fs.pipe(
                f"{BUCKET}/{name}/{sub}/processing.json",
                (RESOURCES / f"upstream_{sub}.json").read_bytes(),
            )


def make_data(data: Path, asset_name: str, records: bool = True) -> Path:
    """Lay out /data as the pipeline mounts it: manifest, producer records and outputs."""
    data.mkdir(parents=True, exist_ok=True)
    (data / "exaspim_manifest.json").write_text(
        json.dumps(
            {"zarr_multiscale": {"input_uri": f"s3://{BUCKET}/{asset_name}/fusion/fused.zarr"}}
        )
    )
    (data / "unrelated.json").write_text('{"not": "a manifest"}')
    if records:
        for stage, mount in (
            ("ccf_alignment", "ccf_alignment"),
            ("soma", "soma_detection_meta"),
            ("fusion", "fusion"),
        ):
            shutil.copytree(RESOURCES / "records" / stage, data / mount, dirs_exist_ok=True)
    ccf = data / "ccf_alignment"
    ccf.mkdir(exist_ok=True)
    (ccf / "841260_to_exaSPIM_SyN_0GenericAffine.mat").write_bytes(b"affine")
    (ccf / "841260_to_exaSPIM_SyN_1InverseWarp.nii.gz").write_bytes(b"warp")
    (ccf / "scratch_not_published.nii.gz").write_bytes(b"x")
    (ccf / "ccf_aligned.zarr" / "0").mkdir(parents=True)
    (ccf / "ccf_aligned.zarr" / "0" / "chunk").write_bytes(b"c")
    (ccf / "ccf_mesh_to_sample" / "a").mkdir(parents=True)
    (ccf / "ccf_mesh_to_sample" / "a" / "x.obj").write_text("v 0 0 0")
    (ccf / "ccf_mesh_to_sample" / "qc").mkdir()
    (ccf / "ccf_mesh_to_sample" / "qc" / "overlay.png").write_bytes(b"png")
    (data / "soma_detection").mkdir(exist_ok=True)
    (data / "soma_detection" / "soma_locations.csv").write_text("x,y,z\n")
    return data
