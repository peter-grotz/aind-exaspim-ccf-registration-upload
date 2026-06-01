"""
Upload capsule — exaSPIM CCF-registration + soma-reg results.

Responsibilities (PLAN.md §10.2):
  Goal 1  build & VALIDATE the per-subfolder + top-level processing.json
          (centralized metadata: producers emit process_record.json; we own the
          only v2 aind-data-schema env). Upstream subfolder processing.json are
          read from the existing S3 asset, read-only, and folded into the top-level.
  Goal 2  curate the published S3 set so ccf_alignment/ and soma_detection/
          match the ExaSPIM spec exactly (whitelist; drop intermediates).
Bucket is PARAMETERIZED: production -> aind-open-data; test -> aind-scratch-data.
"""

from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
import glob
import json
import os
import re
import shutil

import s3fs

import aind_processing_build as apb  # the v2 builder (this capsule's env only)

# ---- Goal-2 publish whitelist (relative to each subfolder) ------------------
# Globs of what MAY be published. processing.json is always kept. Everything
# else the producer wrote to /results stays local and is NOT uploaded.
PUBLISH_WHITELIST = {
    "ccf_alignment": [
        "*_to_exaSPIM_SyN_0GenericAffine.mat",
        "*_to_exaSPIM_SyN_1Warp.nii.gz",
        "*_to_exaSPIM_SyN_1InverseWarp.nii.gz",
        "ccf_aligned.zarr",
        "ccf_anno_to_sample/ccf_anno_in_sample_space.nii.gz",
        "ccf_anno_to_sample/ccf_anno_in_sample_space.zarr",
    ],
    "soma_detection": [
        "soma_locations.csv",
    ],
}
ALWAYS_KEEP = {"processing.json"}
NEVER_UPLOAD = {"process_record.json"}  # internal producer record

UPSTREAM_SUBFOLDERS = apb.UPSTREAM_SUBFOLDERS  # tile_alignment, fusion, flatfield_correction, denoised


# ---------------------------------------------------------------------------
def get_root_s3_prefix(s3_uri, levels_up=1):
    scheme, bucket_and_key = s3_uri.split("://", 1)
    bucket, *key_parts = bucket_and_key.split("/")
    base_key = "/".join(key_parts[:levels_up])
    return f"s3://{bucket}/{base_key}/"


def find_brain_id(input_uri):
    m = re.search(r"exaspim_(\d{6})", input_uri.lower())
    if not m:
        raise ValueError(f"Could not extract exaSPIM ID from {input_uri}")
    return m.group(1)


def resolve_target(s3_reg_path: str) -> str:
    """Parameterized bucket (PLAN.md §11). Default = production asset path.
    Set UPLOAD_BUCKET to override (e.g. 'aind-scratch-data' for test runs)."""
    override = os.environ.get("UPLOAD_BUCKET")
    if override:
        scheme, rest = s3_reg_path.split("://", 1)
        _, *key = rest.split("/")
        return f"s3://{override}/" + "/".join(key)
    return s3_reg_path


def fetch_upstream_processing(s3_root: str, staging: Path, fs: s3fs.S3FileSystem) -> None:
    """Copy upstream subfolders' processing.json from the existing asset into the
    staging tree (read-only) so the aggregator can include their data_processes."""
    for sub in UPSTREAM_SUBFOLDERS:
        remote = s3_root.rstrip("/") + f"/{sub}/processing.json"
        try:
            if fs.exists(remote):
                (staging / sub).mkdir(parents=True, exist_ok=True)
                fs.get(remote, str(staging / sub / "processing.json"))
                print(f"  fetched upstream {sub}/processing.json")
        except Exception as e:  # upstream may be absent; never fatal
            print(f"  skip upstream {sub}: {e}")


def stage_curated(src: Path, staging_sub: Path, patterns: list[str]) -> None:
    """Copy ONLY whitelisted outputs (+ the process_record.json needed for
    aggregation) from a producer's results subfolder into staging."""
    staging_sub.mkdir(parents=True, exist_ok=True)
    keep = list(patterns)
    for rec in NEVER_UPLOAD:  # staged for the builder, removed before upload
        if (src / rec).exists():
            shutil.copy2(src / rec, staging_sub / rec)
    for pat in keep:
        for match in src.glob(pat):
            dest = staging_sub / match.relative_to(src)
            dest.parent.mkdir(parents=True, exist_ok=True)
            if match.is_dir():
                shutil.copytree(match, dest, dirs_exist_ok=True)
            else:
                shutil.copy2(match, dest)


def upload(s3_path: str, folder: str, fs: s3fs.S3FileSystem) -> None:
    url = urlparse(s3_path)
    if url.scheme != "s3":
        raise NotImplementedError(f"Only s3 output is supported, not {url.scheme}")
    print(f"uploading {folder} -> {s3_path}")
    fs.put(folder, url.netloc + url.path.rstrip("/") + "/", recursive=True, maxdepth=10)


def update_smartsheet(brain_id, access_token):
    from aind_exaspim_dataset_utils.smartsheet_util import SmartSheetClient
    client = SmartSheetClient(access_token, "ExM Dataset Summary")
    column_map = {col.title: col.id for col in client.sheet.columns}
    row = client.client.models.Row()
    row.id = client.find_row_id(brain_id)
    row.cells.append({"column_id": column_map.get("CCF Registered"), "value": True, "strict": False})
    row.cells.append({"column_id": column_map.get("Affine Registration Date"),
                      "value": datetime.today().strftime("%m/%d/%Y"), "strict": False})
    client.client.Sheets.update_rows(client.sheet_id, [row])


def main() -> None:
    DATA_FOLDER = Path("../data").resolve()
    RESULTS_FOLDER = Path("../results").resolve()
    manifest = sorted(DATA_FOLDER.glob("*.json"))[0]
    dataset_config = json.loads(manifest.read_text())
    dataset_path = str(dataset_config["zarr_multiscale"]["input_uri"])
    s3_reg_path = resolve_target(get_root_s3_prefix(dataset_path))
    print(f"target asset: {s3_reg_path}")

    fs = s3fs.S3FileSystem()
    staging = RESULTS_FOLDER / "_staging"
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)

    # 1) curate our producer subfolders into staging (whitelist; keep records for builder)
    for sub, patterns in PUBLISH_WHITELIST.items():
        src = DATA_FOLDER / sub
        if src.exists():
            stage_curated(src, staging / sub, patterns)

    # 2) bring in upstream processing.json (read-only) for the aggregate
    fetch_upstream_processing(s3_reg_path, staging, fs)

    # 3) build + VALIDATE per-subfolder and top-level processing.json
    top = apb.assemble(staging)
    print(f"built processing.json with {len(top.data_processes)} processes")

    # 4) drop internal records, then upload the curated, metadata-complete tree
    for rec in staging.rglob("process_record.json"):
        rec.unlink()
    for sub in PUBLISH_WHITELIST:
        if (staging / sub).exists():
            upload(s3_reg_path, str(staging / sub), fs)
    # top-level processing.json -> asset root
    upload(s3_reg_path, str(staging / "processing.json"), fs)

    (RESULTS_FOLDER / "finished_registration.txt").write_text(s3_reg_path)

    token = os.environ.get("SMARTSHEET_TOKEN")  # was hard-coded — moved to a secret
    if token:
        update_smartsheet(find_brain_id(dataset_path), token)


if __name__ == "__main__":
    main()
