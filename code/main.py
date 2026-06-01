"""
Upload capsule — exaSPIM CCF-registration + soma-reg results.

Metadata (Goal 1): delegated to the official `aind-metadata-manager`, which
collects producer `*_data_process.json` files + upstream `processing.json`
(fetched from the asset's S3) and writes the aggregated, validated top-level
`processing.json`.
Curation (Goal 2): publish only the spec-compliant files for ccf_alignment/
and soma_detection/.
Bucket is parameterized: production -> aind-open-data; test -> aind-scratch-data.
"""

from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
import json
import os
import re
import shutil

import s3fs
from aind_metadata_manager.metadata_manager import MetadataManager, MetadataSettings

# ---- Goal-2 publish whitelist (relative to each subfolder) ------------------
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
# Upstream subfolders whose existing processing.json is merged (read-only) by the manager.
UPSTREAM_SUBFOLDERS = ("tile_alignment", "fusion", "flatfield_correction", "denoised")


def get_root_s3_prefix(s3_uri, levels_up=1):
    scheme, bucket_and_key = s3_uri.split("://", 1)
    bucket, *key_parts = bucket_and_key.split("/")
    return f"s3://{bucket}/{'/'.join(key_parts[:levels_up])}/"


def find_brain_id(input_uri):
    m = re.search(r"exaspim_(\d{6})", input_uri.lower())
    if not m:
        raise ValueError(f"Could not extract exaSPIM ID from {input_uri}")
    return m.group(1)


def resolve_target(s3_reg_path: str) -> str:
    """Parameterized bucket. Default = production asset path. Set UPLOAD_BUCKET
    (e.g. 'aind-scratch-data') to override for test runs."""
    override = os.environ.get("UPLOAD_BUCKET")
    if not override:
        return s3_reg_path
    _, rest = s3_reg_path.split("://", 1)
    _, *key = rest.split("/")
    return f"s3://{override}/" + "/".join(key)


def fetch_upstream_metadata(s3_root: str, work: Path, fs: s3fs.S3FileSystem) -> None:
    """Copy upstream metadata from the asset into the work tree (read-only) so
    the manager merges it: any *processing.json and *_data_process.json under
    each upstream subfolder. (The CCF-fusion capsule writes to S3, not through
    the nextflow channel, so its data_process is picked up here.)"""
    base = s3_root.rstrip("/")
    for sub in UPSTREAM_SUBFOLDERS:
        for pattern in ("*processing.json", "*_data_process.json"):
            try:
                matches = fs.glob(f"{base}/{sub}/{pattern}")
            except Exception:
                matches = []
            for remote in matches:
                try:
                    (work / sub).mkdir(parents=True, exist_ok=True)
                    fs.get(remote, str(work / sub / os.path.basename(remote)))
                    print(f"  fetched upstream {sub}/{os.path.basename(remote)}")
                except Exception as e:  # never fatal
                    print(f"  skip upstream {remote}: {e}")


def stage_curated(src: Path, dst: Path, patterns: list) -> None:
    """Copy only whitelisted outputs from a producer subfolder into the publish tree."""
    dst.mkdir(parents=True, exist_ok=True)
    for pat in patterns:
        for match in src.glob(pat):
            out = dst / match.relative_to(src)
            out.parent.mkdir(parents=True, exist_ok=True)
            if match.is_dir():
                shutil.copytree(match, out, dirs_exist_ok=True)
            else:
                shutil.copy2(match, out)


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


def build_processing_json(work: Path) -> Path:
    """Run aind-metadata-manager over `work` to produce the aggregated top-level
    processing.json (Goal 1). Producers' *_data_process.json + fetched upstream
    processing.json are merged + validated. Returns the written file path."""
    settings = MetadataSettings(
        _cli_parse_args=False,
        input_dir=work,
        output_dir=work,
        processor_full_name=os.environ.get("PROCESSOR_FULL_NAME", "AIND Scientific Computing"),
        pipeline_name=os.environ.get("PIPELINE_NAME", "exaspim-data-processing"),
        pipeline_version=os.environ.get("PIPELINE_VERSION", "0.0.0"),
        pipeline_url=os.environ.get(
            "PIPELINE_URL", "https://codeocean.allenneuraldynamics.org/capsule/9578158/tree"
        ),
        aggregate_quality_control=False,  # QC deferred
        skip_ancillary_files=True,        # ancillary/data_description handled elsewhere
    )
    processing = MetadataManager(settings).create_processing_metadata()
    processing.write_standard_file(str(work))
    out = work / "processing.json"
    print(f"built top-level processing.json ({len(processing.data_processes)} processes): {out}")
    return out


def main() -> None:
    DATA_FOLDER = Path("../data").resolve()
    RESULTS_FOLDER = Path("../results").resolve()
    manifest = sorted(DATA_FOLDER.glob("*.json"))[0]
    dataset_config = json.loads(manifest.read_text())
    dataset_path = str(dataset_config["zarr_multiscale"]["input_uri"])
    s3_reg_path = resolve_target(get_root_s3_prefix(dataset_path))
    print(f"target asset: {s3_reg_path}")

    fs = s3fs.S3FileSystem()
    work = RESULTS_FOLDER / "_work"
    pub = RESULTS_FOLDER / "_publish"
    for d in (work, pub):
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(parents=True)

    # 1) stage producer outputs in full (incl. *_data_process.json) for the manager
    for sub in PUBLISH_WHITELIST:
        src = DATA_FOLDER / sub
        if src.exists():
            shutil.copytree(src, work / sub, dirs_exist_ok=True)

    # 2) bring in upstream metadata (read-only) so the manager merges it
    fetch_upstream_metadata(s3_reg_path, work, fs)

    # 3) Goal 1: aggregate + validate the top-level processing.json
    top = build_processing_json(work)

    # 4) Goal 2: curate the publish set; include the top-level processing.json
    for sub, patterns in PUBLISH_WHITELIST.items():
        if (work / sub).exists():
            stage_curated(work / sub, pub / sub, patterns)
    shutil.copy2(top, pub / "processing.json")
    # spec lists a processing.json inside ccf_alignment/ as well
    (pub / "ccf_alignment").mkdir(parents=True, exist_ok=True)
    shutil.copy2(top, pub / "ccf_alignment" / "processing.json")

    # 5) upload the curated, metadata-complete tree
    for sub in PUBLISH_WHITELIST:
        if (pub / sub).exists():
            upload(s3_reg_path, str(pub / sub), fs)
    upload(s3_reg_path, str(pub / "processing.json"), fs)

    (RESULTS_FOLDER / "finished_registration.txt").write_text(s3_reg_path)

    token = os.environ.get("SMARTSHEET_TOKEN")  # was hard-coded — moved to a secret
    if token:
        update_smartsheet(find_brain_id(dataset_path), token)


if __name__ == "__main__":
    main()
