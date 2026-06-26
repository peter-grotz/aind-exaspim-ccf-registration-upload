"""
Upload capsule for the exaSPIM CCF-registration + soma-reg pipeline.

Aggregates the pipeline's producer and upstream metadata into a single validated
top-level ``processing.json`` (via ``aind-metadata-manager`` on aind-data-schema
v2) and publishes the curated subset of results to the asset's S3 location.

Output bucket is environment-driven: production writes to the input asset
(aind-open-data); setting ``OUTPUT_PREFIX`` redirects writes to a scratch dir.
"""

from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
import json
import os
import re
import shutil

import s3fs
from aind_data_schema.core.processing import DataProcess, Processing
from aind_metadata_manager.metadata_manager import MetadataManager, MetadataSettings

# Files published to S3, per producer subfolder (globs relative to the subfolder).
PUBLISH_WHITELIST = {
    "ccf_alignment": [
        "*_to_exaSPIM_SyN_0GenericAffine.mat",
        "*_to_exaSPIM_SyN_1Warp.nii.gz",
        "*_to_exaSPIM_SyN_1InverseWarp.nii.gz",
        "ccf_aligned.zarr",
        "ccf_anno_to_sample/ccf_anno_in_sample_space.nii.gz",
        "ccf_anno_to_sample/ccf_anno_in_sample_space.zarr",
        "ccf_mesh_to_sample/**/*.obj",  # meshes only; the qc/ overlay stays results-only
    ],
    "soma_detection": [
        "soma_locations.csv",
    ],
}

# Upstream subfolders whose existing processing.json is merged (read-only) into the aggregate.
UPSTREAM_SUBFOLDERS = ("tile_alignment", "fusion", "flatfield_correction", "denoised")

# True dependency edges between the processes this pipeline produces, keyed by
# DataProcess.name. The manager cannot infer these from the lightweight producer
# records, so they are set explicitly; edges to a process absent from a given
# document are dropped (see apply_known_dependencies). Keys must match the records'
# `name` exactly -- unmatched keys are ignored.
KNOWN_DEPENDENCIES = {
    "CCF channel fusion":             ["Image tile alignment"],
    "Image atlas alignment - 25 um":  ["CCF channel fusion"],
    "Image atlas alignment - 10 um":  ["Image atlas alignment - 25 um"],
    "CCF annotation to sample space": ["Image atlas alignment - 25 um"],
    "CCF meshes to sample space":     ["Image atlas alignment - 25 um"],
    "Proposal generation":            ["Image tile fusing"],
    "Proposal classification":        ["Proposal generation"],
    "Soma metrics":                   ["Proposal classification"],
}


def get_root_s3_prefix(s3_uri: str) -> str:
    """Return the asset-root S3 prefix (bucket + first key segment) for an input URI."""
    _, bucket_and_key = s3_uri.split("://", 1)
    bucket, *key_parts = bucket_and_key.split("/")
    return f"s3://{bucket}/{key_parts[0]}/"


def find_brain_id(input_uri: str) -> str:
    m = re.search(r"exaspim_(\d{6})", input_uri.lower())
    if not m:
        raise ValueError(f"Could not extract exaSPIM ID from {input_uri}")
    return m.group(1)


def resolve_output(in_base: str) -> str:
    """Resolve the write target. Reads always come from `in_base` (the input asset).
    If OUTPUT_PREFIX is set, outputs go to {OUTPUT_PREFIX}/<asset_name>/ (e.g. a
    scratch test dir); otherwise alongside the input asset (production)."""
    prefix = os.environ.get("OUTPUT_PREFIX")
    if not prefix:
        return in_base
    asset_name = in_base.rstrip("/").split("/")[-1]
    return f"{prefix.rstrip('/')}/{asset_name}/"


def _metadata_settings(input_dir: Path, output_dir: Path) -> MetadataSettings:
    """Shared aind-metadata-manager settings. The pipeline_* fields come from the
    environment with production defaults; PIPELINE_NAME must match the pipeline_name
    the producer records carry."""
    return MetadataSettings(
        _cli_parse_args=False,
        input_dir=input_dir,
        output_dir=output_dir,
        processor_full_name=os.environ.get("PROCESSOR_FULL_NAME", "AIND Scientific Computing"),
        pipeline_name=os.environ.get("PIPELINE_NAME", "exaspim-data-processing"),
        pipeline_version=os.environ.get("PIPELINE_VERSION", "0.0.0"),
        pipeline_url=os.environ.get(
            "PIPELINE_URL", "https://codeocean.allenneuraldynamics.org/capsule/9578158/tree"
        ),
        aggregate_quality_control=False,  # QC deferred
        skip_ancillary_files=True,        # ancillary/data_description handled elsewhere
    )


def apply_known_dependencies(processing: Processing) -> Processing:
    """Replace the manager-inferred dependency_graph with KNOWN_DEPENDENCIES, keeping
    only edges to processes present in this document. Correct for both the root
    aggregate and a stage-scoped document (absent upstream processes resolve to [])."""
    present = {dp.name for dp in processing.data_processes}
    dg = dict(processing.dependency_graph or {})
    for name, deps in KNOWN_DEPENDENCIES.items():
        if name in present:
            dg[name] = [d for d in deps if d in present]
    return processing.model_copy(update={"dependency_graph": dg})


def ensure_code_provenance(processing: Processing) -> Processing:
    """Backfill Code.version='unknown' for any process missing both version and
    commit_hash, so the document stays valid as aind-data-schema moves toward
    requiring one. Logs what was backfilled to surface the upstream gap."""
    d = processing.model_dump()
    patched = []
    for dp in d.get("data_processes", []):
        c = dp.get("code") or {}
        if not c.get("version") and not c.get("commit_hash"):
            c["version"] = "unknown"
            dp["code"] = c
            patched.append(dp.get("name"))
    if not patched:
        return processing
    print(f"  backfilled Code.version='unknown' (missing version/commit_hash): {patched}")
    return Processing.model_validate(d)


def fetch_upstream_metadata(s3_root: str, work: Path, fs: s3fs.S3FileSystem) -> None:
    """Copy each upstream subfolder's existing processing.json from the asset into the
    work tree (read-only) so the manager merges it into the aggregate. Only those
    files are fetched; they are never rewritten on S3. Never fatal."""
    base = s3_root.rstrip("/")
    for sub in UPSTREAM_SUBFOLDERS:
        try:
            matches = fs.glob(f"{base}/{sub}/*processing.json")
        except Exception:
            matches = []
        for remote in matches:
            try:
                (work / sub).mkdir(parents=True, exist_ok=True)
                fs.get(remote, str(work / sub / os.path.basename(remote)))
                print(f"  fetched upstream {sub}/{os.path.basename(remote)}")
            except Exception as e:
                print(f"  skip upstream {remote}: {e}")


def build_processing_json(work: Path) -> Path:
    """Aggregate the staged producer + upstream records into the validated root
    processing.json. Returns the written path."""
    processing = MetadataManager(_metadata_settings(work, work)).create_processing_metadata()
    processing = apply_known_dependencies(processing)
    processing = ensure_code_provenance(processing)
    processing.write_standard_file(str(work))
    out = work / "processing.json"
    print(f"built top-level processing.json ({len(processing.data_processes)} processes): {out}")
    return out


def build_stage_processing_json(stage_src: Path, work_root: Path, out_path: Path):
    """Build a stage-scoped processing.json from only the *_data_process.json in
    stage_src, matching the legacy per-subfolder layout (tile_alignment/, fusion/,
    ... each carry their own record). Returns the path, or None if the stage has no
    producer records."""
    dp_files = sorted(stage_src.glob("*_data_process.json"))
    if not dp_files:
        print(f"no *_data_process.json in {stage_src}; skipping stage processing.json")
        return None
    stage_work = work_root / "_stage" / out_path.parent.name
    if stage_work.exists():
        shutil.rmtree(stage_work)
    stage_work.mkdir(parents=True)
    for f in dp_files:
        shutil.copy2(f, stage_work / f.name)
    processing = MetadataManager(_metadata_settings(stage_work, stage_work)).create_processing_metadata()
    processing = apply_known_dependencies(processing)
    processing = ensure_code_provenance(processing)
    processing.write_standard_file(str(stage_work))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(stage_work / "processing.json", out_path)
    print(f"built stage processing.json ({len(processing.data_processes)} processes) "
          f"for {out_path.parent.name}: {out_path}")
    return out_path


def stage_producer_outputs(data_folder: Path, work: Path) -> None:
    """Copy each producer subfolder (including its *_data_process.json) into the work
    tree so the manager can aggregate them."""
    for sub in PUBLISH_WHITELIST:
        src = data_folder / sub
        if src.exists():
            shutil.copytree(src, work / sub, dirs_exist_ok=True)


def stage_fusion_metadata(data_folder: Path, work: Path) -> None:
    """Fold the fusion capsule's data_process (which arrives via the pipeline's
    /results channel, not S3) into the aggregate. fusion is not in PUBLISH_WHITELIST,
    so it is aggregated but never republished, and the asset's existing
    fusion/processing.json is left untouched."""
    fusion_src = data_folder / "fusion"
    fusion_dps = list(fusion_src.glob("*_data_process.json")) if fusion_src.exists() else []
    if not fusion_dps:
        print("  WARNING: no fusion *_data_process.json in ../data/fusion -- the CCF-channel "
              "fusion record will be ABSENT from processing.json. Confirm the pipeline passes "
              "the fusion capsule's /results to this capsule's /data.")
        return
    (work / "fusion").mkdir(parents=True, exist_ok=True)
    for f in fusion_dps:
        shutil.copy2(f, work / "fusion" / f.name)
        print(f"  staged fusion data_process for root aggregation: {f.name}")


def stage_soma_metadata(data_folder: Path, work: Path) -> None:
    """Fold the soma-detection records into work/soma_detection/ so they aggregate
    alongside the soma->CCF soma_locations.csv.

    The detection records mount at SOMA_META_DIR (a separate mount from
    soma_detection, avoiding a Nextflow input-name collision with the CSV); rglob
    handles a flat or nested destination. Their pipeline_name is normalized to
    PIPELINE_NAME because aind-data-schema requires every DataProcess.pipeline_name
    to be a registered pipeline, and the manager registers only PIPELINE_NAME -- a
    foreign name (the detection capsule stamps "exaspim-soma-detection") would fail
    the whole Processing(). Each record is validated up front; the manager silently
    drops invalid ones, so we warn loudly instead."""
    soma_meta_dir = os.environ.get("SOMA_META_DIR", "soma_detection_meta")
    soma_meta_src = data_folder / soma_meta_dir
    soma_meta_dps = list(soma_meta_src.rglob("*_data_process.json")) if soma_meta_src.exists() else []
    if not soma_meta_dps:
        print(f"  WARNING: no soma *_data_process.json in ../data/{soma_meta_dir} -- soma metadata "
              "will be ABSENT from processing.json. Confirm the pipeline mounts the soma DETECTION "
              f"capsule's /results at ../data/{soma_meta_dir}.")
        return
    (work / "soma_detection").mkdir(parents=True, exist_ok=True)
    pipeline_name = os.environ.get("PIPELINE_NAME", "exaspim-data-processing")
    for f in soma_meta_dps:
        rec = json.loads(f.read_text())
        orig = rec.get("pipeline_name")
        rec["pipeline_name"] = pipeline_name
        (work / "soma_detection" / f.name).write_text(json.dumps(rec, indent=3))
        try:
            DataProcess.model_validate(rec)
            note = "" if orig == pipeline_name else f" (pipeline_name '{orig}' -> '{pipeline_name}')"
            print(f"  staged soma data_process for aggregation: {f.name}{note}")
        except Exception as exc:
            print(f"  WARNING: soma record {f.name} is INVALID and will be DROPPED by the "
                  f"metadata manager -- fix it in the soma-detection capsule. Reason: {exc}")


def stage_curated(src: Path, dst: Path, patterns: list) -> None:
    """Copy only the whitelisted files from a producer subfolder into the publish tree."""
    dst.mkdir(parents=True, exist_ok=True)
    for pat in patterns:
        for match in src.glob(pat):
            out = dst / match.relative_to(src)
            out.parent.mkdir(parents=True, exist_ok=True)
            if match.is_dir():
                shutil.copytree(match, out, dirs_exist_ok=True)
            else:
                shutil.copy2(match, out)


def upload(s3_path: str, local_path: str, fs: s3fs.S3FileSystem, dest_rel: str = "") -> None:
    """Upload local_path to s3_path/<dest_rel>, preserving layout.

    s3fs.put collapses a single-file directory onto the destination key (so a folder
    holding just one file lands as a slashless object). To avoid that, a directory's
    top-level files are each put to an explicit key, while sub-directories (e.g.
    .zarr) use the fast recursive put.
    """
    url = urlparse(s3_path)
    if url.scheme != "s3":
        raise NotImplementedError(f"Only s3 output is supported, not {url.scheme}")
    base = url.netloc + url.path.rstrip("/")
    p = Path(local_path)
    if p.is_dir():
        prefix = f"{base}/{dest_rel.strip('/')}" if dest_rel else base
        for entry in sorted(p.iterdir()):
            if entry.is_dir():
                print(f"uploading {entry}/* -> s3://{prefix}/{entry.name}/")
                fs.put(str(entry).rstrip("/") + "/", f"{prefix}/{entry.name}/", recursive=True, maxdepth=20)
            else:
                print(f"uploading {entry} -> s3://{prefix}/{entry.name}")
                fs.put(str(entry), f"{prefix}/{entry.name}")
    else:
        key = f"{base}/{dest_rel.strip('/')}" if dest_rel else f"{base}/{p.name}"
        print(f"uploading {p} -> s3://{key}")
        fs.put(str(p), key)


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
    data_folder = Path("../data").resolve()
    results_folder = Path("../results").resolve()
    manifest = sorted(data_folder.glob("*.json"))[0]
    dataset_config = json.loads(manifest.read_text())
    dataset_path = str(dataset_config["zarr_multiscale"]["input_uri"])
    in_base = get_root_s3_prefix(dataset_path)   # read inputs from here
    out_base = resolve_output(in_base)            # write outputs here (scratch if OUTPUT_PREFIX set)
    print(f"input asset:   {in_base}")
    print(f"output target: {out_base}")

    fs = s3fs.S3FileSystem()
    work = results_folder / "_work"
    pub = results_folder / "_publish"
    for d in (work, pub):
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(parents=True)

    # Stage producer + upstream metadata, then aggregate the root processing.json.
    stage_producer_outputs(data_folder, work)
    stage_fusion_metadata(data_folder, work)
    stage_soma_metadata(data_folder, work)
    fetch_upstream_metadata(in_base, work, fs)
    top = build_processing_json(work)

    # Curate the publish tree: whitelisted files + root processing.json + a
    # stage-scoped processing.json per producer subfolder that emitted records.
    for sub, patterns in PUBLISH_WHITELIST.items():
        if (work / sub).exists():
            stage_curated(work / sub, pub / sub, patterns)
    shutil.copy2(top, pub / "processing.json")
    for sub in PUBLISH_WHITELIST:
        if (work / sub).exists():
            build_stage_processing_json(work / sub, work, pub / sub / "processing.json")

    # Upload the curated tree to the output target.
    for sub in PUBLISH_WHITELIST:
        if (pub / sub).exists():
            upload(out_base, str(pub / sub), fs, dest_rel=sub)
    upload(out_base, str(pub / "processing.json"), fs, dest_rel="processing.json")

    (results_folder / "finished_registration.txt").write_text(out_base)

    token = os.environ.get("SMARTSHEET_TOKEN")
    if token:
        update_smartsheet(find_brain_id(dataset_path), token)


if __name__ == "__main__":
    main()
