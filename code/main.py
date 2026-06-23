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
from aind_data_schema.core.processing import Processing
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
        "ccf_mesh_to_sample/**/*.obj",  # publish ONLY the warped meshes (.obj);
                                        # the qc/ overlay png stays results-only (never S3)
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


def resolve_output(in_base: str) -> str:
    """Where to WRITE outputs. Reads always come from `in_base` (the input asset,
    e.g. aind-open-data). When OUTPUT_PREFIX is set (e.g. a scratch test dir
    s3://aind-scratch-data/exaspim_processing_test), outputs go to
    {OUTPUT_PREFIX}/<asset_name>/; otherwise alongside the input asset."""
    prefix = os.environ.get("OUTPUT_PREFIX")
    if not prefix:
        return in_base
    asset_name = in_base.rstrip("/").split("/")[-1]
    return f"{prefix.rstrip('/')}/{asset_name}/"


def fetch_upstream_metadata(s3_root: str, work: Path, fs: s3fs.S3FileSystem) -> None:
    """Copy each upstream subfolder's existing processing.json from the asset into
    the work tree (READ-ONLY) so the manager merges it into the ROOT aggregate.
    Only *processing.json is fetched -- those existing records are left untouched
    on S3. Our own producer data_processes (e.g. the CCF-channel fusion record)
    are NOT read from S3; they arrive via the pipeline's /results channel and are
    staged separately."""
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


def upload(s3_path: str, local_path: str, fs: s3fs.S3FileSystem, dest_rel: str = "") -> None:
    """Upload local_path to s3_path/<dest_rel>, preserving the layout deterministically.

    s3fs.put collapses a SINGLE-file directory onto the destination key -- e.g.
    soma_detection/ holding only soma_locations.csv lands as an object literally
    named 'soma_detection' (no slash) instead of soma_detection/soma_locations.csv,
    even with an explicit trailing-slash destination. To defeat that, a directory's
    TOP-LEVEL files are each uploaded to an explicit key, while sub-directories
    (e.g. .zarr, which always contain many files) use the fast recursive put.
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


# Causally-correct dependencies for the processes WE produce (keyed by process
# name; value = list of upstream process names it depends on). The metadata
# manager cannot infer real edges from our lightweight data_process records, so
# it produces a degenerate/incorrect graph; we set the truth explicitly. Only
# edges we own/understand are encoded -- upstream-internal dependencies are left
# as the manager produced them, since we don't own those capsules.
KNOWN_DEPENDENCIES = {
    "CCF channel fusion":             ["Image tile alignment"],
    "Image atlas alignment - 25 um":  ["CCF channel fusion"],
    "Image atlas alignment - 10 um":  ["Image atlas alignment - 25 um"],
    "CCF annotation to sample space": ["Image atlas alignment - 25 um"],
    "CCF meshes to sample space":      ["Image atlas alignment - 25 um"],
    # Soma processes (from the soma DETECTION capsule, mounted via SOMA_META_DIR).
    # Names verified against the actual records (aind-exaspim-soma-detection):
    # the detector reads the standard fused image ("Image tile fusing"), then
    # generate -> classify -> metrics. Keys MUST match the records' `name` exactly;
    # apply_known_dependencies ignores any key not present (non-fatal).
    "Proposal generation":     ["Image tile fusing"],
    "Proposal classification": ["Proposal generation"],
    "Soma metrics":            ["Proposal classification"],
}


def apply_known_dependencies(processing):
    """Override the manager-inferred dependency_graph with KNOWN_DEPENDENCIES for
    the processes we produce, keeping only edges to processes present in THIS
    document. This is correct in both the ROOT aggregate and a stage-scoped doc:
    e.g. in the ccf_alignment stage, 'CCF channel fusion' is absent, so the 25 um
    alignment's dependency correctly resolves to []."""
    present = {dp.name for dp in processing.data_processes}
    dg = dict(processing.dependency_graph or {})
    for name, deps in KNOWN_DEPENDENCIES.items():
        if name in present:
            dg[name] = [d for d in deps if d in present]
    return processing.model_copy(update={"dependency_graph": dg})


def ensure_code_provenance(processing):
    """Future-proofing: aind-data-schema warns now (and will eventually REQUIRE)
    that every Code carries a commit_hash or version. Our own records already set
    a version; this backfills version='unknown' for any process that arrives with
    NEITHER (e.g. an incomplete upstream record like 'In-place multiscale
    generation'), so the published processing.json stays valid once the
    requirement is enforced. Logs what it backfilled so the upstream gap is
    visible/actionable. Returns the (possibly re-validated) Processing."""
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
    processing = apply_known_dependencies(processing)   # fix manager's degenerate graph
    processing = ensure_code_provenance(processing)     # backfill missing Code.version
    processing.write_standard_file(str(work))
    out = work / "processing.json"
    print(f"built top-level processing.json ({len(processing.data_processes)} processes): {out}")
    return out


def build_stage_processing_json(stage_src: Path, work_root: Path, out_path: Path):
    """Build a STAGE-SCOPED processing.json from only the *_data_process.json in
    stage_src (e.g. ccf_alignment), matching the per-subfolder convention where
    each stage folder carries its own processing record (tile_alignment/,
    fusion/, ... each have one). The asset-ROOT processing.json
    (build_processing_json) remains the full aggregated lineage. Returns the
    written path, or None if the stage has no producer records."""
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
    settings = MetadataSettings(
        _cli_parse_args=False,
        input_dir=stage_work,
        output_dir=stage_work,
        processor_full_name=os.environ.get("PROCESSOR_FULL_NAME", "AIND Scientific Computing"),
        pipeline_name=os.environ.get("PIPELINE_NAME", "exaspim-data-processing"),
        pipeline_version=os.environ.get("PIPELINE_VERSION", "0.0.0"),
        pipeline_url=os.environ.get(
            "PIPELINE_URL", "https://codeocean.allenneuraldynamics.org/capsule/9578158/tree"
        ),
        aggregate_quality_control=False,
        skip_ancillary_files=True,
    )
    processing = MetadataManager(settings).create_processing_metadata()
    processing = apply_known_dependencies(processing)   # fix manager's degenerate graph (scoped to this stage)
    processing = ensure_code_provenance(processing)     # backfill missing Code.version
    processing.write_standard_file(str(stage_work))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(stage_work / "processing.json", out_path)
    print(f"built stage processing.json ({len(processing.data_processes)} processes) "
          f"for {out_path.parent.name}: {out_path}")
    return out_path


def main() -> None:
    DATA_FOLDER = Path("../data").resolve()
    RESULTS_FOLDER = Path("../results").resolve()
    manifest = sorted(DATA_FOLDER.glob("*.json"))[0]
    dataset_config = json.loads(manifest.read_text())
    dataset_path = str(dataset_config["zarr_multiscale"]["input_uri"])
    in_base = get_root_s3_prefix(dataset_path)        # read inputs from here (aind-open-data)
    out_base = resolve_output(in_base)                # write outputs here (scratch test dir if set)
    print(f"input asset:   {in_base}")
    print(f"output target: {out_base}")

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

    # 1b) stage the fusion capsule's data_process from /results (passed via the
    # pipeline) so our CCF-channel fusion appears in the ROOT aggregate. fusion is
    # NOT in PUBLISH_WHITELIST, so this is aggregated into the root processing.json
    # but never republished to S3, and the existing fusion/processing.json on S3 is
    # left untouched. If ../data/fusion/ is absent, the pipeline is not passing the
    # fusion capsule's /results to upload -> the fusion record will be missing.
    fusion_src = DATA_FOLDER / "fusion"
    fusion_dps = list(fusion_src.glob("*_data_process.json")) if fusion_src.exists() else []
    if fusion_dps:
        (work / "fusion").mkdir(parents=True, exist_ok=True)
        for f in fusion_dps:
            shutil.copy2(f, work / "fusion" / f.name)
            print(f"  staged fusion data_process for root aggregation: {f.name}")
    else:
        print("  WARNING: no fusion *_data_process.json in ../data/fusion -- the CCF-channel "
              "fusion record will be ABSENT from the root processing.json. Confirm the pipeline "
              "passes the fusion capsule's /results to the upload capsule's /data.")

    # 1c) soma metadata rides in a SEPARATE mount from the soma_locations.csv. Two
    # capsules emit a top-level soma_detection/ folder: soma->CCF (capsule-4249884)
    # produces the canonical soma_locations.csv (with xyz_ccf_auto), and soma
    # DETECTION (capsule-7525101) produces the soma *_data_process.json records.
    # They cannot both mount at ../data/soma_detection (Nextflow input-name
    # collision), so the pipeline mounts the DETECTION connection at
    # ../data/<SOMA_META_DIR> (default soma_detection_meta). We fold ONLY its
    # *_data_process.json into work/soma_detection/, so the soma records aggregate
    # into BOTH the root processing.json and the soma_detection stage processing.json
    # just as if they had arrived alongside the CSV. The published soma_locations.csv
    # stays the canonical soma->CCF one (we never copy a CSV from the meta mount).
    # rglob (recursive) so it finds the records whether the pipeline maps the
    # connection's destination flat (data/soma_detection_meta/*.json) or nested
    # (data/soma_detection_meta/soma_detection/*.json) -- robust to how the CO
    # connection's source/destination paths land.
    soma_meta_dir = os.environ.get("SOMA_META_DIR", "soma_detection_meta")
    soma_meta_src = DATA_FOLDER / soma_meta_dir
    soma_meta_dps = list(soma_meta_src.rglob("*_data_process.json")) if soma_meta_src.exists() else []
    if soma_meta_dps:
        (work / "soma_detection").mkdir(parents=True, exist_ok=True)
        from aind_data_schema.core.processing import DataProcess  # validate, don't silently drop
        # The soma DETECTION capsule stamps its records with its OWN pipeline_name
        # ("exaspim-soma-detection"). aind-data-schema requires every
        # DataProcess.pipeline_name to exist in the aggregated Processing.pipelines,
        # but the manager registers only ONE pipeline (PIPELINE_NAME) from its
        # settings -- so a foreign name makes the WHOLE Processing() fail to validate
        # ("Pipeline name 'exaspim-soma-detection' not found in pipelines list"). We
        # don't own that capsule, so we normalize the folded records' pipeline_name
        # to THIS pipeline's name on the way in, matching every other producer record.
        pipeline_name = os.environ.get("PIPELINE_NAME", "exaspim-data-processing")
        for f in soma_meta_dps:
            rec = json.loads(f.read_text())
            orig = rec.get("pipeline_name")
            rec["pipeline_name"] = pipeline_name
            (work / "soma_detection" / f.name).write_text(json.dumps(rec, indent=3))
            # The metadata manager silently SKIPS records that fail DataProcess
            # validation (e.g. a process_type that isn't a valid ProcessName enum),
            # so they'd vanish from processing.json with no trace. Validate here and
            # warn loudly with the reason so a malformed soma record is visible.
            try:
                DataProcess.model_validate(rec)
                note = "" if orig == pipeline_name else f" (pipeline_name '{orig}' -> '{pipeline_name}')"
                print(f"  staged soma data_process for aggregation: {f.name}{note}")
            except Exception as exc:
                print(f"  WARNING: soma record {f.name} is INVALID and will be DROPPED by the "
                      f"metadata manager -- fix it in the soma-detection capsule. Reason: {exc}")
    else:
        print(f"  WARNING: no soma *_data_process.json in ../data/{soma_meta_dir} -- soma metadata "
              "will be ABSENT from processing.json. Confirm the pipeline mounts the soma DETECTION "
              f"capsule's /results at ../data/{soma_meta_dir} (a SEPARATE mount from soma_detection, "
              "to avoid the input-name collision).")

    # 2) bring in upstream metadata (read-only, from the input asset) so the manager merges it
    fetch_upstream_metadata(in_base, work, fs)

    # 3) Goal 1: aggregate + validate the top-level processing.json
    top = build_processing_json(work)

    # 4) Goal 2: curate the publish set; include the top-level processing.json
    for sub, patterns in PUBLISH_WHITELIST.items():
        if (work / sub).exists():
            stage_curated(work / sub, pub / sub, patterns)
    # Asset ROOT gets the full aggregated lineage; each published subfolder that
    # produced its own *_data_process.json gets a STAGE-SCOPED processing.json,
    # matching the per-subfolder convention (tile_alignment/, fusion/, ... each
    # carry their own stage record). build_stage_processing_json skips subfolders
    # with no producer records (e.g. soma_detection), so only the root holds the
    # aggregate.
    shutil.copy2(top, pub / "processing.json")
    for sub in PUBLISH_WHITELIST:
        if (work / sub).exists():
            build_stage_processing_json(work / sub, work, pub / sub / "processing.json")

    # 5) upload the curated, metadata-complete tree to the OUTPUT target
    for sub in PUBLISH_WHITELIST:
        if (pub / sub).exists():
            upload(out_base, str(pub / sub), fs, dest_rel=sub)
    upload(out_base, str(pub / "processing.json"), fs, dest_rel="processing.json")

    (RESULTS_FOLDER / "finished_registration.txt").write_text(out_base)

    token = os.environ.get("SMARTSHEET_TOKEN")  # was hard-coded — moved to a secret
    if token:
        update_smartsheet(find_brain_id(dataset_path), token)


if __name__ == "__main__":
    main()
