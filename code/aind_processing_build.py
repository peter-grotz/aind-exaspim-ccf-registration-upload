#!/usr/bin/env python3
"""
Centralized processing.json builder (runs in the UPLOAD capsule only).

Architecture (PLAN.md §10.8 / §13.3): producer capsules emit a lightweight,
schema-agnostic "process record" JSON; this module — the ONLY place with the
v2 aind-data-schema stack — converts records + upstream subfolder
processing.json files into VALIDATED v2 Processing objects and writes:
  * a per-subfolder processing.json for our own processes, and
  * the top-level (aggregated) processing.json for the whole asset.

Pinned stack (install in the upload capsule env, git tags — NOT on PyPI):
  pip install "aind-data-schema-models @ git+https://github.com/AllenNeuralDynamics/aind-data-schema-models.git@v5.7.2"
  pip install "aind-data-schema @ git+https://github.com/AllenNeuralDynamics/aind-data-schema.git@v2.7.1"
Requires Python >= 3.10.

Producer "process record" contract (one JSON file `process_record.json` per
process subfolder; may contain a single record or a list):
  {
    "process_type": "Image atlas alignment",   # must be a ProcessName value
    "name": "Image atlas alignment - 25 um",    # unique within the asset
    "stage": "Processing",                       # or "Analysis"
    "start_date_time": "2026-05-28T00:28:48Z",   # tz-aware ISO-8601
    "end_date_time":   "2026-05-28T00:49:32Z",
    "experimenters": ["Di Wang"],
    "code": {"url": "...", "name": "...", "version": "<sha>",
             "run_script": "/code/run", "language": "Python"},
    "parameters": { ... },                        # -> code.parameters
    "output_path": "ccf_alignment/",
    "output_parameters": { ... },
    "notes": "...",
    "pipeline_name": "aind-exaSPIM-ccf-registration"  # must match a pipeline name
  }
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from aind_data_schema.core.processing import Processing, DataProcess
from aind_data_schema.components.identifiers import Code

# --- the pipeline identity (AIND versioning policy: name + semver + url) ------
PIPELINE_NAME = "aind-exaSPIM-ccf-registration"
PIPELINE_VERSION = "1.0.0"  # bump per release; ideally read from PIPELINE_VERSION env / nextflow.config
PIPELINE_URL = "https://codeocean.allenneuraldynamics.org/capsule/9578158/tree"

# subfolders whose existing processing.json comes from UPSTREAM (read-only, may be older schema)
UPSTREAM_SUBFOLDERS = ("tile_alignment", "fusion", "flatfield_correction", "denoised")


def pipeline_code() -> Code:
    return Code(url=PIPELINE_URL, name=PIPELINE_NAME, version=PIPELINE_VERSION,
                run_script="code/main.nf", language="Nextflow")


def build_data_process(rec: dict) -> DataProcess:
    """Build a VALIDATED v2 DataProcess from a producer record."""
    code_fields = dict(rec.get("code") or {})
    if rec.get("parameters") is not None:
        code_fields.setdefault("parameters", rec["parameters"])
    return DataProcess(
        process_type=rec["process_type"],          # pydantic coerces str -> ProcessName
        name=rec.get("name") or rec["process_type"],
        stage=rec.get("stage", "Processing"),
        code=Code(**code_fields),
        experimenters=rec.get("experimenters", []),
        pipeline_name=rec.get("pipeline_name"),
        start_date_time=rec["start_date_time"],
        end_date_time=rec.get("end_date_time"),
        output_path=rec.get("output_path"),
        output_parameters=rec.get("output_parameters"),
        notes=rec.get("notes"),
    )


def ingest_upstream(processing_json: Path) -> list[DataProcess]:
    """Load DataProcess entries from an existing (possibly older-schema) processing.json.

    Validates leniently: tries strict validation, falls back to model_construct so a
    2.2.5 upstream entry never blocks the aggregate (mixed-version aggregation, §13.2 #5).
    """
    payload = json.loads(processing_json.read_text())
    out: list[DataProcess] = []
    for dp in payload.get("data_processes", []):
        try:
            out.append(DataProcess.model_validate(dp))
        except Exception:
            out.append(DataProcess.model_construct(**dp))
    return out


def _records_from(path: Path) -> list[dict]:
    data = json.loads(path.read_text())
    return data if isinstance(data, list) else [data]


def _rebase(value, subfolder: str):
    """Rewrite asset-root-relative paths to be relative to `subfolder`.

    AssetPath is interpreted relative to the metadata directory, so a per-subfolder
    processing.json must carry paths relative to ITS subfolder (top-level keeps the
    asset-root-relative form). Strips a leading '<subfolder>/' and maps a bare
    '<subfolder>' to None (the process outputs to the subfolder root itself).
    """
    if isinstance(value, str):
        v = value.rstrip("/")
        if v == subfolder:
            return None
        prefix = subfolder + "/"
        return value[len(prefix):] if value.startswith(prefix) else value
    if isinstance(value, list):
        return [_rebase(v, subfolder) for v in value]
    if isinstance(value, dict):
        return {k: _rebase(v, subfolder) for k, v in value.items()}
    return value


def _rebased_record(rec: dict, subfolder: str) -> dict:
    """Copy of a record with output_path/output_parameters rebased to the subfolder."""
    r = dict(rec)
    r["output_path"] = _rebase(rec.get("output_path"), subfolder)
    if rec.get("output_parameters") is not None:
        r["output_parameters"] = _rebase(rec["output_parameters"], subfolder)
    return r


def build_processing(data_processes: Iterable[DataProcess]) -> Processing:
    """Construct + VALIDATE a v2 Processing (unique names + chrono sort enforced here)."""
    return Processing(data_processes=list(data_processes), pipelines=[pipeline_code()])


def assemble(results_dir: str | Path) -> Processing:
    """Walk a results tree, write per-subfolder processing.json for our processes,
    and the top-level aggregated processing.json. Returns the top-level object."""
    results = Path(results_dir)
    all_processes: list[DataProcess] = []

    # 1) OUR processes: each producer subfolder drops a process_record.json
    for record_file in sorted(results.glob("*/process_record.json")):
        subfolder = record_file.parent.name
        records = _records_from(record_file)
        # per-subfolder processing.json: paths rebased relative to the subfolder
        sub_procs = [build_data_process(_rebased_record(r, subfolder)) for r in records]
        build_processing(sub_procs).write_standard_file(output_directory=record_file.parent)
        # top-level: keep asset-root-relative paths
        all_processes.extend(build_data_process(r) for r in records)

    # 2) UPSTREAM processes: read their existing processing.json read-only
    for sub in UPSTREAM_SUBFOLDERS:
        existing = results / sub / "processing.json"
        if existing.exists() and not (results / sub / "process_record.json").exists():
            all_processes.extend(ingest_upstream(existing))

    # 3) top-level aggregate (validated; Processing sorts chrono + enforces unique names)
    top = build_processing(all_processes)
    top.write_standard_file(output_directory=results)
    return top


if __name__ == "__main__":
    import sys
    obj = assemble(sys.argv[1] if len(sys.argv) > 1 else "../results")
    print(f"Wrote top-level processing.json with {len(obj.data_processes)} processes")
