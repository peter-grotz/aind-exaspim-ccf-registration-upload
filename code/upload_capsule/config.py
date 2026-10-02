"""What this capsule publishes, and how its processes depend on each other.

Everything that changes when a producer capsule changes lives here: the stages and the
files each one publishes, and the dependency edges between processes.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

UPSTREAM_SUBFOLDERS = ("tile_alignment", "fusion", "flatfield_correction", "denoised")
"""Subfolders of the input asset whose existing ``processing.json`` is merged, read-only."""


@dataclass(frozen=True)
class Stage:
    """One producer stage of this pipeline.

    Attributes
    ----------
    name : str
        Stage name; also the publish subfolder when ``publish`` is set.
    records_dir : str
        Directory under ``/data`` holding the stage's ``*_data_process.json`` records.
    files_dir : str
        Directory under ``/data`` holding the stage's output files.
    publish : bool
        Whether the stage's files and a stage ``processing.json`` are written to the asset.
    patterns : tuple[str, ...]
        Globs, relative to ``files_dir``, of the files that are published.
    """

    name: str
    records_dir: str
    files_dir: str = ""
    publish: bool = False
    patterns: tuple[str, ...] = field(default_factory=tuple)


STAGES = (
    Stage(
        name="ccf_alignment",
        records_dir="ccf_alignment",
        files_dir="ccf_alignment",
        publish=True,
        patterns=(
            "*_to_exaSPIM_SyN_0GenericAffine.mat",
            "*_to_exaSPIM_SyN_1Warp.nii.gz",
            "*_to_exaSPIM_SyN_1InverseWarp.nii.gz",
            "ccf_aligned.zarr",
            "ccf_anno_to_sample/ccf_anno_in_sample_space.nii.gz",
            "ccf_anno_to_sample/ccf_anno_in_sample_space.zarr",
            "ccf_mesh_to_sample/**/*.obj",  # meshes only; the qc/ overlays stay in /results
        ),
    ),
    # soma->CCF writes the CSV; soma detection writes the records. Both produce a
    # soma_detection/ folder, so the records are mounted separately to avoid a Nextflow
    # input-name collision.
    Stage(
        name="soma_detection",
        records_dir=os.environ.get("SOMA_META_DIR", "soma_detection_meta"),
        files_dir="soma_detection",
        publish=True,
        patterns=("soma_locations.csv",),
    ),
    # The CCF-channel fusion record is merged in but nothing is republished: the asset's
    # fusion/ belongs to the upstream processing pipeline.
    Stage(name="ccf_fusion", records_dir="fusion"),
)

DEPENDENCIES: dict[str, list[str]] = {
    # Upstream preprocessing, where supported by evidence (explicit input_data or run
    # timing). Edges resting only on timestamps weeks apart are left out, so 'Image tile
    # alignment' and 'Whole brain masking' stay roots.
    "In-place multiscale generation": ["Inference dispatch"],
    "Image flat-field correction": ["Inference dispatch"],
    "Image tile fusing": ["Image tile alignment"],
    # This pipeline.
    "CCF channel fusion": ["Image tile alignment"],
    "Image atlas alignment - 25 um": ["CCF channel fusion"],
    "Image atlas alignment - 10 um": ["Image atlas alignment - 25 um"],
    "CCF annotation to sample space": ["Image atlas alignment - 25 um"],
    "CCF meshes to sample space": ["Image atlas alignment - 25 um"],
    "Proposal generation": ["Image tile fusing"],
    "Proposal classification": ["Proposal generation"],
    "Soma metrics": ["Proposal classification"],
}
"""Inputs of each process, by ``DataProcess.name``.

Every record a producer stage emits must have an entry; a record without one fails the
run rather than receiving an invented edge. Edges to processes absent from the run are
dropped.
"""

REQUIRED_PROCESSES = ("Image atlas alignment - 25 um", "Image atlas alignment - 10 um")
"""Processes without which nothing is published."""


@dataclass(frozen=True)
class Pipeline:
    """The pipeline recorded in ``Processing.pipelines``."""

    name: str
    version: str
    url: str
    processor: str


def pipeline_from_env() -> Pipeline:
    """Read the pipeline identity from the environment.

    Returns
    -------
    Pipeline
        ``PIPELINE_NAME`` must equal the ``pipeline_name`` on the producers' records.
    """
    return Pipeline(
        name=os.environ.get("PIPELINE_NAME", "exaspim-data-processing"),
        version=os.environ.get("PIPELINE_VERSION", "0.0.0"),
        url=os.environ.get(
            "PIPELINE_URL", "https://codeocean.allenneuraldynamics.org/capsule/9578158/tree"
        ),
        processor=os.environ.get("PROCESSOR_FULL_NAME", "AIND Scientific Computing"),
    )
