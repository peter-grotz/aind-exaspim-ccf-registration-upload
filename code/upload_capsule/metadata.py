"""Build the asset's ``processing.json`` with ``aind-metadata-manager``.

One pass: this pipeline's ``*_data_process.json`` records and the input asset's upstream
``processing.json`` files go to the manager together. It validates them, merges them
(keeping each upstream document's dependency graph and pipelines) and rejects duplicate
names. It chains bare records in the order it finds them, which is not their lineage, so
the edges of this pipeline's processes are then set from
:data:`~upload_capsule.config.DEPENDENCIES`.

Each published subfolder's ``processing.json`` is a slice of that one document, so the
two cannot disagree.

Records are validated before the manager sees them. The manager drops an invalid record
with a log warning and carries on, which would publish incomplete lineage; here an invalid
record fails the run.
"""

from __future__ import annotations

import json
import logging
import shutil
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from aind_data_schema.core.processing import DataProcess, Processing
from aind_metadata_manager.metadata_manager import MetadataManager, MetadataSettings

from upload_capsule.config import DEPENDENCIES, Pipeline

logger = logging.getLogger(__name__)

RECORD_GLOB = "*_data_process.json"


class MetadataError(ValueError):
    """Raised when the records cannot be assembled into publishable metadata."""


@dataclass(frozen=True)
class Record:
    """One producer record, already validated.

    Attributes
    ----------
    path : Path
        Where it was read from.
    document : dict
        Its JSON, with ``pipeline_name`` set to this pipeline's.
    """

    path: Path
    document: dict

    @property
    def name(self) -> str:
        """The record's ``DataProcess.name``."""
        return str(self.document.get("name") or "")


def load_records(directory: Path, pipeline_name: str) -> list[Record]:
    """Read and validate every record under a stage's records directory.

    ``pipeline_name`` is set on each record: the schema requires it to name an entry in
    ``Processing.pipelines``, and producers owned by others stamp their own (the soma
    detector writes ``exaspim-soma-detection``).

    Parameters
    ----------
    directory : Path
        Directory to search, recursively. Absent means the stage did not run.
    pipeline_name : str
        This pipeline's name.

    Returns
    -------
    list[Record]
        The stage's records, sorted by path.

    Raises
    ------
    MetadataError
        If any record is not a valid ``DataProcess``, listing every one that is not.
    """
    if not directory.is_dir():
        return []
    records, invalid = [], []
    for path in sorted(directory.rglob(RECORD_GLOB)):
        document = json.loads(path.read_text(encoding="utf-8"))
        original = document.get("pipeline_name")
        document["pipeline_name"] = pipeline_name
        try:
            DataProcess.model_validate(document)
        except Exception as error:  # noqa: BLE001 - reported together below
            invalid.append(f"{path}: {error}")
            continue
        if original and original != pipeline_name:
            logger.info("%s: pipeline_name %r -> %r", path.name, original, pipeline_name)
        records.append(Record(path, document))
    if invalid:
        raise MetadataError("Invalid producer records:\n" + "\n".join(invalid))
    return records


def _settings(directory: Path, pipeline: Pipeline) -> MetadataSettings:
    """Manager settings that read and write ``directory``."""
    return MetadataSettings(
        _cli_parse_args=False,
        input_dir=directory,
        output_dir=directory,
        processor_full_name=pipeline.processor,
        pipeline_name=pipeline.name,
        pipeline_version=pipeline.version,
        pipeline_url=pipeline.url,
        aggregate_quality_control=False,
        skip_ancillary_files=True,
    )


def _with_edges(
    processing: Processing, names: Iterable[str], dependencies: Mapping[str, Sequence[str]]
) -> Processing:
    """Set the inputs of ``names`` from ``dependencies``, keeping only present processes."""
    present = {process.name for process in processing.data_processes}
    graph = dict(processing.dependency_graph or {})
    for name in names:
        graph[name] = [d for d in dependencies.get(name, []) if d in present]
    return Processing.model_validate(
        {**processing.model_dump(mode="json"), "dependency_graph": graph}
    )


def build_processing(
    records: Sequence[Record],
    upstream_documents: Sequence[Path],
    workdir: Path,
    pipeline: Pipeline,
    dependencies: Mapping[str, Sequence[str]] = DEPENDENCIES,
) -> Processing:
    """Merge this pipeline's records and the upstream documents into one ``Processing``.

    Parameters
    ----------
    records : Sequence[Record]
        Every producer record of this run.
    upstream_documents : Sequence[Path]
        The input asset's existing upstream ``processing.json`` files.
    workdir : Path
        Scratch directory for the manager; emptied first.
    pipeline : Pipeline
        This pipeline.
    dependencies : Mapping[str, Sequence[str]], optional
        Process inputs by name. Every record must have an entry; it is also applied to
        any upstream process it lists.

    Returns
    -------
    Processing
        The asset's ``processing.json``.

    Raises
    ------
    MetadataError
        If a record has no entry in ``dependencies``.
    """
    unknown = [r.name for r in records if r.name not in dependencies]
    if unknown:
        raise MetadataError(f"No dependency entry for {unknown}; add them to config.DEPENDENCIES")
    _reset(workdir)
    for index, record in enumerate(records):
        path = workdir / f"{index:02d}_{record.path.name}"
        path.write_text(json.dumps(record.document), encoding="utf-8")
    # The manager reads every *processing.json under its input directory as a document.
    for index, path in enumerate(upstream_documents):
        shutil.copy2(path, workdir / f"upstream{index}_{path.parent.name}_processing.json")
    processing = MetadataManager(_settings(workdir, pipeline)).create_processing_metadata()
    listed = [p.name for p in processing.data_processes if p.name in dependencies]
    return _with_edges(processing, listed, dependencies)


def stage_document(processing: Processing, names: Iterable[str]) -> Processing:
    """Slice the processes ``names`` out of the asset's ``processing.json``.

    Edges are kept only between the slice's own processes: a stage's first process shows
    no inputs, because what feeds it belongs to another stage.

    Parameters
    ----------
    processing : Processing
        The asset's ``processing.json``.
    names : Iterable[str]
        The stage's process names.

    Returns
    -------
    Processing
        The stage's ``processing.json``.
    """
    keep = set(names)
    processes = [p for p in processing.data_processes if p.name in keep]
    graph = processing.dependency_graph or {}
    used = {p.pipeline_name for p in processes}
    return Processing(
        data_processes=processes,
        pipelines=[c for c in processing.pipelines or [] if c.name in used] or None,
        dependency_graph={
            p.name: [d for d in graph.get(p.name, []) if d in keep] for p in processes
        },
    )


def write_document(processing: Processing, path: Path) -> None:
    """Write a ``Processing`` document to ``path``."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(processing.model_dump_json(indent=3), encoding="utf-8")


def unversioned_processes(processing: Processing) -> list[str]:
    """Return the processes whose code records neither a version nor a commit hash."""
    return [
        p.name
        for p in processing.data_processes
        if not (p.code and (p.code.version or p.code.commit_hash))
    ]


def missing_processes(processing: Processing, required: Iterable[str]) -> list[str]:
    """Return the required processes the document lacks."""
    present = {p.name for p in processing.data_processes}
    return [name for name in required if name not in present]


def _reset(directory: Path) -> None:
    """Empty ``directory``, creating it if needed."""
    if directory.exists():
        shutil.rmtree(directory)
    directory.mkdir(parents=True)
