"""Publish the exaSPIM CCF-registration pipeline's results back to their asset.

1. Identify the asset from the manifest's ``input_uri``, and its subject from the
   asset's own metadata.
2. Build the asset's ``processing.json`` from the producers' records and the asset's
   upstream ``processing.json`` files, in one pass of the metadata manager (see
   :mod:`upload_capsule.metadata`).
3. Curate each publishing stage's files, with its slice of that document.
4. Unless ``--dry-run``, upload the curated tree to the asset.
5. If ``SMARTSHEET_TOKEN`` is set, mark the subject registered.
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
import sys
from pathlib import Path

from upload_capsule import metadata, publish
from upload_capsule.config import REQUIRED_PROCESSES, STAGES, UPSTREAM_SUBFOLDERS, pipeline_from_env
from upload_capsule.inputs import MANIFEST, Asset, InputError, asset_from_manifest, subject_id
from upload_capsule.metadata import MetadataError

logger = logging.getLogger("upload")

TRUE = ("1", "true", "yes", "on")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse the command line."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--data-dir", type=Path, default=Path("../data"))
    parser.add_argument("--results-dir", type=Path, default=Path("../results"))
    parser.add_argument(
        "--manifest", type=Path, help=f"The run's manifest (default: <data-dir>/{MANIFEST})."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=os.environ.get("DRY_RUN", "").strip().lower() in TRUE,
        help="Build and curate everything into /results, but write nothing to the asset.",
    )
    args = parser.parse_args(argv)
    args.manifest = args.manifest or args.data_dir / MANIFEST
    return args


def fetch_upstream(fs, asset: Asset, destination: Path) -> list[Path]:
    """Download the asset's upstream ``processing.json`` files, read-only.

    Parameters
    ----------
    fs : fsspec.AbstractFileSystem
        The asset's filesystem.
    asset : Asset
        The asset.
    destination : Path
        Local directory to download into, one subfolder per upstream stage.

    Returns
    -------
    list[Path]
        The downloaded files. A stage without one is skipped.
    """
    found = []
    for sub in UPSTREAM_SUBFOLDERS:
        for remote in sorted(fs.glob(f"{asset.root}/{sub}/*processing.json")):
            local = destination / sub / Path(remote).name
            local.parent.mkdir(parents=True, exist_ok=True)
            fs.get(remote, str(local))
            found.append(local)
            logger.info("Upstream: %s/%s", sub, local.name)
    return found


def run(args: argparse.Namespace, fs) -> int:
    """Run the capsule against ``fs``.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed arguments.
    fs : fsspec.AbstractFileSystem
        Filesystem holding the asset (S3 in production).

    Returns
    -------
    int
        Process exit status.
    """
    pipeline = pipeline_from_env()
    asset = asset_from_manifest(args.manifest)
    logger.info("Asset: %s", asset.uri)
    token = os.environ.get("SMARTSHEET_TOKEN", "")
    # Resolved before anything is published, so a missing subject cannot fail the run
    # after the asset has already been written.
    subject = subject_id(fs, asset) if token else ""

    work = args.results_dir / "_work"
    publish_root = args.results_dir / "_publish"
    for directory in (work, publish_root):
        if directory.exists():
            shutil.rmtree(directory)
        directory.mkdir(parents=True)

    records_by_stage = {}
    for stage in STAGES:
        records = metadata.load_records(args.data_dir / stage.records_dir, pipeline.name)
        if records:
            records_by_stage[stage.name] = records
            logger.info("Stage %s: %s", stage.name, [r.name for r in records])
        else:
            logger.warning(
                "No records for stage %s in /data/%s; its processes will be absent",
                stage.name,
                stage.records_dir,
            )

    upstream = fetch_upstream(fs, asset, work / "upstream")
    all_records = [r for records in records_by_stage.values() for r in records]
    root = metadata.build_processing(all_records, upstream, work / "manager", pipeline)
    missing = metadata.missing_processes(root, REQUIRED_PROCESSES)
    if missing:
        raise MetadataError(f"Required processes missing; nothing published: {missing}")
    unversioned = metadata.unversioned_processes(root)
    if unversioned:
        logger.warning("No code version or commit recorded for: %s", unversioned)
    logger.info("processing.json: %d processes", len(root.data_processes))

    for stage in STAGES:
        if not stage.publish:
            continue
        source = args.data_dir / stage.files_dir
        copied = (
            publish.curate(source, publish_root / stage.name, stage.patterns)
            if source.is_dir()
            else []
        )
        if not copied:
            logger.warning(
                "Stage %s has nothing to publish in /data/%s", stage.name, stage.files_dir
            )
        if stage.name in records_by_stage:
            names = [r.name for r in records_by_stage[stage.name]]
            metadata.write_document(
                metadata.stage_document(root, names),
                publish_root / stage.name / "processing.json",
            )
    metadata.write_document(root, publish_root / "processing.json")

    if args.dry_run:
        logger.info(
            "Dry run: curated tree is in %s; nothing written to %s", publish_root, asset.uri
        )
        return 0

    count = publish.upload_tree(fs, publish_root, asset.root)
    logger.info("Uploaded %d files to %s", count, asset.uri)

    if token:
        from upload_capsule.smartsheet import mark_registered

        try:
            mark_registered(subject, token)
        except Exception:  # the asset is already published; say so
            logger.exception("Published %s, but the Smartsheet update failed", asset.uri)
            return 1
        logger.info("Smartsheet: marked %s registered", subject)
    return 0


def main() -> int:
    """Entry point."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    args = parse_args()
    import s3fs

    try:
        return run(args, s3fs.S3FileSystem())
    except (InputError, MetadataError) as error:
        logger.error("%s", error)
        return 1


if __name__ == "__main__":
    sys.exit(main())
