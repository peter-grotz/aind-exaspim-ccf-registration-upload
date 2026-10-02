"""Curate the publish tree and write it to the asset."""

from __future__ import annotations

import logging
import shutil
from datetime import date, datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

ARCHIVE_DIR = "original_metadata"


def curate(source: Path, destination: Path, patterns: tuple[str, ...]) -> list[Path]:
    """Copy the files matching ``patterns`` from ``source`` into ``destination``.

    Parameters
    ----------
    source : Path
        A stage's output directory.
    destination : Path
        Where the published copy is assembled.
    patterns : tuple[str, ...]
        Globs relative to ``source``. A matched directory (a zarr) is copied whole.

    Returns
    -------
    list[Path]
        What was copied, relative to ``destination``.
    """
    copied = []
    for pattern in patterns:
        for match in sorted(source.glob(pattern)):
            target = destination / match.relative_to(source)
            target.parent.mkdir(parents=True, exist_ok=True)
            if match.is_dir():
                shutil.copytree(match, target, dirs_exist_ok=True)
            else:
                shutil.copy2(match, target)
            copied.append(target.relative_to(destination))
    return copied


def upload_tree(fs, local_root: Path, remote_root: str) -> int:
    """Upload every file under ``local_root`` to the same relative key under ``remote_root``.

    Files are sent as one explicit list of local and remote paths. A recursive ``put`` of
    a directory holding a single file collapses it onto the destination key, losing the
    folder; explicit pairs avoid that and still upload concurrently.

    Parameters
    ----------
    fs : fsspec.AbstractFileSystem
        Destination filesystem.
    local_root : Path
        The curated tree.
    remote_root : str
        ``<bucket>/<prefix>`` to write under.

    Returns
    -------
    int
        Number of files uploaded.
    """
    files = sorted(p for p in local_root.rglob("*") if p.is_file())
    if not files:
        return 0
    remote = [f"{remote_root.rstrip('/')}/{p.relative_to(local_root).as_posix()}" for p in files]
    fs.put([str(p) for p in files], remote)
    return len(files)


def archive_existing(fs, asset_root: str, filename: str, today: date | None = None) -> str | None:
    """Keep the asset's current ``filename`` under ``original_metadata/`` before replacing it.

    Follows the AIND convention ``original_metadata/<stem>.<YYYYMMDD>.json``. An archive
    already written today is left as it is, so a re-run keeps the version from before the
    first run of the day.

    Parameters
    ----------
    fs : fsspec.AbstractFileSystem
        The asset's filesystem.
    asset_root : str
        ``<bucket>/<name>``.
    filename : str
        A core metadata file at the asset root, e.g. ``processing.json``.
    today : date | None, optional
        Archive date; defaults to today in UTC.

    Returns
    -------
    str | None
        The archive path, or ``None`` when there was nothing to archive.
    """
    current = f"{asset_root}/{filename}"
    if not fs.exists(current):
        return None
    stem = filename.rsplit(".", 1)[0]
    archive = f"{asset_root}/{ARCHIVE_DIR}/{stem}.{(today or datetime.now(timezone.utc).date()):%Y%m%d}.json"
    if fs.exists(archive):
        logger.info("Archive %s already exists; keeping it", archive)
        return archive
    fs.copy(current, archive)
    return archive
