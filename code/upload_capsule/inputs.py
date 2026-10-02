"""Identify the asset this run publishes to, from the manifest and the asset itself."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

SUBJECT_FILES = ("subject.json", "data_description.json")
"""Files at the asset root that name its subject, in the order tried."""


class InputError(ValueError):
    """Raised when the run's inputs do not identify one asset."""


@dataclass(frozen=True)
class Asset:
    """The processed dataset this run reads from and publishes to.

    Attributes
    ----------
    bucket : str
        Bucket holding it.
    name : str
        Dataset name: the first key segment, whatever its naming convention.
    """

    bucket: str
    name: str

    @property
    def root(self) -> str:
        """``<bucket>/<name>``, the path form s3fs uses."""
        return f"{self.bucket}/{self.name}"

    @property
    def uri(self) -> str:
        """``s3://<bucket>/<name>/``."""
        return f"s3://{self.root}/"


def find_manifest(data_dir: Path) -> Path:
    """Find the one manifest under ``data_dir``.

    A manifest is a top-level JSON file with ``zarr_multiscale.input_uri``. Other JSON
    files may be mounted beside it, so the first file found is not assumed to be it.

    Parameters
    ----------
    data_dir : Path
        The capsule's ``/data``.

    Returns
    -------
    Path
        The manifest.

    Raises
    ------
    InputError
        If there is no manifest, or more than one.
    """
    found = []
    for path in sorted(data_dir.glob("*.json")):
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(document, dict) and (document.get("zarr_multiscale") or {}).get("input_uri"):
            found.append(path)
    if len(found) != 1:
        raise InputError(
            f"Expected one manifest with zarr_multiscale.input_uri in {data_dir}, "
            f"found {len(found)}: {[p.name for p in found]}"
        )
    return found[0]


def asset_from_manifest(manifest: Path) -> Asset:
    """Read the target asset from the manifest's ``input_uri``.

    Parameters
    ----------
    manifest : Path
        The manifest.

    Returns
    -------
    Asset
        The asset the input URI lies in.

    Raises
    ------
    InputError
        If the URI is not an ``s3://`` URI with a key.
    """
    uri = json.loads(manifest.read_text(encoding="utf-8"))["zarr_multiscale"]["input_uri"]
    parsed = urlparse(str(uri))
    key = parsed.path.lstrip("/")
    if parsed.scheme != "s3" or not parsed.netloc or not key:
        raise InputError(f"input_uri is not an s3:// URI inside a dataset: {uri!r}")
    return Asset(bucket=parsed.netloc, name=key.split("/")[0])


def subject_id(fs, asset: Asset) -> str:
    """Read the asset's subject from its own metadata, not from its name.

    Parameters
    ----------
    fs : fsspec.AbstractFileSystem
        Filesystem the asset lives on.
    asset : Asset
        The asset.

    Returns
    -------
    str
        The subject id.

    Raises
    ------
    InputError
        If neither ``subject.json`` nor ``data_description.json`` names a subject.
    """
    for filename in SUBJECT_FILES:
        try:
            with fs.open(f"{asset.root}/{filename}", "r") as handle:
                subject = str(json.load(handle).get("subject_id") or "")
        except (FileNotFoundError, OSError, json.JSONDecodeError):
            continue
        if subject:
            return subject
    raise InputError(f"No subject_id in {asset.uri}{' or '.join(SUBJECT_FILES)}")
