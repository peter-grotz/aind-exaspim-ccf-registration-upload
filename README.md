# aind-exaspim-ccf-registration-upload

Publishes the exaSPIM CCF-registration pipeline's results back to their asset on
`aind-open-data`, with one validated `processing.json` describing how they were made.
The last stage of Code Ocean pipeline `9578158`.

## What it does

1. **Finds the asset** from the manifest's `zarr_multiscale.input_uri`: the dataset is the
   URI's first key segment, whatever its name. The subject comes from the asset's own
   `subject.json` (or `data_description.json`), never from its name.
2. **Builds the metadata with `aind-metadata-manager`**, in one pass
   (`code/upload_capsule/metadata.py`). The producers' `*_data_process.json` records and
   the asset's upstream `processing.json` files (`tile_alignment/`, `fusion/`,
   `flatfield_correction/`, `denoised/`) go in together; the manager validates and merges
   them, keeping each upstream document's dependency graph. It cannot know the edges
   between bare records, so those come from `DEPENDENCIES` in
   `code/upload_capsule/config.py`. Each published subfolder's `processing.json` is a
   slice of that document.
3. **Checks before publishing.** An invalid record, a record with no `DEPENDENCIES` entry,
   or a missing atlas alignment fails the run with nothing written.
4. **Publishes** each stage's whitelisted files and stage `processing.json`, and the root
   `processing.json`, replacing what is there.
5. **Updates the tracking sheet** when `SMARTSHEET_TOKEN` is set.

## Inputs (`/data`)

| Path | From |
|---|---|
| `exaspim_manifest.json` | the run's manifest from `cp_jsons`, passed on by registration (override with `--manifest`) |
| `ccf_alignment/` | registration: outputs and records |
| `soma_detection/` | soma→CCF: `soma_locations.csv` |
| `soma_detection_meta/` | soma detection: records (`SOMA_META_DIR`) |
| `fusion/` | CCF-channel fusion: record only; nothing is republished |

AWS credentials need read and write on the asset.

## Outputs (to the asset)

- `processing.json` — the whole lineage.
- `ccf_alignment/` — transforms, `ccf_aligned.zarr`, annotation and meshes in sample
  space, and the stage `processing.json`.
- `soma_detection/` — `soma_locations.csv` and the stage `processing.json`.

## Configuration

| Variable | Default | |
|---|---|---|
| `PIPELINE_NAME` | `exaspim-data-processing` | Must match the producers' `pipeline_name`. |
| `PIPELINE_VERSION`, `PIPELINE_URL`, `PROCESSOR_FULL_NAME` | | Recorded in `processing.json`. |
| `SOMA_META_DIR` | `soma_detection_meta` | Mount of the soma-detection records. |
| `DRY_RUN` | off | Build everything into `/results/_publish` and write nothing to the asset. Same as `--dry-run`. |
| `SMARTSHEET_TOKEN` | | Optional; a Code Ocean secret. |

## Changing a producer

Add or rename a producer's process in `DEPENDENCIES`, and its published files in
`STAGES` (`code/upload_capsule/config.py`).

## Tests

Need Python ≥3.10 and the pinned stack from `environment/Dockerfile`, plus `pytest`:

```bash
python -m pytest tests
```

They run the capsule end to end against a local stand-in for S3, using the records
published for sample 841260, and check the result reproduces that sample's published
`processing.json`.
