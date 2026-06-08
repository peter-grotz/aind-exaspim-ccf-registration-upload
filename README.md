# aind-exaspim-ccf-registration-upload

Aggregates the pipeline's metadata into a validated top-level **`processing.json`**
(via the official `aind-metadata-manager` on `aind-data-schema` v2) and
**publishes** the spec-compliant subset of results to the asset's S3 location.
Runs on Python 3.10 + the v2 schema stack. Part of the exaSPIM CCF-registration
+ soma-reg pipeline (Code Ocean pipeline `9578158`).

## Run it standalone

```bash
cd code && ./run        # = python -u main.py
```

What it does: stages producer `*_data_process.json` from `../data`, fetches
upstream `processing.json` from the asset's S3, runs the manager to build the
aggregated `processing.json` (fixes the dependency graph + backfills Code
versions), curates the publish set, and uploads to S3.

## Inputs
- `../data/<exaspim_manifest>.json` — `zarr_multiscale.input_uri` → the asset.
- Producer outputs passed via the pipeline's `/results`: `../data/ccf_alignment/`,
  `../data/soma_detection/`, `../data/fusion/` (the fusion `*_data_process.json`).
- Upstream `processing.json` is read from the input asset on S3.
- **AWS credentials** with S3 read (input asset) + write (output target).

## Environment variables
- `OUTPUT_PREFIX` *(optional)* — if set, publishes to
  `<OUTPUT_PREFIX>/<asset>/` (scratch test dir); if unset, to the `aind-open-data`
  asset (production).
- `PIPELINE_NAME` (default `exaspim-data-processing`), `PIPELINE_VERSION`,
  `PIPELINE_URL`, `PROCESSOR_FULL_NAME` — stamped into `processing.json`.
  `PIPELINE_NAME` **must match** the `pipeline_name` the producers emit.
- `SMARTSHEET_TOKEN` *(optional)* — if set, updates the tracking sheet; leave
  unset for tests.

## Outputs (to the resolved S3 target)
- `processing.json` — full aggregated lineage (asset root).
- `ccf_alignment/` — whitelisted transforms + zarr + `ccf_anno_to_sample/` +
  a stage-scoped `processing.json`.
- `soma_detection/soma_locations.csv`.

## Notes
- Only this capsule needs the v2 schema stack (git-tag installs, Python ≥3.10);
  producer capsules emit plain JSON via the stdlib `aind_process_record.py`.
- Curation/reduction happens **only here** — producer `/results` are left intact.
