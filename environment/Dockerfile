# hash:sha256:3bd65a6ed0c112fdaa291b29e439fdbfa6a3a2157738a0a0e532d5377a2ce126
# Upload capsule env — Python 3.10 for the v2 metadata stack. Metadata is built
# by the official aind-metadata-manager (+ aind-metadata-upgrader), which run on
# aind-data-schema v2. v2 + models v5 are GitHub tags (NOT on PyPI) and need
# Python >=3.10. Slimmed: main.py only needs s3fs + dataset-utils + the manager.
ARG REGISTRY_HOST
FROM $REGISTRY_HOST/codeocean/mambaforge3:23.1.0-4-python3.10.12-ubuntu22.04

# === TEMPORARY (TEST) — REMOVE BEFORE PRODUCTION ===
# Write all outputs to the scratch test dir instead of the aind-open-data asset;
# upstream metadata is still read from aind-open-data. Delete to restore production.
ENV OUTPUT_PREFIX=s3://aind-scratch-data/exaspim_processing_test

ARG DEBIAN_FRONTEND=noninteractive
ARG AWS_ACCESS_KEY_ID
ARG AWS_DEFAULT_REGION
ARG AWS_SECRET_ACCESS_KEY

# (Removed COPY git-askpass / + GIT_* ARGs: all git pip-installs below are public
#  AllenNeuralDynamics repos, so no build-time git auth is needed. The git-askpass
#  helper was a CO-only build artifact not present in this GitHub-sourced capsule.)

RUN conda install -y --channel=conda-forge \
        s3fs==2023.9.2 \
    && conda clean -ya

# aind-data-schema v2 + its models v5 are git-tag-only (not on PyPI). Install
# models FIRST, then the schema (ordering matters).
RUN pip install -U --no-cache-dir \
    "aind-data-schema-models @ git+https://github.com/AllenNeuralDynamics/aind-data-schema-models.git@v5.7.2" \
    "aind-data-schema @ git+https://github.com/AllenNeuralDynamics/aind-data-schema.git@v2.7.1" \
    pydantic-settings \
    s3fs==2023.9.2 \
    openpyxl==3.1.5 \
    aind-exaspim-dataset-utils@git+https://github.com/AllenNeuralDynamics/aind-exaspim-dataset-utils.git@main \
    awscli

# Official AIND metadata tooling (git-only; deps already satisfied above -> --no-deps).
RUN pip install -U --no-cache-dir --no-deps \
    "aind-metadata-upgrader @ git+https://github.com/AllenNeuralDynamics/aind-metadata-upgrader.git@main" \
    "aind-metadata-manager @ git+https://github.com/AllenNeuralDynamics/aind-metadata-manager.git@main"
