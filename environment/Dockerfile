# hash:sha256:3bd65a6ed0c112fdaa291b29e439fdbfa6a3a2157738a0a0e532d5377a2ce126
# Upload capsule env — bumped to Python 3.10 for the v2 aind-data-schema stack.
# v2 (Processing 2.2.8) + aind-data-schema-models v5 are GitHub tags, NOT on PyPI,
# and require Python >=3.10. Slimmed: the new main.py only needs s3fs +
# dataset-utils + the v2 builder (no heavy scientific stack).
ARG REGISTRY_HOST
FROM $REGISTRY_HOST/codeocean/mambaforge3:23.1.0-4-python3.10.12-ubuntu22.04

ARG DEBIAN_FRONTEND=noninteractive
ARG AWS_ACCESS_KEY_ID
ARG AWS_DEFAULT_REGION
ARG AWS_SECRET_ACCESS_KEY

ARG GIT_ASKPASS
ARG GIT_ACCESS_TOKEN
COPY git-askpass /

RUN conda install -y --channel=conda-forge \
        s3fs==2023.9.2 \
    && conda clean -ya

# aind-data-schema v2 + its models v5 are git-tag-only (not on PyPI). Install
# models FIRST, then the schema (ordering matters).
RUN pip install -U --no-cache-dir \
    "aind-data-schema-models @ git+https://github.com/AllenNeuralDynamics/aind-data-schema-models.git@v5.7.2" \
    "aind-data-schema @ git+https://github.com/AllenNeuralDynamics/aind-data-schema.git@v2.7.1" \
    s3fs==2023.9.2 \
    openpyxl==3.1.5 \
    aind-exaspim-dataset-utils@git+https://github.com/AllenNeuralDynamics/aind-exaspim-dataset-utils.git@main \
    awscli
