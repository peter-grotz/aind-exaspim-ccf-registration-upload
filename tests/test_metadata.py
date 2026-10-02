"""Stage documents and the aggregate, built through aind-metadata-manager."""

import json

import pytest
from conftest import RESOURCES
from upload_capsule import metadata
from upload_capsule.config import Pipeline
from upload_capsule.metadata import MetadataError

PIPELINE = Pipeline("exaspim-data-processing", "0.0.0", "https://example.org/pipeline", "AIND")
RECORDS = RESOURCES / "records"


def _stages(tmp_path):
    return {
        stage: metadata.build_stage_document(
            metadata.load_records(RECORDS / folder, PIPELINE.name), tmp_path / stage, PIPELINE
        )
        for stage, folder in (
            ("ccf_alignment", "ccf_alignment"),
            ("soma_detection", "soma"),
            ("ccf_fusion", "fusion"),
        )
    }


def _upstream(tmp_path):
    paths = []
    for sub in ("fusion", "flatfield_correction", "denoised"):
        path = tmp_path / "upstream" / sub / "processing.json"
        path.parent.mkdir(parents=True)
        path.write_text((RESOURCES / f"upstream_{sub}.json").read_text())
        paths.append(path)
    return paths


def test_a_foreign_pipeline_name_is_replaced(tmp_path):
    record = json.loads(next((RECORDS / "soma").glob("*.json")).read_text())
    record["pipeline_name"] = "exaspim-soma-detection"
    (tmp_path / "x_data_process.json").write_text(json.dumps(record))
    (loaded,) = metadata.load_records(tmp_path, PIPELINE.name)
    assert loaded.document["pipeline_name"] == PIPELINE.name


def test_invalid_records_fail_together(tmp_path):
    record = json.loads(next((RECORDS / "soma").glob("*.json")).read_text())
    for name in ("a", "b"):
        (tmp_path / f"{name}_data_process.json").write_text(
            json.dumps({**record, "process_type": "Soma detection"})
        )
    with pytest.raises(MetadataError) as error:
        metadata.load_records(tmp_path, PIPELINE.name)
    assert "a_data_process.json" in str(error.value)
    assert "b_data_process.json" in str(error.value)


def test_an_absent_stage_has_no_records(tmp_path):
    assert metadata.load_records(tmp_path / "missing", PIPELINE.name) == []


def test_a_record_without_a_dependency_entry_fails(tmp_path):
    records = metadata.load_records(RECORDS / "fusion", PIPELINE.name)
    with pytest.raises(MetadataError, match="CCF channel fusion"):
        metadata.build_stage_document(records, tmp_path / "w", PIPELINE, dependencies={})


def test_stage_documents_carry_only_their_own_edges(tmp_path):
    stages = _stages(tmp_path)
    assert stages["ccf_alignment"].dependency_graph == {
        "Image atlas alignment - 25 um": [],
        "Image atlas alignment - 10 um": ["Image atlas alignment - 25 um"],
        "CCF annotation to sample space": ["Image atlas alignment - 25 um"],
        "CCF meshes to sample space": ["Image atlas alignment - 25 um"],
    }
    assert stages["ccf_fusion"].dependency_graph == {"CCF channel fusion": []}


def test_the_aggregate_reproduces_the_published_841260_document(tmp_path):
    root = metadata.aggregate(_stages(tmp_path), _upstream(tmp_path), tmp_path / "agg", PIPELINE)
    published = json.loads((RESOURCES / "published_root.json").read_text())
    assert sorted(p.name for p in root.data_processes) == sorted(
        p["name"] for p in published["data_processes"]
    )
    assert root.dependency_graph == published["dependency_graph"]
    assert [p.name for p in root.pipelines] == ["exaspim-data-processing"]


def test_edges_between_stages_are_added_in_the_aggregate(tmp_path):
    root = metadata.aggregate(_stages(tmp_path), _upstream(tmp_path), tmp_path / "agg", PIPELINE)
    assert root.dependency_graph["Image atlas alignment - 25 um"] == ["CCF channel fusion"]
    assert root.dependency_graph["Proposal generation"] == ["Image tile fusing"]


def test_missing_and_unversioned_processes_are_reported(tmp_path):
    root = metadata.aggregate(_stages(tmp_path), _upstream(tmp_path), tmp_path / "agg", PIPELINE)
    assert metadata.missing_processes(root, ["Image atlas alignment - 25 um", "Nope"]) == ["Nope"]
    assert metadata.unversioned_processes(root) == ["In-place multiscale generation"]
