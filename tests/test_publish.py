"""Curation, upload and archiving."""

from conftest import BUCKET, make_data
from upload_capsule import publish
from upload_capsule.config import STAGES


def test_only_whitelisted_files_are_curated(tmp_path):
    data = make_data(tmp_path / "data", "d")
    ccf = next(s for s in STAGES if s.name == "ccf_alignment")
    copied = {
        p.as_posix() for p in publish.curate(data / "ccf_alignment", tmp_path / "pub", ccf.patterns)
    }
    assert "841260_to_exaSPIM_SyN_0GenericAffine.mat" in copied
    assert "ccf_aligned.zarr" in copied
    assert "ccf_mesh_to_sample/a/x.obj" in copied
    assert (tmp_path / "pub" / "ccf_aligned.zarr" / "0" / "chunk").exists()
    assert not (tmp_path / "pub" / "scratch_not_published.nii.gz").exists()
    assert not (tmp_path / "pub" / "ccf_mesh_to_sample" / "qc").exists()


def test_a_folder_with_one_file_keeps_its_folder(s3, tmp_path):
    (tmp_path / "pub" / "soma_detection").mkdir(parents=True)
    (tmp_path / "pub" / "soma_detection" / "soma_locations.csv").write_text("x")
    assert publish.upload_tree(s3, tmp_path / "pub", f"{BUCKET}/d") == 1
    assert s3.isfile(f"{BUCKET}/d/soma_detection/soma_locations.csv")
