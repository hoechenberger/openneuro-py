"""Tests for openneuro._models Pydantic response models."""

from openneuro._models import (
    FileInfo,
    GraphQLError,
    Snapshot,
    SnapshotListItem,
)
from tests.utils import load_json


def test_graphql_error():
    """Parse a minimal error payload."""
    err = GraphQLError.model_validate({"message": "Something went wrong"})
    assert err.message == "Something went wrong"


def test_file_info():
    """Parse a complete file-info payload."""
    fi = FileInfo.model_validate(
        {
            "filename": "sub-01/anat/T1w.nii.gz",
            "urls": ["https://example.com/file"],
            "size": 1234,
            "id": "abc123",
        }
    )
    assert fi.filename == "sub-01/anat/T1w.nii.gz"
    assert fi.urls == ["https://example.com/file"]
    assert fi.size == 1234
    assert fi.id == "abc123"


def test_snapshot_from_mock_data():
    """Parse real mock metadata into a Snapshot."""
    data = load_json("mock_metadata_ds000117.json")
    snap = Snapshot.model_validate(data)
    assert snap.id == "ds000117:1.1.0"
    assert len(snap.files) > 0
    assert all(isinstance(f, FileInfo) for f in snap.files)


def test_snapshot_list_item():
    """Parse a minimal snapshot list item."""
    item = SnapshotListItem.model_validate({"id": "ds000117:1.0.0"})
    assert item.id == "ds000117:1.0.0"
