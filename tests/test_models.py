"""Tests for openneuro._models Pydantic response models."""

import pytest
from pydantic import ValidationError

from openneuro._models import (
    FileInfo,
    GraphQLError,
    Snapshot,
    SnapshotListItem,
)
from tests.utils import load_json

# -- Fixtures --


@pytest.fixture
def snapshot_data() -> dict:
    """Load mock snapshot data (inner object, no GraphQL envelope)."""
    data = load_json("mock_metadata_ds000117.json")
    assert isinstance(data, dict)
    return data


# -- GraphQLError --


def test_graphql_error_valid():
    """Parse a minimal error payload."""
    err = GraphQLError.model_validate({"message": "Something went wrong"})
    assert err.message == "Something went wrong"


def test_graphql_error_missing_message():
    """Reject a payload missing the required 'message' field."""
    with pytest.raises(ValidationError):
        GraphQLError.model_validate({})


# -- FileInfo --


def test_file_info_valid():
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


def test_file_info_missing_field():
    """Reject a payload with a missing required field."""
    with pytest.raises(ValidationError):
        FileInfo.model_validate(
            {"filename": "test.txt", "urls": [], "size": 100}
            # missing "id"
        )


def test_file_info_wrong_type():
    """Reject a payload with wrong field types."""
    with pytest.raises(ValidationError):
        FileInfo.model_validate(
            {"filename": "test.txt", "urls": "not-a-list", "size": 100, "id": "x"}
        )


# -- Snapshot --


def test_snapshot_valid():
    """Parse a minimal snapshot payload."""
    snap = Snapshot.model_validate(
        {
            "id": "ds000117:1.0.0",
            "files": [
                {
                    "filename": "README",
                    "urls": ["https://example.com/README"],
                    "size": 100,
                    "id": "f1",
                }
            ],
        }
    )
    assert snap.id == "ds000117:1.0.0"
    assert len(snap.files) == 1
    assert isinstance(snap.files[0], FileInfo)


def test_snapshot_from_mock_data(snapshot_data: dict):
    """Parse real mock metadata into a Snapshot."""
    snap = Snapshot.model_validate(snapshot_data)
    assert snap.id == "ds000117:1.1.0"
    assert len(snap.files) > 0
    assert all(isinstance(f, FileInfo) for f in snap.files)


def test_snapshot_missing_files():
    """Reject a snapshot payload without 'files'."""
    with pytest.raises(ValidationError):
        Snapshot.model_validate({"id": "ds000117:1.0.0"})


# -- SnapshotListItem --


def test_snapshot_list_item_valid():
    """Parse a minimal snapshot list item."""
    item = SnapshotListItem.model_validate({"id": "ds000117:1.0.0"})
    assert item.id == "ds000117:1.0.0"


def test_snapshot_list_item_missing_id():
    """Reject a snapshot list item without 'id'."""
    with pytest.raises(ValidationError):
        SnapshotListItem.model_validate({})
