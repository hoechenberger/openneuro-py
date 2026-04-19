"""Pydantic models for validating OpenNeuro GraphQL API responses.

Only the inner payload objects (snapshots, files, errors) are modeled here.
The outer GraphQL response envelope (``{"data": {"dataset": ...}}``) is
traversed with plain dict access in the download module (keeps complexity lower).
"""

from pydantic import BaseModel


class GraphQLError(BaseModel):
    """A single error entry from a GraphQL error response."""

    message: str


class FileInfo(BaseModel):
    """Metadata for a single file in a dataset snapshot."""

    filename: str
    urls: list[str]
    size: int
    id: str


class Snapshot(BaseModel):
    """A dataset snapshot containing an ID and a list of files."""

    id: str
    files: list[FileInfo]


class SnapshotListItem(BaseModel):
    """Minimal snapshot entry used when listing available snapshots."""

    id: str
