"""Warehouse infrastructure models managed by Alembic."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import Column, BigInteger, DateTime, Index, JSON, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlmodel import Field, SQLModel


class Provenance(SQLModel, table=True):
    """Provenance tracking for all warehouse data.

    Every row in every warehouse layer links to a provenance record
    identifying the run, source, and artifact that produced it.
    """

    __tablename__ = "provenance"
    __table_args__ = (
        Index("ix_provenance_source", "source_type", "source_name"),
        Index("ix_provenance_run_id", "run_id"),
        {"schema": "warehouse"},
    )

    id: int | None = Field(
        default=None,
        sa_column=Column(BigInteger, primary_key=True, autoincrement=True),
    )
    source_type: str = Field(sa_column=Column(Text, nullable=False))
    source_name: str = Field(sa_column=Column(Text, nullable=False))
    run_id: UUID | None = Field(
        default=None, sa_column=Column(PG_UUID(as_uuid=True))
    )
    s3_artifact_path: str | None = Field(default=None, sa_column=Column(Text))
    description: str | None = Field(default=None, sa_column=Column(Text))
    metadata_: dict | None = Field(
        default=None, sa_column=Column("metadata", JSON().with_variant(JSONB(), "postgresql"))
    )
    created_at: datetime | None = Field(
        default=None,
        sa_column=Column(
            DateTime(timezone=True), server_default=func.now(), nullable=False
        ),
    )


class SyncState(SQLModel, table=True):
    """Per-table high-water marks for the CL sync worker.

    Tracks the last-synced version_provenance (and eventually
    version_correction) so the sync flow only processes new rows.
    """

    __tablename__ = "sync_state"
    __table_args__ = {"schema": "warehouse"}

    table_name: str = Field(sa_column=Column(Text, primary_key=True))
    last_provenance: int = Field(
        default=0, sa_column=Column(BigInteger, nullable=False, server_default="0")
    )
    last_correction: int = Field(
        default=0, sa_column=Column(BigInteger, nullable=False, server_default="0")
    )
    updated_at: datetime | None = Field(
        default=None,
        sa_column=Column(
            DateTime(timezone=True), server_default=func.now(), nullable=False
        ),
    )
