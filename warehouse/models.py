"""Warehouse infrastructure models managed by Alembic."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import Column, BigInteger, DateTime, Index, Text, func
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
        default=None, sa_column=Column("metadata", JSONB)
    )
    created_at: datetime | None = Field(
        default=None,
        sa_column=Column(
            DateTime(timezone=True), server_default=func.now(), nullable=False
        ),
    )
