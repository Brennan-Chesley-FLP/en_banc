"""Factory for deriving warehouse raw tables from scraper output models.

Each scraper output model is a SQLModel(table=False) class. The factory
creates a table=True subclass that adds the standard warehouse columns
(row_id, content_hash) and places the table in the scraper's
PostgreSQL schema.

Data is deduplicated on ``content_hash``.  A paired observations table
tracks which scraper runs (provenance entries) observed each row.
"""

from __future__ import annotations

from types import UnionType
from typing import ClassVar, Union, get_args, get_origin

from sqlalchemy import Column, BigInteger, ForeignKey, Index, JSON, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field, SQLModel
from sqlmodel.main import SQLModelMetaclass, default_registry


def _find_jsonb_fields(model: type[SQLModel]) -> dict[str, Field]:
    """Find model fields whose types need JSONB storage.

    SQLModel doesn't auto-map list/dict types to SQL columns.
    This function detects them and returns explicit sa_column
    overrides so they become JSONB columns in PostgreSQL.
    """
    overrides: dict[str, Field] = {}
    annotations: dict[str, type] = {}

    for name, field_info in model.model_fields.items():
        ann = field_info.annotation

        # Unwrap Optional[X] to inspect the inner type
        inner = ann
        origin = get_origin(inner)
        if origin is Union:
            args = [a for a in get_args(inner) if a is not type(None)]
            if len(args) == 1:
                inner = args[0]
                origin = get_origin(inner)

        if origin in (list, dict, set, tuple):
            overrides[name] = Field(
                default=field_info.default,
                sa_column=Column(JSON().with_variant(JSONB(), "postgresql")),
            )
            # Keep the annotation as the nullable JSON type
            annotations[name] = field_info.annotation

    return overrides, annotations


def _create_observations_table(
    schema_name: str,
    raw_table_name: str,
) -> type[SQLModel]:
    """Create an observations junction table for a raw table.

    The observations table records which provenance entries (scraper runs)
    observed each data row, enabling M2M tracking without duplicating
    the full row data.

    Args:
        schema_name: PostgreSQL schema (e.g. ``"ala_publicportal"``).
        raw_table_name: Name of the raw table (e.g. ``"raw_dockets"``).

    Returns:
        A new SQLModel(table=True) class for the observations table.
    """
    obs_table_name = f"{raw_table_name}_observations"
    obs_class_name = f"{schema_name}__{obs_table_name}"

    # FK target: e.g. "ala_publicportal.raw_dockets.row_id"
    raw_fk_target = f"{schema_name}.{raw_table_name}.row_id"

    attrs = {
        "__tablename__": obs_table_name,
        "__table_args__": (
            Index(f"ix_{obs_table_name}_provenance", "provenance_id"),
            {"schema": schema_name},
        ),
        "__annotations__": {
            "row_id": int,
            "provenance_id": int,
            "record_id": int,
            "registry": ClassVar,
        },
        "registry": default_registry,
        "row_id": Field(
            sa_column=Column(
                BigInteger,
                ForeignKey(raw_fk_target),
                primary_key=True,
            ),
        ),
        "provenance_id": Field(
            sa_column=Column(
                BigInteger,
                ForeignKey("warehouse.provenance.id"),
                primary_key=True,
            ),
        ),
        "record_id": Field(
            sa_column=Column(BigInteger, nullable=False),
        ),
    }

    return SQLModelMetaclass(obs_class_name, (SQLModel,), attrs, table=True)


def raw_table_from_model(
    model: type[SQLModel],
    schema_name: str,
    table_name: str,
) -> type[SQLModel]:
    """Derive a table=True SQLModel from a table=False scraper output model.

    Inherits all fields from the source model and adds the standard
    warehouse columns (row_id, content_hash).  Rows are
    deduplicated on ``content_hash``; observation tracking is handled
    by a paired ``{table_name}_observations`` table created
    automatically and accessible via ``cls._observations_cls``.

    Fields with list/dict types are automatically overridden to use
    JSONB columns in PostgreSQL.

    Args:
        model: A SQLModel(table=False) scraper output model class.
        schema_name: PostgreSQL schema name (e.g. "ala_publicportal").
        table_name: Table name within the schema (e.g. "raw_dockets").

    Returns:
        A new SQLModel(table=True) class registered on SQLModel.metadata.
    """
    # Detect list/dict fields that need explicit JSONB columns
    jsonb_overrides, jsonb_annotations = _find_jsonb_fields(model)

    attrs = {
        "__tablename__": table_name,
        "__table_args__": ({"schema": schema_name},),
        "__annotations__": {
            "row_id": int | None,
            "content_hash": str,
            # registry must be annotated as ClassVar so Pydantic
            # doesn't treat it as a model field.
            "registry": ClassVar,
            **jsonb_annotations,
        },
        # SQLAlchemy's DeclarativeMeta needs a registry to map the
        # class to the ORM. Normally SQLModel.__init_subclass__ provides
        # this, but we bypass it by calling SQLModelMetaclass directly.
        "registry": default_registry,
        "row_id": Field(
            default=None,
            sa_column=Column(BigInteger, primary_key=True, autoincrement=True),
        ),
        "content_hash": Field(
            sa_column=Column(Text, unique=True, nullable=False),
        ),
        **jsonb_overrides,
    }

    # Use SQLModelMetaclass directly with table=True so SQLAlchemy
    # table registration happens during class creation.
    # Class name is schema-qualified to avoid collisions when multiple
    # scrapers have tables with the same name (e.g. raw_dockets).
    class_name = f"{schema_name}__{table_name}"
    raw_cls = SQLModelMetaclass(class_name, (model,), attrs, table=True)

    # Create the paired observations table and stash a reference
    raw_cls._observations_cls = _create_observations_table(
        schema_name, table_name
    )

    return raw_cls


def discover_output_models(scraper_class: type) -> list[type]:
    """Extract all output model types from a BaseScraper subclass.

    BaseScraper is Generic[ScraperReturnType], where ScraperReturnType
    is a union of the output model types. For example:

        class AlabamaScraper(BaseScraper[
            AlaOpinionCluster | AlaOralArgument | AlaDocket | AlaHistoricalReleaseList
        ]): ...

    Returns [AlaOpinionCluster, AlaOralArgument, AlaDocket, AlaHistoricalReleaseList].
    """
    from kent.data_types import BaseScraper

    for base in getattr(scraper_class, "__orig_bases__", []):
        origin = get_origin(base)
        if origin is None:
            continue
        if not (isinstance(origin, type) and issubclass(origin, BaseScraper)):
            continue

        args = get_args(base)
        if not args:
            continue

        return_type = args[0]
        type_origin = get_origin(return_type)

        if type_origin is Union or isinstance(return_type, UnionType):
            return [t for t in get_args(return_type) if isinstance(t, type)]

        if isinstance(return_type, type):
            return [return_type]

    return []
