from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool, text
from sqlmodel import SQLModel

from alembic import context

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Import warehouse models so their tables register on SQLModel.metadata.
# The register module triggers raw_table_from_model calls which also
# register on SQLModel.metadata.
import warehouse.models  # noqa: F401
import warehouse.register  # noqa: F401

target_metadata = SQLModel.metadata

# Schemas managed by Alembic migrations
MANAGED_SCHEMAS = ("warehouse", "courtlistener", "ala_publicportal", "conn_jud_ct_gov")


def include_name(name, type_, parent_names):
    """Filter autogenerate to only include our managed schemas."""
    if type_ == "schema":
        return name in MANAGED_SCHEMAS
    return True


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_schemas=True,
        include_name=include_name,
        version_table_schema="warehouse",
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        # Create schemas before running migrations
        for schema in MANAGED_SCHEMAS:
            connection.execute(
                text(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            )
        connection.commit()

        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_schemas=True,
            include_name=include_name,
            version_table_schema="warehouse",
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
