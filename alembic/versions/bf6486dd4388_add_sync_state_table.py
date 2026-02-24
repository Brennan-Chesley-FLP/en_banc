"""add sync_state table

Revision ID: bf6486dd4388
Revises: b3e7d1a24f09
Create Date: 2026-02-23 21:50:10.004904

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'bf6486dd4388'
down_revision: Union[str, Sequence[str], None] = 'b3e7d1a24f09'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table('sync_state',
        sa.Column('table_name', sa.Text(), nullable=False),
        sa.Column('last_provenance', sa.BigInteger(), server_default='0', nullable=False),
        sa.Column('last_correction', sa.BigInteger(), server_default='0', nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('table_name'),
        schema='warehouse'
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table('sync_state', schema='warehouse')
