"""add extracted_citations table

Revision ID: 0f95588863c9
Revises: 391c62effce1
Create Date: 2026-02-15 17:42:08.027792

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0f95588863c9'
down_revision: Union[str, Sequence[str], None] = '391c62effce1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create courtlistener.extracted_citations table."""
    op.create_table(
        'extracted_citations',
        sa.Column('court_id', sa.Text(), nullable=False),
        sa.Column('docket_number', sa.Text(), nullable=False),
        sa.Column('cluster_date_filed', sa.Date(), nullable=False),
        sa.Column('opinion_type', sa.Text(), nullable=False),
        sa.Column('author_str', sa.Text(), nullable=False),
        sa.Column('volume', sa.Text(), nullable=False),
        sa.Column('reporter', sa.Text(), nullable=False),
        sa.Column('page', sa.Text(), nullable=False),
        sa.Column('corrected_citation', sa.Text(), nullable=False),
        sa.Column('year', sa.SmallInteger(), nullable=True),
        sa.Column('court_from_cite', sa.Text(), nullable=True),
        sa.Column('plaintiff', sa.Text(), nullable=True),
        sa.Column('defendant', sa.Text(), nullable=True),
        sa.Column('extracted_at', sa.DateTime(timezone=True),
                  server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint(
            'court_id', 'docket_number', 'cluster_date_filed',
            'opinion_type', 'author_str', 'volume', 'reporter', 'page',
        ),
        schema='courtlistener',
    )


def downgrade() -> None:
    """Drop courtlistener.extracted_citations table."""
    op.drop_table('extracted_citations', schema='courtlistener')
