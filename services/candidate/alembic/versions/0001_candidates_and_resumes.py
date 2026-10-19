"""candidates and resumes (Appendix E.6)

Revision ID: 0001
Revises:
Create Date: 2026-10-19
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

DOCX = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"


def upgrade() -> None:
    # op.f("..."): the name is final (see CLAUDE.md, MIGRATION NAMING RULE)
    op.create_table(
        "candidates",
        sa.Column("id", sa.Uuid(), nullable=False),  # = Identity user_id, no FK (other database)
        sa.Column("full_name", sa.Text(), server_default=sa.text("''"), nullable=False),
        sa.Column("headline", sa.Text(), nullable=True),
        sa.Column("skills", postgresql.JSONB(), server_default=sa.text("'[]'"), nullable=False),
        sa.Column("experience_years", sa.Numeric(4, 1), nullable=True),
        sa.Column("education", postgresql.JSONB(), server_default=sa.text("'[]'"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_candidates"),
        sa.CheckConstraint("experience_years >= 0", name=op.f("ck_candidates_experience_years_non_negative")),
    )

    op.create_table(
        "resumes",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("candidate_id", sa.Uuid(), nullable=False),
        sa.Column("object_key", sa.Text(), nullable=False),
        sa.Column("file_name", sa.Text(), nullable=False),
        sa.Column("content_type", sa.Text(), nullable=False),
        sa.Column("size_bytes", sa.Integer(), nullable=False),
        sa.Column("status", sa.Text(), server_default=sa.text("'UPLOADED'"), nullable=False),
        sa.Column("parsed_data", postgresql.JSONB(), nullable=True),
        sa.Column("parse_error", sa.Text(), nullable=True),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("uploaded_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("parsed_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name="pk_resumes"),
        sa.ForeignKeyConstraint(
            ["candidate_id"], ["candidates.id"], name="fk_resumes_candidate_id_candidates", ondelete="CASCADE"
        ),
        sa.UniqueConstraint("object_key", name="uq_resumes_object_key"),
        sa.CheckConstraint(f"content_type IN ('application/pdf', '{DOCX}')", name=op.f("ck_resumes_content_type")),
        sa.CheckConstraint("size_bytes > 0 AND size_bytes <= 5242880", name=op.f("ck_resumes_size_bytes")),
        sa.CheckConstraint("status IN ('UPLOADED', 'PARSED', 'FAILED')", name=op.f("ck_resumes_status")),
    )
    # One active resume per candidate: a UNIQUE index over the active rows only
    op.create_index(
        "uq_resumes_one_active", "resumes", ["candidate_id"], unique=True, postgresql_where=sa.text("is_active")
    )
    op.create_index("ix_resumes_candidate_uploaded", "resumes", ["candidate_id", sa.text("uploaded_at DESC")])


def downgrade() -> None:
    op.drop_index("ix_resumes_candidate_uploaded", table_name="resumes")
    op.drop_index("uq_resumes_one_active", table_name="resumes")
    op.drop_table("resumes")  # child first: it references candidates
    op.drop_table("candidates")
