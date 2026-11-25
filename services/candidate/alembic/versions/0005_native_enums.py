"""native enum (design v2.2): resumes.status -> resume_status

content_type stays text + CHECK: enum labels are limited to 63 bytes and the DOCX MIME type is 71.

Revision ID: 0005
Revises: 0004
Create Date: 2026-11-26
"""
from alembic import op
from smarthire_common.migration_ops import column_to_enum, column_to_text, create_enum, drop_enum

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint(op.f("ck_resumes_status"), "resumes", type_="check")  # the type rejects other values now
    create_enum("resume_status", ("UPLOADED", "PARSED", "FAILED"))
    column_to_enum("resumes", "status", "resume_status", default="UPLOADED")


def downgrade() -> None:
    column_to_text("resumes", "status", default="UPLOADED")
    drop_enum("resume_status")
    op.create_check_constraint(op.f("ck_resumes_status"), "resumes", "status IN ('UPLOADED', 'PARSED', 'FAILED')")
