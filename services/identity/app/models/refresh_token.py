import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, LargeBinary, Text, func
from sqlalchemy.orm import Mapped, mapped_column
from smarthire_common.models import Base, new_id


class RefreshToken(Base):
    """Appendix E.4. One row per issued refresh token. Rows sharing a family_id belong to one
    login session: every refresh adds a row to the family; reuse of an old row kills the family."""

    __tablename__ = "refresh_tokens"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=new_id)
    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), index=True  # -> ix_refresh_tokens_user_id
    )
    family_id: Mapped[uuid.UUID] = mapped_column(index=True)  # -> ix_refresh_tokens_family_id
    # SHA-256 of the token, never the token itself: a leaked DB row cannot be replayed.
    # unique -> uq_refresh_tokens_token_hash, which is also the index used for lookups.
    token_hash: Mapped[bytes] = mapped_column(LargeBinary, unique=True)
    user_agent: Mapped[str | None] = mapped_column(Text)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # Append-only table (never edited except used/revoked markers): created_at but no updated_at
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
