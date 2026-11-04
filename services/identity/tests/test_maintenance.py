"""Step 14.3b: refresh tokens expired or revoked more than 30 days ago are deleted; nothing else."""

import uuid

from smarthire_common import db, testing
from sqlalchemy import text

from app import maintenance


async def test_old_refresh_tokens_are_deleted(migrated, database_url):
    await testing.truncate(database_url, ["users", "recruiter_profiles", "refresh_tokens", "outbox"])
    db.init_db(database_url)  # no app runs in this test
    try:
        user_id = uuid.uuid4()
        async with db.session_scope() as s:
            await s.execute(text("INSERT INTO users (id, email, password_hash, role) VALUES (:id, 'old@example.com', 'x', 'CANDIDATE')"),
                            {"id": user_id})
            # (name, expires in N days, revoked N days from now): negative = in the past
            for name, expires, revoked in [("expired long ago", -31, None), ("revoked long ago", 5, -31),
                                           ("expired recently", -10, None), ("active", 7, None)]:
                await s.execute(text(
                    "INSERT INTO refresh_tokens (id, user_id, family_id, token_hash, user_agent, expires_at, revoked_at) "
                    "VALUES (:id, :user, :id, :hash, :name, now() + make_interval(days => :expires), now() + make_interval(days => :revoked))"),
                    {"id": uuid.uuid4(), "user": user_id, "hash": name.encode(), "name": name, "expires": expires, "revoked": revoked})
        assert await maintenance.run_all(maintenance.JOBS) == {"outbox": 0, "refresh_tokens": 2}
        async with db.session_scope() as s:
            left = (await s.execute(text("SELECT user_agent FROM refresh_tokens ORDER BY user_agent"))).scalars().all()
        assert left == ["active", "expired recently"]
    finally:
        await db.close_db()
