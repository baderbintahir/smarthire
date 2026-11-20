"""Appendix E.3: every migration has a working downgrade, and the migrations produce exactly
what the models describe."""

from alembic import command


def test_upgrade_downgrade_upgrade(migrated):
    command.downgrade(migrated, "base")
    command.upgrade(migrated, "head")


def test_models_match_migrations(migrated):
    # Like `alembic check`: autogenerate compares models to the migrated DB; raises on any diff
    command.check(migrated)


async def test_schema_names_match_appendix_e4_exactly(migrated, database_url):
    from smarthire_common.testing import schema_object_names

    assert await schema_object_names(database_url, "identity") == {
        "pk_users", "ck_users_status", "uq_users_email_lower",
        "pk_roles", "uq_roles_name",
        "pk_user_roles", "fk_user_roles_user_id_users", "fk_user_roles_role_id_roles", "ix_user_roles_role_id",
        "pk_outbox", "uq_outbox_event_id", "ix_outbox_unpublished",
    }



def test_0005_backfills_roles_and_names(migrated, database_url):
    """Expand step: users created under the old schema get a user_roles row and a name.
    A sync test on purpose: Alembic's env.py runs its own event loop."""
    import uuid

    from smarthire_common.testing import run
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    async def sql(statement: str, params: dict | None = None):
        engine = create_async_engine(database_url)
        try:
            async with engine.begin() as conn:
                result = await conn.execute(text(statement), params or {})
                return result.all() if result.returns_rows else None
        finally:
            await engine.dispose()

    candidate, recruiter = uuid.uuid4(), uuid.uuid4()
    command.downgrade(migrated, "0004")
    try:
        run(sql("TRUNCATE users CASCADE"))
        run(sql("INSERT INTO users (id, email, password_hash, role) VALUES "
                "(:c, 'cand@x.com', 'h', 'CANDIDATE'), (:r, 'rec@x.com', 'h', 'RECRUITER')",
                {"c": candidate, "r": recruiter}))
        run(sql("INSERT INTO recruiter_profiles (user_id, display_name, company) VALUES (:r, 'Rita Recruiter', 'Acme')",
                {"r": recruiter}))
        command.upgrade(migrated, "head")
        rows = run(sql("SELECT u.email, u.full_name, u.current_company, r.name FROM users u "
                       "JOIN user_roles ur ON ur.user_id = u.id JOIN roles r ON r.id = ur.role_id ORDER BY u.email"))
        assert [tuple(r) for r in rows] == [
            ("cand@x.com", "cand", None, "CANDIDATE"),
            ("rec@x.com", "Rita Recruiter", "Acme", "RECRUITER"),
        ]
    finally:
        command.upgrade(migrated, "head")  # leave the schema at head for the other tests
        run(sql("TRUNCATE users CASCADE"))


def test_0006_downgrade_rebuilds_role_and_recruiter_profiles(migrated, database_url):
    """Contract step: the downgrade brings back users.role and recruiter_profiles from the new tables."""
    import uuid

    from smarthire_common.testing import run
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    async def sql(statement: str, params: dict | None = None):
        engine = create_async_engine(database_url)
        try:
            async with engine.begin() as conn:
                result = await conn.execute(text(statement), params or {})
                return result.all() if result.returns_rows else None
        finally:
            await engine.dispose()

    both = uuid.uuid4()
    try:
        run(sql("TRUNCATE users CASCADE"))
        run(sql("INSERT INTO users (id, email, password_hash, full_name, current_company) "
                "VALUES (:u, 'both@x.com', 'h', 'Rita Both', 'Acme')", {"u": both}))
        run(sql("INSERT INTO user_roles (user_id, role_id) VALUES (:u, 1), (:u, 2)", {"u": both}))
        command.downgrade(migrated, "0005")
        assert run(sql("SELECT role FROM users")) == [("CANDIDATE",)]  # lowest role id
        assert run(sql("SELECT display_name, company FROM recruiter_profiles")) == [("Rita Both", "Acme")]
        assert run(sql("SELECT count(*) FROM refresh_tokens")) == [(0,)]
    finally:
        command.upgrade(migrated, "head")
        run(sql("TRUNCATE users CASCADE"))
