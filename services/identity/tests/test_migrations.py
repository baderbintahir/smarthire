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
        "pk_users", "ck_users_role", "ck_users_status", "uq_users_email_lower",
        "pk_recruiter_profiles", "fk_recruiter_profiles_user_id_users",
        "pk_refresh_tokens", "fk_refresh_tokens_user_id_users", "uq_refresh_tokens_token_hash",
        "ix_refresh_tokens_user_id", "ix_refresh_tokens_family_id",
    }
