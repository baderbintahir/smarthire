"""Appendix E.3: every migration has a working downgrade, and the migrations produce exactly
what the models describe."""

from alembic import command


def test_upgrade_downgrade_upgrade(migrated):
    command.downgrade(migrated, "base")
    command.upgrade(migrated, "head")


def test_models_match_migrations(migrated):
    # Like `alembic check`: autogenerate compares models to the migrated DB; raises on any diff
    command.check(migrated)
