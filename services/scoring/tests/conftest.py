import pytest
from smarthire_common import testing


@pytest.fixture(scope="session")
def mongo_url():
    with testing.mongo_replica_set() as url:  # the same single-node replica set as in Compose
        yield url


@pytest.fixture
def settings(mongo_url):
    from app.config import Settings

    return Settings(mongo_url=mongo_url, mongo_database="scoring_test", _env_file=None)
