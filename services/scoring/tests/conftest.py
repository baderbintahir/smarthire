import pytest
from smarthire_common import testing


@pytest.fixture(scope="session")
def mongo_url():
    with testing.mongo_replica_set() as url:  # a replica set: the activity uses a transaction
        yield url


@pytest.fixture
def settings(mongo_url):
    from app.config import Settings

    return Settings(mongo_url=mongo_url, mongo_database="scoring_test", temporal_enabled=False, kafka_enabled=False,
                    _env_file=None)
