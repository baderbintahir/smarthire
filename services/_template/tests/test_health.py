import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app


@pytest.fixture
def client():
    settings = Settings(database_url="sqlite+aiosqlite:///:memory:", _env_file=None)
    # `with` runs the lifespan (init_db / close_db)
    with TestClient(create_app(settings)) as c:
        yield c


def test_live(client):
    assert client.get("/health/live").json() == {"status": "ok"}


def test_ready_checks_database(client):
    r = client.get("/health/ready")
    assert r.status_code == 200
    assert r.json() == {"status": "ok", "checks": {"database": "ok"}}


def test_request_id_header_and_standard_error_body(client):
    r = client.get("/nope", headers={"X-Request-ID": "req-42"})
    assert r.status_code == 404
    assert r.headers["X-Request-ID"] == "req-42"
    assert r.json()["error"] == {
        "code": "HTTP_404",
        "message": "Not Found",
        "request_id": "req-42",
    }
