"""Step 14.3b: Identity's retention jobs. None since design v2.0: refresh sessions live in Redis and
expire by TTL (v1.2), and the outbox is gone (v2.0)."""

from app import maintenance


def test_no_jobs_remain():
    assert maintenance.JOBS == []
