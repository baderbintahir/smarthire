"""Step 14.3b: Identity's retention jobs. Since design v1.2 only the outbox: refresh sessions live in
Redis and expire by TTL, so there is no refresh_tokens job any more."""

from app import maintenance


def test_only_the_outbox_job_remains():
    assert [job.name for job in maintenance.JOBS] == ["outbox"]
