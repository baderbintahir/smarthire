"""Phase 12 + design v2.0: facts -> projections -> metrics, on real Postgres. Events still arrive through the
real idempotent consumer (until Step T10) and become facts; workflows record facts via POST /internal/facts."""

import uuid
from datetime import UTC, date, datetime, timedelta

import pytest
from alembic import command
from smarthire_common import db
from smarthire_common.events import EventEnvelope
from smarthire_common.kafka import EventConsumer

from app.events import handlers
from app.services import metrics

TODAY = date(2026, 11, 3)


def event(event_type: str, payload: dict, at: datetime | None = None) -> EventEnvelope:
    return EventEnvelope(event_type=event_type, producer="test", payload=payload,
                         occurred_at=at or datetime(2026, 11, 3, 10, tzinfo=UTC))


@pytest.fixture
def consumer():
    return EventConsumer(name=handlers.CONSUMER_NAME, topics=handlers.TOPICS, handlers=handlers.HANDLERS,
                         bootstrap_servers="unused", schema_registry_url="unused")


async def feed(consumer, *events):
    for e in events:
        await consumer.process(e, handlers.HANDLERS[e.event_type])


async def numbers() -> dict:
    async with db.session_scope() as s:
        return await metrics.compute(s, today=TODAY)


def job_ready(job_id, title="Backend Engineer", version=3):
    return event("JobReady", {"job_id": job_id, "recruiter_id": str(uuid.uuid4()), "title": title, "status": "READY",
                              "stages": ["Interview"], "eligibility_rules": {}, "application_limit": None,
                              "deadline": None, "version": version})


def submitted(app_id, job_id, candidate_id, at=None, title="Backend Engineer"):
    return event("ApplicationSubmitted", {"application_id": app_id, "job_id": job_id, "candidate_id": candidate_id,
                                          "recruiter_id": str(uuid.uuid4()), "job_title": title}, at)


def stage(app_id, to_stage, at):
    return event("ApplicationStageChanged", {"application_id": app_id, "job_id": str(uuid.uuid4()),
                                             "candidate_id": str(uuid.uuid4()), "from_stage": "SCREENING",
                                             "to_stage": to_stage}, at)


def decided(app_id, decision, at):
    return event("ApplicationDecisionMade", {"application_id": app_id, "job_id": str(uuid.uuid4()),
                                             "candidate_id": str(uuid.uuid4()), "decision": decision}, at)


# ---------------------------------------------------------------- schema


def test_upgrade_downgrade_upgrade(migrated):
    command.downgrade(migrated, "base")
    command.upgrade(migrated, "head")


def test_models_match_migrations(migrated):
    command.check(migrated)


# ---------------------------------------------------------------- the metrics, end to end through the handlers


async def test_all_metrics_from_a_small_story(database, consumer):
    job_a, job_b = str(uuid.uuid4()), str(uuid.uuid4())
    jane, john = str(uuid.uuid4()), str(uuid.uuid4())
    apps = [str(uuid.uuid4()) for _ in range(3)]
    day1, day2 = datetime(2026, 11, 1, 9, tzinfo=UTC), datetime(2026, 11, 2, 9, tzinfo=UTC)
    await feed(
        consumer,
        event("UserRegistered", {"user_id": jane, "role": "CANDIDATE"}),
        event("UserRegistered", {"user_id": john, "role": "CANDIDATE"}),
        event("UserRegistered", {"user_id": str(uuid.uuid4()), "role": "RECRUITER"}),
        event("UserRegistered", {"user_id": str(uuid.uuid4()), "role": "ADMIN"}),  # not counted
        job_ready(job_a, "Backend Engineer"), job_ready(job_a, "Senior Backend Engineer", version=5),  # re-published
        job_ready(job_b, "Data Engineer"),
        submitted(apps[0], job_a, jane, day1), submitted(apps[1], job_b, jane, day2), submitted(apps[2], job_a, john, day2),
        stage(apps[0], "Technical Interview", day1 + timedelta(days=1)),
        stage(apps[0], "Final interview", day1 + timedelta(days=3)),  # only the FIRST interview counts
        decided(apps[0], "HIRED", day1 + timedelta(days=4)),
        decided(apps[1], "REJECTED", day2 + timedelta(days=1)),
        event("JobPublishFailed", {"job_id": job_b, "recruiter_id": str(uuid.uuid4()), "version": 2, "reason": "x"}),
        event("SystemFailure", {"source": "candidate", "kind": "DEAD_LETTER"}),
    )
    m = await numbers()
    assert (m["total_candidates"], m["total_recruiters"], m["total_jobs_published"]) == (2, 1, 2)  # first versions only
    assert m["applications_over_time"] == [{"day": "2026-11-01", "count": 1}, {"day": "2026-11-02", "count": 2}]
    assert m["application_to_interview_rate"] == round(1 / 3, 4)
    assert m["average_days_to_hire"] == 4.0
    assert m["most_applied_jobs"][0] == {"job_id": job_a, "title": "Senior Backend Engineer", "applications": 2}
    assert m["average_applications_per_candidate"] == 1.5
    assert m["failures_last_30_days"] == {"total": 2, "by_source": {"candidate": 1, "job_publishing_workflow": 1}}


async def test_replayed_events_change_nothing(database, consumer):
    events = [event("UserRegistered", {"user_id": str(uuid.uuid4()), "role": "CANDIDATE"}),
              submitted(str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4()))]
    await feed(consumer, *events)
    before = await numbers()
    await feed(consumer, *events)  # redelivered
    assert await numbers() == before


async def test_totals_count_role_holders_through_grant_revoke_and_replay(database, consumer):
    """Design v1.1: a person holding two roles is counted in both totals; grants add, revokes remove;
    v1 events (single role, replayed by a rebuild) still count; replaying everything changes nothing."""
    rita, ana, old = str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())
    story = [
        event("UserRegistered", {"user_id": rita, "role": "RECRUITER", "roles": ["RECRUITER", "CANDIDATE"]}),
        event("UserRegistered", {"user_id": ana, "role": "CANDIDATE", "roles": ["CANDIDATE"]}),
        event("UserRegistered", {"user_id": old, "role": "RECRUITER"}),  # v1 event, before roles existed
        event("RoleGranted", {"user_id": ana, "role": "RECRUITER", "granted_by": None}),
        event("RoleGranted", {"user_id": ana, "role": "ADMIN", "granted_by": rita}),  # admins aren't counted
        event("RoleRevoked", {"user_id": rita, "role": "CANDIDATE", "revoked_by": rita}),
    ]
    await feed(consumer, *story)
    m = await numbers()
    assert (m["total_candidates"], m["total_recruiters"]) == (1, 3)  # ana | rita, old, ana
    await feed(consumer, *story)  # redelivered: processed_events skips every one
    assert await numbers() == m


async def test_revoking_a_role_never_counted_stays_at_zero(database, consumer):
    # A user from before events existed: their registration was never counted
    await feed(consumer, event("RoleRevoked", {"user_id": str(uuid.uuid4()), "role": "RECRUITER", "revoked_by": str(uuid.uuid4())}))
    assert (await numbers())["total_recruiters"] == 0


async def test_application_before_its_job_ready_still_counts_once(database, consumer):
    """Different topics have no order between them: JobReady may be processed after an application."""
    job_id = str(uuid.uuid4())
    await feed(consumer, submitted(str(uuid.uuid4()), job_id, str(uuid.uuid4()), title=""), job_ready(job_id, "Late Title"))
    m = await numbers()
    assert m["total_jobs_published"] == 1 and m["most_applied_jobs"] == [{"job_id": job_id, "title": "Late Title",
                                                                          "applications": 1}]


async def test_empty_read_model_has_no_ratios(database):
    m = await numbers()
    assert m["application_to_interview_rate"] is None and m["average_days_to_hire"] is None
    assert m["average_applications_per_candidate"] is None and m["total_candidates"] == 0


def test_interview_stage_detection():
    assert handlers.is_interview_stage("Technical Interview") and not handlers.is_interview_stage("Offer")


# ---------------------------------------------------------------- the API


def test_metrics_api_is_for_recruiters_and_admins_and_cached(client, tokens, redis_url):
    from smarthire_common import testing

    assert client.get("/analytics/metrics", headers=tokens.headers(role="CANDIDATE")).status_code == 403
    assert client.get("/analytics/metrics").status_code == 401
    body = client.get("/analytics/metrics", headers=tokens.headers(role="RECRUITER")).json()
    assert body["total_candidates"] == 0

    async def cache_ttl():
        from redis.asyncio import Redis

        redis = Redis.from_url(redis_url)
        ttl = await redis.ttl(metrics.CACHE_KEY)
        await redis.aclose()
        return ttl

    assert 0 < testing.run(cache_ttl()) <= 10  # cached for a few seconds
    assert client.get("/analytics/metrics", headers=tokens.headers(role="ADMIN")).json() == body


# ---------------------------------------------------------------- facts (design v2.0)


async def record(kind: str, data: dict, *, fact_id: str | None = None, at: datetime | None = None) -> bool:
    from app.services import facts

    async with db.session_scope() as s:
        return await facts.record_fact(s, fact_id=fact_id or str(uuid.uuid4()), kind=kind,
                                       occurred_at=at or datetime(2026, 11, 3, 10, tzinfo=UTC), data=data)


async def test_a_fact_recorded_twice_counts_once(database):
    data = {"user_id": str(uuid.uuid4()), "role": "CANDIDATE", "roles": ["CANDIDATE"]}
    assert await record("UserRegistered", data, fact_id="register-abc:UserRegistered") is True
    assert await record("UserRegistered", data, fact_id="register-abc:UserRegistered") is False  # a retried step
    assert (await numbers())["total_candidates"] == 1


async def test_facts_keep_only_what_the_metrics_need(database):
    from sqlalchemy import text

    await record("JobReady", {"job_id": str(uuid.uuid4()), "title": "SRE", "version": 1, "description": "long text",
                              "recruiter_id": str(uuid.uuid4())}, fact_id="publish-1:JobReady")
    async with db.session_scope() as s:
        data = (await s.execute(text("SELECT data FROM facts WHERE fact_id = 'publish-1:JobReady'"))).scalar_one()
    assert set(data) == {"job_id", "title", "version"}


async def test_rebuild_from_facts_gives_identical_numbers(database, consumer):
    from app.services import facts

    job = str(uuid.uuid4())
    await feed(consumer, event("UserRegistered", {"user_id": str(uuid.uuid4()), "role": "RECRUITER"}), job_ready(job))
    await record("ApplicationSubmitted", {"application_id": str(uuid.uuid4()), "job_id": job,
                                          "candidate_id": str(uuid.uuid4()), "job_title": "Backend Engineer"})
    await record("ApplicationDecisionMade", {"application_id": str(uuid.uuid4()), "decision": "HIRED"})
    before = await numbers()
    async with db.session_scope() as s:
        assert await facts.rebuild(s) == 4  # events from Kafka and facts from workflows, all in one log
    assert await numbers() == before


def test_internal_facts_endpoint(client, service_tokens, tokens):
    body = {"fact_id": "application-j-c:ApplicationSubmitted", "kind": "ApplicationSubmitted",
            "occurred_at": "2026-11-25T10:00:00Z",
            "data": {"application_id": str(uuid.uuid4()), "job_id": str(uuid.uuid4()),
                     "candidate_id": str(uuid.uuid4()), "job_title": "SRE"}}
    assert client.post("/internal/facts", json=body, headers={"Idempotency-Key": "k"}).status_code == 401
    user_token = {**tokens.headers(role="ADMIN"), "Idempotency-Key": "k"}
    assert client.post("/internal/facts", json=body, headers=user_token).status_code == 401  # users can't
    first = client.post("/internal/facts", json=body, headers=service_tokens.headers())
    assert first.status_code == 200 and first.json() == {"recorded": True}
    again = client.post("/internal/facts", json=body, headers=service_tokens.headers())
    assert again.json() == {"recorded": False}  # a retried step: stored once
    unknown = client.post("/internal/facts", json={**body, "kind": "Nope"}, headers=service_tokens.headers())
    assert unknown.status_code == 422 and unknown.json()["error"]["code"] == "UNKNOWN_FACT_KIND"
    metrics_body = client.get("/analytics/metrics", headers=tokens.headers(role="ADMIN")).json()
    assert metrics_body["total_applications"] == 1
