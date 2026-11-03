"""Phase 11: schema (E.8), routing, queueing from events (idempotent), sending (idempotent, retries,
circuit breaker). Real Postgres; SMTP, Identity and the task queue are fakes."""

import asyncio
import smtplib
import uuid

import httpx
import pytest
from alembic import command
from smarthire_common import db
from smarthire_common.events import new_event
from smarthire_common.kafka import EventConsumer, RetryLater
from sqlalchemy import text

from app.domain.routing import route
from app.domain.templates import render
from app.events import handlers
from app.services import sending

# ---------------------------------------------------------------- schema


def test_upgrade_downgrade_upgrade(migrated):
    command.downgrade(migrated, "base")
    command.upgrade(migrated, "head")


def test_models_match_migrations(migrated):
    command.check(migrated)


async def test_schema_names_match_appendix_e8_exactly(migrated, database_url):
    from smarthire_common.testing import schema_object_names

    assert await schema_object_names(database_url, "notification") == {
        "pk_notification_templates", "pk_notification_log", "uq_notification_log_notification_key",
        "fk_notification_log_template_code_notification_templates", "ck_notification_log_status",
        "ck_notification_log_attempts_non_negative", "ix_notification_log_status_created", "pk_processed_events",
    }


async def rows(sql: str):
    async with db.session_scope() as s:
        return (await s.execute(text(sql))).all()


async def test_templates_are_seeded(database):
    codes = {r[0] for r in await rows("SELECT code FROM notification_templates")}
    assert codes == {"APPLICATION_RECEIVED", "STAGE_CHANGED", "DECISION_MADE", "JOB_READY", "JOB_PUBLISH_FAILED"}

# ---------------------------------------------------------------- routing + rendering (pure)

IDS = {"application_id": str(uuid.uuid4()), "job_id": str(uuid.uuid4()), "candidate_id": str(uuid.uuid4())}


def test_routes():
    stage = route(new_event("ApplicationStageChanged", producer="application",
                            payload={**IDS, "from_stage": "SUBMITTED", "to_stage": "SCREENING", "job_title": "Data Engineer"}))
    assert (stage.template, stage.recipient_id) == ("STAGE_CHANGED", IDS["candidate_id"])
    assert stage.variables == {"job_title": "Data Engineer", "to_stage": "SCREENING"}
    hired = route(new_event("ApplicationDecisionMade", producer="application", payload={**IDS, "decision": "HIRED"}))
    assert "Congratulations" in hired.variables["decision_message"] and hired.variables["job_title"] == "the job"  # v1 event
    assert route(new_event("JobClosed", producer="job", payload={"job_id": IDS["job_id"], "recruiter_id": IDS["job_id"],
                                                                 "version": 2})) is None  # no email for this one


def test_render_is_forgiving_and_keeps_subjects_one_line():
    subject, body = render("Re: {job_title}", "Hi {name}, {missing}", {"job_title": "A\r\nBcc: evil@example.com", "name": "Jo"})
    assert "\n" not in subject and "\r" not in subject  # no header injection through a value
    assert body == "Hi Jo, {missing}"

# ---------------------------------------------------------------- queueing from events


class FakeIdentity:
    def __init__(self, status="ACTIVE", down=False):
        self.status, self.down = status, down

    async def contact(self, user_id):
        if self.down:
            raise httpx.ConnectError("identity unreachable")
        return {"email": f"{user_id[:8]}@example.com", "status": self.status}


class FakeProducer:
    def __init__(self):
        self.sent = []

    def send_task(self, name, args, queue):
        self.sent.append((name, args[0], queue))


@pytest.fixture
def producer(monkeypatch):
    fake = FakeProducer()
    monkeypatch.setattr(handlers, "make_celery", lambda *a, **k: fake)
    return fake


class Settings:
    celery_broker_url = "memory://"


def consumer_for(identity):
    return EventConsumer(name=handlers.CONSUMER_NAME, topics=handlers.TOPICS,
                         handlers=handlers.build_handlers(Settings(), identity),
                         bootstrap_servers="unused", schema_registry_url="unused")


submitted = new_event("ApplicationSubmitted", producer="application",
                      payload={**IDS, "recruiter_id": str(uuid.uuid4()), "job_title": "Data Engineer"})


async def test_event_queues_one_email_and_enqueues_it(database, producer):
    consumer = consumer_for(FakeIdentity())
    assert await consumer.process(submitted, consumer.handlers["ApplicationSubmitted"]) is True
    [(key, recipient, code, status)] = await rows("SELECT notification_key, recipient, template_code, status FROM notification_log")
    assert key == f"{submitted.event_id}:APPLICATION_RECEIVED" and code == "APPLICATION_RECEIVED" and status == "QUEUED"
    assert recipient.endswith("@example.com")
    assert [(name, queue) for name, _, queue in producer.sent] == [("send_email", "emails")]


async def test_replayed_event_queues_nothing_new(database, producer):
    """Even if processed_events were bypassed, notification_key UNIQUE stops a second email."""
    handler = handlers.build_handlers(Settings(), FakeIdentity())["ApplicationSubmitted"]
    async with db.session_scope() as s:
        await handler(s, submitted)
    async with db.session_scope() as s:
        await handler(s, submitted)  # the same event again, straight into the handler
    assert len(await rows("SELECT id FROM notification_log")) == 1 and len(producer.sent) == 1


async def test_suspended_users_get_no_email(database, producer):
    async with db.session_scope() as s:
        await handlers.build_handlers(Settings(), FakeIdentity(status="SUSPENDED"))["ApplicationSubmitted"](s, submitted)
    assert await rows("SELECT id FROM notification_log") == []


async def test_identity_down_means_retry_later(database, producer):
    with pytest.raises(RetryLater):
        async with db.session_scope() as s:
            await handlers.build_handlers(Settings(), FakeIdentity(down=True))["ApplicationSubmitted"](s, submitted)

# ---------------------------------------------------------------- sending


@pytest.fixture(autouse=True)
def fresh_breaker():
    sending.breaker.close()  # the breaker is per process: don't let one test's failures leak into the next


async def queued() -> uuid.UUID:
    async with db.session_scope() as s:
        await handlers.repo.queue(s, key=str(uuid.uuid4()), recipient="jane@example.com", template_code="STAGE_CHANGED",
                                  payload={"job_title": "Data Engineer", "to_stage": "Interview"})
    return (await rows("SELECT id FROM notification_log ORDER BY created_at DESC LIMIT 1"))[0][0]


class FakeSmtp:
    def __init__(self, failures=0):
        self.sent, self.failures = [], failures

    def __call__(self, to, subject, body):
        if self.failures:
            self.failures -= 1
            raise smtplib.SMTPServerDisconnected("mailpit is down")
        self.sent.append((to, subject, body))


async def test_send_renders_and_marks_sent_once(database):
    notification_id, smtp = await queued(), FakeSmtp()
    assert await sending.send(notification_id, smtp) == "sent"
    assert await sending.send(notification_id, smtp) == "skipped"  # a duplicate task: no second email
    [(to, subject, body)] = smtp.sent
    assert subject == "Your application for Data Engineer: Interview" and "Interview stage" in body
    assert (await rows("SELECT status, attempts FROM notification_log"))[0] == ("SENT", 1)


async def test_two_tasks_at_once_send_once(database):
    """SKIP LOCKED: the second task sees the row locked and leaves it alone."""
    notification_id = await queued()
    release = asyncio.Event()
    smtp_calls = []

    def slow_smtp(to, subject, body):
        smtp_calls.append(to)
        asyncio.run_coroutine_threadsafe(release.wait(), loop).result()

    loop = asyncio.get_running_loop()
    first = asyncio.create_task(sending.send(notification_id, slow_smtp))
    while not smtp_calls:
        await asyncio.sleep(0.01)
    assert await sending.send(notification_id, slow_smtp) == "busy"
    release.set()
    assert await first == "sent" and len(smtp_calls) == 1


async def test_smtp_down_records_the_attempt_and_retries_later(database):
    notification_id = await queued()
    with pytest.raises(sending.SendLater):
        await sending.send(notification_id, FakeSmtp(failures=1))
    status, attempts, error = (await rows("SELECT status, attempts, last_error FROM notification_log"))[0]
    assert (status, attempts) == ("QUEUED", 1) and "mailpit is down" in error  # still waiting, not lost
    assert await sending.send(notification_id, FakeSmtp()) == "sent"


async def test_circuit_opens_after_three_failures(database):
    ids, smtp = [await queued() for _ in range(4)], FakeSmtp(failures=100)
    for notification_id in ids[:3]:
        with pytest.raises(sending.SendLater):
            await sending.send(notification_id, smtp)
    calls_before = smtp.failures
    with pytest.raises(sending.SendLater, match="circuit open"):
        await sending.send(ids[3], smtp)
    assert smtp.failures == calls_before  # the open circuit didn't even try SMTP


async def test_gives_up_after_max_attempts(database, monkeypatch):
    monkeypatch.setattr(sending, "MAX_ATTEMPTS", 2)
    notification_id = await queued()
    with pytest.raises(sending.SendLater):
        await sending.send(notification_id, FakeSmtp(failures=5))
    sending.breaker.close()
    assert await sending.send(notification_id, FakeSmtp(failures=5)) == "failed"
    assert (await rows("SELECT status FROM notification_log"))[0][0] == "FAILED"


async def test_sweeper_finds_only_old_queued_emails(database):
    from datetime import UTC, datetime, timedelta

    old, new = await queued(), await queued()
    async with db.session_scope() as s:
        await s.execute(text("UPDATE notification_log SET created_at = now() - interval '5 minutes' WHERE id = :id"), {"id": old})
        assert await handlers.repo.stale_queued(s, datetime.now(UTC) - timedelta(minutes=2)) == [old]
    assert new != old


def test_tasks_are_configured():
    from app.tasks import email

    assert email.send_email.name == "send_email" and email.send_email.max_retries == 20
    assert email.celery_app.conf.task_acks_late
    assert email.celery_app.conf.beat_schedule["requeue-stale-emails"]["schedule"] == 60.0
