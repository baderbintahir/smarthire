"""Phase 11 + design v2.0: schema (E.8), rendering, sending (idempotent, circuit breaker) and the internal
endpoints SendEmailWorkflow calls. Real Postgres; SMTP is a fake."""

import asyncio
import smtplib
import uuid

import pytest
from alembic import command
from smarthire_common import db
from sqlalchemy import text

from app.domain.templates import render
from app.repositories import notifications as repo
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
        "fk_notification_log_template_code_notification_templates",
        "ck_notification_log_attempts_non_negative", "ix_notification_log_status_created",
    }


async def rows(sql: str):
    async with db.session_scope() as s:
        return (await s.execute(text(sql))).all()


async def test_templates_are_seeded(database):
    codes = {r[0] for r in await rows("SELECT code FROM notification_templates")}
    assert codes == {"APPLICATION_RECEIVED", "STAGE_CHANGED", "DECISION_MADE", "JOB_READY", "JOB_PUBLISH_FAILED"}

# ---------------------------------------------------------------- rendering (pure)

IDS = {"application_id": str(uuid.uuid4()), "job_id": str(uuid.uuid4()), "candidate_id": str(uuid.uuid4())}


def test_render_is_forgiving_and_keeps_subjects_one_line():
    subject, body = render("Re: {job_title}", "Hi {name}, {missing}", {"job_title": "A\r\nBcc: evil@example.com", "name": "Jo"})
    assert "\n" not in subject and "\r" not in subject  # no header injection through a value
    assert body == "Hi Jo, {missing}"

# ---------------------------------------------------------------- sending


@pytest.fixture(autouse=True)
def fresh_breaker():
    sending.breaker.close()  # the breaker is per process: don't let one test's failures leak into the next


async def queued() -> uuid.UUID:
    async with db.session_scope() as s:
        await repo.queue(s, key=str(uuid.uuid4()), recipient="jane@example.com", template_code="STAGE_CHANGED",
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
    assert await sending.send(notification_id, smtp) == "skipped"  # a repeated step: no second email
    [(to, subject, body)] = smtp.sent
    assert subject == "Your application for Data Engineer: Interview" and "Interview stage" in body
    assert (await rows("SELECT status, attempts FROM notification_log"))[0] == ("SENT", 1)


async def test_two_sends_at_once_send_once(database):
    """SKIP LOCKED: the second call sees the row locked and leaves it alone (it answers 503: try later)."""
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


async def test_retention_deletes_only_old_sent_emails(database):
    from app import maintenance

    async with db.session_scope() as s:
        for key, status, sent_days_ago in [("old-sent", "SENT", 91), ("recent-sent", "SENT", 10), ("old-failed", "FAILED", None),
                                           ("old-queued", "QUEUED", None)]:
            await s.execute(text(
                "INSERT INTO notification_log (id, notification_key, recipient, template_code, payload, status, attempts, created_at, sent_at) "
                "VALUES (:id, :key, 'a@example.com', 'JOB_READY', '{}', :status, 1, now() - interval '100 days', "
                "now() - make_interval(days => :days))"),
                {"id": uuid.uuid4(), "key": key, "status": status, "days": sent_days_ago})
    assert await maintenance.run_all(maintenance.JOBS) == {"notification_log": 1}
    async with db.session_scope() as s:
        left = (await s.execute(text("SELECT notification_key FROM notification_log ORDER BY 1"))).scalars().all()
    assert left == ["old-failed", "old-queued", "recent-sent"]  # unsent rows: a workflow or a human still needs them


# ---------------------------------------------------------------- internal endpoints (design v2.0)


def test_record_then_send_once(make_client, service_tokens):
    smtp = FakeSmtp()
    client = make_client(smtp)
    email = {"notification_key": "email-application-j-c-STAGE_CHANGED", "recipient": "jane@example.com",
             "template_code": "STAGE_CHANGED", "variables": {"job_title": "SRE", "to_stage": "Interview"}}
    assert client.post("/internal/emails", json=email).status_code == 401  # orchestrator only
    first = client.post("/internal/emails", json=email, headers=service_tokens.headers())
    again = client.post("/internal/emails", json=email, headers=service_tokens.headers())  # a retried step
    assert first.status_code == 200 and first.json() == again.json() and first.json()["status"] == "QUEUED"
    key = email["notification_key"]
    assert client.post(f"/internal/emails/{key}/send", headers=service_tokens.headers()).json() == {"status": "SENT"}
    assert client.post(f"/internal/emails/{key}/send", headers=service_tokens.headers()).json() == {"status": "SENT"}
    assert len(smtp.sent) == 1  # sent once, however often the step runs


def test_smtp_down_is_503_so_the_workflow_retries_then_failed(make_client, service_tokens):
    client = make_client(FakeSmtp(failures=100))  # SMTP stays down
    key = "email-x-JOB_READY"
    client.post("/internal/emails", json={"notification_key": key, "recipient": "r@example.com",
                                          "template_code": "JOB_READY", "variables": {"job_title": "SRE"}},
                headers=service_tokens.headers())
    for _ in range(3):  # never FAILED here, however many attempts: the workflow decides when to give up
        down = client.post(f"/internal/emails/{key}/send", headers=service_tokens.headers())
        assert down.status_code == 503 and down.json()["error"]["code"] in ("SMTP_UNAVAILABLE",)
    failed = client.post(f"/internal/emails/{key}/failed", headers=service_tokens.headers())
    assert failed.json() == {"status": "FAILED"}
    assert client.post("/internal/emails/nope/send", headers=service_tokens.headers()).status_code == 404


def test_send_after_smtp_recovers(make_client, service_tokens):
    smtp = FakeSmtp(failures=1)
    client = make_client(smtp)
    key = "email-y-JOB_READY"
    client.post("/internal/emails", json={"notification_key": key, "recipient": "r@example.com",
                                          "template_code": "JOB_READY", "variables": {"job_title": "SRE"}},
                headers=service_tokens.headers())
    assert client.post(f"/internal/emails/{key}/send", headers=service_tokens.headers()).status_code == 503
    assert client.post(f"/internal/emails/{key}/send", headers=service_tokens.headers()).json() == {"status": "SENT"}
    assert len(smtp.sent) == 1
