"""Step 14.4, part 3: after the load (and chaos), check the system converged. Usage: `make load-verify`.

Done when (plan): no lost applications (DB rows = accepted 202s), no duplicates, every workflow
completes, error rate near zero apart from expected 429s. Also checked: no email left QUEUED and no email
workflow still running (design v2.1). Waits up to 10 minutes for the async work to finish."""

import json
import pathlib
import subprocess
import sys
import time


RESULTS = pathlib.Path(__file__).parent / ".results"


def sql(query: str) -> str:
    return subprocess.run(["docker", "compose", "exec", "-T", "postgres", "sh", "-c", 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tA'],
                          input=query, capture_output=True, text=True, check=True).stdout.strip()


def running_workflows(workflow_type: str = "ApplicationWorkflow") -> int:
    out = subprocess.run(["docker", "compose", "exec", "-T", "temporal", "temporal", "workflow", "count", "-q",
                          f"WorkflowType='{workflow_type}' AND ExecutionStatus='Running'"],
                         capture_output=True, text=True, check=True).stdout
    return int(out.split(":")[-1].strip())


def main() -> int:
    setup = json.loads((RESULTS / "setup.json").read_text())
    run = json.loads((RESULTS / "accepted.json").read_text())
    accepted = run["accepted"]
    jobs = ",".join(f"'{j}'" for j in setup["jobs"])
    ok = True

    def check(name: str, passed: bool, detail: str) -> None:
        nonlocal ok
        ok &= passed
        print(f"{'PASS' if passed else 'FAIL'}  {name}: {detail}")

    # 1. No lost and no phantom applications: the DB holds exactly what the clients were told was accepted
    rows = dict(line.split("|") for line in sql(f"SELECT id, candidate_id || ':' || job_id FROM application.applications "
                                                 f"WHERE job_id IN ({jobs})").splitlines() if line)
    lost = set(accepted) - set(rows)
    phantom = set(rows) - set(accepted)
    check("no lost applications", not lost, f"{len(accepted)} accepted (202), {len(rows)} in the DB, {len(lost)} missing")
    check("no unexplained rows", not phantom, f"{len(phantom)} rows nobody got a 202 for")
    pairs = list(rows.values())
    check("no duplicates", len(pairs) == len(set(pairs)), f"{len(pairs) - len(set(pairs))} candidate+job pairs twice")
    check("idempotent replays", not run["problems"], f"{len(run['problems'])} problems {run['problems'][:3]}")

    # 2. Everything asynchronous finishes (application workflows, email workflows)
    deadline = time.time() + 600
    while True:
        submitted = int(sql(f"SELECT count(*) FROM application.applications WHERE job_id IN ({jobs}) AND status = 'SUBMITTED'"))
        queued = int(sql("SELECT count(*) FROM notification.notification_log WHERE status = 'QUEUED'"))
        running = running_workflows()
        emails = running_workflows("SendEmailWorkflow")
        if (submitted, queued, running, emails) == (0, 0, 0, 0) or time.time() > deadline:
            break
        print(f"  waiting: {submitted} SUBMITTED, {running} application workflows running, {emails} email "
              f"workflows running, {queued} emails queued")
        time.sleep(10)
    check("all workflows completed", submitted == 0 and running == 0, f"{submitted} still SUBMITTED, {running} running")
    statuses = sql(f"SELECT string_agg(status || ' ' || n, ', ') FROM (SELECT status, count(*) n FROM application.applications "
                   f"WHERE job_id IN ({jobs}) GROUP BY 1) s")
    scores = sql(f"SELECT string_agg(score_status || ' ' || n, ', ') FROM (SELECT score_status, count(*) n FROM "
                 f"application.applications WHERE job_id IN ({jobs}) GROUP BY 1) s")
    print(f"      applications: {statuses}; scores: {scores}")
    failed = sql("SELECT count(*) FROM notification.notification_log WHERE status = 'FAILED'")
    check("emails drained", queued == 0 and emails == 0, f"{queued} QUEUED, {emails} still sending, {failed} FAILED")

    # 3. Errors (429 = rate limits doing their job, reported apart)
    status = run["status"]
    total = sum(status.values())
    limited = sum(n for k, n in status.items() if k.endswith(" 429"))
    errors = sum(n for k, n in status.items() if k.split()[-1][0] in "45" and not k.endswith(" 429"))
    rate = errors / total if total else 0
    print(f"      requests {total}: {limited} rate-limited (429), {errors} errors ({rate:.2%}); "
          f"{json.dumps({k: n for k, n in sorted(status.items()) if not k.split()[-1].startswith('2')})}")
    check("error rate near zero (429 excluded)", rate < 0.01, f"{rate:.2%} (chaos windows make some 503s expected)")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
