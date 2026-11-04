"""Step 14.4, part 1: create the test data the load test needs. Usage: `make load-setup n=500`.

Creates one recruiter with a few published jobs and N candidates, each with a profile and a PARSED
resume, then writes their ids and access tokens to tests/load/.results/setup.json.

Setup talks to the services DIRECTLY (their 127.0.0.1 ports), not through the gateway: it isn't the
load we want to measure, and 500 logins through the gateway would only hit its 30 logins/min/IP limit.
Access tokens live 15 minutes: run the load test right after this."""

import asyncio
import json
import pathlib
import subprocess
import sys
import time
import uuid

import httpx

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "events"))
from resume_files import RESUME_LINES, make_pdf  # noqa: E402  (the e2e tests' PDF generator)

IDENTITY, JOB, CANDIDATE = "http://localhost:8001", "http://localhost:8002", "http://localhost:8004"
RESULTS = pathlib.Path(__file__).parent / ".results"
PASSWORD = "load-test-password"
PDF = make_pdf(RESUME_LINES)


def sql(query: str) -> str:
    return subprocess.run(["docker", "compose", "exec", "-T", "postgres", "sh", "-c", 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tA'],
                          input=query, capture_output=True, text=True, check=True).stdout.strip()


async def user(http: httpx.AsyncClient, run: str, n: int | str, role: str) -> tuple[str, str]:
    email = f"load-{run}-{n}@example.com"
    extra = {"display_name": "Load Recruiter"} if role == "RECRUITER" else {}
    r = await http.post(f"{IDENTITY}/auth/register", json={"email": email, "password": PASSWORD, "role": role, **extra})
    r.raise_for_status()
    login = await http.post(f"{IDENTITY}/auth/login", json={"email": email, "password": PASSWORD})
    login.raise_for_status()
    return r.json()["id"], login.json()["access_token"]


async def candidate(http: httpx.AsyncClient, run: str, n: int, limit: asyncio.Semaphore) -> dict:
    async with limit:
        user_id, token = await user(http, run, n, "CANDIDATE")
        auth = {"Authorization": f"Bearer {token}"}
        # The profile is created from UserRegistered (Kafka); PUT also works if the event is late
        r = await http.put(f"{CANDIDATE}/candidates/me", json={"full_name": f"Load Candidate {n}", "experience_years": 4},
                           headers=auth)
        r.raise_for_status()
        r = await http.post(f"{CANDIDATE}/candidates/me/resumes", files={"file": ("cv.pdf", PDF, "application/pdf")}, headers=auth)
        r.raise_for_status()
        return {"id": user_id, "token": token}


async def main(n: int, jobs: int) -> None:
    run = uuid.uuid4().hex[:8]
    started = time.time()
    async with httpx.AsyncClient(timeout=30) as http:
        _, recruiter = await user(http, run, "recruiter", "RECRUITER")
        rec = {"Authorization": f"Bearer {recruiter}"}
        job_ids = []
        for i in range(jobs):
            body = {"title": f"Load test job {i}", "description": "Build Kafka pipelines in Python.\nRequirements:\n- SQL",
                    "employment_type": "FULL_TIME", "stages": ["Screening", "Interview"], "required_skills": ["Python"]}
            job = (await http.post(f"{JOB}/jobs", json=body, headers=rec)).json()
            (await http.post(f"{JOB}/jobs/{job['id']}/publish", headers=rec)).raise_for_status()
            job_ids.append(job["id"])
        limit = asyncio.Semaphore(20)  # registering hashes a password: don't flood Identity
        candidates = await asyncio.gather(*(candidate(http, run, i, limit) for i in range(n)))
    print(f"created {n} candidates and {jobs} jobs in {time.time() - started:.0f} s; waiting for jobs READY + resumes PARSED")
    ids = ",".join(f"'{j}'" for j in job_ids)
    deadline = time.time() + 300
    while time.time() < deadline:
        ready = int(sql(f"SELECT count(*) FROM application.job_replica WHERE job_id IN ({ids}) AND status = 'READY'"))
        parsed = int(sql(f"SELECT count(*) FROM candidate.resumes r JOIN identity.users u ON u.id = r.candidate_id "
                         f"WHERE u.email LIKE 'load-{run}-%' AND r.status = 'PARSED'"))
        print(f"  jobs ready {ready}/{jobs}, resumes parsed {parsed}/{n}")
        if ready == jobs and parsed == n:
            break
        time.sleep(5)
    else:
        sys.exit("setup did not converge in 5 minutes")
    RESULTS.mkdir(exist_ok=True)
    (RESULTS / "setup.json").write_text(json.dumps({"run": run, "created_at": time.time(), "jobs": job_ids,
                                                    "candidates": candidates}))
    print(f"run {run}: ready in {time.time() - started:.0f} s -> {RESULTS / 'setup.json'} (tokens valid ~15 min)")


if __name__ == "__main__":
    asyncio.run(main(int(sys.argv[1]) if len(sys.argv) > 1 else 500, int(sys.argv[2]) if len(sys.argv) > 2 else 5))
