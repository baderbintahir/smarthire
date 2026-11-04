"""Step 14.4, part 2: the load. Usage: `make load-test u=500 t=3m` (after `make load-setup`).

Every simulated user is one candidate from setup.json, going THROUGH THE GATEWAY like a real browser:
  - applies to each job once, with an Idempotency-Key; 10% of the time sends the same request again
    (a double click) and checks it gets the SAME application back
  - when an apply fails (503, timeout, connection reset during chaos), retries LATER with the SAME key:
    that's what makes retries safe (never two applications for one click)
  - looks at its own applications and browses the public job board

429s are the rate limits doing their job: counted separately (results), not as failures.
At the end every accepted application id goes to .results/accepted.json for verify.py."""

import json
import pathlib
import random
import threading
import uuid
from collections import Counter

from locust import HttpUser, between, events, task
from locust.exception import StopUser

RESULTS = pathlib.Path(__file__).parent / ".results"
SETUP = json.loads((RESULTS / "setup.json").read_text())
_candidates = iter(SETUP["candidates"])
_lock = threading.Lock()

ACCEPTED: dict[str, dict] = {}  # application id -> {candidate_id, job_id}
STATUS: Counter = Counter()  # "POST /api/applications 202" -> count
PROBLEMS: list[str] = []


class Candidate(HttpUser):
    wait_time = between(1, 3)

    def on_start(self) -> None:
        with _lock:
            self.me = next(_candidates, None)
        if self.me is None:
            raise StopUser()  # more users than candidates
        self.auth = {"Authorization": f"Bearer {self.me['token']}"}
        self.todo = [(job_id, str(uuid.uuid4())) for job_id in random.sample(SETUP["jobs"], len(SETUP["jobs"]))]

    def _count(self, name: str, response) -> None:
        STATUS[f"{name} {response.status_code}"] += 1

    @task(4)
    def apply(self) -> None:
        if not self.todo:
            return self.my_applications()
        job_id, key = self.todo.pop()
        name = "POST /api/applications"
        headers = {**self.auth, "Idempotency-Key": key}
        with self.client.post("/api/applications", json={"job_id": job_id}, headers=headers, name=name,
                              catch_response=True) as r:
            self._count(name, r)
            if r.status_code == 202:
                application_id = r.json()["id"]
                ACCEPTED[application_id] = {"candidate_id": self.me["id"], "job_id": job_id}
                r.success()
                if random.random() < 0.1:
                    self.double_click(job_id, key, application_id)
            elif r.status_code == 429:
                r.success()  # rate limited: expected under load, try again later with the same key
                self.todo.insert(0, (job_id, key))
            else:
                self.todo.insert(0, (job_id, key))  # retry later, SAME key
                r.failure(f"{r.status_code}: {r.text[:120]}")

    def double_click(self, job_id: str, key: str, application_id: str) -> None:
        name = "POST /api/applications (same key again)"
        with self.client.post("/api/applications", json={"job_id": job_id}, headers={**self.auth, "Idempotency-Key": key},
                              name=name, catch_response=True) as r:
            self._count(name, r)
            if r.status_code == 202 and r.json()["id"] == application_id:
                r.success()
            elif r.status_code == 429:
                r.success()
            else:
                PROBLEMS.append(f"replay of {key} gave {r.status_code} {r.text[:100]}")
                r.failure(f"idempotent replay broken: {r.status_code}")

    @task(2)
    def my_applications(self) -> None:
        with self.client.get("/api/applications/me", headers=self.auth, name="GET /api/applications/me", catch_response=True) as r:
            self._count("GET /api/applications/me", r)
            if r.status_code in (200, 429):
                r.success()

    @task(1)
    def browse_jobs(self) -> None:
        # Public route: 120/min per IP, and every simulated user shares ONE IP here -> many 429s (expected)
        with self.client.get("/api/jobs?limit=20", name="GET /api/jobs", catch_response=True) as r:
            self._count("GET /api/jobs", r)
            if r.status_code in (200, 429):
                r.success()


@events.test_stop.add_listener
def save_results(environment, **_kwargs) -> None:
    (RESULTS / "accepted.json").write_text(json.dumps({"accepted": ACCEPTED, "status": STATUS, "problems": PROBLEMS}))
    print(f"\n{len(ACCEPTED)} applications accepted (202); statuses: {dict(sorted(STATUS.items()))}")
