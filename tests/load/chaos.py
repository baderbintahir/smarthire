"""Step 14.5: while the load test runs, stop and restart one dependency at a time.
Usage (second terminal, right after starting the load): `make chaos`. Then `make load-verify`.

Each outage lasts 30 s, with 20 s between them. What should happen (and verify.py checks afterwards):
  kafka               applies still 202 (outbox), events wait, then flow: backlog -> 0, lag -> 0
  scoring             applications wait in SUBMITTED (Temporal retries the activity), then SCREENING
                      (Scoring is also the Temporal worker for scoring-tq)
  notification        events wait in Kafka (lag grows), then emails go out
  notification-worker emails wait QUEUED in RabbitMQ, then go out
  candidate           applies fail fast (503: Application's breaker), clients retry with the same key"""

import subprocess
import time

OUTAGES = ["kafka", "scoring", "notification", "notification-worker", "candidate"]


def compose(*args: str) -> None:
    subprocess.run(["docker", "compose", *args], check=True, capture_output=True)


def main() -> None:
    started = time.time()
    for service in OUTAGES:
        time.sleep(20)
        print(f"[{time.time() - started:5.0f}s] stop {service}", flush=True)
        compose("stop", service)
        time.sleep(30)
        print(f"[{time.time() - started:5.0f}s] start {service}", flush=True)
        compose("start", service)
    print(f"[{time.time() - started:5.0f}s] chaos done", flush=True)


if __name__ == "__main__":
    main()
