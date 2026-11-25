"""Step 14.5: while the load test runs, stop and restart one dependency at a time.
Usage (second terminal, right after starting the load): `make chaos`. Then `make load-verify`.

Each outage lasts 30 s, with 20 s between them. What should happen (and verify.py checks afterwards), design v2.1:
  scoring        applications wait in SUBMITTED (the orchestrator retries the score step), then SCREENING
  notification   emails wait (SendEmailWorkflow retries its steps), then go out
  analytics      facts wait (record_fact is retried), then the metrics catch up
  candidate      applies fail fast (503: Application's breaker), clients retry with the same key
  orchestrator   applies fail fast (503 WORKFLOWS_UNAVAILABLE: nothing written), clients retry with the same
                 key; running workflows continue when it is back (their state is in Temporal, not in it)"""

import subprocess
import time

OUTAGES = ["scoring", "notification", "analytics", "candidate", "orchestrator"]


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
