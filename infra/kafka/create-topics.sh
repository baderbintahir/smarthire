#!/bin/bash
# One-shot topic setup (the `kafka-init` container in docker-compose.yml). Topics live here, in git,
# because the broker has auto-create turned off: a typo in a topic name is an error, not a new topic.
# Idempotent: only missing topics are created, so it runs on every `docker compose up`.
#
# Topics and keys: Appendix A of docs/implementation-plan.md. Every topic gets a .dlq twin
# (dead-letter queue: messages a consumer gave up on, kept for inspection and reprocessing).
set -euo pipefail

BOOTSTRAP=kafka:19092
PARTITIONS=3      # >1 so a consumer group can spread work over several workers; same key -> same partition
TOPICS=(
  identity.users
  jobs.lifecycle
  jobs.commands
  candidates.profile
  applications.lifecycle
  scoring.results
  assistant.usage
  system.failures
)

# Each kafka-topics.sh call starts a JVM (~2 s): list once, then create only what's missing
existing=$(/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list)

for topic in "${TOPICS[@]}"; do
  for name in "$topic" "$topic.dlq"; do
    if grep -qx "$name" <<<"$existing"; then
      echo "exists:  $name"
    else
      # replication-factor 1: there is only one broker locally (production: 3)
      /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
        --topic "$name" --partitions "$PARTITIONS" --replication-factor 1 >/dev/null
      echo "created: $name"
    fi
  done
done
