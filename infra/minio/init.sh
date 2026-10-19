#!/bin/sh
# One-shot setup for MinIO (the `minio-init` container in docker-compose.yml). Safe to run again:
# every step is idempotent, so it runs on every `docker compose up`.
#   1. a PRIVATE `resumes` bucket (no anonymous access, ever)
#   2. a MinIO user for the Candidate service that can touch ONLY that bucket
# The root account is for this script and the console, never for services.
set -eu

mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null

mc mb --ignore-existing local/resumes
mc anonymous set none local/resumes      # private: files are reached via our API, not bucket URLs

mc admin policy create local candidate-resumes /init/candidate-resumes-policy.json
mc admin user add local "$CANDIDATE_S3_ACCESS_KEY" "$CANDIDATE_S3_SECRET_KEY"
# On re-runs the policy is already attached, which mc reports as an error: ignore just that case
mc admin policy attach local candidate-resumes --user "$CANDIDATE_S3_ACCESS_KEY" 2>/dev/null \
  || mc admin user info local "$CANDIDATE_S3_ACCESS_KEY" | grep -q candidate-resumes

echo "MinIO ready: bucket 'resumes' (private), user for the candidate service"
