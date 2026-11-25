# API walkthrough: the whole flow, request by request

Every public API call, in the order that tests the whole application end to end. Generated from
[`postman/SmartHire.postman_collection.json`](../postman/SmartHire.postman_collection.json) by
`make api-walkthrough`; do not edit by hand.

**Run it in Postman:** import the collection and use **Run collection** on folders 1-9, with a delay of
about 2500 ms between requests (publishing, resume parsing and scoring run in the background).
Verified end to end with newman: 44 requests, 0 failures.

**Before you start**

1. `make up`, then wait until `make ps` shows the services healthy.
2. Create an admin whose password matches the `password` variable (`ChangeMe123!`):
   `docker compose exec -it identity python -m app.cli create-admin admin@example.com`
3. Running it again? Change `candidateEmail` and `recruiterEmail` (an email registers only once: 409).

**How to read the steps**

- Paths are relative to the gateway, `http://localhost:8000`. Values in `{{double braces}}` are collection
  variables, saved by earlier steps ("Saves").
- "Token" is the `Authorization: Bearer ...` header. Refresh and logout use the `refresh_token` cookie
  that login sets (path `/api/identity/auth`); Postman keeps it automatically, with curl use a cookie jar
  (`-c jar -b jar`). A refresh uses the cookie of the most recent login.
- Every error has the shape `{"error": {"code", "message", "request_id", "details?"}}`.

**Where to watch** the background work: Temporal UI http://localhost:8088 (workflows), Mailpit http://localhost:8025 (emails), Jaeger http://localhost:16686
(traces), Grafana http://localhost:3001 (dashboards).

## 1 Auth

### 1. Register (candidate)

`POST /api/identity/auth/register` · Token: none

```json
{
  "email": "{{candidateEmail}}",
  "password": "{{password}}",
  "full_name": "Ali Khan",
  "roles": [
    "CANDIDATE"
  ]
}
```

> Role: public (no token). Body: `full_name` (required), `current_company` (optional), `roles` (one or both of CANDIDATE, RECRUITER; never ADMIN).
> Success: 201 Created, body `{id, full_name, current_company, roles, ...}`. Saves `id` into `candidateId`.

Saves: `candidateId`

### 2. Register (recruiter)

`POST /api/identity/auth/register` · Token: none

```json
{
  "email": "{{recruiterEmail}}",
  "password": "{{password}}",
  "full_name": "Jane Recruiter",
  "current_company": "Acme",
  "roles": [
    "RECRUITER"
  ]
}
```

> Role: public (no token). Same body as any registration: `full_name`, optional `current_company` (current employer), `roles`.
> Success: 201 Created.

### 3. Login (candidate)

`POST /api/identity/auth/login` · Token: none

```json
{
  "email": "{{candidateEmail}}",
  "password": "{{password}}"
}
```

> Role: public (no token). Rate limit: 30/min per IP.
> Success: 200 OK, body `{access_token, ...}` and sets the httpOnly, Secure `refresh_token` cookie (path `/api/identity/auth`). Saves `access_token` into `candidateToken`.

Saves: `candidateToken`

### 4. Login (recruiter)

`POST /api/identity/auth/login` · Token: none

```json
{
  "email": "{{recruiterEmail}}",
  "password": "{{password}}"
}
```

> Role: public (no token). Rate limit: 30/min per IP.
> Success: 200 OK, body `{access_token, ...}` and sets the httpOnly, Secure `refresh_token` cookie (path `/api/identity/auth`). Saves `access_token` into `recruiterToken`.

Saves: `recruiterToken`

### 5. Refresh access token

`POST /api/identity/auth/refresh` · Token: none

> Role: public; authenticated by the httpOnly, Secure `refresh_token` cookie (path `/api/identity/auth`) set by Login. Postman's cookie jar keeps and sends it automatically; the cookie belongs to the most recent Login.
> Success: 200 OK, new `access_token`.

### 6. Get current user

`GET /api/identity/users/me` · Token: candidate token

> Role: any authenticated user (uses `candidateToken`).
> Success: 200 OK.

### 7. Update my name or employer

`PATCH /api/identity/users/me` · Token: candidate token

```json
{
  "full_name": "Ali Khan",
  "current_company": "Freelance"
}
```

> Role: any authenticated user (uses `candidateToken`). Send only the fields to change; `current_company: null` clears it.
> Success: 200 OK; emits `UserProfileUpdated` (field names only). The token's `name` claim changes on the next refresh.

### 8. Recruiter adds the CANDIDATE role

`POST /api/identity/users/me/roles` · Token: recruiter token

```json
{
  "role": "CANDIDATE"
}
```

> Role: any authenticated user (uses `recruiterToken`). Adds CANDIDATE or RECRUITER to my own account (never ADMIN).
> Success: 201 Created (409 ROLE_ALREADY_HELD on a second run). Emits `RoleGranted`; Candidate creates the profile. Then run the next request: the current token does not have the new role yet.

### 9. Refresh recruiter token (after adding a role)

`POST /api/identity/auth/refresh` · Token: none

> Role: public; uses the refresh cookie of the MOST RECENT login, so run "Login (recruiter)" before "Recruiter adds the CANDIDATE role".
> Success: 200 OK. Saves `access_token` into `recruiterToken`; its `roles` claim now lists CANDIDATE and RECRUITER.

Saves: `recruiterToken`

### 10. JWKS

`GET /api/identity/.well-known/jwks.json` · Token: none

> Role: public (no token). Identity's public signing keys.
> Success: 200 OK.

### 11. OpenID configuration

`GET /api/identity/.well-known/openid-configuration` · Token: none

> Role: public (no token).
> Success: 200 OK.

## 2 Jobs (recruiter)

### 12. Create job

`POST /api/jobs` · Token: recruiter token

```json
{
  "title": "Senior Backend Engineer",
  "description": "Build and operate Python microservices.",
  "location": "Islamabad",
  "employment_type": "FULL_TIME",
  "required_skills": [
    "python",
    "fastapi",
    "postgresql"
  ],
  "eligibility_rules": {
    "min_experience_years": 2
  },
  "application_limit": 100,
  "deadline": "{{deadline}}",
  "stages": [
    "Screening",
    "Interview",
    "Offer"
  ]
}
```

> Role: RECRUITER.
> Success: 201 Created, job in status DRAFT. Saves `id` into `jobId`.

Saves: `jobId`, `jobVersion`

### 13. List my jobs

`GET /api/jobs?mine=true` · Token: recruiter token

> Role: RECRUITER (`mine=true` returns the recruiter's own jobs, including drafts).
> Success: 200 OK, `{items, next_cursor}`.

### 14. Get job

`GET /api/jobs/{{jobId}}` · Token: recruiter token

> Role: public for READY/CLOSED jobs; owner otherwise (uses `recruiterToken`).
> Success: 200 OK.

Saves: `jobVersion`

### 15. Update job

`PUT /api/jobs/{{jobId}}` · Token: recruiter token

```json
{
  "title": "Senior Backend Engineer",
  "description": "Build and operate Python microservices with Kafka.",
  "location": "Islamabad",
  "employment_type": "FULL_TIME",
  "required_skills": [
    "python",
    "fastapi",
    "postgresql"
  ],
  "eligibility_rules": {
    "min_experience_years": 2
  },
  "application_limit": 100,
  "deadline": "{{deadline}}",
  "stages": [
    "Screening",
    "Interview",
    "Offer"
  ],
  "version": {{jobVersion}}
}
```

> Role: RECRUITER (owner). Same body as Create plus `version` (optimistic lock).
> Success: 200 OK.
> `version` comes from `jobVersion` (saved by Create job / Get job / Update job): run Get job first if the job changed since.

Saves: `jobVersion`

### 16. Publish job

`POST /api/jobs/{{jobId}}/publish` · Token: recruiter token

> Role: RECRUITER (owner).
> Success: 202 Accepted; status PROCESSING, then READY when the publishing workflow completes.

### 17. List jobs (public)

`GET /api/jobs` · Token: none

> Role: public (no token).
> Success: 200 OK, `{items, next_cursor}`.

## 3 Candidate profile & resume

### 18. Get my profile

`GET /api/candidates/me` · Token: candidate token

> Role: CANDIDATE.
> Success: 200 OK.

### 19. Update my profile

`PUT /api/candidates/me` · Token: candidate token

```json
{
  "headline": "Backend developer",
  "skills": [
    "python",
    "fastapi",
    "postgresql"
  ],
  "experience_years": 3,
  "education": [
    {
      "institution": "NUST",
      "degree": "BS",
      "field": "Computer Science",
      "start_year": 2017,
      "end_year": 2021
    }
  ]
}
```

> Role: CANDIDATE.
> Success: 200 OK.
> No `full_name` here: the name lives in Identity (PATCH /identity/users/me).

### 20. Upload resume

`POST /api/candidates/me/resumes` · Token: candidate token

Body (form-data): `file` = files/Ali_Khan_Sample_Resume.pdf

> Role: CANDIDATE. form-data field `file` (PDF or DOCX, max 5 MB).
> Success: 202 Accepted; the resume is parsed in the background.
> The file is `postman/files/Ali_Khan_Sample_Resume.pdf` (fictional). If Postman can't find it, pick it again in the Body tab, or set Settings > Working directory to the repo's `postman/` folder.

### 21. List my resumes

`GET /api/candidates/me/resumes` · Token: candidate token

> Role: CANDIDATE.
> Success: 200 OK.

### 22. Get candidate

`GET /api/candidates/{{candidateId}}` · Token: candidate token

> Role: the candidate themselves, RECRUITER, or ADMIN (uses `candidateToken`).
> Success: 200 OK.

### 23. Get resume download link

`GET /api/candidates/{{candidateId}}/resume/link` · Token: candidate token

> Role: the candidate themselves, RECRUITER, or ADMIN (uses `candidateToken`).
> Success: 200 OK, `{url, expires_in}` (presigned URL, valid 5 minutes).

## 4 Applications (candidate)

### 24. Apply

`POST /api/applications` · Token: candidate token

Headers: `Idempotency-Key: {{idempotencyKey}}`

```json
{
  "job_id": "{{jobId}}"
}
```

> Role: CANDIDATE. Header `Idempotency-Key` required; the pre-request script generates a fresh `{{$guid}}` and stores it in `idempotencyKey`.
> Success: 202 Accepted. Saves `id` into `applicationId`.

Saves: `applicationId`, `applicationVersion`

### 25. Apply again (same key)

`POST /api/applications` · Token: candidate token

Headers: `Idempotency-Key: {{idempotencyKey}}`

```json
{
  "job_id": "{{jobId}}"
}
```

> Role: CANDIDATE. Reuses the first Apply request's `Idempotency-Key` (stored in `idempotencyKey` by that request's pre-request script) instead of generating a new one. The service returns the same application as the first call.
> Success: 202 Accepted (same response as the first Apply).

### 26. Apply to my own job (blocked)

`POST /api/applications` · Token: recruiter token

Headers: `Idempotency-Key: {{idempotencyKey}}`

```json
{
  "job_id": "{{jobId}}"
}
```

> Role: CANDIDATE, but uses `recruiterToken` after "Recruiter adds the CANDIDATE role" + "Refresh recruiter token": the recruiter now holds CANDIDATE and tries to apply to `jobId`, which they posted.
> Expected: 403 `CANNOT_APPLY_TO_OWN_JOB`.

### 27. List my applications

`GET /api/applications/me` · Token: candidate token

> Role: CANDIDATE.
> Success: 200 OK.

### 28. Get application

`GET /api/applications/{{applicationId}}` · Token: candidate token

> Role: the applicant or the job owner (uses `candidateToken`).
> Success: 200 OK, includes stage history.

Saves: `applicationVersion`

## 5 Applications (recruiter)

### 29. Recruiter board

`GET /api/jobs/{{jobId}}/applications` · Token: recruiter token

> Role: RECRUITER (job owner). Routed by the gateway to the Application service.
> Success: 200 OK.

### 30. Change stage

`PATCH /api/applications/{{applicationId}}/stage` · Token: recruiter token

```json
{
  "to_stage": "Interview",
  "version": {{applicationVersion}},
  "reason": "Strong skills match"
}
```

> Role: RECRUITER (job owner). `version` is the optimistic lock.
> Success: 200 OK.
> `version` comes from `applicationVersion`: run Get application first (scoring moves the application to SCREENING, which changes its version).

Saves: `applicationVersion`

### 31. Add note

`POST /api/applications/{{applicationId}}/notes` · Token: recruiter token

```json
{
  "body": "Good communication in the first call."
}
```

> Role: RECRUITER (job owner).
> Success: 201 Created.

### 32. Make decision

`POST /api/applications/{{applicationId}}/decision` · Token: recruiter token

```json
{
  "decision": "HIRED",
  "version": {{applicationVersion}},
  "reason": "Passed all stages"
}
```

> Role: RECRUITER (job owner). `decision` is HIRED or REJECTED; `version` is the optimistic lock.
> Success: 200 OK.
> `version` comes from `applicationVersion`: run Get application first (scoring moves the application to SCREENING, which changes its version).

## 6 BFF screens

### 33. Application detail screen

`GET /api/bff/applications/{{applicationId}}` · Token: recruiter token

> Role: the applicant or the job owner (uses `recruiterToken`).
> Success: 200 OK, `{application, candidate, score, resume_download, degraded: [..]}`.

### 34. Recruiter dashboard

`GET /api/bff/recruiter/dashboard` · Token: recruiter token

> Role: RECRUITER.
> Success: 200 OK, `{jobs, metrics, degraded}`.

## 7 Analytics

### 35. Metrics

`GET /api/analytics/metrics` · Token: recruiter token

> Role: RECRUITER or ADMIN (uses `recruiterToken`).
> Success: 200 OK.

## 8 Wrap up (run last)

Ends the demo: close the job, try to withdraw, log out. Run after folders 1-7.

### 36. Close job

`POST /api/jobs/{{jobId}}/close` · Token: recruiter token

> Role: RECRUITER (owner). Success: 200 OK. Applications still SUBMITTED are closed; ones the recruiter already acted on keep their state.

### 37. Withdraw application

`POST /api/applications/{{applicationId}}/withdraw` · Token: candidate token

> Role: CANDIDATE (the applicant). Success: 200 OK while the application is active. In a full run the recruiter has already made a decision (folder 5), so this returns 409: a HIRED/REJECTED application is final.

### 38. Logout all sessions

`POST /api/identity/auth/logout-all` · Token: candidate token

> Role: any authenticated user (uses `candidateToken`). Revokes all sessions.
> Success: 204 No Content. Demo: run at the end.

### 39. Logout

`POST /api/identity/auth/logout` · Token: none

> Role: public; authenticated by the httpOnly, Secure `refresh_token` cookie (path `/api/identity/auth`) set by Login. Postman's cookie jar keeps and sends it automatically.
> Success: 204 No Content. Demo: run at the end.

## 9 Admin

### 40. Login (admin)

`POST /api/identity/auth/login` · Token: none

```json
{
  "email": "{{adminEmail}}",
  "password": "{{password}}"
}
```

> Role: public (no token). Admins can't self-register: create one first with `docker compose exec -it identity python -m app.cli create-admin admin@example.com` (same password as `password`, or edit it here).
> Success: 200 OK. Saves `access_token` into `adminToken`.

Saves: `adminToken`

### 41. Revoke access token (by jti)

`POST /api/identity/auth/revoke/:jti` · Token: admin token

> Role: ADMIN. Revokes ONE access token: the pre-request script takes the jti from `candidateToken`. Success: 204 No Content; that token now gets 401 at the gateway.

### 42. Suspend user

`POST /api/identity/users/{{candidateId}}/suspend` · Token: admin token

> Role: ADMIN. Uses `candidateId` (equal to the Identity user id) as the target.
> Success: 204 No Content.

### 43. Grant a role

`POST /api/identity/users/{{candidateId}}/roles` · Token: admin token

```json
{
  "role": "RECRUITER"
}
```

> Role: ADMIN. Grants any role (ADMIN included) to `candidateId`.
> Success: 201 Created (409 ROLE_ALREADY_HELD). Emits `RoleGranted`.

### 44. Revoke a role

`DELETE /api/identity/users/{{candidateId}}/roles/RECRUITER` · Token: admin token

> Role: ADMIN. Takes the role away and bumps `token_version` + sets `revoked_before`: the user's current tokens stop working everywhere; they log in again or refresh.
> Success: 204 No Content (404 ROLE_NOT_HELD; 409 LAST_ROLE if it is their only role). Emits `RoleRevoked`.

## 10 Internal (service-to-service, NOT via gateway)

Direct service URLs inside the local environment. These endpoints are not exposed through the gateway and take no token.

Not part of the flow: these call services directly (not through the gateway), for debugging. Paths are full URLs.

### 45. Identity: user contact

`GET http://localhost:8001/internal/users/{{candidateId}}/contact` · Token: none

> Internal service-to-service endpoint. Not exposed through the gateway; no token. Called by Notification.
> Success: 200 OK.

### 46. Candidate: resume for scoring

`GET http://localhost:8004/internal/candidates/{{candidateId}}/resume` · Token: none

> Internal service-to-service endpoint. Not exposed through the gateway; no token. Called by Scoring.
> Success: 200 OK.

### 47. Job Enrichment: structured job

`GET http://localhost:8003/jobs/{{jobId}}/structured` · Token: none

> Internal service-to-service endpoint. Not exposed through the gateway; no token.
> Success: 200 OK.

### 48. Job Enrichment: search jobs

`GET http://localhost:8003/search/jobs?q=python` · Token: none

> Internal service-to-service endpoint. Not exposed through the gateway; no token.
> Success: 200 OK.

### 49. Scoring: score

`GET http://localhost:8006/scores/{{applicationId}}` · Token: none

> Internal service-to-service endpoint. Not exposed through the gateway; no token. Called by the BFF.
> Success: 200 OK.

### 50. Application: board summary

`GET http://localhost:8005/jobs/{{jobId}}/applications/summary` · Token: none

> Internal service-to-service endpoint. Not exposed through the gateway; no token. Called by the BFF. Application runs 2 replicas without a host port; call it from inside the Compose network.
> Success: 200 OK.
