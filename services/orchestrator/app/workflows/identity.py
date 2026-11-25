"""Identity's actions (design v2.0/2.1, flows W1 and W2).

RegistrationWorkflow   action "register"     id register-{sha256 of the email}
    commit: create_user (Identity)  ->  candidate profile if CANDIDATE  ->  fact UserRegistered
RoleChangeWorkflow     actions "grant-role", "revoke-role"   id role-{user}-{role}-grant|revoke
    commit: grant_role / revoke_role (Identity)  ->  candidate profile on a CANDIDATE grant  ->  fact

The public endpoints check the rules first (email taken, role held, last role) and answer 409/404
without starting anything; the commit steps check again inside their transaction."""

from temporalio import workflow

from app.workflows.base import COMMIT_RETRY, CommitFirst, call, record_fact, rejected


@workflow.defn
class RegistrationWorkflow(CommitFirst):
    @workflow.run
    async def run(self, params: dict) -> str:
        if not await self.is_commit_accepted():
            return "REJECTED"
        user = self.committed
        if "CANDIDATE" in user["roles"]:
            await call("create_candidate_profile", "candidate", "PUT", f"/internal/candidates/{user['id']}")
        # `role` = the first role: kept for facts copied from v1 events, which only had that field
        await record_fact("UserRegistered", {"user_id": user["id"], "role": user["roles"][0], "roles": user["roles"]})
        return "DONE"

    @workflow.update
    async def commit(self, params: dict) -> dict:
        user = await self.run_commit(
            lambda: call("create_user", "identity", "POST", "/internal/users", params["user"], retry=COMMIT_RETRY))
        # The id is the email's hash, so a SECOND registration with the same email can attach to this
        # workflow while it runs. Its user id differs (made per request): it is a duplicate, not a retry
        if user["id"] != params["user"]["id"]:
            raise rejected(409, "EMAIL_ALREADY_REGISTERED", "This email is already registered")
        return user


@workflow.defn
class RoleChangeWorkflow(CommitFirst):
    @workflow.run
    async def run(self, params: dict) -> str:
        if not await self.is_commit_accepted():
            return "REJECTED"
        user_id, role = params["user_id"], params["role"]
        if "revoked_by" in params:
            await record_fact("RoleRevoked", {"user_id": user_id, "role": role})
        else:
            if role == "CANDIDATE":  # a recruiter taking the CANDIDATE role gets a profile now
                await call("create_candidate_profile", "candidate", "PUT", f"/internal/candidates/{user_id}")
            await record_fact("RoleGranted", {"user_id": user_id, "role": role})
        return "DONE"

    @workflow.update
    async def commit(self, params: dict) -> dict:
        user_id, role = params["user_id"], params["role"]
        if "revoked_by" in params:
            return await self.run_commit(lambda: call(
                "revoke_role", "identity", "DELETE", f"/internal/users/{user_id}/roles/{role}", retry=COMMIT_RETRY))
        return await self.run_commit(lambda: call(
            "grant_role", "identity", "POST", f"/internal/users/{user_id}/roles",
            {"role": role, "granted_by": params["granted_by"]}, retry=COMMIT_RETRY))
