"""Candidate's actions (design v2.0/2.1, flow W9).

ResumeProcessingWorkflow   action "upload-resume"   id resume-{resume id}
    commit: register_resume (Candidate)  ->  parse_resume (candidate-parser)
The file is already in MinIO when the workflow starts; only its object key travels through Temporal
(payloads are limited to about 2 MB, and files have no business in a workflow history)."""

from datetime import timedelta

from temporalio import workflow
from temporalio.exceptions import ActivityError

from app.workflows.base import COMMIT_RETRY, CommitFirst, business_error, call


@workflow.defn
class ResumeProcessingWorkflow(CommitFirst):
    @workflow.run
    async def run(self, params: dict) -> str:
        if not await self.wait_for_commit():
            return "REJECTED"
        resume_id = self.committed["id"]
        try:
            # Parsing a big PDF takes seconds: a generous per-attempt timeout, and give up after an hour
            # of the parser being down (the resume stays UPLOADED and can be uploaded again)
            await call("parse_resume", "candidate_parser", "POST", f"/internal/resumes/{resume_id}/parse",
                       timeout_seconds=120, give_up_after=timedelta(hours=1))
        except ActivityError as err:
            if business_error(err) is not None:
                return "UNREADABLE"  # 422: the file can't be read; the resume is FAILED with the reason
            return "PARSER_UNAVAILABLE"
        # Phase 15: embed_resume (semantic index) goes here
        return "PARSED"

    @workflow.update
    async def commit(self, params: dict) -> dict:
        return await self.run_commit(
            lambda: call("register_resume", "candidate", "POST", "/internal/resumes", params["resume"], retry=COMMIT_RETRY))
