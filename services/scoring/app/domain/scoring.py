"""Rule-based fit score, 0-100 (pure, no I/O). Every point is explainable, so the breakdown can tell a
recruiter (or the AI assistant, Phase 16) WHY a candidate scored what they did.

    70  required skills   share of the job's must-haves the candidate has
    10  other job skills  share of the remaining skills found in the job description
    20  experience        candidate years vs the job's minimum (full points at or above it)

Skill names are compared after the shared normalization (smarthire_common.skills), so "k8s" in a resume
matches "Kubernetes" in a job. Semantic similarity is added in Phase 15."""

from smarthire_common.skills import normalize

REQUIRED_POINTS, OTHER_POINTS, EXPERIENCE_POINTS = 70.0, 10.0, 20.0


def _share(wanted: list[str], have: set[str]) -> float:
    return len([s for s in wanted if s in have]) / len(wanted) if wanted else 1.0


def score(*, job_skills: list[str], required_skills: list[str], candidate_skills: list[str],
          min_experience_years: float | None, candidate_years: float | None) -> dict:
    have = {normalize(s) for s in candidate_skills}
    required = list(dict.fromkeys(normalize(s) for s in required_skills)) or list(dict.fromkeys(normalize(s) for s in job_skills))
    others = [s for s in dict.fromkeys(normalize(s) for s in job_skills) if s not in required]

    if min_experience_years:
        experience = EXPERIENCE_POINTS * min(1.0, (candidate_years or 0) / min_experience_years)
    else:  # no minimum: full points for any stated experience, half when we know nothing
        experience = EXPERIENCE_POINTS if candidate_years is not None else EXPERIENCE_POINTS / 2

    total = REQUIRED_POINTS * _share(required, have) + OTHER_POINTS * _share(others, have) + experience
    return {
        "score": round(total, 2),
        "breakdown": {
            "matched_skills": [s for s in required + others if s in have],
            "missing_skills": [s for s in required if s not in have],  # must-haves only: what to ask about
            "experience_match": {"required_years": min_experience_years, "candidate_years": candidate_years,
                                 "points": round(experience, 2)},
        },
    }
