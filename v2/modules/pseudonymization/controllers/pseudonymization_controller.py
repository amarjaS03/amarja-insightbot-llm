"""
Pseudonymization (depseudonymization) API controller.

Standalone module - not part of utility.
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from v2.common.logger import add_log
from v2.modules.pseudonymization.services.pseudonymization_service import PseudonymizationService

router = APIRouter(prefix="/pseudonymization", tags=["Pseudonymization"])


class DepseudonymizeReportRequest(BaseModel):
    user_id: str = Field(..., min_length=1, description="User ID used in storage path")
    session_id: str = Field(..., min_length=1, description="Session ID used in storage path")
    job_id: str = Field(
        ...,
        min_length=1,
        description="Job ID for GCS path (e.g. JOB_123). If Firestore job_id is passed, use firestore_job_id for updates.",
    )
    firestore_job_id: str | None = Field(
        default=None,
        description="Optional Firestore job document ID for updating report_url. If omitted, job update is skipped.",
    )


@router.post("/depseudonymize-report")
async def depseudonymize_report(req: DepseudonymizeReportRequest):
    """
    Run full depseudonymization pipeline:

    1. Download all data from GCS ({user_id}/{session_id}/input_data, output_data/{job_id}) to local pseudonymize/{job_id}
    2. Read original report (analysis_report_original.html or analysis_report.html)
    3. Process images (depseudonymize datasets, recreate images)
    4. Process text (LLM tagging + local replacement)
    5. Convert images to base64
    6. Save report and upload to GCS (overwrites analysis_report.html)
    7. Replace report_url in job if firestore_job_id provided
    """
    # Normalize job_id for GCS: use as-is if already JOB_xxx, else construct
    gcs_job_id = req.job_id if req.job_id.upper().startswith("JOB_") else f"JOB_{req.job_id}"
    firestore_job_id = req.firestore_job_id or (req.job_id if not req.job_id.upper().startswith("JOB_") else None)

    add_log(f"[Pseudonymization] Starting pipeline for user={req.user_id} session={req.session_id} job={gcs_job_id}")

    # Token check: ensure user has sufficient tokens before starting
    from v2.modules.utility.services.token_service import check_can_proceed
    can_proceed, token_msg = await check_can_proceed(req.user_id, "depseudonymization")
    if not can_proceed:
        raise HTTPException(status_code=402, detail=token_msg)
    add_log(f"[Pseudonymization] Token check passed: {token_msg}")

    try:
        service = PseudonymizationService()
        result = await service.run_pipeline(
            user_id=req.user_id,
            session_id=req.session_id,
            job_id=gcs_job_id,
            firestore_job_id=firestore_job_id,
        )
    except Exception as e:
        add_log(f"[Pseudonymization] Pipeline error: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e

    if result.get("status") != "success":
        raise HTTPException(
            status_code=500,
            detail=result.get("error", "Depseudonymization pipeline failed"),
        )

    # Update user tokens: depseudonymization uses LLM for tagging - use required amount as estimate
    from v2.modules.utility.services.token_service import add_used_tokens, get_required_tokens
    tokens_used = result.get("details", {}).get("depseudonymization", {}).get("tokens_used")
    if tokens_used is None:
        tokens_used = get_required_tokens("depseudonymization")
    if tokens_used > 0 and req.user_id:
        await add_used_tokens(req.user_id, tokens_used)
        add_log(f"[Pseudonymization] Added {tokens_used:,} used tokens for user {req.user_id}")

    return {
        "status": "success",
        "report_url": result.get("report_url"),
        "details": result.get("details", {}),
    }
