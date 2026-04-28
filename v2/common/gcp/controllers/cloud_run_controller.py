"""
Cloud Run controller (v2/common/gcp).

Manual testing API: create per-job Cloud Run execution service.
Kept intentionally decoupled from job/session/job_framework modules.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from v2.common.model.api_response import ApiResponse
from v2.common.gcp.services.gcp_cloud_run_service import GcpCloudRunService
from v2.common.logger import add_log


router = APIRouter(prefix="/gcp/cloud-run", tags=["GCP Cloud Run"])


def get_cloud_run_service() -> GcpCloudRunService:
    return GcpCloudRunService()


class CreateRunServiceRequest(BaseModel):
    user_id: str = Field(..., min_length=1)
    session_id: str = Field(..., min_length=1)
    job_id: str = Field(..., min_length=1)


# @router.post("/service/create", response_model=ApiResponse[dict])
# async def create_run_service(
#     req: CreateRunServiceRequest,
#     service: GcpCloudRunService = Depends(get_cloud_run_service),
#     ):
#     """
#     Create/replace a per-job Cloud Run service.

#     Uses (user_id, session_id, job_id) only for:
#     - service name (`exec-<job_id>`)
#     - ensuring bucket prefixes exist for IO.
#     """
#     try:
#         result = service.deploy_job_service(user_id=req.user_id, job_id=req.job_id, session_id=req.session_id)
#         return ApiResponse(status="success", statusCode=200, message="Cloud Run service created", data=result)
#     except Exception as e:
#         add_log(f"[GCP][CLOUD_RUN] create service failed: {str(e)}")
#         raise HTTPException(status_code=500, detail=str(e))

