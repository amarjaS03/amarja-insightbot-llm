"""
Performance Controller for FastAPI v2

Admin-only endpoints for querying job performance data built from milestones.

Endpoints:
    GET /performance/report          - Flat performance table (JSON)
    GET /performance/report/excel    - Flat performance table (Excel download)
"""
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException, Request, Query
from fastapi.responses import StreamingResponse
from v2.common.model.api_response import ApiResponse
from v2.common.logger import add_log
from v2.modules.performance.services.performance_report_service import PerformanceReportService
import traceback

router = APIRouter(prefix="/performance")


# ─── Auth Dependencies ──────────────────────────────────────────────────

async def _require_admin(request: Request) -> Dict[str, Any]:
    """Dependency: require authenticated admin user."""
    user = getattr(request.state, "user", None)
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return user


# ─── Shared filter parameters ───────────────────────────────────────────

def _parse_filters(
    user_id: Optional[str],
    session_id: Optional[str],
    from_date: Optional[str],
    to_date: Optional[str],
):
    """Parse and validate common filter query params."""
    parsed_from = None
    parsed_to = None
    if from_date:
        try:
            parsed_from = datetime.fromisoformat(from_date)
            if parsed_from.tzinfo is None:
                parsed_from = parsed_from.replace(tzinfo=timezone.utc)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid from_date format: '{from_date}'. Use ISO 8601 (e.g. 2026-01-15 or 2026-01-15T10:00:00).",
            )
    if to_date:
        try:
            parsed_to = datetime.fromisoformat(to_date)
            if parsed_to.tzinfo is None:
                parsed_to = parsed_to.replace(tzinfo=timezone.utc)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid to_date format: '{to_date}'. Use ISO 8601 (e.g. 2026-01-31 or 2026-01-31T23:59:59).",
            )
    return user_id, session_id, parsed_from, parsed_to


# ─── Endpoints ──────────────────────────────────────────────────────────

@router.get("/report", response_model=ApiResponse[List[Dict[str, Any]]])
async def get_performance_report(
    user_id: Optional[str] = Query(default=None, description="Filter by user ID"),
    session_id: Optional[str] = Query(default=None, description="Filter by session ID"),
    from_date: Optional[str] = Query(default=None, description="Start date (ISO 8601, e.g. 2026-01-15)"),
    to_date: Optional[str] = Query(default=None, description="End date (ISO 8601, e.g. 2026-01-31)"),
    admin_user: Dict[str, Any] = Depends(_require_admin),
):
    """
    Get flat performance report table as JSON.

    Each row represents a milestone (sub-task) within a job.
    Duration is computed as the gap between consecutive milestone timestamps.

    Filters (all optional, combinable):
        - user_id:    restrict to jobs owned by this user
        - session_id: restrict to jobs in this session
        - from_date:  jobs created on or after this date
        - to_date:    jobs created on or before this date

    Admin only.
    """
    try:
        uid, sid, dt_from, dt_to = _parse_filters(user_id, session_id, from_date, to_date)
        service = PerformanceReportService()
        rows = await service.get_performance_report(
            user_id=uid, session_id=sid, from_date=dt_from, to_date=dt_to
        )
        return ApiResponse[List[Dict[str, Any]]](
            status="success",
            statusCode=200,
            message=f"Performance report: {len(rows)} rows",
            data=rows,
        )
    except HTTPException:
        raise
    except Exception as e:
        add_log(f"PerformanceController: Error generating report: {str(e)} | traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail="Failed to generate performance report")


@router.get("/report/excel")
async def export_performance_report_excel(
    user_id: Optional[str] = Query(default=None, description="Filter by user ID"),
    session_id: Optional[str] = Query(default=None, description="Filter by session ID"),
    from_date: Optional[str] = Query(default=None, description="Start date (ISO 8601)"),
    to_date: Optional[str] = Query(default=None, description="End date (ISO 8601)"),
    admin_user: Dict[str, Any] = Depends(_require_admin),
):
    """
    Export the flat performance report as an Excel (.xlsx) download.

    Same filters as the JSON endpoint.

    Admin only.
    """
    try:
        uid, sid, dt_from, dt_to = _parse_filters(user_id, session_id, from_date, to_date)
        service = PerformanceReportService()
        buf = await service.export_to_excel(
            user_id=uid, session_id=sid, from_date=dt_from, to_date=dt_to
        )

        filename = "performance_report"
        if uid:
            filename += f"_user_{uid[:8]}"
        if sid:
            filename += f"_session_{sid[:8]}"
        filename += ".xlsx"

        return StreamingResponse(
            buf,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
    except HTTPException:
        raise
    except Exception as e:
        add_log(f"PerformanceController: Error exporting Excel: {str(e)} | traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail="Failed to export performance report")
