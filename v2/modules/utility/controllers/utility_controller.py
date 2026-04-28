import os
from pathlib import Path
from fastapi import APIRouter, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel, Field
import pdfkit

from v2.utils.env import init_env

router = APIRouter(prefix="/utility", tags=["Utility"])


class GeneratePdfRequest(BaseModel):
    html: str = Field(..., min_length=1, description="HTML content to render as PDF")


class DepseudonymizeReportRequest(BaseModel):
    user_id: str = Field(..., min_length=1, description="User ID used in storage path")
    session_id: str = Field(..., min_length=1, description="Session ID used in storage path")
    job_id: str = Field(..., min_length=1, description="Job ID (for example JOB_123)")


def _find_wkhtmltopdf_path() -> str | None:
    """
    Find wkhtmltopdf executable path with fallback priority:
    1. PATH_WKHTMLTOPDF environment variable (highest priority override)
    2. Constants value (path_wkhtmltopdf) - use directly if set
    3. Common Linux/Unix paths (if file exists)
    4. System PATH lookup
    5. None (let pdfkit auto-detect)
    """
    # Priority 1: Environment variable override (highest priority)
    env_path = os.getenv("PATH_WKHTMLTOPDF", "").strip()
    if env_path:
        return env_path
    
    # Priority 2: Constants value - use directly if provided (user has configured it)
    constants = init_env()
    constants_path = constants.get("path_wkhtmltopdf")
    if constants_path:
        # Use the path from constants directly, even if file check fails
        # (file might exist at runtime in container/environment)
        return str(constants_path).strip()
    
    # Priority 3: Common Linux/Unix paths (only if file exists)
    common_paths = [
        "/usr/local/bin/wkhtmltopdf",
        "/usr/bin/wkhtmltopdf",
        "/bin/wkhtmltopdf",
        "/opt/wkhtmltopdf/bin/wkhtmltopdf",
    ]
    for common_path in common_paths:
        path = Path(common_path)
        if path.exists() and path.is_file():
            return str(path.resolve())
    
    # Priority 4: Try to find in PATH
    import shutil
    which_path = shutil.which("wkhtmltopdf")
    if which_path:
        return which_path
    
    # Return None to let pdfkit auto-detect
    return None


def _resolve_job_paths(user_id: str, session_id: str, job_id: str) -> tuple[Path, Path]:
    """
    Resolve input/output paths for depseudonymization on Cloud Run mount.
    Expected layout:
      {DATA_DIR}/{user_id}/{session_id}/input_data
      {DATA_DIR}/{user_id}/{session_id}/output_data/{job_id}
    """
    data_dir = (os.getenv("DATA_DIR") or "").strip()
    if not data_dir:
        raise RuntimeError("DATA_DIR environment variable is required for depseudonymization")

    base = Path(data_dir) / user_id / session_id
    input_dir = base / "input_data"
    job_output_dir = base / "output_data" / job_id
    return input_dir, job_output_dir


@router.post("/generate-pdf")
async def generate_pdf(req: GeneratePdfRequest):
    """Generate a PDF from HTML and return it as a downloadable file."""
    path_wkhtmltopdf = _find_wkhtmltopdf_path()
    
    try:
        # If path is None, pdfkit will try to auto-detect
        if path_wkhtmltopdf:
            config = pdfkit.configuration(wkhtmltopdf=path_wkhtmltopdf)
        else:
            # Let pdfkit auto-detect (may raise exception if not found)
            config = pdfkit.configuration()
        
        options = {
            "enable-local-file-access": "",
            "no-stop-slow-scripts": "",
            "load-error-handling": "ignore",
            "load-media-error-handling": "ignore",
            "print-media-type": None,
        }
        pdf_data = pdfkit.from_string(req.html, False, configuration=config, options=options)
        return Response(
            content=pdf_data,
            media_type="application/pdf",
            headers={"Content-Disposition": "attachment; filename=report.pdf"},
        )
    except Exception as exc:
        error_msg = str(exc)
        if "wkhtmltopdf" in error_msg.lower() or "executable" in error_msg.lower():
            raise HTTPException(
                status_code=500,
                detail=(
                    f"Failed to generate PDF: {error_msg}. "
                    "Please ensure wkhtmltopdf is installed and accessible. "
                    "You can set PATH_WKHTMLTOPDF environment variable to specify the executable path."
                )
            )
        raise HTTPException(status_code=500, detail=f"Failed to generate PDF: {error_msg}")


@router.post("/depseudonymize-report")
async def depseudonymize_report(req: DepseudonymizeReportRequest):
    """
    Standalone depseudonymization endpoint (v2-owned pipeline).
    Execution layer no longer contains this implementation.
    """
    try:
        _input_dir, job_output_dir = _resolve_job_paths(req.user_id, req.session_id, req.job_id)

        if not job_output_dir.exists():
            raise HTTPException(
                status_code=404,
                detail=f"Job output directory not found: {job_output_dir}",
            )
        raise HTTPException(
            status_code=501,
            detail=(
                "Depseudonymization pipeline is not implemented in v2 yet. "
                "This endpoint is reserved for the v2 implementation."
            ),
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Depseudonymization failed: {str(exc)}")
