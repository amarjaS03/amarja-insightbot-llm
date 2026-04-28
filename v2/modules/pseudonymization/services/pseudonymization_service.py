"""
Pseudonymization (depseudonymization) pipeline service.

Flow:
1. Download all data from GCS to local pseudonymize/{job_id}/ (input_data + output_data)
2. Run depseudonymization: datasets -> text (LLM tag + regex replace) -> base64
3. Save: image depseudonymization outputs, tagged report, overwrite analysis_report.html
4. Upload analysis_report.html to GCS, update job report_url
"""

import asyncio
import json
import shutil
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from v2.common.logger import add_log
from v2.utils.env import init_env


def _build_pseudonymized_columns_map(domain_data: dict) -> Dict[str, List[str]]:
    """
    Build pseudonymization map from domain_directory or domain_dictionary:
    { "data_set_name.pkl": ["col1","col2"], ... }
    """
    out: Dict[str, List[str]] = {}
    dsf = domain_data.get("data_set_files", {}) if isinstance(domain_data, dict) else {}
    if not isinstance(dsf, dict):
        return out
    for ds_name, meta in dsf.items():
        if not isinstance(meta, dict):
            continue
        cols = meta.get("pseudonymized_columns") or meta.get("pseudonymized_cols") or []
        if not isinstance(cols, list):
            cols = []
        cleaned: List[str] = []
        seen = set()
        for c in cols:
            cs = str(c).strip()
            if cs and cs.lower() not in seen:
                seen.add(cs.lower())
                cleaned.append(cs)
        out[str(ds_name)] = cleaned
    return out


def _load_domain_for_pseudonymization(input_dir: Path) -> dict:
    """Load domain_directory.json from input_data folder."""
    path = input_dir / "domain_directory.json"
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            add_log(f"PseudonymizationService: Error loading domain_directory.json: {e}")
    return {}


class PseudonymizationService:
    """Service for running the depseudonymization pipeline."""

    def __init__(self, storage_service=None, job_manager=None):
        from v2.common.gcp import GcpManager
        self._storage_service = storage_service or GcpManager._get_instance()._storage_service
        self._job_manager = job_manager
        if not self._job_manager:
            from v2.modules.job_framework.manager.job.job_manager import JobManager
            self._job_manager = JobManager()

    async def run_pipeline(
        self,
        user_id: str,
        session_id: str,
        job_id: str,
        firestore_job_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Run full depseudonymization pipeline.

        Args:
            user_id: User ID (string)
            session_id: Session ID (string)
            job_id: Job ID string used in GCS paths (e.g. "JOB_123")
            firestore_job_id: Optional Firestore job document ID for updating report_url

        Returns:
            Summary dict with status, report_url, error, etc.
        """
        result: Dict[str, Any] = {
            "status": "failed",
            "report_url": None,
            "error": None,
            "details": {},
        }
        constants = init_env()
        bucket_name = constants.get("storage_bucket")
        if not bucket_name:
            result["error"] = "storage_bucket not configured"
            return result

        base_prefix = f"{user_id}/{session_id}"
        input_prefix = f"{base_prefix}/input_data"
        output_prefix = f"{base_prefix}/output_data/{job_id}"

        project_root = Path(__file__).resolve().parents[4]
        local_base = project_root / "pseudonymize" / job_id
        local_input = local_base / "input_data"
        local_output = local_base
        # Normalize for log streaming: frontend joins with job_id (JOB_ stripped)
        logs_job_id = str(job_id).replace("JOB_", "", 1) if (job_id and str(job_id).startswith("JOB_")) else job_id

        try:
            # 1. Download from GCS: output_data only (report, images, datasets). Domain JSON fetched separately.
            add_log(f"[Pseudonymization] Downloading output from GCS to {local_base}", job_id=logs_job_id)
            await self._download_folder(
                bucket_name=bucket_name,
                gcs_prefix=output_prefix,
                local_dir=local_output,
                exclude_subdirs=["input_data"],
            )
            # Download only domain JSON files (not whole input_data)
            await self._download_domain_json_only(bucket_name, input_prefix, local_input)
            if not local_output.exists():
                result["error"] = f"Download failed: no output at {local_output}"
                return result
            add_log(f"[Pseudonymization] Download complete: {local_base}", job_id=logs_job_id)

            # 2. Load domain from local (downloaded) or GCS for pseudonymized_columns_map
            domain_data = _load_domain_for_pseudonymization(local_input)
            if not domain_data:
                try:
                    for gcs_domain in [f"{input_prefix}/domain_directory.json", f"{input_prefix}/domain_dictionary.json"]:
                        if self._storage_service._file_exists(bucket_name, gcs_domain):
                            domain_bytes = await asyncio.to_thread(
                                self._storage_service._download_bytes,
                                bucket_name,
                                gcs_domain,
                            )
                            if domain_bytes:
                                domain_data = json.loads(domain_bytes.decode("utf-8"))
                                add_log("[Pseudonymization] Loaded domain from GCS", job_id=logs_job_id)
                                break
                except Exception as e:
                    add_log(f"[Pseudonymization] Could not load domain from GCS (non-fatal): {e}", job_id=logs_job_id)
            pseudonymized_columns_map = _build_pseudonymized_columns_map(domain_data)
            if not pseudonymized_columns_map:
                add_log("[Pseudonymization] No pseudonymized columns in domain; using fallback from snapshot headers", job_id=logs_job_id)

            # 3. Run depseudonymization: datasets (regex) -> text (LLM tag + regex replace) -> base64 -> save
            add_log("[Pseudonymization] Running depseudonymization pipeline", job_id=logs_job_id)
            from v2.modules.pseudonymization.services.depseudonymization_pipeline import run_depseudonymization_pipeline

            dep_result = await run_depseudonymization_pipeline(
                job_output_dir=local_output,
                pseudonymized_columns_map=pseudonymized_columns_map,
                job_id=logs_job_id,
            )
            result["details"]["depseudonymization"] = dep_result

            if dep_result.get("status") != "success":
                result["error"] = dep_result.get("error", "Depseudonymization pipeline failed")
                return result

            # 4. Upload depseudonymized report to GCS (overwrite analysis_report.html)
            report_path = local_output / "analysis_report.html"
            if not report_path.exists():
                result["error"] = "analysis_report.html not produced"
                return result

            gcs_report_blob = f"{output_prefix}/analysis_report.html"
            with open(report_path, "rb") as f:
                report_bytes = f.read()

            await asyncio.to_thread(
                self._storage_service._upload_bytes,
                bucket_name,
                report_bytes,
                gcs_report_blob,
                "text/html",
            )
            add_log(f"[Pseudonymization] Uploaded report to gs://{bucket_name}/{gcs_report_blob}", job_id=logs_job_id)

            # 4b. Upload tagged report to GCS (analysis_report_tagged.html)
            tagged_path = local_output / "analysis_report_tagged.html"
            if tagged_path.exists():
                gcs_tagged_blob = f"{output_prefix}/analysis_report_tagged.html"
                with open(tagged_path, "rb") as f:
                    tagged_bytes = f.read()
                await asyncio.to_thread(
                    self._storage_service._upload_bytes,
                    bucket_name,
                    tagged_bytes,
                    gcs_tagged_blob,
                    "text/html",
                )
                add_log(f"[Pseudonymization] Uploaded tagged report to gs://{bucket_name}/{gcs_tagged_blob}", job_id=logs_job_id)

            # 5. Make blob public and get URL (add cache-busting param so clients get fresh depseudonymized report)
            base_url = await self._get_or_make_public_url(bucket_name, gcs_report_blob)
            report_url = f"{base_url}?v={int(time.time())}" if base_url else None
            result["report_url"] = report_url

            # 6. Update job report_url in Firestore (cache-busting ensures /analysis/report/ fetches latest)
            if report_url and firestore_job_id:
                await self._update_job_report_url(firestore_job_id, report_url)

            result["status"] = "success"
            add_log(f"[Pseudonymization] Pipeline completed. Report URL: {report_url}", job_id=logs_job_id)

        except Exception as e:
            import traceback
            add_log(f"[Pseudonymization] Pipeline failed: {e} | {traceback.format_exc()}", job_id=logs_job_id)
            result["error"] = str(e)
        finally:
            # Cleanup local folder after upload
            if local_base.exists():
                try:
                    shutil.rmtree(local_base)
                    add_log(f"[Pseudonymization] Cleaned up local folder: {local_base}", job_id=logs_job_id)
                except Exception as cleanup_err:
                    add_log(f"[Pseudonymization] Cleanup warning: {cleanup_err}", job_id=logs_job_id)

        return result

    async def _download_folder(
        self,
        bucket_name: str,
        gcs_prefix: str,
        local_dir: Path,
        exclude_subdirs: Optional[List[str]] = None,
    ) -> None:
        """Download all blobs under gcs_prefix to local_dir. Exclude paths containing exclude_subdirs."""
        blobs = self._storage_service._list_files(bucket_name, prefix=gcs_prefix)
        if not blobs:
            add_log(f"[Pseudonymization] No files found at {gcs_prefix}")
            return

        local_dir.mkdir(parents=True, exist_ok=True)
        prefix = gcs_prefix.rstrip("/") + "/" if gcs_prefix else ""
        exclude = exclude_subdirs or []

        for blob_name in blobs:
            if blob_name.endswith("/"):
                continue
            rel_path = blob_name[len(prefix):] if blob_name.startswith(prefix) else blob_name.split("/")[-1]
            if any(ex in rel_path for ex in exclude):
                continue
            local_path = local_dir / rel_path
            local_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                data = await asyncio.to_thread(
                    self._storage_service._download_bytes,
                    bucket_name,
                    blob_name,
                )
                with open(local_path, "wb") as f:
                    f.write(data)
                add_log(f"[Pseudonymization] Downloaded {blob_name} -> {local_path}")
            except Exception as e:
                add_log(f"[Pseudonymization] Failed to download {blob_name}: {e}")
                raise

    async def _download_domain_json_only(
        self, bucket_name: str, input_prefix: str, local_input: Path
    ) -> None:
        """Download only domain_directory.json and domain_dictionary.json from input_data."""
        for filename in ["domain_directory.json", "domain_dictionary.json"]:
            gcs_blob = f"{input_prefix.rstrip('/')}/{filename}"
            if not self._storage_service._file_exists(bucket_name, gcs_blob):
                continue
            try:
                data = await asyncio.to_thread(
                    self._storage_service._download_bytes,
                    bucket_name,
                    gcs_blob,
                )
                if data:
                    local_input.mkdir(parents=True, exist_ok=True)
                    (local_input / filename).write_bytes(data)
            except Exception as e:
                add_log(f"[Pseudonymization] Failed to download {filename}: {e}")

    async def _get_or_make_public_url(self, bucket_name: str, blob_name: str) -> Optional[str]:
        """Get public URL for blob, making it public if needed."""
        try:
            bucket = self._storage_service._client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            try:
                await asyncio.to_thread(blob.make_public)
            except Exception:
                pass
            return blob.public_url
        except Exception as e:
            add_log(f"[Pseudonymization] Could not get public URL: {e}")
            return None

    async def _update_job_report_url(self, job_id: str, report_url: str) -> None:
        """Update job's report_url in Firestore."""
        try:
            from v2.modules.job_framework.models.job_model import JobUpdate

            current = await self._job_manager._get_job_by_id(job_id)
            if not current:
                add_log(f"[Pseudonymization] Job {job_id} not found, skipping report_url update")
                return
            updated_config = dict(current.job_config or {})
            updated_config["report_url"] = report_url
            await self._job_manager._update_job(job_id, JobUpdate(job_config=updated_config))
            add_log(f"[Pseudonymization] Updated report_url for job {job_id}")
        except Exception as e:
            add_log(f"[Pseudonymization] Failed to update job report_url: {e}")

    async def depseudonymize_simple_qna_html_and_save(
        self,
        user_id: str,
        session_id: str,
        query_id: str,
        html_content: str,
    ) -> str:
        """
        Simple QnA depseudonymization: read html -> LLM tag -> regex replace -> return and save to bucket.

        Flow:
        1. Load domain from GCS to get pseudonymized_columns_map
        2. If pseudonymized columns exist: depseudonymize (LLM tag + regex replace)
        3. Save HTML to GCS at {user_id}/{session_id}/output_data/{query_id}/analysis_report.html
        4. Return HTML (depseudonymized or original)
        """
        if not html_content:
            return html_content

        constants = init_env()
        bucket_name = constants.get("storage_bucket")
        if not bucket_name:
            add_log("[SimpleQnA Depseudo] No storage_bucket configured, skipping save")
            return html_content

        # Load domain from GCS
        domain_data = await self._load_domain_from_gcs(bucket_name, user_id, session_id)
        pseudonymized_columns_map = _build_pseudonymized_columns_map(domain_data)

        if pseudonymized_columns_map:
            # Depseudonymize: read html -> LLM tag -> regex replace
            from v2.modules.pseudonymization.services.depseudonymization_pipeline import depseudonymize_simple_qna_html

            async def _save_tagged_to_gcs(tagged_html: str) -> None:
                blob = f"{user_id}/{session_id}/output_data/{query_id}/analysis_report_tagged.html"
                await asyncio.to_thread(
                    self._storage_service._upload_bytes,
                    bucket_name,
                    tagged_html.encode("utf-8"),
                    blob,
                    "text/html",
                )
                add_log(f"[SimpleQnA Depseudo] Saved tagged report to gs://{bucket_name}/{blob}")

            html_content = await depseudonymize_simple_qna_html(
                html_content,
                pseudonymized_columns_map,
                save_tagged_callback=_save_tagged_to_gcs,
            )
            add_log("[SimpleQnA Depseudo] Applied text depseudonymization")
        else:
            add_log("[SimpleQnA Depseudo] No pseudonymized columns in domain, using original HTML")

        # Save to GCS (depseudonymized or original)
        gcs_blob = f"{user_id}/{session_id}/output_data/{query_id}/analysis_report.html"
        try:
            await asyncio.to_thread(
                self._storage_service._upload_bytes,
                bucket_name,
                html_content.encode("utf-8"),
                gcs_blob,
                "text/html",
            )
            add_log(f"[SimpleQnA Depseudo] Saved HTML to gs://{bucket_name}/{gcs_blob}")
        except Exception as e:
            add_log(f"[SimpleQnA Depseudo] Failed to save to GCS: {e}")

        return html_content

    async def pseudonymize_query_for_execution(
        self,
        user_id: str,
        session_id: str,
        query: str,
    ) -> str:
        """
        If session has pseudonymized=true: get query -> LLM tag -> pseudonymize tagged values -> return.
        Otherwise return query as-is.
        """
        if not query:
            return query
        if not await self._is_session_pseudonymized(session_id):
            return query
        constants = init_env()
        bucket_name = constants.get("storage_bucket")
        if not bucket_name:
            return query
        domain_data = await self._load_domain_from_gcs(bucket_name, user_id, session_id)
        pseudonymized_columns_map = _build_pseudonymized_columns_map(domain_data)
        if not pseudonymized_columns_map:
            return query
        from v2.modules.pseudonymization.services.depseudonymization_pipeline import pseudonymize_query_for_execution as _pseudo_query_pipeline
        result = await _pseudo_query_pipeline(query, pseudonymized_columns_map)
        if result != query:
            add_log("[Query Pseudonymization] Applied pseudonymization before execution")
        return result

    async def _is_session_pseudonymized(self, session_id: str) -> bool:
        """Check if session has pseudonymized=true in Firestore."""
        try:
            from v2.common.gcp import GcpManager
            gcp = GcpManager._get_instance()
            doc = await gcp._firestore_service._get_document("sessions", session_id)
            return bool(doc.get("pseudonymized", False))
        except Exception as e:
            add_log(f"[Pseudonymization] Could not check session pseudonymized: {e}")
            return False

    async def _load_domain_from_gcs(self, bucket_name: str, user_id: str, session_id: str) -> dict:
        """Load domain_directory.json from GCS for session."""
        gcs_blob = f"{user_id}/{session_id}/input_data/domain_directory.json"
        if not self._storage_service._file_exists(bucket_name, gcs_blob):
            return {}
        try:
            data = await asyncio.to_thread(
                self._storage_service._download_bytes,
                bucket_name,
                gcs_blob,
            )
            return json.loads(data.decode("utf-8")) if data else {}
        except Exception as e:
            add_log(f"[SimpleQnA Depseudo] Error loading domain_directory.json: {e}")
        return {}
