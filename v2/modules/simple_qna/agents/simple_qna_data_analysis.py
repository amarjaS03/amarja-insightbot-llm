"""
Simple QnA Data Analysis Agent for v2

Migrated from execution_layer/agents/simpleqna_data_analysis.py
Adapted for v2 module structure - pure execution logic, no Firestore awareness.
"""
import os
import json
import logging
import asyncio
from pathlib import Path
from typing import Dict, Any, List
from pydantic import BaseModel
from dotenv import load_dotenv
from langchain_core.runnables import Runnable
from execution_layer.utils.llm_core import CALLER_CONFIG, call_llm

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CancelledException(Exception):
    """Raised when processing is cancelled by user"""
    pass


def check_cancellation(state: dict, checkpoint_name: str = "") -> bool:
    """Check if the current session/job has been cancelled.
    
    Args:
        state: The pipeline state containing session_id and cancellation_manager
        checkpoint_name: Optional name for logging which checkpoint detected cancellation
        
    Returns:
        True if cancelled, False otherwise
        
    Raises:
        CancelledException if cancelled (for cleaner control flow)
    """
    cancellation_manager = state.get("cancellation_manager")
    if not cancellation_manager:
        return False
    
    session_id = state.get("session_id")
    job_id = state.get("job_id")
    
    if cancellation_manager.is_cancelled(session_id=session_id, job_id=job_id):
        checkpoint_info = f" at checkpoint: {checkpoint_name}" if checkpoint_name else ""
        logger.info(f"🛑 [CANCELLATION] Processing cancelled for session={session_id}, job={job_id}{checkpoint_info}")
        state["cancelled"] = True
        raise CancelledException(f"Processing cancelled{checkpoint_info}")
    
    return False


class SubQuery(BaseModel):
    id: int
    sub_query: str
    code_instructions: str
    expected_output: str

class Plan(BaseModel):
    plan: List[SubQuery]

MODULE_DIR = Path(__file__).parent.resolve()
EXEC_BASE_DIR = MODULE_DIR.parent.parent.parent.parent.resolve() / "execution_layer"
domain_directory_path = EXEC_BASE_DIR / 'input_data' / 'domain_directory.json'
domain_directory = {}
try:
    if domain_directory_path.exists():
        with open(domain_directory_path, 'r', encoding='utf-8') as f:
            domain_directory = json.load(f)
    else:
        logger.warning(f"Domain directory not found at {domain_directory_path}")
except Exception as e:
    logger.warning(f"Error loading domain directory from {domain_directory_path}: {e}")
    
def _build_query_analysis_prompt(input_dir: str) -> str:
    """Build the query analysis prompt per request using domain_directory.json."""
    dd = {}
    try:
        path = Path(input_dir) / "domain_directory.json"
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                dd = json.load(f)
                logger.info(f"Domain directory loaded for query analysis from {path}: {len(dd)} entries")
    except Exception as e:
        logger.error(f"CRITICAL: Failed to load domain directory from {input_dir} for query analysis: {e}")
    return f"""You are an expert who understands user query and devises a plan to solve it.
You have domain knowledge: {dd}

Step-by-step process:
Step 1: Analyze the user query with given domain knowledge to determine the intent(What user wants)
Step 2: Use the intent and break down the complex query into small solvable sub queries(which can be solved by simple operations)
Step 3: Determine the exact step by step plan to solve the sub queries in right order(steps to be taken and operations to be performed)
Step 4: Determine the appropriate output for each sub query (like tables) and specify the final tabular result should be named result_df when applicable.

Response format (JSON):
{{
    "plan": [
        {{
            "id": "1",
            "sub_query": "sub query 1",
            "code_instructions": "code instructions for sub query 1",
            "expected_output": "expected output for sub query 1"
        }},
        {{
            "id": "2",
            "sub_query": "sub query 2",
            "code_instructions": "code instructions for sub query 2",
            "expected_output": "expected output for sub query 2"
        }},
        ...
    ]
}}
**Important**:
- Include required dimensions and facts correctly so it will be easy to form an answer based on the output.
- In plan code_instructions, include specific instructions to create a final DataFrame named result_df when the output is tabular, and follow small/large output rules: if result_df.shape[0] <= 20 print fully; if > 20 save result_df to CSV in output_data and print a marker line exactly as __SAVED_CSV__=<csv_path> and also print a 10-row sample.
- In plan expected_output, include the expected columns of result_df.
- Do not include any additional text or explanations outside the JSON object.
"""

QUERY_ANALYSIS_PROMPT = f"""You are an expert who understands user query and devises a plan to solve it.
You have domain knowledge: {domain_directory}

Step-by-step process:
Step 1: Analyze the user query with given domain knowledge to determine the intent(What user wants)
Step 2: Use the intent and break down the complex query into small solvable sub queries(which can be solved by simple operations)
Step 3: Determine the exact step by step plan to solve the sub queries in right order(steps to be taken and operations to be performed)
Step 4: Determine the appropriate output for each sub query (like tables) and specify the final tabular result should be named result_df when applicable.

Response format (JSON):
{{
    "plan": [
        {{
            "id": "1",
            "sub_query": "sub query 1",
            "code_instructions": "code instructions for sub query 1",
            "expected_output": "expected output for sub query 1"
        }},
        {{
            "id": "2",
            "sub_query": "sub query 2",
            "code_instructions": "code instructions for sub query 2",
            "expected_output": "expected output for sub query 2"
        }},
        ...
    ]
}}
**Important**:
- Include required dimensions and facts correctly so it will be easy to form an answer based on the output.
- In plan code_instructions, include specific instructions to create a final DataFrame named result_df when the output is tabular, and follow small/large output rules: if result_df.shape[0] <= 20 print fully; if > 20 save result_df to CSV in output_data and print a marker line exactly as __SAVED_CSV__=<csv_path> and also print a 10-row sample.
- In plan expected_output, include the expected columns of result_df.
- Do not include any additional text or explanations outside the JSON object.
"""


class SimpleQnaDataAnalysisAgent(Runnable):
    """
    Simple QnA Data Analysis Agent.
    
    Orchestrates query analysis, code execution, and answer generation.
    Returns structured result for storage in Firestore.
    """
    
    def __init__(self):
        self.output_dir = EXEC_BASE_DIR / "output_data"

    async def analyze_user_query(self, user_query: str, state: dict) -> Dict[str, Any]:
        """Analyze user query to determine analysis approach"""
        try:
            logger.info(f"Processing user query: {user_query}")
            prompt = _build_query_analysis_prompt(state.get("input_dir", "/app/execution_layer/input_data"))
            content_text = await call_llm(
                SYS_PROMPT=prompt,
                USER_ANALYSIS_PROMPT="User query",
                USER_PROMPT=user_query,
                json_output=True,
                api_type="chat",
                temperature=CALLER_CONFIG["V2_SIMPLEQNA_QUERY"]["temperature"],
                max_tokens=CALLER_CONFIG["V2_SIMPLEQNA_QUERY"]["max_tokens"],
                state=state,
            )
            parsed = json.loads(content_text)
            return parsed
        except Exception as e:
            logger.error(f"Error analyzing user query: {e}")
            return {}

    async def execute_task(self, task: Dict, state: dict) -> Dict:
        """Execute a single EDA task using the code agent"""
        from v2.modules.simple_qna.agents.simple_qna_executor import SimpleQnaCodeAgent
        
        code_agent = SimpleQnaCodeAgent()
        task_state = state.copy()
        task_state["command"] = task["code_instructions"]
        task_state["current_query"] = task["sub_query"]
        task_state["current_expected_output"] = task["expected_output"]
        
        # Execute the task
        result_state = await code_agent.ainvoke(task_state)

        # Update main state
        state["history"] = result_state["history"]
        state["last_code"] = result_state["last_code"]
        state["last_output"] = result_state["last_output"]
        state["last_error"] = result_state["last_error"]
        
        return {
            "task": task["sub_query"],
            "output": result_state.get("last_output", ""),
            "error": result_state.get("last_error"),
            "success": not result_state.get("last_error")
        }
    
    async def judge_answer(self, state: dict) -> None:
        """Build final HTML answer via LLM and populate state with file info and blob."""
        last_output = (state.get("last_output") or "").strip()
        saved_csv_path = ""
        for line in last_output.splitlines():
            if line.startswith("__SAVED_CSV__="):
                saved_csv_path = line.split("=", 1)[1].strip()
                break

        html_output = ""
        download_blob = None

        if saved_csv_path and os.path.exists(saved_csv_path):
            try:
                # Remember file info in state for ease of use by UI
                state["csv_file_path"] = saved_csv_path
                state["csv_file_name"] = os.path.basename(saved_csv_path)

                # Build sample rows (first ~10)
                import csv
                preview_rows = []
                resolved_csv_path = saved_csv_path
                if not os.path.isabs(resolved_csv_path):
                    resolved_csv_path = str((EXEC_BASE_DIR / saved_csv_path).resolve())
                with open(resolved_csv_path, "r", encoding="utf-8", newline="") as fp:
                    reader = csv.reader(fp)
                    for idx, row in enumerate(reader):
                        preview_rows.append(row)
                        if idx >= 10:
                            break
                headers = preview_rows[0] if preview_rows else []
                rows = preview_rows[1:] if len(preview_rows) > 1 else []

                # Normalize identifier-like columns so IDs never contain commas in previews
                try:
                    rows = self._normalize_identifier_columns(headers, rows)
                except Exception:
                    # Fail silently; normalization is a best-effort enhancement
                    pass

                # Detect missing values for data-quality note
                try:
                    has_missing = self._has_missing_in_rows(rows)
                except Exception:
                    has_missing = False

                # Create blob (base64) for robust client-side download and register placeholder var
                download_blob = self._csv_to_blob(resolved_csv_path)
                download_base64 = download_blob.get("data_base64", "") if download_blob else ""
                placeholder = f"${{os.path.basename(saved_csv_path)}}"
                try:
                    blob_vars: Dict[str, Any] = state.get("blob_vars", {}) or {}
                    if download_base64:
                        blob_vars[os.path.basename(saved_csv_path)] = download_base64
                        state["blob_vars"] = blob_vars
                except Exception:
                    pass

                # Read answer rules
                rules_text = ""
                try:
                    answer_rules_path = MODULE_DIR / "answer_rules.md"
                    if not answer_rules_path.exists():
                        # Fallback to execution_layer path
                        answer_rules_path = EXEC_BASE_DIR / "agents" / "answer_rules.md"
                    if answer_rules_path.exists():
                        with open(answer_rules_path, "r", encoding="utf-8") as rf:
                            rules_text = rf.read()
                except Exception:
                    rules_text = ""

                # Let LLM build HTML content fragment with scoped CSS (NOT a full page)
                summary_system = (
                    "You are a senior analyst. Generate an HTML CONTENT FRAGMENT (NOT a full page - NO <!DOCTYPE>, <html>, <head>, or <body> tags). "
                    "The HTML will be embedded within a parent page, so it must NOT override parent styles or layout. "
                    "CRITICAL CSS RULES: "
                    "1. All CSS must be inside a <style> tag with scoped classes prefixed with 'qna-response-' (e.g., 'qna-response-container', 'qna-response-table'). "
                    "2. Use ONLY CSS classes (NO inline styles except where absolutely necessary). "
                    "3. Do NOT use global selectors like 'body', 'html', 'div', 'table' without the 'qna-response-' prefix. "
                    "4. Keep styles scoped to avoid conflicts with parent page CSS. "
                    "5. Use a wrapper div with class 'qna-response-container' for all content. "
                    "TABLE LAYOUT RULES: "
                    "- Render tables in standard horizontal layout with readable columns. "
                    "- Do NOT rotate text or make it vertical (no 'writing-mode', 'transform: rotate', or similar). "
                    "- Do NOT use extreme word breaking such as 'word-break: break-all' or column widths that force one character per line. "
                    "- Prefer letting the table scroll horizontally inside a wrapper div with class 'qna-response-table-wrapper' using 'overflow-x: auto'. "
                    "- Use normal table semantics with <table>, <thead>, <tbody>, <tr>, <th>, and <td>; set 'table-layout: auto' and let columns size naturally. "
                    "FORMATTING RULES: "
                    "- IDENTIFIERS (columns with 'id', 'code', 'number', 'no', 'account' in name): NEVER add thousand separators, currency symbols, or any formatting. Keep as raw integers/strings (e.g., 21226 NOT 21,226). "
                    "- Currency: INR (₹) with Indian digit grouping and 0–2 decimals. "
                    "- Percentages: append %, scale values in [0,1] by 100, round to 1–2 decimals. "
                    "- Counts: integers with grouping, no unit. "
                    "- Dates: use YYYY-MM-DD. "
                    "Follow the Answer Rules strictly. Show a concise summary and ONE sample table. "
                    "Do NOT add any 'Download CSV' buttons or links; CSV download controls will be provided by the hosting application."
                )
                summary_user = json.dumps({
                    "answer_rules": rules_text,
                    "original_query": state.get("original_query", ""),
                    "result_type": "csv_large",
                    "file_name": os.path.basename(saved_csv_path),
                    "download_placeholder": placeholder,
                    "sample_table": {"headers": headers, "rows": rows}
                })
                html_output = await call_llm(
                    SYS_PROMPT=summary_system,
                    USER_ANALYSIS_PROMPT="Summary request",
                    USER_PROMPT=summary_user,
                    api_type="chat",
                    temperature=CALLER_CONFIG["V2_SIMPLEQNA_JUDGE_CSV"]["temperature"],
                    max_tokens=CALLER_CONFIG["V2_SIMPLEQNA_JUDGE_CSV"]["max_tokens"],
                    state=state,
                )
                html_output = (html_output or "").strip()
                try:
                    for var_name, b64 in (state.get("blob_vars") or {}).items():
                        token = f"${{{var_name}}}"
                        html_output = html_output.replace(token, f"data:text/csv;base64,{b64}")
                except Exception:
                    pass
                # Fallback: if the model did not insert the placeholder/data URL, inject a standard download link
                try:
                    if download_base64 and "data:text/csv;base64," not in html_output:
                        safe_file = os.path.basename(saved_csv_path)
                        fallback = (
                            f'<div class="qna-response-container">'
                            f'<a class="qna-response-download" '
                            f'download="{safe_file}" '
                            f'href="data:text/csv;base64,{download_base64}">Download CSV</a>'
                            f'</div>'
                        )
                        html_output = (html_output or "") + "\n" + fallback
                except Exception:
                    pass
                # Sanitize HTML to ensure it's a fragment, not a full page
                html_output = self._sanitize_html_fragment(html_output)
                # Inject a deterministic note when missing values are present
                try:
                    html_output = self._inject_missing_value_note(html_output, has_missing)
                except Exception:
                    pass
            except Exception as e:
                html_output = f"<p>Saved file detected but preview failed: {str(e)}</p>"
        else:
            # Small answer path
            cleaned_lines = []
            for ln in (last_output or "").splitlines():
                if cleaned_lines and ln.strip() == cleaned_lines[-1].strip():
                    continue
                cleaned_lines.append(ln)
            safe = "\n".join(cleaned_lines) if cleaned_lines else "No output produced."

            # Detect missing values in plain-text output
            try:
                has_missing_small = self._has_missing_in_text(safe)
            except Exception:
                has_missing_small = False
            rules_text = ""
            try:
                answer_rules_path = MODULE_DIR / "answer_rules.md"
                if not answer_rules_path.exists():
                    # Fallback to execution_layer path
                    answer_rules_path = EXEC_BASE_DIR / "agents" / "answer_rules.md"
                if answer_rules_path.exists():
                    with open(answer_rules_path, "r", encoding="utf-8") as rf:
                        rules_text = rf.read()
            except Exception:
                rules_text = ""

            summary_system = (
                "You are a senior analyst. Generate an HTML CONTENT FRAGMENT (NOT a full page - NO <!DOCTYPE>, <html>, <head>, or <body> tags). "
                "The HTML will be embedded within a parent page, so it must NOT override parent styles or layout. "
                "CRITICAL CSS RULES: "
                "1. All CSS must be inside a <style> tag with scoped classes prefixed with 'qna-response-' (e.g., 'qna-response-container', 'qna-response-table'). "
                "2. Use ONLY CSS classes (NO inline styles except where absolutely necessary). "
                "3. Do NOT use global selectors like 'body', 'html', 'div', 'table' without the 'qna-response-' prefix. "
                "4. Keep styles scoped to avoid conflicts with parent page CSS. "
                "5. Use a wrapper div with class 'qna-response-container' for all content. "
                "FORMATTING RULES: "
                "IDENTIFIERS (columns with 'id', 'code', 'number', 'no', 'account' in name): NEVER add thousand separators or any formatting - keep as raw integers/strings (e.g., 21226 NOT 21,226). "
                "Currency: INR (₹) with Indian digit grouping. Percentages: append % with proper scaling. Counts: integers with grouping. Dates: YYYY-MM-DD. "
                "Follow the Answer Rules strictly. Prefer a single clean table or readable text."
            )
            summary_user = json.dumps({
                "answer_rules": rules_text,
                "original_query": state.get("original_query", ""),
                "result_type": "small_text",
                "printed_text": safe
            })
            html_output = await call_llm(
                SYS_PROMPT=summary_system,
                USER_ANALYSIS_PROMPT="Summary request",
                USER_PROMPT=summary_user,
                api_type="chat",
                temperature=CALLER_CONFIG["V2_SIMPLEQNA_JUDGE_TEXT"]["temperature"],
                max_tokens=CALLER_CONFIG["V2_SIMPLEQNA_JUDGE_TEXT"]["max_tokens"],
                state=state,
            )
            html_output = (html_output or "").strip()
            # Sanitize HTML to ensure it's a fragment, not a full page
            html_output = self._sanitize_html_fragment(html_output)
            # Inject a deterministic note when missing values are present
            try:
                html_output = self._inject_missing_value_note(html_output, has_missing_small)
            except Exception:
                pass

        # Persist - use job-specific output directory from state
        try:
            # Use output_dir from state if available (job-specific), otherwise fall back to self.output_dir
            out_dir = Path(state.get("output_dir") or self.output_dir)
            out_dir.mkdir(parents=True, exist_ok=True)
            # Save as analysis_report.html to match standard endpoint expectations
            output_path = out_dir / "analysis_report.html"
            with open(output_path, "w", encoding="utf-8") as fp:
                fp.write(html_output)
            state["final_output_html"] = html_output
            state["final_html_report"] = html_output  # Also set this for consistency
            state["html_file_path"] = str(output_path)
            if download_blob:
                state["download"] = download_blob
            logger.info(f"Saved HTML report to: {output_path}")
        except Exception as e:
            logger.warning(f"Could not save HTML summary: {e}")

    def _has_missing_in_rows(self, rows: List[List[str]]) -> bool:
        """Detect if any cell in preview rows looks like a missing value."""
        for row in rows or []:
            for cell in row or []:
                s = "" if cell is None else str(cell).strip()
                if not s:
                    return True
                if s.lower() in {"nan", "none", "null", "n/a"}:
                    return True
        return False

    def _has_missing_in_text(self, text: str) -> bool:
        """Detect missing-value markers in a plain-text table representation."""
        if not text:
            return False
        lowered = text.lower()
        for token in [" nan", "\tnan", " na ", " n/a", " none", " null"]:
            if token in lowered:
                return True
        return False

    def _inject_missing_value_note(self, html_content: str, has_missing: bool) -> str:
        """Append a short, user-friendly note when missing values are present."""
        if not has_missing:
            return html_content

        note_html = (
            '<div class="qna-response-note">'
            'Note: Cells shown as <strong>N/A</strong> or blank indicate that the underlying source data '
            'did not contain those details for the corresponding record (for example, missing customer details), '
            'while the numeric values such as outstanding amounts remain accurate based on the available data.'
            "</div>"
        )

        # Try to insert just before the final closing div (wrapper) if present
        closing_div = "</div>"
        idx = html_content.rfind(closing_div)
        if idx == -1:
            return html_content + note_html
        return html_content[:idx] + note_html + html_content[idx:]

    def _normalize_identifier_columns(self, headers: List[str], rows: List[List[str]]) -> List[List[str]]:
        """Remove comma formatting from identifier-like columns in preview rows."""
        if not headers or not rows:
            return rows

        id_indices = []
        for i, h in enumerate(headers):
            name = (h or "").strip().lower()
            if not name:
                continue
            if (
                "id" in name
                or name.endswith(" id")
                or "code" in name
                or name.endswith(" no")
                or "number" in name
            ):
                id_indices.append(i)

        if not id_indices:
            return rows

        normalized_rows: List[List[str]] = []
        for row in rows:
            new_row = list(row)
            for idx in id_indices:
                if idx < len(new_row) and new_row[idx] is not None:
                    s = str(new_row[idx])
                    new_row[idx] = s.replace(",", "")
            normalized_rows.append(new_row)

        return normalized_rows

    def _sanitize_html_fragment(self, html_content: str) -> str:
        """Remove full page structure and ensure HTML is a fragment suitable for embedding"""
        import re
        
        # Remove DOCTYPE if present
        html_content = re.sub(r'<!DOCTYPE[^>]*>', '', html_content, flags=re.IGNORECASE)
        
        # Extract content from <body> tags if present, or remove <html> and <head> tags
        body_match = re.search(r'<body[^>]*>(.*?)</body>', html_content, re.DOTALL | re.IGNORECASE)
        if body_match:
            html_content = body_match.group(1)
        else:
            # Remove <html> tags if present
            html_content = re.sub(r'<html[^>]*>', '', html_content, flags=re.IGNORECASE)
            html_content = re.sub(r'</html>', '', html_content, flags=re.IGNORECASE)
            # Remove <head> tags and their content
            html_content = re.sub(r'<head[^>]*>.*?</head>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
        
        # Ensure wrapper div exists with qna-response-container class
        if not re.search(r'class=["\']qna-response-container', html_content, re.IGNORECASE):
            html_content = f'<div class="qna-response-container">{html_content}</div>'
        
        return html_content.strip()

    def _csv_to_blob(self, csv_path: str) -> Dict[str, Any]:
        try:
            import base64
            with open(csv_path, "rb") as fp:
                b64 = base64.b64encode(fp.read()).decode("utf-8")
            return {
                "file_name": os.path.basename(csv_path),
                "mime_type": "text/csv",
                "data_base64": b64
            }
        except Exception:
            return {}
    
    async def ainvoke(self, state: dict, config=None, **kwargs) -> dict:
        """
        Main data analysis agent logic - orchestrates the entire pipeline.
        
        Returns structured result:
        {
            "answer": "<html>",
            "metrics": {...},
            "error": null
        }
        """
        
        try:
            original_query = state["original_query"]
            
            # Cancellation checkpoint: Before starting
            check_cancellation(state, "before_query_analysis")
            
            # Step 1: Analyze user query
            logger.info("Analyzing user query...")
            query_analysis = await self.analyze_user_query(original_query, state)
            plan = query_analysis.get("plan", [])
            logger.info("Query analysis complete")
            
            # Cancellation checkpoint: After query analysis
            check_cancellation(state, "after_query_analysis")

            # step 2: Execute each sub-query in the plan
            for i, task in enumerate(plan):
                # Cancellation checkpoint: Before each task
                check_cancellation(state, f"before_task_{i+1}")
                
                task_result = await self.execute_task(task, state)
                state.setdefault("results", []).append(task_result)      
                logger.info(f"Task result: {state['results']}")
                if task_result["error"]:
                    logger.warning(f"Task failed: {task_result['error']}")
                
                # Cancellation checkpoint: After each task
                check_cancellation(state, f"after_task_{i+1}")
            
            # Cancellation checkpoint: Before judging answer
            check_cancellation(state, "before_judge_answer")
            
            # step 3: Judge the answer
            await self.judge_answer(state)
            
            # Return structured result (no Firestore awareness)
            return {
                "answer": state.get("final_output_html", ""),
                "metrics": state.get("metrics", {}),
                "error": state.get("error"),
                "cancelled": state.get("cancelled", False)
            }
            
        except CancelledException:
            # User cancelled - this is not an error, just exit gracefully
            logger.info("🛑 Processing cancelled by user - exiting gracefully")
            return {
                "answer": None,
                "metrics": state.get("metrics", {}),
                "error": None,
                "cancelled": True
            }
            
        except Exception as e:
            logger.error(f"Data Analysis Agent error: {e}")
            return {
                "answer": None,
                "metrics": state.get("metrics", {}),
                "error": f"Data Analysis Agent failed: {str(e)}",
                "cancelled": False
            }

    def invoke(self, state: dict, config=None, **kwargs) -> dict:
        """Synchronous wrapper"""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                return asyncio.create_task(self.ainvoke(state, config, **kwargs))
            else:
                return asyncio.run(self.ainvoke(state, config, **kwargs))
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(self.ainvoke(state, config, **kwargs))
