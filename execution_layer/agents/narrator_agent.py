import os
import json
import logging
import asyncio
import re
import base64
import mimetypes
from typing import List, Dict, Any
from pathlib import Path
import datetime as dt

from dotenv import load_dotenv
from agents.llm_client import llm_call
from langchain_core.runnables import Runnable

from bs4 import BeautifulSoup

from agents.executor import CodeAgent
from agents.token_manager import check_token_limit_internal, complete_job_gracefully, TokenLimitExceededException
from agents.perf_utils import log_resources

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Updated Vision Analysis Prompt for better business context
VISION_ANALYSIS_PROMPT = """You are a senior management consultant responsible for analyzing data visualizations for executive presentations.

Your responsibilities:
1. **Chart Selection**: Determine which visualizations provide the most strategic business value
2. **Business Interpretation**: Explain what each chart reveals about the business in simple terms
3. **Executive Annotation**: Create clear annotations that highlight key insights for decision-makers
4. **Strategic Context**: Connect each visualization to the broader business question

For each visualization, provide:
- **Selection Decision**: Include/Exclude with business justification
- **Executive Summary**: 1-2 sentences explaining the key insight for busy executives
- **Business Implications**: How this insight impacts business decisions
- **Technical Details**: Brief explanation of the data represented (for appendix)
- **Recommendations**: Any additional visualizations that would strengthen the business case

Focus on creating a compelling visual narrative that drives business decisions."""

# Final Synthesis Prompt for McKinsey-style report
FINAL_SYNTHESIS_PROMPT = """You are creating a McKinsey-style executive report that transforms data analysis into strategic business insights.

    IMPORTANT: You must provide detailed business-focused thinking logs throughout your analysis process so users can see your business reasoning and strategic decision-making process. Think like a business consultant, not a technical analyst.

    CRITICAL RELIABILITY / NO-HALLUCINATION RULES:
    - Use ONLY the information explicitly provided in the input JSON (frame_text, file_analyses, hypothesis_summary/findings, eda_summary, and any file paths listed there).
    - NEVER invent or make up:
    - entity names (e.g., account/customer/product names),
    - table rows,
    - numbers/metrics,
    - filenames/paths,
    - insights that are not directly supported by the provided data.
    - If a value is not present in file_analyses (e.g., CSV preview_rows) and cannot be derived from the provided summaries, DO NOT include it.
    Instead, omit that detail or state it is unavailable from the current artifacts.
    - When creating HTML tables with specific values, ONLY use values that appear in the provided CSV preview_rows (or other explicit artifacts).
    - Do not “guess” missing values to make tables look complete. Partial tables are acceptable; fabricated tables are not.

    Create a comprehensive HTML report with these characteristics:

    **McKinsey-Style Elements:**
    1. **Clean, minimalist design** with ample white space
    2. **Blue accent color** (#003DA5) for headers and key elements
    3. **Executive summary** at the beginning with key takeaways in bullet points
    4. **Clear section headers** with numbering (1.0, 1.1, etc.)
    5. **Highlighted key insights** in callout boxes
    6. **Data tables** with minimal gridlines and clear formatting
    7. **Annotated visualizations** with clear titles and insights
    8. **Action-oriented recommendations** in the conclusion

    **Structure:**
    1. **Executive Summary** - Key findings (1 page)
    2. **Business Context** - Background and question framing
    3. **Analysis Approach** - Methodology overview (brief)
    4. **Key Findings** - Main insights with supporting evidence
    5. **Implications** - Business impact of findings (omit in concise mode)
    6. **Recommendations** - Actionable next steps (omit in concise mode)

    **Verbosity Control (from input JSON):**
    - You will receive `report_verbosity` with value `concise` or `full`.
    - If `report_verbosity` is `concise`: keep report shorter, focus only on key insights/tables, avoid unnecessary detail, and DO NOT include sections titled "Implications" or "Recommendations".
    - If `report_verbosity` is `full`: provide deeper explanations while preserving the same report style and structure.

    **Technical Requirements:**
    - Include specifics while showing findings and recommendations.
    - Professional HTML with McKinsey-style CSS
    - Responsive design with clean layout
    - HTML tables for data summaries
    - Image references: use ONLY the filename (e.g. correlation_heatmap.png) in img src, not full paths. Example: <img src="correlation_heatmap.png" alt="Correlation heatmap: ..." />
    - Provide currency formatting (include currency symbol based on the data e.g. INR:₹, USD:$, etc.)
    - Interactive elements where appropriate(tables should not be scrollable both horizontally and vertically, fit the table to the page layout)
    - First header should be the Question and the Date,use original_query and current_date

    **Styling Instructions:**
    - Do not use any external css files.
    - Use only classes as a css selector for styling.
    - Do not use any css element selectors like h1, p, div, span, etc for styling.
    - Keep the Width of the page max 100% and initial width of the page is 100%.
    - Report styling should be done using classes only.
    - Report styling should not make impact on other element outside report.

    **Important:**
    1. Print layout:
    - The page must print to A4 paper using CSS `@page { size: A4; }`.
    - Use margins equal to 3% of the page on all edges for print. Ensure margins apply when printing.
    2. Screen/responsive layout:
    - The same file must render nicely in a browser and adapt to narrow screens.
    - Use a centered sheet/container that visually matches an A4 sheet but scales down on small screens.
    - Create a responsive HTML table (scrollable on small screens) that can be exported to a PDF using python and PDFKit. The table should render cleanly, fitting within A4 page margins and maintaining layout consistency.
    3. Sizing & spacing:
    - Ensure the container uses `max-width: 100%` for responsiveness.
    - Do not include any shadow in the report styling.
    4. Typography:
    - Main text must be `text-align: justify`.
    - Use readable font sizes that scale on small screens (use rem units or clamp()).
    - Set a single global typography system: apply `font-family: Arial, Helvetica, sans-serif;` on the outer report wrapper class so ALL report text (headers, tables, captions, callouts) inherits it, and use consistent rem/clamp sizing without overriding font-family elsewhere.
    5. Print optimizations:
    - Disable any extraneous page chrome that could break layout.
    - Add `@media print` rules to ensure crisp printing and no overflow.
    - Add page-break rules for multi-page content.
    6. Accessibility & meta:
    - Include `<meta name="viewport" content="width=device-width, initial-scale=1">`.
    7. Output format:
    - Output a complete, valid HTML5 document only (no extra text). 

    Make it comprehensive yet accessible to business executives.

    Response format (JSON):
    {
        "thinking_logs": [
            "💼 Reviewing analysis for strategic business insights...",
            "🏢 Structuring findings for executive decision-making...",
            "📈 Translating data patterns into business opportunities...",
            "🎯 Crafting actionable recommendations for stakeholders...",
            "📊 Designing executive-ready business presentation..."
        ],
        "html_report": "complete HTML code with embedded CSS"
    }

    Do not mention McKinsey in the report.
    Make sure to include 4-6 detailed thinking_logs that show your actual reasoning process.

"""

class NarratorAgent(Runnable):
    def __init__(self, output_dir):
        self.output_dir = output_dir or os.path.join('execution_layer', 'output_data')
        # Ensure narrator output directory exists
        self.narrator_dir = Path(self.output_dir) / "narrator"
        self.narrator_dir.mkdir(parents=True, exist_ok=True)

    def convert_images_to_base64(self, html_content: str) -> str:
        """
        Convert <img src> in HTML to Base64 by extracting paths from src attributes.

        Args:
            html_content (str): HTML content as string.

        Returns:
            str: Updated HTML with Base64 images.
        """
        soup = BeautifulSoup(html_content, "html.parser")
        
        # Get current script directory for relative path resolution
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_dir = self.output_dir
        
        logger.info(f"Converting images to Base64")
        logger.info(f"Script directory: {script_dir}")
        logger.info(f"Output directory: {output_dir}")
        
        img_tags = soup.find_all("img")
        logger.info(f"Found {len(img_tags)} img tags in HTML")
        
        for img_tag in img_tags:
            src = img_tag.get("src")
            if not src:
                continue
                
            logger.info(f"Processing image with src: {src}")
            
            try:
                # Handle relative paths
                if src.startswith("./"):
                    src = src[2:]  # Remove ./
                
                # Construct absolute path
                img_path = src
                logger.info(f"Absolute image path: {img_path}")
                
                # Get mime type
                mime_type, _ = mimetypes.guess_type(img_path)
                if mime_type is None:
                    mime_type = "image/png"
                
                # Read image and convert to Base64
                with open(img_path, "rb") as f:
                    encoded = base64.b64encode(f.read()).decode("utf-8")
                
                # Replace src with Base64 data URI
                base64_src = f"data:{mime_type};base64,{encoded}"
                
                # Create a new img tag with the base64 src
                new_img_tag = soup.new_tag("img")
                new_img_tag["src"] = base64_src
                
                # Preserve all original attributes except src
                for attr_name, attr_value in img_tag.attrs.items():
                    if attr_name != "src":
                        new_img_tag[attr_name] = attr_value
                
                # Replace the old tag with the new one
                img_tag.replace_with(new_img_tag)
                
                logger.info(f"Updated src to base64 (truncated): {base64_src[:100]}...")
                logger.info(f"✅ Successfully converted {src}")
                
            except FileNotFoundError:
                logger.error(f"❌ File not found: {img_path}")
            except Exception as e:
                logger.error(f"❌ Error processing {img_path}: {str(e)}")

        # Convert back to string
        result_html = str(soup)
        logger.info(f"Final HTML length: {len(result_html)} characters")
        
        # Verify base64 data presence
        if "data:image" in result_html:
            logger.info("✅ Base64 data found in final HTML")
        else:
            logger.warning("⚠️ No base64 data found in final HTML")

        return result_html

    async def analyze_image_with_vision(self, image_path: str, context: str, state: dict) -> Dict[str, Any]:
        """Analyze image with enhanced context for graph selection and annotation"""
        try:
            if not os.path.exists(image_path):
                logger.warning(f"Image not found: {image_path}")
                return {
                    "path": image_path,
                    "selection": "exclude",
                    "reasoning": "Image file not found",
                    "executive_summary": "",
                    "business_implications": "",
                    "technical_details": "",
                    "recommendations": ""
                }
            
            logger.info(f"Analyzing image for curation: {image_path}")
            # Read and encode image
            with open(image_path, "rb") as image_file:
                base64_image = base64.b64encode(image_file.read()).decode('utf-8')
            
            vision_msg = f"""
    Analyze this visualization for inclusion in executive report.

    Business Context: {context}
    Image Path: {image_path}

    Please provide:
    1. Selection decision (include/exclude) with business justification
    2. Executive summary (1-2 sentences explaining the key insight)
    3. Business implications (how this insight impacts business decisions)
    4. Technical details (brief explanation of the data represented)
    5. Recommendations (additional visualizations that would strengthen the business case)

    Return as JSON:
    {{
        "selection": "include|exclude",
        "reasoning": "business justification for inclusion/exclusion",
        "executive_summary": "1-2 sentence key insight for executives",
        "business_implications": "how this insight impacts business decisions",
        "technical_details": "brief explanation of the data represented",
        "recommendations": "additional visualizations needed"
    }}
    """
            
            # Check token limit internally before making LLM call (MULTI-USER SAFE)
            can_proceed, token_message, should_complete = check_token_limit_internal(state, estimated_tokens=800)
            
            if not can_proceed:
                if should_complete:
                    # Complete job gracefully instead of failing
                    print(f"🔥 [NARRATOR_AGENT] {token_message}")
                    return complete_job_gracefully(state)
                else:
                    # Hard failure (insufficient tokens from start)
                    state["error"] = f"🚫 PROCESS STOPPED: {token_message}"
                    print(f"🚫 [NARRATOR_AGENT] {token_message}")
                    raise TokenLimitExceededException(token_message)
            
            print(f"📊 [NARRATOR_AGENT] {token_message}")
            
            vision_messages = [
                {"role": "system", "content": VISION_ANALYSIS_PROMPT},
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": vision_msg},
                        {
                            "type": "input_image",
                            "image_url": f"data:image/{Path(image_path).suffix[1:]};base64,{base64_image}",
                        },
                    ],
                },
            ]
            content, usage = await llm_call(vision_messages, max_output_tokens=800)

            state["metrics"]["prompt_tokens"] += usage["input_tokens"]
            state["metrics"]["completion_tokens"] += usage["output_tokens"]
            state["metrics"]["total_tokens"] += usage["input_tokens"] + usage["output_tokens"]
            state["metrics"]["successful_requests"] += 1
            tokens_used = usage["input_tokens"] + usage["output_tokens"]
            print(f"📊 [NARRATOR_AGENT] Used {tokens_used} tokens (Total so far: {state['metrics']['total_tokens']})")
            
            # Try to parse JSON response
            try:
                json_match = re.search(r'\{.*\}', content, re.DOTALL)
                if json_match:
                    result = json.loads(json_match.group())
                    result["path"] = image_path
                    return result
            except json.JSONDecodeError:
                pass
            
            # Fallback if JSON parsing fails
            return {
                "path": image_path,
                "selection": "include",
                "reasoning": "Analysis completed",
                "executive_summary": content[:150],
                "business_implications": "This visualization provides insights relevant to the business question",
                "technical_details": f"Chart from {Path(image_path).parent.name} analysis",
                "recommendations": ""
            }
            
        except Exception as e:
            logger.error(f"Error analyzing image {image_path}: {e}")
            return {
                "path": image_path,
                "selection": "exclude",
                "reasoning": f"Error analyzing image: {str(e)}",
                "executive_summary": "",
                "business_implications": "",
                "technical_details": "",
                "recommendations": ""
            }
    
    def generate_error_report(self, error_msg: str) -> str:
            """Generate a basic HTML report in case of errors"""
            return f"""
            <!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Data Analysis Report</title>
  <style>
    .mck-report {{
      font-family: Arial, Helvetica, sans-serif;
      background: #f8f9fa;
      margin: 40px;
      color: #333;
    }}
    .mck-container {{
      margin: auto;
      background: #fff;
      border-radius: 8px;
    }}
    .mck-title {{
      font-size: 24px;
      margin-bottom: 10px;
      color: #222;
    }}
    .mck-subtitle {{
      font-size: 20px;
      margin-top: 20px;
      color: #b00020;
    }}
    .mck-text {{
      font-size: 14px;
      line-height: 1.6;
    }}
    .mck-error-box {{
      border-left: 4px solid #b00020;
      background: #fff4f4;
      padding: 15px;
      margin-top: 15px;
      border-radius: 4px;
    }}
    .mck-strong {{
      color: #b00020;
      font-weight: bold;
    }}
    .mck-em {{
      font-style: italic;
    }}
  </style>
</head>
<body>
  <div class="mck-report">
    <div class="mck-container">      
      <h2 class="mck-subtitle">Generation Error</h2>
      <div class="mck-error-box">
        <p class="mck-text"><span class="mck-strong">Issue:</span> Something went wrong</p>
        <p class="mck-text">User <span class="mck-strong">{error_msg}</p>
        <p class="mck-text"><span class="mck-em">Next step:</span> For more please reach out to your administrator</p>
      </div>
    </div>
  </div>
</body>
</html>
            """
    
    def _gather_all_output_files(self) -> List[str]:
        """Collect all files under output_data directory (non-recursive)."""
        output_dir = self.output_dir
        if not os.path.exists(output_dir):
            return []
        return [str(p) for p in Path(output_dir).rglob("*") if p.is_file()]

    @staticmethod
    def _extract_html_report(content: str) -> str:
        """Best-effort extraction of HTML report from LLM output."""
        raw = (content or "").strip()
        if not raw:
            return ""

        # Remove common fenced wrappers.
        fenced = re.sub(r"^```(?:json|html)?\s*", "", raw)
        fenced = re.sub(r"\s*```$", "", fenced).strip()

        # 1) Try direct JSON payload.
        try:
            parsed = json.loads(fenced)
            if isinstance(parsed, dict):
                return str(parsed.get("html_report", "") or "").strip()
        except Exception:
            pass

        # 2) Try extracting embedded JSON object.
        obj_match = re.search(r"\{[\s\S]*\}", fenced)
        if obj_match:
            try:
                parsed = json.loads(obj_match.group(0))
                if isinstance(parsed, dict):
                    return str(parsed.get("html_report", "") or "").strip()
            except Exception:
                pass

        # 3) If already contains HTML, return from first HTML marker.
        html_idx = fenced.lower().find("<!doctype html>")
        if html_idx == -1:
            html_idx = fenced.lower().find("<html")
        if html_idx != -1:
            return fenced[html_idx:].strip()

        return fenced

    async def _draft_report_frame(self, state: dict, all_files: List[str]) -> Dict[str, Any]:
        """LLM call to draft narrative frame & choose up to 10 key files."""
        milestone_cb = state.get("milestone_callback", lambda *a, **k: None)
        milestone_cb("Narrator: Building report frame prompt", "narrator_frame_prompt_build", {"dependency": "sequential", "is_llm_call": False})
        FRAME_PROMPT = (
            "You are a senior strategy consultant preparing a final executive report.\n"
            "Inputs: user query, hypothesis summary & findings, EDA summary, and a list of all files.\n"
            "You will also receive report_verbosity ('concise' or 'full').\n"
            "Tasks:\n"
            "1. Write a concise narrative frame that sets the story.\n"
            "2. Select files that best support the report.\n"
            "   - If report_verbosity is 'concise', select at most 5 files and prioritize CSV/TXT summaries.\n"
            "   - If report_verbosity is 'full', select up to 10 files.\n"
            "\n"
            "CRITICAL RELIABILITY RULES:\n"
            "- You MUST select files ONLY from the provided all_files list.\n"
            "- selected_files MUST be an exact subset of all_files (exact string match). Do NOT invent paths.\n"
            "- If no files are relevant, return selected_files as an empty list.\n"
            "Return JSON: { 'frame_text': str, 'selected_files': [paths] }" )

        payload = {
            "original_query": state.get("original_query", ""),
            "report_verbosity": state.get("narrator_verbosity", "full"),
            "hypothesis_summary": state.get("hypothesis_summary", ""),
            "hypothesis_findings": state.get("hypothesis_findings", []),
            "eda_summary": state.get("eda_summary", ""),
            "all_files": all_files,
        }

        try:
            # Check token limit internally before making LLM call (MULTI-USER SAFE)
            can_proceed, token_message, should_complete = check_token_limit_internal(state, estimated_tokens=2000)
            
            if not can_proceed:
                if should_complete:
                    # Complete job gracefully instead of failing
                    print(f"🔥 [NARRATOR_AGENT] {token_message}")
                    return complete_job_gracefully(state)
                else:
                    # Hard failure (insufficient tokens from start)
                    state["error"] = f"🚫 PROCESS STOPPED: {token_message}"
                    print(f"🚫 [NARRATOR_AGENT] {token_message}")
                    raise TokenLimitExceededException(token_message)
            
            print(f"📊 [NARRATOR_AGENT] {token_message}")
            
            messages = [
                {"role": "system", "content": FRAME_PROMPT},
                {"role": "user", "content": json.dumps(payload, indent=2)},
            ]
            content, usage = await llm_call(messages, json_response=True, max_output_tokens=2000)
            if not content:
                content = "{}"

            if isinstance(state.get("metrics"), dict):
                m = state["metrics"]
                m["prompt_tokens"] += usage["input_tokens"]
                m["completion_tokens"] += usage["output_tokens"]
                m["total_tokens"] += usage["input_tokens"] + usage["output_tokens"]
                m["successful_requests"] += 1
            tokens_used = usage["input_tokens"] + usage["output_tokens"]
            print(f"📊 [NARRATOR_AGENT] Used {tokens_used} tokens (Total so far: {state['metrics']['total_tokens']})")
            milestone_cb("Narrator: LLM report frame done", "narrator_frame_llm_done", {"dependency": "sequential", "is_llm_call": True})
            return json.loads(content)
        except Exception as e:
            logger.error(f"Error drafting report frame: {e}")
            return {"frame_text": "", "selected_files": []}

    async def _quick_file_analysis(self, path: str, state: dict) -> Dict[str, Any]:
        """Produce a lightweight analysis summary for a single file."""
        p = Path(path)
        if not p.exists():
            return {"file": path, "error": "not found"}

        ext = p.suffix.lower()
        try:
            if ext in {".png", ".jpg", ".jpeg", ".svg"}:
                analysis = await self.analyze_image_with_vision(path, "Executive report", state)
                print("="*300)
                logger.info(f"Analysis: {path}")
                logger.info(f"Analysis: {analysis}")
                print("="*300)
                return {"file": path, "type": "image", "analysis": analysis}
            elif ext == ".csv":
                import pandas as pd
                # Bounded preview (avoid loading huge CSVs) but include REAL VALUES so the narrator does not invent rows.
                preview_n = 20
                df_preview = pd.read_csv(p, nrows=preview_n)

                # Best-effort total row count (can be expensive on huge files, so fallback gracefully)
                approx_total_rows = None
                try:
                    with open(p, "rb") as f:
                        line_count = 0
                        for _ in f:
                            line_count += 1
                    approx_total_rows = max(0, line_count - 1)
                except Exception:
                    approx_total_rows = None

                def _clean_cell(v: Any) -> Any:
                    try:
                        if pd.isna(v):
                            return None
                    except Exception:
                        pass
                    s = str(v)
                    if len(s) > 200:
                        s = s[:200] + "…"
                    return s

                preview_rows: List[Dict[str, Any]] = []
                try:
                    cols = list(df_preview.columns)
                    for _, row in df_preview.iterrows():
                        rec = {str(c): _clean_cell(row[c]) for c in cols}
                        preview_rows.append(rec)
                except Exception:
                    preview_rows = []

                dtypes = {str(c): str(t) for c, t in df_preview.dtypes.items()}
                return {
                    "file": path,
                    "type": "csv",
                    "columns": list(df_preview.columns),
                    "dtypes": dtypes,
                    "preview_row_count": len(preview_rows),
                    "preview_rows": preview_rows,
                    "approx_total_rows": approx_total_rows,
                    "note": f"CSV preview limited to first {preview_n} rows; do not assume values beyond preview unless other artifacts confirm.",
                }
            elif ext in {".txt", ".md"}:
                text = p.read_text(encoding="utf-8", errors="ignore")
                return {"file": path, "type": "text", "snippet": text[:500]}
            else:
                return {"file": path, "type": "binary", "info": "unsupported for preview"}
        except Exception as e:
            return {"file": path, "error": str(e)}

    async def _analyze_selected_files_parallel(self, selected_files: List[str], state: dict) -> List[Dict[str, Any]]:
        """Analyze selected files concurrently with bounded parallelism."""
        if not selected_files:
            return []

        semaphore = asyncio.Semaphore(4)
        milestone_cb = state.get("milestone_callback", lambda *a, **k: None)

        async def _analyze_one(fp: str) -> Dict[str, Any]:
            async with semaphore:
                analysis = await self._quick_file_analysis(fp, state)
                is_llm = analysis.get("type") == "image"
                sub_desc = "Vision analyzed" if is_llm else "Parsed"
                milestone_cb(
                    f"Narrator: {sub_desc} file {Path(fp).name}",
                    "narrator_file_analysis",
                    {"file": str(fp), "dependency": "parallelizable", "is_llm_call": is_llm},
                )
                log_resources(label="narrator_after_file_analysis", state=state, extra={"file": fp})
                return analysis

        return await asyncio.gather(*[_analyze_one(fp) for fp in selected_files])

    async def _generate_final_html(self, state: dict, frame_text: str, file_analyses: List[Dict[str, Any]]) -> str:
        """Use FINAL_SYNTHESIS_PROMPT to create McKinsey-style HTML."""
        milestone_cb = state.get("milestone_callback", lambda *a, **k: None)
        context = {
            "original_query": state.get("original_query", ""),
            "current_date": dt.datetime.now().strftime("%Y-%m-%d"),
            "analysis_mode": state.get("analysis_mode", "slim"),
            "report_verbosity": state.get("narrator_verbosity", "full"),
            "frame_text": frame_text,
            "file_analyses": file_analyses,
            "hypothesis_summary": state.get("hypothesis_summary", ""),
            "hypothesis_findings": state.get("hypothesis_findings", []),
            "eda_summary": state.get("eda_summary", ""),
        }

        try:
            milestone_cb("Narrator: Building final HTML prompt", "narrator_final_prompt_build", {"dependency": "sequential", "is_llm_call": False})
            # Check token limit internally before making LLM call (MULTI-USER SAFE)
            can_proceed, token_message, should_complete = check_token_limit_internal(state, estimated_tokens=6000)
            
            if not can_proceed:
                if should_complete:
                    # Complete job gracefully instead of failing
                    print(f"🔥 [NARRATOR_AGENT] {token_message}")
                    return complete_job_gracefully(state)
                else:
                    # Hard failure (insufficient tokens from start)
                    state["error"] = f"🚫 PROCESS STOPPED: {token_message}"
                    print(f"🚫 [NARRATOR_AGENT] {token_message}")
                    raise TokenLimitExceededException(token_message)
            
            print(f"📊 [NARRATOR_AGENT] {token_message}")
            
            messages = [
                {"role": "system", "content": FINAL_SYNTHESIS_PROMPT},
                {"role": "user", "content": json.dumps(context, indent=2)},
            ]
            content, usage = await llm_call(messages, json_response=True, max_output_tokens=6000)
            if not content:
                content = "{}"

            if isinstance(state.get("metrics"), dict):
                m = state["metrics"]
                m["prompt_tokens"] += usage["input_tokens"]
                m["completion_tokens"] += usage["output_tokens"]
                m["total_tokens"] += usage["input_tokens"] + usage["output_tokens"]
                m["successful_requests"] += 1
            tokens_used = usage["input_tokens"] + usage["output_tokens"]
            print(f"📊 [NARRATOR_AGENT] Used {tokens_used} tokens (Total so far: {state['metrics']['total_tokens']})")
            milestone_cb("Narrator: LLM final HTML done", "narrator_final_llm_done", {"dependency": "sequential", "is_llm_call": True})
            
            try:
                result = json.loads(content)
                
                # Stream LLM thinking logs via progress_callback
                progress_callback = state.get("progress_callback", lambda *args, **kwargs: None)
                thinking_logs = result.get('thinking_logs', [])
                
                for i, log in enumerate(thinking_logs):
                    progress_callback(f"🤖 Narrator LLM #{i+1}", log, "📖")
                    # Small delay to make logs visible
                    await asyncio.sleep(0.2)
                
                html_report = result.get("html_report", "")
                return html_report.strip()
            except json.JSONDecodeError:
                return self._extract_html_report(content)
        except Exception as e:
            logger.error(f"Error generating final HTML: {e}")
            return self.generate_error_report(str(e))

    async def ainvoke(self, state: dict, config=None, **kwargs) -> dict:
        """Main narrator agent logic with sequential analysis and McKinsey-style reporting"""
        milestone_cb = state.get("milestone_callback", lambda *a, **k: None)

        try:
            logger.info("Starting comprehensive McKinsey-style report generation...")
            log_resources(label="narrator_start", state=state)

            # Initialize metrics if not present
            if "metrics" not in state:
                state["metrics"] = {
                    "prompt_tokens": 0,
                    "completion_tokens": 0,
                    "total_tokens": 0,
                    "successful_requests": 0
                }
           
            # 1. Gather all files in output_data
            all_files = self._gather_all_output_files()
            is_slim = state.get("narrator_verbosity") == "concise"
            if is_slim:
                # Slim mode: never include image files in report pipeline.
                image_ext = {".png", ".jpg", ".jpeg", ".svg", ".pdf", ".webp", ".gif"}
                all_files = [f for f in all_files if Path(f).suffix.lower() not in image_ext]
            milestone_cb(f"Narrator: Gathered {len(all_files)} files", "narrator_gather_complete", {"file_count": len(all_files), "dependency": "sequential", "is_llm_call": False})
            log_resources(label="narrator_after_gather_files", state=state, extra={"all_files_count": len(all_files)})
            logger.info(f"[Narrator] Collected {all_files} files from output_data")
            logger.info(f"[Narrator] Collected {len(all_files)} files from output_data")

            # 2. Draft report frame and select key files (subtasks: prompt build, LLM)
            frame_dict = await self._draft_report_frame(state, all_files)
            log_resources(label="narrator_after_frame_llm", state=state)
            frame_text = frame_dict.get("frame_text", "")
            selected_files = frame_dict.get("selected_files", [])
            if is_slim:
                # Extra safety: enforce no images in slim and bound selected files for speed.
                image_ext = {".png", ".jpg", ".jpeg", ".svg", ".pdf", ".webp", ".gif"}
                selected_files = [f for f in selected_files if Path(f).suffix.lower() not in image_ext][:5]
            logger.info(f"[Narrator] LLM selected {selected_files} files for deep dive")
            logger.info(f"[Narrator] LLM selected {len(selected_files)} files for deep dive")

            # 3. Analyze selected files in parallel (bounded concurrency)
            file_analyses = await self._analyze_selected_files_parallel(selected_files, state)

            # 4. Generate final HTML report (subtasks: prompt build, LLM, post-process)
            html_report = await self._generate_final_html(state, frame_text, file_analyses)
            log_resources(label="narrator_after_final_html_llm", state=state, extra={"html_len": len(html_report) if isinstance(html_report, str) else None})
            
            # Clean the HTML report - handle both code block and non-code block cases (subprocess)
            clean_html = html_report
            if "```html" in html_report or "```" in html_report:
                # Extract HTML content from code blocks if present
                matches = re.findall(r'```(?:html)?(.*?)```', html_report, re.DOTALL)
                if matches:
                    clean_html = matches[0].strip()
                else:
                    clean_html = html_report.replace("```html", "").replace("```", "").strip()
            
            clean_html = clean_html.strip()
            if clean_html:
                # Keep at most one doctype, anchored to the top.
                first_doctype = clean_html.lower().find("<!doctype html>")
                if first_doctype > 0:
                    clean_html = clean_html[first_doctype:]
                clean_html = re.sub(r"(?is)^\s*(?:<!doctype html>\s*)+", "<!DOCTYPE html>\n", clean_html).strip()

            # Ensure minimal HTML structure if needed.
            if "<html" in clean_html.lower():
                if not clean_html.lower().startswith("<!doctype html>"):
                    clean_html = f"<!DOCTYPE html>\n{clean_html}"
            else:
                clean_html = f"<!DOCTYPE html>\n<html><body>{clean_html}</body></html>"
            milestone_cb("Narrator: Post-processing HTML", "narrator_final_html_complete", {"dependency": "sequential", "is_llm_call": False})

             # Convert images to base64
            # try:
            #     base64_html = await asyncio.get_event_loop().run_in_executor(
            #         None,
            #         lambda: self.convert_images_to_base64(clean_html)
            #     )
            # except Exception as e:
            #     logger.error(f"Error converting images to base64: {e}")
            #     base64_html = clean_html  # Fallback to clean HTML if base64 conversion fails

            logger.info("Generated report details:")
            logger.info(f"Original length: {len(html_report)}")
            logger.info(f"Cleaned length: {len(clean_html)}")
            # logger.info(f"Base64 length: {len(base64_html)}")
            # logger.info(f"Contains base64 images: {'data:image' in base64_html}")

            milestone_cb("Narrator: Report complete", "narrator_complete", {"dependency": "sequential", "is_llm_call": False})
            # 5. Save outputs to state and disk for downstream use
            state["final_html_report"] = clean_html
            state["narrator_frame_text"] = frame_text
            state["narrator_file_analyses"] = file_analyses
            log_resources(label="narrator_end", state=state, extra={"clean_html_len": len(clean_html)})
 
        except Exception as e:
            logger.error(f"Narrator Agent error: {e}")
            state["error"] = f"Narrator Agent failed: {str(e)}"
            
        return state
    
    def invoke(self, state: dict, config=None, **kwargs):
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
