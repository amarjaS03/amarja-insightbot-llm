import os
import json
import logging
import asyncio
import re
import base64
import mimetypes
from typing import Dict, Any, List
from pathlib import Path
from openai import AsyncOpenAI
from dotenv import load_dotenv
from langchain_core.runnables import Runnable
from bs4 import BeautifulSoup

from agents.eda_agent import EDAAgent
from agents.hypothesis_agent import HypothesisAgent
from agents.narrator_agent import NarratorAgent
from agents.analysis_mode import (
    normalize_analysis_mode,
    infer_question_depth,
    executor_max_attempts,
    narrator_verbosity,
    skip_hypothesis,
)
from agents.token_manager import check_token_limit_internal, complete_job_gracefully, TokenLimitExceededException
from agents.llm_client import ENFORCED_MODEL

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def convert_images_to_base64_for_report(*, html_content: str, output_dir: str | Path | None = None) -> str:
    """
    Convert <img src="..."> in an HTML string to base64 data URIs.
    Intended for final report packaging so the HTML becomes self-contained.
    """
    soup = BeautifulSoup(html_content or "", "html.parser")

    out_dir = Path(output_dir) if output_dir else None
    script_dir = Path(__file__).resolve().parent

    # Build a basename->path map from output_dir for fallback resolution
    available_images: Dict[str, Path] = {}
    if out_dir and out_dir.exists():
        for ext in ("*.png", "*.jpg", "*.jpeg", "*.svg", "*.gif", "*.webp"):
            for p in out_dir.glob(ext):
                available_images[p.name.lower()] = p
        img_utils = out_dir / "image_utils"
        if img_utils.exists():
            for p in img_utils.rglob("*.png"):
                available_images[p.name.lower()] = p

    for img_tag in soup.find_all("img"):
        src = (img_tag.get("src") or "").strip()
        if not src:
            continue

        # Skip already-inlined or remote images
        if src.startswith("data:") or re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", src):
            continue

        # Strip querystring/fragment and normalize
        src_clean = src.split("?", 1)[0].split("#", 1)[0]
        if src_clean.startswith("./"):
            src_clean = src_clean[2:]

        src_path = Path(src_clean)
        candidates: List[Path] = []
        if src_path.is_absolute():
            candidates.append(src_path)
        if out_dir:
            candidates.append(out_dir / src_path)
            candidates.append(out_dir / src_path.name)
        candidates.append(Path.cwd() / src_path)
        candidates.append(script_dir / src_path)
        # Fallback: match by basename from available images
        if out_dir and src_path.name:
            by_basename = available_images.get(src_path.name.lower())
            if by_basename:
                candidates.append(by_basename)

        img_path = next((p for p in candidates if p and p.exists()), None)
        if not img_path:
            logger.warning(f"[base64] Image not found for src='{src}' (tried: {candidates})")
            continue

        mime_type, _ = mimetypes.guess_type(str(img_path))
        if not mime_type:
            mime_type = "image/png"

        try:
            encoded = base64.b64encode(img_path.read_bytes()).decode("utf-8")
        except Exception as e:
            logger.warning(f"[base64] Failed reading '{img_path}': {e}")
            continue

        base64_src = f"data:{mime_type};base64,{encoded}"

        new_img_tag = soup.new_tag("img")
        new_img_tag["src"] = base64_src
        for attr_name, attr_value in img_tag.attrs.items():
            if attr_name != "src":
                new_img_tag[attr_name] = attr_value
        img_tag.replace_with(new_img_tag)

    return str(soup)


class CancelledException(Exception):
    """Raised when processing is cancelled by user"""
    pass


def check_cancellation(state: dict, checkpoint_name: str = "") -> bool:
    """Check if the current session/job has been cancelled.
    
    Args:
        state: The pipeline state containing session_id, job_id and cancellation_manager
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
        logger.info(f"🛑 [CANCELLATION] Analysis cancelled for session={session_id}, job={job_id}{checkpoint_info}")
        state["cancelled"] = True
        raise CancelledException(f"Processing cancelled{checkpoint_info}")
    
    return False

# Domain directory will be loaded dynamically per job
def load_domain_directory(input_dir: str = '/app/execution_layer/input_data') -> dict:
    """Load domain directory from dynamic input path"""
    domain_path = os.path.join(input_dir, 'domain_directory.json')
    if os.path.exists(domain_path):
        try:
            with open(domain_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Error loading domain directory from {domain_path}: {e}")
    else:
        logger.warning(f"Domain directory not found at {domain_path}")
    return {}


def build_pseudonymized_columns_map(domain_directory: dict) -> Dict[str, List[str]]:
    """
    Build a domain-agnostic pseudonymization map from domain_directory.json:
      { "data_set_name.pkl": ["col1","col2"], "data_set_name2.pkl": ["col3"] }
    """
    out: Dict[str, List[str]] = {}
    dsf = domain_directory.get("data_set_files", {}) if isinstance(domain_directory, dict) else {}
    if not isinstance(dsf, dict):
        return out
    for ds_name, meta in dsf.items():
        if not isinstance(meta, dict):
            continue
        cols = meta.get("pseudonymized_columns")
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

def create_query_analysis_prompt(domain_directory: dict) -> str:
    """Create query analysis prompt with dynamic domain directory"""
    return f"""You are an expert query analyst who understands user queries and have domain knowledge.
Domain Directory: {domain_directory}

IMPORTANT: You must provide detailed business-focused thinking logs throughout your analysis process so users can see your business reasoning and decision-making process. Think like a business analyst, not a technical analyst.

Step-by-step business analysis process with thinking logs:
Step 1: Analyze the user query and use your domain knowledge to determine the intent(What user wants)
Step 2: **CRITICAL - RELEVANCY CHECK**: Carefully examine the Domain Directory above to determine if the available datasets contain the necessary columns, dimensions, and facts to answer this query. Check:
   - Are the required data fields/columns present in any of the datasets?
   - Does the domain context match what the user is asking about?
   - Can the query be answered with the available data, even partially?
Step 3: If query IS supportable - break down the complex query into sub queries
Step 4: Determine the brief and exact plan to solve the sub queries(include required dimensions and facts)
Step 5: Determine the appropriate output like types of graphs, charts, tables, etc. 

Response format (JSON):
{{
    "thinking_logs": [
        "💼 Understanding business question and stakeholder needs...",
        "🔍 Checking data availability: [examining domain directory for required fields]",
        "🏢 Identifying relevant business entities and KPIs: [specific business areas]",
        "🎯 Defining business objective: [what decision will this inform]",
        "📋 Breaking down into business sub-questions: [specific business queries]",
        "💡 Planning analysis approach to drive business insights: [business strategy]",
        "📊 Selecting business-relevant outputs: [executive dashboards/reports]"
    ],
    "is_supportable": true/false,
    "unsupported_reason": "Only provide if is_supportable is false. Give a clear, user-friendly explanation of why this query cannot be answered with the available data. Mention what data would be needed and suggest alternative questions that CAN be answered with the current datasets.",
    "user_intent": "brief description of what user wants",
    "sub_queries": "list of sub queries (empty array if not supportable)",
    "plan": "brief plan to solve the sub queries (empty array if not supportable) [{{"sub_query1":"plan1", "sub_query2":"plan2",...}}]",
    "expected_output": "list of output and it's brief description of expected output (empty array if not supportable)"
}}

CRITICAL RULES FOR RELEVANCY CHECK:
1. If is_supportable is FALSE, you MUST provide a helpful unsupported_reason that:
   - Explains what data is missing (e.g., "The dataset does not contain salary/compensation information")
   - Lists what columns/data would be needed
   - Suggests 2-3 alternative questions the user CAN ask based on available data
2. If is_supportable is TRUE, set unsupported_reason to null
3. Be conservative - if you're unsure whether the data supports the query, lean towards marking it as unsupportable with a clear explanation
4. Always include 4-6 detailed thinking_logs that show your actual reasoning process, including your data availability assessment
"""

class DataAnalysisAgent(Runnable):
    def __init__(self, output_dir):
        self.client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.model = (os.getenv("MODEL_NAME") or ENFORCED_MODEL).strip() or ENFORCED_MODEL
        self.output_dir = output_dir
        print("output_dir in DataAnalysisAgent", self.output_dir)

    async def analyze_user_query(self, user_query: str, state: dict) -> Dict[str, Any]:
        """Analyze user query to determine analysis approach and check data supportability"""
        try:
            logger.info(f"Processing user query: {user_query}")
            
            # Load domain directory dynamically from job-specific input directory
            input_dir = state.get('input_dir', '/app/execution_layer/input_data')
            domain_directory = load_domain_directory(input_dir)
            
            print(f"🔧 [DATA ANALYSIS AGENT] Using input dir: {input_dir}")
            print(f"📁 [DATA ANALYSIS AGENT] Domain directory loaded: {len(domain_directory)} entries")
            
            # Create dynamic prompt with job-specific domain directory
            query_analysis_prompt = create_query_analysis_prompt(domain_directory)
            
            # Check token limit internally before making LLM call (MULTI-USER SAFE)
            can_proceed, token_message, should_complete = check_token_limit_internal(state, estimated_tokens=800)
            
            if not can_proceed:
                if should_complete:
                    # Complete job gracefully instead of failing
                    print(f"🔥 [DATA_ANALYSIS_AGENT] {token_message}")
                    return complete_job_gracefully(state)
                else:
                    # Hard failure (insufficient tokens from start)
                    state["error"] = f"🚫 PROCESS STOPPED: {token_message}"
                    print(f"🚫 [DATA_ANALYSIS_AGENT] {token_message}")
                    raise TokenLimitExceededException(token_message)
            
            print(f"📊 [DATA_ANALYSIS_AGENT] {token_message}")
            
            response = await self.client.responses.create(
                model=self.model,
                input=[
                    {"role": "system", "content": query_analysis_prompt},
                    {"role": "user", "content": f"User query: {user_query}"}
                ],
                text={"format": {"type": "json_object"}},
                max_output_tokens=1100,
            )
            
            # Update metrics in state
            state["metrics"]["prompt_tokens"] += getattr(response.usage, "input_tokens", 0)
            state["metrics"]["completion_tokens"] += getattr(response.usage, "output_tokens", 0)
            state["metrics"]["total_tokens"] += (
                getattr(response.usage, "input_tokens", 0) + getattr(response.usage, "output_tokens", 0)
            )
            state["metrics"]["successful_requests"] += 1
            
            # Log token usage for this call
            if hasattr(response, "usage") and response.usage:
                tokens_used = getattr(response.usage, "input_tokens", 0) + getattr(response.usage, "output_tokens", 0)
                print(f"📊 [DATA_ANALYSIS_AGENT] Used {tokens_used} tokens (Total so far: {state['metrics']['total_tokens']})")
            
            content = getattr(response, "output_text", None)
            if not content:
                try:
                    content = response.output[0].content[0].text
                except Exception:
                    content = "{}"
            
            result = json.loads(content)
            
            # Stream LLM thinking logs via progress_callback
            progress_callback = state.get("progress_callback", lambda *args, **kwargs: None)
            thinking_logs = result.get('thinking_logs', [])
            
            for i, log in enumerate(thinking_logs):
                progress_callback(f"🤖 LLM Thinking #{i+1}", log, "🧠")
                # Small delay to make logs visible
                await asyncio.sleep(0.03)
            
            # Check if query is supportable
            is_supportable = result.get('is_supportable', True)  # Default to True for backward compatibility
            
            if not is_supportable:
                unsupported_reason = result.get('unsupported_reason', 'The query cannot be answered with the available data.')
                logger.warning(f"⚠️ Query not supportable: {unsupported_reason}")
                progress_callback("⚠️ Query Not Supportable", unsupported_reason, "🚫")
                
                # Mark in state that query is not supportable
                state["query_not_supportable"] = True
                state["unsupported_reason"] = unsupported_reason
                
                print(f"⚠️ [DATA_ANALYSIS_AGENT] Query not supportable: {unsupported_reason}")
            
            logger.info(f"Query analysis complete: {result.get('user_intent', '')} (supportable: {is_supportable})")
            return result
        except Exception as e:
            logger.error(f"Error analyzing user query: {e}")
            return self._fallback_query_analysis(user_query)

    def _fallback_query_analysis(self, user_query: str) -> Dict[str, Any]:
        """Fallback analysis if JSON parsing fails"""
        return {
            "thinking_logs": [
                "💼 Fallback mode: Unable to complete full analysis",
                "🔍 Will attempt generic data exploration approach"
            ],
            "is_supportable": True,  # Assume supportable in fallback to allow pipeline to continue
            "unsupported_reason": None,
            "user_intent": "Analyze the dataset and solve the user query",
            "sub_queries": ["Analyze dataset", "Identify related columns and datasets", "Generate output based on analysis"],
            "plan": [{"Analyze dataset":"Use EDA techniques to explore dataset"}, {"Identify related columns and datasets":"Identify key relationships and patterns"}, {"Generate output based on analysis":"Generate visualizations and summary statistics"}],
            "expected_output": "Charts, graphs, and summary statistics based on the dataset",
        }
        
    async def ainvoke(self, state: dict, config=None, **kwargs) -> dict:
        """Main data analysis agent logic - orchestrates the entire pipeline"""
        
        try:
            original_query = state["original_query"]
            analysis_mode = normalize_analysis_mode(state.get("analysis_mode"))
            state["analysis_mode"] = analysis_mode

            session_pseudonymized = bool(state.get("session_pseudonymized"))

            # Load domain directory once and expose key metadata to all downstream agents.
            try:
                input_dir = state.get('input_dir', '/app/execution_layer/input_data')
                domain_directory = load_domain_directory(input_dir)
                state["domain_directory"] = domain_directory
                state["pseudonymized_columns_map"] = build_pseudonymized_columns_map(domain_directory)
            except Exception:
                state.setdefault("domain_directory", {})
                state.setdefault("pseudonymized_columns_map", {})

            # Create image artifacts when session is pseudonymized OR domain has pseudonymized columns
            # (needed for depseudonymization - executor must populate pseudonymized_columns in image_master)
            try:
                Path(self.output_dir).mkdir(parents=True, exist_ok=True)
                if session_pseudonymized or state.get("pseudonymized_columns_map"):
                    (Path(self.output_dir) / "image_utils").mkdir(parents=True, exist_ok=True)
                    # Ensure image_master.json exists early so generated code can safely update it.
                    image_master_path = Path(self.output_dir) / "image_master.json"
                    if not image_master_path.exists():
                        image_master_path.write_text("{}", encoding="utf-8")
            except Exception:
                pass
            
            # Get progress and milestone callbacks from state (passed from execution_api.py)
            progress_callback = state.get("progress_callback", lambda *args, **kwargs: None)
            milestone_cb = state.get("milestone_callback", lambda *a, **k: None)

            # Cancellation checkpoint: Before starting
            check_cancellation(state, "before_analysis_start")

            # Step 1: Analyze user query (pure LLM)
            logger.info("Analyzing user query...")
            progress_callback("Query Analysis", "Understanding your question and checking data availability", "🧠")
            
            query_analysis = await self.analyze_user_query(original_query, state)
            state["query_analysis"] = query_analysis
            # Use pre-set question_depth (e.g. from /analyze_job) if available; otherwise infer from query
            question_depth = state.get("question_depth") or infer_question_depth(original_query, query_analysis)
            state["question_depth"] = question_depth
            state["executor_max_retries"] = executor_max_attempts(analysis_mode, question_depth)
            state["narrator_verbosity"] = narrator_verbosity(analysis_mode)
            logger.info(f"[PIPELINE] analysis_mode={analysis_mode}, question_depth={question_depth}, executor_max_retries={state['executor_max_retries']}, narrator_verbosity={state['narrator_verbosity']}")
            milestone_cb("Analysis: Query analysis", "analysis_query_complete", {"dependency": "sequential", "is_llm_call": True})
            logger.info(f"Query analysis result: {query_analysis}")
            logger.info("Query analysis complete")
            
            # Check if query is not supportable - return early with explanation
            if state.get("query_not_supportable", False):
                unsupported_reason = state.get("unsupported_reason", "Unable to answer this query with available data.")
                logger.warning(f"⚠️ Returning early - query not supportable: {unsupported_reason}")
                
                progress_callback("Analysis Complete", "Query cannot be supported with available data", "⚠️")
                
                # Set appropriate state for unsupported query response
                state["analysis_skipped"] = True
                state["analysis_skip_reason"] = "query_not_supportable"
                state["error"] = None  # Not an error, just unsupported
                
                return state
            
            # Cancellation checkpoint: After query analysis
            check_cancellation(state, "after_query_analysis")
            
            # Step 2: Run EDA with determined user intent
            logger.info(f"Running EDA with intent: {query_analysis['user_intent']}")
            progress_callback("EDA Started", "Starting exploratory data analysis", "📊")
            
            eda_agent = EDAAgent(output_dir=self.output_dir)
            # Set EDA analysis type and run on the same shared state
            state["command"] = f"Perform {query_analysis} analysis: {original_query}"
            
            progress_callback("EDA In Progress", "Analyzing data patterns and distributions", "🔍")
            state = await eda_agent.ainvoke(state, config, **kwargs)
            
            print("="*60)
            logger.info(f"eda_summary in data analysis: {state['eda_summary']}")
            print("="*100)
            # EDAAgent already updated the shared state
            
            milestone_cb("Analysis: EDA phase", "analysis_eda_complete", {"dependency": "sequential", "is_llm_call": False})
            # Cancellation checkpoint: After EDA
            check_cancellation(state, "after_eda")

            # Step 3: Pass to Hypothesis Agent
            if skip_hypothesis(analysis_mode):
                logger.info("Slim mode enabled: skipping hypothesis stage")
                state["hypothesis_findings"] = []
                state["hypothesis_summary"] = "Hypothesis stage intentionally skipped in slim mode."
                milestone_cb("Analysis: Hypothesis skipped (slim mode)", "analysis_hypothesis_skipped", {"dependency": "sequential", "is_llm_call": False})
            else:
                logger.info("Passing to Hypothesis Agent...")
                progress_callback("Hypothesis Testing", "Testing statistical hypotheses and insights", "🔬")
                
                hypothesis_agent = HypothesisAgent(output_dir=self.output_dir)
                state["query_analysis"] = query_analysis
                state = await hypothesis_agent.ainvoke(state, config, **kwargs)

                print("="*100)
                logger.info(f"Hypothesis Findings: {state['hypothesis_findings']}")
                print("="*100)

                # # # Update state with hypothesis results
                print("="*100)
                logger.info(f"Hypothesis Summary: {state['hypothesis_summary']}")
                print("="*100)
                
                milestone_cb("Analysis: Hypothesis testing", "analysis_hypothesis_complete", {"dependency": "sequential", "is_llm_call": False})
                # Cancellation checkpoint: After Hypothesis
                check_cancellation(state, "after_hypothesis")

            # Step 4: Pass to Narrator Agent
            logger.info("Passing to Narrator Agent...")
            progress_callback("Story Generation", "Creating narrative insights from analysis", "📖")
            
            narrator_agent = NarratorAgent(output_dir=self.output_dir)
            state = await narrator_agent.ainvoke(state, config, **kwargs)
            
            milestone_cb("Analysis: Narrator/story generation", "analysis_narrator_complete", {"dependency": "sequential", "is_llm_call": False})
            # Cancellation checkpoint: After Narrator
            check_cancellation(state, "after_narrator")

            milestone_cb("Analysis: Converting report and saving", "analysis_report_saving", {"dependency": "sequential", "is_llm_call": False})
            # Get original HTML and convert images to base64 (everything is stored in GCS, so paths need to be resolved)
            original_html = (state.get("final_html_report") or "").strip()
            base64_original_html = None
            
            if original_html:
                Path(self.output_dir).mkdir(parents=True, exist_ok=True)
                # Convert images to base64 before processing
                try:
                    base64_original_html = await asyncio.get_event_loop().run_in_executor(
                        None,
                        lambda: convert_images_to_base64_for_report(
                            html_content=original_html, output_dir=self.output_dir
                        )
                    )
                    if not base64_original_html:
                        base64_original_html = original_html  # Fallback to original if conversion fails
                except Exception as e:
                    logger.warning(f"Error converting images to base64 for original report: {e}")
                    base64_original_html = original_html  # Fallback to original if conversion fails

            # Execution layer only packages reports; data restoration pipeline lives in v2.
            # We always persist a portable base64 analysis report, and for pseudonymized sessions
            # we additionally save the original report (non-base64) for the separate v2 pipeline.
            if session_pseudonymized:
                progress_callback(
                    "Report Generation",
                    "Pseudonymized session detected: saving original + base64 analysis reports",
                    "📄",
                )
                if original_html:
                    try:
                        (Path(self.output_dir) / "analysis_report_original.html").write_text(
                            original_html, encoding="utf-8", errors="ignore"
                        )
                        logger.info("Saved original HTML report (analysis_report_original.html)")
                    except Exception as e:
                        logger.warning(f"Error saving original report: {e}")
                state["report_processing"] = {"status": "dual_report_saved"}
            else:
                progress_callback(
                    "Report Generation",
                    "Non-pseudonymized session: skipping image metadata flow",
                    "ℹ️",
                )
                state["report_processing"] = {"status": "single_report_saved", "reason": "session_not_pseudonymized"}

            if base64_original_html:
                state["final_html_report"] = base64_original_html
                try:
                    (Path(self.output_dir) / "analysis_report.html").write_text(
                        base64_original_html, encoding="utf-8", errors="ignore"
                    )
                    logger.info("Saved base64 analysis report (analysis_report.html)")
                except Exception as e:
                    logger.warning(f"Error saving final analysis report: {e}")
            
            milestone_cb("Analysis: Report saved", "analysis_report_complete", {"dependency": "sequential", "is_llm_call": False})
            # Step 5: Final report generation
            progress_callback("Report Generation", "Generating comprehensive analysis report", "📋")

            state["error"] = state.get("error")
            logger.info("Data Analysis pipeline completed successfully")

            milestone_cb("Analysis: Pipeline complete", "analysis_complete", {"dependency": "sequential", "is_llm_call": False})
            # Final completion
            progress_callback("Analysis Complete", "Analysis completed successfully! 🎉", "✅")
        
        except CancelledException:
            # User cancelled - this is not an error, just exit gracefully
            progress_callback = state.get("progress_callback", lambda *args, **kwargs: None)
            progress_callback("Analysis Cancelled", "Processing stopped by user", "🛑")
            logger.info("🛑 Analysis cancelled by user - exiting gracefully")
            state["cancelled"] = True
            state["error"] = None  # Clear any error since this was intentional
            
        except Exception as e:
            # Emit error progress
            progress_callback = state.get("progress_callback", lambda *args, **kwargs: None)
            progress_callback("Analysis Failed", f"Something went wrong", "❌")
            
            logger.error(f"Data Analysis Agent error: {e}")
            state["error"] = f"Data Analysis Agent failed: {str(e)}"
            
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