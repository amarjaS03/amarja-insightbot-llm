"""
Simple QnA Code Executor Agent for v2

Migrated from execution_layer/agents/simpleqna_executor.py
Adapted for v2 module structure - pure execution logic.
"""
import os
import json
import logging
import asyncio
import re
from pathlib import Path
from typing import Dict

from dotenv import load_dotenv
from langchain_core.runnables import Runnable

from execution_layer.agents.coding_tool import JupyterExecutionTool
from execution_layer.utils.llm_core import CALLER_CONFIG, call_llm

# load .env
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
        logger.info(f"🛑 [CANCELLATION] Code execution cancelled for session={session_id}, job={job_id}{checkpoint_info}")
        state["cancelled"] = True
        raise CancelledException(f"Processing cancelled{checkpoint_info}")
    
    return False

MODULE_DIR = Path(__file__).parent.resolve()
EXEC_BASE_DIR = MODULE_DIR.parent.parent.parent.parent.resolve() / "execution_layer"
domain_directory_path = EXEC_BASE_DIR / 'input_data' / 'domain_directory.json'
domain_directory = {}
try:
    if domain_directory_path.exists():
        with open(domain_directory_path, 'r', encoding='utf-8') as f:
            domain_directory = json.load(f)
except Exception as e:
    logger.warning(f"Error loading domain directory: {e}")

def _load_domain_directory(input_dir: str = "/app/execution_layer/input_data") -> Dict:
    """Load domain_directory.json from the current session input directory.
    Returns empty dict if missing or unreadable.
    """
    try:
        path = Path(input_dir) / "domain_directory.json"
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                dd = json.load(f)
                logger.info(f"Domain directory loaded successfully from {path}: {len(dd)} entries")
                return dd
    except Exception as e:
        logger.error(f"CRITICAL: Failed to load domain directory from {input_dir}: {e}")
    return {}


def _build_system_prompt(input_dir: str) -> str:
    """Construct the system prompt per request using the live domain directory.
    
    NOTE: This prompt is intentionally kept SIMPLE and focused on DATA ACCURACY.
    Unit formatting and presentation is handled separately in judge_answer() 
    to avoid cognitive overload during code generation.
    """
    domain_directory = _load_domain_directory(input_dir)
    return f"""You are a Python data science assistant.
Convert the user's request into executable Python code.
Use the JSON list of previous runs (code + output) for context.
also refer to this Domain Directory: {domain_directory}

IMPORTANT RULES:
- Only return valid Python code
- Always READ from the absolute path '/app/execution_layer/input_data/' (e.g., pd.read_pickle('/app/execution_layer/input_data/Loan_Customer_Summary.pkl'))
- Always SAVE any CSVs or artifacts to the absolute path '/app/execution_layer/output_data/' with descriptive names
- Prefer table/text outputs over images. Create charts only if explicitly requested or essential.
- Standardize the main tabular answer as a DataFrame named result_df when applicable.
- For tabular results:
  - If result_df has 20 rows or fewer, DO NOT save a file. Print result_df fully (e.g., print(result_df.to_string(index=False))).
  - If result_df has more than 20 rows, save to a CSV in '/app/execution_layer/output_data/' with a descriptive name. After saving, PRINT A SINGLE MARKER LINE exactly as: __SAVED_CSV__=<csv_path>. Also print a small sample (e.g., print(result_df.head(10).to_string(index=False))).
- Save only the final result_df (do NOT save any intermediate DataFrames).
- Important: Avoid duplicate outputs. Do NOT both print and return result_df as the final expression. If you print the table or marker, make the last line `None` to avoid echoing the object in Jupyter.
- Reference existing variables or dataframes from previous executions
- For data exploration, make sure to output the result (use print() if needed)
- Do not include explanations or markdown
- Make the last line an expression that evaluates to the final object (e.g., result_df or a scalar)

CRITICAL ERROR HANDLING:
- If previous code failed, analyze the error and generate corrected code
- Handle common errors like missing files, wrong column names, data type issues
- Use only standard libraries: pandas, numpy, matplotlib, seaborn, scipy. Do NOT use statsmodels.
- For prediction or forecasting: do NOT use ML. Only read data, compute trends (moving averages, growth rates), and describe patterns.
- If you see "ModuleNotFoundError" for a module, use os.system('pip install <module>') to install it
- For missing files or directories, first verify existence with os.path.exists('/app/execution_layer/input_data') and list with os.listdir('/app/execution_layer/input_data'). Never assume relative paths.
"""


class SimpleQnaCodeAgent(Runnable):
    """
    Simple QnA Code Agent.
    
    Executes Python code based on natural language instructions.
    Handles retries and error recovery.
    """
    
    def __init__(self):
        # spin up a persistent Jupyter kernel
        self.executor = JupyterExecutionTool()
        self.max_retries = 3

    async def nl_to_code(self, nl_command: str, history: list, retry_count: int = 0, state: dict = None) -> str:
        # pass last 10 history items as context including errors
        ctx = json.dumps(history[-5:], indent=2) if history else "[]"
        
        retry_context = ""
        if retry_count > 0:
            retry_context = f"\nThis is retry attempt {retry_count}. Previous attempts failed. Analyze the errors in context and generate code to fix the issues."
            

        user_msg = f"""
        Context (last 5 runs, with errors if any):
        {ctx}
        {retry_context}

        query: {state["current_query"]}
        code instructions: {nl_command}
        expected output: {state["current_expected_output"]}

        IMPORTANT ENVIRONMENT DETAILS:
        - Absolute input directory: /app/execution_layer/input_data
        - Absolute output directory: /app/execution_layer/output_data
        - Before reading any file, verify os.path.exists('/app/execution_layer/input_data').
        - List available files with os.listdir('/app/execution_layer/input_data') when needed.
        - Never use relative 'input_data' or 'output_data' paths.
        """
        
        try:
            system_prompt = _build_system_prompt(state.get("input_dir", "/app/execution_layer/input_data"))
            code = await call_llm(
                SYS_PROMPT=system_prompt,
                USER_ANALYSIS_PROMPT="Task",
                USER_PROMPT=user_msg,
                api_type="chat",
                system_role="developer",
                temperature=CALLER_CONFIG["V2_SIMPLEQNA_CODE"]["temperature"],
                max_tokens=CALLER_CONFIG["V2_SIMPLEQNA_CODE"]["max_tokens"],
                state=state,
            )
            
            # Clean up code formatting
            if code.startswith("```python"):
                code = code.split("```python")[1]
            if code.startswith("```"):
                code = code.split("```")[1]
            if code.endswith("```"):
                code = code[:-3]
            
            code = code.strip()
            return code
            
        except Exception as e:
            logger.error(f"Error generating code: {e}")
            # Check if it's an API-specific error and provide helpful information
            if "api" in str(e).lower() or "openai" in str(e).lower():
                return f"print('API Error: Please check your OpenAI API key and model access. Error: {str(e)}')"
            else:
                return f"print('Error generating code: {str(e)}')"

    async def execute_with_retry(self, nl_command: str, history: list, state: dict = None) -> dict:
        """Execute code with retry logic for error handling"""
        
        for attempt in range(self.max_retries):
            try:
                # Check for cancellation before each attempt
                if state:
                    check_cancellation(state, f"before_code_attempt_{attempt + 1}")
                
                # Generate code
                code = await self.nl_to_code(nl_command, history, attempt, state)
                
                # Execute code
                logger.info(f"Executing code (attempt {attempt + 1}): {code}")
                result = self.executor.execute_code(code)
                logger.info(f"Execution result (attempt {attempt + 1}): {result}")
                
                # If successful, return result (with enforcement for large results needing CSV save)
                if result["success"] or not result["error"]:
                    out_text = result.get("output", "") or ""
                    has_marker = "__SAVED_CSV__=" in out_text
                    # Detect pandas print footer like "[136 rows x 2 columns]"
                    m = re.search(r"\[(\d+)\s+rows\s+x\s+\d+\s+columns\]", out_text)
                    num_rows = int(m.group(1)) if m else 0
                    if num_rows > 20 and not has_marker and attempt < self.max_retries - 1:
                        # Soft-fail and retry with context note
                        error_entry = {
                            "code": code,
                            "output": result["output"],
                            "error": "Large result_df detected (>20 rows) but __SAVED_CSV__ marker missing. Add finalize block to save CSV and print marker.",
                            "attempt": attempt + 1
                        }
                        history.append(error_entry)
                        logger.warning("Missing __SAVED_CSV__ marker for large result. Retrying with instruction to add finalize block.")
                        continue
                    return {
                        "code": code,
                        "output": result["output"],
                        "error": None,
                        "attempts": attempt + 1
                    }
                
                # If failed and not last attempt, add error to history for context
                if attempt < self.max_retries - 1:
                    error_entry = {
                        "code": code,
                        "output": result["output"],
                        "error": result["error"],
                        "attempt": attempt + 1
                    }
                    history.append(error_entry)
                    logger.warning(f"Attempt {attempt + 1} failed: {result['error']}. Retrying...")
                else:
                    # Last attempt failed
                    return {
                        "code": code,
                        "output": result["output"],
                        "error": result["error"],
                        "attempts": attempt + 1
                    }
                    
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} exception: {e}")
                if attempt == self.max_retries - 1:
                    return {
                        "code": f"print('Failed after {self.max_retries} attempts')",
                        "output": "",
                        "error": str(e),
                        "attempts": attempt + 1
                    }
        
        # Should not reach here, but just in case
        return {
            "code": "",
            "output": "",
            "error": "Max retries exceeded",
            "attempts": self.max_retries
        }

    async def ainvoke(self, state: dict, config=None, **kwargs) -> dict:
        nl = state.get("command", "").strip()
        if not nl:
            state["error"] = "No 'command' found in state"
            return state

        # init history if missing
        history = state.setdefault("history", [])

        try:
            # Execute with retry logic
            logger.info(f"Processing command: {nl}")
            result = await self.execute_with_retry(nl, history.copy(), state)  # Pass state for metrics tracking

            # Append successful result to history
            entry = {
                "code": result["code"],
                "output": result["output"],
                "error": result["error"],
                "attempts": result.get("attempts", 1)
            }
            history.append(entry)

            # Update state
            state.update({
                "last_code": result["code"],
                "last_output": result["output"],
                "last_error": result["error"],
                "history": history,
                "error": None
            })
            
            if result["error"]:
                logger.warning(f"Final execution failed after {result.get('attempts', 1)} attempts: {result['error']}")
            else:
                logger.info(f"Execution successful after {result.get('attempts', 1)} attempts")
        
        except CancelledException:
            # User cancelled - this is not an error, just exit gracefully
            logger.info("🛑 Code execution cancelled by user - exiting gracefully")
            state["cancelled"] = True
            state["error"] = None  # Clear any error since this was intentional
            # Re-raise to propagate to parent agent
            raise
            
        except Exception as e:
            logger.error(f"CodeAgent error: {e}")
            state["error"] = str(e)

        return state

    def invoke(self, state: dict, config=None, **kwargs) -> dict:
        # sync wrapper
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
