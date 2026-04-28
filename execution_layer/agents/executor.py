import os
import json
import logging
import asyncio

from dotenv import load_dotenv
from agents.llm_client import llm_call
from langchain_core.runnables import Runnable

from execution_layer.agents.coding_tool import JupyterExecutionTool
from agents.analysis_mode import normalize_analysis_mode, executor_max_attempts
from agents.token_manager import check_token_limit_internal, complete_job_gracefully, TokenLimitExceededException

# load .env
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Updated system prompt with directory specifications and error handling
SYSTEM_PROMPT_WITH_IMAGE_MASTER = """You are a Python data science assistant.
    Convert the user's request into executable Python code.
    Use the JSON list of previous runs (code + output) for context.

    IMPORTANT: You must provide detailed business-focused thinking logs throughout your analysis process so users can see your business reasoning and strategic decision-making process. Think like a business analyst, not just a technical coder.

    IMPORTANT RULES:
    - Only return valid Python code
    - Always use the input directory specified in state['input_dir'] for reading files (e.g., pd.read_pickle(os.path.join(state['input_dir'], '*.pkl')))
    - The input directory is dynamically set per job to access job-specific data files
    - DO NOT assume files. Always read domain_directory.json from state['input_dir'] to discover which dataset files exist (typically .pkl) and load accordingly.
    - Always save plots, charts, outputs to the output directory specified in state['output_dir'] with descriptive names (e.g., plt.savefig(os.path.join(state['output_dir'], 'correlation_between_features.png')))
    - The output directory is dynamically set per job to ensure outputs are saved in job-specific locations
    - IMAGE MASTER + DATASET SNAPSHOT RULES (CRITICAL):
    - There is ONE source of truth for images: a single JSON file at:
        os.path.join(state['output_dir'], 'image_master.json')

    - For EACH image you generate (PNG/JPG/JPEG), you MUST:
        1) Save a dataset snapshot CSV BEFORE generating/saving the image:
        - This snapshot MUST contain the exact DataFrame used for that image (only relevant columns).
        - Naming convention (mandatory):
            If image is '<image_stem>.png' saved under state['output_dir'], then snapshot MUST be:
            os.path.join(state['output_dir'], 'image_utils', '<image_stem>__dataset.csv')
        - Snapshot saving happens regardless of whether image generation later succeeds or fails.
        - Do NOT print the dataset contents to stdout.
        - IMPORTANT: The ONLY datasets considered "used in image generation" are these snapshot CSVs saved under:
            os.path.join(state['output_dir'], 'image_utils', ...)
          You may load source data from state['input_dir'] to build the DataFrame, but the dataset you RECORD for the image
          (data_set_name/data_set_path in image_master.json) MUST ALWAYS be the snapshot CSV under output_dir/image_utils.
        - IMPORTANT: In image_master.json, data_set_name/data_set_path MUST refer to THIS snapshot dataset:
            - data_set_name = '<image_stem>__dataset.csv'
            - data_set_path = absolute path of that snapshot CSV
            (NOT the original .pkl/.csv from input_dir)
        - STRICT: data_set_path MUST resolve inside os.path.join(state['output_dir'], 'image_utils', ...). Never point it anywhere else.
        - IMPORTANT (DO NOT MESS THIS UP): The snapshot CSV is NOT an "image" and MUST NEVER appear as a key/record in image_master.json.
            - Only the actual rendered image file (png/jpg/jpeg) is tracked in image_master.json.
            - Snapshots live on disk under `image_utils/` and are referenced by each image record's data_set_name/data_set_path fields only.

        2) Dynamically READ/UPDATE image_master.json INSIDE YOUR GENERATED CODE (DIRECT CODE ONLY):
        - Load the current image_master.json at the START into a dict (or {} if missing).
        - While generating images, build an in-memory dict of updates for successful images and a list/set of image_names to delete.
        - At the VERY END of the code (one finalization block), apply all deletes + updates and write image_master.json ONCE (atomic write).
        - If an image generation fails (exception), ensure that image name is NOT present in final master:
            remove it if it already existed and do not add it as an update.
        - This must be done via try/except so cleanup is deterministic.
        - STRICT: image_master.json MUST ONLY CONTAIN IMAGE ENTRIES.
            - Keys MUST be the image filename and MUST end with .png, .jpg, or .jpeg (case-insensitive).
            - Never create/update/delete entries for any non-image file (e.g., .csv snapshots, .json, .txt, .html).
        - pseudonymized_columns MUST represent the FINAL column names present in the snapshot CSV (post-transform/rename),
            not the original dataset column names.
            - If you rename a pseudonymized column (e.g., Name -> Name_account), then "Name_account" MUST be included.
            - If you create a derived column that contains pseudonymized values from a pseudonymized column, include the derived column name.
            - Do NOT include columns that are not present in the snapshot CSV.
        - CRITICAL: Do NOT set the "code" field until AFTER the image is successfully saved.
            - On success: set/update only the "code" field (and ensure required fields path/data_set_name/data_set_path/pseudonymized_columns are set).
            - On failure: remove the record for that image name from the final master.
    - image_master.json schema (copy exactly; do not add nulls; use false/empty string defaults):
        ({
        "image_name.png":{
            "path":"abs path",
            "code":"<code used to generate image>",
            "data_set_name":"name of the data set used in code",
            "data_set_path":"abs path of the data set used in code",
            "pseudonymized_columns":["c1","c2"],
            "selected_by_narrator": false
        },
        "image_name_2.png":{...}
        })

    - How to compute pseudonymized_columns dynamically (do NOT hardcode column names):
        - You will be given a dict in the prompt and it is also available at runtime as:
            state_pseudonymized_columns_map = { "<dataset_file_name>": ["col1","col2",...], ... }
        These are the ORIGINAL pseudonymized columns for each source dataset.
        - Keep a variable source_dataset_name = "<dataset_file_name>" for the dataset you loaded from input_dir (e.g., "Opportunity.pkl").
        - Track pseudonymized columns THROUGH transformations:
            - Start with base_pseudo = state_pseudonymized_columns_map.get(source_dataset_name, [])
            - Maintain a list derived_pseudo = [] for any new/rename columns that still contain pseudonymized values.
            Example:
                df["Name_account"] = df["Name"]
                derived_pseudo.append("Name_account")
            Or if you rename columns:
                df = df.rename(columns={"Name": "Name_account"})
                derived_pseudo.append("Name_account")
        - After writing the snapshot CSV, read its header columns, then compute the FINAL list:
            final_candidates = base_pseudo + derived_pseudo
            pseudonymized_columns = case_insensitive_intersection(snapshot_columns, final_candidates)
        IMPORTANT: The resulting pseudonymized_columns MUST be snapshot header column names (post-transform).
        - Store that list as pseudonymized_columns in image_master.json.

    - CRITICAL PATH BUILDING RULES:
    - state dict contains: 'session_id', 'job_id', 'input_dir', 'output_dir'
    - ALWAYS use session-aware paths when building absolute paths for image_master.json:
        * input_dir format: /app/execution_layer/input_data/{session_id}
        * output_dir format: /app/execution_layer/output_data/{session_id}/{job_id}
    - When storing paths in image_master.json, INCLUDE session_id in the path.
        Example CORRECT path: "/app/execution_layer/output_data/{session_id}/{job_id}/image_utils/snapshot.csv"
        Example WRONG path: "/app/execution_layer/output_data/{job_id}/image_utils/snapshot.csv" (missing session_id)
    - Use os.path.abspath() or os.path.join() with state vars to build all paths dynamically.
    - Always store ABSOLUTE paths in image_master.json fields: path, data_set_path.

    - IMPORTANT FORMAT / LINT RULES (to avoid frequent IndentationError):
    - NEVER start a top-level statement with stray leading spaces. No lines like: " df = ..."
    - Use 4-space indentation ONLY inside blocks (if/for/try/with/def/class).
    - Avoid embedding a huge multiline string of the whole script inside the script (this often causes indentation errors).
        If you must store code, store ONLY the minimal plot-specific snippet as a single triple-quoted string,
        and assign it ONLY AFTER a successful savefig.
    - Import os module when saving files: import os
    - Example usage:
    * For plots: plt.savefig(os.path.join(state['output_dir'], 'my_chart.png'))
    * For data: df.to_csv(os.path.join(state['output_dir'], 'processed_data.csv'))
    * For JSON: with open(os.path.join(state['output_dir'], 'results.json'), 'w') as f: json.dump(data, f)
    - PATH BUILDING RULE (CRITICAL):
    - NEVER build file paths using f-strings or string concatenation (this frequently breaks on Windows due to backslashes).
    - ALWAYS build paths using os.path.join(...), using state['input_dir'] and state['output_dir'] variables.
    - Reference existing variables or dataframes from previous executions

    STRICT RELIABILITY / NO-HALLUCINATION RULES (CRITICAL):
    - Never create, assume, infer, or "fill in" any data, names, values, IDs, rows, or file contents that you did not actually compute from loaded data.
    - Never invent file paths or filenames. Only reference files that you actually created in the code or that you verified exist on disk.
    - If you are not 100% sure about a value, do NOT output it. Compute/verify it first, or omit it.
    - Be explicit and deterministic about what gets written:
        - Create directories before writing files.
        - Write files only after variables are computed (no placeholder writes).
        - Prefer atomic writes for JSON (temp + replace) when updating shared artifacts.

    SAVE-TO-DISK INSTEAD OF "KEEPING IN MEMORY" (CRITICAL):
    - If any intermediate DataFrame/result will be reused later in the workflow (e.g., by narrator/report or by a later run),
      you MUST persist it to disk under state['output_dir'] as a CSV (or JSON when appropriate) and then refer to that file path.
    - Do NOT rely on a variable only existing in memory across runs for anything that should appear in outputs.
    - For image-related data: the ONLY dataset recorded in image_master.json MUST be the snapshot CSV under:
        os.path.join(state['output_dir'], 'image_utils', '<image_stem>__dataset.csv')
      (as described above). Use that snapshot as the persisted source-of-truth for the plotted data.

    - For data exploration, make sure to output the result (use print() if needed
    - Do not include explanations or markdown
    - Make the last line an expression that shows the result

    CRITICAL ERROR HANDLING:
    - If previous code failed, analyze the error and generate corrected code
    - Handle common errors like missing files, wrong column names, data type issues
    - IMPORTANT pandas dtype safety:
        - If you create/encounter Categorical columns (e.g., via pd.cut / pd.Categorical), NEVER fill missing values with a new value
          that is not already a category (e.g., do NOT do `cat_col.fillna(0)`).
        - For missing categoricals, either:
            (a) convert to string/object before fill: `s = s.astype("string").fillna("Unknown")`, OR
            (b) add an explicit category then fill: `s = s.cat.add_categories(["Unknown"]).fillna("Unknown")`.
        - Only use `fillna(0)` on numeric Series/DataFrames.
    - Use only standard libraries: pandas, numpy, matplotlib, seaborn, scipy Do NOT use statsmodels, sklearn, scikit-learn.
    - For prediction or forecasting requests: do NOT use ML models. Only read the data, compute trends (e.g., moving averages, growth rates), and describe patterns. No sklearn predictive models.
    - If you see "ModuleNotFoundError" for a module, use os.system('pip install <module>') to install it
    - For missing modules, find alternative approaches with available libraries

    Response format (JSON):
    {
        "thinking_logs": [
            "💼 Understanding business question behind the analysis request...",
            "📈 Planning approach to generate business-relevant insights...",
            "🎯 Selecting methods that will answer key business questions...",
            "💡 Implementing analysis to support business decisions...",
            "📊 Ensuring outputs are business-ready for stakeholders..."
        ],
        "code": "your executable Python code here"
    }

    Make sure to include 4-6 detailed thinking_logs that show your actual reasoning process.
"""


SYSTEM_PROMPT_NO_IMAGE_MASTER = """You are a Python data science assistant.
    Convert the user's request into executable Python code.
    Use the JSON list of previous runs (code + output) for context.

    IMPORTANT: You must provide detailed business-focused thinking logs throughout your analysis process so users can see your business reasoning and strategic decision-making process. Think like a business analyst, not just a technical coder.

    IMPORTANT RULES:
    - Only return valid Python code
    - Always use the input directory specified in state['input_dir'] for reading files.
    - Always save plots/charts/outputs to state['output_dir'] with descriptive names.
    - DO NOT create, read, or update image metadata files for this session:
        - Do NOT create or update image_master.json
        - Do NOT create or write files under image_utils/
        - Do NOT create dataset snapshot CSVs for images
    - IMPORTANT PATH RULE:
        - Never build paths with f-strings or string concatenation.
        - Always use os.path.join(...) with state['input_dir'] and state['output_dir'].
    - Keep code minimal and incremental; do not paste large prior scripts.
    - IMPORTANT pandas dtype safety:
        - If you create/encounter Categorical columns (e.g., via pd.cut / pd.Categorical), NEVER fill missing values with a new value
          that is not already a category (e.g., do NOT do `cat_col.fillna(0)`).
        - For missing categoricals, either:
            (a) convert to string/object before fill: `s = s.astype("string").fillna("Unknown")`, OR
            (b) add an explicit category then fill: `s = s.cat.add_categories(["Unknown"]).fillna("Unknown")`.
        - Only use `fillna(0)` on numeric Series/DataFrames.
    - Use only standard libraries: pandas, numpy, matplotlib, seaborn, scipy. Do NOT use statsmodels, sklearn, scikit-learn.
    - For prediction or forecasting: do NOT use ML. Only read data, compute trends (moving averages, growth rates), and describe patterns.

    Response format (JSON):
    {
        "thinking_logs": [
            "💼 Understanding business question behind the analysis request...",
            "📈 Planning approach to generate business-relevant insights...",
            "🎯 Selecting methods that will answer key business questions...",
            "💡 Implementing analysis to support business decisions...",
            "📊 Ensuring outputs are business-ready for stakeholders..."
        ],
        "code": "your executable Python code here"
    }
"""


class CodeAgent(Runnable):
    def __init__(self):
        # spin up a persistent Jupyter kernel
        self.executor = JupyterExecutionTool()
        self.max_retries = 1

    def _compact_history_for_prompt(self, history: list, *, max_items: int = 4) -> str:
        """
        Reduce prompt bloat and prevent the model from re-printing entire previous scripts.
        We keep only short snippets plus the error context.
        """
        if not history:
            return "[]"
        compact = []
        for item in history[-max_items:]:
            if not isinstance(item, dict):
                continue
            code = (item.get("generated_code") or item.get("code") or "") or ""
            output = (item.get("output") or "") or ""
            err = item.get("error")
            compact.append(
                {
                    "attempt": item.get("attempt", item.get("attempts")),
                    "error": err,
                    "code_snippet": code[:1200],
                    "output_snippet": output[:1200],
                }
            )
        return json.dumps(compact, indent=2, ensure_ascii=False)

    async def nl_to_code(self, nl_command: str, history: list, retry_count: int = 0, state: dict = {}) -> str:
        # pass compact history items as context including errors
        ctx = self._compact_history_for_prompt(history, max_items=4)
        
        retry_context = ""
        if retry_count > 0:
            retry_context = f"\nThis is retry attempt {retry_count}. Previous attempts failed. Analyze the errors in context and generate code to fix the issues."
            

        # Get dynamic paths from state or use sensible defaults.
        # Defaults should respect session-specific inputs and job-specific outputs.
        session_id = state.get('session_id')
        job_id = state.get('job_id')

        if state.get('input_dir'):
            input_dir = state['input_dir']
        else:
            base_input = '/app/execution_layer/input_data'
            input_dir = os.path.join(base_input, session_id) if session_id else base_input

        if state.get('output_dir'):
            output_dir = state['output_dir']
        else:
            base_output = '/app/execution_layer/output_data'
            output_dir = os.path.join(base_output, job_id) if job_id else base_output

        # Defensive: represent paths safely inside prompts across platforms (Windows backslashes, etc.).
        # We still instruct the model to use state['input_dir']/state['output_dir'] at runtime.
        input_dir_display = input_dir.replace("\\", "/")
        output_dir_display = output_dir.replace("\\", "/")
        
        # # Debug logging to verify paths (ASCII-only for Windows consoles)
        # print("[EXECUTOR] Using paths for code generation:")
        # print(f"[EXECUTOR] Input dir: {input_dir}")
        # print(f"[EXECUTOR] Output dir: {output_dir}")
        # print(f"[EXECUTOR] Job ID: {state.get('job_id', 'unknown')}")
        
        user_msg = f"""
            Context (last 5 runs, with errors if any):
            {ctx}

            User request:
            {nl_command}
            {retry_context}

            state_pseudonymized_columns_map (dataset -> [pseudonymized columns]):
            {json.dumps(state.get("pseudonymized_columns_map", {}), ensure_ascii=False)}

            Generate Python code only. 
            CRITICAL: Use these specific paths:
            - For reading files: {input_dir_display}
            - For saving outputs: {output_dir_display}
            
            IMPORTANT: Do NOT hardcode paths using string literals or f-strings. Always use state vars + os.path.join:
            - Example: plt.savefig(os.path.join(state['output_dir'], 'my_chart.png'))
            - Example: df = pd.read_pickle(os.path.join(state['input_dir'], 'Opportunity.pkl'))

            IMPORTANT: Do NOT paste prior code blocks. Generate ONLY the minimal incremental code required for THIS request.
            Assume the Jupyter kernel persists across executions and can reuse variables from previous successful runs.
        """

        if normalize_analysis_mode(state.get("analysis_mode")) == "slim":
            user_msg += """

            SLIM MODE HARD RULES:
            - Do NOT generate or save any images/charts/plots.
            - Do NOT call plt.savefig or write png/jpg/jpeg/svg/pdf outputs.
            - Focus on meaningful tables, grouped summaries, and concise high-level text insights.
            - Save only tabular/text artifacts (csv/json/txt) needed for a fast report.
            """

        # Use image_master prompt when domain has pseudonymized columns (needed for depseudonymization).
        # session_pseudonymized OR pseudonymized_columns_map ensures we populate pseudonymized_columns in image_master.
        use_image_master_prompt = bool(
            state.get("session_pseudonymized") or state.get("pseudonymized_columns_map")
        )
        if normalize_analysis_mode(state.get("analysis_mode")) == "slim":
            use_image_master_prompt = False
        system_prompt = SYSTEM_PROMPT_WITH_IMAGE_MASTER if use_image_master_prompt else SYSTEM_PROMPT_NO_IMAGE_MASTER
        
        try:
            # Check token limit internally before making LLM call (MULTI-USER SAFE)
            can_proceed, token_message, should_complete = check_token_limit_internal(state, estimated_tokens=1000)
            
            # if not can_proceed:
            #     if should_complete:
            #         # Complete job gracefully instead of failing
            #         print(f"[EXECUTOR] {token_message}")
            #         return complete_job_gracefully(state)
            #     else:
            #         # Hard failure (insufficient tokens from start)
            #         state["error"] = f"PROCESS STOPPED: {token_message}"
            #         print(f"[EXECUTOR] {token_message}")
            #         raise TokenLimitExceededException(token_message)
            
            # print(f"[EXECUTOR] {token_message}")
            
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_msg},
            ]
            content, usage = await llm_call(messages, json_response=True, max_output_tokens=8000)
            if not content:
                content = "{}"

            # Update metrics in state if available
            if state and "metrics" in state:
                state["metrics"]["prompt_tokens"] += usage["input_tokens"]
                state["metrics"]["completion_tokens"] += usage["output_tokens"]
                state["metrics"]["total_tokens"] += usage["input_tokens"] + usage["output_tokens"]
                state["metrics"]["successful_requests"] += 1

            tokens_used = usage["input_tokens"] + usage["output_tokens"]
            print(f"[EXECUTOR] Used {tokens_used} tokens (Total so far: {state['metrics']['total_tokens']})")
            
            try:
                result = json.loads(content)
                
                # Stream LLM thinking logs via progress_callback
                progress_callback = state.get("progress_callback", lambda *args, **kwargs: None)
                thinking_logs = result.get('thinking_logs', [])
                
                for i, log in enumerate(thinking_logs):
                    progress_callback(f"🤖 Code Gen #{i+1}", log, "⚙️")
                    # Small delay to make logs visible
                    await asyncio.sleep(0.2)
                
                code = result.get("code", "")
            except json.JSONDecodeError:
                # Fallback to original behavior if JSON parsing fails
                code = content
            
            # Clean up code formatting (for fallback cases)
            if code.startswith("```python"):
                code = code.split("```python")[1]
            if code.startswith("```"):
                code = code.split("```")[1]
            if code.endswith("```"):
                code = code[:-3]
            
            code = code.strip()
            # logger.info(f"Generated code (attempt {retry_count + 1}): {code}")
            return code
            
        except Exception as e:
            logger.error(f"Error generating code: {e}")
            raise RuntimeError(f"Error generating code: {e}") from e

    async def execute_with_retry(self, nl_command: str, history: list, state: dict = {}) -> dict:
        """Execute code with retry logic for error handling"""
        
        for attempt in range(self.max_retries):
            try:
                # Generate code
                generated_code = await self.nl_to_code(nl_command, history, attempt, state)

                # Inject state-derived context variables so generated code can use them reliably.
                # This prevents NameError when the LLM references state_pseudonymized_columns_map.
                try:
                    _pcm = state.get("pseudonymized_columns_map", {})
                    if not isinstance(_pcm, dict):
                        _pcm = {}
                except Exception:
                    _pcm = {}
                injected = (
                    "import json\n"
                    f"state_pseudonymized_columns_map = json.loads({json.dumps(json.dumps(_pcm, ensure_ascii=False))})\n"
                )
                exec_code = injected + "\n" + generated_code
                
                # Execute code
                logger.info(
                    "Executing code (attempt %s). code_chars=%s preview=%r",
                    attempt + 1,
                    len(exec_code),
                    exec_code[:400],
                )
                result = self.executor.execute_code(exec_code)
                logger.info(f"Execution result (attempt {attempt + 1}): {result}")
                
                # If successful, return result
                if result["success"] or not result["error"]:
                    return {
                        "generated_code": generated_code,
                        "exec_code": exec_code,
                        "output": result["output"],
                        "error": None,
                        "attempts": attempt + 1
                    }
                
                # If failed and not last attempt, add error to history for context
                if attempt < self.max_retries - 1:
                    error_entry = {
                        "generated_code": generated_code,
                        "exec_code": exec_code,
                        "output": result["output"],
                        "error": result["error"],
                        "attempt": attempt + 1
                    }
                    history.append(error_entry)
                    logger.warning(f"Attempt {attempt + 1} failed: {result['error']}. Retrying...")
                    # Milestone: Executor failed, retrying
                    milestone_cb = state.get("milestone_callback", lambda *a, **k: None)
                    err_msg = (result.get("error") or "")[:80]
                    milestone_cb(f"EDA: Executor failed (attempt {attempt + 1}), retrying: {err_msg}", "eda_executor_retrying", {"attempt": attempt + 1, "dependency": "sequential", "is_llm_call": False})
                else:
                    # Last attempt failed
                    return {
                        "generated_code": generated_code,
                        "exec_code": exec_code,
                        "output": result["output"],
                        "error": result["error"],
                        "attempts": attempt + 1
                    }
                    
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} exception: {e}")
                if attempt == self.max_retries - 1:
                    return {
                        "generated_code": f"print('Failed after {self.max_retries} attempts')",
                        "exec_code": f"print('Failed after {self.max_retries} attempts')",
                        "output": "",
                        "error": str(e),
                        "attempts": attempt + 1
                    }
        
        # Should not reach here, but just in case
        return {
            "generated_code": "",
            "exec_code": "",
            "output": "",
            "error": "Max retries exceeded",
            "attempts": self.max_retries
        }

    async def ainvoke(self, state: dict, config=None, **kwargs) -> dict:
        nl = state.get("command", "").strip()
        if not nl:
            state["error"] = "No 'command' found in state"
            return state

        configured_retries = state.get("executor_max_retries")
        if isinstance(configured_retries, int) and configured_retries > 0:
            self.max_retries = 1
        elif "analysis_mode" in state:
            mode = normalize_analysis_mode(state.get("analysis_mode"))
            depth = state.get("question_depth", "medium")
            self.max_retries = min(1, executor_max_attempts(mode, depth))

        # init history if missing
        history = state.setdefault("history", [])

        try:
            # Execute with retry logic
            logger.info(f"Processing command: {nl}")
            result = await self.execute_with_retry(nl, history.copy(), state)  # Pass state for metrics tracking

            # Append successful result to history
            entry = {
                "generated_code": result.get("generated_code", ""),
                "exec_code": result.get("exec_code", ""),
                "output": result["output"],
                "error": result["error"],
                "attempts": result.get("attempts", 1)
            }
            history.append(entry)

            # Update state
            state.update({
                "last_code": result.get("generated_code", ""),
                "last_exec_code": result.get("exec_code", ""),
                "last_output": result["output"],
                "last_error": result["error"],
                "history": history,
                "error": None
            })
            
            if result["error"]:
                logger.warning(f"Final execution failed after {result.get('attempts', 1)} attempts: {result['error']}")
            else:
                logger.info(f"Execution successful after {result.get('attempts', 1)} attempts")
            
        except Exception as e:
            logger.error(f"CodeAgent error: {e}")
            state["error"] = str(e)

        return state

    def invoke(self, state: dict, config=None, **kwargs):
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
