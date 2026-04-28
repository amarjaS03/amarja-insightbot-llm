import os
import json
import logging
import asyncio
import re
import base64
from typing import List, Dict, Any
from pathlib import Path
from dotenv import load_dotenv
from agents.llm_client import llm_call
from langchain_core.runnables import Runnable

from agents.executor import CodeAgent
from agents.analysis_mode import (
    normalize_analysis_mode,
    eda_tasks_per_cycle,
    eda_cycle_count,
    skip_image_analysis,
)
from agents.token_manager import check_token_limit_internal, TokenLimitExceededException

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

EDA_MAX_IMAGES = 10
EDA_MAX_FILES = 10

EDA_SYSTEM_PROMPT = """You are an expert data science EDA (Exploratory Data Analysis) coordinator.
Break down user requests into exactly 5 lightweight, executable subtasks for data analysis.

HARD LIMITS (MUST OBEY):
- Maximum 10 images (PNG/JPG) total across all tasks.
- Maximum 10 data files (CSV, etc.) total across all tasks.
- Plan subtasks so the combined output stays within these limits. Prefer fewer, higher-value visualizations.

IMPORTANT: You must provide detailed business-focused thinking logs throughout your analysis process so users can see your business reasoning and strategic decision-making process. Think like a business analyst, not a technical analyst.
 
Instructions:
- Analyze the user's intent.
- Determine appropriate outputs (graphs, charts, tables, etc.).
- Select simple graphs/charts that are easy to interpret like bar charts, line graphs, pie charts, heatmaps, etc.
- For each visualization, include correct dimensions and facts so results are easy to understand.
- Add proper legends, axis labels, and titles for clarity.
- For each graph/chart/table, include a brief statement of what it shows, how it was created, and why it is relevant; incorporate this brief explanation into the visualization as legends.
- Break the request into small, focused subtasks. Each task should be quick to execute and produce clear output.
- Prefer meaningful tables and high-value summary statistics when speed is important.
 
Response format (JSON):
{
    "thinking_logs": [
        "💼 Understanding business problem and decision-making needs...",
        "📈 Identifying key business metrics and performance indicators...",
        "🎯 Focusing on insights that will impact business strategy...",
        "📋 Planning business-relevant analysis steps...",
        "💡 Designing visualizations for executive decision-making..."
    ],
    "tasks": [
        {
            "task_id": 1,
            "description": "read domain_directory.json and print it",
            "code_instruction": "Read the domain_directory.json file from input_data and print it."
        },
        {
            "task_id": 2,
            "description": "Load dataset",
            "code_instruction": "Load the dataset from the input_data directory based on user request."
        }
    ]
}
 
Additional requirements:
- First task: read domain_directory.json and understand the dataset.
- Check input_data directory for available datasets and their column names.
- Save generated files using the dynamic output_dir path with appropriate names.
- Perform basic data cleaning and cleansing if needed.
 
Important:
First two tasks should be to read the domain_directory.json and load the dataset and then refer plan from query_analysis['plan'].
Make sure to include 4-6 detailed thinking_logs that show your actual reasoning process.
"""

ANALYSIS_SYSTEM_PROMPT = """You are an expert data analyst reviewing EDA task outputs.
Based on the completed EDA tasks and their outputs, determine if more analysis is needed.

HARD LIMITS: Do NOT suggest additional tasks if total outputs would exceed 10 images or 10 files. Prefer sufficient=true when near limits.

You will receive:
- List of completed tasks with their outputs
- Current analysis goal

Respond with JSON:
{
    "sufficient": true/false,
    "reasoning": "explanation of why analysis is sufficient or not",
    "additional_tasks": [
        {
            "task_id": X,
            "description": "task description",
            "code_instruction": "specific instruction"
        }
    ]
}

If sufficient=true, additional_tasks should be empty."""

VISION_SYSTEM_PROMPT = """You are an expert data visualization analyst with the ability to interpret charts, graphs, and visual data representations.

Your role is to analyze generated visualizations and provide insights about:
- Chart types and their appropriateness for the data
- Patterns, trends, and anomalies visible in the visualizations
- Data distribution characteristics
- Relationships between variables
- Outliers and notable data points
- Statistical insights that can be derived from the visual patterns
- Quality and clarity of the visualizations

For each image, provide:
1. Description of what the visualization shows
2. Key insights and patterns observed
3. Statistical or analytical conclusions
4. Any recommendations for further analysis

Be concise but thorough in your analysis. Focus on actionable insights that complement the numerical analysis."""

SYNTHESIS_SYSTEM_PROMPT = """You are an expert data analyst creating concise EDA summaries.
Based on the EDA task outputs, create a brief, insightful summary.

Focus on:
- Key findings and patterns
- Data quality observations
- Statistical insights
- Notable correlations or anomalies
- Clear recommendations based on findings

Keep the summary concise but informative. Structure with clear sections."""

class EDAAgent(Runnable):
    def __init__(self, output_dir):
        self.state = None
        self.output_dir = output_dir
        self.max_iterations = 3  # Dynamically overridden by analysis mode
        # Lazily created. Creating a new CodeAgent per task was extremely slow
        # and could cause memory pressure (each CodeAgent starts a Jupyter kernel).
        self._code_agent = None

    def _get_code_agent(self):
        if self._code_agent is None:
            self._code_agent = CodeAgent()
        return self._code_agent

    def _cleanup_code_agent(self):
        """Best-effort cleanup of the underlying Jupyter kernel."""
        try:
            if self._code_agent and getattr(self._code_agent, "executor", None):
                self._code_agent.executor.cleanup()
        except Exception:
            pass

    def encode_image_to_base64(self, image_path: str):
        """Encode image to base64 for Vision API"""
        try:
            with open(image_path, "rb") as image_file:
                return base64.b64encode(image_file.read()).decode('utf-8')
        except Exception as e:
            logger.error(f"Error encoding image {image_path}: {e}")
            return None

    async def _analyze_single_image(self, img_path: Path, state: dict) -> str | None:
        """Analyze one image and return markdown output."""
        try:
            if img_path.suffix.lower() not in ['.png', '.jpg', '.jpeg']:
                return None

            base64_image = self.encode_image_to_base64(str(img_path))
            if not base64_image:
                return None

            vision_msg = f"""
Analyze this data visualization image generated by the EDA process: {img_path.name}

Please provide:
1. Description of the chart/graph type and what it shows
2. Key patterns, trends, or insights visible
3. Statistical observations
4. Any notable findings or anomalies
5. How this visualization contributes to understanding the data

Be concise but thorough in your analysis.
"""
            token_check = check_token_limit_internal(state, estimated_tokens=600)
            can_proceed = True
            token_message = ""
            should_complete_job = False
            if isinstance(token_check, tuple):
                if len(token_check) >= 1:
                    can_proceed = bool(token_check[0])
                if len(token_check) >= 2:
                    token_message = str(token_check[1])
                if len(token_check) >= 3:
                    should_complete_job = bool(token_check[2])
            else:
                can_proceed = bool(token_check)
                token_message = "Token check returned non-tuple result"

            if not can_proceed:
                if should_complete_job:
                    return f"**{img_path.name}:** Vision analysis skipped early due to token limits."
                state["error"] = f"🚫 PROCESS STOPPED: {token_message}"
                raise TokenLimitExceededException(token_message)

            messages = [
                {"role": "system", "content": VISION_SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": vision_msg},
                        {
                            "type": "input_image",
                            "image_url": f"data:image/{img_path.suffix[1:]};base64,{base64_image}",
                        },
                    ],
                },
            ]
            analysis, usage = await llm_call(messages, max_output_tokens=600)

            state["metrics"]["prompt_tokens"] += usage["input_tokens"]
            state["metrics"]["completion_tokens"] += usage["output_tokens"]
            state["metrics"]["total_tokens"] += usage["input_tokens"] + usage["output_tokens"]
            state["metrics"]["successful_requests"] += 1

            milestone_cb = state.get("milestone_callback", lambda *a, **k: None)
            milestone_cb(
                f"EDA: Vision analyzed image {img_path.name}",
                "eda_vision_image_done",
                {"file": img_path.name, "dependency": "parallelizable", "is_llm_call": True},
            )
            return f"**{img_path.name}:**\n{analysis}"
        except Exception as e:
            logger.error(f"Error analyzing image {img_path}: {e}", exc_info=True)
            return f"**{img_path.name}:** Error analyzing image - {str(e)}"

    async def analyze_images(self, state: dict) -> str:
        """Analyze generated images using Vision API and create combined report"""
        
        # Look for images in the execution_layer/output_data/ directory
        # Ensure we have a Path object so .exists() and .glob() work correctly
        # from pathlib import Path  # local import to avoid any missing import issues in other contexts
        eda_output_dir = Path(self.output_dir)
        image_extensions = ['.png', '.jpg', '.jpeg', '.svg', '.pdf']
        
        found_images = []
        if eda_output_dir.exists():
            for ext in image_extensions:
                found_images.extend(list(eda_output_dir.glob(f"*{ext}")))
        
        # Remove duplicates
        found_images = list(set(found_images))
        
        if not found_images:
            logger.info("No images found for vision analysis")
            return "No visualizations were generated during the analysis."
        
        logger.info(f"Found {len(found_images)} images for vision analysis")
        
        semaphore = asyncio.Semaphore(3)

        async def _with_limit(path: Path):
            async with semaphore:
                return await self._analyze_single_image(path, state)

        results = await asyncio.gather(*[_with_limit(p) for p in found_images])
        vision_analyses = [r for r in results if isinstance(r, str) and r.strip()]
        
        if vision_analyses:
            combined_report = "## Visual Analysis Report\n\n" + "\n\n".join(vision_analyses)
            logger.info("Vision analysis completed successfully")
            return combined_report
        else:
            return "## Visual Analysis Report\n\nNo images could be analyzed."

    def _safe_json(self, content: str, fallback: dict) -> dict:
            """Safely parse JSON returned by the LLM, returning fallback if parsing fails."""
            try:
                return json.loads(content)
            except Exception:
                match = re.search(r'\{.*\}', content, re.DOTALL)
                if match:
                    try:
                        return json.loads(match.group())
                    except Exception:
                        pass
            return fallback


    async def plan_initial_tasks(self, user_command: str, eda_outputs: List[Dict], state: dict) -> Dict[str, Any]:
        """Break down user request into initial EDA subtasks"""
            
        # Context from previous EDA outputs (not code execution details)
        context = ""
        if eda_outputs:
            context_items = []
            for item in eda_outputs[-3:]:  # Last 3 EDA outputs
                context_items.append(f"Task: {item['task']}\nOutput: {item['output'][:800]}...")
            context = "\n\n".join(context_items)

        analysis_mode = normalize_analysis_mode(state.get("analysis_mode"))
        mode_instruction = ""
        if analysis_mode == "slim":
            mode_instruction = """
    Mode: slim
    - Do NOT create tasks that generate images/charts/plots.
    - Focus on meaningful tables, grouped summaries, and high-level insights.
    - Keep tasks lightweight and fast to execute.
"""

        user_msg = f"""
    Previous EDA context:
    {context}

    Current user request:
    {user_command}

    Query analysis:
    {state["query_analysis"]}
    Break this down into exactly 5 lightweight EDA subtasks. Return valid JSON only.
    {mode_instruction}
    """

        try:
            messages = [
                {"role": "system", "content": EDA_SYSTEM_PROMPT},
                {"role": "user", "content": user_msg},
            ]
            content, usage = await llm_call(messages, json_response=True, max_output_tokens=1000)
            if not content:
                content = "{}"

            # Update metrics in state
            state["metrics"]["prompt_tokens"] += usage["input_tokens"]
            state["metrics"]["completion_tokens"] += usage["output_tokens"]
            state["metrics"]["total_tokens"] += usage["input_tokens"] + usage["output_tokens"]
            state["metrics"]["successful_requests"] += 1

            # Robust JSON parsing
            result = self._safe_json(content, self._fallback_plan(user_command))

            # Milestone: EDA plan created
            milestone_cb = state.get("milestone_callback", lambda *a, **k: None)
            task_count = len(result.get("tasks", []))
            milestone_cb(f"EDA: Plan created ({task_count} tasks)", "eda_plan_created", {"task_count": task_count, "dependency": "sequential", "is_llm_call": True})
            
            # Stream LLM thinking logs via progress_callback
            progress_callback = state.get("progress_callback", lambda *args, **kwargs: None)
            thinking_logs = result.get('thinking_logs', [])
            
            for i, log in enumerate(thinking_logs):
                progress_callback(f"🤖 EDA LLM #{i+1}", log, "📊")
                # Small delay to make logs visible
                await asyncio.sleep(0.2)
            
            return result
                
        except Exception as e:
            logger.error(f"Error planning EDA tasks: {e}")
            return self._fallback_plan(user_command)

    def _normalize_cycle_tasks(self, tasks: List[Dict[str, Any]], state: dict, cycle_number: int) -> List[Dict[str, Any]]:
        """
        Enforce exactly 5 EDA tasks per cycle.
        If planner returns fewer, append deterministic filler tasks.
        """
        target = eda_tasks_per_cycle(normalize_analysis_mode(state.get("analysis_mode")))
        analysis_mode = normalize_analysis_mode(state.get("analysis_mode"))
        normalized: List[Dict[str, Any]] = []
        image_task_tokens = (
            "chart",
            "plot",
            "graph",
            "visual",
            "savefig",
            ".png",
            ".jpg",
            ".jpeg",
            ".svg",
            "heatmap",
            "histogram",
            "scatter",
            "line chart",
            "bar chart",
            "pie",
        )

        for t in tasks or []:
            if not isinstance(t, dict):
                continue
            if "description" not in t or "code_instruction" not in t:
                continue
            description = str(t["description"])
            instruction = str(t["code_instruction"])
            if analysis_mode == "slim":
                check_text = f"{description} {instruction}".lower()
                if any(tok in check_text for tok in image_task_tokens):
                    continue
            normalized.append(
                {
                    "task_id": t.get("task_id", len(normalized) + 1),
                    "description": description,
                    "code_instruction": instruction,
                }
            )

        if len(normalized) >= target:
            return normalized[:target]

        if analysis_mode == "slim":
            fillers = [
                ("Validate data quality basics", "Run null, duplicate, and type checks for key columns and save concise findings."),
                ("Generate key KPI table", "Create a compact KPI summary table aligned to the query and save it as CSV."),
                ("Build grouped summary table", "Create grouped aggregation tables by key business dimensions and save outputs."),
                ("Create top/bottom performers table", "Generate top and bottom segment tables for key metrics and save them."),
                ("Summarize high-level insights", "Write a short high-level insight summary in plain language to a text file."),
            ]
        else:
            fillers = [
                ("Validate data quality basics", "Run null, duplicate, and type checks for key columns and save concise findings."),
                ("Generate key KPI summary", "Create a compact KPI summary table aligned to the query and save it."),
                ("Create relevant trend chart", "Generate one relevant chart aligned to the main question and save it with labels and title."),
                ("Create relevant comparison chart", "Generate one comparison chart across key segments and save it with clear legend."),
                ("Summarize cycle insights", "Write a short cycle summary with key insights and recommendations to a text file."),
            ]

        idx = 0
        while len(normalized) < target:
            desc, instr = fillers[idx % len(fillers)]
            normalized.append(
                {
                    "task_id": len(normalized) + 1,
                    "description": f"Cycle {cycle_number}: {desc}",
                    "code_instruction": instr,
                }
            )
            idx += 1

        return normalized

    def _fallback_plan(self, user_command: str) -> Dict[str, Any]:
        """Fallback plan if JSON parsing fails"""
        return {
            "tasks": [
                {
                    "task_id": 1,
                    "description": "Execute user request",
                    "code_instruction": user_command
                }
            ]
        }

    async def analyze_completeness(self, eda_outputs: List[Dict], original_request: str, state: dict) -> Dict[str, Any]:
        """Determine if current analysis is sufficient or needs more tasks"""
        
        outputs_summary = []
        for output in eda_outputs:
            outputs_summary.append(f"Task: {output['task']}\nOutput: {output['output'][:300]}...")

        img_count, file_count = self._count_eda_outputs(state.get("output_dir", self.output_dir))
        
        analysis_msg = f"""
Original request: {original_request}

Completed EDA tasks:
{chr(10).join(outputs_summary)}

Current outputs: {img_count} images, {file_count} data files (max 10 each). Do NOT suggest additional_tasks if limits would be exceeded.

Is this analysis sufficient for the original request? Return valid JSON only.
"""

        try:
            messages = [
                {"role": "system", "content": ANALYSIS_SYSTEM_PROMPT},
                {"role": "user", "content": analysis_msg},
            ]
            content, usage = await llm_call(messages, json_response=True, max_output_tokens=500)
            content = content or ""

            # Update metrics in state
            state["metrics"]["prompt_tokens"] += usage["input_tokens"]
            state["metrics"]["completion_tokens"] += usage["output_tokens"]
            state["metrics"]["total_tokens"] += usage["input_tokens"] + usage["output_tokens"]
            state["metrics"]["successful_requests"] += 1

            json_match = re.search(r'\{.*\}', content, re.DOTALL)

            if json_match:
                return json.loads(json_match.group())
            else:
                return {"sufficient": True, "reasoning": "Analysis complete", "additional_tasks": []}
                
        except Exception as e:
            logger.error(f"Error analyzing completeness: {e}")
            return {"sufficient": True, "reasoning": "Error in analysis", "additional_tasks": []}

    async def synthesize_results(self, eda_outputs: List[Dict], image_paths: List[str], vision_report: str, state: dict) -> str:
        """Create a comprehensive summary from EDA outputs including vision analysis"""
        milestone_cb = state.get("milestone_callback", lambda *a, **k: None)
        milestone_cb("EDA: Building synthesis prompt", "eda_synthesis_prompt_build", {"dependency": "sequential", "is_llm_call": False})
        analysis_mode = normalize_analysis_mode(state.get("analysis_mode"))
        
        outputs_summary = []
        for output in eda_outputs:
            outputs_summary.append(f"Task: {output['task']}\nFindings: {output['output'][:400]}...")

        synthesis_input = f"""
EDA Analysis Results:
{chr(10).join(outputs_summary)}

Generated Visualizations: {len(image_paths)} chart(s)
Chart files: {', '.join(image_paths) if image_paths else 'None'}

Vision Analysis Report:
{vision_report}

Create a comprehensive EDA summary that combines both the numerical analysis results and the visual insights from the charts.
Mode instruction: {"Keep it short and meaningful, focus on high-level insights and tables, and do not include recommendations or implications." if analysis_mode == "slim" else "Provide deep analytical detail, clear insight explanations, and actionable recommendations."}
"""

        try:
            messages = [
                {"role": "system", "content": SYNTHESIS_SYSTEM_PROMPT},
                {"role": "user", "content": synthesis_input},
            ]
            content, usage = await llm_call(messages, max_output_tokens=1000)

            # Update metrics in state
            state["metrics"]["prompt_tokens"] += usage["input_tokens"]
            state["metrics"]["completion_tokens"] += usage["output_tokens"]
            state["metrics"]["total_tokens"] += usage["input_tokens"] + usage["output_tokens"]
            state["metrics"]["successful_requests"] += 1

            milestone_cb("EDA: LLM synthesis done", "eda_synthesis_llm_done", {"dependency": "sequential", "is_llm_call": True})
            return content
            
        except Exception as e:
            logger.error(f"Error synthesizing results: {e}")
            return f"Analysis completed with {len(eda_outputs)} tasks. Summary generation failed: {str(e)}"

    def _count_eda_outputs(self, output_dir: str | Path) -> tuple[int, int]:
        """Count images and data files in output_dir. Returns (image_count, file_count)."""
        out = Path(output_dir) if output_dir else Path()
        if not out.exists():
            return 0, 0
        image_ext = {".png", ".jpg", ".jpeg", ".svg", ".pdf"}
        data_ext = {".csv", ".xlsx", ".xls", ".json", ".parquet"}
        img_count = sum(1 for f in out.iterdir() if f.is_file() and f.suffix.lower() in image_ext)
        file_count = sum(1 for f in out.iterdir() if f.is_file() and f.suffix.lower() in data_ext)
        # Also count image_utils/*.csv
        img_utils = out / "image_utils"
        if img_utils.exists():
            file_count += sum(1 for f in img_utils.iterdir() if f.is_file() and f.suffix.lower() == ".csv")
        return img_count, file_count

    def extract_image_paths(self, code: str, output: str) -> List[str]:
        """Extract potential image file paths from code and output"""
        image_paths = []
        
        # Look for savefig calls in code
        savefig_matches = re.findall(r'plt\.savefig\([\'"]([^\'"]+)[\'"]', code)
        image_paths.extend(savefig_matches)
        
        # Look for common image file extensions in output
        image_extensions = ['.png', '.jpg', '.jpeg', '.svg', '.pdf']
        for ext in image_extensions:
            matches = re.findall(r'([^\s]+' + re.escape(ext) + r')', output)
            image_paths.extend(matches)
        
        return image_paths

    async def execute_task(self, task: Dict, state: dict) -> Dict:
        """Execute a single EDA task using the code agent"""
        code_agent = self._get_code_agent()
        milestone_cb = state.get("milestone_callback", lambda *a, **k: None)
        task_id = task.get("task_id", 0)
        task_desc = task.get("description", "unknown")[:60]

        # Debug logging to verify EDA agent has correct paths
        print(f"🔧 [EDA AGENT] Executing task: {task['description']}")
        print(f"📥 Input dir in state: {state.get('input_dir', 'NOT SET')}")
        print(f"📤 Output dir in state: {state.get('output_dir', 'NOT SET')}")
        print(f"🆔 Job ID in state: {state.get('job_id', 'NOT SET')}")
        
        # Set the command for this specific task on the shared state
        state["command"] = task["code_instruction"]

        # Execute the task on the shared state
        result_state = await code_agent.ainvoke(state)

        # Milestone: Executor done (marks end of step - time taken = duration)
        if result_state.get("last_error"):
            milestone_cb(f"EDA: Executor failed step {task_id}", "eda_executor_failed", {"task_id": task_id, "error": str(result_state["last_error"])[:200], "dependency": "sequential", "is_llm_call": False})
        else:
            milestone_cb(f"EDA: Executor completed step {task_id}", "eda_executor_completed", {"task_id": task_id, "dependency": "sequential", "is_llm_call": False})
  
        # Extract image paths
        new_images = self.extract_image_paths(
            result_state.get("last_code", ""), 
            result_state.get("last_output", "")
        )
        
        # Update main state
        state["history"] = result_state["history"]
        state["last_code"] = result_state["last_code"]
        state["last_output"] = result_state["last_output"]
        state["last_error"] = result_state["last_error"]
        state["image_paths"].extend(new_images)
        
        return {
            "task": task["description"],
            "output": result_state.get("last_output", ""),
            "error": result_state.get("last_error"),
            "success": not result_state.get("last_error")
        }

    async def ainvoke(self, state: dict, config=None, **kwargs) -> dict:
        """Main EDA agent logic with iterative task execution"""
        self.state = state
        self.output_dir = self.output_dir
        original_request = state["command"]
        iteration = 0
        analysis_mode = normalize_analysis_mode(state.get("analysis_mode"))
        question_depth = state.get("question_depth", "medium")
        self.max_iterations = eda_cycle_count(analysis_mode, question_depth)

        # Initialize required state keys
        state.setdefault("eda_outputs", [])
        state.setdefault("image_paths", [])
        state.setdefault("history", [])
        
        try:
            # Step 1: Plan initial tasks
            milestone_cb = state.get("milestone_callback", lambda *a, **k: None)

            logger.info("Planning initial EDA tasks...")
            plan = await self.plan_initial_tasks(original_request, state["eda_outputs"], state)
            
            current_tasks = self._normalize_cycle_tasks(plan.get("tasks", []), state, cycle_number=1)
            logger.info(f"Planned {len(current_tasks)} initial tasks")
            logger.info(f"Plan: {plan}")
            
            # Iterative execution and analysis
            while iteration < self.max_iterations:
                iteration += 1
                logger.info(f"EDA Iteration {iteration}")

                # Check output limits before running tasks
                img_count, file_count = self._count_eda_outputs(state.get("output_dir", self.output_dir))
                if img_count >= EDA_MAX_IMAGES or file_count >= EDA_MAX_FILES:
                    logger.info(f"EDA output limits reached ({img_count} images, {file_count} files). Skipping further tasks.")
                    break

                # Step 2: Execute current tasks
                for task in current_tasks:
                    img_count, file_count = self._count_eda_outputs(state.get("output_dir", self.output_dir))
                    if img_count >= EDA_MAX_IMAGES or file_count >= EDA_MAX_FILES:
                        logger.info(f"EDA limits reached. Skipping remaining tasks.")
                        break

                    logger.info(f"Executing: {task['description']}")

                    task_result = await self.execute_task(task, state)
                    state["eda_outputs"].append(task_result)

                    if task_result["error"]:
                        logger.warning(f"Task failed: {task_result['error']}")

                # Step 3: Analyze if more tasks are needed (LLM call)
                if analysis_mode == "slim":
                    logger.info("Slim mode: single EDA cycle complete")
                    break

                logger.info("Analyzing analysis completeness...")
                completeness = await self.analyze_completeness(state["eda_outputs"], original_request, state)
                milestone_cb("EDA: Completeness analyzed", "eda_completeness_analyzed", {"dependency": "sequential", "is_llm_call": True})
                
                if completeness.get("sufficient", True) or not completeness.get("additional_tasks"):
                    logger.info(f"Analysis sufficient: {completeness.get('reasoning', 'Complete')}")
                    break

                # Skip additional tasks if at output limits
                img_count, file_count = self._count_eda_outputs(state.get("output_dir", self.output_dir))
                if img_count >= EDA_MAX_IMAGES or file_count >= EDA_MAX_FILES:
                    logger.info(f"EDA limits reached. Not adding more tasks.")
                    break

                logger.info(f"Need more analysis: {completeness.get('reasoning', 'Continuing')}")
                current_tasks = self._normalize_cycle_tasks(
                    completeness.get("additional_tasks", []),
                    state,
                    cycle_number=iteration + 1,
                )

                # Update task IDs to avoid conflicts
                max_id = max([len(state["eda_outputs"])], default=0)
                for i, task in enumerate(current_tasks):
                    task["task_id"] = max_id + i + 1

            # Step 4: Vision Analysis (LLM)
            if skip_image_analysis(analysis_mode):
                logger.info("Slim mode: skipping EDA vision image analysis")
                vision_report = "Visual image analysis skipped in slim mode."
                state["vision_analysis"] = vision_report
                milestone_cb("EDA: Vision analysis skipped (slim mode)", "eda_vision_skipped", {"dependency": "sequential", "is_llm_call": False})
            else:
                logger.info("Starting vision analysis of generated images...")
                vision_report = await self.analyze_images(state)
                state["vision_analysis"] = vision_report
                milestone_cb("EDA: Vision analysis done", "eda_vision_complete", {"dependency": "sequential", "is_llm_call": True})

            # Step 5: Generate final synthesis (LLM)
            logger.info("Generating final EDA synthesis...")
            state["eda_summary"] = await self.synthesize_results(
                state["eda_outputs"],
                state["image_paths"],
                vision_report,
                state
            )
            milestone_cb("EDA: Synthesis complete", "eda_synthesis_complete", {"dependency": "sequential", "is_llm_call": False})
            state["error"] = None
            
        except Exception as e:
            logger.error(f"EDA Agent error: {e}")
            state["error"] = f"EDA Agent failed: {str(e)}"
        finally:
            # Ensure we don't leak a Jupyter kernel per request.
            self._cleanup_code_agent()

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
