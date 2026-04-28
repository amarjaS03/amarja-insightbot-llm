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
from agents.analysis_mode import normalize_analysis_mode, hypothesis_task_count

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

HYPOTHESIS_GENERATION_PROMPT = """You are a data scientist generating testable hypotheses based on EDA Summary.

IMPORTANT: You must provide detailed business-focused thinking logs throughout your analysis process so users can see your business reasoning and strategic decision-making process. Think like a business analyst, not a technical analyst.

## Input Analysis
Based on the EDA summary, generate specific hypotheses that can be tested with statistical analysis and code execution.

## Technical Requirements
- All generated graphs, images, output data subsets, and analysis files must be saved using the dynamic output directory path
- Use appropriate statistical methods and visualizations for hypothesis testing
- Ensure hypotheses are statistically testable and methodologically sound
- Base hypotheses on EDA findings including distributions, correlations, and patterns


## Response Format (JSON)
{
    "thinking_logs": [
        "💼 Reviewing business insights from exploratory analysis...",
        "📈 Spotting patterns that could impact business performance...",
        "🎯 Formulating business hypotheses that drive decisions...",
        "💡 Selecting tests that validate business assumptions...",
        "🏢 Connecting findings to strategic business outcomes..."
    ],
    "hypotheses": [
        {
            "id": 1,
            "hypothesis": "Clear hypothesis statement in simple, non-technical language",
            "rationale": "Business-friendly explanation of why this hypothesis matters",
            "test_approach": "Technical approach for testing (statistical methods, visualizations)",
            "expected_insights": "Plain language description of potential business insights"
        },
        {}
    ]
}

## Hypothesis Generation Criteria
- Generate hypotheses testable through statistical analysis or data visualizations
- Ensure relevance to the user's original query and business context
- Focus on hypotheses that could provide actionable business insights
- Use correct dimensions and measures identified in EDA findings
- Prioritize hypotheses with clear business implications

## Technical Implementation Guidelines
- Utilize appropriate statistical tests (t-tests, chi-square, correlation analysis, etc.)
- Create visualizations that clearly demonstrate hypothesis validation/rejection
- Implement proper statistical significance testing where applicable
- Generate clear data visualizations for hypothesis confirmation

## Visualization Specifications
Restrict to interpretable statistical visualizations:
- Generate clear, labeled visualizations with business-friendly titles and legends
- CRITICAL for exported PNGs: chart titles, subtitles, and suptitles must describe the business question only (e.g. "Correlation: ride distance vs. booking value"). Do NOT put "Hypothesis 1", "Hypothesis 2", hypothesis IDs, or internal test labels in any visible plot text—those are for filenames only.
- Bar charts, line graphs, histograms for distribution analysis
- Scatter plots for correlation testing
- Pie charts for categorical proportion analysis
- Heatmaps for correlation matrices
- Simple statistical comparison charts
- Use correct dimensions and measures identified in EDA findings 
- Use appropriate scales and approximations if necessary
- Avoid complex visualizations (box plots, violin plots, advanced statistical plots)

## Output Language Requirements
CRITICAL: Write all hypothesis statements, rationales, and expected insights in simple, business-friendly language that non-technical stakeholders can understand. Avoid statistical jargon in these fields.


"""

HYPOTHESIS_COMMAND_GENERATION_PROMPT = """You are an expert data scientist creating simple, clear commands for hypothesis testing.

IMPORTANT: You must provide detailed business-focused thinking logs throughout your analysis process so users can see your business reasoning and strategic decision-making process. Think like a business analyst, not a technical analyst.

Your role is to convert a hypothesis into a simple, actionable natural language command.

Create a simple nl command for testing this hypothesis. Include:
1. Which specific data file to load(do not load files from output_data directory)
2. Which columns to use(refer from domain directory) 
3. What statistical test or analysis to do
4. What chart/visualization to create(only pie, bar, line charts and heatmaps)
5. Use "hypothesis_{hypothesis['id']}_" prefix for all saved **file names** on disk only.

CRITICAL — visible chart text (titles, suptitles, labels shown in the PNG):
- Use stakeholder-facing wording only (metrics and dimensions), e.g. "Average booking value by vehicle type" or "Correlation between ride distance and booking value".
- Never instruct code to display "Hypothesis N", hypothesis numbers, or internal IDs on the figure; those belong in filenames, not on the chart.

Response format (JSON):
{
    "thinking_logs": [
        "💼 Understanding business hypothesis and decision impact...",
        "📊 Identifying business-critical data sources and metrics...",
        "💡 Choosing analysis approach for business validation...",
        "📈 Planning executive-friendly visualization strategy...",
        "🎯 Creating actionable business analysis plan..."
    ],
    "command": "simple, clear command in natural language"
}

Make sure to include 4-5 detailed thinking_logs that show your actual reasoning process.
"""

# Add these new prompts at the top of the file
VISION_ANALYSIS_PROMPT = """You are an expert data visualization analyst. 

Your role is to analyze data visualizations and extract key insights from charts, graphs, and plots.

For each image provided:
1. Identify the chart type and what data it represents
2. Describe key patterns, trends, or relationships shown
3. Note any statistical significance or notable findings
4. Identify any anomalies or outliers
5. Assess the quality and clarity of the visualization

Provide clear, objective analysis focusing on what the data shows, not interpretation of business implications.

Return your analysis in a structured format describing what you observe in the visualization."""

HYPOTHESIS_JUDGMENT_PROMPT = """You are an expert statistical analyst providing final judgment on hypothesis testing results.

Your role is to synthesize all evidence (text analysis, data files, and visual analysis) to make a definitive judgment on the hypothesis.

Provide judgment on:
1. Whether the hypothesis is SUPPORTED, REJECTED, or INCONCLUSIVE
2. Strength of evidence (STRONG, MODERATE, WEAK)  
3. Statistical significance and confidence level
4. Key supporting or contradicting evidence
5. Business implications and actionable insights
6. Quality assessment of the analysis approach

Focus on objective statistical evaluation and clear business-friendly conclusions.

Use simple language that non-technical stakeholders can understand."""

class HypothesisAgent(Runnable):
    def __init__(self, output_dir):
        self.output_dir = Path(output_dir)  # Convert to Path object
        print("output_dir in HypothesisAgent", self.output_dir)
        # Lazily created. Creating a new CodeAgent repeatedly is slow and leaks kernels.
        self._code_agent = None

    def _get_code_agent(self):
        if self._code_agent is None:
            self._code_agent = CodeAgent()
        return self._code_agent

    def _cleanup_code_agent(self):
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

    def _generate_default_hypotheses(self, eda_summary: str) -> List[Dict]:
        """Return a minimal list of generic hypotheses when LLM parsing fails"""
        logger.warning("Using default hypotheses due to parsing failure or LLM error.")
        return [
            {
                "id": 1,
                "hypothesis": "There are statistically significant differences in one or more key numerical variables across distinct groups within the dataset.",
                "rationale": "Preliminary exploratory analysis often reveals variation between groups; formal statistical testing is required to determine whether observed differences are significant rather than due to random noise.",
                "test_approach": "Load the dataset, identify a categorical grouping variable, group the data accordingly, apply an appropriate statistical test (e.g., ANOVA or non-parametric equivalent) to a selected numerical variable, and visualize group-level summaries.",
                "expected_insights": "Determine whether meaningful group-level differences exist and identify which groups deviate from overall patterns, supporting deeper investigation or downstream modeling."
            }
        ]
    
    async def generate_hypothesis_command(self, hypothesis: Dict, input_files: List[str], domain_directory: Dict, state: dict) -> str:
        """Generate specific command for hypothesis testing using LLM"""
        
        command_context = f"""
        Hypothesis : {hypothesis}

        Available Files:
        - Input Files (raw data files from app/execution_layer/input_data directory): {input_files}

        Refer to the domain directory: {domain_directory} and understand the data structure.
        Generate only a clear, simple command - no complex instructions.
        """
# - EDA Files(curated by eda agent and saved in output_data/eda directory): {eda_files}
        
        try:
            messages = [
                {"role": "system", "content": HYPOTHESIS_COMMAND_GENERATION_PROMPT},
                {"role": "user", "content": command_context},
            ]
            content, usage = await llm_call(messages, json_response=True, max_output_tokens=400)
            if not content:
                content = "{}"

            # Update metrics in state
            state["metrics"]["prompt_tokens"] += usage["input_tokens"]
            state["metrics"]["completion_tokens"] += usage["output_tokens"]
            state["metrics"]["total_tokens"] += usage["input_tokens"] + usage["output_tokens"]
            state["metrics"]["successful_requests"] += 1
            
            try:
                result = json.loads(content)
                
                # Stream LLM thinking logs via progress_callback
                progress_callback = state.get("progress_callback", lambda *args, **kwargs: None)
                thinking_logs = result.get('thinking_logs', [])
                
                for i, log in enumerate(thinking_logs):
                    progress_callback(f"🤖 Command Gen #{i+1}", log, "⚗️")
                    # Small delay to make logs visible
                    await asyncio.sleep(0.2)
                
                generated_command = result.get("command", "")
            except json.JSONDecodeError:
                # Fallback to original behavior if JSON parsing fails
                generated_command = content.strip()
            
            # logger.info(f"Generated command for hypothesis {hypothesis['id']}: {generated_command[:100]}...")
            
            return generated_command
            
        except Exception as e:
            logger.error(f"Error generating hypothesis command: {e}")
            # Fallback to basic command
            return f"Test hypothesis: {hypothesis['hypothesis']} using statistical analysis and visualization. Save results using the dynamic output directory path."
    
    def get_created_files(self, hypothesis) -> List[str]:
        """Check which expected files were actually created"""
        # Look specifically in app/execution_layer/output_data folder
        created_files = []
        hypothesis_id = hypothesis['id']

        pattern = re.compile(rf"^hypothesis_{hypothesis_id}.*")  # files start with hypothesis_<id>
        output_path = self.output_dir  

        # Check if the output directory exists
        if not output_path.exists():
            logger.warning(f"Output directory does not exist: {output_path}")
            return created_files

        for file_path in output_path.iterdir():
            # print(f"Checking file: {file_path}")
            # for files in file_path.iterdir():
            if file_path.is_file() and pattern.search(file_path.name):
                created_files.append(str(file_path))  # Return full path
                logger.info(f"✅ for hypothesis {hypothesis_id}: {file_path} found")

        return created_files

    async def execute_hypothesis_test(self, hypothesis: Dict, state: dict) -> Dict:
            """Execute hypothesis test with LLM-generated command and enhanced tracking"""            
            # Get file context using dynamic input directory
            input_files = []
            input_dir_str = state.get('input_dir', '/app/execution_layer/input_data')
            input_dir = Path(input_dir_str)
            
            print(f"🔧 [HYPOTHESIS AGENT] Using input dir: {input_dir_str}")
            print(f"🆔 Job ID: {state.get('job_id', 'unknown')}")
            
            if input_dir.exists():
                for filename in os.listdir(input_dir):
                    file_path = input_dir / filename
                    if file_path.is_file():
                        input_files.append(str(file_path))
                        
            domain_directory = {}
            try:
                domain_path = input_dir / "domain_directory.json"
                if domain_path.exists():
                    with open(domain_path, "r") as f:
                        domain_directory = json.load(f)
                    print(f"📁 [HYPOTHESIS AGENT] Domain directory loaded: {len(domain_directory)} entries")
            except Exception as e:
                logger.warning(f"Could not load domain directory from {input_dir}: {e}")
            # print("input_files",input_files)
            # print("domain_directory",domain_directory)  
            milestone_cb = state.get("milestone_callback", lambda *a, **k: None)
            milestone_cb(f"Hypothesis: Generating command for hypothesis {hypothesis['id']}", "hypothesis_command_gen", {"hypothesis_id": hypothesis["id"], "dependency": "parallelizable", "is_llm_call": True})
            # Step 1: Generate specific command using LLM
            logger.info(f"🎯 Generating command for hypothesis {hypothesis['id']}...")
            generated_command = await self.generate_hypothesis_command(
                hypothesis, input_files,domain_directory, state
            )
            
            # Step 2: Pass only the simple generated command to code agent
            simple_command = generated_command
            print("simple_command",simple_command)
            
            milestone_cb(f"Hypothesis: Called executor for hypothesis {hypothesis['id']}", "hypothesis_called_executor", {"hypothesis_id": hypothesis["id"], "dependency": "parallelizable", "is_llm_call": False})
            # Step 3: Execute the test using CodeAgent (reuse a single kernel)
            code_agent = self._get_code_agent()
            state["command"] = simple_command
            result_state = await code_agent.ainvoke(state)
            if result_state.get("last_error"):
                milestone_cb(f"Hypothesis: Executor failed for hypothesis {hypothesis['id']}", "hypothesis_executor_failed", {"hypothesis_id": hypothesis["id"], "dependency": "parallelizable", "is_llm_call": False})
            else:
                milestone_cb(f"Hypothesis: Executor completed for hypothesis {hypothesis['id']}", "hypothesis_executor_completed", {"hypothesis_id": hypothesis["id"], "dependency": "parallelizable", "is_llm_call": False})
            
            # # # Step 4: Extract expected file paths from generated code
            # # generated_code = result_state.get("last_code", "")
            # # expected_files = self.extract_file_paths_from_code(generated_code)
            
            # Step 5: Check which files were actually created
            created_files = self.get_created_files(hypothesis)
            print("created_files",created_files)
            # # Step 6: Update image paths in main state
            # state["image_paths"].extend(created_files)
            
            logger.info(f"✅ Hypothesis {hypothesis['id']} execution complete: {len(created_files)} files created")
            
            return {
                "hypothesis_id": hypothesis["id"],
                "hypothesis": hypothesis["hypothesis"],
                "rationale": hypothesis["rationale"],
                "test_approach": hypothesis["test_approach"],
                "generated_command": generated_command,
                "created_files": created_files,
                # "confirmation_code": generated_code,
                # "confirmation_results": result_state.get("last_output", ""),
                # "confirmation_error": result_state.get("last_error"),
                # "success": not result_state.get("last_error"),
                # "expected_files": expected_files,
                # "execution_output": result_state.get("last_output", "")
            }

    async def generate_hypotheses(self, eda_outputs: List[Dict], eda_summary: str, state: dict) -> List[Dict]:
        """Generate testable hypotheses based on EDA results"""
        
        # Prepare EDA context
        eda_context = f"""
        EDA Summary: {eda_summary}
        Query Analysis Context:{state['query_analysis']}(only for reference, do not give too much attention to it)
        """

        try:
            messages = [
                {"role": "system", "content": HYPOTHESIS_GENERATION_PROMPT},
                {"role": "user", "content": eda_context},
            ]
            content, usage = await llm_call(messages, json_response=True, max_output_tokens=800)
            if not content:
                content = "{}"

            # Update metrics in state
            state["metrics"]["prompt_tokens"] += usage["input_tokens"]
            state["metrics"]["completion_tokens"] += usage["output_tokens"]
            state["metrics"]["total_tokens"] += usage["input_tokens"] + usage["output_tokens"]
            state["metrics"]["successful_requests"] += 1
            
            try:
                result = json.loads(content)
                
                # Stream LLM thinking logs via progress_callback
                progress_callback = state.get("progress_callback", lambda *args, **kwargs: None)
                thinking_logs = result.get('thinking_logs', [])
                
                for i, log in enumerate(thinking_logs):
                    progress_callback(f"🤖 Hypothesis LLM #{i+1}", log, "🔬")
                    # Small delay to make logs visible
                    await asyncio.sleep(0.2)
                
                milestone_cb = state.get("milestone_callback", lambda *a, **k: None)
                milestone_cb("Hypothesis: Hypotheses generated successfully", "hypothesis_generated", {"dependency": "sequential", "is_llm_call": True})
                return result.get("hypotheses", [])
            except json.JSONDecodeError:
                json_match = re.search(r'\{[\s\S]*\}', content, re.MULTILINE)
                if json_match:
                    try:
                        result = json.loads(json_match.group())
                        
                        # Stream LLM thinking logs via progress_callback even for fallback parsing
                        progress_callback = state.get("progress_callback", lambda *args, **kwargs: None)
                        thinking_logs = result.get('thinking_logs', [])
                        
                        for i, log in enumerate(thinking_logs):
                            progress_callback(f"🤖 Hypothesis LLM #{i+1}", log, "🔬")
                            await asyncio.sleep(0.2)

                        milestone_cb = state.get("milestone_callback", lambda *a, **k: None)
                        milestone_cb("Hypothesis: Hypotheses generated (fallback parse)", "hypothesis_generated", {"dependency": "sequential", "is_llm_call": True})
                        return result.get("hypotheses", [])
                    except Exception:
                        pass
            return self._generate_default_hypotheses(eda_summary)   
        except Exception as e:
            logger.error(f"Error generating hypotheses: {e}")
            return self._generate_default_hypotheses(eda_summary)

    async def confirm_hypothesis(self, hypothesis: Dict, state: dict) -> Dict:
        """Confirm/test a single hypothesis using the code agent with enhanced instructions"""
        code_agent = self._get_code_agent()
        
        # Enhanced confirmation instruction with context and directory specification
        confirmation_instruction = f"""
HYPOTHESIS CONFIRMATION TASK:

Hypothesis: {hypothesis['hypothesis']}
Rationale: {hypothesis['rationale']}
Test Approach: {hypothesis['test_approach']}

IMPORTANT: Save ALL outputs using the dynamic output directory path with descriptive names:
- Charts: "hypothesis_{hypothesis['id']}_[description].png"
- Data files: "hypothesis_{hypothesis['id']}_[description].csv"
- Results: "hypothesis_{hypothesis['id']}_results.txt"

EDA Context: {state.get('eda_summary', '')[:500]}...

Create comprehensive code to:
1. Load and prepare the data based on EDA findings
2. Perform the statistical test or analysis specified
3. Create clear visualizations showing test results
4. Calculate statistical significance, confidence intervals, effect sizes
5. Save all charts and data to "execution_layer/output_data" directory
6. Provide clear conclusions about hypothesis support/rejection
7. Include any relevant statistical metrics

Make sure visualizations clearly show whether the hypothesis is supported or rejected.
"""
        
        # Execute the confirmation test
        state["command"] = confirmation_instruction
        result_state = await code_agent.ainvoke(state)
        
        # Extract insights from test results
        insights = result_state.get("last_output", "")
        
        # Update image paths in main state
        if result_state.get("image_paths"):
            state["image_paths"].extend(result_state["image_paths"])
        
        return {
            "hypothesis_id": hypothesis["id"],
            "hypothesis": hypothesis["hypothesis"],
            "rationale": hypothesis["rationale"],
            "test_approach": hypothesis["test_approach"],
            "confirmation_code": result_state.get("last_code", ""),
            "confirmation_results": insights,
            "confirmation_error": result_state.get("last_error"),
            "success": not result_state.get("last_error"),
            "insights": insights,
            "visualization_paths": result_state.get("image_paths", [])
        }

    async def synthesize_hypothesis_results(self,state: dict) -> str:
        """Iteratively read each judge summary file and build a synthesis incrementally.

        We process one file at a time to stay within context limits. After each file we:
        1. Ask the LLM to evaluate support status and produce a JSON finding.
        2. Ask the LLM to extend/refine an overall synthesis (passed back in every step).
        The final synthesis after the last file is returned and the list of findings is
        stored in ``state['hypothesis_findings']``.
        """

        from pathlib import Path
        import re, json, textwrap

                # hypo_dir = "/output_data"
        # Updated to use flat output directory path
        hypo_dir = self.output_dir
        pattern = re.compile(r"^hypothesis_(\d+)_judge_summary\.txt$")

        if not hypo_dir.exists():
            logger.warning("Hypothesis directory not found, skipping synthesis.")
            state["hypothesis_findings"] = []
            return "No hypothesis judge summaries were found."

        # Collect and sort files numerically
        judge_files = [fp for fp in hypo_dir.iterdir() if fp.is_file() and pattern.match(fp.name)]
        judge_files.sort(key=lambda p: int(pattern.match(p.name).group(1)))

        findings = []            # python list holding dicts for each hypothesis
        findings_jsonl = ""      # newline-delimited JSON objects used as compact context
        current_synthesis = ""   # incrementally updated synthesis text returned by LLM

        STEP_PROMPT = textwrap.dedent(
            """
            You are an expert data scientist reviewing *one* hypothesis judge summary at a time.
            TASKS:
            1. Read the judge summary text for the current hypothesis.
            2. Decide whether the hypothesis is SUPPORTED, REJECTED, or INCONCLUSIVE.
               (Look for the analyst's "Hypothesis Status" section or derive from evidence.)
            3. Produce a JSON object called finding with keys:
               • hypothesis_id (int)
               • hypothesis (string, concise)
               • result_status (SUPPORTED/REJECTED/INCONCLUSIVE)
               • result (brief description of the founding)
               • rationale (one plain-English sentence)
            4. Combine ALL findings so far (provided as PREVIOUS_FINDINGS JSONL) with this new finding
               and write an UPDATED synthesis report in simple, business language (max 150 words).

            OUTPUT: Return STRICTLY a JSON object with two keys:
              "finding": <finding JSON>,
              "synthesis": "updated synthesis text"
            Do NOT return anything else.
            """
        )

        for file_path in judge_files:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    summary_text = f.read()
            except Exception as e:
                logger.error(f"Error reading {file_path}: {e}")
                continue

            user_msg = (
                f"PREVIOUS_FINDINGS (JSONL):\n{findings_jsonl if findings_jsonl else 'None'}\n\n"
                f"PREVIOUS_SYNTHESIS:\n{current_synthesis if current_synthesis else 'None'}\n\n"
                f"CURRENT_SUMMARY ({file_path.name}):\n{summary_text}"
            )

            try:
                messages = [
                    {"role": "system", "content": STEP_PROMPT},
                    {"role": "user", "content": user_msg},
                ]
                content, usage = await llm_call(messages, json_response=True, max_output_tokens=600)
                if not content:
                    content = "{}"

                if isinstance(state.get("metrics"), dict):
                    state["metrics"]["prompt_tokens"] += usage["input_tokens"]
                    state["metrics"]["completion_tokens"] += usage["output_tokens"]
                    state["metrics"]["total_tokens"] += usage["input_tokens"] + usage["output_tokens"]
                    state["metrics"]["successful_requests"] += 1

                parsed = json.loads(content)

                finding = parsed.get("finding")
                synthesis_text = parsed.get("synthesis", "")

                if finding:
                    finding["file_path"] = str(file_path)
                    findings.append(finding)
                    findings_jsonl += json.dumps(finding) + "\n"

                current_synthesis = synthesis_text.strip() if synthesis_text else current_synthesis
                milestone_cb = state.get("milestone_callback", lambda *a, **k: None)
                milestone_cb(f"Hypothesis: LLM synthesis step {file_path.name}", "hypothesis_synthesis_step", {"file": file_path.name, "dependency": "sequential", "is_llm_call": True})

            except Exception as e:
                logger.error(f"LLM processing failed for {file_path.name}: {e}")
                continue

        # After all files processed
        # Store results in state for downstream agents
        # state["hypothesis_findings"] = findings  # list of structured findings per hypothesis id
        # state["hypothesis_summary"] = current_synthesis  # combined synthesis text

        # Return a simple dict for immediate access if caller needs it
        return {
            "hypothesis_findings": findings,
            "hypothesis_summary": current_synthesis if current_synthesis else "No synthesis generated."
        }
    
    async def _analyze_text_files(self, created_files: List[str], hypothesis_id: int) -> str:
            """Analyze all text files for the hypothesis"""
            text_analysis = ""
            text_files = [f for f in created_files if f.endswith('.txt')]
            
            for file_path in text_files:
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                        text_analysis += f"\n\nFile: {Path(file_path).name}\nContent: {content[:800]}..."
                except Exception as e:
                    logger.error(f"Error reading text file {file_path}: {e}")
                    text_analysis += f"\n\nFile: {Path(file_path).name}\nError: Could not read file"
            
            return text_analysis if text_analysis else "No text files found for analysis."

    async def _analyze_data_files(self, created_files: List[str], hypothesis_id: int) -> str:
        """Analyze all CSV and data files for the hypothesis"""
        data_analysis = ""
        data_files = [f for f in created_files if f.endswith(('.csv', '.pkl', '.pickle'))]
        
        for file_path in data_files:
            try:
                if file_path.endswith('.csv'):
                    import pandas as pd
                    df = pd.read_csv(file_path)
                    data_analysis += f"\n\nData File: {Path(file_path).name}\n"
                    data_analysis += f"Shape: {df.shape}\n"
                    data_analysis += f"Columns: {list(df.columns)}\n"
                    data_analysis += f"Sample Data:\n{df.head(3).to_string()}\n"
                    
                    # Add basic statistics if numeric columns exist
                    numeric_cols = df.select_dtypes(include=['number']).columns
                    if len(numeric_cols) > 0:
                        data_analysis += f"Statistics:\n{df[numeric_cols].describe().to_string()}\n"
                        
            except Exception as e:
                logger.error(f"Error reading data file {file_path}: {e}")
                data_analysis += f"\n\nData File: {Path(file_path).name}\nError: Could not read file"
        
        return data_analysis if data_analysis else "No data files found for analysis."

    async def _analyze_images_for_hypothesis(self, created_files: List[str], hypothesis_id: int, hypothesis_statement: str, state: dict) -> List[Dict]:
        """Analyze all images using Vision API and return combined analysis"""
        image_files = [f for f in created_files if f.lower().endswith(('.png', '.jpg', '.jpeg'))]
        
        if not image_files:
            return []
        
        image_analyses = []
        
        for img_path in image_files:
            try:
                base64_image = self.encode_image_to_base64(img_path)
                if not base64_image:
                    continue
                
                vision_context = f"""
    Analyze this visualization for Hypothesis {hypothesis_id}:
    "{hypothesis_statement}"

    Image file: {Path(img_path).name}

    Provide detailed analysis of what this chart shows and how it relates to testing the hypothesis.
    """
                
                vision_messages = [
                    {"role": "system", "content": VISION_ANALYSIS_PROMPT},
                    {
                        "role": "user",
                        "content": [
                            {"type": "input_text", "text": vision_context},
                            {
                                "type": "input_image",
                                "image_url": f"data:image/png;base64,{base64_image}",
                            },
                        ],
                    },
                ]
                analysis_text, usage = await llm_call(vision_messages, max_output_tokens=600)

                # Update metrics in state
                state["metrics"]["prompt_tokens"] += usage["input_tokens"]
                state["metrics"]["completion_tokens"] += usage["output_tokens"]
                state["metrics"]["total_tokens"] += usage["input_tokens"] + usage["output_tokens"]
                state["metrics"]["successful_requests"] += 1

                image_analyses.append({
                    "img": Path(img_path).name,
                    "description": analysis_text
                })
                
            except Exception as e:
                logger.error(f"Error analyzing image {img_path}: {e}")
                image_analyses.append({
                    "img": Path(img_path).name,
                    "description": f"Error analyzing image: {str(e)}"
                })
        
        return image_analyses
    
    async def _make_final_judgment(self, hypothesis_id: int, hypothesis_statement: str, generated_command: str,
                             text_analysis: str, data_analysis: str, image_analysis: List[Dict], state: dict) -> str:
        """Make final judgment using all evidence"""
        
        # Prepare image analysis summary
        image_summary = ""
        for img_data in image_analysis:
            image_summary += f"\nImage: {img_data['img']}\nAnalysis: {img_data['description']}\n"
        
        judgment_context = f"""
    HYPOTHESIS JUDGMENT FOR ID: {hypothesis_id}

    Hypothesis Statement: {hypothesis_statement}
    Generated Command: {generated_command}

    EVIDENCE ANALYSIS:

    Text Files Analysis:
    {text_analysis}

    Data Files Analysis:
    {data_analysis}

    Visual Analysis Summary:
    {image_summary if image_summary else "No images analyzed"}

    Based on all the evidence above, provide a comprehensive judgment on this hypothesis.
    """
        
        try:
            messages = [
                {"role": "system", "content": HYPOTHESIS_JUDGMENT_PROMPT},
                {"role": "user", "content": judgment_context},
            ]
            content, usage = await llm_call(messages, max_output_tokens=1000)

            # Update metrics in state
            state["metrics"]["prompt_tokens"] += usage["input_tokens"]
            state["metrics"]["completion_tokens"] += usage["output_tokens"]
            state["metrics"]["total_tokens"] += usage["input_tokens"] + usage["output_tokens"]
            state["metrics"]["successful_requests"] += 1

            return content
            
        except Exception as e:
            logger.error(f"Error creating final judgment for hypothesis {hypothesis_id}: {e}")
            return f"Error creating judgment for hypothesis {hypothesis_id}: {str(e)}"
    
    async def _save_judgment_summary(self, hypothesis_id: int, hypothesis_statement: str, generated_command: str,
                               text_analysis: str, data_analysis: str, image_analysis: List[Dict], final_judgment: str):
        """Save comprehensive judgment summary to file"""
        
        # base_path = Path(self.output_dir)
        # if not base_path.exists():
        #     alt = Path('/output_data')
        #     if alt.exists():
        #         base_path = alt
        summary_path = self.output_dir / f"hypothesis_{hypothesis_id}_judge_summary.txt"
        
        try:
            with open(summary_path, 'w', encoding='utf-8') as f:
                f.write(f"HYPOTHESIS {hypothesis_id} JUDGMENT SUMMARY\n")
                f.write("="*60 + "\n\n")
                
                f.write(f"HYPOTHESIS STATEMENT:\n{hypothesis_statement}\n\n")
                f.write(f"GENERATED COMMAND:\n{generated_command}\n\n")
                
                f.write("EVIDENCE ANALYSIS:\n")
                f.write("-"*30 + "\n\n")
                
                f.write("TEXT FILES ANALYSIS:\n")
                f.write(text_analysis + "\n\n")
                
                f.write("DATA FILES ANALYSIS:\n")
                f.write(data_analysis + "\n\n")
                
                f.write("VISUAL ANALYSIS:\n")
                for img_data in image_analysis:
                    f.write(f"Image: {img_data['img']}\n")
                    f.write(f"Description: {img_data['description']}\n\n")
                
                f.write("FINAL JUDGMENT:\n")
                f.write("-"*20 + "\n")
                f.write(final_judgment + "\n\n")
                
                f.write(f"Analysis completed at: {asyncio.get_event_loop().time()}\n")
            
            logger.info(f"✅ Judgment summary saved: {summary_path}")
            
        except Exception as e:
            logger.error(f"Error saving judgment summary for hypothesis {hypothesis_id}: {e}")

    async def judge_hypothesis(self, hypothesis_result: Dict, state: dict) -> None:
        """Judge hypothesis by ID - only analyze files for this specific hypothesis"""
        
        hypothesis_id = hypothesis_result["hypothesis_id"]
        hypothesis_statement = hypothesis_result.get("hypothesis", "")
        generated_command = hypothesis_result.get("generated_command", "")
        created_files = hypothesis_result.get("created_files", [])
        
        logger.info(f"⚖️ Judging Hypothesis {hypothesis_id} - Found {len(created_files)} specific files")
        if not created_files:
            logger.warning(f"No files found for hypothesis {hypothesis_id}")
            # base_path = Path(self.output_dir)
            # if not base_path.exists():
            #     alt = Path('/output_data')
            #     if alt.exists():
            #         base_path = alt
            summary_path = self.output_dir / f"hypothesis_{hypothesis_id}_judge_summary.txt"
            with open(summary_path, 'w', encoding='utf-8') as f:
                f.write(f"Hypothesis {hypothesis_id} Judgment Summary\n")
                f.write("="*50 + "\n")
                f.write(f"Hypothesis: {hypothesis_statement}\n")
                f.write("Status: INCONCLUSIVE - No analysis files generated\n")
            return
        # Step 1: Analyze text files
        text_analysis = await self._analyze_text_files(created_files, hypothesis_id)
        
        # Step 2: Analyze CSV/data files  
        data_analysis = await self._analyze_data_files(created_files, hypothesis_id)
        
        # Step 3: Analyze images using Vision API
        image_analysis = await self._analyze_images_for_hypothesis(created_files, hypothesis_id, hypothesis_statement, state)
        # Step 4: Make final judgment using all evidence
        final_judgment = await self._make_final_judgment(
            hypothesis_id, hypothesis_statement, generated_command,
            text_analysis, data_analysis, image_analysis, state
        )
        # Step 5: Save comprehensive summary
        await self._save_judgment_summary(
            hypothesis_id, hypothesis_statement, generated_command,
            text_analysis, data_analysis, image_analysis, final_judgment
        )
        logger.info(f"✅ Hypothesis {hypothesis_id} judgment complete - Summary saved")

    async def ainvoke(self, state: dict, config=None, **kwargs) -> dict:
        """Main hypothesis agent logic with enhanced flow: Generation → Confirmation → Vision → Synthesis"""
        milestone_cb = state.get("milestone_callback", lambda *a, **k: None)
        analysis_mode = normalize_analysis_mode(state.get("analysis_mode"))
        question_depth = state.get("question_depth", "medium")

        try:
            if analysis_mode == "slim":
                logger.info("Slim mode: hypothesis stage skipped")
                state["hypothesis_findings"] = []
                state["hypothesis_summary"] = "Hypothesis stage intentionally skipped in slim mode."
                milestone_cb("Hypothesis: Skipped (slim mode)", "hypothesis_skipped", {"dependency": "sequential", "is_llm_call": False})
                return state

            # Step 1: Generate hypotheses based on EDA results (LLM)
            logger.info("🧬 Generating hypotheses...")
            hypotheses = await self.generate_hypotheses(
                state["eda_outputs"],
                state["eda_summary"],
                state
            )
        
            if not hypotheses:
                logger.warning("No hypotheses generated")
                state["hypothesis_findings"] = []
                return state
            
            target_count = hypothesis_task_count(analysis_mode, question_depth)
            if target_count > 0:
                if len(hypotheses) > target_count:
                    hypotheses = hypotheses[:target_count]
                while len(hypotheses) < target_count:
                    next_id = len(hypotheses) + 1
                    hypotheses.append({
                        "id": next_id,
                        "hypothesis": f"There is a meaningful relationship in key business metrics relevant to the query (Hypothesis {next_id}).",
                        "rationale": "This tests whether the observed pattern is consistent enough to support business decisions.",
                        "test_approach": "Use an appropriate statistical comparison/correlation test and a simple chart to validate the pattern.",
                        "expected_insights": "Clarifies whether this potential relationship should influence decisions."
                    })
            milestone_cb(f"Hypothesis: Created {len(hypotheses)} hypotheses", "hypothesis_plan_created", {"count": len(hypotheses), "dependency": "sequential", "is_llm_call": True})
            logger.info(f"Generated {len(hypotheses)} hypotheses")
            logger.info(f"Generated hypotheses: {(hypotheses)}")
            
#             hypotheses = [
#   {
#     "id": 1,
#     "hypothesis": "Top-performing branches (PANVEL and NEW PANVEL) have significantly higher average weekly gold loan disbursements compared to low-performing branches (khopoli and NERAL).",
#     "rationale": "Understanding the performance gap between branches helps allocate resources efficiently and identify successful strategies that can be replicated across other branches.",
#     "test_approach": "Perform a t-test comparing the mean weekly disbursement amounts of top-performing branches versus low-performing branches. Visualize weekly disbursement distributions using bar charts for each branch.",
#     "expected_insights": "This will clarify how large the performance differences are and whether the top branches' higher disbursements are statistically significant, guiding branch-level strategy and investment."
#   },
#   {
#     "id": 2,
#     "hypothesis": "There is a significant seasonal pattern in gold loan disbursements, with a peak in May 2025 and a sharp decline in June and July 2025 across all branches.",
#     "rationale": "Identifying seasonal trends can help the bank plan marketing campaigns, staffing, and inventory to better match customer demand cycles.",
#     "test_approach": "Analyze monthly disbursement totals across branches using line graphs to visualize trends. Use repeated measures ANOVA or time series decomposition to test for significant seasonality.",
#     "expected_insights": "Confirming seasonality will enable proactive planning around peak and low periods, improving operational efficiency and customer service."
#   }
# ]

            # Step 2: Execute and judge each hypothesis
            # hypothesis_results = []
            
            for hypothesis in hypotheses:  # Limit to 2 hypotheses for testing
                logger.info(f"🔬 Testing hypothesis {hypothesis['id']}: {hypothesis['hypothesis'][:50]}...")

                # Step 2a: Execute the test with LLM-generated command (executor - not LLM)
                execution_result = await self.execute_hypothesis_test(hypothesis, state)
                milestone_cb(f"Hypothesis: Test executed {hypothesis['id']}", "hypothesis_test_executed", {"hypothesis_id": hypothesis["id"], "dependency": "parallelizable", "is_llm_call": False})
                
                # Step 2b: Judge the results by hypothesis ID (LLM)
                await self.judge_hypothesis(execution_result, state)
                milestone_cb(f"Hypothesis: Judged {hypothesis['id']}", "hypothesis_judged", {"hypothesis_id": hypothesis["id"], "dependency": "sequential", "is_llm_call": True})

            # step 3: Synthesize final results (LLM)
            synthesis_result = await self.synthesize_hypothesis_results(state)
            hypothesis_findings = synthesis_result["hypothesis_findings"]
            hypothesis_summary = synthesis_result["hypothesis_summary"]
            state["hypothesis_findings"] = hypothesis_findings  # list of structured findings per hypothesis id
            state["hypothesis_summary"] = hypothesis_summary  # combined synthesis text
            milestone_cb("Hypothesis: Synthesis done", "hypothesis_synthesis_complete", {"dependency": "sequential", "is_llm_call": True})

            # # # Update state with hypothesis results
            
            # # Step 2: Confirm/test hypotheses
            # logger.info(f"Confirming {len(hypotheses)} hypotheses...")
            # hypothesis_findings = []
            
            # for hypothesis in hypotheses[:3]:  # Limit to 3 hypotheses to avoid too many tests
            #     logger.info(f"Confirming hypothesis: {hypothesis['hypothesis']}")
            #     finding = await self.confirm_hypothesis(hypothesis, state)
            #     hypothesis_findings.append(finding)
                
            #     # Extract patterns from successful tests
            #     if finding["success"] and finding["confirmation_results"]:
            #         # Simple pattern extraction (could be enhanced)
            #         patterns = re.findall(r'correlation|trend|pattern|significant|increase|decrease|supported|rejected', 
            #                             finding["confirmation_results"].lower())
            #         if patterns:
            #             state.setdefault("patterns_found", []).extend(patterns)
            
            # state["hypothesis_findings"] = hypothesis_findings
            
            # # Step 3: Vision Analysis of hypothesis images
            # logger.info("Starting vision analysis of hypothesis testing images...")
            # vision_report = await self.analyze_hypothesis_images(state)
            # state["hypothesis_vision_analysis"] = vision_report

            # # Step 4: Generate comprehensive synthesis
            # logger.info("Generating hypothesis testing synthesis...")
            # state["hypothesis_synthesis"] = await self.synthesize_hypothesis_results(
            #     hypothesis_findings,
            #     vision_report,
            #     state
            # )
            
            # logger.info(f"Hypothesis testing completed. {len(hypothesis_findings)} hypotheses tested.")
            
        except Exception as e:
            logger.error(f"Hypothesis Agent error: {e}")
            state["error"] = f"Hypothesis Agent failed: {str(e)}"
        finally:
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
