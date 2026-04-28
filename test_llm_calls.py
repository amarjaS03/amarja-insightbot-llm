"""
Full LLM call tester — run one at a time.
Uncomment ONE call at the bottom, run, then move to the next.
"""
import asyncio, json
from execution_layer.utils.llm_core import call_llm
from execution_layer.agents.data_analysis_agent import create_query_analysis_prompt, load_domain_directory
from dotenv import load_dotenv
load_dotenv()

from execution_layer.utils.llm_core import llm_call

# ── Import all prompts directly from agent files ────────────
from execution_layer.agents.data_analysis_agent import create_query_analysis_prompt, load_domain_directory
from execution_layer.agents.eda_agent import EDA_SYSTEM_PROMPT, ANALYSIS_SYSTEM_PROMPT, VISION_SYSTEM_PROMPT, SYNTHESIS_SYSTEM_PROMPT
from execution_layer.agents.hypothesis_agent import HYPOTHESIS_GEN_AND_CMD_PROMPT, HYPOTHESIS_JUDGMENT_PROMPT, VISION_ANALYSIS_PROMPT as HYP_VISION_PROMPT
from execution_layer.agents.narrator_agent import FINAL_SYNTHESIS_PROMPT, VISION_ANALYSIS_PROMPT as NAR_VISION_PROMPT

STATE = {"metrics": {}}  # minimal state for all calls

INPUT_DIR = r"c:\Users\Zing56\Desktop\insightbot-backend\execution_layer\local_data\anonymous\no_session\input_data"

# ════════════════════════════════════════════════════════════
# CALL 1 — DATA_ANALYSIS_AGENT  (data_analysis_agent.py:266)
# ════════════════════════════════════════════════════════════
# async def test_1_data_analysis_agent():
#     print("\n" + "="*60)
#     print("CALL 1: DATA_ANALYSIS_AGENT")
#     print("="*60)

#     domain_directory = load_domain_directory(INPUT_DIR)
#     system_prompt = create_query_analysis_prompt(domain_directory)

#     user_query = "What are demand patterns for ride bookings across days and times?"
#     content = await call_llm(
#                 SYS_PROMPT=system_prompt,
#                 USER_ANALYSIS_PROMPT="User query",
#                 USER_PROMPT=user_query,
#                 json_output=True,
#                 max_tokens=2000,
#             )
#     if not content:
#                 content = "{}"

#     result = json.loads(content)
#     print("OUTPUT:", content[:500])
#     result = json.loads(content)
#     print("\nParsed keys:", list(result.keys()))
#     return result


# # ════════════════════════════════════════════════════════════
# # CALL 2 — EDA_PLAN  (eda_agent.py:298)
# # ════════════════════════════════════════════════════════════
# async def test_2_eda_plan():
#     print("\n" + "="*60)
#     print("CALL 2: EDA_PLAN")
#     print("="*60)

#     user_msg = """
#     Previous EDA context: None

#     Current user request:
#     Perform EDA analysis: What are demand patterns for ride bookings across days and times?

#     Query analysis:
#     {"user_intent": "Understand ride booking demand by day and time", "sub_queries": ["bookings by weekday", "bookings by hour"]}

#     Break this down into exactly 5 lightweight EDA subtasks. Return valid JSON only.
#     """

#     content = await llm_call(
#         input_data=[
#             {"role": "system", "content": EDA_SYSTEM_PROMPT},
#             {"role": "user",   "content": user_msg}
#         ],
#         state=STATE,
#         caller_label="EDA_PLAN",
#         max_tokens=1500,
#     )
#     print("OUTPUT:", content[:500])
#     result = json.loads(content)
#     print("\nTasks planned:", len(result.get("tasks", [])))
#     return result


# # ════════════════════════════════════════════════════════════
# # CALL 3 — CODE_EXECUTOR  (executor.py:360)
# # ════════════════════════════════════════════════════════════
# async def test_3_code_executor():
#     print("\n" + "="*60)
#     print("CALL 3: CODE_EXECUTOR")
#     print("="*60)

#     # Import system prompt from executor directly
#     from execution_layer.agents.executor import SYSTEM_PROMPT_NO_IMAGE_MASTER

#     user_msg = f"""
#     input_dir: {INPUT_DIR}
#     output_dir: c:/Users/Zing56/Desktop/insightbot-backend/execution_layer/local_data/anonymous/no_session/output_data/test-llm-calls
#     command: Load domain_directory.json from input_dir and print the dataset names and columns.
#     history: []
#     """

#     content = await llm_call(
#         input_data=[
#             {"role": "system", "content": SYSTEM_PROMPT_NO_IMAGE_MASTER},
#             {"role": "user",   "content": user_msg}
#         ],
#         state=STATE,
#         caller_label="CODE_EXECUTOR",
#     )
#     print("OUTPUT:", content[:500])
#     result = json.loads(content)
#     print("\nCode snippet:", result.get("code", "")[:200])
#     return result


# # ════════════════════════════════════════════════════════════
# # CALL 4 — EDA_COMPLETENESS  (eda_agent.py:428)
# # ════════════════════════════════════════════════════════════
# async def test_4_eda_completeness():
#     print("\n" + "="*60)
#     print("CALL 4: EDA_COMPLETENESS")
#     print("="*60)

#     analysis_msg = """
# Original request: What are demand patterns for ride bookings across days and times?

# Completed EDA tasks:
# Task: Load domain_directory.json
# Output: Dataset has 100k rows with columns: Date, Time, Booking ID, Booking Status...

# Task: Plot bookings by weekday
# Output: Friday has highest bookings (5100), Sunday lowest (2800)...

# Current outputs: 2 images, 3 data files (max 10 each).
# Is this analysis sufficient? Return valid JSON only.
# """

#     content = await llm_call(
#         input_data=[
#             {"role": "system", "content": ANALYSIS_SYSTEM_PROMPT},
#             {"role": "user",   "content": analysis_msg}
#         ],
#         state=STATE,
#         caller_label="EDA_COMPLETENESS",
#     ) or ""
#     print("OUTPUT:", content[:500])
#     result = json.loads(content)
#     print("\nSufficient:", result.get("sufficient"))
#     return result


# # ════════════════════════════════════════════════════════════
# # CALL 5 — EDA_SYNTHESIS  (eda_agent.py:471)
# # ════════════════════════════════════════════════════════════
# async def test_5_eda_synthesis():
#     print("\n" + "="*60)
#     print("CALL 5: EDA_SYNTHESIS")
#     print("="*60)

#     synthesis_input = """
# EDA Analysis Results:
# Task: Load dataset
# Findings: 100k rows, 21 columns, no missing dates...

# Task: Bookings by weekday
# Findings: Friday=5100, Monday=4200, Sunday=2800...

# Task: Bookings by hour
# Findings: Peak at 8-10 AM (3200 bookings), low at 3-5 AM (120 bookings)...

# Generated Visualizations: 2 chart(s)
# Chart files: bookings_by_weekday.png, bookings_by_hour.png

# Vision Analysis Report: Charts show clear temporal demand patterns...

# Create a comprehensive EDA summary combining numerical and visual insights.
# """

#     content = await llm_call(
#         input_data=[
#             {"role": "system", "content": SYNTHESIS_SYSTEM_PROMPT},
#             {"role": "user",   "content": synthesis_input}
#         ],
#         state=STATE,
#         caller_label="EDA_SYNTHESIS",
#     )
#     print("OUTPUT:", content[:500])
#     return content


# # ════════════════════════════════════════════════════════════
# # CALL 6 — HYPOTHESIS_GEN_CMD  (hypothesis_agent.py:352)
# # ════════════════════════════════════════════════════════════
# async def test_6_hypothesis_gen_cmd():
#     print("\n" + "="*60)
#     print("CALL 6: HYPOTHESIS_GEN_CMD")
#     print("="*60)

#     eda_context = f"""
# EDA Summary:
# Friday has highest bookings. Peak demand 8-10 AM on weekdays. 
# Weekend late-night demand is secondary peak.

# User Intent: Understand ride booking demand by day and time

# Available data files: ['Ncr_Ride_Bookings.pkl']
# Domain directory: {{"data_set_files": {{"Ncr_Ride_Bookings.pkl": {{"columns": ["Date","Time","Booking ID","Booking Status","Vehicle Type"]}}}}}}

# Generate exactly 3 DISTINCT hypotheses with test commands.
# """

#     content = await llm_call(
#         input_data=[
#             {"role": "system", "content": HYPOTHESIS_GEN_AND_CMD_PROMPT},
#             {"role": "user",   "content": eda_context}
#         ],
#         state=STATE,
#         caller_label="HYPOTHESIS_GEN_CMD",
#     )
#     print("OUTPUT:", content[:500])
#     result = json.loads(content)
#     print("\nHypotheses generated:", len(result.get("hypotheses", [])))
#     return result


# # ════════════════════════════════════════════════════════════
# # CALL 7 — HYPOTHESIS_JUDGE  (hypothesis_agent.py:623)
# # ════════════════════════════════════════════════════════════
# async def test_7_hypothesis_judge():
#     print("\n" + "="*60)
#     print("CALL 7: HYPOTHESIS_JUDGE")
#     print("="*60)

#     judgment_context = """
# HYPOTHESIS JUDGMENT FOR ID: 1

# Hypothesis Statement: Weekday morning bookings (8-10 AM) are significantly higher than other time periods.
# Generated Command: Load Ncr_Ride_Bookings.pkl, extract hour from Time, run chi-square test by hour bucket.

# EVIDENCE ANALYSIS:

# Text Files Analysis:
# Chi-square statistic: 245.3, p-value: 0.00001
# Morning peak (8-10 AM): 3200 bookings vs average 1800

# Data Files Analysis:
# hypothesis_1_chi_square_results.csv
# Shape: (24, 3), Columns: [hour, booking_count, pct_of_total]
# Sample: hour=8, booking_count=1650, pct_of_total=8.2

# Visual Analysis Summary:
# Image: hypothesis_1_line_chart.png
# Analysis: Line chart shows clear peak at hours 8-10, trough at 3-5 AM.

# Based on all evidence above, provide a comprehensive judgment.
# """

#     content = await llm_call(
#         input_data=[
#             {"role": "system", "content": HYPOTHESIS_JUDGMENT_PROMPT},
#             {"role": "user",   "content": judgment_context}
#         ],
#         state=STATE,
#         caller_label="HYPOTHESIS_JUDGE",
#     )
#     print("OUTPUT:", content[:500])
#     return content


# # ════════════════════════════════════════════════════════════
# # CALL 8 — HYPOTHESIS_SYNTH  (hypothesis_agent.py:484)
# # ════════════════════════════════════════════════════════════
# async def test_8_hypothesis_synth():
#     print("\n" + "="*60)
#     print("CALL 8: HYPOTHESIS_SYNTH")
#     print("="*60)

#     BATCH_PROMPT = """
# You are an expert data scientist reviewing ALL hypothesis judge summaries at once.
# For EACH summary, produce a finding object with keys:
#   hypothesis_id (int), hypothesis (string), result_status (SUPPORTED/REJECTED/INCONCLUSIVE),
#   result (brief description), rationale (one sentence).
# Then write an overall synthesis in simple business language (max 200 words).

# Return STRICTLY JSON: { "findings": [<finding>, ...], "synthesis": "..." }
# """

#     user_msg = """
# === hypothesis_1_judge_summary.txt ===
# HYPOTHESIS 1 JUDGMENT SUMMARY
# Hypothesis: Weekday morning demand peaks at 8-10 AM
# FINAL JUDGMENT:
# SUPPORTED with STRONG evidence. Chi-square p<0.001. Morning bookings 40% above average.

# === hypothesis_2_judge_summary.txt ===
# HYPOTHESIS 2 JUDGMENT SUMMARY
# Hypothesis: Friday has highest weekly demand
# FINAL JUDGMENT:
# SUPPORTED with MODERATE evidence. Friday 18% above weekly average.
# """

#     content = await llm_call(
#         input_data=[
#             {"role": "system", "content": BATCH_PROMPT},
#             {"role": "user",   "content": user_msg}
#         ],
#         state=STATE,
#         caller_label="HYPOTHESIS_SYNTH",
#     )
#     print("OUTPUT:", content[:500])
#     result = json.loads(content)
#     print("\nFindings:", len(result.get("findings", [])))
#     return result


# # ════════════════════════════════════════════════════════════
# # CALL 9 — NARRATOR_FRAME  (narrator_agent.py:543)
# # ════════════════════════════════════════════════════════════
# async def test_9_narrator_frame():
#     print("\n" + "="*60)
#     print("CALL 9: NARRATOR_FRAME")
#     print("="*60)

#     FRAME_PROMPT = (
#         "You are a senior strategy consultant preparing a final executive report.\n"
#         "Inputs: user query, hypothesis summary & findings, EDA summary, and a list of all files.\n"
#         "Tasks:\n"
#         "1. Write a concise narrative frame that sets the story.\n"
#         "2. Select up to 10 files that best support the report.\n"
#         "CRITICAL: selected_files MUST be exact strings from all_files.\n"
#         "Return JSON: { 'frame_text': str, 'selected_files': [paths] }"
#     )

#     payload = {
#         "original_query": "What are demand patterns for ride bookings?",
#         "report_verbosity": "full",
#         "hypothesis_summary": "Morning weekday demand is 40% higher than average.",
#         "hypothesis_findings": [
#             {"hypothesis_id": 1, "hypothesis": "Weekday morning peak", "result_status": "SUPPORTED", "result": "40% higher", "rationale": "p<0.001"}
#         ],
#         "eda_summary": "100k rows. Friday peak. Morning surge 8-10 AM.",
#         "all_files": [
#             "output_data/bookings_by_weekday.csv",
#             "output_data/bookings_by_hour.csv",
#             "output_data/heatmap.png",
#             "output_data/hypothesis_1_results.csv",
#         ]
#     }

#     content = await llm_call(
#         input_data=[
#             {"role": "system", "content": FRAME_PROMPT},
#             {"role": "user",   "content": json.dumps(payload, indent=2)}
#         ],
#         state=STATE,
#         caller_label="NARRATOR_FRAME",
#         max_tokens=2000,
#     )
#     print("OUTPUT:", content[:500])
#     result = json.loads(content)
#     print("\nframe_text:", result.get("frame_text", "")[:100])
#     print("selected_files:", result.get("selected_files", []))
#     return result


# # ════════════════════════════════════════════════════════════
# # CALL 10 — NARRATOR_REPORT  (narrator_agent.py:758)
# # ════════════════════════════════════════════════════════════
# async def test_10_narrator_report():
#     print("\n" + "="*60)
#     print("CALL 10: NARRATOR_REPORT")
#     print("="*60)

#     context = {
#         "original_query": "What are demand patterns for ride bookings?",
#         "current_date": "2026-04-17",
#         "analysis_mode": "deep_dive",
#         "report_verbosity": "full",
#         "frame_text": "Ride demand peaks on weekday mornings. Friday drives highest weekly volume.",
#         "file_analyses": [
#             {
#                 "file": "bookings_by_weekday.csv",
#                 "type": "csv",
#                 "columns": ["Weekday", "Booking_Count"],
#                 "preview_rows": [
#                     {"Weekday": "Friday", "Booking_Count": "5100"},
#                     {"Weekday": "Monday", "Booking_Count": "4200"},
#                     {"Weekday": "Sunday", "Booking_Count": "2800"},
#                 ]
#             }
#         ],
#         "hypothesis_summary": "Morning weekday demand is 40% higher than average.",
#         "hypothesis_findings": [
#             {"hypothesis_id": 1, "hypothesis": "Weekday morning peak", "result_status": "SUPPORTED", "result": "40% higher", "rationale": "p<0.001"}
#         ],
#         "eda_summary": "100k rows. Friday peak. Morning surge 8-10 AM.",
#         "available_images": ["bookings_by_weekday.png", "heatmap.png"]
#     }

#     content = await llm_call(
#         input_data=[
#             {"role": "system", "content": FINAL_SYNTHESIS_PROMPT},
#             {"role": "user",   "content": json.dumps(context, indent=2)}
#         ],
#         state=STATE,
#         caller_label="NARRATOR_REPORT",
#         max_tokens=6000,
#     )
#     print(f"OUTPUT length: {len(content)} chars")
#     print("Starts with HTML:", content.strip()[:50])
#     return content


# # ════════════════════════════════════════════════════════════
# # ▼▼▼  CONTROL PANEL — uncomment ONE at a time  ▼▼▼
# # ════════════════════════════════════════════════════════════
# async def main():
#     await test_1_data_analysis_agent()
#     # await test_2_eda_plan()
#     # ...
#     # await test_2_eda_plan()
#      #await test_3_code_executor()
#     #await test_4_eda_completeness()
#      #await test_5_eda_synthesis()
#     #await test_6_hypothesis_gen_cmd()
#     #await test_7_hypothesis_judge()
#     # await test_8_hypothesis_synth()
#     # await test_9_narrator_frame()
#      #await test_10_narrator_report()
# 
# asyncio.run(main())