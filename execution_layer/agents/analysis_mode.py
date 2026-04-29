from __future__ import annotations

from typing import Any


DEFAULT_ANALYSIS_MODE = "slim"


def normalize_analysis_mode(mode: str | None) -> str:
    """Normalize inbound mode values to supported internal modes."""
    raw = (mode or "").strip().lower().replace("-", "_").replace(" ", "_")
    if raw in {"slim", "slim_analysis"}:
        return "slim"
    if raw in {"deep", "deep_dive", "deepdive", "deep_analysis"}:
        return "deep_dive"
    return DEFAULT_ANALYSIS_MODE


def infer_question_depth(original_query: str, query_analysis: dict[str, Any] | None = None) -> str:
    """
    Infer shallow/medium/deep depth from query complexity.
    This is a lightweight deterministic heuristic to drive cycle/task limits.
    """
    q = (original_query or "").strip().lower()
    qa = query_analysis or {}
    sub_queries = qa.get("sub_queries", [])
    sub_query_count = len(sub_queries) if isinstance(sub_queries, list) else 0

    score = 0
    score += min(len(q) // 120, 3)
    score += min(sub_query_count // 2, 3)

    deep_markers = (
        "root cause",
        "segmentation",
        "cohort",
        "forecast",
        "trend",
        "drivers",
        "multi",
        "compare",
        "impact",
        "correlation",
        "hypothesis",
    )
    marker_hits = sum(1 for m in deep_markers if m in q)
    score += min(marker_hits, 3)

    if score <= 1:
        return "shallow"
    if score <= 3:
        return "medium"
    return "deep"


def eda_tasks_per_cycle(_: str) -> int:
    return 5


def eda_cycle_count(mode: str, depth: str) -> int:
    if mode == "slim":
        return 1
    return {"shallow": 1, "medium": 2, "deep": 2}.get(depth, 2)


def eda_min_cycles(mode: str, depth: str) -> int:
    """Minimum EDA cycles that MUST run before the completeness check can stop the loop."""
    if mode == "slim":
        return 1
    return {"shallow": 1, "medium": 1, "deep": 2}.get(depth, 1)


def hypothesis_task_count(mode: str, depth: str) -> int:
    if mode == "slim":
        return 0
    return {"shallow": 2, "medium": 3, "deep": 3}.get(depth, 3)


def executor_max_attempts(mode: str, depth: str) -> int:
    if mode == "slim":
        return 2
    return {"shallow": 1, "medium": 2, "deep": 2}.get(depth, 2)


def narrator_verbosity(mode: str) -> str:
    return "concise" if mode == "slim" else "full"


def skip_hypothesis(mode: str) -> bool:
    return mode == "slim"


def skip_image_analysis(mode: str) -> bool:
    # Slim mode avoids image-analysis/synthesis steps (vision pipeline).
    return mode == "slim"
