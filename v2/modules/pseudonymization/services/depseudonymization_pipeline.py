"""
V2 depseudonymization pipeline - general-purpose, no report-specific logic.

Pipeline: download -> depseudonymize datasets (regex) -> LLM tag text -> regex replace -> base64 -> save.
- LLM used ONLY for tagging suspected pseudonymized values (uppercase/UPPER_SNAKE_CASE).
- Actual depseudonymization uses regex + reverse translation table (no LLM).
"""

import asyncio
import base64
import json
import mimetypes
import os
import queue
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd
import matplotlib.image as mpimg
from bs4 import BeautifulSoup
from execution_layer.utils.llm_core import CALLER_CONFIG, call_llm
from v2.common.logger import add_log

# --------------------------------------------------------------------------------------
# Reverse mapping (must match pseudonymization mapping)
# --------------------------------------------------------------------------------------

_MAPPER: Dict[str, str] = {
    "A": "Q", "B": "W", "C": "E", "D": "R", "E": "T",
    "F": "Y", "G": "U", "H": "I", "I": "O", "J": "P",
    "K": "A", "L": "S", "M": "D", "N": "F", "O": "G",
    "P": "H", "Q": "J", "R": "K", "S": "L", "T": "Z",
    "U": "X", "V": "C", "W": "V", "X": "B", "Y": "N", "Z": "M",
    "0": "5", "1": "6", "2": "7", "3": "8", "4": "9",
    "5": "0", "6": "1", "7": "2", "8": "3", "9": "4",
    " ": "_", '"': "!", "'": "@",
}

_REVERSE_MAPPER: Dict[str, str] = {}
for _k, _v in _MAPPER.items():
    if _v not in _REVERSE_MAPPER:
        _REVERSE_MAPPER[_v] = _k
_REVERSE_TABLE = str.maketrans(_REVERSE_MAPPER)

# Forward mapping for query pseudonymization (real -> pseudonymized)
_MAPPER_EXTENDED: Dict[str, str] = {
    **_MAPPER,
    **{k.lower(): v for k, v in _MAPPER.items() if isinstance(k, str) and len(k) == 1 and k.isalpha()},
}
_FORWARD_TABLE = str.maketrans(_MAPPER_EXTENDED)


def _pseudonymize_text(text: str) -> str:
    """Apply forward pseudonymization (real value -> pseudonymized)."""
    if text is None:
        return text
    s = str(text).strip()
    if not s:
        return s
    return s.upper().translate(_FORWARD_TABLE)


def _depseudonymize_text(text: str) -> str:
    if text is None:
        return text
    s = str(text)
    return s.translate(_REVERSE_TABLE) if s else s


def _depseudonymize_scalar(value: Any) -> Any:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return value
    except Exception:
        pass
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, np.integer)):
        mapped = _depseudonymize_text(str(value))
        try:
            return int(mapped)
        except Exception:
            return mapped
    if isinstance(value, (float, np.floating)):
        mapped = _depseudonymize_text(str(value))
        try:
            return float(mapped)
        except Exception:
            return mapped
    if isinstance(value, str):
        return _depseudonymize_text(value)
    try:
        return _depseudonymize_text(str(value))
    except Exception:
        return value


# --------------------------------------------------------------------------------------
# HTML helpers
# --------------------------------------------------------------------------------------

_IMG_SRC_RE = re.compile(r"<img\b[^>]*?\bsrc\s*=\s*(['\"])(?P<src>.*?)\1", flags=re.IGNORECASE | re.DOTALL)
_TAGGED_TOKEN_RE = re.compile(r"'(?P<col>[^':]{1,200}):(?P<val>[^']{0,500})'")
# Fallback: unquoted "ColumnName:VALUE" (e.g. in table cells from execution layer)
# Value can contain A-Z, 0-9, _, space, &, and &amp; (HTML entity) - e.g. "XFOZTR_GOS &amp; UQL_EGKH"
_UNQUOTED_COL_VAL_RE = re.compile(
    r"(?<!['\w])(?P<col>[\w\s]{1,80}):(?P<val>(?:[A-Z0-9_ ]|&amp;|&)+)(?![A-Z0-9_&;])",
)
# Standalone pseudonymized fragments after & or &amp; (e.g. "_UQL_EGKH" in "UNITED OIL &_UQL_EGKH")
# Only match values with underscore (typical of pseudonymized names like UQL_EGKH, XFOZTR_GOS)
_STANDALONE_AFTER_AMP_RE = re.compile(
    r"(&amp;|&)\s*(?P<val>(?:[A-Z0-9]+_[A-Z0-9_]+|_[A-Z0-9_]{2,50}))(?=[\s.<\"')\]]|$)",
)


def _extract_image_names_from_html(html: str) -> Set[str]:
    names: Set[str] = set()
    for m in _IMG_SRC_RE.finditer(html or ""):
        src = (m.group("src") or "").strip()
        if not src or src.startswith("data:"):
            continue
        if src.startswith("./"):
            src = src[2:]
        base = os.path.basename(src)
        if base:
            names.add(base)
    return names


def _rewrite_img_srcs(html: str, src_map: Dict[str, str]) -> str:
    def _repl(m: re.Match) -> str:
        quote = m.group(1) or '"'
        src = (m.group("src") or "").strip()
        src2 = src[2:] if src.startswith("./") else src
        base = os.path.basename(src2)
        if base in src_map:
            new_src = src_map[base]
            return m.group(0).replace(f"{quote}{m.group('src')}{quote}", f"{quote}{new_src}{quote}")
        return m.group(0)
    return _IMG_SRC_RE.sub(_repl, html)


# --------------------------------------------------------------------------------------
# image_master.json
# --------------------------------------------------------------------------------------

def _load_image_master(path: str | Path) -> Dict[str, dict]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8", errors="ignore")) or {}
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _write_image_master_atomic(path: str | Path, master: Dict[str, dict]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(master, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)


def _fix_image_master_paths(job_output_dir: Path, image_master_path: Path) -> Dict[str, dict]:
    master = _load_image_master(image_master_path)
    if not master:
        return master
    for img_name, meta in list(master.items()):
        if not isinstance(meta, dict):
            continue
        orig_path = str(meta.get("path") or "").strip()
        if orig_path:
            img_basename = Path(orig_path).name
            correct_path = job_output_dir / img_basename
            meta["path"] = str(correct_path.resolve())
        orig_dataset = str(meta.get("data_set_path") or "").strip()
        if orig_dataset:
            dataset_basename = Path(orig_dataset).name
            correct_dataset = job_output_dir / "image_utils" / dataset_basename
            meta["data_set_path"] = str(correct_dataset.resolve())
    _write_image_master_atomic(image_master_path, master)
    return master


def _update_selected_by_narrator(clean_html: str, image_master_path: Path) -> Tuple[Dict[str, dict], List[str]]:
    referenced = _extract_image_names_from_html(clean_html or "")
    master = _load_image_master(image_master_path)
    matched: List[str] = []
    for img_name, meta in list(master.items()):
        if not isinstance(meta, dict):
            continue
        meta["selected_by_narrator"] = img_name in referenced
        if img_name in referenced:
            matched.append(img_name)
    _write_image_master_atomic(image_master_path, master)
    return master, matched


# --------------------------------------------------------------------------------------
# Dataset depseudonymization
# --------------------------------------------------------------------------------------

def _depseudonymize_selected_datasets(
    image_master_path: Path,
    job_output_dir: Path,
    only_if_selected: bool,
    pseudonymized_columns_map: Optional[Dict[str, List[str]]],
) -> List[Dict[str, Any]]:
    master = _load_image_master(image_master_path)
    results: List[Dict[str, Any]] = []
    for img_name, meta in list(master.items()):
        if not isinstance(meta, dict):
            continue
        if only_if_selected and not bool(meta.get("selected_by_narrator", False)):
            continue
        dataset_path = str(meta.get("data_set_path") or "").strip()
        pseudo_cols = meta.get("pseudonymized_columns") or []
        if not dataset_path:
            continue
        if not isinstance(pseudo_cols, list):
            pseudo_cols = []
        src_p = Path(dataset_path)
        if not src_p.exists():
            candidate = job_output_dir / "image_utils" / src_p.name
            if candidate.exists():
                src_p = candidate
            else:
                continue
        if not pseudo_cols and pseudonymized_columns_map:
            try:
                snapshot_cols = list(pd.read_csv(src_p, nrows=0).columns)
            except Exception:
                snapshot_cols = []
            candidates = []
            for v in pseudonymized_columns_map.values():
                if isinstance(v, list):
                    candidates.extend([str(x) for x in v if str(x).strip()])
            snap_lc = {str(c).strip().lower(): str(c) for c in snapshot_cols if str(c).strip()}
            cand_lc = {str(c).strip().lower() for c in candidates if str(c).strip()}
            pseudo_cols = [snap_lc[k] for k in snap_lc if k in cand_lc]
            if pseudo_cols:
                meta["pseudonymized_columns"] = pseudo_cols
        # Fallback: detect columns with pseudonymized values (uppercase pattern) when map match fails
        if not pseudo_cols:
            _PSEUDO_PATTERN = re.compile(r"^[A-Z0-9_&.!@]+\.?$")  # pseudonymized: uppercase, digits, common chars
            try:
                df_sample = pd.read_csv(src_p, nrows=20, dtype=str)
                for col in df_sample.columns:
                    if not col or col.strip() == "":
                        continue
                    non_null = df_sample[col].dropna().astype(str).str.strip()
                    if len(non_null) == 0:
                        continue
                    matches = non_null.str.match(_PSEUDO_PATTERN)
                    if matches.any() and matches.sum() >= max(1, len(non_null) * 0.5):
                        pseudo_cols.append(str(col))
                if pseudo_cols:
                    meta["pseudonymized_columns"] = pseudo_cols
                    add_log(f"[Depseudo] Inferred pseudonymized_columns for '{img_name}': {pseudo_cols}")
            except Exception:
                pass
        if not pseudo_cols:
            continue
        dst_p = src_p.with_name(f"{src_p.stem}__depseudonymized{src_p.suffix}")
        try:
            df = pd.read_csv(src_p, dtype=str)
            cols_lc = {str(c).strip().lower(): str(c) for c in df.columns if str(c).strip()}
            cols_to_process = [cols_lc.get(str(c).strip().lower()) for c in pseudo_cols if str(c).strip().lower() in cols_lc]
            if not cols_to_process:
                continue
            for c in cols_to_process:
                if c:
                    df[c] = df[c].apply(lambda v: v if pd.isna(v) else _depseudonymize_scalar(v))
            df.to_csv(dst_p, index=False)
            meta["de_pseudonimization_status"] = True
            meta["de_pseudonimized_dataset_path"] = str(dst_p.resolve())
            results.append({"image": img_name, "status": "ok"})
        except Exception as e:
            results.append({"image": img_name, "status": "failed", "reason": str(e)})
    _write_image_master_atomic(image_master_path, master)
    return results


# --------------------------------------------------------------------------------------
# Jupyter code executor (v2 self-contained)
# --------------------------------------------------------------------------------------

class _JupyterExecutor:
    """Minimal Jupyter kernel executor for plot recreation."""

    def __init__(self):
        from jupyter_client.manager import KernelManager
        self.km = KernelManager()
        self.km.start_kernel()
        self.kc = self.km.client()
        self.kc.start_channels()
        self._setup()

    def _setup(self):
        setup = """
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
"""
        self.execute_code(setup)

    def execute_code(self, code: str) -> Dict[str, Any]:
        try:
            msg_id = self.kc.execute(code)
            timeout_s = int(os.getenv("JUPYTER_EXEC_TIMEOUT_SECONDS", "300"))
            start = time.monotonic()
            saw_idle = False
            got_reply = False
            error = None
            outputs = []
            while True:
                if (time.monotonic() - start) > timeout_s:
                    error = f"Timeout after {timeout_s}s"
                    break
                try:
                    msg = self.kc.get_iopub_msg(timeout=1)
                except queue.Empty:
                    msg = None
                if msg and msg.get("parent_header", {}).get("msg_id") == msg_id:
                    mt = msg.get("header", {}).get("msg_type")
                    content = msg.get("content", {}) or {}
                    if mt == "stream":
                        outputs.append(content.get("text", ""))
                    elif mt == "error":
                        tb = content.get("traceback") or []
                        error = "\n".join(tb) or content.get("evalue", "Error")
                    elif mt == "status" and content.get("execution_state") == "idle":
                        saw_idle = True
                try:
                    shell_msg = self.kc.get_shell_msg(timeout=0.1)
                except queue.Empty:
                    shell_msg = None
                if shell_msg and shell_msg.get("parent_header", {}).get("msg_id") == msg_id:
                    if shell_msg.get("header", {}).get("msg_type") == "execute_reply":
                        got_reply = True
                if got_reply and saw_idle:
                    break
            return {"success": error is None, "error": error, "output": "\n".join(outputs)}
        except Exception as e:
            return {"success": False, "error": str(e), "output": ""}

    def cleanup(self):
        try:
            self.kc.stop_channels()
            self.km.shutdown_kernel()
        except Exception:
            pass


def _depseudonymize_tagged_code(code: str, allowed_columns: List[str]) -> str:
    allowed = {str(c).strip().lower() for c in (allowed_columns or []) if str(c).strip()}

    def _repl(m: re.Match) -> str:
        col = str(m.group("col") or "").strip().lower()
        val = str(m.group("val") or "")
        if col in allowed:
            return _depseudonymize_text(val)
        return m.group(0)
    return _TAGGED_TOKEN_RE.sub(_repl, code)


def _is_probably_blank(image_path: str | Path) -> bool:
    p = Path(image_path)
    if not p.exists():
        return True
    try:
        if p.stat().st_size < 8000:
            return True
    except Exception:
        pass
    try:
        arr = mpimg.imread(str(p))
        if arr.dtype not in (np.float32, np.float64):
            arr = arr.astype(np.float32)
            if arr.max() > 1.5:
                arr = arr / 255.0
        if arr.ndim == 3 and arr.shape[-1] == 4:
            arr = arr[..., :3]
        return float(arr.mean()) > 0.93 and float(arr.std()) < 0.02
    except Exception:
        return False


async def _llm_recreate_plot(
    original_code: str,
    dataset_csv_path: str,
    image_output_path: str,
    dataset_columns: List[str],
    candidate_cols: List[str],
    error_context: Optional[str] = None,
) -> Dict[str, Any]:
    system = (
        'Return JSON only: {"need_code_de_pseudonymization": true/false, "code": "python code", "pseudonymized_columns": ["..."]}\n'
        "STRICT: Load dataset ONLY from dataset_csv_path with pandas.read_csv. Save plot EXACTLY to image_output_path.\n"
        "Do NOT plt.show().\n"
        "CRITICAL - The dataset is ALREADY DEPSEUDONYMIZED. All values are real. Use them directly:\n"
        "- Use df[col] or plot_df[col] values AS-IS for axis labels, tick labels, legends - do NOT prefix with column names.\n"
        "- WRONG: labels like 'Account Name:UNITED OIL' or f'{col}:{val}' - never add column name to data values.\n"
        "- CORRECT: plt.barh(df['Account Name'], df['Annual Revenue']) - the values appear directly.\n"
        "- Set need_code_de_pseudonymization=false (data is already real). Do NOT tag values."
    )
    payload = {
        "dataset_csv_path": dataset_csv_path,
        "image_output_path": image_output_path,
        "dataset_columns": dataset_columns,
        "candidate_pseudonymized_columns": candidate_cols,
        "original_code_reference": original_code,
        "previous_error": error_context or "",
    }
    content = await call_llm(
        SYS_PROMPT=system,
        USER_ANALYSIS_PROMPT="Plot recreation payload",
        USER_PROMPT=json.dumps(payload, ensure_ascii=False),
        json_output=True,
        max_tokens=CALLER_CONFIG["V2_DEPSEUDO_PLOT"]["max_tokens"],
    )
    if not content:
        content = "{}"
    try:
        parsed = json.loads(content)
    except Exception:
        parsed = {}
    need = bool(parsed.get("need_code_de_pseudonymization", False))
    code = str(parsed.get("code") or "")
    cols = [str(c) for c in (parsed.get("pseudonymized_columns") or [])
            if str(c).strip().lower() in {str(x).strip().lower() for x in candidate_cols if str(x).strip()}]
    return {"need_code_de_pseudonymization": need, "code": code, "pseudonymized_columns": cols}


async def _recreate_selected_images(image_master_path: Path, only_if_selected: bool) -> List[Dict[str, Any]]:
    master = _load_image_master(image_master_path)
    results: List[Dict[str, Any]] = []
    executor = _JupyterExecutor()
    try:
        for image_name, meta in list(master.items()):
            if not isinstance(meta, dict):
                continue
            if only_if_selected and not bool(meta.get("selected_by_narrator", False)):
                continue
            dep_dataset_path = str(meta.get("de_pseudonimized_dataset_path") or "").strip()
            if not dep_dataset_path:
                continue
            orig_img_path = str(meta.get("path") or "").strip()
            if not Path(dep_dataset_path).exists():
                continue
            try:
                cols = list(pd.read_csv(dep_dataset_path, nrows=0).columns)
            except Exception:
                cols = []
            out_dir = Path(dep_dataset_path).parent
            stem = Path(orig_img_path).stem
            ext = Path(orig_img_path).suffix or ".png"
            dep_img_path = str((out_dir / f"{stem}__depseudonymized{ext}").resolve())
            candidate_cols = [str(c) for c in (meta.get("pseudonymized_columns") or [])]
            last_err = None
            for _ in range(2):
                payload = await _llm_recreate_plot(
                    original_code=str(meta.get("code") or ""),
                    dataset_csv_path=str(Path(dep_dataset_path).resolve()),
                    image_output_path=dep_img_path,
                    dataset_columns=cols,
                    candidate_cols=candidate_cols,
                    error_context=last_err,
                )
                code = str(payload.get("code") or "")
                if not code.strip():
                    last_err = "Empty code from LLM"
                    continue
                if payload.get("need_code_de_pseudonymization"):
                    code = _depseudonymize_tagged_code(code, payload.get("pseudonymized_columns") or [])
                res = executor.execute_code(code)
                if res.get("success") and not _is_probably_blank(dep_img_path):
                    meta["de_pseudonimized_image_path"] = dep_img_path
                    meta["de_pseudonimization_status"] = True
                    results.append({"image": image_name, "status": "ok"})
                    break
                last_err = res.get("error") or "Execution failed"
            else:
                results.append({"image": image_name, "status": "failed", "reason": last_err})
    finally:
        executor.cleanup()
    _write_image_master_atomic(image_master_path, master)
    return results


def _write_depseudonymized_images_html(original_html: str, image_master_path: Path, output_path: Path) -> str:
    master = _load_image_master(image_master_path)
    out_dir = output_path.parent
    mapping: Dict[str, str] = {}
    for img_name, meta in master.items():
        if not isinstance(meta, dict):
            continue
        dep_img = str(meta.get("de_pseudonimized_image_path") or "").strip()
        if not dep_img:
            continue
        dep_p = Path(dep_img)
        if not dep_p.exists():
            continue
        try:
            rel = dep_p.resolve().relative_to(out_dir.resolve())
            mapping[img_name] = str(rel).replace("\\", "/")
        except Exception:
            mapping[img_name] = str(dep_p).replace("\\", "/")
    out_html = _rewrite_img_srcs(original_html or "", mapping)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(out_html, encoding="utf-8", errors="ignore")
    return out_html


# --------------------------------------------------------------------------------------
# Text depseudonymization (LLM)
# --------------------------------------------------------------------------------------

def _depseudonymize_tagged_html(html: str, allowed_columns: List[str]) -> str:
    """Find 'col_name:value' (quoted) or col_name:value (unquoted) via regex, replace with depseudonymized value."""
    allowed = {str(c).strip().lower() for c in (allowed_columns or []) if str(c).strip()}

    def _repl_quoted(m: re.Match) -> str:
        col = str(m.group("col") or "").strip().lower()
        val = str(m.group("val") or "").strip().replace("&amp;", "&")
        if col in allowed:
            return _depseudonymize_text(val)
        return m.group(0)

    def _repl_unquoted(m: re.Match) -> str:
        col = str(m.group("col") or "").strip().lower()
        val = str(m.group("val") or "").strip().replace("&amp;", "&")
        if col in allowed:
            return _depseudonymize_text(val)
        return m.group(0)

    def _repl_standalone(m: re.Match) -> str:
        amp = m.group(1) or "&"
        val = str(m.group("val") or "").strip()
        return amp + " " + _depseudonymize_text(val)

    # 1. Replace quoted tokens: 'Column:VALUE'
    out = _TAGGED_TOKEN_RE.sub(_repl_quoted, html)
    # 2. Fallback: replace unquoted "Column:VALUE" (e.g. execution layer table cells with "Name:XFOZTR_GOS & UQL_EGKH")
    out = _UNQUOTED_COL_VAL_RE.sub(_repl_unquoted, out)
    # 3. Standalone fragments after & or &amp; (e.g. "&_UQL_EGKH" -> "& GAS CORP")
    out = _STANDALONE_AFTER_AMP_RE.sub(_repl_standalone, out)
    return out


async def _llm_tag_pseudonymized_html(
    *,
    html_report: str,
    pseudonymized_columns_map: Dict[str, List[str]],
) -> Dict[str, Any]:
    """
    Ask the LLM to tag suspected pseudonymized values in visible HTML text.
    Only values matching ^[A-Z0-9_]{2,}$ (uppercase or UPPER_SNAKE_CASE) are tagged.
    """
    candidate_cols = []
    for v in pseudonymized_columns_map.values():
        if isinstance(v, list):
            candidate_cols.extend([str(x) for x in v if str(x).strip()])
    seen = set()
    candidate_cols = [c for c in candidate_cols if not (c.lower() in seen or seen.add(c.lower()))]
    system = (
        "Return JSON only (no markdown, no explanations):\n"
        "{\n"
        '  "html_report": "<full html string>",\n'
        '  "selected_columns": ["..."],\n'
        '  "need_depseudonymization": true/false\n'
        "}\n\n"
        "TASK:\n"
        "- You are given an HTML report.\n"
        "- Scan ONLY VISIBLE TEXT content (tables, headings, paragraphs, list items, captions).\n"
        "- Do NOT tag or modify text inside <script> or <style> blocks.\n"
        "\n"
        "ABSOLUTE PRESERVATION RULE (CRITICAL):\n"
        "- You MUST return the SAME html_report string as input, byte-for-byte, EXCEPT for inserting tagging tokens.\n"
        "- Do NOT rewrite, reformat, prettify, minify, or normalize the HTML.\n"
        "- Do NOT change ANY characters, including:\n"
        "  - quotes (single vs double),\n"
        "  - whitespace/newlines/indentation,\n"
        "  - punctuation,\n"
        "  - casing,\n"
        "  - special characters,\n"
        "  - HTML entities (e.g., do NOT convert '&' to '&amp;' or vice-versa),\n"
        "  - Unicode characters.\n"
        "- Do NOT add/remove attributes or tags. Do NOT reorder anything.\n"
        "- Do NOT tag HTML attributes (src/href/style/class/etc). Tag VISIBLE TEXT ONLY.\n\n"
        "TAGGING RULES (STRICT - FOLLOW EXACTLY):\n"
        "\n"
        "RULE 1 - REGEX GATE (MANDATORY):\n"
        "- ONLY tag a value if it matches this regex: ^[A-Z0-9_ &]+$\n"
        "- This means: 2+ characters, uppercase letters (A-Z), digits (0-9), underscores (_), spaces, and ampersand (&). NO lowercase.\n"
        "- If a value contains even ONE lowercase letter (a-z), DO NOT TAG IT. It is not pseudonymized.\n"
        "- Examples of VALID pseudonymized values to tag: 'ACC_123', 'JOHN_DOE', 'XFOZTR_GOS & UQL_EGKH' (company names with &)\n"
        "- Examples of INVALID (do NOT tag): 'John', 'Technology Inc.', 'acc_123' (has lowercase)\n"
        "\n"
        "RULE 2 - CONTEXT VALIDATION:\n"
        "- The value must appear in a data-like context (table cells, list items showing data).\n"
        "- Do NOT tag: UI labels, headings, navigation text, button text, form labels, instructions.\n"
        "- Do NOT tag: Normal English words that happen to be all caps (e.g., 'NOTE:', 'WARNING:', 'TOTAL:').\n"
        "\n"
        "RULE 3 - COLUMN MATCHING:\n"
        "- The value should reasonably belong to one of the candidate_columns provided.\n"
        "- If you're unsure which column it belongs to, skip tagging.\n"
        "\n"
        "RULE 4 - TAGGING FORMAT:\n"
        "- When you tag a value, REPLACE it with EXACTLY: 'Column Name:VALUE' (include the single quotes).\n"
        "- The single quotes MUST be present around the entire token.\n"
        "- Do NOT add quotes around VALUE itself - VALUE must be copied EXACTLY as-is from the original text.\n"
        "- Do NOT add any extra spaces, punctuation, or characters.\n"
        "- Column Name MUST be one of candidate_columns (case-insensitive match).\n"
        "\n"
        "RULE 5 - AVOID DUPLICATES:\n"
        "- Do NOT tag values that are already tagged (i.e., already inside a token like 'Column:VALUE').\n"
        "- Do NOT tag the same value multiple times in nested ways.\n"
        "\n"
        "RULE 6 - SELECTED COLUMNS:\n"
        "- In your response, include only columns you actually used for tagging in 'selected_columns'.\n"
        "- Set need_depseudonymization=true ONLY if you added at least one tag; otherwise false.\n"
        "\n"
        "CRITICAL REMINDERS:\n"
        "- Pseudonymized values are ALWAYS UPPERCASE or UPPER_SNAKE_CASE. If it has lowercase, it's NOT pseudonymized.\n"
        "- When in doubt, DO NOT TAG. It's better to miss a value than to tag incorrectly.\n"
        "- Your html_report output must be IDENTICAL to input except for the tagged tokens you inserted.\n"
    )
    payload = {
        "candidate_columns": candidate_cols,
        "pseudonymized_columns_map": pseudonymized_columns_map,
        "html_report": html_report,
    }
    content = await call_llm(
        SYS_PROMPT=system,
        USER_ANALYSIS_PROMPT="HTML tagging payload",
        USER_PROMPT=json.dumps(payload, ensure_ascii=False),
        json_output=True,
        max_tokens=CALLER_CONFIG["V2_DEPSEUDO_TAG"]["max_tokens"],
    )
    if not content:
        content = "{}"
    try:
        parsed = json.loads(content)
    except Exception:
        parsed = {}
    html_out = str(parsed.get("html_report") or "")
    selected = [str(c) for c in (parsed.get("selected_columns") or []) if str(c).strip().lower() in {x.lower() for x in candidate_cols}]
    need = bool(parsed.get("need_depseudonymization", False))
    return {"html_report": html_out or html_report, "selected_columns": selected, "need_depseudonymization": need}


# --------------------------------------------------------------------------------------
# Query pseudonymization (before sending to execution)
# --------------------------------------------------------------------------------------

async def _llm_tag_query_for_pseudonymization(
    query: str,
    pseudonymized_columns_map: Dict[str, List[str]],
) -> Dict[str, Any]:
    """
    LLM tags values in user query that should be pseudonymized before sending to execution.
    Returns tagged query and selected columns.
    """
    candidate_cols = []
    for v in pseudonymized_columns_map.values():
        if isinstance(v, list):
            candidate_cols.extend([str(x) for x in v if str(x).strip()])
    seen = set()
    candidate_cols = [c for c in candidate_cols if not (c.lower() in seen or seen.add(c.lower()))]
    system = (
        'Return JSON only: {"tagged_query": "<query with tagged values>", "selected_columns": ["..."], "need_pseudonymization": true/false}\n'
        "TASK: User will send a natural language query. Identify values that correspond to data in pseudonymized columns "
        "(e.g., names, IDs, company names) and tag them as 'Column:VALUE' (single quotes, Column from candidate_columns).\n"
        "Only tag values that are clearly entity/data values (names, IDs, codes), NOT common words or question structure.\n"
        "Preserve the rest of the query exactly. Set need_pseudonymization=true only if you added at least one tag."
    )
    payload = {"candidate_columns": candidate_cols, "query": query}
    content = await call_llm(
        SYS_PROMPT=system,
        USER_ANALYSIS_PROMPT="Query tagging payload",
        USER_PROMPT=json.dumps(payload, ensure_ascii=False),
        json_output=True,
        max_tokens=CALLER_CONFIG["V2_DEPSEUDO_QUERY"]["max_tokens"],
    )
    if not content:
        content = "{}"
    try:
        parsed = json.loads(content)
    except Exception:
        parsed = {}
    tagged = str(parsed.get("tagged_query") or "")
    selected = [str(c) for c in (parsed.get("selected_columns") or []) if str(c).strip().lower() in {x.lower() for x in candidate_cols}]
    need = bool(parsed.get("need_pseudonymization", False))
    return {"tagged_query": tagged or query, "selected_columns": selected, "need_pseudonymization": need}


def _pseudonymize_tagged_query(query: str, allowed_columns: List[str]) -> str:
    """Replace tagged tokens 'Column:VALUE' with pseudonymized VALUE."""
    if not query or not allowed_columns:
        return query
    allowed = {str(c).strip().lower() for c in allowed_columns if str(c).strip()}

    def _repl(m: re.Match) -> str:
        col = str(m.group("col") or "").strip().lower()
        val = str(m.group("val") or "")
        if col in allowed:
            return _pseudonymize_text(val)
        return m.group(0)
    return _TAGGED_TOKEN_RE.sub(_repl, query)


async def pseudonymize_query_for_execution(
    query: str,
    pseudonymized_columns_map: Dict[str, List[str]],
) -> str:
    """
    Pseudonymize user query before sending to execution: get query -> LLM tag -> pseudonymize tagged values.
    """
    if not query or not isinstance(pseudonymized_columns_map, dict) or not pseudonymized_columns_map:
        return query
    try:
        tagged = await _llm_tag_query_for_pseudonymization(query, pseudonymized_columns_map)
        need = bool(tagged.get("need_pseudonymization", False))
        tagged_query = str(tagged.get("tagged_query") or "")
        selected = tagged.get("selected_columns") or []
        if not need or not tagged_query:
            return query
        return _pseudonymize_tagged_query(tagged_query, [str(c) for c in selected])
    except Exception as e:
        add_log(f"[Pseudonymize Query] Failed: {e}")
        return query


# --------------------------------------------------------------------------------------
# Depseudonymization (HTML)
# --------------------------------------------------------------------------------------

async def depseudonymize_simple_qna_html(
    html_content: str,
    pseudonymized_columns_map: Dict[str, List[str]],
    save_tagged_callback=None,
) -> str:
    """
    Simple QnA text-only depseudonymization: read html -> LLM tag -> regex replace.
    Used when execution layer returns HTML; v2 depseudonymizes before returning/saving.
    If save_tagged_callback(html: str) is provided, it is called with the tagged HTML before regex replace.
    """
    return await _depseudonymize_html_text(
        html_content, pseudonymized_columns_map,
        out_dir=None, job_id=None, save_tagged_callback=save_tagged_callback,
    )


async def _depseudonymize_html_text(
    html_report: str,
    pseudonymized_columns_map: Dict[str, List[str]],
    out_dir: Optional[Path] = None,
    job_id: Optional[str] = None,
    save_tagged_callback=None,
) -> str:
    """
    LLM tag pseudonymized values (uppercase/UPPER_SNAKE_CASE) -> save tagged report -> regex depseudonymize.
    save_tagged_callback: optional async callable(tagged_html: str) for saving tagged report (e.g. to GCS).
    """
    if not html_report or not isinstance(pseudonymized_columns_map, dict) or not pseudonymized_columns_map:
        return html_report
    try:
        tagged = await _llm_tag_pseudonymized_html(
            html_report=html_report,
            pseudonymized_columns_map=pseudonymized_columns_map,
        )
        need = bool(tagged.get("need_depseudonymization", False))
        tagged_html = str(tagged.get("html_report") or "")
        selected = tagged.get("selected_columns") or []

        # Save tagged report before depseudonymization (local or via callback)
        if out_dir:
            tagged_path = out_dir / _REPORT_TAGGED
            tagged_path.write_text(tagged_html, encoding="utf-8", errors="ignore")
            add_log(f"[Depseudo] Tagged report saved: {tagged_path}", job_id=job_id)
        if save_tagged_callback and tagged_html:
            try:
                result = save_tagged_callback(tagged_html)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                add_log(f"[Depseudo] Failed to save tagged report via callback: {e}", job_id=job_id)

        if not need or not tagged_html:
            return html_report
        # Regex-only depseudonymization (no LLM)
        return _depseudonymize_tagged_html(tagged_html, [str(c) for c in selected])
    except Exception as e:
        add_log(f"[Depseudo] Text depseudonymization failed: {e}", job_id=job_id)
        return html_report


# --------------------------------------------------------------------------------------
# Base64 conversion
# --------------------------------------------------------------------------------------

def _convert_images_to_base64(html_content: str, output_dir: Path) -> str:
    soup = BeautifulSoup(html_content or "", "html.parser")
    for img_tag in soup.find_all("img"):
        src = (img_tag.get("src") or "").strip()
        if not src or src.startswith("data:") or re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", src):
            continue
        src_clean = src.split("?", 1)[0].split("#", 1)[0]
        if src_clean.startswith("./"):
            src_clean = src_clean[2:]
        candidates = [output_dir / src_clean, Path.cwd() / src_clean]
        img_path = next((p for p in candidates if p.exists()), None)
        if not img_path:
            continue
        mime = mimetypes.guess_type(str(img_path))[0] or "image/png"
        try:
            encoded = base64.b64encode(img_path.read_bytes()).decode("utf-8")
        except Exception:
            continue
        new_tag = soup.new_tag("img")
        new_tag["src"] = f"data:{mime};base64,{encoded}"
        for k, v in img_tag.attrs.items():
            if k != "src":
                new_tag[k] = v
        img_tag.replace_with(new_tag)
    return soup.decode(formatter="minimal")


# --------------------------------------------------------------------------------------
# Main pipeline
# --------------------------------------------------------------------------------------

# Config: report file names (general pipeline - no hardcoded report-type logic)
_REPORT_ORIGINAL = "analysis_report_original.html"
_REPORT_MAIN = "analysis_report.html"
_REPORT_TAGGED = "analysis_report_tagged.html"


async def run_depseudonymization_pipeline(
    job_output_dir: str | Path,
    pseudonymized_columns_map: Dict[str, List[str]],
    job_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Full depseudonymization: tagged text -> regex depseudonymize -> base64 -> save.
    Saves: image depseudonymization outputs, tagged report, overwrites analysis_report.html.
    """
    out_dir = Path(job_output_dir)
    result: Dict[str, Any] = {"status": "failed", "report_path": None, "error": None}

    def _log(msg: str) -> None:
        add_log(msg, job_id=job_id)

    input_report = out_dir / _REPORT_ORIGINAL
    if not input_report.exists():
        input_report = out_dir / _REPORT_MAIN
    if not input_report.exists():
        result["error"] = "No analysis report found"
        return result

    try:
        html_content = input_report.read_text(encoding="utf-8", errors="ignore")
    except Exception as e:
        result["error"] = str(e)
        return result

    image_master_path = out_dir / "image_master.json"
    if not image_master_path.exists():
        _log("[Depseudo] No image_master.json - skipping image steps")
        clean_html = html_content
    else:
        try:
            _fix_image_master_paths(out_dir, image_master_path)
        except Exception as e:
            _log(f"[Depseudo] Path fix warning: {e}")
        _update_selected_by_narrator(html_content, image_master_path)
        _depseudonymize_selected_datasets(
            image_master_path, out_dir, only_if_selected=True,
            pseudonymized_columns_map=pseudonymized_columns_map,
        )
        await _recreate_selected_images(image_master_path, only_if_selected=True)
        dep_images_path = out_dir / "analysis_report_depseudonymized_images.html"
        clean_html = _write_depseudonymized_images_html(html_content, image_master_path, dep_images_path)

    # Fix image paths in HTML to match local filenames (for base64 resolution)
    soup = BeautifulSoup(clean_html, "html.parser")
    for img_tag in soup.find_all("img"):
        src = (img_tag.get("src") or "").strip()
        if not src or src.startswith("data:"):
            continue
        src_path = Path(src)
        filename = src_path.name
        for candidate in [out_dir / filename, out_dir / "image_utils" / filename]:
            if candidate.exists():
                img_tag["src"] = str(candidate.relative_to(out_dir)).replace("\\", "/")
                break
    clean_html = soup.decode(formatter="minimal")

    # Text depseudonymization (LLM tag + save tagged report + regex depseudonymize)
    clean_html = await _depseudonymize_html_text(
        clean_html, pseudonymized_columns_map, out_dir=out_dir, job_id=job_id
    )

    # Base64 and save
    final_html = await asyncio.get_event_loop().run_in_executor(
        None, lambda: _convert_images_to_base64(clean_html, out_dir)
    )
    if not final_html:
        final_html = clean_html

    output_path = out_dir / _REPORT_MAIN
    output_path.write_text(final_html, encoding="utf-8", errors="ignore")
    result["status"] = "success"
    result["report_path"] = str(output_path)
    _log(f"[Depseudo] Pipeline complete (overwrite): {output_path}")
    return result
