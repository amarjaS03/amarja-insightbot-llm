import asyncio
import json
import logging
import os
import re
import base64
import mimetypes
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd
import matplotlib.image as mpimg
from dotenv import load_dotenv
from bs4 import BeautifulSoup

# Ensure execution_layer is in path or importable
from execution_layer.agents.coding_tool import JupyterExecutionTool
from execution_layer.utils.llm_core import CALLER_CONFIG, call_llm

load_dotenv()

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------------------
# Helpers moved from data_analysis_agent.py
# --------------------------------------------------------------------------------------

def load_domain_directory(input_dir: str) -> dict:
    """Load domain directory from input path"""
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


def convert_images_to_base64_for_report(*, html_content: str, output_dir: str | Path | None = None) -> str:
    """
    Convert <img src="..."> in an HTML string to base64 data URIs.
    Intended for final report packaging so the HTML becomes self-contained.
    """
    soup = BeautifulSoup(html_content or "", "html.parser")

    out_dir = Path(output_dir) if output_dir else None
    script_dir = Path(__file__).resolve().parent

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
        else:
            if out_dir:
                candidates.append(out_dir / src_path)
            candidates.append(Path.cwd() / src_path)
            candidates.append(script_dir / src_path)

        img_path = next((p for p in candidates if p.exists()), None)
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

    return soup.decode(formatter="minimal")


# --------------------------------------------------------------------------------------
# Reverse mapping (must match pseudonymization mapping in API layer)
# --------------------------------------------------------------------------------------

MAPPER: Dict[str, str] = {
    'A': 'Q', 'B': 'W', 'C': 'E', 'D': 'R', 'E': 'T',
    'F': 'Y', 'G': 'U', 'H': 'I', 'I': 'O', 'J': 'P',
    'K': 'A', 'L': 'S', 'M': 'D', 'N': 'F', 'O': 'G',
    'P': 'H', 'Q': 'J', 'R': 'K', 'S': 'L', 'T': 'Z',
    'U': 'X', 'V': 'C', 'W': 'V', 'X': 'B', 'Y': 'N', 'Z': 'M',
    '0': '5', '1': '6', '2': '7', '3': '8', '4': '9',
    '5': '0', '6': '1', '7': '2', '8': '3', '9': '4',
    ' ': '_', '"': '!', "'": '@',
}

# Build reverse translation table for de-pseudonymization.
# IMPORTANT: Pseudonymized strings are ALWAYS UPPERCASE or UPPER_SNAKE_CASE.
# Do NOT include lowercase mappings as those would be real values, not pseudonymized.
_REVERSE_MAPPER: Dict[str, str] = {}
for _k, _v in MAPPER.items():
    if _v not in _REVERSE_MAPPER:
        _REVERSE_MAPPER[_v] = _k
_REVERSE_TRANSLATION_TABLE = str.maketrans(_REVERSE_MAPPER)


def depseudonymize_text(text: str) -> str:
    if text is None:
        return text
    s = str(text)
    if s == "":
        return s
    return s.translate(_REVERSE_TRANSLATION_TABLE)


def depseudonymize_scalar(value: Any) -> Any:
    """
    De-pseudonymize a scalar value and try to preserve its datatype.

    - None/NaN stays as-is
    - bool stays as-is
    - int/float get digit-reversal applied and cast back when possible
    - other objects are returned as depseudonymized string
    """
    if value is None:
        return None
    try:
        if pd.isna(value):
            return value
    except Exception:
        pass
    if isinstance(value, bool):
        return value

    # numeric types
    try:
        # numpy integer types also satisfy this via pandas scalar checks, but isinstance is ok.
        if isinstance(value, (int, np.integer)):
            mapped = depseudonymize_text(str(value))
            try:
                return int(mapped)
            except Exception:
                return mapped
        if isinstance(value, (float, np.floating)):
            mapped = depseudonymize_text(str(value))
            try:
                return float(mapped)
            except Exception:
                return mapped
    except Exception:
        pass

    if isinstance(value, str):
        return depseudonymize_text(value)

    # fallback
    try:
        return depseudonymize_text(str(value))
    except Exception:
        return value


# --------------------------------------------------------------------------------------
# HTML helpers (no BeautifulSoup dependency for regex parts)
# --------------------------------------------------------------------------------------

_IMG_SRC_RE = re.compile(r"<img\b[^>]*?\bsrc\s*=\s*(['\"])(?P<src>.*?)\1", flags=re.IGNORECASE | re.DOTALL)


def extract_image_names_from_html(html: str) -> Set[str]:
    """
    Extract image basenames referenced in HTML via <img src="...">.
    Returns {"foo.png", "bar.jpg"}.
    """
    names: Set[str] = set()
    s = html or ""
    for m in _IMG_SRC_RE.finditer(s):
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
    """
    Replace <img src="..."> where basename(src) matches src_map keys.
    """
    if not html:
        return html

    def _repl(m: re.Match) -> str:
        quote = m.group(1) or '"'
        src = (m.group("src") or "").strip()
        if src.startswith("./"):
            src2 = src[2:]
        else:
            src2 = src
        base = os.path.basename(src2)
        if base in src_map:
            new_src = src_map[base]
            return m.group(0).replace(f"{quote}{m.group('src')}{quote}", f"{quote}{new_src}{quote}")
        return m.group(0)

    return _IMG_SRC_RE.sub(_repl, html)


# --------------------------------------------------------------------------------------
# image_master.json IO
# --------------------------------------------------------------------------------------

def load_image_master(image_master_path: str | Path) -> Dict[str, dict]:
    p = Path(image_master_path)
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8", errors="ignore")) or {}
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def write_image_master_atomic(image_master_path: str | Path, master: Dict[str, dict]) -> None:
    p = Path(image_master_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(master, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)


def fix_image_master_paths(
    *,
    job_output_dir: str | Path,
    image_master_path: str | Path,
) -> Dict[str, dict]:
    """
    Normalize all paths in image_master.json to match the actual job output directory structure.
    
    This fixes paths that may have been created by the executor without session_id or with
    container-relative paths. Works by matching basenames to known folder structure:
    - Images: job_output_dir/*.png|jpg|jpeg
    - Datasets: job_output_dir/image_utils/*.csv
    
    Returns the corrected master dict (also writes it back to disk atomically).
    """
    job_dir = Path(job_output_dir)
    master = load_image_master(image_master_path)
    
    if not master:
        logger.warning(f"[FixImagePaths] No image_master.json to fix at {image_master_path}")
        return master
    
    logger.info(f"[FixImagePaths] Normalizing paths for {len(master)} images in image_master")
    fixed_count = 0
    
    for img_name, meta in list(master.items()):
        if not isinstance(meta, dict):
            continue
        
        # Fix image path
        orig_path = str(meta.get("path") or "").strip()
        if orig_path:
            img_basename = Path(orig_path).name
            correct_path = job_dir / img_basename
            if str(meta.get("path")) != str(correct_path):
                meta["path"] = str(correct_path.resolve())
                fixed_count += 1
                logger.info(f"[FixImagePaths] Fixed path for '{img_name}': {correct_path}")
        
        # Fix dataset path
        orig_dataset = str(meta.get("data_set_path") or "").strip()
        if orig_dataset:
            dataset_basename = Path(orig_dataset).name
            correct_dataset = job_dir / "image_utils" / dataset_basename
            if str(meta.get("data_set_path")) != str(correct_dataset):
                meta["data_set_path"] = str(correct_dataset.resolve())
                fixed_count += 1
                logger.info(f"[FixImagePaths] Fixed dataset for '{img_name}': {correct_dataset}")
    
    if fixed_count > 0:
        write_image_master_atomic(image_master_path, master)
        logger.info(f"[FixImagePaths] Fixed {fixed_count} path(s) and saved image_master.json")
    else:
        logger.info(f"[FixImagePaths] All paths already correct, no changes needed")
    
    return master


def update_selected_by_narrator_from_html(
    *,
    clean_html_path: str | Path,
    image_master_path: str | Path,
    set_unselected_to_false: bool = False,
    ) -> Tuple[Dict[str, dict], List[str]]:
    """
    Reads the HTML report, extracts referenced image names, and sets selected_by_narrator=True
    for matching image_master entries.
    """
    html_p = Path(clean_html_path)
    clean_html = html_p.read_text(encoding="utf-8", errors="ignore") if html_p.exists() else ""
    referenced = extract_image_names_from_html(clean_html)

    master = load_image_master(image_master_path)
    matched: List[str] = []
    for img_name, meta in list(master.items()):
        if not isinstance(meta, dict):
            continue
        if img_name in referenced:
            meta["selected_by_narrator"] = True
            matched.append(img_name)
        elif set_unselected_to_false:
            meta["selected_by_narrator"] = False

    write_image_master_atomic(image_master_path, master)
    return master, matched


def update_selected_by_narrator_from_html_content(
    *,
    clean_html: str,
    image_master_path: str | Path,
    set_unselected_to_false: bool = False,
    ) -> Tuple[Dict[str, dict], List[str]]:
    """
    Same as update_selected_by_narrator_from_html, but takes HTML content directly
    (e.g., narrator's clean_html stored in state["final_html_report"]).
    """
    referenced = extract_image_names_from_html(clean_html or "")
    master = load_image_master(image_master_path)
    matched: List[str] = []
    for img_name, meta in list(master.items()):
        if not isinstance(meta, dict):
            continue
        if img_name in referenced:
            meta["selected_by_narrator"] = True
            matched.append(img_name)
        elif set_unselected_to_false:
            meta["selected_by_narrator"] = False

    write_image_master_atomic(image_master_path, master)
    return master, matched


def depseudonymize_selected_datasets(
    *,
    image_master_path: str | Path,
    only_if_selected: bool = True,
    overwrite: bool = False,
    pseudonymized_columns_map: Optional[Dict[str, List[str]]] = None,
    ) -> List[Dict[str, str]]:
    """
    For each image (optionally only if selected_by_narrator==True):
    - Load its data_set_path CSV (snapshot)
    - De-pseudonymize ONLY columns listed in pseudonymized_columns using local reverse mapper
    - Write either in-place (overwrite=True) or to a sibling file and store it in:
        de_pseudonimization_status=true
        de_pseudonimized_dataset_path=<abs depseudonymized snapshot path>
    """
    master = load_image_master(image_master_path)
    results: List[Dict[str, str]] = []
    
    # Get job output dir from image_master_path for path resolution
    job_output_dir = Path(image_master_path).parent
    logger.info(f"[DepseudoDatasets] Processing {len(master)} images from image_master")

    for img_name, meta in list(master.items()):
        if not isinstance(meta, dict):
            continue
        if only_if_selected and not bool(meta.get("selected_by_narrator", False)):
            logger.info(f"[DepseudoDatasets] Skipping '{img_name}' - not selected by narrator")
            continue

        dataset_path = str(meta.get("data_set_path") or "").strip()
        pseudo_cols = meta.get("pseudonymized_columns") or []
        if not dataset_path:
            logger.warning(f"[DepseudoDatasets] Skipping '{img_name}' - missing data_set_path")
            results.append({"image": img_name, "status": "skipped", "reason": "missing data_set_path"})
            continue
        if not isinstance(pseudo_cols, list):
            pseudo_cols = []

        src_p = Path(dataset_path)
        # Handle paths from container (missing session_id) - try basename match in job dir
        if not src_p.exists():
            # Try finding by basename in job_output_dir/image_utils
            basename = src_p.name
            candidate = job_output_dir / "image_utils" / basename
            if candidate.exists():
                logger.info(f"[DepseudoDatasets] Resolved '{img_name}' dataset via basename: {candidate}")
                src_p = candidate
            else:
                logger.warning(f"[DepseudoDatasets] Skipping '{img_name}' - dataset not found: {dataset_path}")
                results.append({"image": img_name, "status": "skipped", "reason": f"dataset not found: {dataset_path}"})
                continue

        # Fallback: if image_master doesn't have pseudonymized_columns, try deriving them from the global map
        # by matching snapshot header columns (domain-agnostic, no hardcoding).
        if not pseudo_cols:
            logger.info(f"[DepseudoDatasets] '{img_name}' has empty pseudonymized_columns, attempting fallback...")
            if isinstance(pseudonymized_columns_map, dict) and pseudonymized_columns_map:
                try:
                    snapshot_cols = list(pd.read_csv(src_p, nrows=0).columns)
                    logger.info(f"[DepseudoDatasets] Snapshot columns for '{img_name}': {snapshot_cols}")
                except Exception as e:
                    logger.warning(f"[DepseudoDatasets] Failed to read snapshot headers for '{img_name}': {e}")
                    snapshot_cols = []
                # Union all candidate columns from the map, then intersect with snapshot header (case-insensitive).
                candidates: List[str] = []
                for dataset_name, v in pseudonymized_columns_map.items():
                    if isinstance(v, list):
                        candidates.extend([str(x) for x in v if str(x).strip()])
                logger.info(f"[DepseudoDatasets] Candidate pseudonymized columns from map: {list(set(candidates))}")
                
                snap_lc_to_actual = {str(c).strip().lower(): str(c) for c in snapshot_cols if str(c).strip()}
                cand_lc = {str(c).strip().lower() for c in candidates if str(c).strip()}
                pseudo_cols = [snap_lc_to_actual[k] for k in snap_lc_to_actual.keys() if k in cand_lc]
                
                if pseudo_cols:
                    logger.info(f"[DepseudoDatasets] Matched columns for '{img_name}': {pseudo_cols}")
                    meta["pseudonymized_columns"] = pseudo_cols
                else:
                    logger.warning(f"[DepseudoDatasets] No column matches found for '{img_name}'")
            else:
                logger.warning(f"[DepseudoDatasets] No pseudonymized_columns_map provided for fallback")
                
            if not pseudo_cols:
                logger.warning(f"[DepseudoDatasets] Skipping '{img_name}' - no pseudonymized columns (fallback failed)")
                results.append({"image": img_name, "status": "skipped", "reason": "no pseudonymized_columns in image_master (and no header match from map)"})
                continue

        # destination
        if overwrite:
            dst_p = src_p
        else:
            stem = src_p.stem  # e.g. foo__dataset
            dst_p = src_p.with_name(f"{stem}__depseudonymized{src_p.suffix}")

        logger.info(f"[DepseudoDatasets] Processing '{img_name}' with {len(pseudo_cols)} pseudonymized column(s): {pseudo_cols}")
        
        try:
            # Read as strings to preserve token formatting; downstream reads can infer numerics again.
            df = pd.read_csv(src_p, dtype=str)

            # Strictly follow image_master-provided columns with robust case-insensitive matching.
            cols_lower_to_actual = {str(c).strip().lower(): str(c) for c in df.columns if str(c).strip()}
            cols_to_process: List[str] = []
            for c in pseudo_cols:
                key = str(c).strip().lower()
                if key in cols_lower_to_actual:
                    cols_to_process.append(cols_lower_to_actual[key])

            if not cols_to_process:
                results.append({"image": img_name, "status": "skipped", "reason": "pseudonymized_columns not found in dataset"})
                continue

            for c in cols_to_process:
                df[c] = df[c].apply(lambda v: v if pd.isna(v) else depseudonymize_scalar(v))

            df.to_csv(dst_p, index=False)

            meta["de_pseudonimization_status"] = True
            meta["de_pseudonimized_dataset_path"] = str(dst_p.resolve())

            results.append({"image": img_name, "status": "ok", "reason": f"depseudonymized columns: {cols_to_process}"})
        except Exception as e:
            results.append({"image": img_name, "status": "failed", "reason": str(e)})

    write_image_master_atomic(image_master_path, master)
    return results


# --------------------------------------------------------------------------------------
# Plot/code recreation + depseudonymized HTML variant
# --------------------------------------------------------------------------------------

_TAGGED_TOKEN_RE = re.compile(r"'(?P<col>[^':]{1,200}):(?P<val>[^']{0,500})'")


def depseudonymize_tagged_html(html: str, allowed_columns: List[str]) -> str:
    """
    Replace tagged tokens in HTML text like 'Column:VALUE' with depseudonymized VALUE,
    but ONLY when Column is in allowed_columns (case-insensitive).
    """
    if not html:
        return html
    allowed = {str(c).strip().lower() for c in (allowed_columns or []) if str(c).strip()}

    def _repl(m: re.Match) -> str:
        col = str(m.group("col") or "").strip().lower()
        val = str(m.group("val") or "")
        if col in allowed:
            return depseudonymize_text(val)
        return m.group(0)

    return _TAGGED_TOKEN_RE.sub(_repl, html)


async def _llm_tag_pseudonymized_html(
    *,
    html_report: str,
    pseudonymized_columns_map: Dict[str, List[str]],
    ) -> Dict[str, Any]:
    """
    Ask the LLM to tag suspected pseudonymized values in visible HTML text.
    """
    # Flatten candidate columns for guidance
    candidate_cols: List[str] = []
    if isinstance(pseudonymized_columns_map, dict):
        for v in pseudonymized_columns_map.values():
            if isinstance(v, list):
                candidate_cols.extend([str(x) for x in v if str(x).strip()])
    # de-dupe preserve order
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
        "- ONLY tag a value if it matches this EXACT regex: ^[A-Z0-9_]{2,}$\n"
        "- This means: 2+ characters, ONLY uppercase letters (A-Z), digits (0-9), and underscores (_). NO lowercase.\n"
        "- If a value contains even ONE lowercase letter (a-z), DO NOT TAG IT. It is not pseudonymized.\n"
        "- Examples of VALID pseudonymized values to tag: 'ACC_123', 'JOHN_DOE', 'TECHNOLOGY', 'A1B2'\n"
        "- Examples of INVALID (do NOT tag): 'John', 'Technology Inc.', 'acc_123' (has lowercase), 'A&B' (has &)\n"
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
        max_tokens=CALLER_CONFIG["V1_DEPSEUDO_TAG"]["max_tokens"],
    )
    if not content:
        content = "{}"

    try:
        parsed = json.loads(content) if content else {}
    except Exception:
        parsed = {}

    html_out = str(parsed.get("html_report") or "")
    selected = parsed.get("selected_columns") or []
    if not isinstance(selected, list):
        selected = []
    need = bool(parsed.get("need_depseudonymization", False))

    # sanitize selected_columns to candidate list
    allowed_lc = {c.lower() for c in candidate_cols}
    selected = [str(c) for c in selected if str(c).strip().lower() in allowed_lc]

    return {
        "html_report": html_out or html_report,
        "selected_columns": selected,
        "need_depseudonymization": need,
    }


async def depseudonymize_html_text_via_llm(
    *,
    html_report: str,
    pseudonymized_columns_map: Optional[Dict[str, List[str]]] = None,
    ) -> Dict[str, Any]:
    """
    Full text depseudonymization flow:
    - LLM tags values in visible text as 'Column:VALUE'
    - If need_depseudonymization, replace tags locally using reverse mapper
    Returns a summary dict including tagged and depseudonymized HTML.
    """
    summary: Dict[str, Any] = {
        "status": "skipped",
        "need_depseudonymization": False,
        "selected_columns": [],
        "tagged_html": "",
        "depseudonymized_html": "",
        "error": "",
    }
    if not html_report:
        logger.warning("[TextDepseudo] Empty HTML report provided")
        return summary
    
    if not isinstance(pseudonymized_columns_map, dict) or not pseudonymized_columns_map:
        logger.warning(f"[TextDepseudo] Invalid or empty pseudonymized_columns_map: {type(pseudonymized_columns_map)}")
        return summary

    # Flatten columns for logging
    all_cols = []
    for cols_list in pseudonymized_columns_map.values():
        if isinstance(cols_list, list):
            all_cols.extend(cols_list)
    logger.info(f"[TextDepseudo] Starting text depseudonymization with columns: {all_cols}")

    try:
        tagged_payload = await _llm_tag_pseudonymized_html(
            html_report=html_report,
            pseudonymized_columns_map=pseudonymized_columns_map,
        )
        tagged_html = str(tagged_payload.get("html_report") or "")
        selected_columns = tagged_payload.get("selected_columns") or []
        need = bool(tagged_payload.get("need_depseudonymization", False))

        summary["tagged_html"] = tagged_html
        summary["selected_columns"] = selected_columns
        summary["need_depseudonymization"] = need

        logger.info(f"[TextDepseudo] LLM tagging complete - need_depseudonymization: {need}, selected_columns: {selected_columns}")

        if not need or not tagged_html:
            summary["status"] = "ok"
            summary["depseudonymized_html"] = html_report
            if not need:
                logger.info("[TextDepseudo] LLM determined no depseudonymization needed")
            return summary

        dep_html = depseudonymize_tagged_html(tagged_html, [str(c) for c in selected_columns])
        summary["depseudonymized_html"] = dep_html
        
        # Verify changes were made
        if dep_html != html_report:
            logger.info(f"[TextDepseudo] Text depseudonymization applied - HTML changed")
        else:
            logger.warning("[TextDepseudo] Text depseudonymization returned same HTML")
        
        summary["status"] = "ok"
        return summary
    except Exception as e:
        logger.exception("Text depseudonymization via LLM failed")
        summary["status"] = "failed"
        summary["error"] = str(e)
        return summary


def depseudonymize_tagged_code(code: str, allowed_columns: List[str]) -> str:
    """
    Replace tagged tokens like 'Vehicle Type:TWOAT' with the local de-pseudonymized value of TWOAT.
    """
    if not code:
        return code
    allowed = {str(c).strip().lower() for c in (allowed_columns or []) if str(c).strip()}

    def _repl(m: re.Match) -> str:
        col = str(m.group("col") or "").strip().lower()
        val = str(m.group("val") or "")
        if col in allowed:
            return depseudonymize_text(val)
        return m.group(0)

    return _TAGGED_TOKEN_RE.sub(_repl, code)


def is_probably_blank_image(image_path: str | Path) -> bool:
    """
    Heuristic blank detector: file too small OR mostly white/near-constant.
    """
    p = Path(image_path)
    if not p.exists():
        return True
    try:
        if p.stat().st_size < 8_000:
            return True
    except Exception:
        pass

    try:
        arr = mpimg.imread(str(p))
        if arr.dtype != np.float32 and arr.dtype != np.float64:
            arr = arr.astype(np.float32)
            if arr.max() > 1.5:
                arr = arr / 255.0
        if arr.ndim == 3 and arr.shape[-1] == 4:
            arr = arr[..., :3]
        mean = float(arr.mean())
        std = float(arr.std())
        if mean > 0.93 and std < 0.02:
            return True
        if std < 0.005:
            return True
        return False
    except Exception:
        return False


async def _llm_recreate_plot_payload(
    *,
    original_code: str,
    dataset_csv_path: str,
    image_output_path: str,
    dataset_columns: List[str],
    candidate_pseudonymized_columns: List[str],
    error_context: Optional[str] = None,
    ) -> Dict[str, object]:
    """
    LLM call that returns JSON for plot recreation.
    """
    system = (
        "Return JSON only (no markdown, no explanations):\n"
        "{\n"
        '  "need_code_de_pseudonymization": true/false,\n'
        '  "code": "python code as a string",\n'
        '  "pseudonymized_columns": ["..."]\n'
        "}\n"
        "\n"
        "STRICT CONSTRAINTS FOR THE code FIELD:\n"
        "- The code must be valid Python and runnable as-is.\n"
        "- Load the dataset ONLY from dataset_csv_path using pandas.read_csv.\n"
        "- Do NOT read any other files.\n"
        "- Do NOT print or display the dataset.\n"
        "- Do NOT call plt.show().\n"
        "- Save the plot EXACTLY to image_output_path.\n"
        "- Ensure directories exist for the output path.\n"
        "- Use 4-space indentation only; avoid IndentationError.\n"
        "\n"
        "CODE-DE-PSEUDONYMIZATION TAGGING:\n"
        "- If your generated code contains pseudonymized values in titles/labels/legends/headings that should be de-pseudonymized,\n"
        "  tag ONLY those values as the exact token: 'Column Name:VALUE' (single quotes included).\n"
        "- Set need_code_de_pseudonymization=true if you added any such tags.\n"
        "- pseudonymized_columns must be a subset of candidate_pseudonymized_columns.\n"
    )

    payload = {
        "dataset_csv_path": dataset_csv_path,
        "image_output_path": image_output_path,
        "dataset_columns": dataset_columns,
        "candidate_pseudonymized_columns": candidate_pseudonymized_columns,
        "original_code_reference": original_code,
        "previous_error": error_context or "",
    }

    content = await call_llm(
        SYS_PROMPT=system,
        USER_ANALYSIS_PROMPT="Plot recreation payload",
        USER_PROMPT=json.dumps(payload, ensure_ascii=False),
        json_output=True,
        max_tokens=CALLER_CONFIG["V1_DEPSEUDO_PLOT"]["max_tokens"],
    )
    if not content:
        content = "{}"

    try:
        parsed = json.loads(content)
    except Exception:
        parsed = {}

    need = bool(parsed.get("need_code_de_pseudonymization", False))
    code = str(parsed.get("code") or "")
    cols = parsed.get("pseudonymized_columns") or []
    if not isinstance(cols, list):
        cols = []
    allowed_lc = {str(c).strip().lower() for c in candidate_pseudonymized_columns if str(c).strip()}
    cols = [str(c) for c in cols if str(c).strip().lower() in allowed_lc]

    return {
        "need_code_de_pseudonymization": need,
        "code": code,
        "pseudonymized_columns": cols,
    }


async def recreate_selected_images_from_image_master(
    *,
    image_master_path: str | Path,
    only_if_selected: bool = True,
    ) -> List[Dict[str, Any]]:
    """
    For each selected image in image_master.json:
    - Take stored code as reference
    - Use depseudonymized dataset snapshot if available
    - Ask LLM to rewrite code to use that dataset and save a depseudonymized image
    - Execute code in a local Jupyter kernel
    """
    master = load_image_master(image_master_path)
    results: List[Dict[str, Any]] = []
    job_output_dir = Path(image_master_path).parent
    logger.info(f"[RecreateImages] Processing {len(master)} images from image_master")
    executor = JupyterExecutionTool()

    try:
        for image_name, meta in list(master.items()):
            if not isinstance(meta, dict):
                continue
            if only_if_selected and not bool(meta.get("selected_by_narrator", False)):
                logger.info(f"[RecreateImages] Skipping '{image_name}' - not selected by narrator")
                continue

            orig_img_path = str(meta.get("path") or "").strip()
            # Only recreate images when we have a depseudonymized dataset snapshot.
            dep_dataset_path = str(meta.get("de_pseudonimized_dataset_path") or "").strip()
            if not dep_dataset_path:
                logger.warning(f"[RecreateImages] Skipping '{image_name}' - no de_pseudonimized_dataset_path (datasets were not depseudonymized successfully)")
                results.append(
                    {
                        "image": image_name,
                        "status": "skipped",
                        "reason": "no de_pseudonimized_dataset_path; skipping image recreation",
                    }
                )
                continue
            dataset_path = dep_dataset_path
            original_code = str(meta.get("code") or "")
            candidate_pseudo_cols = meta.get("pseudonymized_columns") or []
            if not isinstance(candidate_pseudo_cols, list):
                candidate_pseudo_cols = []

            if not orig_img_path or not dataset_path:
                results.append({"image": image_name, "status": "skipped", "reason": "missing path/data_set_path"})
                continue
            if not Path(dataset_path).exists():
                results.append({"image": image_name, "status": "skipped", "reason": f"dataset missing: {dataset_path}"})
                continue

            try:
                cols = list(pd.read_csv(dataset_path, nrows=0).columns)
            except Exception:
                cols = []

            out_dir = Path(dataset_path).parent  # image_utils folder
            stem = Path(orig_img_path).stem
            ext = Path(orig_img_path).suffix or ".png"
            dep_img_path = str((out_dir / f"{stem}__depseudonymized{ext}").resolve())

            last_err: Optional[str] = None
            regenerated_ok = False
            llm_calls = 0
            llm_nonempty_code = 0
            for attempt in range(2):
                payload = await _llm_recreate_plot_payload(
                    original_code=original_code,
                    dataset_csv_path=str(Path(dataset_path).resolve()),
                    image_output_path=dep_img_path,
                    dataset_columns=cols,
                    candidate_pseudonymized_columns=[str(c) for c in candidate_pseudo_cols],
                    error_context=last_err,
                )
                llm_calls += 1
                need_depseudo = bool(payload.get("need_code_de_pseudonymization", False))
                code = str(payload.get("code") or "")
                pseudo_cols_from_llm = payload.get("pseudonymized_columns") or []
                if not isinstance(pseudo_cols_from_llm, list):
                    pseudo_cols_from_llm = []

                if not code.strip():
                    last_err = "Empty code returned from LLM"
                    continue
                llm_nonempty_code += 1

                if need_depseudo:
                    code = depseudonymize_tagged_code(code, [str(c) for c in pseudo_cols_from_llm])

                exec_result = executor.execute_code(code)
                if exec_result.get("success"):
                    if is_probably_blank_image(dep_img_path):
                        last_err = "Generated image looks blank; ensure plot uses non-empty data and renders visible marks."
                        continue

                    regenerated_ok = True
                    meta["de_pseudonimized_image_path"] = dep_img_path
                    meta["de_pseudonimization_status"] = True
                    results.append(
                        {
                            "image": image_name,
                            "status": "ok",
                            "reason": "recreated depseudonymized image",
                            "llm_calls": llm_calls,
                            "llm_nonempty_code": llm_nonempty_code,
                        }
                    )
                    break

                last_err = exec_result.get("error") or exec_result.get("output") or "Execution failed"

            if not regenerated_ok:
                results.append(
                    {
                        "image": image_name,
                        "status": "failed",
                        "reason": last_err or "unknown",
                        "llm_calls": llm_calls,
                        "llm_nonempty_code": llm_nonempty_code,
                    }
                )

    finally:
        try:
            executor.cleanup()
        except Exception:
            pass

    try:
        write_image_master_atomic(image_master_path, master)
    except Exception:
        pass

    return results


def _summarize_depseudonymization_results(dataset_results: List[Dict[str, Any]], image_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    ds_ok = sum(1 for r in (dataset_results or []) if isinstance(r, dict) and r.get("status") == "ok")
    ds_failed = sum(1 for r in (dataset_results or []) if isinstance(r, dict) and r.get("status") == "failed")
    ds_skipped = sum(1 for r in (dataset_results or []) if isinstance(r, dict) and r.get("status") == "skipped")

    img_ok = sum(1 for r in (image_results or []) if isinstance(r, dict) and r.get("status") == "ok")
    img_failed = sum(1 for r in (image_results or []) if isinstance(r, dict) and r.get("status") == "failed")
    img_skipped = sum(1 for r in (image_results or []) if isinstance(r, dict) and r.get("status") == "skipped")

    llm_calls = 0
    llm_nonempty = 0
    for r in (image_results or []):
        if not isinstance(r, dict):
            continue
        try:
            llm_calls += int(r.get("llm_calls", 0) or 0)
            llm_nonempty += int(r.get("llm_nonempty_code", 0) or 0)
        except Exception:
            continue

    return {
        "dataset_depseudonymized_ok": ds_ok,
        "dataset_failed": ds_failed,
        "dataset_skipped": ds_skipped,
        "images_regenerated_ok": img_ok,
        "images_failed": img_failed,
        "images_skipped": img_skipped,
        "llm_calls_total": llm_calls,
        "llm_nonempty_code_total": llm_nonempty,
    }


def write_depseudonymized_images_html_from_content(
    *,
    original_html: str,
    image_master_path: str | Path,
    output_html_path: str | Path,
    ) -> Tuple[str, Path]:
    out_p = Path(output_html_path)
    out_dir = out_p.parent
    master = load_image_master(image_master_path)

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
    out_p.parent.mkdir(parents=True, exist_ok=True)
    out_p.write_text(out_html, encoding="utf-8", errors="ignore")
    return out_html, out_p


async def run_job_depseudonymization_async(
    *,
    job_output_dir: str | Path,
    report_filename: str = "analysis_report.html",
    clean_html: str = "",
    only_if_selected: bool = True,
    pseudonymized_columns_map: Optional[Dict[str, List[str]]] = None,
    ) -> Dict[str, Any]:
    """
    Async entrypoint for the full depseudonymization pipeline.
    """
    out_dir = Path(job_output_dir)
    image_master_path = out_dir / "image_master.json"
    report_path = out_dir / report_filename
    dep_report_path = out_dir / "analysis_report_depseudonymized_images.html"

    summary: Dict[str, Any] = {
        "selected_images": [],
        "dataset_results": [],
        "image_results": [],
        "counts": {},
        "depseudonymized_report_path": "",
        "depseudonymized_html": "",
        "status": "skipped",
        "error": "",
    }

    if not image_master_path.exists():
        summary["status"] = "skipped"
        summary["error"] = "image_master.json not found"
        return summary
    
    # FIX PATHS FIRST: Normalize all paths in image_master to match actual job structure
    # This handles both old (container-relative) and new (session-aware) paths
    logger.info("[JobDepseudo] Step 0: Fixing image_master.json paths to match job output dir")
    try:
        fix_image_master_paths(
            job_output_dir=out_dir,
            image_master_path=image_master_path,
        )
    except Exception as e:
        logger.warning(f"[JobDepseudo] Path fixing failed (non-critical): {e}")

    html_for_selection = (clean_html or "").strip()
    if not html_for_selection:
        if not report_path.exists():
            summary["status"] = "skipped"
            summary["error"] = "analysis_report.html not found (and no clean_html provided)"
            return summary
        html_for_selection = report_path.read_text(encoding="utf-8", errors="ignore")

    try:
        _master, matched = update_selected_by_narrator_from_html_content(
            clean_html=html_for_selection,
            image_master_path=image_master_path,
            set_unselected_to_false=False,
        )
        summary["selected_images"] = matched

        dataset_results = depseudonymize_selected_datasets(
            image_master_path=image_master_path,
            only_if_selected=only_if_selected,
            overwrite=False,
            pseudonymized_columns_map=pseudonymized_columns_map,
        )
        summary["dataset_results"] = dataset_results

        image_results = await recreate_selected_images_from_image_master(
            image_master_path=image_master_path,
            only_if_selected=only_if_selected,
        )
        summary["image_results"] = image_results

        summary["counts"] = _summarize_depseudonymization_results(dataset_results, image_results)
        try:
            logger.info(
                "[DE-PSEUDONYMIZATION] snapshots_ok=%s snapshots_skipped=%s snapshots_failed=%s images_ok=%s images_skipped=%s images_failed=%s llm_calls=%s llm_nonempty_code=%s",
                summary["counts"].get("dataset_depseudonymized_ok", 0),
                summary["counts"].get("dataset_skipped", 0),
                summary["counts"].get("dataset_failed", 0),
                summary["counts"].get("images_regenerated_ok", 0),
                summary["counts"].get("images_skipped", 0),
                summary["counts"].get("images_failed", 0),
                summary["counts"].get("llm_calls_total", 0),
                summary["counts"].get("llm_nonempty_code_total", 0),
            )
        except Exception:
            pass

        dep_html, dep_report = write_depseudonymized_images_html_from_content(
            original_html=html_for_selection,
            image_master_path=image_master_path,
            output_html_path=dep_report_path,
        )
        summary["depseudonymized_report_path"] = str(dep_report.resolve())
        summary["depseudonymized_html"] = dep_html
        summary["status"] = "ok"
        return summary
    except Exception as e:
        logger.exception("Depseudonymization pipeline failed for job_output_dir=%s", str(out_dir))
        summary["status"] = "failed"
        summary["error"] = str(e)
        return summary


async def run_depseudonymization_pipeline(
    *,
    job_output_dir: str | Path,
    pseudonymized_columns_map: Dict[str, List[str]],
    # domain_directory kept for signature compatibility but not strictly used if map is provided
    domain_directory: dict = None,
) -> Dict[str, Any]:
    """
    On-demand pipeline to run full depseudonymization (Images + Text).
    Reads 'analysis_report_original.html' (or 'analysis_report.html') from job_output_dir.
    Returns summary dict and saves 'depseudonymized_report.html'.
    """
    out_dir = Path(job_output_dir)
    logger.info(f"🚀 [Depseudonymization] Starting pipeline for dir: {out_dir}")
    
    result = {
        "status": "skipped",
        "report_path": None,
        "error": None,
        "details": {}
    }

    # 1. Identify input report
    input_report_path = out_dir / "analysis_report_original.html"
    if not input_report_path.exists():
        logger.info(f"ℹ️ [Depseudonymization] 'analysis_report_original.html' not found, checking fallback...")
        input_report_path = out_dir / "analysis_report.html"
    
    if not input_report_path.exists():
        msg = "No analysis report found to depseudonymize"
        logger.error(f"❌ [Depseudonymization] {msg}")
        result["status"] = "failed"
        result["error"] = msg
        return result

    logger.info(f"✅ [Depseudonymization] Found input report: {input_report_path.name}")

    try:
        html_content = input_report_path.read_text(encoding="utf-8", errors="ignore")
    except Exception as e:
        msg = f"Failed to read report: {e}"
        logger.error(f"❌ [Depseudonymization] {msg}")
        result["status"] = "failed"
        result["error"] = msg
        return result

    # 2. Run Image Depseudonymization (Recreate Images)
    logger.info("🎨 [Depseudonymization] Step 1: Image Depseudonymization (Recreate)")
    clean_html = html_content
    dep_summary = {}
    try:
        dep_summary = await run_job_depseudonymization_async(
            job_output_dir=out_dir,
            clean_html=clean_html,
            only_if_selected=True,
            pseudonymized_columns_map=pseudonymized_columns_map,
        )
        result["details"]["image_depseudonymization"] = dep_summary
        
        if dep_summary.get("status") == "ok":
            new_html = dep_summary.get("depseudonymized_html")
            img_counts = dep_summary.get("counts", {})
            logger.info(f"✅ [Depseudonymization] Images processed: {img_counts}")
            if new_html:
                clean_html = new_html
                try:
                    # Save intermediate report: Images Recreated (only one, not duplicate)
                    (out_dir / "analysis_report_images_recreated.html").write_text(
                        clean_html, encoding="utf-8", errors="ignore"
                    )
                    logger.info("💾 [Depseudonymization] Saved intermediate: analysis_report_images_recreated.html")
                except Exception as e:
                    logger.warning(f"Failed to save images_recreated report: {e}")
        else:
            logger.warning(f"⚠️ [Depseudonymization] Image step status: {dep_summary.get('status')} - {dep_summary.get('error')}")

    except Exception as e:
        logger.warning(f"⚠️ [Depseudonymization] Image depseudonymization failed: {e}")
        result["details"]["image_depseudonymization_error"] = str(e)

    # 3. Run Text Depseudonymization (LLM Tagging + Replacement)
    logger.info("📝 [Depseudonymization] Step 2: Text Depseudonymization (Tag & Replace)")
    txt_summary = {}
    try:
        txt_summary = await depseudonymize_html_text_via_llm(
            html_report=clean_html,
            pseudonymized_columns_map=pseudonymized_columns_map,
        )
        result["details"]["text_depseudonymization"] = txt_summary
        
        # Save intermediate report: Tagged Text (if available) - Before local replacement if possible, 
        # but the function returns tags + replacement result together. 
        # We can extract 'tagged_html' from summary if provided.
        tagged_html = txt_summary.get("tagged_html")
        if tagged_html:
            try:
                (out_dir / "analysis_report_tagged_text.html").write_text(
                    tagged_html, encoding="utf-8", errors="ignore"
                )
                logger.info("💾 [Depseudonymization] Saved intermediate: analysis_report_tagged_text.html")
            except Exception:
                pass
        
        if txt_summary.get("status") == "ok":
            if txt_summary.get("need_depseudonymization"):
                final_html = txt_summary.get("depseudonymized_html")
                if final_html:
                    clean_html = final_html
                logger.info("✅ [Depseudonymization] Text changes applied.")
            else:
                logger.info("ℹ️ [Depseudonymization] No text changes needed.")
        else:
             logger.warning(f"⚠️ [Depseudonymization] Text step failed: {txt_summary.get('error')}")

    except Exception as e:
        logger.warning(f"⚠️ [Depseudonymization] Text depseudonymization failed: {e}")
        result["details"]["text_depseudonymization_error"] = str(e)

    # 4. Finalize: Convert images to Base64 and Save
    logger.info("💾 [Depseudonymization] Step 3: Finalizing Report")
    try:
        # IMPORTANT: Fix image paths in HTML first to match actual file locations
        # The HTML may have paths like "execution_layer/output_data/JOB_xxx/image.png" (missing session_id)
        # We need to rewrite them to actual relative paths from the output dir
        soup = BeautifulSoup(clean_html, "html.parser")
        for img_tag in soup.find_all("img"):
            src = (img_tag.get("src") or "").strip()
            if not src or src.startswith("data:"):
                continue
            
            # Extract just the filename
            src_path = Path(src)
            filename = src_path.name
            
            # Check if this file exists in the job output dir
            actual_file = out_dir / filename
            if actual_file.exists():
                # Update to relative path (just filename)
                img_tag["src"] = filename
                logger.info(f"[Finalize] Fixed image src: {src} -> {filename}")
        
        # Serialize back to HTML (use minimal formatter to avoid corruption)
        fixed_html = soup.decode(formatter="minimal")
        
        # Run conversion in thread pool to avoid blocking async loop
        final_base64_html = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: convert_images_to_base64_for_report(
                html_content=fixed_html, output_dir=out_dir
            )
        )
        if not final_base64_html:
            final_base64_html = fixed_html

        output_path = out_dir / "depseudonymized_report.html"
        output_path.write_text(final_base64_html, encoding="utf-8", errors="ignore")
        
        logger.info(f"🎉 [Depseudonymization] Pipeline Completed. Report saved to: {output_path}")
        result["status"] = "success"
        result["report_path"] = str(output_path)
        result["report_html"] = final_base64_html  # Return the HTML content
        
    except Exception as e:
        logger.error(f"❌ [Depseudonymization] Final save failed: {e}")
        result["status"] = "failed"
        result["error"] = f"Final save failed: {e}"
    
    return result


def depseudonymize_csv_file(
    *,
    csv_path: str | Path,
    pseudonymized_columns_map: Dict[str, List[str]],
    output_path: Optional[str | Path] = None,
) -> Dict[str, Any]:
    """
    Depseudonymize a CSV file by applying depseudonymize_scalar to matching columns.
    Returns summary dict with processing results.
    """
    result = {
        "status": "skipped",
        "columns_processed": [],
        "rows_processed": 0,
        "error": None,
        "output_path": None
    }

    try:
        # Read CSV as strings to preserve format
        df = pd.read_csv(csv_path, dtype=str)
        original_row_count = len(df)

        # Get all pseudonymized columns from map
        all_pseudo_cols = []
        for cols_list in pseudonymized_columns_map.values():
            if isinstance(cols_list, list):
                all_pseudo_cols.extend([str(c).strip() for c in cols_list if str(c).strip()])

        logger.info(f"[DepseudoCSV] CSV columns: {list(df.columns)}")
        logger.info(f"[DepseudoCSV] Pseudonymized columns from map: {all_pseudo_cols}")

        # Case-insensitive column matching
        cols_lower_to_actual = {str(c).strip().lower(): str(c) for c in df.columns if str(c).strip()}
        cols_to_process = []
        for c in all_pseudo_cols:
            key = str(c).strip().lower()
            if key in cols_lower_to_actual:
                cols_to_process.append(cols_lower_to_actual[key])

        if not cols_to_process:
            logger.warning(f"[DepseudoCSV] No matching pseudonymized columns found. CSV cols: {list(df.columns)}, Map cols: {all_pseudo_cols}")
            result["status"] = "skipped"
            result["error"] = f"No pseudonymized columns found in CSV. CSV columns: {list(df.columns)}, Map columns: {all_pseudo_cols}"
            return result

        logger.info(f"[DepseudoCSV] Processing columns: {cols_to_process}")

        # Apply depseudonymization - process all non-NaN values
        for c in cols_to_process:
            def process_value(v):
                if pd.isna(v):
                    return v
                if v is None:
                    return v
                if isinstance(v, str) and not v.strip():
                    return v  # Empty string stays empty
                return depseudonymize_scalar(v)
            
            df[c] = df[c].apply(process_value)

        logger.info(f"[DepseudoCSV] Depseudonymized {len(cols_to_process)} column(s) with {original_row_count} row(s)")

        # Determine output path
        if output_path:
            output_path = Path(output_path)
        else:
            csv_path = Path(csv_path)
            output_path = csv_path.with_name(f"{csv_path.stem}_depseudonymized{csv_path.suffix}")

        # Save depseudonymized CSV
        df.to_csv(output_path, index=False)

        result["status"] = "success"
        result["columns_processed"] = cols_to_process
        result["rows_processed"] = original_row_count
        result["output_path"] = str(output_path)

    except Exception as e:
        result["status"] = "failed"
        result["error"] = str(e)

    return result


def convert_csv_to_base64(*, csv_path: str | Path) -> str:
    """
    Convert CSV file to base64 data URI for HTML embedding.
    """
    try:
        with open(csv_path, 'rb') as f:
            csv_data = f.read()
        base64_content = base64.b64encode(csv_data).decode('utf-8')
        return f"data:text/csv;base64,{base64_content}"
    except Exception as e:
        logger.warning(f"Failed to convert CSV to base64: {e}")
        return ""


def resolve_csv_path(*, session_id: str, csv_file_name: str, csv_file_path: Optional[str] = None) -> Optional[Path]:
    """
    Resolve CSV file path using multiple strategies.
    Returns Path object if found, None otherwise.
    """
    # Strategy 1: Construct path using session_id and filename
    candidate_path = Path("execution_layer") / "output_data" / session_id / csv_file_name
    if candidate_path.exists():
        return candidate_path

    # Strategy 2: Try relative path if provided
    if csv_file_path:
        # Try absolute path first
        csv_path = Path(csv_file_path)
        if csv_path.exists():
            return csv_path

        # Try relative to execution_layer
        rel_path = Path("execution_layer") / csv_file_path
        if rel_path.exists():
            return rel_path

    return None


def process_simple_qna_csv_depseudonymization(
    *,
    session_id: str,
    csv_file_path: Optional[str] = None,
    csv_file_name: Optional[str] = None,
    pseudonymized_columns_map: Dict[str, List[str]],
) -> Dict[str, Any]:
    """
    Complete CSV processing pipeline for Simple QnA depseudonymization.
    Returns dict with base64 blob and processing status.
    """
    result = {
        "status": "skipped",
        "csv_base64": None,
        "columns_processed": [],
        "error": None
    }

    if not csv_file_name and not csv_file_path:
        result["status"] = "skipped"
        result["error"] = "No CSV file information provided"
        return result

    # Resolve CSV path
    csv_path = resolve_csv_path(
        session_id=session_id,
        csv_file_name=csv_file_name or "",
        csv_file_path=csv_file_path
    )

    if not csv_path:
        result["status"] = "failed"
        result["error"] = f"CSV file not found: {csv_file_name or csv_file_path}"
        return result

    # Depseudonymize CSV
    depseudo_result = depseudonymize_csv_file(
        csv_path=csv_path,
        pseudonymized_columns_map=pseudonymized_columns_map,
        output_path=None  # Will use default naming
    )

    if depseudo_result["status"] != "success":
        result["status"] = "failed"
        result["error"] = depseudo_result.get("error", "CSV depseudonymization failed")
        return result

    # Convert to base64
    depseudo_path = depseudo_result["output_path"]
    csv_base64 = convert_csv_to_base64(csv_path=depseudo_path)

    if not csv_base64:
        result["status"] = "failed"
        result["error"] = "Failed to convert CSV to base64"
        return result

    result["status"] = "success"
    result["csv_base64"] = csv_base64
    result["columns_processed"] = depseudo_result["columns_processed"]

    return result


def protect_csv_links_in_html(*, html_content: str) -> Tuple[str, List[str], Optional[Any]]:
    """
    Protect CSV download links in HTML by replacing them with placeholders.
    Returns tuple of (protected_html, original_links, parent_element).
    Preserves the parent element context to restore link in correct location.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    original_links = []
    parent_element = None

    for a in soup.find_all('a', href=True):
        href = a['href']
        if href.strip().startswith('data:text/csv;base64'):
            original_links.append(str(a))
            # Store parent element for proper restoration
            parent_element = a.parent
            # Replace with placeholder - use comment node that won't break HTML structure
            placeholder = soup.new_string("<!-- CSV_DOWNLOAD_LINK_PLACEHOLDER -->")
            a.replace_with(placeholder)
            break  # Only handle first CSV link

    return str(soup), original_links, parent_element


def restore_csv_links_in_html(*, html_content: str, csv_base64: Optional[str] = None, original_link: Optional[str] = None) -> str:
    """
    Restore CSV download links in HTML, preferring new depseudonymized version.
    Ensures link is placed outside/after tables, not inside them.
    """
    placeholder = "<!-- CSV_DOWNLOAD_LINK_PLACEHOLDER -->"

    if placeholder not in html_content:
        logger.warning("[RestoreCSVLink] Placeholder not found in HTML")
        return html_content

    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Find placeholder comment - search in all text nodes
        placeholder_found = False
        placeholder_node = None
        
        # Search through all elements
        for element in soup.descendants:
            if isinstance(element, str) and placeholder in element:
                placeholder_node = element
                placeholder_found = True
                break
        
        if not placeholder_found:
            logger.warning("[RestoreCSVLink] Placeholder text node not found, using string replacement")
            # Fallback to string replacement
            if csv_base64:
                new_link = f'<div style="text-align: center; margin: 20px 0;"><a href="data:text/csv;base64,{csv_base64}" download="data.csv" style="display: inline-block; padding: 10px 20px; background-color: #007bff; color: white; text-decoration: none; border-radius: 5px;">Download CSV</a></div>'
                return html_content.replace(placeholder, new_link, 1)
            elif original_link:
                return html_content.replace(placeholder, original_link, 1)
            return html_content

        # Determine which link to use
        if csv_base64:
            link_html = f'<a href="data:text/csv;base64,{csv_base64}" download="data.csv" style="display: inline-block; padding: 10px 20px; background-color: #007bff; color: white; text-decoration: none; border-radius: 5px; margin-top: 10px;">Download CSV</a>'
        elif original_link:
            link_html = original_link
        else:
            link_html = ""

        if placeholder_node and hasattr(placeholder_node, 'parent') and placeholder_node.parent:
            # Check if placeholder is inside a table - if so, move it outside
            current = placeholder_node.parent
            table_parent = None
            
            while current:
                if hasattr(current, 'name') and current.name == 'table':
                    table_parent = current
                    break
                if hasattr(current, 'parent'):
                    current = current.parent
                else:
                    break
            
            if table_parent:
                # Place link after the table
                link_div = soup.new_tag('div', style='text-align: center; margin: 20px 0;')
                link_soup = BeautifulSoup(link_html, 'html.parser')
                if link_soup.a:
                    link_div.append(link_soup.a)
                else:
                    link_div.append(link_soup)
                # Insert after table
                table_parent.insert_after(link_div)
                # Remove placeholder
                if placeholder_node.parent:
                    placeholder_node.extract()
                logger.info("[RestoreCSVLink] CSV link placed after table")
                return str(soup)
            
            # Not in table - replace placeholder directly
            link_soup = BeautifulSoup(link_html, 'html.parser')
            if link_soup.a:
                placeholder_node.replace_with(link_soup.a)
            else:
                placeholder_node.replace_with(link_soup)
            logger.info("[RestoreCSVLink] CSV link replaced in place")
            return str(soup)
        else:
            # Fallback: simple string replacement
            logger.warning("[RestoreCSVLink] Could not find placeholder parent, using string replacement")
            if csv_base64:
                new_link = f'<div style="text-align: center; margin: 20px 0;"><a href="data:text/csv;base64,{csv_base64}" download="data.csv" style="display: inline-block; padding: 10px 20px; background-color: #007bff; color: white; text-decoration: none; border-radius: 5px;">Download CSV</a></div>'
                return html_content.replace(placeholder, new_link, 1)
            elif original_link:
                return html_content.replace(placeholder, original_link, 1)
            return html_content.replace(placeholder, "", 1)
            
    except Exception as e:
        logger.error(f"[RestoreCSVLink] Error restoring CSV link: {e}")
        # Fallback to simple string replacement
        if csv_base64:
            new_link = f'<div style="text-align: center; margin: 20px 0;"><a href="data:text/csv;base64,{csv_base64}" download="data.csv" style="display: inline-block; padding: 10px 20px; background-color: #007bff; color: white; text-decoration: none; border-radius: 5px;">Download CSV</a></div>'
            return html_content.replace(placeholder, new_link, 1)
        elif original_link:
            return html_content.replace(placeholder, original_link, 1)
        return html_content


async def run_simple_qna_depseudonymization(
    *,
    session_id: str,
    html_content: str,
    csv_file_path: Optional[str] = None,
    csv_file_name: Optional[str] = None,
    pseudonymized_columns_map: Dict[str, List[str]],
) -> Dict[str, Any]:
    """
    Complete Simple QnA depseudonymization pipeline.
    Handles both CSV depseudonymization and HTML text depseudonymization.
    """
    result = {
        "status": "success",
        "html_content": html_content,
        "csv_processed": False,
        "text_processed": False,
        "csv_columns_processed": [],
        "text_changes_made": False,
        "error": None
    }

    try:
        # Step 1: Process CSV if present
        csv_result = None
        protected_html = html_content
        original_csv_link = None

        if csv_file_path or csv_file_name:
            # Protect CSV links before LLM processing
            protected_html, original_links, parent_element = protect_csv_links_in_html(html_content=html_content)
            original_csv_link = original_links[0] if original_links else None

            logger.info(f"[SimpleQnA Depseudo] Processing CSV: {csv_file_name or csv_file_path}")

            # Process CSV depseudonymization
            csv_result = process_simple_qna_csv_depseudonymization(
                session_id=session_id,
                csv_file_path=csv_file_path,
                csv_file_name=csv_file_name,
                pseudonymized_columns_map=pseudonymized_columns_map
            )

            if csv_result["status"] == "success":
                result["csv_processed"] = True
                result["csv_columns_processed"] = csv_result["csv_columns_processed"]
                logger.info(f"[SimpleQnA Depseudo] CSV processed successfully: {len(csv_result['csv_columns_processed'])} columns")
            else:
                logger.warning(f"[SimpleQnA Depseudo] CSV processing failed: {csv_result.get('error')}")

        # Step 2: Process HTML text depseudonymization (always run if pseudonymized_columns_map exists)
        logger.info(f"[SimpleQnA Depseudo] Starting text depseudonymization...")
        text_result = await depseudonymize_html_text_via_llm(
            html_report=protected_html,
            pseudonymized_columns_map=pseudonymized_columns_map
        )

        final_html = protected_html
        if text_result.get("status") == "ok":
            result["text_processed"] = True
            need_depseudo = text_result.get("need_depseudonymization", False)
            if need_depseudo:
                dep_html = text_result.get("depseudonymized_html", protected_html)
                if dep_html and dep_html != protected_html:
                    final_html = dep_html
                    result["text_changes_made"] = True
                    logger.info(f"[SimpleQnA Depseudo] Text depseudonymization applied successfully")
                else:
                    logger.warning(f"[SimpleQnA Depseudo] Text depseudonymization returned same HTML")
            else:
                logger.info(f"[SimpleQnA Depseudo] Text depseudonymization not needed (no pseudonymized values detected)")
        else:
            error_msg = text_result.get('error', 'Unknown error')
            logger.warning(f"[SimpleQnA Depseudo] Text depseudonymization failed: {error_msg}")

        # Step 3: Restore CSV links (only if CSV was processed)
        if csv_result and csv_result.get("status") == "success":
            logger.info(f"[SimpleQnA Depseudo] Restoring CSV link...")
            final_html = restore_csv_links_in_html(
                html_content=final_html,
                csv_base64=csv_result.get("csv_base64"),
                original_link=original_csv_link
            )

        result["html_content"] = final_html

    except Exception as e:
        result["status"] = "failed"
        result["error"] = str(e)
        logger.exception(f"Simple QnA depseudonymization failed: {e}")

    return result


def run_depseudonymization_on_host(session_id: str, job_id: str) -> Dict[str, Any]:
    """
    Host-side entry point that:
    1. Resolves paths (execution_layer/output_data/{session_id}/{job_id})
    2. Loads domain and pseudonymization map
    3. Runs the async pipeline synchronously
    """
    import sys
    # Ensure logs show up in stdout if not configured
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    logger.info(f"▶️ [RunDepseudonymization] Request received for Session: {session_id}, Job: {job_id}")
    
    # Use pathlib for robust path handling
    cwd = Path.cwd()
    
    # Define base paths
    base_output_dir = cwd / "execution_layer" / "output_data"
    base_input_dir = cwd / "execution_layer" / "input_data"
    
    logger.info(f"📂 [RunDepseudonymization] Base paths - Output: {base_output_dir}, Input: {base_input_dir}")

    # 1. Resolve paths
    # Host session input dir: execution_layer/input_data/{session_id}
    session_input_dir = base_input_dir / session_id
    if not session_input_dir.exists():
        logger.warning(f"⚠️ [RunDepseudonymization] Session input directory does not exist: {session_input_dir}")
    else:
        logger.info(f"✅ [RunDepseudonymization] Session input dir found: {session_input_dir}")

    # Host job output dir: execution_layer/output_data/{session_id}/{job_id}
    job_output_dir = base_output_dir / session_id / job_id
    
    if not job_output_dir.exists():
        # Log exact location attempted
        logger.error(f"❌ [RunDepseudonymization] Job output directory NOT FOUND at: {job_output_dir}")
        return {"error": f"Job output directory not found: {job_output_dir}", "status": "failed"}

    logger.info(f"✅ [RunDepseudonymization] Job output dir found: {job_output_dir}")

    # 2. Load Domain Directory & Build Map
    # Ensure we convert Path objects to strings for legacy helper functions if they don't support Path
    domain_directory = load_domain_directory(str(session_input_dir))
    pseudonymized_columns_map = build_pseudonymized_columns_map(domain_directory)
    
    logger.info(f"📋 [RunDepseudonymization] Domain Directory entries: {len(domain_directory)}")
    logger.info(f"🗺️ [RunDepseudonymization] Pseudonymization Map keys: {list(pseudonymized_columns_map.keys())}")

    if not pseudonymized_columns_map:
        logger.warning(f"⚠️ [RunDepseudonymization] No pseudonymization map found (or empty) for this session.")

    # 3. Run Pipeline
    try:
        result = asyncio.run(run_depseudonymization_pipeline(
            job_output_dir=job_output_dir,
            domain_directory=domain_directory,
            pseudonymized_columns_map=pseudonymized_columns_map
        ))
        logger.info(f"🏁 [RunDepseudonymization] Finished with status: {result.get('status')}")
        
        # 4. Upload depseudonymized report to Firebase Storage if successful
        if result.get('status') == 'success':
            depseudonymized_report_path = job_output_dir / "depseudonymized_report.html"
            if depseudonymized_report_path.exists():
                try:
                    from api_layer.firebase_config import firebase_crud
                    # Upload as analysis_report.html in storage (same path as original)
                    storage_path = f"{session_id}/{job_id}/analysis_report.html"
                    firebase_storage_url = firebase_crud.upload_file_to_storage(
                        str(depseudonymized_report_path), 
                        storage_path
                    )
                    if firebase_storage_url:
                        logger.info(f"📤 [RunDepseudonymization] Depseudonymized report uploaded to Firebase Storage: {firebase_storage_url}")
                        result["report_url"] = firebase_storage_url
                    else:
                        logger.warning(f"⚠️ [RunDepseudonymization] Failed to upload depseudonymized report to Firebase Storage")
                except Exception as upload_error:
                    logger.warning(f"⚠️ [RunDepseudonymization] Error uploading depseudonymized report: {str(upload_error)}")
        
        return result
    except Exception as e:
        logger.exception(f"❌ [RunDepseudonymization] Unexpected failure: {e}")
        return {"error": str(e), "status": "failed"}
