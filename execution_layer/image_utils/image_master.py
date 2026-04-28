import csv
import json
from pathlib import Path
from typing import Any, Dict, List, Optional


def image_master_path(output_dir: str | Path) -> Path:
    return Path(output_dir) / "image_master.json"


def load_image_master(output_dir: str | Path) -> Dict[str, Any]:
    p = image_master_path(output_dir)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8", errors="ignore")) or {}
    except Exception:
        return {}


def write_image_master_atomic(output_dir: str | Path, master: Dict[str, Any]) -> Path:
    p = image_master_path(output_dir)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(master, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)
    return p


def read_csv_header_columns(csv_path: str | Path) -> List[str]:
    p = Path(csv_path)
    if not p.exists():
        return []
    try:
        with p.open("r", encoding="utf-8", errors="ignore", newline="") as f:
            reader = csv.reader(f)
            header = next(reader, [])
            return [str(c) for c in header if str(c).strip()]
    except Exception:
        return []


def compute_pseudonymized_columns(used_columns: List[str], anomaly_columns: List[str]) -> List[str]:
    if not used_columns or not anomaly_columns:
        return []
    anom_lc = {str(c).strip().lower() for c in anomaly_columns if str(c).strip()}
    out: List[str] = []
    for c in used_columns:
        c_str = str(c).strip()
        if c_str and c_str.lower() in anom_lc:
            out.append(c_str)
    # de-dupe preserve order
    seen = set()
    dedup: List[str] = []
    for c in out:
        if c not in seen:
            seen.add(c)
            dedup.append(c)
    return dedup


def upsert_image_entry(
    master: Dict[str, Any],
    *,
    image_name: str,
    image_path: str,
    code: str,
    data_set_name: str,
    data_set_path: str,
    pseudonymized_columns: List[str],
) -> Dict[str, Any]:
    """
    Upsert an image entry while preserving narrator selection if already present.
    """
    prev = master.get(image_name) if isinstance(master, dict) else None
    prev = prev if isinstance(prev, dict) else {}

    master[image_name] = {
        "path": image_path,
        "code": code,
        "data_set_name": data_set_name,
        "data_set_path": data_set_path,
        "pseudonymized_columns": pseudonymized_columns or [],
        "selected_by_narrator": bool(prev.get("selected_by_narrator", False)),
    }
    return master


