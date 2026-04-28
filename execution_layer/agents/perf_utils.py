import json
import os
import time
import gc
from typing import Any, Dict, Optional


def _read_proc_status() -> Dict[str, str]:
    """
    Linux-only best-effort process stats.
    Cloud Run runs on Linux, so /proc is available there.
    """
    out: Dict[str, str] = {}
    try:
        with open("/proc/self/status", "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                if ":" not in line:
                    continue
                k, v = line.split(":", 1)
                out[k.strip()] = v.strip()
    except Exception:
        pass
    return out


def _kb_to_mb(v: str) -> Optional[float]:
    try:
        # e.g. "123456 kB"
        parts = v.split()
        if not parts:
            return None
        n = float(parts[0])
        return round(n / 1024.0, 2)
    except Exception:
        return None


def snapshot_resources(*, label: str, state: Optional[dict] = None, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Collect lightweight resource signals without extra dependencies.
    """
    now = time.monotonic()
    status = _read_proc_status()
    rss_mb = _kb_to_mb(status.get("VmRSS", ""))
    vms_mb = _kb_to_mb(status.get("VmSize", ""))
    threads = None
    try:
        threads = int((status.get("Threads") or "").strip() or "0")
    except Exception:
        threads = None

    cpu_count = None
    try:
        cpu_count = os.cpu_count()
    except Exception:
        cpu_count = None

    cpu_time_s = None
    try:
        cpu_time_s = round(time.process_time(), 3)
    except Exception:
        cpu_time_s = None

    wall_elapsed_s = None
    delta_cpu_s = None
    delta_wall_s = None

    if isinstance(state, dict):
        perf = state.setdefault("_perf", {})
        t0 = perf.setdefault("t0", now)
        last_wall = perf.get("last_wall")
        last_cpu = perf.get("last_cpu")
        try:
            wall_elapsed_s = round(now - float(t0), 3)
        except Exception:
            wall_elapsed_s = None
        try:
            if last_wall is not None:
                delta_wall_s = round(now - float(last_wall), 3)
        except Exception:
            delta_wall_s = None
        try:
            if last_cpu is not None and cpu_time_s is not None:
                delta_cpu_s = round(cpu_time_s - float(last_cpu), 3)
        except Exception:
            delta_cpu_s = None
        perf["last_wall"] = now
        perf["last_cpu"] = cpu_time_s

    payload: Dict[str, Any] = {
        "label": label,
        "pid": os.getpid(),
        "cpu_count": cpu_count,
        "cpu_time_s": cpu_time_s,
        "wall_elapsed_s": wall_elapsed_s,
        "delta_wall_s": delta_wall_s,
        "delta_cpu_s": delta_cpu_s,
        "rss_mb": rss_mb,
        "vms_mb": vms_mb,
        "threads": threads,
    }
    if extra:
        payload.update(extra)
    return payload


def log_resources(*, label: str, state: Optional[dict] = None, extra: Optional[Dict[str, Any]] = None) -> None:
    """
    Print a single-line JSON record that is easy to filter in Cloud Run logs.
    """
    payload = snapshot_resources(label=label, state=state, extra=extra)
    try:
        print("[RESOURCE]", json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    except Exception:
        # Best-effort fallback
        print("[RESOURCE]", payload)


def maybe_gc(*, label: str, state: Optional[dict] = None, rss_threshold_mb: float = 12000.0) -> bool:
    """
    Best-effort garbage collection when RSS gets high.
    This won't always return memory to OS, but can reduce Python heap pressure.
    Returns True if GC was triggered.
    """
    snap = snapshot_resources(label=label, state=state)
    rss = snap.get("rss_mb")
    try:
        rss_f = float(rss) if rss is not None else None
    except Exception:
        rss_f = None

    if rss_f is not None and rss_f >= float(rss_threshold_mb):
        try:
            before = snapshot_resources(label=label + "_before_gc", state=state)
            unreachable = gc.collect()
            after = snapshot_resources(label=label + "_after_gc", state=state, extra={"gc_unreachable": unreachable})
            print("[GC]", json.dumps({"before": before, "after": after}, ensure_ascii=False, separators=(",", ":")))
        except Exception as e:
            print("[GC]", {"error": str(e), "label": label})
        return True
    return False

