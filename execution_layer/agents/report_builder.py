"""
Deterministic HTML report from job output artifacts (CSVs, TXTs, PNGs).
Overwrites analysis_report.html when required inputs validate.
"""

from __future__ import annotations

import csv
import html
import json
import re
from pathlib import Path
from typing import Any


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="ignore").strip()


def _paragraphs_from_text(text: str) -> str:
    blocks = [b.strip() for b in re.split(r"\n\s*\n+", text) if b.strip()]
    if not blocks:
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        blocks = lines or [""]
    parts = []
    for b in blocks:
        esc = html.escape(b)
        esc = esc.replace("\n", "<br>\n")
        parts.append(f'<p class="rb-p">{esc}</p>')
    return "\n".join(parts)


def _csv_to_html_table(csv_path: Path, *, max_rows: int = 2000) -> str:
    with csv_path.open(newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        rows = [[(c or "").strip() for c in row] for row in reader]
    if not rows:
        return '<p class="rb-note">Empty file.</p>'
    header = rows[0]
    body = []
    for row in rows[1:]:
        if not any(cell for cell in row):
            continue
        body.append(row)
    body = body[:max_rows]
    ths = "".join(f"<th>{html.escape(h)}</th>" for h in header)
    trs = []
    for row in body:
        cells = row + [""] * (len(header) - len(row))
        cells = cells[: len(header)]
        tds = "".join(f"<td>{html.escape(c)}</td>" for c in cells)
        trs.append(f"<tr>{tds}</tr>")
    tbody = "\n".join(trs)
    return f'<table class="rb-table"><thead><tr>{ths}</tr></thead><tbody>\n{tbody}\n</tbody></table>'


def _rel(src: Path, base: Path) -> str:
    try:
        return src.relative_to(base).as_posix()
    except ValueError:
        return src.name


def _img_tag(src: Path, base: Path, alt: str) -> str:
    if not src.is_file():
        return ""
    rel = html.escape(_rel(src, base), quote=True)
    alt_e = html.escape(alt, quote=True)
    return f'<figure class="rb-fig"><img src="{rel}" alt="{alt_e}" loading="lazy"/></figure>'


def _section(title: str, inner: str) -> str:
    tid = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-") or "section"
    return f'<section class="rb-section" id="{html.escape(tid, quote=True)}"><h2 class="rb-h2">{html.escape(title)}</h2>\n{inner}\n</section>'


def _find_first(out: Path, patterns: tuple[str, ...]) -> Path | None:
    for pat in patterns:
        for p in sorted(out.glob(pat)):
            if p.is_file():
                return p
    return None


def rebuild_analysis_report_html(output_dir: str | Path) -> dict[str, Any]:
    """
    Build analysis_report.html from known artifacts under output_dir.
    Returns a dict with status, checks (for JSON), and optional error.
    """
    base = Path(output_dir).resolve()
    checks = {
        "tables_rendered": False,
        "images_embedded": False,
        "sections_created": False,
    }
    out: dict[str, Any] = {"status": "fail", "checks": checks}

    pickup_csv = base / "pickup_location_booking_counts.csv"
    if not pickup_csv.is_file():
        out["error"] = "pickup_location_booking_counts.csv missing"
        return out

    with pickup_csv.open(newline="", encoding="utf-8", errors="replace") as f:
        n_data = sum(1 for _ in csv.reader(f)) - 1
    if n_data < 1:
        out["error"] = "pickup_location_booking_counts.csv has no data rows"
        return out

    schema_txt = base / "schema_summary.txt"
    dq_csv = base / "pickup_location_data_quality.csv"
    bar_png = base / "pickup_location_booking_counts_bar.png"

    h1_summary = base / "hypothesis_1_judge_summary.txt"
    h1_csv = base / "hypothesis_1_vehicle_type_booking_value_summary.csv"
    h1_png = base / "hypothesis_1_average_booking_value_by_vehicle_type.png"
    h2_summary = base / "hypothesis_2_judge_summary.txt"
    h2_csv = base / "hypothesis_2_correlation_matrix.csv"
    h2_png = base / "hypothesis_2_correlation_heatmap.png"

    # Fallback globs if exact names differ slightly
    if not h1_summary.is_file():
        h1_summary = _find_first(base, ("hypothesis_1*judge*summary*.txt",)) or h1_summary
    if not h1_csv.is_file():
        h1_csv = _find_first(base, ("hypothesis_1*vehicle*booking*value*summary*.csv",)) or h1_csv
    if not h1_png.is_file():
        h1_png = _find_first(base, ("hypothesis_1*average*booking*value*.png",)) or h1_png
    if not h2_summary.is_file():
        h2_summary = _find_first(base, ("hypothesis_2*judge*summary*.txt",)) or h2_summary
    if not h2_csv.is_file():
        h2_csv = _find_first(base, ("hypothesis_2*correlation*matrix*.csv",)) or h2_csv
    if not h2_png.is_file():
        h2_png = _find_first(base, ("hypothesis_2*correlation*heatmap*.png",)) or h2_png

    sections_html: list[str] = []
    tables_ok = True
    images_ok = True

    # Executive summary: short lead from pickup CSV top rows + schema line count
    try:
        with pickup_csv.open(newline="", encoding="utf-8", errors="replace") as f:
            r = csv.reader(f)
            hdr = next(r, [])
            top_row = next(r, None)
        summary_bits = [
            "Pickup location booking totals were aggregated from the cleaned dataset.",
            f"The ranking table contains {n_data} location rows with data.",
        ]
        if top_row and hdr:
            pairs = []
            for i, c in enumerate(top_row):
                col = hdr[i] if i < len(hdr) else f"col{i}"
                pairs.append(f"{col}: {(c or '').strip()}")
            summary_bits.append("First data row: " + "; ".join(pairs) + ".")
        exec_inner = _paragraphs_from_text("\n\n".join(summary_bits))
    except Exception:
        exec_inner = '<p class="rb-p">See tables below for quantitative results.</p>'

    sections_html.append(_section("Executive Summary", exec_inner))

    if schema_txt.is_file():
        sections_html.append(_section("Data Overview", _paragraphs_from_text(_read_text(schema_txt))))
    else:
        sections_html.append(_section("Data Overview", "<p class=\"rb-note\">schema_summary.txt not found.</p>"))

    pickup_block = [_csv_to_html_table(pickup_csv)]
    if bar_png.is_file():
        pickup_block.append(_img_tag(bar_png, base, "Pickup location booking counts"))
    else:
        images_ok = False
    sections_html.append(_section("Pickup Location Analysis", "\n".join(pickup_block)))

    if dq_csv.is_file():
        sections_html.append(_section("Data Quality", _csv_to_html_table(dq_csv)))
    else:
        sections_html.append(_section("Data Quality", "<p class=\"rb-note\">pickup_location_data_quality.csv not found.</p>"))
        tables_ok = False

    h1_parts: list[str] = []
    if h1_summary.is_file():
        h1_parts.append(_paragraphs_from_text(_read_text(h1_summary)))
    if h1_csv.is_file():
        h1_parts.append(_csv_to_html_table(h1_csv))
    elif h1_summary.is_file() or h1_png.is_file():
        tables_ok = False
    if h1_png.is_file():
        h1_parts.append(_img_tag(h1_png, base, "Average booking value by vehicle type"))
    elif h1_csv.is_file() or h1_summary.is_file():
        images_ok = False
    if h1_parts:
        sections_html.append(_section("Vehicle type vs. booking value", "\n".join(h1_parts)))

    h2_parts: list[str] = []
    if h2_summary.is_file():
        h2_parts.append(_paragraphs_from_text(_read_text(h2_summary)))
    if h2_csv.is_file():
        h2_parts.append(_csv_to_html_table(h2_csv))
    elif h2_summary.is_file() or h2_png.is_file():
        tables_ok = False
    if h2_png.is_file():
        h2_parts.append(_img_tag(h2_png, base, "Correlation heatmap"))
    elif h2_csv.is_file() or h2_summary.is_file():
        images_ok = False
    if h2_parts:
        sections_html.append(_section("Ride distance vs. booking value", "\n".join(h2_parts)))

    checks["tables_rendered"] = tables_ok
    checks["images_embedded"] = images_ok
    checks["sections_created"] = len(sections_html) >= 4

    css = """<style>.rb-wrap{font-family:Arial,Helvetica,sans-serif;max-width:960px;margin:0 auto;padding:1rem;color:#111}.rb-h1{font-size:1.5rem;color:#003DA5}.rb-h2{font-size:1.15rem;color:#003DA5;margin-top:1.25rem}.rb-table{width:100%;border-collapse:collapse;margin:.75rem 0;font-size:.9rem}.rb-table th,.rb-table td{border:1px solid #ccc;padding:.35rem .5rem;text-align:left}.rb-table thead{background:#f0f4fa}.rb-p{line-height:1.5;margin:.5rem 0}.rb-note{color:#666;font-size:.9rem}.rb-fig{margin:1rem 0}.rb-fig img{max-width:100%;height:auto}</style>"""

    doc = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width, initial-scale=1"/><title>Analysis Report</title>{css}</head><body><div class="rb-wrap"><h1 class="rb-h1">Analysis Report</h1>
{chr(10).join(sections_html)}
</div></body></html>"""

    report_path = base / "analysis_report.html"
    report_path.write_text(doc, encoding="utf-8")

    out["status"] = "success"
    out["checks"] = checks
    out["output_path"] = str(report_path)
    return out


def try_rebuild_data_driven_report(output_dir: str | Path) -> dict[str, Any] | None:
    """
    If pickup_location_booking_counts.csv exists with data, rebuild report.
    Otherwise return None (caller keeps narrator HTML).
    """
    base = Path(output_dir).resolve()
    pickup = base / "pickup_location_booking_counts.csv"
    if not pickup.is_file():
        return None
    try:
        with pickup.open(newline="", encoding="utf-8", errors="replace") as f:
            n = sum(1 for _ in csv.reader(f)) - 1
        if n < 1:
            return None
    except OSError:
        return None
    return rebuild_analysis_report_html(base)
