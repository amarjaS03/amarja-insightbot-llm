"""
Deterministic HTML report from job output artifacts (CSVs, TXTs, PNGs).
Overwrites analysis_report.html when required inputs validate.
"""

from __future__ import annotations

import csv
import html
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


def _header_suggests_numeric(h: str) -> bool:
    s = (h or "").strip().lower()
    keys = (
        "rank",
        "count",
        "pct",
        "percent",
        "share",
        "ratio",
        "total",
        "mean",
        "avg",
        "sum",
        "value",
        "amount",
        "statistic",
        "p-value",
        "p_value",
    )
    if s in {"n", "#"}:
        return True
    return any(k in s for k in keys)


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
    num_cols = [_header_suggests_numeric(h) for h in header]
    ths = "".join(f"<th>{html.escape(h)}</th>" for h in header)
    trs = []
    for row in body:
        cells = row + [""] * (len(header) - len(row))
        cells = cells[: len(header)]
        tds_parts = []
        for i, c in enumerate(cells):
            cls = ' class="rb-num"' if i < len(num_cols) and num_cols[i] else ""
            tds_parts.append(f"<td{cls}>{html.escape(c)}</td>")
        trs.append("<tr>" + "".join(tds_parts) + "</tr>")
    tbody = "\n".join(trs)
    inner = (
        f'<table class="rb-table"><thead><tr>{ths}</tr></thead><tbody>\n{tbody}\n</tbody></table>'
    )
    return f'<div class="rb-table-wrap">{inner}</div>'


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
    cap = html.escape(alt, quote=False)
    return (
        f'<figure class="rb-fig"><img src="{rel}" alt="{alt_e}" loading="lazy"/>'
        f'<figcaption class="rb-fig-cap">{cap}</figcaption></figure>'
    )


def _section(title: str, inner: str) -> str:
    tid = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-") or "section"
    return (
        f'<section class="rb-section" id="{html.escape(tid, quote=True)}">'
        f'<h2 class="rb-h2"><span class="rb-h2-line"></span>{html.escape(title)}</h2>\n{inner}\n</section>'
    )


def _callout_missing(message: str) -> str:
    esc = html.escape(message, quote=False)
    return (
        f'<div class="rb-callout" role="status"><strong class="rb-callout-title">Note</strong>'
        f'<p class="rb-callout-text">{esc}</p></div>'
    )


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
        sections_html.append(
            _section("Data Overview", _callout_missing("Schema summary was not written for this run (optional file missing)."))
        )

    pickup_block = [_csv_to_html_table(pickup_csv)]
    if bar_png.is_file():
        pickup_block.append(_img_tag(bar_png, base, "Pickup location booking counts"))
    else:
        images_ok = False
    sections_html.append(_section("Pickup Location Analysis", "\n".join(pickup_block)))

    if dq_csv.is_file():
        sections_html.append(_section("Data Quality", _csv_to_html_table(dq_csv)))
    else:
        sections_html.append(
            _section(
                "Data Quality",
                _callout_missing("Location data-quality metrics file was not produced for this run (optional)."),
            )
        )
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

    css = """<style>
:root{--navy:#003366;--blue:#0056b3;--blue-soft:#e8f0fa;--text:#1a1a1a;--muted:#555;--line:#d0d7e2;--paper:#fff;--canvas:#f4f7fb;--zebra:#eef3f9;}
*,*::before,*::after{box-sizing:border-box;}
body{margin:0;background:var(--canvas);color:var(--text);font-family:Arial,Helvetica,sans-serif;font-size:15px;line-height:1.55;}
.rb-wrap{max-width:920px;margin:0 auto;padding:1.75rem 1.25rem 2.5rem;}
.rb-sheet{background:var(--paper);box-shadow:0 0 0 1px var(--line);padding:1.5rem 1.35rem 2rem;border-radius:2px;}
.rb-h1{font-size:1.4rem;font-weight:700;color:var(--navy);margin:0 0 .25rem;padding-bottom:.6rem;border-bottom:3px solid var(--blue);}
.rb-sub{color:var(--muted);font-size:.92rem;margin:0 0 1.25rem;}
.rb-section{margin:0;padding:0;}
.rb-section + .rb-section{margin-top:1.5rem;padding-top:1.35rem;border-top:2px solid var(--blue);}
.rb-h2{font-size:1.08rem;font-weight:700;color:var(--blue);margin:0 0 .75rem;padding-bottom:.45rem;border-bottom:2px solid var(--blue);display:flex;align-items:center;gap:.5rem;}
.rb-h2-line{display:inline-block;width:4px;min-height:1.1em;background:var(--blue);border-radius:1px;flex-shrink:0;}
.rb-p{margin:.55rem 0 .85rem;line-height:1.55;color:var(--text);}
.rb-note{color:var(--muted);font-size:.88rem;margin:.5rem 0;}
.rb-table-wrap{margin:.85rem 0;border:1px solid var(--line);border-radius:4px;overflow:visible;}
.rb-table{width:100%;border-collapse:collapse;font-size:.9rem;}
.rb-table thead th{background:var(--blue-soft);color:var(--navy);font-weight:700;text-align:left;padding:.55rem .65rem;border-bottom:2px solid var(--blue);}
.rb-table tbody td{padding:.48rem .65rem;border-bottom:1px solid var(--line);vertical-align:top;}
.rb-table tbody tr:nth-child(even){background:var(--zebra);}
.rb-num{text-align:right;font-variant-numeric:tabular-nums;}
.rb-fig{margin:1.1rem 0 0;text-align:center;}
.rb-fig img{max-width:100%;height:auto;border:1px solid var(--line);border-radius:2px;}
.rb-fig-cap{margin-top:.45rem;font-size:.86rem;color:var(--blue);font-weight:600;}
.rb-callout{margin:.75rem 0;padding:.75rem 1rem;background:var(--blue-soft);border-left:4px solid var(--blue);border-radius:0 3px 3px 0;}
.rb-callout-title{color:var(--navy);display:block;margin-bottom:.25rem;font-size:.92rem;}
.rb-callout-text{margin:0;font-size:.9rem;color:var(--text);}
@media print{
  body{background:#fff;}
  .rb-wrap{max-width:100%;padding:0;}
  .rb-sheet{box-shadow:none;border:none;}
}
*,*::before,*::after{max-height:none!important;height:auto!important;overflow:visible!important;}
</style>"""

    doc = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width, initial-scale=1"/><title>Analysis Report</title>{css}</head><body><div class="rb-wrap"><div class="rb-sheet"><h1 class="rb-h1">Analysis Report</h1>
<p class="rb-sub">Executive summary of automated analysis outputs (slim / data-driven view).</p>
{chr(10).join(sections_html)}
</div></div></body></html>"""

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
