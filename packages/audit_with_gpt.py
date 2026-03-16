#!/usr/bin/env python3
"""
Audit the Bellwether pipeline using GPT-5.2.

Sends all active pipeline code to the OpenAI API in focused passes,
then aggregates the findings into a concise ~3-page PDF audit report.

Usage:
    python audit_with_gpt.py

Output:
    audit_report.pdf  - Formatted PDF report
    audit_report.md   - Raw markdown (for reference)

Cost estimate: ~$1.25-1.50
"""

import os
import sys
import re
import time
from pathlib import Path
from datetime import datetime
from openai import OpenAI
from fpdf import FPDF

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MODEL = "gpt-5.2"
BASE_DIR = Path(__file__).resolve().parent
PIPELINES_DIR = BASE_DIR / "packages" / "pipelines"
OUTPUT_MD = BASE_DIR / "audit_report.md"
OUTPUT_PDF = BASE_DIR / "audit_report.pdf"

# Get API key from environment or file
api_key = os.environ.get("OPENAI_API_KEY")
if not api_key:
    key_file = BASE_DIR / "openai_api_key.txt"
    if key_file.exists():
        api_key = key_file.read_text().strip()
if not api_key:
    print("ERROR: No OPENAI_API_KEY found. Set the env var or create openai_api_key.txt")
    sys.exit(1)

client = OpenAI(api_key=api_key)

# ---------------------------------------------------------------------------
# File groups — split to stay within context limits
# ---------------------------------------------------------------------------

PASS_1_FILES = [
    "packages/pipelines/config.py",
    "packages/pipelines/logging_config.py",
    "packages/pipelines/pipeline_daily_refresh.py",
    "packages/pipelines/pipeline_discover_markets_v2.py",
    "packages/pipelines/pipeline_classify_kalshi_events.py",
    "packages/pipelines/pipeline_classify_categories.py",
    "packages/pipelines/pipeline_classify_electoral.py",
    "packages/pipelines/pipeline_check_resolutions.py",
    "packages/pipelines/pipeline_merge_to_master.py",
    "packages/pipelines/pipeline_reclassify_incomplete.py",
    "packages/pipelines/pipeline_get_election_dates.py",
    "packages/pipelines/pipeline_select_election_winners.py",
    "packages/pipelines/pipeline_refresh_political_tags.py",
    "packages/pipelines/pipeline_election_eve_prices.py",
]

PASS_2_FILES = [
    "packages/pipelines/pull_polymarket_prices.py",
    "packages/pipelines/pull_kalshi_prices.py",
    "packages/pipelines/truncate_polymarket_prices.py",
    "packages/pipelines/truncate_kalshi_prices.py",
    "packages/pipelines/fetch_orderbooks.py",
    "packages/pipelines/fetch_pm_event_slugs.py",
    "packages/pipelines/fetch_resolution_prices.py",
    "packages/pipelines/fetch_panel_a_trades.py",
    "packages/pipelines/pull_trades_for_vwap.py",
    "packages/pipelines/enrich_markets_with_api_data.py",
    "packages/pipelines/create_tickers.py",
    "packages/pipelines/postprocess_tickers.py",
    "packages/pipelines/build_tree.py",
    "packages/pipelines/fix_name_collisions.py",
    "packages/pipelines/generate_market_map.py",
]

PASS_3_FILES = [
    "packages/pipelines/calculate_all_political_brier_scores.py",
    "packages/pipelines/create_brier_cohorts.py",
    "packages/pipelines/brier_score_analysis.py",
    "packages/pipelines/calibration_density_plots.py",
    "packages/pipelines/calibration_density_plots_elections.py",
    "packages/pipelines/election_winner_markets_comparison.py",
    "packages/pipelines/partisan_bias_calibration.py",
    "packages/pipelines/table_partisan_bias_regression.py",
    "packages/pipelines/generate_web_data.py",
    "packages/pipelines/generate_monitor_data.py",
    "packages/pipelines/generate_civic_elections.py",
    "packages/pipelines/export_liquidity_for_website.py",
    "packages/pipelines/export_liquidity_timeseries.py",
    "packages/pipelines/calculate_liquidity_metrics.py",
    "packages/pipelines/generate_liquidity_analysis.py",
    "packages/pipelines/aggregate_trader_partisanship.py",
]

PASS_4_FILES = [
    "packages/api/worker.js",
    "packages/website/server/cloudflare-worker.js",
    "packages/website/server/wrangler.toml",
    "packages/pipelines/test_pipeline_components.py",
    "packages/pipelines/test_pipeline_sample.py",
    "packages/pipelines/audit/audit_validator.py",
    "packages/pipelines/audit/audit_anomaly.py",
    "packages/pipelines/audit/audit_changelog.py",
    "packages/pipelines/audit/audit_daily_summary.py",
    "packages/pipelines/audit/audit_config.py",
    "packages/pipelines/audit/audit_gpt_logger.py",
    "packages/pipelines/audit/__init__.py",
    "packages/pipelines/prompts/ticker_prompt.md",
]

PASSES = [
    ("Core Pipeline Orchestration & Classification",  PASS_1_FILES),
    ("Price Fetching, Truncation & Market Matching",   PASS_2_FILES),
    ("Analysis, Brier Scores & Web Data Generation",   PASS_3_FILES),
    ("Workers, Tests & Audit Infrastructure",          PASS_4_FILES),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_files(file_list: list) -> str:
    """Concatenate files into a single string with markers."""
    parts = []
    for rel_path in file_list:
        full_path = BASE_DIR / rel_path
        if not full_path.exists():
            parts.append(f"\n{'='*60}\n# FILE: {rel_path} [NOT FOUND]\n{'='*60}\n")
            continue
        content = full_path.read_text(errors="replace")
        parts.append(f"\n{'='*60}\n# FILE: {rel_path}\n{'='*60}\n{content}")
    return "\n".join(parts)


def call_gpt(system_prompt: str, user_prompt: str, pass_name: str) -> str:
    """Call GPT-5.2 and return the response text."""
    print(f"\n  Sending {pass_name}...")
    start = time.time()

    response = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.2,
    )

    result = response.choices[0].message.content
    elapsed = time.time() - start
    tokens_in = response.usage.prompt_tokens
    tokens_out = response.usage.completion_tokens
    cost_in = tokens_in * 1.75 / 1_000_000
    cost_out = tokens_out * 14.0 / 1_000_000
    print(f"  Done in {elapsed:.0f}s ({tokens_in:,} in / {tokens_out:,} out) "
          f"[${cost_in + cost_out:.2f}]")
    return result, tokens_in, tokens_out


# ---------------------------------------------------------------------------
# PDF generation
# ---------------------------------------------------------------------------

class AuditPDF(FPDF):
    """PDF with header/footer for the audit report."""

    def header(self):
        self.set_font("Helvetica", "B", 9)
        self.set_text_color(100, 100, 100)
        self.cell(0, 8, "Bellwether Platform - Pipeline Audit Report", align="L")
        self.cell(0, 8, datetime.now().strftime("%B %d, %Y"), align="R", new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(200, 200, 200)
        self.line(self.l_margin, self.get_y(), self.w - self.r_margin, self.get_y())
        self.ln(4)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(128, 128, 128)
        self.cell(0, 10, f"Page {self.page_no()}/{{nb}}", align="C")


def _sanitize(text: str) -> str:
    """Replace Unicode chars that Helvetica can't render."""
    replacements = {
        "\u2014": "--",   # em-dash
        "\u2013": "-",    # en-dash
        "\u2018": "'",    # left single quote
        "\u2019": "'",    # right single quote
        "\u201c": '"',    # left double quote
        "\u201d": '"',    # right double quote
        "\u2026": "...",  # ellipsis
        "\u2022": "-",    # bullet
        "\u2192": "->",   # arrow
        "\u2713": "[x]",  # checkmark
        "\u2717": "[ ]",  # cross
        "\u00a0": " ",    # non-breaking space
    }
    for ch, repl in replacements.items():
        text = text.replace(ch, repl)
    # Strip any remaining non-latin-1 characters
    return text.encode("latin-1", errors="replace").decode("latin-1")


def markdown_to_pdf(md_text: str, output_path: Path):
    """Convert markdown text to a formatted PDF."""
    md_text = _sanitize(md_text)
    pdf = AuditPDF(orientation="P", unit="mm", format="A4")
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.add_page()

    width = pdf.w - pdf.l_margin - pdf.r_margin

    for line in md_text.split("\n"):
        stripped = line.strip()

        # Thematic break
        if stripped in ("---", "***", "___"):
            pdf.ln(2)
            pdf.set_draw_color(200, 200, 200)
            pdf.line(pdf.l_margin, pdf.get_y(), pdf.w - pdf.r_margin, pdf.get_y())
            pdf.ln(4)
            continue

        # Headings
        if stripped.startswith("# ") and not stripped.startswith("## "):
            pdf.ln(4)
            pdf.set_font("Helvetica", "B", 16)
            pdf.set_text_color(0, 0, 0)
            pdf.multi_cell(width, 8, _clean_md(stripped.lstrip("# ")))
            pdf.ln(2)
            continue
        if stripped.startswith("## "):
            pdf.ln(3)
            pdf.set_font("Helvetica", "B", 13)
            pdf.set_text_color(30, 30, 30)
            pdf.multi_cell(width, 7, _clean_md(stripped.lstrip("# ")))
            pdf.ln(1.5)
            continue
        if stripped.startswith("### "):
            pdf.ln(2)
            pdf.set_font("Helvetica", "B", 11)
            pdf.set_text_color(50, 50, 50)
            pdf.multi_cell(width, 6, _clean_md(stripped.lstrip("# ")))
            pdf.ln(1)
            continue

        # Empty line
        if not stripped:
            pdf.ln(3)
            continue

        # Bullet points — render as "  - text"
        if stripped.startswith("- ") or stripped.startswith("* "):
            pdf.set_font("Helvetica", "", 10)
            pdf.set_text_color(0, 0, 0)
            text = "  -  " + _clean_md(stripped[2:])
            pdf.multi_cell(width, 5, text)
            continue

        # Numbered list — render as "  1. text"
        num_match = re.match(r"^(\d+)\.\s+(.*)", stripped)
        if num_match:
            pdf.set_font("Helvetica", "", 10)
            pdf.set_text_color(0, 0, 0)
            text = f"  {num_match.group(1)}.  {_clean_md(num_match.group(2))}"
            pdf.multi_cell(width, 5, text)
            continue

        # Regular paragraph
        pdf.set_font("Helvetica", "", 10)
        pdf.set_text_color(0, 0, 0)
        pdf.multi_cell(width, 5, _clean_md(stripped))

    pdf.output(str(output_path))


def _clean_md(text: str) -> str:
    """Strip markdown formatting for plain-text PDF rendering."""
    text = re.sub(r'\*\*([^*]+)\*\*', r'\1', text)  # bold
    text = re.sub(r'\*([^*]+)\*', r'\1', text)       # italic
    text = re.sub(r'`([^`]+)`', r'\1', text)         # code
    return text


# ---------------------------------------------------------------------------
# Audit prompts
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are a senior software engineer conducting a thorough code audit of the \
Bellwether Platform — a data pipeline that collects, classifies, and analyzes \
political prediction market data from Polymarket and Kalshi.

Your audit should be rigorous, specific, and actionable. For each issue, cite \
the exact file and line number. Categorize findings as:
  - CRITICAL: Bugs that will cause data loss, incorrect results, or runtime failures
  - HIGH: Issues that could cause silent data quality problems or security risks
  - MEDIUM: Code quality, maintainability, or robustness concerns
  - LOW: Style, naming, or minor improvement suggestions

Focus on:
1. Correctness: Logic errors, off-by-one, wrong data types, missing edge cases
2. Data integrity: Race conditions, partial writes, missing validation, data loss paths
3. API usage: Incorrect endpoints, missing error handling, rate limiting issues
4. Security: API key exposure, injection risks, unsafe file operations
5. Reliability: Missing retries, no timeouts, unbounded loops, memory issues
6. Architecture: Circular dependencies, tight coupling, missing abstractions
"""

PASS_PROMPT_TEMPLATE = """\
## Audit Pass: {pass_name}

Review the following {file_count} source files thoroughly.

For each finding, provide:
- Severity: CRITICAL / HIGH / MEDIUM / LOW
- File: exact file path
- Line(s): approximate line number(s)
- Issue: what's wrong
- Recommendation: how to fix it

Also note anything that is particularly well-implemented.

At the end, provide a summary: total findings by severity, and the top 3 \
most important things to fix.

---

{code}
"""

SYNTHESIS_PROMPT = """\
You are writing a final audit report for the Bellwether Platform pipeline \
(a political prediction market data system). Below are findings from 4 \
separate audit passes reviewing 58 source files (~36,000 lines of code).

Write a concise, professional report (~2000-2500 words) structured as:

1. EXECUTIVE SUMMARY (2-3 paragraphs): overall code health assessment, \
key strengths, primary risks, and top-level recommendation.

2. CRITICAL & HIGH FINDINGS: consolidated, deduplicated list of the most \
important issues. For each: severity, file:line, description, recommendation.

3. MEDIUM FINDINGS: consolidated list, grouped by theme (data integrity, \
error handling, etc.)

4. LOW FINDINGS: brief bullet list.

5. ARCHITECTURE & DESIGN: what's working well, what could improve.

6. PRIORITIZED ACTION ITEMS: top 10 numbered items, most impactful first.

Deduplicate issues that appear across passes. Keep specific file references. \
Be direct — this report will be read by the project lead.

---

{all_findings}
"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("BELLWETHER PIPELINE AUDIT (via GPT-5.2)")
    print("=" * 60)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Model: {MODEL}")
    print(f"Estimated cost: ~$1.25-1.50")

    all_findings = []
    total_in = 0
    total_out = 0

    for i, (pass_name, file_list) in enumerate(PASSES, 1):
        print(f"\n--- Pass {i}/4: {pass_name} ---")
        code = load_files(file_list)
        char_count = len(code)
        print(f"  Files: {len(file_list)}, Characters: {char_count:,}")

        prompt = PASS_PROMPT_TEMPLATE.format(
            pass_name=pass_name,
            file_count=len(file_list),
            code=code,
        )

        findings, tok_in, tok_out = call_gpt(SYSTEM_PROMPT, prompt, pass_name)
        total_in += tok_in
        total_out += tok_out
        all_findings.append(f"# Pass {i}: {pass_name}\n\n{findings}")

    # Synthesis pass
    print(f"\n--- Synthesis: Aggregating findings ---")
    combined = "\n\n---\n\n".join(all_findings)
    report, tok_in, tok_out = call_gpt(
        "You are a senior software engineer writing a final audit report.",
        SYNTHESIS_PROMPT.format(all_findings=combined),
        "Synthesis",
    )
    total_in += tok_in
    total_out += tok_out

    # Cost summary
    cost_in = total_in * 1.75 / 1_000_000
    cost_out = total_out * 14.0 / 1_000_000
    total_cost = cost_in + cost_out
    print(f"\n  Total tokens: {total_in:,} in / {total_out:,} out")
    print(f"  Total cost: ${total_cost:.2f}")

    # Write markdown
    full_md = f"""# Bellwether Pipeline Audit Report

Generated: {datetime.now().strftime('%B %d, %Y')}
Model: {MODEL}
Files audited: {sum(len(fl) for _, fl in PASSES)}
Total cost: ${total_cost:.2f}

---

{report}
"""
    OUTPUT_MD.write_text(full_md)
    print(f"\n  Markdown: {OUTPUT_MD}")

    # Generate PDF
    print(f"  Generating PDF...")
    markdown_to_pdf(full_md, OUTPUT_PDF)
    pdf_size = OUTPUT_PDF.stat().st_size / 1024
    print(f"  PDF: {OUTPUT_PDF} ({pdf_size:.0f} KB)")

    print(f"\n{'=' * 60}")
    print(f"Audit complete!")
    print(f"  PDF:      {OUTPUT_PDF}")
    print(f"  Markdown: {OUTPUT_MD}")
    print(f"  Cost:     ${total_cost:.2f}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
