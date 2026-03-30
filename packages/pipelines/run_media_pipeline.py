#!/usr/bin/env python3
"""
================================================================================
Standalone Media Pipeline Runner
================================================================================

Runs the full media citation pipeline end-to-end, independently of the main
Bellwether daily pipeline. Can also be imported and called from
pipeline_daily_refresh.py for integrated runs.

Usage:
    # Standalone (uses defaults)
    python run_media_pipeline.py

    # Custom output directory (e.g. for a standalone site)
    python run_media_pipeline.py --output-dir /path/to/site/data

    # Skip steps
    python run_media_pipeline.py --skip-discover   # reuse existing raw citations
    python run_media_pipeline.py --skip-fragility   # skip fragility calc (just match + generate)

    # Standalone site mode: generate a self-contained site directory
    python run_media_pipeline.py --standalone --output-dir /path/to/site

================================================================================
"""

import argparse
import subprocess
import sys
import time
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from config import DATA_DIR, WEBSITE_DIR, SCRIPTS_DIR

PIPELINE_STEPS = [
    ("pipeline_media_discover_citations.py", "Discover citations (GDELT)"),
    ("pipeline_media_extract_markets.py",    "Extract & match market references"),
    ("pipeline_media_calculate_fragility.py", "Calculate fragility metrics"),
    ("generate_media_web_data.py",           "Generate website JSON"),
]


def run_step(script_name, description, script_dir=None, extra_args=None):
    """Run a pipeline script. Returns True on success."""
    base = script_dir or SCRIPTS_DIR
    script_path = base / script_name

    if not script_path.exists():
        print(f"  SKIP  {description} (script not found: {script_name})")
        return False

    print(f"  RUN   {description}")
    start = time.time()

    cmd = [sys.executable, str(script_path)]
    if extra_args:
        cmd.extend(extra_args)

    result = subprocess.run(
        cmd,
        capture_output=True, text=True, timeout=1800,
    )

    elapsed = time.time() - start
    status = "OK" if result.returncode == 0 else "FAIL"
    print(f"  {status}    {description} ({elapsed:.1f}s)")

    if result.returncode != 0 and result.stderr:
        for line in result.stderr.strip().split("\n")[:5]:
            print(f"         {line}")

    return result.returncode == 0


def copy_standalone_assets(output_dir):
    """
    Copy media.html, js/media.js, css/styles.css, and favicon to output_dir
    so the media page works as a self-contained static site.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "js").mkdir(exist_ok=True)
    (output_dir / "css").mkdir(exist_ok=True)
    (output_dir / "data").mkdir(exist_ok=True)

    src = WEBSITE_DIR  # packages/docs/ or docs/
    copies = [
        ("media.html", "index.html"),   # Serve as index for standalone
        ("js/media.js", "js/media.js"),
        ("css/styles.css", "css/styles.css"),
    ]

    # Also copy media.html as-is for direct linking
    copies.append(("media.html", "media.html"))

    for src_rel, dst_rel in copies:
        src_path = src / src_rel
        if src_path.exists():
            dst_path = output_dir / dst_rel
            shutil.copy2(src_path, dst_path)
            print(f"  COPY  {src_rel} -> {dst_rel}")

    # Copy favicon if it exists
    for fav in ("favicon.svg", "favicon.ico"):
        fav_path = src / fav
        if fav_path.exists():
            shutil.copy2(fav_path, output_dir / fav)

    # Copy web data JSONs
    for name in ("media_summary.json", "media_outlets.json", "media_citations.json"):
        src_json = src / "data" / name
        if src_json.exists():
            shutil.copy2(src_json, output_dir / "data" / name)
            print(f"  COPY  data/{name}")


def run_pipeline(skip_discover=False, skip_fragility=False, output_dir=None, standalone=False, backfill=None):
    """
    Run the media pipeline. Returns True if all steps succeeded.

    Args:
        skip_discover: Skip GDELT API fetch, reuse existing raw citations
        skip_fragility: Skip fragility calculation
        output_dir: Custom output directory for web JSON (None = default docs/data/)
        standalone: If True, also copy HTML/CSS/JS assets for self-contained site
        backfill: Number of days to look back for discovery (e.g. 30)
    """
    print("=" * 60)
    print("MEDIA CITATION PIPELINE")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    skips = set()
    if skip_discover:
        skips.add("pipeline_media_discover_citations.py")
    if skip_fragility:
        skips.add("pipeline_media_calculate_fragility.py")

    results = {}
    all_ok = True

    for script, desc in PIPELINE_STEPS:
        if script in skips:
            print(f"  SKIP  {desc} (--skip flag)")
            results[script] = None
            continue

        extra_args = None
        if script == "pipeline_media_discover_citations.py" and backfill:
            extra_args = ["--backfill", str(backfill)]

        ok = run_step(script, desc, extra_args=extra_args)
        results[script] = ok

        if not ok:
            all_ok = False
            # Don't abort on discover/extract failure — try to generate web data
            # with whatever we have. But do skip downstream matching/fragility
            # if discover failed.
            if script == "pipeline_media_discover_citations.py":
                print("  WARN  Discovery failed, skipping extract + fragility")
                skips.add("pipeline_media_extract_markets.py")
                skips.add("pipeline_media_calculate_fragility.py")
            elif script == "pipeline_media_extract_markets.py":
                print("  WARN  Extraction failed, skipping fragility")
                skips.add("pipeline_media_calculate_fragility.py")

    # If custom output_dir, copy the generated JSON there
    if output_dir:
        output_path = Path(output_dir)
        (output_path / "data").mkdir(parents=True, exist_ok=True)
        web_data_dir = WEBSITE_DIR / "data"
        for name in ("media_summary.json", "media_outlets.json", "media_citations.json"):
            src = web_data_dir / name
            if src.exists():
                shutil.copy2(src, output_path / "data" / name)
                print(f"  COPY  {name} -> {output_path / 'data' / name}")

    # Standalone mode: copy all assets
    if standalone and output_dir:
        print("\n  Copying standalone site assets...")
        copy_standalone_assets(output_dir)

    # Summary
    print("\n" + "=" * 60)
    ok_count = sum(1 for v in results.values() if v is True)
    fail_count = sum(1 for v in results.values() if v is False)
    skip_count = sum(1 for v in results.values() if v is None)
    print(f"Done: {ok_count} OK, {fail_count} FAIL, {skip_count} SKIP")
    print("=" * 60)

    return all_ok


def main():
    parser = argparse.ArgumentParser(
        description="Run the media citation pipeline (standalone or integrated)"
    )
    parser.add_argument(
        "--skip-discover", action="store_true",
        help="Skip GDELT discovery, reuse existing raw citations"
    )
    parser.add_argument(
        "--skip-fragility", action="store_true",
        help="Skip fragility calculation"
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Custom output directory for web data (default: docs/data/)"
    )
    parser.add_argument(
        "--standalone", action="store_true",
        help="Generate a self-contained site directory with HTML/CSS/JS + data"
    )
    parser.add_argument(
        "--backfill", type=int, metavar="DAYS",
        help="Force discovery to look back N days (e.g. --backfill 30)"
    )

    args = parser.parse_args()

    ok = run_pipeline(
        skip_discover=args.skip_discover,
        skip_fragility=args.skip_fragility,
        output_dir=args.output_dir,
        standalone=args.standalone,
        backfill=args.backfill,
    )

    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
