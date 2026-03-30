# Bellwether Project Notes

## Market Feedback Review

### Data Source
User-submitted market feedback from the "Help Us Match Markets" feature:
```
https://docs.google.com/spreadsheets/d/e/2PACX-1vRPiDl8J5hruzzB3_CR83cDz1xrVob9XAgZn_cyfulKX4e3oBGmSbUvP_Ax4hSoesSDoDJXffWtqvjI/pub?output=csv
```

### CSV Columns
- `Timestamp` - When feedback was submitted
- `Feedback Type` - `same-event`, `not-political`, `wrong-category`, `other`
- `Description` - User's notes
- `Market Count` - Number of markets in submission
- `Markets (JSON)` - Array of market objects with keys, labels, platforms

### Review Process
1. Fetch the CSV using the URL above
2. Check `last_reviewed_timestamp` below to find where we left off
3. Review new rows since that timestamp
4. For each issue:
   - `not-political`: Consider removing from political markets
   - `wrong-category`: Fix category classification
   - `same-event`: Link markets for cross-platform comparison
   - `other`: Investigate the specific issue noted
5. Update `last_reviewed_timestamp` after each session

### Review Log
| Date | Last Reviewed Timestamp | Rows Reviewed | Actions Taken |
|------|------------------------|---------------|---------------|
| 2026-02-10 | (none) | 0 | Initial setup |
| 2026-02-10 | 2026-02-11T02:52:03.673Z | 10 | See actions below |
| 2026-02-12 | 2026-02-12T22:41:28.460Z | 17 | See actions below |

#### 2026-02-10 Review Actions:
1. **NOT_POLITICAL**: Marked 60 markets (Naismith basketball, KOSPI, Nasdaq, SEC basketball, Google AI)
2. **PARTISAN_CONTROL**: Recategorized 17 Senate control markets (not specific races)
3. **CANDIDACY_ANNOUNCEMENT**: Recategorized 61 Kalshi "who will run" markets (not comparable to "will win")
4. **Noted issues to fix later**:
   - All 11,008 electoral markets missing `candidate` field
   - Market URLs not stored in data
   - Colombia president market matching needs investigation

#### 2026-02-12 Review Actions:
1. **NOT_POLITICAL**: Updated 1 market (KXMUSKBLUESKY-26). Most flagged markets (Harvey Weinstein, S&P Bitcoin, MicroStrategy, Viking Therapeutics, 9 Elon markets) were already marked NOT_POLITICAL.
2. **WRONG CATEGORY**: Verified CONTROLS-2026-D/R already in PARTISAN_CONTROL, KXSENATEMED-26-GRA and KXPRESNOMR-28-STE already ELECTORAL. No changes needed.
3. **SAME-EVENT**: Kevin Warsh Fed Chair markets (KXFEDCHAIRNOM-29-KW + Polymarket 572469) identified for matching. Both exist in data.
4. **DATA ISSUES noted for investigation**:
   - Monitor grouping issues with KY/GA/SC primaries (wrong Kalshi matches in grouped view)
   - Dutch PM Rob Jetten probability issue
   - 2026 US Senate general / 2028 Dem Primary matching issues
   - These are website display/grouping issues, not master data issues

<!-- UPDATE THIS AFTER EACH REVIEW SESSION -->
**last_reviewed_timestamp: 2026-02-12T22:41:28.460Z**

---

## Media Pipeline

### Architecture
The media pipeline is **integrated into the main daily pipeline** (Phase 5b in `pipeline_daily_refresh.py`).
It runs automatically as part of the daily 06:00 UTC cron on `/opt/bellwether/` (v2/hetzner branch).

**Pipeline steps** (all non-blocking — failures don't stop the main pipeline):
1. `pipeline_media_discover_citations.py` — Discover citations via GDELT
2. `pipeline_media_extract_markets.py` — Extract & match market references (GPT)
3. `pipeline_media_calculate_fragility.py` — Calculate fragility metrics
4. `generate_media_web_data.py` — Generate `docs/data/media_*.json` for website

**Output files** (committed by `run_pipeline.sh` alongside other `docs/data/` files):
- `docs/data/media_summary.json` — Hero stats, timeline
- `docs/data/media_outlets.json` — Outlet leaderboard with grades
- `docs/data/media_citations.json` — Top 500 recent citations

**Frontend**: `docs/media.html` + `docs/js/media.js`

### Hetzner Setup
After pulling the merged v2/hetzner branch, install new dependencies:
```bash
sudo -u bellwether /opt/bellwether/venv/bin/pip install -r /opt/bellwether/packages/pipelines/requirements.txt
```

Add `NEWSAPI_KEY` to `/opt/bellwether/.env` (optional but recommended for broader coverage).

No separate cron needed — media runs as part of the existing daily pipeline.
