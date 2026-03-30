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

## Media Pipeline - Hetzner Deployment

### Architecture
The media pipeline runs on Hetzner **separately from production**:

```
/opt/bellwether/          ← production (v2/hetzner branch) — DO NOT TOUCH
/opt/bellwether-media/    ← media pipeline (media branch)
/opt/bellwether/.env      ← shared secrets
/opt/bellwether/venv/     ← shared venv
/opt/bellwether/locks/    ← separate lock files per pipeline
```

The media pipeline **only** pulls from and pushes to the `media` branch. It never touches `v2/hetzner`.

### Setup Steps (on Hetzner, as root)

#### 1. Clone media branch into separate directory
```bash
git clone --branch media https://github.com/elliotjames-paschal/bellwether-platform.git /opt/bellwether-media
chown -R bellwether:bellwether /opt/bellwether-media
sudo -u bellwether git -C /opt/bellwether-media config user.name "Bellwether Bot"
sudo -u bellwether git -C /opt/bellwether-media config user.email "bellwether-bot@noreply.github.com"
```

#### 2. Set up SSH deploy key (for push to media branch)
The production deploy key may already cover this. Test with:
```bash
sudo -u bellwether git -C /opt/bellwether-media push --dry-run origin media
```
If it fails, the existing deploy key needs write access, or set the remote to use the PAT:
```bash
# Option: use PAT-based remote (reads PAT from .env at push time)
sudo -u bellwether git -C /opt/bellwether-media remote set-url origin git@github.com:elliotjames-paschal/bellwether-platform.git
```

#### 3. Install new dependencies
```bash
sudo -u bellwether /opt/bellwether/venv/bin/pip install -r /opt/bellwether-media/packages/pipelines/requirements.txt
```

#### 4. Add NEWSAPI_KEY to .env (optional but recommended)
```bash
nano /opt/bellwether/.env
# Add: NEWSAPI_KEY=your_key_here
```

#### 5. Test run
```bash
sudo -u bellwether /opt/bellwether-media/packages/pipelines/hetzner/run_media_pipeline.sh
```

#### 6. Add cron job
```bash
# As root, edit bellwether's crontab:
crontab -u bellwether -e

# Add this line (runs at 07:30 UTC, 1.5h after main pipeline):
30 7 * * * /opt/bellwether-media/packages/pipelines/hetzner/run_media_pipeline.sh >> /opt/bellwether/logs/media_cron.log 2>&1
```

### Status
- [ ] Push `media` branch to remote
- [ ] Clone on Hetzner
- [ ] Install deps
- [ ] Test run
- [ ] Add cron
- [ ] Verify first automated run
