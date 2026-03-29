# Bellwether → Hetzner VPS Migration

## Overview

Migrate the daily pipeline from Stanford Sherlock (SLURM self-resubmitting jobs) to a Hetzner CPX11 VPS ($4.99/mo) with cron.

**Target:** Hetzner CPX11 — 2 vCPU, 2 GB RAM, 40 GB disk, Ubuntu 24.04 LTS

## Migration Steps

### Step 0: Generate an SSH key for the VPS (~1 min)

On your local machine:

```bash
ssh-keygen -t ed25519 -C "bellwether-hetzner" -f ~/.ssh/hetzner_bellwether
```

Copy the public key (you'll paste it into Hetzner):

```bash
cat ~/.ssh/hetzner_bellwether.pub
```

### Step 1: Provision the VPS (~5 min)

1. Go to https://console.hetzner.cloud → Create a project → **Add Server**
2. Configure:
   - **Location:** Ashburn (US) or Falkenstein (EU)
   - **Image:** Ubuntu 24.04
   - **Server type:** Under **Shared Resources**, pick **CPX11** (2 vCPU, 2 GB RAM, 40 GB disk, $4.99/mo). If the pipeline needs more RAM, upgrade to **CPX21** (3 vCPU, 4 GB RAM, $9.99/mo).
   - **SSH keys:** Click "Add SSH Key", paste the contents of `~/.ssh/hetzner_bellwether.pub`
   - **Name:** `bellwether`
3. Click **Create & Buy Now**
4. Note the IP address (5.78.158.28)

Connect with:

```bash
ssh -i ~/.ssh/hetzner_bellwether root@<vps-ip>
```

### Step 2: Run the setup script (~10 min)

```bash
# SSH into VPS as root
ssh root@<vps-ip>

# Clone the repo (v2/data-full branch)
git clone --branch v2/data-full https://github.com/vcbee/bellwether-platform.git /opt/bellwether-setup

# Run setup
bash /opt/bellwether-setup/packages/pipelines/hetzner/setup.sh
```

This installs Python, creates the venv, sets up cron, configures firewall, etc.

### Step 3: Add API keys (~2 min)

```bash
ssh root@<vps-ip>
nano /opt/bellwether/.env
```

Fill in: `OPENAI_API_KEY`, `DOME_API_KEY`, `GOOGLE_CIVIC_API_KEY`

### Step 4: Transfer data from Sherlock (~15-30 min)

From your local machine:

```bash
# Option A: Sherlock → local → VPS (two-hop)
bash packages/pipelines/hetzner/transfer_data.sh paschal <vps-ip>

# Option B: If you already have data locally
bash packages/pipelines/hetzner/transfer_data.sh --local <vps-ip>
```

### Step 5: Set up GitHub PAT for git push (~2 min)

The VPS needs write access to push daily data updates.

1. Go to https://github.com/settings/tokens?type=beta → **Generate new token**
2. Configure:
   - **Token name:** `bellwether-vps`
   - **Expiration:** 1 year
   - **Repository access:** Only select repositories → `elliotjames-paschal/bellwether-platform`
   - **Permissions → Repository permissions → Contents:** Read and write
3. Generate and copy the token

Add it to the VPS env file:

```bash
nano /opt/bellwether/.env
```

Add this line:

```
GITHUB_PAT=github_pat_XXXXX
```

### Step 6: Test run (~5-15 min)

```bash
sudo -u bellwether /opt/bellwether/packages/pipelines/hetzner/run_pipeline.sh
```

Watch the output. If it completes successfully, you're good.

### Step 7: Health check

```bash
sudo -u bellwether bash /opt/bellwether/packages/pipelines/hetzner/healthcheck.sh
```

Should show all green. Fix any failures.

### Step 8: Run both in parallel (~1 week)

Keep Sherlock running for 1 week while the VPS proves itself. Compare outputs.

### Step 9: Teardown Sherlock

```bash
ssh paschal@login.sherlock.stanford.edu
bash bellwether-platform/packages/pipelines/hetzner/teardown_sherlock.sh
```

## Schedule

| Job | Schedule | Command |
|-----|----------|---------|
| Daily refresh | 06:00 UTC daily | `run_pipeline.sh` |
| Weekly refresh | 08:00 UTC Sundays | `run_pipeline.sh --weekly-refresh` |

## Monitoring

- **Logs:** `/opt/bellwether/logs/pipeline_runs/`
- **Cron log:** `/opt/bellwether/logs/cron.log`
- **Email alerts:** Configured via `logs/email_config.json` (uses existing logging_config.py)
- **Health check:** `healthcheck.sh` — run anytime, or add to cron for weekly checks

## Monthly cost

| Component | Cost |
|-----------|------|
| Hetzner CPX11 | $4.99/mo |
| Total | ~$5/mo |

## Files

```
packages/pipelines/hetzner/
├── MIGRATION.md          # This file
├── setup.sh              # One-time VPS provisioning
├── run_pipeline.sh       # Daily runner (called by cron)
├── transfer_data.sh      # Data migration helper
├── healthcheck.sh        # Diagnostic tool
└── teardown_sherlock.sh  # Sherlock cleanup (after migration)
```
