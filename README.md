# Job Monitor

Python automation that scans company career pages on schedule, filters early-career roles with Gemini, deduplicates by URL in Supabase, sends an email digest through Resend, and publishes a live dashboard table on GitHub Pages.

## 1) Supabase setup (exact SQL)

Create a project in Supabase, open SQL Editor, and run:

```sql
CREATE TABLE seen_jobs (
  id SERIAL PRIMARY KEY,
  title TEXT,
  company TEXT,
  url TEXT UNIQUE NOT NULL,
  ai_reason TEXT,
  matched BOOLEAN DEFAULT FALSE,
  date_found TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

The `url` column is `UNIQUE` and is the only dedup key.

## 2) API keys required

- `SUPABASE_URL`: Supabase project URL (`Settings -> API`)
- `SUPABASE_KEY`: Supabase `service_role` or key with table write access (`Settings -> API`)
- `GEMINI_API_KEY`: Google AI Studio key for Gemini
- `RESEND_API_KEY`: Resend API key
- `ALERT_EMAIL`: recipient email for alert digests
- `RESEND_FROM_EMAIL` (optional): sender email (defaults to `onboarding@resend.dev`)

## 3) GitHub Secrets setup

In GitHub: `Settings -> Secrets and variables -> Actions -> New repository secret`.

Add these secrets:

- `SUPABASE_URL`
- `SUPABASE_KEY`
- `GEMINI_API_KEY`
- `RESEND_API_KEY`
- `ALERT_EMAIL`
- `RESEND_FROM_EMAIL` (recommended)

Workflows:

- `.github/workflows/job_monitor.yml` (companies): every 2 hours (`0 */2 * * *`)
- `.github/workflows/job_monitor_trackers.yml` (trackers): every 6 hours (`30 */6 * * *`)
- Both also support manual trigger: `workflow_dispatch`

## 4) Live dashboard table (GitHub Pages)

Every workflow run generates `docs/data.json` from Supabase and deploys `docs/index.html` to GitHub Pages.

In GitHub repo settings, enable Pages with:

- Source: **GitHub Actions**

After the first successful deployment job, your live table URL will be:

- `https://<your-github-username>.github.io/<your-repo-name>/`

## 5) Add or remove companies

Edit `companies.json`:

```json
[
  { "name": "Stripe", "url": "https://jobs.lever.co/stripe" }
]
```

Tips:

- Prefer Greenhouse/Lever/Ashby board URLs where possible.
- Use direct careers pages only when board URLs are not available.
- Keep `name` and `url` keys exactly as shown.

## 6) If a company returns 403 blocked

The script logs blocked targets and continues:

- `⚠️ [Company] blocked — likely IP restriction, consider adding to manual list`

What to do:

- Keep the company in `companies.json` and let future runs retry.
- Switch to a board URL (Greenhouse/Lever/Ashby) if available.
- If blocks persist, temporarily remove the company from automation and track it manually.

## Local run

```bash
pip install -r requirements.txt
playwright install chromium
playwright install-deps chromium
python monitor.py
```
