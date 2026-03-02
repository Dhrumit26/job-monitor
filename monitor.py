import json
import os
import random
import re
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin

import google.generativeai as genai
import resend
from dotenv import load_dotenv
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright
from supabase import Client, create_client

try:
    # playwright-stealth<=1.x API
    from playwright_stealth import stealth_sync as legacy_stealth_sync
except Exception:
    legacy_stealth_sync = None

try:
    # playwright-stealth>=2.x API
    from playwright_stealth import Stealth
except Exception:
    Stealth = None


TARGET_SELECTORS = [
    "[data-qa='job-title']",
    ".job-title",
    ".posting-title",
    "h3 a",
    "h2 a",
    ".careers-job-title",
]

JOB_LINK_KEYWORDS = ("job", "career", "position", "role", "opening")
PAGE_GOTO_TIMEOUT_MS = int(os.getenv("PAGE_GOTO_TIMEOUT_MS", "30000"))
MIN_DELAY_SECONDS = float(os.getenv("MIN_DELAY_SECONDS", "1"))
MAX_DELAY_SECONDS = float(os.getenv("MAX_DELAY_SECONDS", "2"))
DEFAULT_GEMINI_MODEL = "gemini-1.5-flash"
GEMINI_FALLBACK_MODELS = [
    "gemini-1.5-flash",
    "gemini-1.5-flash-latest",
    "gemini-1.5-flash-8b",
    "gemini-2.0-flash",
    "gemini-2.0-flash-lite",
    "gemini-2.5-flash",
]
EARLY_INCLUDE_KEYWORDS = (
    "junior",
    "associate",
    "entry",
    "entry-level",
    "graduate",
    "new grad",
    "new-grad",
    "trainee",
    "intern",
    "level i",
    "level ii",
    "swe i",
    "swe ii",
    "0-2",
    "0 to 2",
    "0+ years",
    "1+ years",
    "2+ years",
)
EARLY_EXCLUDE_KEYWORDS = (
    "senior",
    "sr.",
    "staff",
    "lead",
    "principal",
    "distinguished",
    "manager",
    "director",
    "head of",
    "vp",
    "chief",
    "c-level",
    "experienced",
    "seasoned",
    "3+ years",
    "4+ years",
    "5+ years",
)


def log(message: str) -> None:
    print(message, flush=True)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_companies(path: str) -> list[dict[str, str]]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def select_company_shard(companies: list[dict[str, str]]) -> list[dict[str, str]]:
    total_groups = max(1, int(os.getenv("TOTAL_GROUPS", "1")))
    group_index = int(os.getenv("GROUP_INDEX", "0"))
    if group_index < 0 or group_index >= total_groups:
        log(f"⚠️ Invalid shard config GROUP_INDEX={group_index}, TOTAL_GROUPS={total_groups}; using all companies")
        return companies
    return [company for idx, company in enumerate(companies) if idx % total_groups == group_index]


def init_supabase() -> Client | None:
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        log("❌ Supabase not configured (SUPABASE_URL/SUPABASE_KEY missing)")
        return None
    try:
        return create_client(url, key)
    except Exception as exc:
        log(f"❌ Supabase init failed ({exc})")
        return None


def init_gemini() -> genai.GenerativeModel | None:
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        log("❌ Gemini not configured (GEMINI_API_KEY missing)")
        return None
    try:
        genai.configure(api_key=api_key)
        configured_model = os.getenv("GEMINI_MODEL", DEFAULT_GEMINI_MODEL).strip() or DEFAULT_GEMINI_MODEL
        selected_model = resolve_gemini_model(configured_model)
        log(f"🤖 Gemini model selected: {selected_model}")
        return genai.GenerativeModel(selected_model)
    except Exception as exc:
        log(f"❌ Gemini init failed ({exc})")
        return None


def resolve_gemini_model(preferred_model: str) -> str:
    """
    Pick a working model from account-available models.
    Keeps preferred model first, then known fallbacks.
    """
    candidates: list[str] = [preferred_model]
    for model in GEMINI_FALLBACK_MODELS:
        if model not in candidates:
            candidates.append(model)

    try:
        available: set[str] = set()
        for model in genai.list_models():
            methods = getattr(model, "supported_generation_methods", []) or []
            if "generateContent" not in methods:
                continue
            model_name = getattr(model, "name", "")
            if not model_name:
                continue
            available.add(model_name)
            if model_name.startswith("models/"):
                available.add(model_name.split("/", 1)[1])

        for candidate in candidates:
            if candidate in available or f"models/{candidate}" in available:
                return candidate

        # Final fallback to first available generation model.
        normalized = sorted(
            [m.split("/", 1)[1] if m.startswith("models/") else m for m in available]
        )
        if normalized:
            return normalized[0]
    except Exception as exc:
        log(f"⚠️ Could not list Gemini models ({exc}); using preferred model")

    return preferred_model


def init_resend() -> bool:
    api_key = os.getenv("RESEND_API_KEY")
    if not api_key:
        log("❌ Resend not configured (RESEND_API_KEY missing)")
        return False
    resend.api_key = api_key
    return True


def normalize_url(base_url: str, href: str | None) -> str:
    if not href:
        return ""
    return urljoin(base_url, href.strip())


def dedupe_links(links: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[str] = set()
    out: list[dict[str, str]] = []
    for link in links:
        href = (link.get("url") or "").strip()
        title = (link.get("title") or "").strip()
        if not href or href in seen:
            continue
        seen.add(href)
        out.append({"title": title, "url": href})
    return out


def configure_stealth_context(context: Any) -> None:
    """Apply stealth scripts in a version-compatible way."""
    if Stealth is None:
        return
    try:
        stealth = Stealth(init_scripts_only=True)
        for script in getattr(stealth, "script_payload", []):
            context.add_init_script(script)
    except Exception as exc:
        log(f"⚠️ Failed to apply context stealth ({exc})")


def apply_page_stealth(page: Any) -> None:
    if legacy_stealth_sync is None:
        return
    try:
        legacy_stealth_sync(page)
    except Exception as exc:
        log(f"⚠️ Failed to apply page stealth ({exc})")


def scrape_company_links(page: Any, company_name: str, company_url: str) -> tuple[list[dict[str, str]], bool]:
    links: list[dict[str, str]] = []
    used_targeted = False

    for selector in TARGET_SELECTORS:
        elements = page.query_selector_all(selector)
        if not elements:
            continue
        used_targeted = True
        for element in elements:
            title = (element.inner_text() or "").strip()
            href = element.get_attribute("href")
            if not href:
                anchor = element.query_selector("a")
                if anchor:
                    href = anchor.get_attribute("href")
                    if not title:
                        title = (anchor.inner_text() or "").strip()
            full_url = normalize_url(company_url, href)
            if full_url:
                links.append({"title": title or "Untitled Role", "url": full_url})

    if links:
        return dedupe_links(links), used_targeted

    all_anchors = page.eval_on_selector_all(
        "a",
        "els => els.map(e => ({text: (e.innerText || '').trim(), href: e.href || ''}))",
    )
    for item in all_anchors:
        href = (item.get("href") or "").strip()
        text = (item.get("text") or "").strip()
        if not href:
            continue
        lower_href = href.lower()
        if any(keyword in lower_href for keyword in JOB_LINK_KEYWORDS):
            links.append({"title": text or "Untitled Role", "url": href})

    if not links:
        log(f"⚠️ {company_name} produced no extractable job links")
    return dedupe_links(links), used_targeted


def strip_markdown_fences(text: str) -> str:
    cleaned = text.strip()
    cleaned = re.sub(r"^```(?:json)?", "", cleaned, flags=re.IGNORECASE).strip()
    cleaned = re.sub(r"```$", "", cleaned).strip()
    return cleaned.strip("` \njson")


def heuristic_filter_jobs(link_list: list[dict[str, str]]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for item in link_list:
        title = str(item.get("title", "")).strip()
        url = str(item.get("url", "")).strip()
        if not url:
            continue
        haystack = f"{title} {url}".lower()
        if any(keyword in haystack for keyword in EARLY_EXCLUDE_KEYWORDS):
            continue
        if any(keyword in haystack for keyword in EARLY_INCLUDE_KEYWORDS):
            out.append({"title": title or "Untitled Role", "url": url})
    return dedupe_links(out)


def gemini_filter_jobs(
    model: genai.GenerativeModel | None,
    company_name: str,
    link_list: list[dict[str, str]],
) -> tuple[list[dict[str, str]], bool, str]:
    """
    Returns:
      - matches
      - whether Gemini should remain enabled for subsequent companies
      - filter mode label for logging/persistence
    """
    if not link_list:
        return [], True, "none"
    if model is None:
        fallback = heuristic_filter_jobs(link_list)
        log(f"🧠 Fallback filter: {len(fallback)} matches at {company_name}")
        return fallback, False, "heuristic"

    prompt = f"""You are a job filter for an early-career candidate (0-3 years exp).

APPROVE only:
Junior, Associate, Entry Level, Graduate, Trainee, Intern,
Level I, Level II, SWE I, SWE II, roles requiring 0-2 years

REJECT without exception:
Senior, Sr., Staff, Lead, Principal, Distinguished, Manager,
Director, Head of, VP, C-Level, roles requiring 3+ years,
words like "experienced" or "seasoned" in title

Here is a list of job links from {company_name}'s career page:
{json.dumps(link_list)}

Return ONLY a valid JSON array, no markdown, no explanation.
Format: [{{"title": "Job Title", "url": "https://..."}}]
If no early-career roles found, return exactly: []"""

    try:
        response = model.generate_content(prompt)
        text = strip_markdown_fences(response.text or "")
        parsed = json.loads(text)
        if not isinstance(parsed, list):
            log(f"⚠️ Gemini parse warning at {company_name}: non-array response")
            return [], True, "gemini"
        out: list[dict[str, str]] = []
        for item in parsed:
            if not isinstance(item, dict):
                continue
            title = str(item.get("title", "")).strip()
            url = str(item.get("url", "")).strip()
            if title and url:
                out.append({"title": title, "url": url})
        log(f"🤖 Gemini: {len(out)} early-career matches at {company_name}")
        return dedupe_links(out), True, "gemini"
    except json.JSONDecodeError as exc:
        log(f"⚠️ Gemini parse failed at {company_name} ({exc})")
        return [], True, "gemini"
    except Exception as exc:
        message = str(exc).lower()
        if "429" in message or "quota" in message or "rate limit" in message:
            log(
                f"⚠️ Gemini quota/rate-limit at {company_name}; "
                "switching to heuristic filter for remaining companies"
            )
            fallback = heuristic_filter_jobs(link_list)
            log(f"🧠 Fallback filter: {len(fallback)} matches at {company_name}")
            return fallback, False, "heuristic"
        log(f"❌ Gemini failed at {company_name} ({exc})")
        return [], True, "gemini"


def is_new_job(supabase: Client | None, url: str) -> bool | None:
    if supabase is None:
        log(f"❌ Supabase unavailable, skipping job check for {url}")
        return None
    try:
        response = supabase.table("seen_jobs").select("url").eq("url", url).limit(1).execute()
        return len(response.data or []) == 0
    except Exception as exc:
        log(f"❌ Supabase check failed for {url} ({exc})")
        return None


def save_job(
    supabase: Client | None,
    title: str,
    company: str,
    url: str,
    matched: bool,
    ai_reason: str,
) -> bool:
    if supabase is None:
        log(f"❌ Supabase unavailable, skipping save for {url}")
        return False
    payload = {
        "title": title,
        "company": company,
        "url": url,
        "matched": matched,
        "ai_reason": ai_reason,
    }
    try:
        supabase.table("seen_jobs").upsert(payload, on_conflict="url").execute()
        return True
    except Exception as exc:
        log(f"❌ Supabase upsert failed for {url} ({exc})")
        return False


def build_email_html(matches: list[dict[str, str]]) -> str:
    cards = []
    for job in matches:
        cards.append(
            f"""
            <div style="padding:18px 0;border-bottom:1px solid #e5e7eb;">
              <div style="font-size:13px;color:#6b7280;font-weight:600;margin-bottom:6px;">{job["company"]}</div>
              <div style="font-size:20px;color:#111827;font-weight:700;margin-bottom:12px;">{job["title"]}</div>
              <a href="{job["url"]}" style="display:inline-block;background:#22c55e;color:#ffffff;text-decoration:none;padding:10px 16px;border-radius:8px;font-weight:600;">
                Apply Now
              </a>
            </div>
            """
        )

    return f"""
    <html>
      <body style="margin:0;padding:0;background:#ffffff;font-family:Arial,sans-serif;">
        <div style="max-width:600px;margin:0 auto;padding:24px;">
          <h2 style="color:#111827;">New Early-Career Job Matches</h2>
          {''.join(cards)}
          <p style="margin-top:20px;color:#6b7280;font-size:12px;">Job Monitor • Next scan in 2 hours</p>
        </div>
      </body>
    </html>
    """


def send_digest_email(matches: list[dict[str, str]]) -> None:
    if not matches:
        return
    to_email = os.getenv("ALERT_EMAIL")
    from_email = os.getenv("RESEND_FROM_EMAIL", "onboarding@resend.dev")
    if not to_email:
        log("❌ ALERT_EMAIL missing, cannot send email")
        return
    subject = f"🎯 {len(matches)} New Early-Career Jobs Found — {datetime.now().strftime('%Y-%m-%d')}"
    html = build_email_html(matches)
    try:
        resend.Emails.send(
            {
                "from": from_email,
                "to": [to_email],
                "subject": subject,
                "html": html,
            }
        )
        log(f"📧 Email sent — {len(matches)} matches this run")
    except Exception as exc:
        log(f"❌ Email failed ({exc})")


def main() -> None:
    load_dotenv()
    log(f"▶ Starting scan — {now_iso()}")

    script_dir = os.path.dirname(os.path.abspath(__file__))
    companies_path = os.path.join(script_dir, "companies.json")
    companies = load_companies(companies_path)
    shard_companies = select_company_shard(companies)

    supabase = init_supabase()
    model = init_gemini()
    resend_ready = init_resend()
    gemini_enabled = model is not None

    total_groups = max(1, int(os.getenv("TOTAL_GROUPS", "1")))
    group_index = int(os.getenv("GROUP_INDEX", "0"))
    log(
        f"🧩 Shard {group_index + 1}/{total_groups} active — "
        f"{len(shard_companies)} of {len(companies)} companies assigned"
    )

    scraped_count = 0
    blocked_count = 0
    total_matched = 0
    total_new = 0
    new_matches: list[dict[str, str]] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        configure_stealth_context(context)

        for company in shard_companies:
            company_name = company.get("name", "Unknown")
            company_url = company.get("url", "")
            t0 = time.perf_counter()
            page = context.new_page()
            try:
                log(f"🌐 Scraping {company_name}...")
                apply_page_stealth(page)
                page.goto(company_url, wait_until="domcontentloaded", timeout=PAGE_GOTO_TIMEOUT_MS)
                links, _used_targeted = scrape_company_links(page, company_name, company_url)
                scraped_count += 1
                elapsed = time.perf_counter() - t0
                log(f"🌐 Scraping {company_name}... done ({elapsed:.1f}s)")
            except PlaywrightTimeoutError as exc:
                reason = str(exc)
                log(f"❌ Scraping {company_name}... failed ({reason})")
                if "403" in reason:
                    blocked_count += 1
                    log(f"⚠️ {company_name} blocked — likely IP restriction, consider adding to manual list")
                page.close()
                time.sleep(random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS))
                continue
            except Exception as exc:
                reason = str(exc)
                log(f"❌ Scraping {company_name}... failed ({reason})")
                if "403" in reason:
                    blocked_count += 1
                    log(f"⚠️ {company_name} blocked — likely IP restriction, consider adding to manual list")
                page.close()
                time.sleep(random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS))
                continue

            ai_matches, gemini_enabled, filter_mode = gemini_filter_jobs(
                model if gemini_enabled else None,
                company_name,
                links,
            )
            total_matched += len(ai_matches)

            for match in ai_matches:
                title = match["title"]
                url = match["url"]

                is_new = is_new_job(supabase, url)
                if is_new is None:
                    log(f"⏭  SKIP (db error): {title} @ {company_name}")
                    continue
                if not is_new:
                    log(f"⏭  SKIP (seen): {title} @ {company_name}")
                    continue

                saved = save_job(
                    supabase=supabase,
                    title=title,
                    company=company_name,
                    url=url,
                    matched=True,
                    ai_reason=f"{filter_mode} early-career match",
                )
                if not saved:
                    continue

                total_new += 1
                new_matches.append({"company": company_name, "title": title, "url": url})
                log(f"✅ NEW: {title} @ {company_name}")

            page.close()
            time.sleep(random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS))

        browser.close()

    if resend_ready:
        send_digest_email(new_matches)
    elif new_matches:
        log("❌ Resend unavailable, email not sent")

    log(
        f"✅ Done — {scraped_count} scraped, {total_new} new, "
        f"{total_matched} matched, {blocked_count} blocked"
    )


if __name__ == "__main__":
    main()
