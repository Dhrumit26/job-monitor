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
from openai import OpenAI
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
RETRY_TIMEOUT_MS = int(os.getenv("RETRY_TIMEOUT_MS", "60000"))
MAX_SCRAPE_RETRIES = int(os.getenv("MAX_SCRAPE_RETRIES", "1"))
MIN_DELAY_SECONDS = float(os.getenv("MIN_DELAY_SECONDS", "1"))
MAX_DELAY_SECONDS = float(os.getenv("MAX_DELAY_SECONDS", "2"))
NEXT_SCAN_HOURS = os.getenv("NEXT_SCAN_HOURS", "2")
GEMINI_MAX_RETRIES = int(os.getenv("GEMINI_MAX_RETRIES", "2"))
GEMINI_RETRY_BASE_SECONDS = float(os.getenv("GEMINI_RETRY_BASE_SECONDS", "3"))
DEFAULT_GEMINI_MODEL = "gemini-1.5-flash"
DEFAULT_OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
GEMINI_FALLBACK_MODELS = [
    "gemini-1.5-flash",
    "gemini-1.5-flash-latest",
    "gemini-1.5-flash-8b",
    "gemini-2.0-flash",
    "gemini-2.0-flash-lite",
    "gemini-2.5-flash",
]
POSITION_INCLUDE_KEYWORDS = (
    "software engineer",
    "software developer",
    "software development engineer",
    "developer",
    "engineer",
    "swe",
    "sde",
    "application engineer",
    "applications engineer",
    "web engineer",
    "mobile engineer",
    "ios engineer",
    "android engineer",
    "backend engineer",
    "back-end engineer",
    "frontend engineer",
    "front-end engineer",
    "fullstack engineer",
    "full stack engineer",
    "platform engineer",
    "qa engineer",
    "test engineer",
    "site reliability engineer",
    "sre",
    "devops engineer",
)
EARLY_CAREER_INCLUDE_KEYWORDS = (
    "entry",
    "entry-level",
    "entry level",
    "early career",
    "early-career",
    "junior",
    "new grad",
    "new-grad",
    "new graduate",
    "new grads",
    "campus",
    "campus hire",
    "university grad",
    "college grad",
    "graduate program",
    "rotational",
    "apprentice",
    "apprenticeship",
    "graduate",
    "intern",
    "internship",
    "associate",
    "associate engineer",
    "engineer i",
    "software engineer i",
    "swe i",
    "sde i",
    "level i",
    "level 1",
    "l1",
    "0-1",
    "0 to 1 years",
    "0-1 years",
    "0 year",
    "0 years",
    "0 to 1",
    "0+ year",
    "0+ years",
    "1+ year",
    "1 year",
    "1 years",
)
SENIORITY_REJECT_KEYWORDS = (
    "senior",
    "sr.",
    "sr ",
    "snr",
    "experienced",
    "staff",
    "principal",
    "distinguished",
    "lead",
    "tech lead",
    "team lead",
    "manager",
    "management",
    "director",
    "head of",
    "vp",
    "vice president",
    "chief",
    "architect",
)
CLEARANCE_REJECT_KEYWORDS = (
    "security clearance",
    "clearance required",
    "must be cleared",
    "requires clearance",
    "us citizen only clearance",
    "active clearance",
    "active secret",
    "active top secret",
    "top secret",
    "secret clearance",
    "public trust",
    "ts/sci",
    "ts sci",
    "ts/si",
    "polygraph",
    "dod clearance",
    "classified",
)
US_LOCATION_INCLUDE_KEYWORDS = (
    "united states",
    "united states only",
    "usa",
    "u.s",
    "u.s.",
    "us",
    "us only",
    "us-based",
    "us based",
    "based in us",
    "based in the us",
    "within the us",
    "work authorization in the us",
    "must reside in us",
    "remote us only",
    "remote us",
    "remote us",
    "remote - us",
    "remote (us)",
    "remote (usa)",
    "remote, united states",
    "remote, us",
    "new york",
    "nyc",
    "san francisco",
    "bay area",
    "seattle",
    "austin",
    "boston",
    "chicago",
    "los angeles",
    "washington dc",
    "washington, dc",
    "atlanta",
    "denver",
    "miami",
    "dallas",
    "houston",
    "phoenix",
    "philadelphia",
    "san diego",
    "san jose",
    "portland",
    "nashville",
    "charlotte",
    "detroit",
    "minneapolis",
    "salt lake city",
)
NON_US_LOCATION_REJECT_KEYWORDS = (
    "outside us",
    "outside the us",
    "non-us",
    "non us",
    "not us",
    "singapore",
    "india",
    "canada",
    "toronto",
    "vancouver",
    "united kingdom",
    "uk",
    "london",
    "england",
    "scotland",
    "wales",
    "europe",
    "eu ",
    "e.u.",
    "germany",
    "france",
    "ireland",
    "spain",
    "italy",
    "sweden",
    "norway",
    "denmark",
    "finland",
    "switzerland",
    "belgium",
    "netherlands",
    "portugal",
    "austria",
    "poland",
    "czech",
    "romania",
    "australia",
    "new zealand",
    "japan",
    "south korea",
    "korea",
    "israel",
    "uae",
    "dubai",
    "saudi",
    "qatar",
    "hong kong",
    "taiwan",
    "china",
    "malaysia",
    "indonesia",
    "thailand",
    "vietnam",
    "philippines",
    "brazil",
    "mexico",
    "argentina",
    "chile",
    "colombia",
    "peru",
    "south africa",
    "emea",
    "apac",
    "latam",
)


def log(message: str) -> None:
    print(message, flush=True)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_companies(path: str) -> list[dict[str, str]]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def is_tracker_source(company: dict[str, str]) -> bool:
    name = (company.get("name") or "").lower()
    url = (company.get("url") or "").lower()
    name_keywords = ("tracker", "list", "feed", "board", "meta-source")
    url_keywords = (
        "simplify.jobs",
        "github.com/simplifyjobs/new-grad-positions",
        "jobright.ai",
        "jobsfornewgrad.com",
        "indeed.com",
        "forbes.com/lists",
        "greatplacetowork.com",
        "linkedin.com/jobs/software-engineer-new-grad-jobs",
    )
    return any(keyword in name for keyword in name_keywords) or any(
        keyword in url for keyword in url_keywords
    )


def filter_company_sources(companies: list[dict[str, str]]) -> list[dict[str, str]]:
    source_mode = os.getenv("SOURCE_MODE", "all").strip().lower()
    if source_mode == "companies":
        return [company for company in companies if not is_tracker_source(company)]
    if source_mode == "trackers":
        return [company for company in companies if is_tracker_source(company)]
    return companies


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


def init_openai() -> tuple[OpenAI | None, str]:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        log("❌ OpenAI not configured (OPENAI_API_KEY missing)")
        return None, DEFAULT_OPENAI_MODEL
    try:
        client = OpenAI(api_key=api_key)
        log(f"🤖 OpenAI model selected: {DEFAULT_OPENAI_MODEL}")
        return client, DEFAULT_OPENAI_MODEL
    except Exception as exc:
        log(f"❌ OpenAI init failed ({exc})")
        return None, DEFAULT_OPENAI_MODEL


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


def goto_with_retry(page: Any, company_name: str, company_url: str) -> None:
    last_exc: Exception | None = None
    for attempt in range(MAX_SCRAPE_RETRIES + 1):
        timeout_ms = PAGE_GOTO_TIMEOUT_MS if attempt == 0 else RETRY_TIMEOUT_MS
        try:
            page.goto(company_url, wait_until="domcontentloaded", timeout=timeout_ms)
            return
        except Exception as exc:
            last_exc = exc
            if attempt < MAX_SCRAPE_RETRIES:
                log(
                    f"🔁 Retry {attempt + 1}/{MAX_SCRAPE_RETRIES} for {company_name} "
                    f"after navigation error ({exc})"
                )
                continue
            raise

    if last_exc is not None:
        raise last_exc


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


def should_reject_before_ai(title: str, url: str) -> bool:
    haystack = f"{title} {url}".lower()
    if any(keyword in haystack for keyword in SENIORITY_REJECT_KEYWORDS):
        return True
    if any(keyword in haystack for keyword in CLEARANCE_REJECT_KEYWORDS):
        return True
    if any(keyword in haystack for keyword in NON_US_LOCATION_REJECT_KEYWORDS):
        return True
    return False


def is_candidate_for_ai(title: str, url: str) -> bool:
    haystack = f"{title} {url}".lower()
    has_position = any(keyword in haystack for keyword in POSITION_INCLUDE_KEYWORDS)
    has_early_career = any(keyword in haystack for keyword in EARLY_CAREER_INCLUDE_KEYWORDS)
    has_us_signal = any(keyword in haystack for keyword in US_LOCATION_INCLUDE_KEYWORDS)
    # Allow SDE/SWE-like titles through even when explicit early/location tokens are sparse.
    explicit_engineer_signal = "sde" in haystack or "swe" in haystack
    return has_position and (has_early_career or has_us_signal or explicit_engineer_signal)


def select_ai_candidates(link_list: list[dict[str, str]]) -> list[dict[str, str]]:
    candidates: list[dict[str, str]] = []
    for item in link_list:
        title = str(item.get("title", "")).strip()
        url = str(item.get("url", "")).strip()
        if not title or not url:
            continue
        if should_reject_before_ai(title, url):
            continue
        if is_candidate_for_ai(title, url):
            candidates.append({"title": title, "url": url})
    return dedupe_links(candidates)


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
        log(f"❌ Gemini unavailable, skipping Gemini filtering at {company_name}")
        return [], False, "gemini_unavailable"

    candidates = select_ai_candidates(link_list)
    if not candidates:
        log(f"🧪 Prefilter: 0 AI candidates at {company_name}")
        return [], True, "prefilter"

    log(f"🧪 Prefilter: {len(candidates)} AI candidates at {company_name}")

    prompt = f"""You are a strict job filter.

Goal:
Select ONLY jobs that match ALL conditions:
1) Location is in the United States (or remote within US)
2) Early-career role around 0-1 years experience
3) No security clearance required

Use the title, URL text, and any location cues available in the input.
If a job does not clearly satisfy all three, reject it.

Important:
- Inputs were prefiltered for software-engineering-like, early-career-like, US-like signals.
- You must still reject anything that is actually non-US, senior/staff/principal+, or requires security clearance.

Here is a list of candidate job links from {company_name}'s career page:
{json.dumps(candidates)}

Return ONLY a valid JSON array, no markdown, no explanation.
Format: [{{"title": "Job Title", "url": "https://..."}}]
If no early-career roles found, return exactly: []"""

    try:
        response = None
        for attempt in range(GEMINI_MAX_RETRIES + 1):
            try:
                response = model.generate_content(prompt)
                break
            except Exception as exc:
                message = str(exc).lower()
                is_rate_limit = "429" in message or "quota" in message or "rate limit" in message
                if not is_rate_limit:
                    raise

                # Hard exhausted quota won't recover with retries in this run.
                if "limit: 0" in message:
                    log(
                        f"⚠️ Gemini quota exhausted at {company_name} "
                        "(limit: 0). Skipping this company and continuing."
                    )
                    return [], False, "gemini_rate_limited"

                if attempt < GEMINI_MAX_RETRIES:
                    wait_seconds = GEMINI_RETRY_BASE_SECONDS * (2**attempt)
                    log(
                        f"⏳ Gemini rate-limited at {company_name}; "
                        f"retrying in {wait_seconds:.0f}s "
                        f"({attempt + 1}/{GEMINI_MAX_RETRIES})"
                    )
                    time.sleep(wait_seconds)
                    continue

                log(
                    f"⚠️ Gemini rate-limit persisted at {company_name}; "
                    "skipping this company and continuing."
                )
                return [], False, "gemini_rate_limited"

        if response is None:
            return [], True, "gemini"

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
        log(f"❌ Gemini failed at {company_name} ({exc})")
        return [], True, "gemini"


def openai_filter_jobs(
    client: OpenAI | None,
    model_name: str,
    company_name: str,
    link_list: list[dict[str, str]],
) -> tuple[list[dict[str, str]], bool, str]:
    if not link_list:
        return [], True, "none"
    if client is None:
        log(f"❌ OpenAI unavailable, skipping OpenAI filtering at {company_name}")
        return [], False, "openai_unavailable"

    prompt = f"""You are a strict job filter.

Goal:
Select ONLY jobs that match ALL conditions:
1) Location is in the United States (or remote within US)
2) Early-career role around 0-1 years experience
3) No security clearance required

Use the title, URL text, and any location cues available in the input.
If a job does not clearly satisfy all three, reject it.

Important:
- Inputs were prefiltered for software-engineering-like, early-career-like, US-like signals.
- You must still reject anything that is actually non-US, senior/staff/principal+, or requires security clearance.

Here is a list of candidate job links from {company_name}'s career page:
{json.dumps(link_list)}

Return ONLY a valid JSON array, no markdown, no explanation.
Format: [{{"title": "Job Title", "url": "https://..."}}]
If no early-career roles found, return exactly: []"""

    try:
        response = client.chat.completions.create(
            model=model_name,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
        )
        text = ""
        if response.choices and response.choices[0].message:
            text = response.choices[0].message.content or ""
        text = strip_markdown_fences(text)
        parsed = json.loads(text)
        if not isinstance(parsed, list):
            log(f"⚠️ OpenAI parse warning at {company_name}: non-array response")
            return [], True, "openai"
        out: list[dict[str, str]] = []
        for item in parsed:
            if not isinstance(item, dict):
                continue
            title = str(item.get("title", "")).strip()
            url = str(item.get("url", "")).strip()
            if title and url:
                out.append({"title": title, "url": url})
        log(f"🧠 OpenAI: {len(out)} early-career matches at {company_name}")
        return dedupe_links(out), True, "openai"
    except json.JSONDecodeError as exc:
        log(f"⚠️ OpenAI parse failed at {company_name} ({exc})")
        return [], True, "openai"
    except Exception as exc:
        message = str(exc).lower()
        if "429" in message or "quota" in message or "rate limit" in message:
            log(f"⚠️ OpenAI rate-limited at {company_name}; skipping OpenAI for this run")
            return [], False, "openai_rate_limited"
        log(f"❌ OpenAI failed at {company_name} ({exc})")
        return [], True, "openai"


def filter_jobs_with_ai(
    gemini_model: genai.GenerativeModel | None,
    openai_client: OpenAI | None,
    openai_model_name: str,
    gemini_enabled: bool,
    openai_enabled: bool,
    company_name: str,
    links: list[dict[str, str]],
) -> tuple[list[dict[str, str]], bool, bool, str]:
    if gemini_enabled:
        matches, next_gemini_enabled, mode = gemini_filter_jobs(
            gemini_model, company_name, links
        )
        if matches:
            return matches, next_gemini_enabled, openai_enabled, mode
        if mode not in ("gemini_rate_limited", "gemini_unavailable"):
            return matches, next_gemini_enabled, openai_enabled, mode
        gemini_enabled = next_gemini_enabled

    if openai_enabled:
        matches, next_openai_enabled, mode = openai_filter_jobs(
            openai_client, openai_model_name, company_name, links
        )
        return matches, gemini_enabled, next_openai_enabled, mode

    return [], gemini_enabled, openai_enabled, "ai_unavailable"


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
          <p style="margin-top:20px;color:#6b7280;font-size:12px;">Job Monitor • Next scan in {NEXT_SCAN_HOURS} hours</p>
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
    filtered_companies = filter_company_sources(companies)
    shard_companies = select_company_shard(filtered_companies)

    supabase = init_supabase()
    gemini_model = init_gemini()
    openai_client, openai_model_name = init_openai()
    resend_ready = init_resend()
    gemini_enabled = gemini_model is not None
    openai_enabled = openai_client is not None

    total_groups = max(1, int(os.getenv("TOTAL_GROUPS", "1")))
    group_index = int(os.getenv("GROUP_INDEX", "0"))
    source_mode = os.getenv("SOURCE_MODE", "all").strip().lower()
    log(
        f"🧩 Shard {group_index + 1}/{total_groups} ({source_mode}) active — "
        f"{len(shard_companies)} of {len(filtered_companies)} selected sources "
        f"({len(companies)} total in file)"
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
                goto_with_retry(page, company_name, company_url)
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

            ai_matches, gemini_enabled, openai_enabled, filter_mode = filter_jobs_with_ai(
                gemini_model,
                openai_client,
                openai_model_name,
                gemini_enabled,
                openai_enabled,
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
