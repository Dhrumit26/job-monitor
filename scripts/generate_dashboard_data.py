import json
import os
from datetime import datetime, timezone

from supabase import create_client


def main() -> None:
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")
    if not supabase_url or not supabase_key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_KEY are required")

    supabase = create_client(supabase_url, supabase_key)
    response = (
        supabase.table("seen_jobs")
        .select("title,company,url,ai_reason,matched,date_found")
        .order("date_found", desc=True)
        .limit(5000)
        .execute()
    )
    rows = response.data or []
    matched_rows = [row for row in rows if row.get("matched")]

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_rows": len(rows),
        "matched_rows": len(matched_rows),
        "rows": rows,
    }

    docs_dir = "docs"
    os.makedirs(docs_dir, exist_ok=True)
    with open(os.path.join(docs_dir, "data.json"), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=True, indent=2)


if __name__ == "__main__":
    main()
