import os
from datetime import datetime, timezone

import requests
from supabase import create_client

ODSOFT_ENDPOINT = "https://boamp-datadila.opendatasoft.com/api/records/1.0/search/"


def fetch_boamp_open(rows: int = 1000, max_pages: int = 50):
    """
    Récupère les avis BOAMP encore ouverts (datelimitereponse >= aujourd'hui).
    Pagination via start.
    """
    today = datetime.now(timezone.utc).date().isoformat()  # ex: "2026-02-13"

    all_records = []
    start = 0

    # ODSQL: date limite >= aujourd'hui
    where = f"datelimitereponse >= date'{today}'"

    for _ in range(max_pages):
        params = {
            "dataset": "boamp",
            "rows": rows,
            "start": start,
            "where": where,
            "sort": "-datelimitereponse",
        }

        r = requests.get(ODSOFT_ENDPOINT, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        records = data.get("records", [])

        if not records:
            break

        all_records.extend(records)
        start += rows

    return all_records


def main():
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if not supabase_url or not supabase_key:
        raise RuntimeError(
            "Missing Supabase credentials: SUPABASE_URL and/or SUPABASE_SERVICE_ROLE_KEY"
        )

    supabase = create_client(supabase_url, supabase_key)

    records = fetch_boamp_open(rows=1000)
    print(f"🔎 Open tenders fetched: {len(records)}")

    payload = []
    for rec in records:
        record_id = rec.get("recordid")
        if record_id:
            payload.append({"id": record_id, "raw": rec})

    if not payload:
        print("⚠️ No open tenders found from Opendatasoft.")
        return

    supabase.table("boamp_notices").upsert(payload).execute()
    print(f"✅ Imported/Upserted {len(payload)} open tenders into boamp_notices")


if __name__ == "__main__":
    main()