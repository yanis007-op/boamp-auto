import os
import requests
from supabase import create_client

ODSOFT_ENDPOINT = "https://boamp-datadila.opendatasoft.com/api/records/1.0/search/"

def fetch_boamp(rows: int = 100):
    params = {
        "dataset": "boamp",
        "rows": rows,
    }
    r = requests.get(ODSOFT_ENDPOINT, params=params, timeout=30)
    if r.status_code != 200:
        print("URL called:", r.url)
        print("Status:", r.status_code)
        print("Body:", r.text[:500])
    r.raise_for_status()
    return r.json().get("records", [])

def main():
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if not supabase_url or not supabase_key:
        raise RuntimeError("Missing Supabase credentials: SUPABASE_URL and/or SUPABASE_SERVICE_ROLE_KEY")

    supabase = create_client(supabase_url, supabase_key)

    records = fetch_boamp(rows=100)

    payload = []
    for rec in records:
        record_id = rec.get("recordid")
        if record_id:
            payload.append({
                "id": record_id,
                "raw": rec
            })

    if not payload:
        print("⚠️ No records found from Opendatasoft.")
        return

    res = supabase.table("boamp_notices").upsert(payload).execute()
    print(f"✅ Imported/Upserted {len(payload)} records into boamp_notices")

if __name__ == "__main__":
    main()
