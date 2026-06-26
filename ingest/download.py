# ingest/download.py
# Download one day's 24 hourly GH Archive files into data/.
# Idempotent: skips files already on disk. Validates each download.
# Called by the Airflow DAG as: python ingest/download.py YYYYMMDD

import sys
import os
import urllib.request

# --- where files land (the gitignored data/ folder) ---
DATA_DIR = "/Users/felipefumero/projects/github-firehose-activity/data"
BASE_URL = "https://data.gharchive.org"

def download_day(date_yyyymmdd):
    # turn 20240602 → 2024-06-02 (GH Archive's URL date format)
    y, m, d = date_yyyymmdd[:4], date_yyyymmdd[4:6], date_yyyymmdd[6:8]
    date_dashed = f"{y}-{m}-{d}"

    os.makedirs(DATA_DIR, exist_ok=True)  # ensure data/ exists

    # GH Archive has one file per HOUR (0–23, NOT zero-padded)
    for hour in range(24):
        filename = f"{date_dashed}-{hour}.json.gz"
        url = f"{BASE_URL}/{filename}"
        dest = os.path.join(DATA_DIR, filename)

        # --- IDEMPOTENT CHECK: already downloaded + valid? skip it ---
        # We check size > 1KB to skip the tiny error-page files (the Day 1 missing-hour lesson).
        if os.path.exists(dest) and os.path.getsize(dest) > 1024:
            print(f"  skip (exists): {filename}")
            continue

        # --- download ---
        try:
            urllib.request.urlretrieve(url, dest)
            size = os.path.getsize(dest)

            # --- VALIDATE: a real file is MBs; a 127-byte error page is not ---
            if size < 1024:
                print(f"  MISSING/invalid hour, removing: {filename} ({size} bytes)")
                os.remove(dest)        # delete the bad stub so it's not mistaken for data
            else:
                print(f"  downloaded: {filename} ({size // (1024*1024)} MB)")
        except Exception as e:
            print(f"  FAILED: {filename} — {e}")

if __name__ == "__main__":
    # the date comes from the DAG ({{ ds_nodash }}); default to 20240602 if run by hand
    date_arg = sys.argv[1] if len(sys.argv) > 1 else "20240602"
    print(f"=== downloading GH Archive for {date_arg} ===")
    download_day(date_arg)
    print("=== done ===")