import argparse
import datetime
import sqlite3
import pandas as pd

DATE_FORMAT_STRING = "%Y-%m-%d"

def get_date_strings(last_date, numDays):
    days = [last_date.strftime(DATE_FORMAT_STRING)]
    while len(days) < numDays:
        last_date -= datetime.timedelta(days=1)
        days.append(last_date.strftime(DATE_FORMAT_STRING))
    return days


DB_PATH = "bluesky_posts.db"
CURRENT_DATE = datetime.date.today()
DAYS_BACK = 3

# ----------------------------------------
# Parse command line
# ----------------------------------------
parser = argparse.ArgumentParser(
    description="Ingest Bluesky posts or generate embeddings."
)

parser.add_argument(
    "--db-path",
    type=str,
    default=DB_PATH,
    help="Path to the SQLite database file."
)
parser.add_argument(
    "--current-date",
    type=str,
    default=CURRENT_DATE.strftime(DATE_FORMAT_STRING),
    help="Latest date to start export from.  ('YYYY-MM-DD')"
)

args = parser.parse_args()
DB_PATH = args.db_path
CURRENT_DATE = datetime.datetime.strptime(args.current_date, DATE_FORMAT_STRING)
days = get_date_strings(CURRENT_DATE, DAYS_BACK)

# Connect to the SQLite database
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

placeholders = ','.join('?' for _ in days)

# Run the query
cursor.execute(f"""
SELECT uri, created_at, created_date, created_hour, text, embedding_blob
FROM posts
WHERE embedding_blob IS NOT NULL
AND LANGS='en'
AND created_date IN ({placeholders})
""", days)

# Process rows
rows = cursor.fetchall()
data = []

for row in rows:
    uri, created_at, created_date, created_hour, text, blob = row
    try:
        embedding = list(memoryview(blob).cast('f'))
    except Exception:
        embedding = None
    parts = uri.split('/')
    handle = parts[2] if len(parts) > 2 else ''
    post_id = parts[-1] if len(parts) > 3 else ''
    url = f"https://bsky.app/profile/{handle}/post/{post_id}"
    data.append({
        "uri": uri,
        "post_url": url,
        "created_at": created_at,
        "created_date": created_date,
        "created_hour": created_hour,
        "text": text,
        "embedding": embedding
    })

# Create DataFrame
df = pd.DataFrame(data)

# Save to Parquet
print(f"Exporting {len(df)} rows to Parquet")
df.to_parquet("exported_posts.parquet", index=False)
