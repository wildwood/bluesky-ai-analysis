import argparse
import datetime
import duckdb
import sqlite3
import numpy as np
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
DAYS_BACK = 1#3

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

# Connect to DuckDB and attach SQLite
con = duckdb.connect()
con.execute("INSTALL sqlite_scanner;")
con.execute("LOAD sqlite_scanner;")
con.execute(f"ATTACH '{DB_PATH}' AS mydb (TYPE sqlite);")

escaped_days = ', '.join(f"'{d}'" for d in days)

# Query posts from SQLite via DuckDB
query = f"""
        SELECT uri, created_at, created_date, created_hour, text, embedding_blob
        FROM mydb.posts
        WHERE created_date in ({escaped_days})
        AND created_hour = 19
        AND embedding_blob IS NOT NULL
"""
print("Getting query plan")
con.execute("SET explain_output = 'all';")
explain_result = con.execute(f"EXPLAIN {query}").fetchall()
print("Query plan:")
for row in explain_result:
    print(row[0])
print("Executing query: ", query)
print("Using dates: ", days)
df = con.execute(query).fetchdf()
print("received rows:", len(df))

# Convert embedding_blob from binary to list of floats
def decode_embedding(blob):
    try:
        return np.frombuffer(blob, dtype=np.float32).tolist()
    except Exception:
        return None

df["embedding"] = df["embedding_blob"].apply(decode_embedding)
df = df.drop(columns=["embedding_blob"])

def build_live_link(uri):
    parts = uri.split('/')
    handle = parts[2] if len(parts) > 2 else ''
    post_id = parts[-1] if len(parts) > 3 else ''
    return f"https://bsky.app/profile/{handle}/post/{post_id}"

df["post_url"] = df["uri"].apply(build_live_link)
#df = df.drop(columns={"post_url"})

# Export to Parquet
filename = f"posts-{CURRENT_DATE.strftime(DATE_FORMAT_STRING)}.parquet"
df.to_parquet(filename, index=False)
print("Export complete. Rows written:", len(df))
