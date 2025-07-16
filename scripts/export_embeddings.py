import argparse
import datetime
import logging
import sqlite3
import numpy as np
import pandas as pd

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.info("Logger created.")

DATE_FORMAT_STRING = "%Y-%m-%d"

def get_date_strings(last_date, numDays):
    days = [last_date.strftime(DATE_FORMAT_STRING)]
    while len(days) < numDays:
        last_date -= datetime.timedelta(days=1)
        days.append(last_date.strftime(DATE_FORMAT_STRING))
    return days

# Convert embedding_blob from binary to list of floats
def decode_embedding(blob):
    try:
        return np.frombuffer(blob, dtype=np.float32).tolist()
    except Exception:
        return None

def build_live_link(uri):
    parts = uri.split('/')
    handle = parts[2] if len(parts) > 2 else ''
    post_id = parts[-1] if len(parts) > 3 else ''
    return f"https://bsky.app/profile/{handle}/post/{post_id}"

DB_PATH = "bluesky_posts.db"
CURRENT_DATE = datetime.date.today()
DAYS_BACK = 3
OUTPUT_DIR = "."

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
parser.add_argument(
    "--output-dir",
    type=str,
    default= OUTPUT_DIR,
    help="Directory for output files.  Needs to already exist."
)

args = parser.parse_args()
DB_PATH = args.db_path
CURRENT_DATE = datetime.datetime.strptime(args.current_date, DATE_FORMAT_STRING)
days = get_date_strings(CURRENT_DATE, DAYS_BACK)
OUTPUT_DIR = args.output_dir

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

query = f"""
    SELECT uri, created_at, created_date, created_hour, text, embedding_blob
    FROM posts
    WHERE created_date = ?
    AND created_hour = ?
    AND embedding_blob IS NOT NULL
"""

for day in days:
    for hour in range(24):
        logger.info(f"Starting sqlite query for {day} {hour:0>2}")
        cursor.execute(query, (day, hour))
        rows = cursor.fetchall()
        logger.info("Finished sqlite query")
        logger.info("rows: ", len(rows))

        batch_size = 10_000
        for i in range(0, len(rows), batch_size):
            chunk = rows[i:i + batch_size]

            df = pd.DataFrame(chunk, columns=[
                "uri", "created_at", "created_date", "created_hour", "text", "embedding_blob"
            ])
            logger.info(f"Loaded {len(df)} rows from SQLite")

            df["embedding"] = df["embedding_blob"].apply(decode_embedding)
            df = df.drop(columns=["embedding_blob"])
            df["post_url"] = df["uri"].apply(build_live_link)

            # Export to Parquet
            filename = f"{OUTPUT_DIR}/posts-{day}-{hour:0>2}.{i//batch_size}.parquet"
            df.to_parquet(filename, index=False)
            logger.info(f"Wrote {len(df)} rows to {filename}")
        logger.info(f"Export complete for {day} {hour:0>2}.")
    logger.info(f"Finished with day {day}")

conn.close()
