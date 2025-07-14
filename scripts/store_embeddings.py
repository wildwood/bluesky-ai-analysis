import argparse
import logging
import sqlite3
import sys
import time

import numpy as np
from sentence_transformers import SentenceTransformer

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.info("Logger created.")

# ----------------------------------------
# Config
# ----------------------------------------
DB_PATH = "bluesky_posts.db"
MODEL_NAME = "all-MiniLM-L6-v2"
BATCH_SIZE = 128  # this should be a command line arg soon
MAX_COMMENT_LEN = 300

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

args = parser.parse_args()
DB_PATH = args.db_path

# ----------------------------------------
# Load model
# ----------------------------------------
logger.info(f"Loading embedding model: {MODEL_NAME}")
model = SentenceTransformer(MODEL_NAME)

# ----------------------------------------
# Open DB connection
# ----------------------------------------
conn = sqlite3.connect(DB_PATH)
conn.execute("PRAGMA journal_mode=WAL;")
cursor = conn.cursor()

# ----------------------------------------
# Fetch posts that need embeddings
# ----------------------------------------
cursor.execute("""
    SELECT uri, text FROM posts
    WHERE embedding IS NULL AND LENGTH(text) > 10 AND langs='en'
    ORDER BY created_date DESC, created_hour DESC
    LIMIT 30000;
""")
rows = cursor.fetchall()
logger.info(f"Loaded {len(rows)} posts without embeddings.")

if not rows:
    sleep_seconds = 60
    logger.info(f"Nothing to embed — sleeping {sleep_seconds} seconds before exiting.")
    time.sleep(sleep_seconds)
    exit(0)

# ----------------------------------------
# Embed in batches
# ----------------------------------------
uris = [row[0] for row in rows]
texts = [row[1][:MAX_COMMENT_LEN] for row in rows]

logger.info(f"Embedding {len(texts)} posts...")

for i in range(0, len(texts), BATCH_SIZE):
    batch_texts = texts[i:i + BATCH_SIZE]
    batch_uris = uris[i:i + BATCH_SIZE]

    embeddings = model.encode(batch_texts, show_progress_bar=False)

    for uri, embedding in zip(batch_uris, embeddings):
        vector = np.array(embedding, dtype=np.float32)
        # Pack to bytes
        embedding_blob = vector.tobytes()

        cursor.execute("""
            UPDATE posts
            SET embedding_blob = ?, embedding = 'y'
            WHERE uri = ?
        """, (embedding_blob, uri))

    conn.commit()
    if i // BATCH_SIZE % 10 == 0:
        logger.info(f"Committed batch {i // BATCH_SIZE + 1}")

# ----------------------------------------
# Done
# ----------------------------------------
logger.info("All embeddings saved.")
conn.close()
