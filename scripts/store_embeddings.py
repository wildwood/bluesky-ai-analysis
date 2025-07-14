import argparse
import sqlite3

import numpy as np
from sentence_transformers import SentenceTransformer

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
print(f"Loading embedding model: {MODEL_NAME}")
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
    LIMIT 10000;
""")
rows = cursor.fetchall()
print(f"Loaded {len(rows)} posts without embeddings.")

if not rows:
    print("Nothing to embed — exiting.")
    exit(0)

# ----------------------------------------
# Embed in batches
# ----------------------------------------
uris = [row[0] for row in rows]
texts = [row[1][:MAX_COMMENT_LEN] for row in rows]

print(f"Embedding {len(texts)} posts...")

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
            SET embedding_blob = ?
            WHERE uri = ?
        """, (embedding_blob, uri))

    conn.commit()
    if i // BATCH_SIZE % 10 == 0:
        print(f"Committed batch {i // BATCH_SIZE + 1}")

# ----------------------------------------
# Done
# ----------------------------------------
print("All embeddings saved.")
conn.close()
