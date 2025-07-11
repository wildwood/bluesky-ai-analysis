import argparse
import sqlite3
import json
import numpy as np
import faiss
from pathlib import Path

# ----------------------------------------
# Config
# ----------------------------------------
DB_PATH = "bluesky_posts.db"
INDEX_PATH = Path("faiss_index/index.faiss")
META_PATH = Path("faiss_index/metadata.json")

INDEX_PATH.parent.mkdir(exist_ok=True)

EMBEDDING_DIM = 384  # for MiniLM

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
# Connect to DB
# ----------------------------------------
conn = sqlite3.connect(DB_PATH)
conn.execute("PRAGMA journal_mode=WAL;")
cursor = conn.cursor()

cursor.execute("""
    SELECT uri, embedding, text
    FROM posts
    WHERE embedding IS NOT NULL
""")

rows = cursor.fetchall()
print(f"Loaded {len(rows)} embeddings.")

# ----------------------------------------
# Build NumPy matrix
# ----------------------------------------
embeddings = []
metadata = []

for uri, embedding_json, text in rows:
    vector = np.array(json.loads(embedding_json), dtype=np.float32)
    embeddings.append(vector)
    metadata.append({"uri": uri, "text": text})

embeddings = np.vstack(embeddings)
print(f"Embeddings shape: {embeddings.shape}")

# ----------------------------------------
# Build FAISS index
# ----------------------------------------
index = faiss.IndexFlatL2(EMBEDDING_DIM)
index.add(embeddings)

faiss.write_index(index, str(INDEX_PATH))
print(f"Saved FAISS index to {INDEX_PATH}")

# Save URI lookup
with open(META_PATH, "w") as f:
    json.dump(metadata, f, indent=2)
print(f"Saved metadata to {META_PATH}")

conn.close()
