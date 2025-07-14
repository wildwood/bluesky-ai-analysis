import argparse
import asyncio
import websockets
import cbor2
import sqlite3
import json
import datetime

JETSTREAM_URI = "wss://jetstream1.us-west.bsky.network/subscribe"
TARGET_COLLECTION = "app.bsky.feed.post"
DB_PATH = "bluesky_posts.db"

# Set up SQLite DB
def init_db():
    print("Connecting to databse ", DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS posts (
            uri TEXT PRIMARY KEY,
            repo TEXT,
            rkey TEXT,
            created_at TEXT,
            created_date TEXT,
            created_hour INTEGER,
            text TEXT,
            langs TEXT,
            raw_json TEXT,
            embedding TEXT,
            embedding_blob BLOB
        );
    ''')
    c.execute('''
        CREATE INDEX IF NOT EXISTS idx_posts_created_date_hour ON posts (created_date, created_hour);
    ''')
    conn.commit()
    return conn

# Main ingestion loop
async def listen_and_store():
    conn = init_db()

    async with websockets.connect(JETSTREAM_URI) as ws:
        print("Connected to Jetstream...")
        while True:
            msg = await ws.recv()
            data = json.loads(msg)
            #print(f"Decoded: {data}")

            if data.get("kind") == "commit":
                commit = data.get("commit", {})
                if (
                        commit.get("operation") == "create" and
                        commit.get("collection") == "app.bsky.feed.post"
                ):
                    record = commit.get("record", {})
                    if not record:
                        return

                    post_uri = f"at://{data['did']}/{commit['collection']}/{commit['rkey']}"
                    rkey = commit["rkey"]
                    text = record.get("text", "")
                    created_at = record.get("createdAt", "")
                    langs = ",".join(record.get("langs", [])) if "langs" in record else None

                    #print(f"Inserting post: {text[:40]}...")

                    dt = datetime.datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                    created_date = dt.date().isoformat()  # '2025-06-27'
                    created_hour = dt.hour  # 14

                    c = conn.cursor()
                    c.execute('''
                        INSERT OR IGNORE INTO posts
                        (uri, repo, rkey, created_at, created_date, created_hour, text, langs)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (post_uri, data["did"], rkey, created_at, created_date, created_hour, text, langs))

                    conn.commit()

if __name__ == "__main__":
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

    asyncio.run(listen_and_store())
