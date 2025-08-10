#!/usr/bin/env python3
import argparse
import json
import os
import re
import sqlite3
import time
from datetime import datetime

DDL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA busy_timeout=5000;
PRAGMA wal_autocheckpoint=1000;

CREATE TABLE IF NOT EXISTS posts (
  uri            TEXT PRIMARY KEY,
  author_did     TEXT NOT NULL,
  rkey           TEXT NOT NULL,
  cid            TEXT,
  created_at     TEXT NOT NULL,
  time_us        INTEGER NOT NULL,
  indexed_first  INTEGER,
  indexed_last   INTEGER,
  text           TEXT,
  reply_parent   TEXT,
  reply_root     TEXT,
  quote_uri      TEXT,
  langs_json     TEXT,
  lang_en        INTEGER NOT NULL DEFAULT 0,
  has_embedding  INTEGER NOT NULL DEFAULT 0,
  is_reply       INTEGER GENERATED ALWAYS AS (reply_parent IS NOT NULL) STORED,
  is_quote       INTEGER GENERATED ALWAYS AS (quote_uri    IS NOT NULL) STORED,
  created_day    TEXT GENERATED ALWAYS AS (substr(created_at,1,10)) STORED,
  created_hour   TEXT GENERATED ALWAYS AS (substr(created_at,1,13)) STORED,
  emb_model      TEXT,
  emb_dims       INTEGER,
  emb_vec        BLOB
);

CREATE INDEX IF NOT EXISTS idx_posts_created           ON posts(created_at);
CREATE INDEX IF NOT EXISTS idx_posts_author_created    ON posts(author_did, created_at);
CREATE INDEX IF NOT EXISTS idx_posts_created_day       ON posts(created_day);
CREATE INDEX IF NOT EXISTS idx_posts_created_hour      ON posts(created_hour);
CREATE INDEX IF NOT EXISTS idx_posts_lang_en_day       ON posts(lang_en, created_day);
CREATE INDEX IF NOT EXISTS idx_posts_timeus            ON posts(time_us);
CREATE INDEX IF NOT EXISTS idx_posts_has_embedding_day ON posts(has_embedding, created_day);
"""

UPSERT = """
INSERT INTO posts (uri, author_did, rkey, cid, created_at, time_us,
                   indexed_first, indexed_last, text,
                   reply_parent, reply_root, quote_uri,
                   langs_json, lang_en,
                   emb_model, emb_dims, emb_vec, has_embedding)
VALUES (:uri, :author_did, :rkey, :cid, :created_at, :time_us,
        :indexed_at, :indexed_at, :text,
        :reply_parent, :reply_root, :quote_uri,
        :langs_json, :lang_en,
        :emb_model, :emb_dims, :emb_vec, :has_embedding)
ON CONFLICT(uri) DO UPDATE SET
  cid           = excluded.cid,
  created_at    = COALESCE(excluded.created_at, posts.created_at),
  time_us       = COALESCE(posts.time_us, excluded.time_us),
  indexed_first = MIN(posts.indexed_first, excluded.indexed_first),
  indexed_last  = MAX(posts.indexed_last,  excluded.indexed_last),
  text          = excluded.text,
  reply_parent  = excluded.reply_parent,
  reply_root    = excluded.reply_root,
  quote_uri     = excluded.quote_uri,
  langs_json    = excluded.langs_json,
  lang_en       = excluded.lang_en,
  emb_model     = excluded.emb_model,
  emb_dims      = excluded.emb_dims,
  emb_vec       = COALESCE(excluded.emb_vec, posts.emb_vec),
  has_embedding = CASE WHEN excluded.emb_vec IS NOT NULL THEN 1 ELSE posts.has_embedding END;
"""

def ensure_db(db_path):
    new = not os.path.exists(db_path)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=ON;")
    if new:
        conn.executescript(DDL)
        conn.commit()
    return conn

def load_checkpoint(state_dir, path):
    os.makedirs(state_dir, exist_ok=True)
    ck_path = os.path.join(state_dir, "import_checkpoints.json")
    try:
        with open(ck_path, "r") as f:
            data = json.load(f)
    except Exception:
        data = {}
    return data.get(path, 0), ck_path, data

def save_checkpoint(ck_path, data):
    tmp = ck_path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, separators=(",", ":"))
        f.flush(); os.fsync(f.fileno())
    os.replace(tmp, ck_path)

def iter_files_for_day(indir, day):
    # day: YYYY-MM-DD
    for h in range(24):
        hh = f"{h:02d}"
        base = f"{day}{hh}.ndjson"
        for suffix in ("", ".part"):
            path = os.path.join(indir, base + suffix)
            if os.path.exists(path):
                yield path

def parse_line(line, indexed_at_us):
    ev = json.loads(line)
    # Expect flattened post records produced by your stream writer
    # Required fields check (light)
    if ev.get("kind") != "post" or "uri" not in ev:
        return None
    required = ["uri", "did", "rkey", "created_at", "time_us"]
    if any(ev.get(k) in (None, "", 0) for k in required):
        print(f"Error with line: {line}")  # append to dead-letter.ndjson?
        return None
    langs = ev.get("langs") or []
    if isinstance(langs, str):
        langs = [langs]
    rec = {
        "uri": ev["uri"],
        "author_did": ev.get("did") or ev.get("repo"),
        "rkey": ev.get("rkey"),
        "cid": ev.get("cid"),
        "created_at": ev.get("created_at") or "",
        "time_us": int(ev.get("time_us") or 0),
        "indexed_at": indexed_at_us,
        "text": ev.get("text"),
        "reply_parent": ev.get("reply_parent"),
        "reply_root": ev.get("reply_root"),
        "quote_uri": ev.get("quote_uri"),
        "langs_json": json.dumps(langs, separators=(",", ":")),
        "lang_en": 1 if "en" in langs else 0,
        "emb_model": None,
        "emb_dims": None,
        "emb_vec": None,
        "has_embedding": 0,
    }
    return rec

def import_file(conn, path, start_byte, batch, ck_data, ck_path):
    print(f"Importing {path}")
    size = os.path.getsize(path)
    if size <= start_byte:
        return start_byte
    cur = conn.cursor()
    inserted = 0
    with open(path, "r") as f:
        f.seek(start_byte)
        buf = []
        indexed_at_us = int(time.time() * 1_000_000)
        while True:
            pos = f.tell()
            line = f.readline()
            if not line:
                break
            rec = parse_line(line, indexed_at_us)
            if not rec:
                continue
            buf.append(rec)
            if len(buf) >= batch:
                cur.executemany(UPSERT, buf)
                conn.commit()
                inserted += len(buf)
                buf.clear()
                # checkpoint bytes so we can resume safely
                ck_data[path] = f.tell()
                save_checkpoint(ck_path, ck_data)
        if buf:
            cur.executemany(UPSERT, buf)
            conn.commit()
            inserted += len(buf)
            ck_data[path] = f.tell()
            save_checkpoint(ck_path, ck_data)
    # Keep WAL trimmed
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
    conn.commit()
    print(f"[import] {os.path.basename(path)} +{inserted} rows")
    return ck_data[path]

def day_to_dbpath(outdir, day):
    return os.path.join(outdir, f"posts_{day}.db")

DAY_RE = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}\.ndjson(?:\.part)?$')

def files_grouped_by_day(indir):
    by_day = {}
    for name in os.listdir(indir):
        if not DAY_RE.match(name):
            continue
        day = name[:10]  # YYYY-MM-DD from filename
        by_day.setdefault(day, []).append(os.path.join(indir, name))
    # sort hours within each day
    for day in by_day:
        by_day[day].sort()
    return dict(sorted(by_day.items()))  # oldest day first

def main():
    ap = argparse.ArgumentParser(description="Import NDJSON into per-day SQLite DBs")
    ap.add_argument("--indir", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--state", default="state")
    ap.add_argument("--batch", type=int, default=2000)
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    state_dir = args.state if os.path.isabs(args.state) else os.path.join(args.outdir, args.state)

    groups = files_grouped_by_day(args.indir)
    for day, paths in groups.items():
        db_path = os.path.join(args.outdir, f"posts_{day}.db")
        conn = ensure_db(db_path)
        try:
            for path in paths:
                start, ck_path, ck_data = load_checkpoint(state_dir, path)
                import_file(conn, path, start, args.batch, ck_data, ck_path)
        finally:
            conn.close()

def old_main():
    ap = argparse.ArgumentParser(description="Import hourly NDJSON into a per-day SQLite DB")
    ap.add_argument("--indir", required=True, help="Input directory with *.ndjson / *.ndjson.part")
    ap.add_argument("--outdir", required=True, help="Output directory for day DBs")
    ap.add_argument("--state", default="state", help="Directory to store checkpoints.json")
    ap.add_argument("--day", required=True, help="Day to import, YYYY-MM-DD (UTC)")
    ap.add_argument("--batch", type=int, default=2000, help="Rows per transaction")
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    state_dir = args.state if os.path.isabs(args.state) else os.path.join(args.outdir, args.state)

    db_path = day_to_dbpath(args.outdir, args.day)
    conn = ensure_db(db_path)

    try:
        for path in iter_files_for_day(args.indir, args.day):
            start, ck_path, ck_data = load_checkpoint(state_dir, path)
            newpos = import_file(conn, path, start, args.batch, ck_data, ck_path)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
