# Bluesky Ingest & Embed Pipeline

## 🚀 What this does

- Pulls real-time posts from Bluesky's Jetstream feed
- Stores raw posts in SQLite (`bluesky_posts.db`)
- Generates semantic embeddings with sentence-transformers
- Builds a FAISS index for semantic search

## 📂 Project structure

- `stream_to_file.py` — Jetstream listener outputs to json
- `file_to_db.py` - load json into sqlite
- `embed.py` — Embedding batch script
- `build_faiss.py` — Creates FAISS index
- `search.py` — Query FAISS for nearest posts

## ⚙️ Requirements

- Python 3.11+
- SQLite
- `sentence-transformers`, `torch`, `faiss-cpu`

## 🔑 Notes

- Keep `.env` or API keys out of version control!
- Use a `.gitignore` for `*.db`, `*.env`, `__pycache__`, etc.
