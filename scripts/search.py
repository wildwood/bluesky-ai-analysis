import faiss
import json
import numpy as np
from sentence_transformers import SentenceTransformer

import torch
torch.set_num_threads(1)

# ------------------------
# Config
# ------------------------
INDEX_PATH = "faiss_index/index.faiss"
META_PATH = "faiss_index/metadata.json"
MODEL_NAME = "all-MiniLM-L6-v2"
TOP_K = 5

# ------------------------
# Load index + metadata
# ------------------------
print("Loading FAISS index...")
index = faiss.read_index(INDEX_PATH)

with open(META_PATH) as f:
    metadata = json.load(f)

print(f"Loaded index with {len(metadata)} posts.")

# ------------------------
# Load embedding model
# ------------------------
model = SentenceTransformer(MODEL_NAME)

# ------------------------
# Query loop
# ------------------------
query = input("\nEnter your search query: ").strip()

query_embedding = model.encode([query])
query_embedding = np.array(query_embedding).astype("float32")

D, I = index.search(query_embedding, TOP_K)

print("\nTop matches:")
for idx, dist in zip(I[0], D[0]):
    print(f"URI: {metadata[idx]['uri']}")
    print(f"Text: {metadata[idx]['text']}")
    print(f"Distance: {dist:.4f}")
