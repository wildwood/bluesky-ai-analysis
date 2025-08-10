#!/usr/bin/env python3
import argparse, asyncio, json, os, shutil, signal, sys, time
from datetime import datetime, timezone
import websockets

# ---- helpers ---------------------------------------------------------------

def hour_key_from_timeus(time_us: int) -> str:
    # time_us is microseconds since epoch (UTC)
    ts = time_us / 1_000_000.0
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    # flat pattern: YYYY-MM-DDTHH.ndjson / .part
    return dt.strftime("%Y-%m-%dT%H")


def atomic_write_json(path, obj):
    tmp = path + ".tmp"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(tmp, "w") as f:
        json.dump(obj, f, separators=(",", ":"))
        f.flush(); os.fsync(f.fileno())
    os.replace(tmp, path)


def commit_checkpoint_timestamp(time_us: int, cursor_path):
    if time_us is not None:
        atomic_write_json(cursor_path, {"time_us": time_us})


def load_cursor(path):
    try:
        with open(path, "r") as f:
            return int(json.load(f)["time_us"])
    except Exception:
        return None


def should_skip_event(ev):
    time_us = ev.get("time_us")
    if not isinstance(time_us, int):
        return True
    if ev.get("kind") != "commit":
        return True

    commit = ev.get("commit", {})
    if (
            commit.get("operation") != "create" or
            commit.get("collection") != "app.bsky.feed.post"
    ):
        return True

    record = commit.get("record", {})
    if not record:
        return True

    return False

def flatten_post_events(ev):
    """
    Return a list of flattened records from a Jetstream message.
    Assume message has already been checked to be a bluesky post.
    Handles commit/create events for app.bsky.feed.post.
    """
    time_us = ev.get("time_us")

    commit = ev.get("commit", {})
    record = commit.get("record", {})

    did = ev.get("did") or ev.get("repo")
    cid = commit.get("cid")

    post_uri = f"at://{did}/{commit['collection']}/{commit['rkey']}"
    rkey = commit["rkey"]
    text = record.get("text", "")
    created_at = record.get("createdAt", "")
    langs = record.get("langs", [])
    if isinstance(langs, str):
        langs = [langs]
    if langs is None:
        langs = []

    # reply pointers
    reply = record.get("reply") or {}
    parent_uri = (reply.get("parent") or {}).get("uri")
    root_uri = (reply.get("root") or {}).get("uri")

    # quote pointer (if any)
    quote_uri = None
    embed = record.get("embed") or {}
    if embed.get("$type") == "app.bsky.embed.record#view" and "record" in embed:
        quote_uri = (embed["record"].get("uri")
                     if isinstance(embed["record"], dict) else None)
    elif embed.get("$type") == "app.bsky.embed.record" and "record" in embed:
        quote_uri = (embed["record"].get("uri")
                     if isinstance(embed["record"], dict) else None)

    # print(f"Inserting post: {text[:40]}...")

    return {
        "kind": "post",
        "uri": post_uri,
        "cid": cid,
        "did": did,
        "rkey": rkey,
        "created_at": created_at,
        "time_us": time_us,
        "text": text,
        "reply_parent": parent_uri,
        "reply_root": root_uri,
        "quote_uri": quote_uri,
        "langs": langs
    }

# ---- runtime ---------------------------------------------------------------

async def run(outdir: str, url: str, flush_count: int, flush_seconds: float):
    state_dir = os.path.join(outdir, "state")
    os.makedirs(state_dir, exist_ok=True)
    cursor_path = os.path.join(state_dir, "cursor.json")

    max_time_us = load_cursor(cursor_path)

    # open file handles per hour -> {"hour":"fileobj or None for closed"}
    open_files = {}  # hour -> fileobj

    # graceful shutdown
    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())

    last_flush = time.time()

    async def flush_and_checkpoint(force=False):
        nonlocal last_flush, max_time_us
        now = time.time()
        if not force and (now - last_flush) < flush_seconds:
            return
        # fsync and close/reopen to ensure durability
        for hour, fh in list(open_files.items()):
            if fh is None:
                continue
            fh.flush(); os.fsync(fh.fileno())
            # if this is a .part for a past hour, roll it to .ndjson
            # current wall-clock hour keeps .part open
            now_hour = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H")
            if hour != now_hour:
                path_part = os.path.join(outdir, f"{hour}.ndjson.part")
                path_final = os.path.join(outdir, f"{hour}.ndjson")
                fh.close()
                with open(path_final, "a") as out, open(path_part, "r") as inp:
                    shutil.copyfileobj(inp, out)
                    out.flush()
                    os.fsync(out.fileno())
                os.unlink(path_part)
                open_files[hour] = None  # mark closed
        commit_checkpoint_timestamp(max_time_us, cursor_path)
        last_flush = now

    async def writer_loop():
        nonlocal max_time_us
        pending = 0
        try:
            qs = f"?cursor={max_time_us}" if max_time_us else ""
            async with websockets.connect(url + qs, max_size=None) as ws:
                print(f"[connect] {url}{qs}", flush=True)
                while not stop.is_set():
                    msg = await ws.recv()  # text JSON per message
                    ev = json.loads(msg)
                    if should_skip_event(ev):
                        continue

                    tu = ev.get("time_us")
                    hour = hour_key_from_timeus(tu)

                    # choose target file path
                    path = os.path.join(outdir, f"{hour}.ndjson.part")

                    # open if needed
                    fh = open_files.get(hour, None)
                    if fh is None:
                        fh = open(path, "a", buffering=1)  # line-buffered
                        open_files[hour] = fh

                    # write line
                    fh.write(json.dumps(flatten_post_events(ev), separators=(",", ":")) + "\n")
                    pending += 1

                    # advance cursor to the max observed (events can arrive slightly out of order)
                    if max_time_us is None or tu > max_time_us:
                        max_time_us = tu

                    # flush conditions
                    if pending >= flush_count or (time.time() - last_flush) >= flush_seconds:
                        await flush_and_checkpoint()
                        pending = 0
        except websockets.ConnectionClosed:
            print("[disconnect] shutting down...", flush=True)
        except Exception as e:
            print(f"[error] {e}", file=sys.stderr, flush=True)
        finally:
            await flush_and_checkpoint(force=True)

    await asyncio.gather(writer_loop())

def main():
    p = argparse.ArgumentParser(description="Jetstream → hourly NDJSON (partitioned by time_us)")
    p.add_argument("--outdir", default=".", help="Output directory (default: current dir)")
    p.add_argument("--url", default="wss://jetstream1.us-west.bsky.network/subscribe", help="Jetstream WS URL")
    p.add_argument("--flush-count", type=int, default=200, help="Flush after N events (default 200)")
    p.add_argument("--flush-seconds", type=float, default=3.0, help="Flush every N seconds (default 3.0)")
    args = p.parse_args()
    try:
        asyncio.run(run(args.outdir, args.url, args.flush_count, args.flush_seconds))
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
