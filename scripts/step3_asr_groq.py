#!/usr/bin/env python3
"""Step 3: ASR via Groq Whisper — Multi-key rotation

Reads api_keys from config/credentials.json (groq.api_keys array).
Per-key hourly + daily limits. Rotates on either limit.
Checkpoint/resume: safe to kill and restart.
"""
import json, os, time, subprocess
from datetime import datetime
from pathlib import Path
from groq import Groq
import httpx

# ── Config ──
PROJECT = Path("/root/projects/bili-transcripts")
CREDS_PATH = PROJECT / "config/credentials.json"
PROXY_URL = "http://127.0.0.1:7890"

with open(CREDS_PATH) as f:
    creds = json.load(f)

API_KEYS = creds.get("groq", {}).get("api_keys", [])
if not API_KEYS:
    single = os.environ.get("GROQ_API_KEY", "").strip() or creds.get("groq", {}).get("api_key", "")
    if not single:
        raise SystemExit("No Groq API keys found")
    API_KEYS = [single]

INPUT_JSON = PROJECT / "data/raw/asr_queue.json"
COOKIES_PATH = PROJECT / "config/cookies.txt"
TRANSCRIPTS_DIR = PROJECT / "data/transcripts_asr"
STATUS_PATH = PROJECT / "data/raw/asr_status.json"
LOG_DIR = PROJECT / "logs"
TMP_DIR = Path("/tmp/bili-asr")

MAX_DURATION_SEC = 1800
DELAY_BETWEEN = 4.0
HOURLY_LIMIT_PER_KEY = 6000   # ~100min/hr per key (ASH)
DAILY_LIMIT_PER_KEY = 25000   # ~7h/day per key (ASD)
MAX_FILE_SIZE = 24 * 1024 * 1024
DOWNLOAD_TIMEOUT = 120

for d in [TRANSCRIPTS_DIR, LOG_DIR, TMP_DIR]:
    d.mkdir(parents=True, exist_ok=True)

LOG_PATH = LOG_DIR / f"asr_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

def log(msg):
    line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    print(line, flush=True)
    with open(LOG_PATH, "a") as f:
        f.write(line + "\n")

def load_status():
    if STATUS_PATH.exists():
        with open(STATUS_PATH) as f:
            st = json.load(f)
        if "key_usage" not in st:
            st["key_usage"] = {}
        return st
    return {
        "processed": {},
        "key_usage": {},
        "stats": {"ok": 0, "no_speech": 0, "error_download": 0, "error_transcribe": 0},
    }

def save_status(st):
    tmp = STATUS_PATH.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(st, f, ensure_ascii=False, indent=2)
    tmp.replace(STATUS_PATH)

def key_hash(key):
    return key[-8:]

def get_key_usage(st, kh):
    if kh not in st["key_usage"]:
        st["key_usage"][kh] = {
            "hourly_audio": 0, "hour_start": time.time(),
            "daily_audio": 0, "day_start": time.time(),
        }
    ku = st["key_usage"][kh]
    # Reset counters if period elapsed
    now = time.time()
    if now - ku.get("hour_start", 0) >= 3600:
        ku["hourly_audio"] = 0
        ku["hour_start"] = now
    if now - ku.get("day_start", 0) >= 86400:
        ku["daily_audio"] = 0
        ku["day_start"] = now
    return ku

def key_has_quota(st, kh, dur):
    """Check if key has both hourly and daily quota for given duration."""
    ku = get_key_usage(st, kh)
    return (ku["hourly_audio"] + dur <= HOURLY_LIMIT_PER_KEY and
            ku["daily_audio"] + dur <= DAILY_LIMIT_PER_KEY)

def find_available_key(st, start_idx, dur):
    """Find next key with available quota. Returns (index, key_hash) or (None, None)."""
    for i in range(len(API_KEYS)):
        idx = (start_idx + i) % len(API_KEYS)
        kh = key_hash(API_KEYS[idx])
        if key_has_quota(st, kh, dur):
            return idx, kh
    return None, None

def download_audio(bvid, out):
    out.unlink(missing_ok=True)
    try:
        r = subprocess.run([
            "yt-dlp", "-f", "ba[ext=m4a]/ba", "--no-video", "--no-playlist",
            "--cookies", str(COOKIES_PATH), "--socket-timeout", "30",
            "--retries", "2", "--limit-rate", "2M", "-o", str(out), "-q",
            f"https://www.bilibili.com/video/{bvid}"
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
           timeout=DOWNLOAD_TIMEOUT)
    except (subprocess.TimeoutExpired, Exception):
        out.unlink(missing_ok=True)
        return False
    if r.returncode != 0 or not out.exists():
        out.unlink(missing_ok=True)
        return False
    if out.stat().st_size > MAX_FILE_SIZE:
        out.unlink(missing_ok=True)
        return False
    return True

def transcribe(client, path, bvid):
    with open(path, "rb") as f:
        data = f.read()
    t = client.audio.transcriptions.create(
        file=(f"{bvid}.m4a", data),
        model="whisper-large-v3-turbo",
        language="zh", temperature=0,
        response_format="verbose_json",
    )
    return (t.text or "").strip(), float(getattr(t, "duration", 0) or 0)

def main():
    with open(INPUT_JSON) as f:
        videos = json.load(f).get("videos", [])
    videos = [v for v in videos if int(v.get("duration", 0) or 0) <= MAX_DURATION_SEC]

    st = load_status()
    done = sum(1 for s in st["processed"].values() if s in ("ok", "no_speech"))
    log("=" * 60)
    log(f"Step 3 ASR | total={len(videos)} | done={done} | remaining={len(videos)-done}")
    log(f"Available keys: {len(API_KEYS)}")

    # Find first available key
    current_key_idx, kh = find_available_key(st, 0, 0)
    if current_key_idx is None:
        log("All keys exhausted")
        save_status(st)
        return

    client = Groq(api_key=API_KEYS[current_key_idx], http_client=httpx.Client(proxy=PROXY_URL))
    kh = key_hash(API_KEYS[current_key_idx])
    ku = get_key_usage(st, kh)
    log(f"Starting with key ..{kh} (hourly={ku['hourly_audio']/3600:.2f}h daily={ku['daily_audio']/3600:.2f}h)")
    run_ok = 0
    consecutive_dl_fails = 0

    for idx, v in enumerate(videos, 1):
        bvid = v.get("bvid")
        if not bvid or st["processed"].get(bvid) in ("ok", "no_speech"):
            continue

        dur = int(v.get("duration", 0) or 0)

        # Check quota, rotate if needed
        if not key_has_quota(st, kh, dur):
            reason = "hourly" if get_key_usage(st, kh)["hourly_audio"] + dur > HOURLY_LIMIT_PER_KEY else "daily"
            log(f"Key ..{kh} {reason} limit reached. Rotating...")
            save_status(st)
            new_idx, new_kh = find_available_key(st, current_key_idx + 1, dur)
            if new_idx is not None:
                current_key_idx = new_idx
                kh = new_kh
                client = Groq(api_key=API_KEYS[current_key_idx], http_client=httpx.Client(proxy=PROXY_URL))
                ku = get_key_usage(st, kh)
                log(f"Rotated to key ..{kh} (hourly={ku['hourly_audio']/3600:.2f}h daily={ku['daily_audio']/3600:.2f}h)")
            else:
                log("All keys exhausted")
                break

        audio = TMP_DIR / f"{bvid}.m4a"
        if not download_audio(bvid, audio):
            st["processed"][bvid] = "error_download"
            st["stats"]["error_download"] += 1
            consecutive_dl_fails += 1
            save_status(st)
            log(f"[{idx}/{len(videos)}] {bvid} download failed (streak: {consecutive_dl_fails})")
            if consecutive_dl_fails >= 10:
                log("10 consecutive download failures — stopping")
                break
            time.sleep(1)
            continue

        consecutive_dl_fails = 0

        try:
            text, actual = transcribe(client, audio, bvid)
            ku = get_key_usage(st, kh)
            if text:
                (TRANSCRIPTS_DIR / f"{bvid}.txt").write_text(text, encoding="utf-8")
                st["processed"][bvid] = "ok"
                st["stats"]["ok"] += 1
                run_ok += 1
                log(f"[{idx}/{len(videos)}] {bvid} ok | {actual:.1f}s | key ..{kh}")
            else:
                st["processed"][bvid] = "no_speech"
                st["stats"]["no_speech"] += 1
                log(f"[{idx}/{len(videos)}] {bvid} no_speech")
            ku["hourly_audio"] += actual
            ku["daily_audio"] += actual
        except Exception as e:
            err_msg = str(e)[:200]
            if "rate_limit" in err_msg.lower() or "429" in err_msg:
                log(f"[{idx}/{len(videos)}] {bvid} rate limited on ..{kh}, rotating...")
                new_idx, new_kh = find_available_key(st, current_key_idx + 1, dur)
                if new_idx is not None:
                    current_key_idx = new_idx
                    kh = new_kh
                    client = Groq(api_key=API_KEYS[new_idx], http_client=httpx.Client(proxy=PROXY_URL))
                    log(f"Rotated to key ..{kh}")
                    continue
                else:
                    log("All keys exhausted")
                    break
            st["processed"][bvid] = "error_transcribe"
            st["stats"]["error_transcribe"] += 1
            log(f"[{idx}/{len(videos)}] {bvid} ERR: {type(e).__name__}: {err_msg}")
        finally:
            audio.unlink(missing_ok=True)

        if run_ok % 20 == 0 and run_ok > 0:
            save_status(st)
            ku = get_key_usage(st, kh)
            log(f"Checkpoint: key ..{kh} hourly={ku['hourly_audio']/3600:.2f}h daily={ku['daily_audio']/3600:.2f}h | files={len(list(TRANSCRIPTS_DIR.glob('*.txt')))}")
        time.sleep(DELAY_BETWEEN)

    save_status(st)
    total_files = len(list(TRANSCRIPTS_DIR.glob("*.txt")))
    log("=" * 60)
    log(f"Run done | this_run={run_ok} | stats={st['stats']}")
    for i, k in enumerate(API_KEYS):
        kh_i = key_hash(k)
        ku_i = get_key_usage(st, kh_i)
        log(f"  Key ..{kh_i}: hourly={ku_i['hourly_audio']/3600:.2f}h daily={ku_i['daily_audio']/3600:.2f}h")
    log(f"total_files={total_files}")

if __name__ == "__main__":
    main()
