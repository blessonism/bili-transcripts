#!/usr/bin/env python3
"""PandaWiki 导入 — 批量导入 B站文稿。

读取 classification.json + doc_suitability.json + transcripts_polished/ + transcripts/ + transcripts_asr/。
优先级: polished > subtitle > asr。
跳过 suitable=false 的视频。
"""

import json
import os
import sys
import time
import urllib.request
import urllib.error
import ssl
import datetime

BASE_URL = "https://127.0.0.1:2443"
ADMIN_ACCOUNT = "admin"
ADMIN_PASSWORD = "PandaWiki2026Admin"

PROJECT_DIR = "/root/projects/bili-transcripts"
CLASSIFICATION_FILE = f"{PROJECT_DIR}/data/classified/classification.json"
SUITABILITY_FILE = f"{PROJECT_DIR}/data/classified/doc_suitability.json"
TRANSCRIPTS_POLISHED_DIR = f"{PROJECT_DIR}/data/transcripts_polished"
TRANSCRIPTS_DIR = f"{PROJECT_DIR}/data/transcripts"
TRANSCRIPTS_ASR_DIR = f"{PROJECT_DIR}/data/transcripts_asr"
PROGRESS_FILE = f"{PROJECT_DIR}/data/pandawiki_import_progress.json"

CATEGORY_EMOJI = {
    "人情世故": "🎭",
    "职业发展": "💼",
    "认知成长": "🧠",
    "技术工具": "💻",
    "学业考试": "📚",
    "影视娱乐": "🎬",
    "生活方式": "🍳",
    "深度内容": "🌍",
}

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE


def api_request(path, method="GET", data=None, token=None):
    url = f"{BASE_URL}{path}"
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    body = json.dumps(data).encode() if data else None
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        resp = urllib.request.urlopen(req, context=ctx, timeout=30)
        return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        err_body = e.read().decode() if e.fp else ""
        print(f"  HTTP {e.code}: {err_body[:200]}")
        return None
    except Exception as e:
        print(f"  Request error: {e}")
        return None


def login():
    resp = api_request("/api/v1/user/login", "POST", {
        "account": ADMIN_ACCOUNT,
        "password": ADMIN_PASSWORD,
    })
    if resp and resp.get("success"):
        return resp["data"]["token"]
    print(f"Login failed: {resp}")
    sys.exit(1)


def get_kb_id(token):
    resp = api_request("/api/v1/knowledge_base/list", token=token)
    if resp and resp.get("success") and resp["data"]:
        return resp["data"][0]["id"]
    print(f"No knowledge base found: {resp}")
    sys.exit(1)


def create_folder(token, kb_id, name, parent_id="", emoji=""):
    resp = api_request("/api/v1/node", "POST", {
        "kb_id": kb_id,
        "type": 1,
        "name": name,
        "parent_id": parent_id,
        "emoji": emoji,
    }, token=token)
    if resp and resp.get("success"):
        return resp["data"]["id"]
    print(f"  Failed to create folder '{name}': {resp}")
    return None


def create_document(token, kb_id, name, content, parent_id="", emoji="📄"):
    resp = api_request("/api/v1/node", "POST", {
        "kb_id": kb_id,
        "type": 2,
        "name": name,
        "content": content,
        "parent_id": parent_id,
        "emoji": emoji,
    }, token=token)
    if resp and resp.get("success"):
        return resp["data"]["id"]
    print(f"  Failed to create doc '{name[:30]}': {resp}")
    return None


def read_transcript(bvid):
    """Read transcript with priority: polished > subtitle > asr."""
    for d in [TRANSCRIPTS_POLISHED_DIR, TRANSCRIPTS_DIR, TRANSCRIPTS_ASR_DIR]:
        path = os.path.join(d, f"{bvid}.txt")
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                content = f.read().strip()
            if content:
                source = "polished" if d == TRANSCRIPTS_POLISHED_DIR else ("subtitle" if d == TRANSCRIPTS_DIR else "asr")
                return content, source
    return "", "none"


def format_duration(seconds):
    h = seconds // 3600
    m = (seconds % 3600) // 60
    if h > 0:
        return f"{h}小时{m}分钟"
    return f"{m}分钟"


def format_document_content(video, transcript, source):
    c = video["classification"]
    dur = format_duration(video["duration"])
    pubdate = video.get("pubdate", "")
    if isinstance(pubdate, (int, float)) and pubdate > 0:
        pubdate = datetime.datetime.fromtimestamp(pubdate).strftime("%Y-%m-%d")

    tags = ", ".join(c.get("tags", []))
    source_label = {
        "polished": "润色文稿",
        "subtitle": "AI字幕（原始）",
        "asr": "ASR转写（原始）",
    }.get(source, source)

    md = f"""> {c['summary']}

| 属性 | 值 |
|------|-----|
| UP主 | {video['upper']} |
| 时长 | {dur} |
| 发布日期 | {pubdate} |
| 分类 | {c['primary_category']} / {c['sub_category']} |
| 标签 | {tags} |
| 文稿来源 | {source_label} |
| 链接 | [{video['bvid']}](https://www.bilibili.com/video/{video['bvid']}) |

## 完整文稿

{transcript}
"""
    return md


def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {"imported_bvids": [], "folder_ids": {}}


def save_progress(progress):
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)


def main():
    print("=== PandaWiki B站文稿导入 ===")

    with open(CLASSIFICATION_FILE, encoding="utf-8") as f:
        data = json.load(f)
    videos = data["videos"]
    print(f"Loaded {len(videos)} classified videos")

    # Load suitability filter
    suitability = {}
    if os.path.exists(SUITABILITY_FILE):
        with open(SUITABILITY_FILE, encoding="utf-8") as f:
            suit_data = json.load(f)
            suitability = suit_data.get("results", {})

    # Filter suitable videos with transcripts
    videos_to_import = []
    for v in videos:
        if suitability and not suitability.get(v["bvid"], {}).get("suitable", True):
            continue
        transcript, source = read_transcript(v["bvid"])
        if transcript:
            videos_to_import.append((v, transcript, source))
    print(f"Videos to import: {len(videos_to_import)}")

    progress = load_progress()
    imported = set(progress["imported_bvids"])
    folder_ids = progress["folder_ids"]
    print(f"Already imported: {len(imported)}")

    token = login()
    print("Logged in")

    kb_id = get_kb_id(token)
    print(f"KB ID: {kb_id}")

    from collections import defaultdict
    by_category = defaultdict(lambda: defaultdict(list))
    for v, transcript, source in videos_to_import:
        c = v["classification"]
        by_category[c["primary_category"]][c["sub_category"]].append((v, transcript, source))

    total = len(videos_to_import) - len(imported)
    done = 0
    errors = 0

    for primary, subs in sorted(by_category.items()):
        emoji = CATEGORY_EMOJI.get(primary, "📄")
        primary_key = f"primary:{primary}"

        if primary_key not in folder_ids:
            fid = create_folder(token, kb_id, primary, emoji=emoji)
            if not fid:
                errors += 1
                continue
            folder_ids[primary_key] = fid
            save_progress(progress)
            time.sleep(0.1)

        primary_fid = folder_ids[primary_key]

        for sub, items in sorted(subs.items()):
            sub_key = f"sub:{primary}/{sub}"

            if sub_key not in folder_ids:
                fid = create_folder(token, kb_id, sub, parent_id=primary_fid)
                if not fid:
                    errors += 1
                    continue
                folder_ids[sub_key] = fid
                save_progress(progress)
                time.sleep(0.1)

            sub_fid = folder_ids[sub_key]

            for v, transcript, source in items:
                if v["bvid"] in imported:
                    continue

                content = format_document_content(v, transcript, source)
                doc_id = create_document(
                    token, kb_id,
                    v["title"],
                    content,
                    parent_id=sub_fid,
                    emoji="🎬",
                )

                if doc_id:
                    imported.add(v["bvid"])
                    progress["imported_bvids"] = list(imported)
                    done += 1
                    if done % 10 == 0:
                        save_progress(progress)
                        print(f"  [{done}/{total}] imported")
                else:
                    errors += 1

                time.sleep(0.2)

    save_progress(progress)
    print(f"\n=== Done ===")
    print(f"Imported: {done}")
    print(f"Errors: {errors}")
    print(f"Total in KB: {len(imported)}")


if __name__ == "__main__":
    main()
