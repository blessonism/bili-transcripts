#!/usr/bin/env python3
"""Step 4.7: 文稿润色 — 将字幕/ASR 文稿转为可读文档。

模型: Gemini 3.1 Pro High
仅处理 doc_suitability.json 中 suitable=true 的视频。
输出: data/transcripts_polished/{bvid}.txt
"""

import json
import os
import time
import urllib.request
import urllib.error

BASE_DIR = "/root/projects/bili-transcripts"
CLASSIFICATION_FILE = f"{BASE_DIR}/data/classified/classification.json"
SUITABILITY_FILE = f"{BASE_DIR}/data/classified/doc_suitability.json"
TRANSCRIPTS_DIR = f"{BASE_DIR}/data/transcripts"
TRANSCRIPTS_ASR_DIR = f"{BASE_DIR}/data/transcripts_asr"
OUTPUT_DIR = f"{BASE_DIR}/data/transcripts_polished"
PROGRESS_FILE = f"{BASE_DIR}/data/classified/.progress_polish.json"

API_URL = "https://sukisq.zeabur.app/v1/chat/completions"
API_KEY = "sk-P0WtEZXyhmrgvMgL0Q4W8Detae9uWvPhbVTPC6NGdIeTslFi"
MODEL = "gemini-3.1-pro-high"

SYSTEM_PROMPT = """你是一个字幕润色工具。将视频字幕/ASR转写文本转为可读文档。

润色规则（最小干预原则）：
1. 删除语气词和填充词（嗯、啊、然后、就是说、对吧、就是、那个、这个、就是说、你知道吗、对不对等）
2. 合并自我纠正（"不对，应该是 X" → 直接保留 X）
3. 修正明显的 ASR 同音字错误（根据上下文判断）
4. 按话题/逻辑转折分段，每段加一个小标题（从内容提炼，不自编）
5. 规范标点（补全句号、逗号，修正断句）
6. 保留说话人风格 — 不把口语改成书面语，"这玩意儿"不改成"这个东西"
7. 不改变原意、不补充内容、不添加你的观点
8. 如果是对话/访谈格式，保留说话人区分（如果能从上下文判断）

输出格式：
- 直接输出润色后的文本，不要加任何前缀/后缀/解释
- 使用 Markdown 格式（## 小标题）
- 每段之间空一行"""

# 无效文稿标记
INVALID_MARKERS = ["[无字幕]", "[无内容]", "[empty]"]


def read_transcript(bvid):
    for d in [TRANSCRIPTS_DIR, TRANSCRIPTS_ASR_DIR]:
        path = os.path.join(d, f"{bvid}.txt")
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                content = f.read().strip()
            # 过滤无效文稿
            if any(marker in content for marker in INVALID_MARKERS):
                return ""
            if len(content) < 50:
                return ""
            return content
    return ""


def api_call(messages, retries=3):
    payload = json.dumps({
        "model": MODEL,
        "messages": messages,
        "temperature": 0.2,
        "max_tokens": 16000,
    }).encode("utf-8")

    req = urllib.request.Request(
        API_URL,
        data=payload,
        headers={
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json",
        },
    )

    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=300) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            return data["choices"][0]["message"]["content"].strip()
        except Exception as e:
            print(f"  attempt {attempt+1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(5 * (attempt + 1))
    return None


def polish_transcript(title, transcript, classification):
    """Polish a transcript in a single API call."""
    summary = classification.get("summary", "")
    category = f"{classification.get('primary_category', '')}/{classification.get('sub_category', '')}"

    user_msg = f"视频标题：{title}\n分类：{category}\n摘要：{summary}\n\n原始文稿：\n{transcript}"
    return api_call([
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_msg},
    ])


def main():
    with open(CLASSIFICATION_FILE, encoding="utf-8") as f:
        cls_data = json.load(f)
    video_map = {v["bvid"]: v for v in cls_data["videos"]}

    with open(SUITABILITY_FILE, encoding="utf-8") as f:
        suit_data = json.load(f)
    suitability = suit_data["results"]

    suitable_bvids = [bvid for bvid, s in suitability.items() if s.get("suitable")]
    print(f"Suitable videos: {len(suitable_bvids)}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    progress = {}
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, encoding="utf-8") as f:
            progress = json.load(f)

    already_done = set(progress.keys())
    for f_name in os.listdir(OUTPUT_DIR):
        if f_name.endswith(".txt"):
            already_done.add(f_name.replace(".txt", ""))

    remaining = [b for b in suitable_bvids if b not in already_done]
    total = len(suitable_bvids)
    done = len(already_done & set(suitable_bvids))
    print(f"Total: {total}, Done: {done}, Remaining: {len(remaining)}")

    if not remaining:
        print("All polished!")
        return

    errors = 0
    for i, bvid in enumerate(remaining):
        video = video_map.get(bvid, {})
        title = video.get("title", bvid)
        classification = video.get("classification", {})

        transcript = read_transcript(bvid)
        if not transcript:
            print(f"[{done+i+1}/{total}] {bvid} — no transcript or invalid, skip")
            progress[bvid] = "no_transcript"
            continue

        char_count = len(transcript)
        print(f"[{done+i+1}/{total}] {bvid} — {title[:40]}... ({char_count}字)", end=" ", flush=True)

        result = polish_transcript(title, transcript, classification)

        if result:
            out_path = os.path.join(OUTPUT_DIR, f"{bvid}.txt")
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(result)
            progress[bvid] = "ok"
            print(f"→ ✅ ({len(result)}字)")
        else:
            progress[bvid] = "error"
            errors += 1
            print("→ ❌")

        if (i + 1) % 5 == 0:
            with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
                json.dump(progress, f, ensure_ascii=False, indent=2)
            print(f"  [saved]")

        time.sleep(2)

    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)

    ok = sum(1 for v in progress.values() if v == "ok")
    print(f"\nDone! Polished: {ok}, Errors: {errors}")


if __name__ == "__main__":
    main()
