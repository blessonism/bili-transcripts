#!/usr/bin/env python3
"""Step 4.5: 文档适用性筛选 — 判断视频是否适合做成文档。

模型: Gemini Flash
输入: classification.json + transcripts + transcripts_asr
输出: data/classified/doc_suitability.json
"""

import json
import os
import sys
import time
import urllib.request
import urllib.error

BASE_DIR = "/root/projects/bili-transcripts"
CLASSIFICATION_FILE = f"{BASE_DIR}/data/classified/classification.json"
TRANSCRIPTS_DIR = f"{BASE_DIR}/data/transcripts"
TRANSCRIPTS_ASR_DIR = f"{BASE_DIR}/data/transcripts_asr"
OUTPUT_FILE = f"{BASE_DIR}/data/classified/doc_suitability.json"
PROGRESS_FILE = f"{BASE_DIR}/data/classified/.progress_suitability.json"

API_URL = "https://sukisq.zeabur.app/v1/chat/completions"
API_KEY = "sk-P0WtEZXyhmrgvMgL0Q4W8Detae9uWvPhbVTPC6NGdIeTslFi"
MODEL = "gemini-3-flash-preview"

SYSTEM_PROMPT = """你是一个内容筛选专家。判断一个视频的字幕/文稿是否适合转化为可读文档。

不适合做文档的情况：
1. 纯娱乐/搞笑/整活类 — 内容主要是搞笑、整蛅、恶搞，没有信息价值
2. 视听依赖型 — 旅拍、美食探店、舞蹈、音乐表演、ASMR、视觉特效展示，核心价值在视觉/听觉而非文字
3. 广告/带货/课程推销 — 主要目的是卖东西，信息密度低
4. 信息密度过低 — 大量语气词、重复、无实质内容，转成文字后没有可读性
5. 纯影视解说 — 电影/剧集剧情复述，离开画面无法理解

适合做文档的情况：
- 知识/教程/方法论类
- 经验分享/复盘类
- 深度分析/行业洞察类
- 访谈/对话类（有实质内容）
- 技术教程/工具介绍类
- 思维方法/心理分析类

请严格按以下 JSON 格式输出，不要输出其他内容：
{"suitable": true, "reason": "一句话说明原因"}

规则：
1. 偏向保留 — 如果拿不准，判 true
2. 混合型内容（如 vlog 夹干货）判 true
3. 只看文稿内容本身，不要因为分类是“影视娱乐”就直接判 false"""


def read_transcript(bvid):
    for d in [TRANSCRIPTS_DIR, TRANSCRIPTS_ASR_DIR]:
        path = os.path.join(d, f"{bvid}.txt")
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                return f.read()
    return ""


def api_call(messages, retries=3):
    payload = json.dumps({
        "model": MODEL,
        "messages": messages,
        "temperature": 0.1,
        "max_tokens": 200,
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
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            content = data["choices"][0]["message"]["content"].strip()
            if content.startswith("```"):
                content = content.split("\n", 1)[1]
                content = content.rsplit("```", 1)[0]
            return json.loads(content)
        except Exception as e:
            print(f"  attempt {attempt+1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(2 * (attempt + 1))
    return None


def main():
    with open(CLASSIFICATION_FILE, encoding="utf-8") as f:
        data = json.load(f)
    videos = data["videos"]
    print(f"Total classified videos: {len(videos)}")

    # Load progress
    progress = {}
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, encoding="utf-8") as f:
            progress = json.load(f)

    remaining = [v for v in videos if v["bvid"] not in progress]
    done = len(progress)
    total = len(videos)
    print(f"Done: {done}, Remaining: {len(remaining)}")

    if not remaining:
        print("All filtered!")
        write_output(videos, progress)
        return

    errors = 0
    for i, video in enumerate(remaining):
        bvid = video["bvid"]
        transcript = read_transcript(bvid)[:800]
        if not transcript:
            progress[bvid] = {"suitable": False, "reason": "无文稿"}
            print(f"[{done+i+1}/{total}] {bvid} — no transcript, skip")
            continue

        c = video["classification"]
        user_msg = f"视频信息：\n标题：{video['title']}\n分类：{c['primary_category']}/{c['sub_category']}\n摘要：{c['summary']}\n\n文稿前800字：\n{transcript}"

        print(f"[{done+i+1}/{total}] {bvid} — {video['title'][:40]}...", end=" ", flush=True)

        result = api_call([
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_msg},
        ])

        if result:
            progress[bvid] = result
            mark = "✅" if result.get("suitable") else "❌"
            print(f"→ {mark} {result.get('reason', '')[:40]}")
        else:
            # Default to suitable on failure
            progress[bvid] = {"suitable": True, "reason": "API失败，默认保留"}
            errors += 1
            print("→ FAILED (default: keep)")

        if (i + 1) % 20 == 0:
            with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
                json.dump(progress, f, ensure_ascii=False, indent=2)
            print(f"  [saved {done+i+1}/{total}]")

        time.sleep(0.3)

    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)

    write_output(videos, progress)

    suitable = sum(1 for v in progress.values() if v.get("suitable"))
    print(f"\nDone! Suitable: {suitable}/{total}, Errors: {errors}")


def write_output(videos, progress):
    results = {}
    for v in videos:
        bvid = v["bvid"]
        if bvid in progress:
            results[bvid] = progress[bvid]

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump({
            "meta": {
                "total": len(results),
                "suitable": sum(1 for v in results.values() if v.get("suitable")),
                "filtered_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "model": MODEL,
            },
            "results": results,
        }, f, ensure_ascii=False, indent=2)
    print(f"Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
