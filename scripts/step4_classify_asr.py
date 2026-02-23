#!/usr/bin/env python3
"""Step 4 补跑: 对 ASR 转写的视频做分类。

复用 step4 的分类逻辑，输入改为 transcripts_asr 目录。
结果合并到 classification.json。
"""

import json
import os
import sys
import time
import urllib.request
import urllib.error

BASE_DIR = "/root/projects/bili-transcripts"
VIDEOS_JSON = f"{BASE_DIR}/data/raw/videos.json"
TRANSCRIPTS_ASR_DIR = f"{BASE_DIR}/data/transcripts_asr"
OUTPUT_DIR = f"{BASE_DIR}/data/classified"
OUTPUT_FILE = f"{OUTPUT_DIR}/classification.json"
PROGRESS_FILE = f"{OUTPUT_DIR}/.progress_asr.json"

import json as _json_creds
with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../config/credentials.json")) as _f:
    _creds = _json_creds.load(_f)
API_URL = _creds.get("classify", {}).get("api_url", "https://api.openai.com/v1/chat/completions")
API_KEY = _creds.get("classify", {}).get("api_key", "")
MODEL = "gemini-3-flash-preview"

CATEGORY_SCHEMA = """分类体系（主分类 → 子分类）：

🎭 人情世故
  - 社交沟通（夸人、接话、聊天技巧、small talk、道歉）
  - 饭局酒局（敬酒、饭局聊天、送礼、宴请）
  - 职场人际（边界感、麻烦别人、领导相处）
  - 恋爱关系（暧昧边界、恋爱模式、分手、约会）

💼 职业发展
  - 求职面试（简历、面试技巧、找工作）
  - 创业商业（商业思维、创业复盘、变现、外贸）
  - 职场成长（初入职场、跳槽、职业规划）

🧠 认知成长
  - 思维方法（学习方法、思维模型、计划制定）
  - 心理自我（内耗、自我认知、情绪管理）
  - 哲学思辨（哲学、社会批判、深度思考）

💻 技术工具
  - 编程开发（编程语言、框架、系统、算法）
  - AI 应用（AI编程、AI视频、AI工具、LLM）
  - 效率工具（笔记软件、Mac配置、浏览器、窗口管理）
  - 设计创作（PS/PPT/PR/AE/UI设计、视频剪辑）

📚 学业考试
  - 公考（申论、行测、面试）
  - 学术科研（论文写作、数学建模、LaTeX）
  - 大学课程（数学、计算机、英语）

🎬 影视娱乐
  - 影视解说（电影/剧集解说、动画）
  - 综艺脱口秀（小品、脱口秀、综艺）
  - 音乐ASMR（音乐、ASMR、治愈）

🍳 生活方式
  - 美食烹饪（菜谱、美食探店）
  - 数码好物（数码产品、好物推荐）
  - 生活技巧（装修、收纳、日常技巧）
  - 摄影拍照（拍照技巧、摄影教程）

🌍 深度内容
  - 人物访谈（播客、深度对话、人物故事）
  - 行业洞察（行业分析、商业案例、社会观察）
  - 投资理财（投资、理财、财务自由）"""

SYSTEM_PROMPT = f"""你是一个视频内容分类专家。根据视频的标题、简介和文稿内容，对视频进行分类。

{CATEGORY_SCHEMA}

请严格按以下 JSON 格式输出，不要输出其他内容：
{{"primary_category": "主分类名（如：人情世故）", "sub_category": "子分类名（如：社交沟通）", "confidence": 0.95, "summary": "一句话概括视频核心内容（20-40字）", "tags": ["标签1", "标签2", "标签3"]}}

规则：
1. confidence 范围 0.0-1.0，表示分类置信度
2. 如果视频明显跨分类，选最主要的，confidence 适当降低
3. tags 给 2-5 个关键词标签
4. summary 基于文稿内容写，不要只看标题猜"""


def api_call(messages, retries=3):
    payload = json.dumps({
        "model": MODEL,
        "messages": messages,
        "temperature": 0.1,
        "max_tokens": 500,
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
    with open(VIDEOS_JSON) as f:
        all_videos = {v["bvid"]: v for v in json.load(f)["videos"]}

    # Load existing classification
    existing_classified = {}
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE) as f:
            data = json.load(f)
            existing_classified = {v["bvid"]: v for v in data["videos"]}
    print(f"Existing classified: {len(existing_classified)}")

    # Find ASR transcripts not yet classified
    asr_bvids = sorted(
        f.replace(".txt", "")
        for f in os.listdir(TRANSCRIPTS_ASR_DIR)
        if f.endswith(".txt")
    )
    already_classified = {v["bvid"] for v in existing_classified.values()}

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    progress = {}
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            progress = json.load(f)

    remaining = [b for b in asr_bvids if b not in already_classified and b not in progress]
    total = len(asr_bvids)
    done = len(progress)
    print(f"ASR total: {total}, Already done: {len(already_classified & set(asr_bvids))}, Progress: {done}, Remaining: {len(remaining)}")

    if not remaining:
        print("All ASR classified!")
        merge_output(all_videos, existing_classified, progress)
        return

    errors = 0
    for i, bvid in enumerate(remaining):
        video = all_videos.get(bvid)
        if not video:
            print(f"[{done+i+1}/{total}] {bvid} — no metadata, skip")
            continue

        tpath = os.path.join(TRANSCRIPTS_ASR_DIR, f"{bvid}.txt")
        with open(tpath, encoding="utf-8") as f:
            transcript = f.read()[:1500]

        upper = video["upper"]["name"] if isinstance(video["upper"], dict) else video["upper"]
        desc = (video.get("desc", "") or "")[:200]
        dur_min = video["duration"] / 60

        user_msg = f"视频信息：\n标题：{video['title']}\nUP主：{upper}\n时长：{dur_min:.0f}分钟\n简介：{desc}\n\n文稿前1500字：\n{transcript}"

        print(f"[{done+i+1}/{total}] {bvid} — {video['title'][:40]}...", end=" ", flush=True)

        result = api_call([
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_msg},
        ])

        if result:
            progress[bvid] = result
            print(f"→ {result['primary_category']}/{result['sub_category']} ({result['confidence']})")
        else:
            progress[bvid] = {"error": "failed"}
            errors += 1
            print("→ FAILED")

        if (i + 1) % 10 == 0:
            with open(PROGRESS_FILE, "w") as f:
                json.dump(progress, f, ensure_ascii=False, indent=2)
            print(f"  [saved {done+i+1}/{total}]")

        time.sleep(0.5)

    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)

    merge_output(all_videos, existing_classified, progress)
    print(f"\nDone! Errors: {errors}")


def merge_output(all_videos, existing_classified, new_progress):
    """Merge existing classification with new ASR classifications."""
    results = list(existing_classified.values())

    for bvid, cls in new_progress.items():
        if "error" in cls:
            continue
        v = all_videos.get(bvid, {})
        upper = v.get("upper", {})
        results.append({
            "bvid": bvid,
            "title": v.get("title", ""),
            "upper": upper.get("name", "") if isinstance(upper, dict) else upper,
            "duration": v.get("duration", 0),
            "link": f"https://www.bilibili.com/video/{bvid}",
            "cover": v.get("cover", ""),
            "pubdate": v.get("pubdate", ""),
            "classification": cls,
            "source": "asr",
        })

    results.sort(key=lambda x: (x["classification"]["primary_category"], x["classification"]["sub_category"]))

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump({
            "meta": {
                "total": len(results),
                "classified_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "model": MODEL,
                "note": "merged: subtitle + asr"
            },
            "videos": results
        }, f, ensure_ascii=False, indent=2)
    print(f"Output: {OUTPUT_FILE} ({len(results)} videos)")


if __name__ == "__main__":
    main()
