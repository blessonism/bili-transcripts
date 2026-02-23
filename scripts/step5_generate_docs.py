#!/usr/bin/env python3
"""Step 5: 文档库生成 — 按分类输出结构化 Markdown 文档库。

输入: data/classified/classification.json + doc_suitability.json + transcripts_polished/ + transcripts/ + transcripts_asr/
输出: docs/ 目录

优先级: transcripts_polished > transcripts > transcripts_asr
跳过 doc_suitable=false 的视频。
"""

import json
import os
import re
import time
from collections import defaultdict

BASE_DIR = "/root/projects/bili-transcripts"
CLASSIFICATION_FILE = f"{BASE_DIR}/data/classified/classification.json"
SUITABILITY_FILE = f"{BASE_DIR}/data/classified/doc_suitability.json"
TRANSCRIPTS_POLISHED_DIR = f"{BASE_DIR}/data/transcripts_polished"
TRANSCRIPTS_DIR = f"{BASE_DIR}/data/transcripts"
TRANSCRIPTS_ASR_DIR = f"{BASE_DIR}/data/transcripts_asr"
DOCS_DIR = f"{BASE_DIR}/docs"



def sanitize_filename(name, max_len=60):
    name = re.sub(r'[\\/:*?"<>|]', '', name)
    name = name.strip()
    if len(name) > max_len:
        name = name[:max_len]
    return name


def sanitize_dirname(name):
    name = re.sub(r'[\\/:*?"<>|]', '', name)
    return name.strip()


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


def generate_video_md(video, transcript, source):
    c = video["classification"]
    dur = format_duration(video["duration"])
    pubdate = video.get("pubdate", "")
    if isinstance(pubdate, (int, float)) and pubdate > 0:
        import datetime
        pubdate = datetime.datetime.fromtimestamp(pubdate).strftime("%Y-%m-%d")

    source_label = {
        "polished": "润色文稿",
        "subtitle": "AI字幕（原始）",
        "asr": "ASR转写（原始）",
    }.get(source, source)

    md = f"""# {video["title"]}

| 属性 | 值 |
|------|-----|
| UP主 | {video["upper"]} |
| 时长 | {dur} |
| 发布日期 | {pubdate} |
| 分类 | {c["primary_category"]} / {c["sub_category"]} |
| 置信度 | {c["confidence"]} |
| 标签 | {", ".join(c.get("tags", []))} |
| 文稿来源 | {source_label} |
| 链接 | [{video["bvid"]}]({video["link"]}) |

## 摘要

{c["summary"]}

## 完整文稿

{transcript}
"""
    return md


def main():
    with open(CLASSIFICATION_FILE, encoding="utf-8") as f:
        data = json.load(f)
    videos = data["videos"]

    # Load suitability filter
    suitability = {}
    if os.path.exists(SUITABILITY_FILE):
        with open(SUITABILITY_FILE, encoding="utf-8") as f:
            suit_data = json.load(f)
            suitability = suit_data.get("results", {})

    # Filter: only suitable videos (or all if no suitability file)
    if suitability:
        filtered = [v for v in videos if suitability.get(v["bvid"], {}).get("suitable", True)]
        print(f"Loaded {len(videos)} videos, {len(filtered)} suitable")
    else:
        filtered = videos
        print(f"Loaded {len(videos)} videos (no suitability filter)")

    # Clean docs dir
    if os.path.exists(DOCS_DIR):
        import shutil
        shutil.rmtree(DOCS_DIR)

    # Group by category
    by_category = defaultdict(lambda: defaultdict(list))
    for v in filtered:
        c = v["classification"]
        by_category[c["primary_category"]][c["sub_category"]].append(v)

    # Generate per-video markdown files
    file_count = 0
    polished_count = 0
    for primary, subs in sorted(by_category.items()):
        primary_dir = sanitize_dirname(primary)
        for sub, vids in sorted(subs.items()):
            sub_dir = sanitize_dirname(sub)
            dir_path = os.path.join(DOCS_DIR, primary_dir, sub_dir)
            os.makedirs(dir_path, exist_ok=True)

            for v in vids:
                transcript, source = read_transcript(v["bvid"])
                if not transcript:
                    continue
                if source == "polished":
                    polished_count += 1
                md = generate_video_md(v, transcript, source)
                fname = sanitize_filename(f"{v['bvid']}_{v['title']}") + ".md"
                fpath = os.path.join(dir_path, fname)
                with open(fpath, "w", encoding="utf-8") as f:
                    f.write(md)
                file_count += 1

    print(f"Generated {file_count} markdown files ({polished_count} polished)")

    # Generate INDEX.md
    generate_index(filtered, by_category)
    print(f"Generated INDEX.md")

    # Generate per-category index
    for primary, subs in sorted(by_category.items()):
        generate_category_index(primary, subs)
    print(f"Generated category index files")


def generate_index(videos, by_category):
    lines = ["# B站收藏夹文稿库\n"]
    lines.append(f"> 共 {len(videos)} 个视频 | 自动生成于 {time.strftime('%Y-%m-%d %H:%M')}\n")
    lines.append("│ 文稿已经过 AI 筛选和润色处理\n")
    lines.append("## 分类总览\n")

    for primary, subs in sorted(by_category.items()):
        total = sum(len(v) for v in subs.values())
        primary_dir = sanitize_dirname(primary)
        lines.append(f"### {primary}（{total}）\n")
        for sub, vids in sorted(subs.items()):
            sub_dir = sanitize_dirname(sub)
            lines.append(f"- [{sub}]({primary_dir}/{sub_dir}/)（{len(vids)}）")
        lines.append("")

    # Top uploaders
    from collections import Counter
    uppers = Counter(v["upper"] for v in videos)
    lines.append("## UP主排行\n")
    for name, cnt in uppers.most_common(20):
        lines.append(f"- {name}（{cnt}）")
    lines.append("")

    with open(os.path.join(DOCS_DIR, "INDEX.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def generate_category_index(primary, subs):
    primary_dir = sanitize_dirname(primary)
    dir_path = os.path.join(DOCS_DIR, primary_dir)
    os.makedirs(dir_path, exist_ok=True)

    lines = [f"# {primary}\n"]
    total = sum(len(v) for v in subs.values())
    lines.append(f"> 共 {total} 个视频\n")

    for sub, vids in sorted(subs.items()):
        sub_dir = sanitize_dirname(sub)
        lines.append(f"## {sub}（{len(vids)}）\n")
        vids_sorted = sorted(vids, key=lambda x: x["classification"]["confidence"], reverse=True)
        for v in vids_sorted:
            c = v["classification"]
            dur = format_duration(v["duration"])
            fname = sanitize_filename(f"{v['bvid']}_{v['title']}") + ".md"
            lines.append(f"- [{v['title']}]({sub_dir}/{fname})")
            lines.append(f"  - {c['summary']} | {v['upper']} | {dur}")
        lines.append("")

    with open(os.path.join(dir_path, "README.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


if __name__ == "__main__":
    main()
