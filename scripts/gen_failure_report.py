#!/usr/bin/env python3
"""从 asr_status.json + 视频元数据生成失败清单。

产出：
  data/raw/asr_failures.json   — 结构化 JSON（程序可读）
  data/raw/asr_failures.md     — Markdown 表格（人类可读）

随时可跑，幂等。
"""
import json
from pathlib import Path
from datetime import datetime

PROJECT = Path("/root/projects/bili-transcripts")
STATUS = PROJECT / "data/raw/asr_status.json"
QUEUE = PROJECT / "data/raw/asr_queue_short_20m.json"
SKIPPED = PROJECT / "data/raw/asr_skipped_long_20m.json"
ALL_VIDEOS = PROJECT / "data/raw/no_subtitle.json"

OUT_JSON = PROJECT / "data/raw/asr_failures.json"
OUT_MD = PROJECT / "data/raw/asr_failures.md"

# Build bvid → video metadata lookup
video_map = {}
for src in [QUEUE, SKIPPED, ALL_VIDEOS]:
    if src.exists():
        with open(src) as f:
            data = json.load(f)
        for v in data.get("videos", []):
            bvid = v.get("bvid")
            if bvid and bvid not in video_map:
                video_map[bvid] = v

# Load status
with open(STATUS) as f:
    status = json.load(f)

# Categorize failures
failures = {"error_download": [], "error_transcribe": [], "no_speech": []}
for bvid, st in status["processed"].items():
    if st in failures:
        meta = video_map.get(bvid, {})
        failures[st].append({
            "bvid": bvid,
            "title": meta.get("title", "未知"),
            "duration": meta.get("duration", 0),
            "upper": (meta.get("upper") or {}).get("name", "未知"),
            "link": f"https://www.bilibili.com/video/{bvid}",
            "status": st,
        })

# Sort each category by bvid
for k in failures:
    failures[k].sort(key=lambda x: x["bvid"])

now = datetime.now().isoformat()
report = {
    "generated_at": now,
    "summary": {
        "total_processed": len(status["processed"]),
        "ok": status["stats"].get("ok", 0),
        "error_download": len(failures["error_download"]),
        "error_transcribe": len(failures["error_transcribe"]),
        "no_speech": len(failures["no_speech"]),
        "remaining": 883 - len(status["processed"]),
    },
    "failures": failures,
}

# Write JSON
with open(OUT_JSON, "w", encoding="utf-8") as f:
    json.dump(report, f, ensure_ascii=False, indent=2)

# Write Markdown
def fmt_dur(s):
    m, sec = divmod(int(s), 60)
    return f"{m}:{sec:02d}"

with open(OUT_MD, "w", encoding="utf-8") as f:
    f.write(f"# ASR 失败清单\n\n")
    f.write(f"> 生成时间: {now}\n\n")
    f.write(f"## 总览\n\n")
    f.write(f"| 状态 | 数量 |\n|------|------|\n")
    f.write(f"| ✅ 转录成功 | {report['summary']['ok']} |\n")
    f.write(f"| ❌ 下载失败 | {report['summary']['error_download']} |\n")
    f.write(f"| ❌ 转录失败 | {report['summary']['error_transcribe']} |\n")
    f.write(f"| ⚠️ 无语音内容 | {report['summary']['no_speech']} |\n")
    f.write(f"| ⏳ 未处理 | {report['summary']['remaining']} |\n\n")

    if failures["error_download"]:
        f.write(f"## ❌ 下载失败（{len(failures['error_download'])} 个）\n\n")
        f.write(f"可能原因：视频已删除/下架、地区限制、Cookie 失效\n\n")
        f.write(f"| BV号 | 标题 | 时长 | UP主 | 链接 |\n")
        f.write(f"|------|------|------|------|------|\n")
        for v in failures["error_download"]:
            f.write(f"| {v['bvid']} | {v['title'][:30]} | {fmt_dur(v['duration'])} | {v['upper']} | [链接]({v['link']}) |\n")
        f.write("\n")

    if failures["error_transcribe"]:
        f.write(f"## ❌ 转录失败（{len(failures['error_transcribe'])} 个）\n\n")
        f.write(f"可能原因：Groq API 错误、音频格式不支持、文件过大\n\n")
        f.write(f"| BV号 | 标题 | 时长 | UP主 | 链接 |\n")
        f.write(f"|------|------|------|------|------|\n")
        for v in failures["error_transcribe"]:
            f.write(f"| {v['bvid']} | {v['title'][:30]} | {fmt_dur(v['duration'])} | {v['upper']} | [链接]({v['link']}) |\n")
        f.write("\n")

    if failures["no_speech"]:
        f.write(f"## ⚠️ 无语音内容（{len(failures['no_speech'])} 个）\n\n")
        f.write(f"Whisper 未识别到语音，可能是纯音乐/纯画面/极短片段\n\n")
        f.write(f"| BV号 | 标题 | 时长 | UP主 | 链接 |\n")
        f.write(f"|------|------|------|------|------|\n")
        for v in failures["no_speech"]:
            f.write(f"| {v['bvid']} | {v['title'][:30]} | {fmt_dur(v['duration'])} | {v['upper']} | [链接]({v['link']}) |\n")
        f.write("\n")

print(f"Done. JSON: {OUT_JSON}")
print(f"Done. MD:   {OUT_MD}")
print(f"Summary: ok={report['summary']['ok']}, dl_fail={report['summary']['error_download']}, tx_fail={report['summary']['error_transcribe']}, no_speech={report['summary']['no_speech']}, remaining={report['summary']['remaining']}")
