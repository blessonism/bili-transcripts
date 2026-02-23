#!/usr/bin/env python3
"""快速导入单个 B 站视频到文稿库。

用法:
    .venv/bin/python3 scripts/quick_import.py BV1xxxxxx [--no-deploy]
    .venv/bin/python3 scripts/quick_import.py https://www.bilibili.com/video/BV1xxxxxx

流程: 获取元数据 → 提取字幕/ASR → AI 分类 → 追加到 classification.json → 重建 MDX → 部署
"""
import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.request
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from wbi import enc_wbi, get_wbi_keys

import httpx

# ── Paths ──
PROJECT = Path("/root/projects/bili-transcripts")
DOCS_V2 = Path("/root/projects/bili-docs-v2")
CREDS_PATH = PROJECT / "config/credentials.json"
COOKIES_PATH = PROJECT / "config/cookies.txt"
CLASSIFICATION_JSON = PROJECT / "data/classified/classification.json"
TRANSCRIPTS_DIR = PROJECT / "data/transcripts"
TRANSCRIPTS_ASR_DIR = PROJECT / "data/transcripts_asr"
PROXY_URL = "http://127.0.0.1:7890"

with open(CREDS_PATH) as f:
    _creds = json.load(f)

BILI_CREDS = _creds["bilibili"]
CLASSIFY_URL = _creds.get("classify", {}).get("api_url", "")
CLASSIFY_KEY = _creds.get("classify", {}).get("api_key", "")
CLASSIFY_MODEL = "gemini-3-flash-preview"
GROQ_KEYS = _creds.get("groq", {}).get("api_keys", [])


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def extract_bvid(s):
    m = re.search(r'(BV[a-zA-Z0-9]+)', s)
    return m.group(1) if m else None


def make_client():
    return httpx.Client(
        cookies={
            'SESSDATA': BILI_CREDS['sessdata'],
            'bili_jct': BILI_CREDS['bili_jct'],
            'buvid3': BILI_CREDS['buvid3'],
            'DedeUserID': BILI_CREDS['dedeuserid'],
        },
        headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://www.bilibili.com',
        },
        timeout=20,
    )


# ── Step 1: 获取元数据 ──
def fetch_metadata(bvid):
    client = make_client()
    r = client.get('https://api.bilibili.com/x/web-interface/view', params={'bvid': bvid})
    data = r.json()
    if data['code'] != 0:
        log(f"获取元数据失败: code={data['code']} msg={data.get('message', '')}")
        return None
    d = data['data']
    return {
        "bvid": d["bvid"], "aid": d["aid"], "title": d["title"],
        "upper": d["owner"]["name"], "duration": d["duration"],
        "desc": d.get("desc", ""),
        "link": f"https://www.bilibili.com/video/{d['bvid']}",
        "cover": d.get("pic", ""), "pubdate": d.get("pubdate", 0),
    }


# ── Step 2: 提取字幕 ──
def extract_subtitle(bvid, aid):
    client = make_client()
    r = client.get('https://api.bilibili.com/x/player/pagelist', params={'bvid': bvid})
    pages = r.json()
    if pages['code'] != 0 or not pages['data']:
        return "", "none"

    img_key, sub_key = get_wbi_keys()
    all_text = []

    for i, p in enumerate(pages['data']):
        cid = p['cid']
        params = enc_wbi({'aid': aid, 'cid': cid, 'bvid': bvid}, img_key, sub_key)
        r = client.get('https://api.bilibili.com/x/player/wbi/v2', params=params)
        data = r.json()
        if data['code'] != 0:
            continue

        subtitles = data['data'].get('subtitle', {}).get('subtitles', [])
        if not subtitles:
            continue

        sub_url = None
        for s in subtitles:
            if s.get('lan') == 'ai-zh':
                sub_url = s['subtitle_url']
                break
        if not sub_url:
            for s in subtitles:
                if 'zh' in s.get('lan', ''):
                    sub_url = s['subtitle_url']
                    break
        if not sub_url:
            sub_url = subtitles[0]['subtitle_url']

        if sub_url.startswith('//'):
            sub_url = 'https:' + sub_url

        r2 = client.get(sub_url)
        body = r2.json().get('body', [])
        lines, prev = [], ''
        for item in body:
            content = item.get('content', '').strip()
            if content and content != prev:
                lines.append(content)
                prev = content
        if lines:
            if len(pages['data']) > 1:
                all_text.append(f"\n## {p.get('part', f'P{i+1}')}\n" + '\n'.join(lines))
            else:
                all_text.append('\n'.join(lines))

        if i < len(pages['data']) - 1:
            time.sleep(0.5)

    if all_text:
        return '\n'.join(all_text).strip(), "subtitle"
    return "", "none"


# ── Step 2b: ASR ──
def asr_transcribe(bvid):
    if not GROQ_KEYS:
        log("无 Groq API key，跳过 ASR")
        return "", "none"

    from groq import Groq

    audio_path = PROJECT / "data/tmp_audio" / f"{bvid}.m4a"
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.unlink(missing_ok=True)

    out_template = str(audio_path).rsplit(".", 1)[0] + ".%(ext)s"
    log("下载音频...")
    try:
        r = subprocess.run([
            "yt-dlp", "-f", "ba/worst", "-x", "--audio-format", "m4a",
            "--no-playlist",
            "--cookies", str(COOKIES_PATH), "--socket-timeout", "30",
            "--retries", "2", "--limit-rate", "2M", "-o", out_template, "-q",
            f"https://www.bilibili.com/video/{bvid}"
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=120)
    except subprocess.TimeoutExpired:
        log("音频下载超时")
        return "", "none"

    if r.returncode != 0 or not audio_path.exists():
        log("音频下载失败")
        return "", "none"

    if audio_path.stat().st_size > 25 * 1024 * 1024:
        log(f"音频过大 ({audio_path.stat().st_size / 1024 / 1024:.1f}MB > 25MB)")
        audio_path.unlink(missing_ok=True)
        return "", "none"

    log("ASR 转写中...")
    for key in GROQ_KEYS:
        try:
            client = Groq(api_key=key, http_client=httpx.Client(proxy=PROXY_URL))
            with open(audio_path, "rb") as f:
                audio_data = f.read()
            t = client.audio.transcriptions.create(
                file=(f"{bvid}.m4a", audio_data),
                model="whisper-large-v3-turbo",
                language="zh", temperature=0,
                response_format="verbose_json",
            )
            text = (t.text or "").strip()
            audio_path.unlink(missing_ok=True)
            return (text, "asr") if text else ("", "none")
        except Exception as e:
            log(f"ASR key 失败: {e}")
            continue

    audio_path.unlink(missing_ok=True)
    log("所有 Groq key 均失败")
    return "", "none"


# ── Step 3: AI 分类 ──
CATEGORY_SCHEMA = """分类体系（主分类 → 子分类）：

人情世故
  - 社交沟通（夸人、接话、聊天技巧、small talk、道歉）
  - 饭局酒局（敬酒、饭局聊天、送礼、宴请）
  - 职场人际（边界感、麻烦别人、领导相处）
  - 恋爱关系（暧昧边界、恋爱模式、分手、约会）

职业发展
  - 求职面试（简历、面试技巧、找工作）
  - 创业商业（商业思维、创业复盘、变现、外贸）
  - 职场成长（初入职场、跳槽、职业规划）

认知成长
  - 思维方法（学习方法、思维模型、计划制定）
  - 心理自我（内耗、自我认知、情绪管理）
  - 哲学思辨（哲学、社会批判、深度思考）

技术工具
  - 编程开发（编程语言、框架、系统、算法）
  - AI 应用（AI编程、AI视频、AI工具、LLM）
  - 效率工具（笔记软件、Mac配置、浏览器、窗口管理）
  - 设计创作（PS/PPT/PR/AE/UI设计、视频剪辑）

学业考试
  - 公考（申论、行测、面试）
  - 学术科研（论文写作、数学建模、LaTeX）
  - 大学课程（数学、计算机、英语）

影视娱乐
  - 影视解说（电影/剧集解说、动画）
  - 综艺脱口秀（小品、脱口秀、综艺）
  - 音乐ASMR（音乐、ASMR、治愈）

生活方式
  - 美食烹饪（菜谱、美食探店）
  - 数码好物（数码产品、好物推荐）
  - 生活技巧（装修、收纳、日常技巧）
  - 摄影拍照（拍照技巧、摄影教程）

深度内容
  - 人物访谈（播客、深度对话、人物故事）
  - 行业洞察（行业分析、商业案例、社会观察）
  - 投资理财（投资、理财、财务自由）"""

SYSTEM_PROMPT = f"""你是一个视频内容分类专家。根据视频的标题、简介和文稿内容，对视频进行分类。

{CATEGORY_SCHEMA}

返回 JSON 格式：
{{"primary_category": "主分类名", "sub_category": "子分类名", "confidence": 0.95, "summary": "一句话概括视频核心内容（20-40字）", "tags": ["标签1", "标签2", "标签3"]}}

1. confidence 范围 0.0-1.0
2. 如果视频明显跨分类，选最主要的，confidence 适当降低
3. tags 给出 3-5 个关键词标签
4. summary 基于文稿内容写，不要只看标题猜"""


def classify_video(video, transcript):
    if not CLASSIFY_URL or not CLASSIFY_KEY:
        log("无分类 API 配置")
        return None

    upper = video.get("upper", "")
    desc = (video.get("desc", "") or "")[:200]
    dur_min = video["duration"] / 60
    user_msg = (f"视频信息：\n标题：{video['title']}\nUP主：{upper}\n"
                f"时长：{dur_min:.0f}分钟\n简介：{desc}\n\n文稿前1500字：\n{transcript[:1500]}")

    payload = json.dumps({
        "model": CLASSIFY_MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_msg},
        ],
        "temperature": 0.1, "max_tokens": 500,
    }).encode("utf-8")

    req = urllib.request.Request(CLASSIFY_URL, data=payload, headers={
        "Authorization": f"Bearer {CLASSIFY_KEY}",
        "Content-Type": "application/json",
    })

    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            content = data["choices"][0]["message"]["content"].strip()
            if content.startswith("```"):
                content = content.split("\n", 1)[1]
                content = content.rsplit("```", 1)[0]
            return json.loads(content)
        except Exception as e:
            log(f"分类 attempt {attempt+1} 失败: {e}")
            if attempt < 2:
                time.sleep(2)
    return None


# ── Step 4: 更新 classification.json ──
def update_classification(video, classification):
    if not CLASSIFICATION_JSON.exists():
        data = {"meta": {"total": 0}, "videos": []}
    else:
        with open(CLASSIFICATION_JSON) as f:
            data = json.load(f)

    existing = {v["bvid"] for v in data["videos"]}
    if video["bvid"] in existing:
        for i, v in enumerate(data["videos"]):
            if v["bvid"] == video["bvid"]:
                data["videos"][i] = {**video, "classification": classification}
                break
        log(f"更新已有条目: {video['bvid']}")
    else:
        data["videos"].append({**video, "classification": classification})
        data["meta"]["total"] = len(data["videos"])
        log(f"新增条目: {video['bvid']}")

    tmp = CLASSIFICATION_JSON.with_suffix('.tmp')
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, CLASSIFICATION_JSON)


# ── Step 5: 重建 + 部署 ──
def rebuild_and_deploy():
    log("生成 MDX...")
    r = subprocess.run(
        [".venv/bin/python3", str(DOCS_V2 / "scripts/generate-mdx.py")],
        cwd=str(PROJECT), capture_output=True, text=True, timeout=120,
    )
    if r.returncode != 0:
        log(f"MDX 生成失败: {r.stderr[:200]}")
        return False

    log("Git push...")
    subprocess.run(["git", "add", "-A"], cwd=str(DOCS_V2), capture_output=True)
    result = subprocess.run(
        ["git", "diff", "--cached", "--quiet"], cwd=str(DOCS_V2), capture_output=True,
    )
    if result.returncode == 0:
        log("无文件变更，跳过 push")
    else:
        subprocess.run(
            ["git", "commit", "-m", f"feat: quick import {datetime.now().strftime('%Y-%m-%d %H:%M')}"],
            cwd=str(DOCS_V2), capture_output=True,
        )
        subprocess.run(["git", "push"], cwd=str(DOCS_V2), capture_output=True, timeout=60)

    log("Build & Deploy...")
    deploy_script = DOCS_V2 / "deploy.sh"
    if deploy_script.exists():
        r = subprocess.run(
            ["bash", str(deploy_script)],
            cwd=str(DOCS_V2), capture_output=True, text=True, timeout=600,
        )
        if r.returncode != 0:
            log(f"部署失败: {r.stderr[:200]}")
            return False
    else:
        log("deploy.sh 不存在，跳过部署")
        return False

    log("部署完成")
    return True


def main():
    parser = argparse.ArgumentParser(description="快速导入单个 B 站视频到文稿库")
    parser.add_argument("video", help="BV 号或 B 站视频 URL")
    parser.add_argument("--no-deploy", action="store_true", help="跳过部署（仅提取+分类）")
    parser.add_argument("--force-asr", action="store_true", help="强制 ASR（即使有字幕）")
    args = parser.parse_args()

    bvid = extract_bvid(args.video)
    if not bvid:
        print(f"无法识别 BV 号: {args.video}")
        sys.exit(1)

    log(f"开始导入: {bvid}")

    # 1. 元数据
    log("获取视频元数据...")
    video = fetch_metadata(bvid)
    if not video:
        sys.exit(1)
    log(f"标题: {video['title']}")
    log(f"UP主: {video['upper']} | 时长: {video['duration'] // 60}分钟")

    # 2. 字幕
    transcript, source = "", "none"
    if not args.force_asr:
        log("尝试提取 B 站字幕...")
        transcript, source = extract_subtitle(bvid, video["aid"])

    if transcript:
        log(f"字幕提取成功 ({source}, {len(transcript)} 字)")
        TRANSCRIPTS_DIR.mkdir(parents=True, exist_ok=True)
        with open(TRANSCRIPTS_DIR / f"{bvid}.txt", "w", encoding="utf-8") as f:
            f.write(transcript)
    else:
        if video["duration"] > 1800:
            log(f"视频 {video['duration'] // 60} 分钟 > 30min，无字幕且超时长限制")
            sys.exit(1)
        log("无字幕，尝试 ASR...")
        transcript, source = asr_transcribe(bvid)
        if transcript:
            log(f"ASR 成功 ({len(transcript)} 字)")
            TRANSCRIPTS_ASR_DIR.mkdir(parents=True, exist_ok=True)
            with open(TRANSCRIPTS_ASR_DIR / f"{bvid}.txt", "w", encoding="utf-8") as f:
                f.write(transcript)
        else:
            print("字幕和 ASR 均失败")
            sys.exit(1)

    # 3. 分类
    log("AI 分类中...")
    classification = classify_video(video, transcript)
    if not classification:
        print("分类失败")
        sys.exit(1)
    log(f"分类: {classification['primary_category']} / {classification['sub_category']} (confidence: {classification['confidence']})")
    log(f"摘要: {classification['summary']}")

    # 4. 更新 classification.json
    update_classification(video, classification)

    # 5. 部署
    if args.no_deploy:
        log(f"导入完成 (--no-deploy): {bvid} → {classification['primary_category']}/{classification['sub_category']}")
    else:
        if rebuild_and_deploy():
            log(f"导入并部署完成: {bvid}")
        else:
            log("导入完成但部署失败，可手动 deploy.sh")

    return 0


if __name__ == "__main__":
    sys.exit(main())
