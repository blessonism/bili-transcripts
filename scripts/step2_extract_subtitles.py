#!/usr/bin/env python3
"""Step 2: AI 字幕批量提取

对默认收藏夹视频逐个提取 B 站 AI 字幕，输出纯文本到 data/transcripts/。
支持断点续传、限流控制、多P视频。
"""
import json
import time
import sys
import httpx
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent))
from wbi import enc_wbi, get_wbi_keys

BASE_DIR = Path('/root/projects/bili-transcripts')
CONFIG_PATH = BASE_DIR / 'config' / 'credentials.json'
VIDEOS_PATH = BASE_DIR / 'data' / 'raw' / 'videos.json'
TRANSCRIPTS_DIR = BASE_DIR / 'data' / 'transcripts'
STATUS_PATH = BASE_DIR / 'data' / 'raw' / 'subtitle_status.json'
LOG_PATH = BASE_DIR / 'logs' / f'step2_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

DELAY_PER_VIDEO = 1.0  # 秒
DELAY_PER_PAGE = 0.5
RETRY_DELAY = 10
MAX_RETRIES = 3
WBI_REFRESH_INTERVAL = 300  # 每 5 分钟刷新 WBI keys

TRANSCRIPTS_DIR.mkdir(parents=True, exist_ok=True)

def log(msg):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f'[{ts}] {msg}'
    print(line, flush=True)
    with open(LOG_PATH, 'a') as f:
        f.write(line + '\n')

def load_credentials():
    with open(CONFIG_PATH) as f:
        return json.load(f)['bilibili']

def make_client(creds):
    cookies = {
        'SESSDATA': creds['sessdata'],
        'bili_jct': creds['bili_jct'],
        'buvid3': creds['buvid3'],
        'DedeUserID': creds['dedeuserid'],
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://www.bilibili.com',
    }
    return httpx.Client(cookies=cookies, headers=headers, timeout=20)

def load_status():
    """\u52a0\u8f7d\u5df2\u5904\u7406\u72b6\u6001\uff08\u65ad\u70b9\u7eed\u4f20\uff09"""
    if STATUS_PATH.exists():
        with open(STATUS_PATH) as f:
            return json.load(f)
    return {}

def save_status(status):
    with open(STATUS_PATH, 'w') as f:
        json.dump(status, f, ensure_ascii=False, indent=2)

def get_cids(client, bvid):
    """\u83b7\u53d6\u89c6\u9891\u6240\u6709\u5206P\u7684 cid"""
    r = client.get('https://api.bilibili.com/x/player/pagelist', params={'bvid': bvid})
    data = r.json()
    if data['code'] != 0:
        return None
    return [(p['cid'], p.get('part', f'P{i+1}')) for i, p in enumerate(data['data'])]

def get_subtitle_url(client, aid, bvid, cid, img_key, sub_key):
    """\u83b7\u53d6\u5b57\u5e55 URL\uff08\u4f18\u5148 ai-zh\uff09"""
    params = enc_wbi({'aid': aid, 'cid': cid, 'bvid': bvid}, img_key, sub_key)
    r = client.get('https://api.bilibili.com/x/player/wbi/v2', params=params)
    data = r.json()
    if data['code'] != 0:
        return None, data['code']
    
    subtitles = data['data'].get('subtitle', {}).get('subtitles', [])
    if not subtitles:
        return None, 0
    
    # \u4f18\u5148\u9009 ai-zh\uff0c\u5176\u6b21\u4efb\u4f55\u4e2d\u6587\uff0c\u6700\u540e\u7b2c\u4e00\u4e2a
    for s in subtitles:
        if s.get('lan') == 'ai-zh':
            return s['subtitle_url'], 0
    for s in subtitles:
        if 'zh' in s.get('lan', ''):
            return s['subtitle_url'], 0
    return subtitles[0]['subtitle_url'], 0

def download_subtitle_text(client, url):
    """\u4e0b\u8f7d\u5b57\u5e55 JSON \u5e76\u8f6c\u4e3a\u7eaf\u6587\u672c"""
    if url.startswith('//'):
        url = 'https:' + url
    r = client.get(url)
    data = r.json()
    body = data.get('body', [])
    if not body:
        return ''
    
    # \u62fc\u63a5\u6240\u6709\u5b57\u5e55\u6761\u76ee\uff0c\u6bcf\u6761\u4e4b\u95f4\u7528\u7a7a\u683c\u8fde\u63a5
    # \u53bb\u9664\u91cd\u590d\u7684\u76f8\u90bb\u884c\uff08B \u7ad9\u5b57\u5e55\u6709\u65f6\u4f1a\u91cd\u590d\uff09
    lines = []
    prev = ''
    for item in body:
        content = item.get('content', '').strip()
        if content and content != prev:
            lines.append(content)
            prev = content
    return '\n'.join(lines)

def process_video(client, video, img_key, sub_key):
    """\u5904\u7406\u5355\u4e2a\u89c6\u9891\uff0c\u8fd4\u56de (status, transcript_text)"""
    bvid = video['bvid']
    aid = video['aid']
    
    # \u83b7\u53d6\u5206P\u5217\u8868
    cids = get_cids(client, bvid)
    if cids is None:
        return 'error_pagelist', ''
    
    all_text = []
    multi_part = len(cids) > 1
    
    for i, (cid, part_name) in enumerate(cids):
        sub_url, code = get_subtitle_url(client, aid, bvid, cid, img_key, sub_key)
        
        if code == -403:
            return 'error_wbi', ''
        if code == -101:
            return 'error_cookie', ''
        
        if sub_url is None:
            if multi_part:
                all_text.append(f'\n## {part_name}\n[\u65e0\u5b57\u5e55]')
            continue
        
        text = download_subtitle_text(client, sub_url)
        if text:
            if multi_part:
                all_text.append(f'\n## {part_name}\n{text}')
            else:
                all_text.append(text)
        
        if i < len(cids) - 1:
            time.sleep(DELAY_PER_PAGE)
    
    if not all_text:
        return 'no_subtitle', ''
    
    full_text = '\n'.join(all_text).strip()
    return 'ok', full_text

def main():
    log('=' * 50)
    log('Step 2: AI \u5b57\u5e55\u6279\u91cf\u63d0\u53d6')
    log('=' * 50)
    
    # \u52a0\u8f7d\u89c6\u9891\u5217\u8868\uff08\u53ea\u53d6\u9ed8\u8ba4\u6536\u85cf\u5939\uff09
    with open(VIDEOS_PATH) as f:
        data = json.load(f)
    videos = data['videos']
    log(f'\u9ed8\u8ba4\u6536\u85cf\u5939\u89c6\u9891\u6570: {len(videos)}')
    
    # \u52a0\u8f7d\u65ad\u70b9
    status = load_status()
    already_done = sum(1 for s in status.values() if s in ('ok', 'no_subtitle'))
    log(f'\u5df2\u5904\u7406: {already_done}, \u5f85\u5904\u7406: {len(videos) - already_done}')
    
    creds = load_credentials()
    client = make_client(creds)
    img_key, sub_key = get_wbi_keys(client)
    last_wbi_refresh = time.time()
    log('WBI keys OK')
    
    # \u7edf\u8ba1
    stats = {'ok': 0, 'no_subtitle': 0, 'error_pagelist': 0, 'error_wbi': 0, 'error_cookie': 0, 'error_other': 0}
    
    for i, video in enumerate(videos):
        bvid = video['bvid']
        
        # \u8df3\u8fc7\u5df2\u5904\u7406
        if bvid in status and status[bvid] in ('ok', 'no_subtitle'):
            stats[status[bvid]] += 1
            continue
        
        # \u5b9a\u671f\u5237\u65b0 WBI keys
        if time.time() - last_wbi_refresh > WBI_REFRESH_INTERVAL:
            try:
                img_key, sub_key = get_wbi_keys(client)
                last_wbi_refresh = time.time()
            except Exception:
                pass
        
        # \u5904\u7406
        retries = 0
        result = 'error_other'
        text = ''
        
        while retries < MAX_RETRIES:
            try:
                result, text = process_video(client, video, img_key, sub_key)
                break
            except httpx.TimeoutException:
                retries += 1
                log(f'  [{i+1}/{len(videos)}] {bvid} \u8d85\u65f6 (\u91cd\u8bd5 {retries}/{MAX_RETRIES})')
                time.sleep(RETRY_DELAY)
            except Exception as e:
                retries += 1
                log(f'  [{i+1}/{len(videos)}] {bvid} \u5f02\u5e38: {e} (\u91cd\u8bd5 {retries}/{MAX_RETRIES})')
                time.sleep(RETRY_DELAY)
        
        if result == 'error_wbi':
            log(f'  [{i+1}] {bvid} WBI \u7b7e\u540d\u5931\u8d25\uff0c\u5237\u65b0 keys')
            try:
                img_key, sub_key = get_wbi_keys(client)
                last_wbi_refresh = time.time()
                result, text = process_video(client, video, img_key, sub_key)
            except Exception:
                pass
        
        if result == 'error_cookie':
            log(f'\n\u274c Cookie \u5df2\u8fc7\u671f\uff0c\u505c\u6b62\u5904\u7406\u3002\u8bf7\u66f4\u65b0 SESSDATA \u540e\u91cd\u65b0\u8fd0\u884c\uff08\u652f\u6301\u65ad\u70b9\u7eed\u4f20\uff09')
            save_status(status)
            sys.exit(1)
        
        # \u4fdd\u5b58\u7ed3\u679c
        if result == 'ok' and text:
            out_path = TRANSCRIPTS_DIR / f'{bvid}.txt'
            with open(out_path, 'w', encoding='utf-8') as f:
                f.write(text)
        
        status[bvid] = result
        stats[result] = stats.get(result, 0) + 1
        
        # \u6bcf 50 \u4e2a\u89c6\u9891\u4fdd\u5b58\u4e00\u6b21\u65ad\u70b9 + \u6253\u5370\u8fdb\u5ea6
        processed = i + 1
        if processed % 50 == 0 or processed == len(videos):
            save_status(status)
            ok_count = sum(1 for s in status.values() if s == 'ok')
            no_sub = sum(1 for s in status.values() if s == 'no_subtitle')
            errors = sum(1 for s in status.values() if s.startswith('error'))
            log(f'\u8fdb\u5ea6: {processed}/{len(videos)} | \u6709\u5b57\u5e55: {ok_count} | \u65e0\u5b57\u5e55: {no_sub} | \u9519\u8bef: {errors}')
        elif processed % 10 == 0:
            log(f'  [{processed}/{len(videos)}] ...')
        
        time.sleep(DELAY_PER_VIDEO)
    
    # \u6700\u7ec8\u4fdd\u5b58
    save_status(status)
    
    # \u7edf\u8ba1\u62a5\u544a
    ok_count = sum(1 for s in status.values() if s == 'ok')
    no_sub = sum(1 for s in status.values() if s == 'no_subtitle')
    errors = sum(1 for s in status.values() if s.startswith('error'))
    total = ok_count + no_sub + errors
    
    log(f'\n{"=" * 50}')
    log(f'\u5b8c\u6210\u7edf\u8ba1:')
    log(f'  \u603b\u5904\u7406: {total}')
    log(f'  \u6709\u5b57\u5e55: {ok_count} ({ok_count/total*100:.1f}%)')
    log(f'  \u65e0\u5b57\u5e55: {no_sub} ({no_sub/total*100:.1f}%)')
    log(f'  \u9519\u8bef: {errors}')
    log(f'\n\u5b57\u5e55\u6587\u4ef6\u76ee\u5f55: {TRANSCRIPTS_DIR}')
    log(f'\u72b6\u6001\u6587\u4ef6: {STATUS_PATH}')
    
    # \u751f\u6210\u65e0\u5b57\u5e55\u89c6\u9891\u5217\u8868\uff08\u4f9b Step 3 ASR \u4f7f\u7528\uff09
    no_sub_videos = [v for v in videos if status.get(v['bvid']) == 'no_subtitle']
    no_sub_path = BASE_DIR / 'data' / 'raw' / 'no_subtitle.json'
    with open(no_sub_path, 'w', encoding='utf-8') as f:
        json.dump({
            'meta': {'total': len(no_sub_videos), 'generated_at': datetime.now().isoformat()},
            'videos': no_sub_videos,
        }, f, ensure_ascii=False, indent=2)
    log(f'\u65e0\u5b57\u5e55\u89c6\u9891\u5217\u8868: {no_sub_path} ({len(no_sub_videos)} \u4e2a)')

if __name__ == '__main__':
    main()
