#!/usr/bin/env python3
"""Step 1: 收藏夹元数据采集（含 WBI 签名）

支持两种模式：
  - 增量模式（默认）：只拉取上次扫描后的新收藏，按 fav_time 截断
  - 全量模式（--full）：扫描所有收藏夹全部页面

增量模式依赖 .last_scan.json 记录上次扫描的最新 fav_time。
若该文件不存在，自动降级为全量模式。
"""
import json
import time
import sys
import argparse
import httpx
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent))
from wbi import enc_wbi, get_wbi_keys

BASE_DIR = Path('/root/projects/bili-transcripts')
CONFIG_PATH = BASE_DIR / 'config' / 'credentials.json'
OUTPUT_PATH = BASE_DIR / 'data' / 'raw' / 'videos.json'
PROGRESS_PATH = BASE_DIR / 'data' / 'raw' / '.fetch_progress.json'
LAST_SCAN_PATH = BASE_DIR / 'data' / 'raw' / '.last_scan.json'
LOG_PATH = BASE_DIR / 'logs' / f'step1_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

PAGE_SIZE = 20
DELAY = 1.5
# 增量模式下，连续碰到多少个已知视频才停止扫描该收藏夹
# 防止 fav_time 排序偶尔不严格单调
INCREMENTAL_STOP_THRESHOLD = 3


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
    return httpx.Client(cookies=cookies, headers=headers, timeout=15)


def load_progress():
    if PROGRESS_PATH.exists():
        with open(PROGRESS_PATH) as f:
            p = json.load(f)
        log(f'断点恢复: 已采集 {len(p["videos"])} 个, folder_idx={p.get("folder_idx",0)}, page={p["next_page"]}')
        return p
    return {'videos': [], 'next_page': 1, 'total': None, 'folder_idx': 0, 'seen_bvids': []}


def atomic_write_json(path, data, indent=None):
    """原子写入 JSON：先写临时文件再 rename，防止崩溃导致文件损坏"""
    import os
    tmp = Path(str(path) + '.tmp')
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=indent)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def save_progress(p):
    atomic_write_json(PROGRESS_PATH, p)


def load_last_scan():
    """加载上次扫描状态，用于增量模式截断"""
    if LAST_SCAN_PATH.exists():
        with open(LAST_SCAN_PATH) as f:
            return json.load(f)
    return None


def save_last_scan(latest_fav_time, total_known):
    """保存本次扫描状态"""
    atomic_write_json(LAST_SCAN_PATH, {
        'last_scan_time': datetime.now().isoformat(),
        'latest_fav_time': latest_fav_time,
        'total_known': total_known,
    }, indent=2)


def load_existing_videos():
    """加载现有 videos.json，用于增量 append"""
    if OUTPUT_PATH.exists():
        with open(OUTPUT_PATH) as f:
            data = json.load(f)
        return data.get('videos', [])
    return []


def fetch_page(client, media_id, page, img_key, sub_key):
    params = enc_wbi({'media_id': media_id, 'pn': page, 'ps': PAGE_SIZE, 'platform': 'web'}, img_key, sub_key)
    r = client.get('https://api.bilibili.com/x/v3/fav/resource/list', params=params)
    return r.json()


def get_all_folders(client, uid, img_key, sub_key):
    params = enc_wbi({'up_mid': uid}, img_key, sub_key)
    r = client.get('https://api.bilibili.com/x/v3/fav/folder/created/list-all', params=params)
    d = r.json()
    if d['code'] != 0:
        log(f'获取收藏夹列表失败: {d}')
        return []
    return d['data']['list']


def parse_video_item(item, folder_title):
    """从 API 返回的 item 构造标准化视频对象"""
    bvid = item.get('bvid', '')
    return {
        'bvid': bvid,
        'aid': item.get('id', 0),
        'title': item.get('title', ''),
        'desc': item.get('intro', ''),
        'duration': item.get('duration', 0),
        'upper': {
            'name': item.get('upper', {}).get('name', ''),
            'mid': item.get('upper', {}).get('mid', 0),
        },
        'cover': item.get('cover', ''),
        'pubdate': item.get('pubtime', 0),
        'tid': item.get('type', 0),
        'page_count': item.get('page', 1),
        'fav_time': item.get('fav_time', 0),
        'folder': folder_title,
        'link': f'https://www.bilibili.com/video/{bvid}',
    }


def fetch_folder(client, media_id, folder_title, img_key, sub_key, start_page, seen_bvids,
                 incremental=False, cutoff_fav_time=0):
    """采集单个收藏夹的视频。

    Args:
        incremental: 是否增量模式
        cutoff_fav_time: 增量模式下的截断时间戳（fav_time <= 此值的视频视为已知）
    
    Returns:
        (new_videos, last_page, img_key, sub_key, cookie_expired)
    """
    videos = []
    page = start_page
    errors = 0
    consecutive_known = 0  # 增量模式：连续碰到已知视频的计数
    should_stop_after_page = False  # 增量截断：扫完当前页再停

    while True:
        try:
            data = fetch_page(client, media_id, page, img_key, sub_key)

            if data['code'] != 0:
                log(f'  [{folder_title}] page {page} 错误: code={data["code"]}')
                errors += 1
                if data['code'] == -403:
                    img_key, sub_key = get_wbi_keys(client)
                    time.sleep(3)
                    data = fetch_page(client, media_id, page, img_key, sub_key)
                    if data['code'] == -101:
                        log('Cookie 已过期（WBI 重试后发现）')
                        return videos, page, img_key, sub_key, True
                    if data['code'] != 0:
                        if errors >= 5:
                            log(f'  [{folder_title}] 连续错误 {errors} 次，停止该收藏夹')
                            break
                        # 重试同一页而非跳页
                        time.sleep(3)
                        continue
                elif data['code'] == -101:
                    log('Cookie 已过期')
                    return videos, page, img_key, sub_key, True
                else:
                    if errors >= 5:
                        break
                    # 重试同一页而非跳页
                    time.sleep(3)
                    continue

            errors = 0
            medias = data['data'].get('medias') or []
            has_more = data['data'].get('has_more', False)

            if not medias:
                log(f'  [{folder_title}] page {page} 无数据，完成')
                break

            added = 0
            for item in medias:
                bvid = item.get('bvid', '')
                fav_time = item.get('fav_time', 0)

                # 增量截断：碰到已知视频，累计计数
                if incremental and bvid in seen_bvids and fav_time <= cutoff_fav_time:
                    consecutive_known += 1
                    if consecutive_known >= INCREMENTAL_STOP_THRESHOLD:
                        # 标记截断，但继续扫完当前页（防止同页有未知视频）
                        should_stop_after_page = True
                    continue
                else:
                    consecutive_known = 0

                if bvid in seen_bvids:
                    continue

                seen_bvids.add(bvid)
                videos.append(parse_video_item(item, folder_title))
                added += 1

            log(f'  [{folder_title}] page {page}: +{added} 新 ({len(medias)} 返回, has_more={has_more})')

            if should_stop_after_page:
                log(f'  [{folder_title}] 增量截断（连续 {INCREMENTAL_STOP_THRESHOLD} 个已知视频），停止该收藏夹')
                break

            page += 1

            if not has_more:
                log(f'  [{folder_title}] has_more=False，完成')
                break

            time.sleep(DELAY)

        except httpx.TimeoutException:
            log(f'  [{folder_title}] page {page} 超时，重试')
            time.sleep(10)
            continue
        except Exception as e:
            log(f'  [{folder_title}] page {page} 异常: {e}')
            return videos, page, img_key, sub_key, False

    return videos, page, img_key, sub_key, False


def main():
    parser = argparse.ArgumentParser(description='B站收藏夹元数据采集')
    parser.add_argument('--full', action='store_true', help='全量模式（忽略增量状态，重新扫描所有收藏夹）')
    args = parser.parse_args()

    # 判断运行模式
    last_scan = load_last_scan()
    is_incremental = not args.full and last_scan is not None

    mode_label = '增量' if is_incremental else '全量'
    log('=' * 50)
    log(f'Step 1: 收藏夹元数据采集（{mode_label}模式）')
    if is_incremental:
        log(f'  上次扫描: {last_scan["last_scan_time"]}')
        log(f'  截断 fav_time: {last_scan["latest_fav_time"]}')
        log(f'  已知视频数: {last_scan["total_known"]}')
    log('=' * 50)

    creds = load_credentials()
    client = make_client(creds)
    img_key, sub_key = get_wbi_keys(client)
    log('WBI keys OK')

    # 获取所有收藏夹
    folders = get_all_folders(client, creds['dedeuserid'], img_key, sub_key)
    log(f'发现 {len(folders)} 个收藏夹:')
    total_expected = 0
    for f in folders:
        log(f'  {f["title"]} (id={f["id"]}, count={f["media_count"]})')
        total_expected += f['media_count']
    log(f'预期总视频数: {total_expected}')

    # 加载已有数据
    if is_incremental:
        # 增量模式：从现有 videos.json 加载，append 新视频
        all_videos = load_existing_videos()
        seen_bvids = set(v['bvid'] for v in all_videos)
        cutoff_fav_time = last_scan['latest_fav_time']
        log(f'已加载 {len(all_videos)} 个已知视频')
    else:
        # 全量模式：断点恢复或从零开始
        progress = load_progress()
        all_videos = progress['videos']
        seen_bvids = set(progress.get('seen_bvids', [v['bvid'] for v in all_videos]))
        cutoff_fav_time = 0

    start_folder_idx = 0
    start_page = 1
    if not is_incremental:
        progress = load_progress()
        start_folder_idx = progress.get('folder_idx', 0)
        start_page = progress['next_page'] if start_folder_idx > 0 or progress['next_page'] > 1 else 1

    cookie_expired = False
    new_count = 0
    max_fav_time = cutoff_fav_time  # 追踪本次扫描中最新的 fav_time

    for i, folder in enumerate(folders):
        if not is_incremental and i < start_folder_idx:
            continue

        page = start_page if (not is_incremental and i == start_folder_idx) else 1
        start_page = 1

        log(f'\n采集收藏夹 [{i+1}/{len(folders)}]: {folder["title"]} ({folder["media_count"]} 个)')

        if folder['media_count'] == 0:
            log(f'  空收藏夹，跳过')
            continue

        new_videos, last_page, img_key, sub_key, expired = fetch_folder(
            client, folder['id'], folder['title'], img_key, sub_key, page, seen_bvids,
            incremental=is_incremental, cutoff_fav_time=cutoff_fav_time,
        )
        all_videos.extend(new_videos)
        new_count += len(new_videos)

        # 追踪最新 fav_time
        for v in new_videos:
            if v['fav_time'] > max_fav_time:
                max_fav_time = v['fav_time']

        # 全量模式保存断点
        if not is_incremental:
            save_progress({
                'videos': all_videos,
                'next_page': 1,
                'total': total_expected,
                'folder_idx': i + 1,
                'seen_bvids': list(seen_bvids),
            })

        log(f'  小计: +{len(new_videos)} 新视频 (累计 {len(all_videos)})')

        if expired:
            cookie_expired = True
            break

        time.sleep(1)

    client.close()

    # 无论是否 cookie 过期，先保存已采集的数据（防止丢失增量进度）
    result = {
        'meta': {
            'total': len(all_videos),
            'folders_scanned': len(folders),
            'fetched_at': datetime.now().isoformat(),
            'mode': mode_label,
            'new_videos': new_count,
            'cookie_expired': cookie_expired,
        },
        'videos': all_videos,
    }
    atomic_write_json(OUTPUT_PATH, result, indent=2)
    log(f'\n结果已写入: {OUTPUT_PATH} (new: {new_count})')

    # 清理全量模式断点文件
    if not cookie_expired and PROGRESS_PATH.exists():
        PROGRESS_PATH.unlink()

    # 保存扫描状态（供下次增量使用）
    # 如果没有新视频，保留已有的 max_fav_time；防止 max() 空序列
    if max_fav_time == 0 and all_videos:
        fav_times = [v['fav_time'] for v in all_videos if v.get('fav_time', 0) > 0]
        max_fav_time = max(fav_times) if fav_times else 0
    save_last_scan(max_fav_time, len(all_videos))
    log(f'扫描状态已保存: latest_fav_time={max_fav_time}, total={len(all_videos)}')

    if cookie_expired:
        log('\nCookie 已过期，已保存当前进度。请更新 Cookie 后重新运行。')
        sys.exit(1)

    # 统计
    durations = [v['duration'] for v in all_videos if v['duration'] > 0]
    total_hours = sum(durations) / 3600 if durations else 0
    uppers = set(v['upper']['name'] for v in all_videos)

    log(f'\n最终统计:')
    log(f'  模式: {mode_label}')
    log(f'  本次新增: {new_count}')
    log(f'  视频总数: {len(all_videos)}')
    log(f'  总时长: {total_hours:.1f} 小时')
    log(f'  UP主数: {len(uppers)}')
    if durations:
        log(f'  平均时长: {sum(durations)/len(durations)/60:.1f} 分钟')

    # 按收藏夹统计
    from collections import Counter
    folder_counts = Counter(v['folder'] for v in all_videos)
    log(f'\n按收藏夹分布:')
    for fname, cnt in folder_counts.most_common():
        log(f'  {fname}: {cnt}')


if __name__ == '__main__':
    main()
