import json
import httpx

with open('/root/projects/bili-transcripts/config/credentials.json') as f:
    creds = json.load(f)['bilibili']

cookies = {
    'SESSDATA': creds['sessdata'],
    'bili_jct': creds['bili_jct'],
    'buvid3': creds['buvid3'],
    'DedeUserID': creds['dedeuserid'],
}
media_id = creds['media_id']

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': 'https://www.bilibili.com',
}

with httpx.Client(cookies=cookies, headers=headers, timeout=15) as client:
    # 1. 验证登录态
    r = client.get('https://api.bilibili.com/x/web-interface/nav')
    nav = r.json()
    if nav['code'] == 0:
        uname = nav['data']['uname']
        mid = nav['data']['mid']
        print(f'✅ 登录验证通过: {uname} (UID: {mid})')
    else:
        print(f'❌ 登录失败: {nav}')
        exit(1)

    # 2. 拉取收藏夹第一页
    r = client.get('https://api.bilibili.com/x/v3/fav/resource/list', params={
        'media_id': media_id,
        'pn': 1,
        'ps': 20,
        'platform': 'web',
    })
    fav = r.json()
    if fav['code'] == 0:
        info = fav['data']['info']
        total = info['media_count']
        title = info['title']
        print(f'✅ 收藏夹: "{title}" | 共 {total} 个视频')
        # 打印前3个视频标题
        for i, item in enumerate(fav['data']['medias'][:3]):
            print(f'   [{i+1}] {item["title"]} (BV{item["bvid"] if "bvid" in item else item["id"]})')
        print(f'   ... 共 {total} 个')
    else:
        print(f'❌ 收藏夹获取失败: {fav}')
        exit(1)
