# bili-transcripts

B 站收藏夹视频自动化文稿提取 Pipeline。从收藏夹拉取视频元数据，提取字幕/ASR 转写，AI 分类，生成结构化文档库，最终输出为 Fumadocs 兼容的 MDX 文件并自动部署。

## 工作流

```
Cookie 自动刷新 → 增量元数据采集 → 字幕提取 → ASR 转写 → AI 分类 → 文档生成 → MDX 生成 → Git Push → Build & Deploy
```

### 各步骤说明

| 步骤 | 脚本 | 说明 |
|------|------|------|
| Step 0 | `pipeline.py` 内置 | 检查 B 站 Cookie 有效性，过期时自动刷新（需 `refresh_token`） |
| Step 1 | `step1_fetch_metadata.py` | 增量拉取收藏夹视频元数据，支持 `--full` 全量模式 |
| Step 2 | `step2_extract_subtitles.py` | 提取 B 站 AI 字幕（支持断点续传、多 P 视频） |
| Step 3 | `step3_asr_groq.py` | 无字幕视频通过 Groq Whisper ASR 转写（≤30min，额度耗尽自动暂停） |
| Step 4 | `step4_classify.py` | 使用 LLM 对有文稿的视频进行分类（8 主分类 × 26 子分类） |
| Step 5 | `step5_generate_docs.py` | 按分类生成结构化 Markdown 文档库 |
| Pipeline | `pipeline.py` | 端到端编排，串联所有步骤，含日志、错误处理、Cron 支持 |

### 特性

- **增量采集**：基于 `fav_time` 截断，只拉取新收藏的视频
- **Cookie 自动续期**：利用 B 站官方刷新 API + `refresh_token` 自动续期
- **ASR 额度感知**：Groq API 额度耗尽时保存进度退出，下次自动继续
- **断点续传**：每个步骤都有 checkpoint 机制，中断后可安全重启
- **原子写入**：所有关键文件使用 `write → fsync → rename` 模式，防止崩溃损坏
- **过滤规则**：无字幕且 >30min 的视频跳过 ASR；有字幕的长视频照常处理

## 快速开始

### 1. 环境准备

```bash
# Python 3.10+
python3 -m venv .venv
source .venv/bin/activate  # bash/zsh
pip install httpx groq
```

ASR 步骤还需要 `yt-dlp`：

```bash
pip install yt-dlp
```

### 2. 配置凭据

```bash
cp config/credentials.example.json config/credentials.json
```

编辑 `config/credentials.json`，填入你的凭据：

#### B 站凭据获取

1. 浏览器登录 B 站
2. F12 → Application → Cookies → `https://www.bilibili.com`
3. 复制以下字段：

| 字段 | 来源 | 说明 |
|------|------|------|
| `sessdata` | Cookie: `SESSDATA` | HttpOnly，需从 DevTools Application 面板获取 |
| `bili_jct` | Cookie: `bili_jct` | CSRF token |
| `buvid3` | Cookie: `buvid3` | 设备标识 |
| `dedeuserid` | Cookie: `DedeUserID` | 用户 UID |
| `refresh_token` | Console: `localStorage.getItem('ac_time_value')` | Cookie 自动刷新必需 |
| `media_id` | 收藏夹 URL 中的 `fid` 参数 | 默认收藏夹 ID |

#### Groq API Key（ASR 用）

1. 注册 [Groq Console](https://console.groq.com/)
2. 创建 API Key
3. 填入 `groq.api_key`（或多个 key 填入 `groq.api_keys` 数组以轮换使用）

#### 分类 API（Step 4 用）

支持任何 OpenAI 兼容 API（OpenAI、Gemini、Claude 等）：

| 字段 | 说明 |
|------|------|
| `classify.api_url` | API endpoint（如 `https://api.openai.com/v1/chat/completions`） |
| `classify.api_key` | API Key |
| `classify.model` | 模型名（如 `gpt-4o-mini`、`gemini-2.0-flash`） |

### 3. 首次运行

```bash
# 全量采集（首次必须，建立基线）
.venv/bin/python3 scripts/step1_fetch_metadata.py --full

# 或直接跑完整 pipeline
.venv/bin/python3 scripts/pipeline.py
```

### 4. 定时运行（Cron）

```bash
# 每 2 天凌晨 4:00 自动执行
0 20 */2 * * cd /path/to/bili-transcripts && .venv/bin/python3 scripts/pipeline.py >> /tmp/bili-pipeline.log 2>&1
```

### 5. 存量 ASR 清理（可选）

首次部署时，如果有大量无字幕视频需要 ASR 转写，`pipeline.py` 单次运行可能因 Groq 额度限制无法处理完。
`asr_runner.sh` 是一个一次性循环脚本，专门用于清理存量：

```bash
# 后台启动，自动循环直到当天额度用完
nohup bash scripts/asr_runner.sh &
```

工作方式：
- 循环调用 `step3_asr_groq.py`，每轮之间等待 61 分钟（Groq 小时级额度重置）
- 检测到当天所有 key 的 daily limit 用完后自动退出
- 所有视频处理完毕也会自动退出
- 最多跑 24 轮（安全上限，约 24 小时）
- 日志输出到 `/tmp/asr-runner.log`

> 存量清完后不需要再跑这个脚本，日常增量由 `pipeline.py` 的 cron 任务处理。

### 6. 快速导入单个视频

看到一个好视频，想立刻导出文案到文稿库：

```bash
# 直接传 BV 号或完整 URL
.venv/bin/python3 scripts/quick_import.py BV1xxxxxx
.venv/bin/python3 scripts/quick_import.py https://www.bilibili.com/video/BV1xxxxxx

# 仅提取+分类，不触发部署
.venv/bin/python3 scripts/quick_import.py BV1xxxxxx --no-deploy

# 强制走 ASR（即使有字幕）
.venv/bin/python3 scripts/quick_import.py BV1xxxxxx --force-asr
```

流程：获取元数据 → 提取字幕/ASR → AI 分类 → 追加到 classification.json → 重建 MDX → Git Push → Build & Deploy。全程约 3-5 分钟。

## 目录结构

```
bili-transcripts/
├── scripts/
│   ├── pipeline.py              # 端到端编排入口
│   ├── step1_fetch_metadata.py  # 增量/全量元数据采集
│   ├── step2_extract_subtitles.py # B 站 AI 字幕提取
│   ├── step3_asr_groq.py       # Groq Whisper ASR 转写
│   ├── step4_classify.py       # LLM 分类（字幕文稿）
│   ├── step4_classify_asr.py   # LLM 分类（ASR 文稿）
│   ├── step5_generate_docs.py  # Markdown 文档库生成
│   ├── wbi.py                  # B 站 WBI 签名工具
│   ├── asr_runner.sh           # 存量 ASR 清理循环脚本（一次性使用）
│   └── quick_import.py         # 单视频快速导入（手动触发）
├── config/
│   ├── credentials.json         # 凭据（gitignore）
│   └── credentials.example.json # 凭据模板
├── data/                        # 运行时数据（gitignore）
│   ├── raw/                     # 元数据、状态文件
│   ├── transcripts/             # B 站 AI 字幕
│   ├── transcripts_asr/         # ASR 转写文稿
│   └── classified/              # 分类结果
├── logs/                        # 运行日志（gitignore）
├── .gitignore
└── README.md
```

## 分类体系

| 主分类 | 子分类 |
|--------|--------|
| 人情世故 | 社交沟通、饭局酒局、职场人际、恋爱关系 |
| 职业发展 | 求职面试、创业商业、职场成长 |
| 认知成长 | 思维方法、心理自我、哲学思辨 |
| 技术工具 | 编程开发、AI 应用、效率工具、设计创作 |
| 学业考试 | 公考、学术科研、大学课程 |
| 影视娱乐 | 影视解说、综艺脱口秀、音乐 ASMR |
| 生活方式 | 美食烹饪、数码好物、生活技巧、摄影拍照 |
| 深度内容 | 人物访谈、行业洞察、投资理财 |

## 文档站部署（可选）

Pipeline 生成的文档可以配合 [Fumadocs](https://fumadocs.vercel.app/) 部署为文档站。需要额外的 `generate-mdx.py` 脚本将分类结果转换为 MDX 文件。

## License

MIT
