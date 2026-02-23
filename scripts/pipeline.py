#!/usr/bin/env python3
"""bili-transcripts 端到端自动化 Pipeline

流程: cookie_refresh → step1(增量) → step2(字幕) → step3(ASR, ≤30min) → step4(分类) → step5(docs) → mdx → git push → build
每 2 天由 cron 触发。全程日志记录。

ASR 额度耗尽时保存进度退出，下次继续。
无字幕且 >30min 的视频跳过 ASR；有字幕的长视频照常处理。
"""

import json
import base64
import fcntl
import os
import re
import subprocess
import sys
import time
import tempfile
import urllib.request
import urllib.error
from datetime import datetime
from pathlib import Path

# ── Paths ──
PROJECT = Path("/root/projects/bili-transcripts")
DOCS_V2 = Path("/root/projects/bili-docs-v2")
CREDS_PATH = PROJECT / "config/credentials.json"
LOG_DIR = PROJECT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_PATH = LOG_DIR / f"pipeline_{RUN_ID}.log"
PYTHON = str(PROJECT / ".venv/bin/python3")
SCRIPTS = PROJECT / "scripts"

# ── Helpers ──
def atomic_write_json(path, data, mode=0o644):
    """Atomic JSON write: tmp → fsync → replace."""
    fd = tempfile.NamedTemporaryFile(
        mode="w", dir=path.parent, suffix=".tmp", delete=False, prefix="."
    )
    try:
        json.dump(data, fd, ensure_ascii=False, indent=2)
        fd.flush()
        os.fsync(fd.fileno())
        fd.close()
        os.chmod(fd.name, mode)
        os.replace(fd.name, path)
    except Exception:
        fd.close()
        try:
            os.unlink(fd.name)
        except OSError:
            pass
        raise

# ── Logging ──
def log(msg, level="INFO"):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [{level}] {msg}"
    print(line, flush=True)
    with open(LOG_PATH, "a") as f:
        f.write(line + "\n")

def run_step(name, cmd, timeout=1800, soft_fail_patterns=None):
    """Run a pipeline step, return (success, stdout_tail).
    soft_fail_patterns: list of strings; if any appears in output, treat non-zero exit as soft success.
    """
    log(f"▶ {name}")
    start = time.time()
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout, cwd=str(PROJECT)
        )
        elapsed = time.time() - start
        # Log last 30 lines of output
        output = (result.stdout or "") + (result.stderr or "")
        tail = "\n".join(output.strip().split("\n")[-30:])
        with open(LOG_PATH, "a") as f:
            f.write(f"--- {name} output (last 30 lines) ---\n{tail}\n---\n")

        if result.returncode != 0:
            # Check if this is a recognized soft-fail case
            if soft_fail_patterns and any(p in output for p in soft_fail_patterns):
                log(f"⚠ {name} exited non-zero but matched soft-fail pattern ({elapsed:.1f}s)", "WARN")
                return True, tail
            log(f"✗ {name} failed (exit={result.returncode}, {elapsed:.1f}s)", "ERROR")
            return False, tail
        log(f"✓ {name} done ({elapsed:.1f}s)")
        return True, tail
    except subprocess.TimeoutExpired:
        log(f"✗ {name} timed out after {timeout}s", "ERROR")
        return False, ""
    except Exception as e:
        log(f"✗ {name} exception: {e}", "ERROR")
        return False, ""


# ── Step 0: Cookie Refresh ──
def refresh_cookie():
    """Check if cookie needs refresh, and refresh if needed."""
    log("Step 0: Cookie 检查与刷新")
    try:
        creds = json.load(open(CREDS_PATH))["bilibili"]
    except Exception as e:
        log(f"无法读取凭据: {e}", "ERROR")
        return False

    sessdata = creds.get("sessdata", "")
    bili_jct = creds.get("bili_jct", "")
    refresh_token = creds.get("refresh_token", "")

    if not sessdata or not refresh_token:
        log("缺少 SESSDATA 或 refresh_token，跳过刷新", "WARN")
        return True  # 继续尝试，可能还没过期

    cookies = {
        "SESSDATA": sessdata,
        "bili_jct": bili_jct,
        "buvid3": creds.get("buvid3", ""),
        "DedeUserID": creds.get("dedeuserid", ""),
    }
    cookie_str = "; ".join(f"{k}={v}" for k, v in cookies.items())

    # 1. Check if refresh needed
    try:
        req = urllib.request.Request(
            "https://passport.bilibili.com/x/passport-login/web/cookie/info",
            headers={"Cookie": cookie_str}
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
    except Exception as e:
        log(f"Cookie info 检查失败: {e}", "WARN")
        return True  # 网络问题，继续尝试

    if data.get("code") != 0:
        log(f"Cookie info 返回异常: code={data.get('code')}, msg={data.get('message')}", "WARN")
        # Cookie 可能已过期，尝试刷新
        need_refresh = True
        server_ts = int(time.time() * 1000)  # fallback to local time
    else:
        need_refresh = data.get("data", {}).get("refresh", False)
        server_ts = data.get("data", {}).get("timestamp", 0)
        if not server_ts:
            server_ts = int(time.time() * 1000)

    if not need_refresh:
        log("Cookie 有效，无需刷新")
        return True

    log("Cookie 需要刷新，开始刷新流程...")

    # 2. Generate CorrespondPath (use server timestamp per B站 API contract)
    try:
        correspond_path = _generate_correspond_path(server_ts)
    except Exception as e:
        log(f"生成 CorrespondPath 失败: {e}", "ERROR")
        return False

    # 3. Get refresh_csrf
    try:
        req = urllib.request.Request(
            f"https://www.bilibili.com/correspond/1/{correspond_path}",
            headers={"Cookie": cookie_str}
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            html = resp.read().decode("utf-8")
        m = re.search(r'<div\s+id="1-name"[^>]*>([^<]+)</div>', html)
        if not m:
            log("无法从 HTML 提取 refresh_csrf", "ERROR")
            return False
        refresh_csrf = m.group(1).strip()
    except Exception as e:
        log(f"获取 refresh_csrf 失败: {e}", "ERROR")
        return False

    # 4. Refresh cookie
    try:
        post_data = urllib.parse.urlencode({
            "csrf": bili_jct,
            "refresh_csrf": refresh_csrf,
            "source": "main_web",
            "refresh_token": refresh_token,
        }).encode()
        req = urllib.request.Request(
            "https://passport.bilibili.com/x/passport-login/web/cookie/refresh",
            data=post_data,
            headers={"Cookie": cookie_str, "Content-Type": "application/x-www-form-urlencoded"}
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
            # Extract new cookies from Set-Cookie headers
            new_cookies = {}
            for header in resp.headers.get_all("Set-Cookie") or []:
                for part in header.split(";"):
                    part = part.strip()
                    if "=" in part:
                        k, v = part.split("=", 1)
                        if k in ("SESSDATA", "bili_jct", "DedeUserID", "sid"):
                            new_cookies[k] = v
    except Exception as e:
        log(f"刷新 Cookie 失败: {e}", "ERROR")
        return False

    if result.get("code") != 0:
        log(f"刷新 Cookie 返回异常: {result}", "ERROR")
        return False

    new_refresh_token = result.get("data", {}).get("refresh_token", "")
    if not new_refresh_token:
        log("刷新成功但未返回新 refresh_token", "WARN")

    # 5. Confirm refresh (with new cookie, old refresh_token)
    new_cookie_str = "; ".join(f"{k}={v}" for k, v in new_cookies.items()) if new_cookies else cookie_str
    try:
        post_data = urllib.parse.urlencode({
            "csrf": new_cookies.get("bili_jct", bili_jct),
            "refresh_token": refresh_token,  # old token
        }).encode()
        req = urllib.request.Request(
            "https://passport.bilibili.com/x/passport-login/web/confirm/refresh",
            data=post_data,
            headers={"Cookie": new_cookie_str, "Content-Type": "application/x-www-form-urlencoded"}
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            confirm = json.loads(resp.read())
    except Exception as e:
        log(f"确认刷新失败: {e}", "WARN")
        # 不阻塞，新 cookie 可能已经生效

    # 6. Update credentials.json (with file lock to prevent concurrent clobber)
    lock_path = CREDS_PATH.with_suffix(".lock")
    try:
        lock_fd = open(lock_path, "w")
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (IOError, OSError):
        log("凭据文件被锁定（另一个进程正在写入），跳过更新", "WARN")
        return False
    try:
        all_creds = json.load(open(CREDS_PATH))
        if new_cookies.get("SESSDATA"):
            all_creds["bilibili"]["sessdata"] = new_cookies["SESSDATA"]
        if new_cookies.get("bili_jct"):
            all_creds["bilibili"]["bili_jct"] = new_cookies["bili_jct"]
        if new_refresh_token:
            all_creds["bilibili"]["refresh_token"] = new_refresh_token
        # Atomic write with secure permissions
        fd = tempfile.NamedTemporaryFile(
            mode="w", dir=CREDS_PATH.parent, suffix=".tmp",
            delete=False, prefix=".creds_"
        )
        try:
            json.dump(all_creds, fd, ensure_ascii=False, indent=2)
            fd.flush()
            os.fsync(fd.fileno())
            fd.close()
            os.chmod(fd.name, 0o600)
            os.replace(fd.name, CREDS_PATH)
        except Exception:
            os.unlink(fd.name)
            raise
        log("Cookie 刷新成功，凭据已更新")
        return True
    except Exception as e:
        log(f"更新凭据文件失败: {e}", "ERROR")
        return False
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        lock_fd.close()


def _generate_correspond_path(ts_ms):
    """Generate CorrespondPath using RSA encryption of timestamp."""
    # Use openssl for RSA encryption (avoid external Python deps)
    pubkey = (
        "-----BEGIN PUBLIC KEY-----\n"
        "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDLgd2OAkcGVtoE3ThUREbio0Eg\n"
        "Uc/prcajMKXvkCKFCWhJYJcLkcM2DKKcSeFpD/j6Boy538YXnR6VhcuUJOhH2x71\n"
        "nzPjfdTcqMz7djHKETQGDQIor3LLFqc1wc7GbsTENBMOx9dtA737KjAKc4PUjHPY\n"
        "djCqUiIdQJAN0HCmAQIDAQAB\n"
        "-----END PUBLIC KEY-----"
    )
    plaintext = f"refresh_{ts_ms}"
    # Write pubkey to temp file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".pem", delete=False) as f:
        f.write(pubkey)
        pubkey_path = f.name
    try:
        result = subprocess.run(
            ["openssl", "rsautl", "-encrypt", "-pubin", "-inkey", pubkey_path],
            input=plaintext.encode(), capture_output=True, timeout=5
        )
        if result.returncode != 0:
            # Try pkeyutl (newer openssl)
            result = subprocess.run(
                ["openssl", "pkeyutl", "-encrypt", "-pubin", "-inkey", pubkey_path],
                input=plaintext.encode(), capture_output=True, timeout=5
            )
        if result.returncode != 0:
            raise RuntimeError(f"openssl failed: {result.stderr.decode()[:200]}")
        return base64.b16encode(result.stdout).decode().lower()
    finally:
        os.unlink(pubkey_path)


# ── Step 3 wrapper: filter >30min no-subtitle videos ──
def prepare_asr_queue():
    """Generate ASR queue: no-subtitle videos ≤30min."""
    log("准备 ASR 队列（无字幕 ≤30min）")
    no_sub_path = PROJECT / "data/raw/no_subtitle.json"
    if not no_sub_path.exists():
        log("no_subtitle.json 不存在，跳过 ASR", "WARN")
        return False

    try:
        with open(no_sub_path) as f:
            data = json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        log(f"no_subtitle.json 解析失败: {e}", "ERROR")
        return False

    videos = data.get("videos", [])
    max_dur = 1800  # 30 minutes
    filtered = []
    dropped = 0
    for v in videos:
        try:
            dur = int(v.get("duration", 0) or 0)
            if dur <= max_dur:
                filtered.append(v)
        except (ValueError, TypeError):
            dropped += 1
    skipped = len(videos) - len(filtered) - dropped

    queue_path = PROJECT / "data/raw/asr_queue.json"
    atomic_write_json(queue_path, {"videos": filtered, "meta": {
        "total": len(filtered),
        "skipped_over_30m": skipped,
        "dropped_invalid": dropped,
        "generated_at": datetime.now().isoformat(),
        "max_duration_sec": max_dur,
    }})

    log(f"ASR 队列: {len(filtered)} 个视频（跳过 {skipped} 个 >30min, {dropped} 个无效）")
    return len(filtered) > 0


# ── Step 4 unified: classify both subtitle + ASR transcripts ──
def has_new_transcripts():
    """Check if there are unclassified transcripts."""
    progress_path = PROJECT / "data/classified/.progress.json"
    progress_asr_path = PROJECT / "data/classified/.progress_asr.json"

    existing = set()
    for p in [progress_path, progress_asr_path]:
        if p.exists():
            with open(p) as f:
                existing.update(json.load(f).keys())

    for d in ["transcripts", "transcripts_asr"]:
        tdir = PROJECT / "data" / d
        if tdir.exists():
            for f in tdir.iterdir():
                if f.suffix == ".txt" and f.stem not in existing:
                    return True
    return False


# ── Git push ──
def git_push():
    """Commit and push docs changes to GitHub."""
    log("Git commit & push")
    git_cwd = str(DOCS_V2)

    # Check for uncommitted changes
    result = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True, cwd=git_cwd)
    has_changes = bool(result.stdout.strip())

    if has_changes:
        changed_files = len(result.stdout.strip().split("\n"))
        ts = datetime.now().strftime("%Y-%m-%d %H:%M")
        subprocess.run(["git", "add", "-A"], check=True, cwd=git_cwd)
        subprocess.run(
            ["git", "commit", "-m", f"feat: auto-update {changed_files} docs ({ts})"],
            check=True, capture_output=True, cwd=git_cwd
        )
        log(f"已提交 {changed_files} 个文件变更")

    # Check if ahead of remote (covers previous failed pushes too)
    ahead_result = subprocess.run(
        ["git", "rev-list", "--count", "@{u}..HEAD"],
        capture_output=True, text=True, cwd=git_cwd
    )
    ahead = int(ahead_result.stdout.strip()) if ahead_result.returncode == 0 else 0

    if ahead == 0:
        log("无需推送（本地与远程同步）")
        return True

    result = subprocess.run(["git", "push"], capture_output=True, text=True, timeout=60, cwd=git_cwd)
    if result.returncode != 0:
        log(f"git push 失败: {result.stderr}", "ERROR")
        return False
    log(f"已推送 {ahead} 个 commit")
    return True


# ── Build & Deploy ──
def build_and_deploy():
    """Run deploy.sh to rebuild and restart the docs site."""
    log("Build & Deploy")
    deploy_script = DOCS_V2 / "deploy.sh"
    if not deploy_script.exists():
        log("deploy.sh 不存在", "ERROR")
        return False

    ok, tail = run_step("deploy.sh", ["bash", str(deploy_script)], timeout=600)
    return ok


# ── Main Pipeline ──
def main():
    log("=" * 60)
    log(f"Pipeline 启动 (run_id={RUN_ID})")
    log("=" * 60)

    start = time.time()
    started_at = datetime.now().isoformat()
    results = {}

    # Step 0: Cookie refresh
    results["cookie_refresh"] = refresh_cookie()
    if not results["cookie_refresh"]:
        log("Cookie 刷新失败，尝试继续（可能还有效）", "WARN")

    # Step 1: Fetch metadata (incremental)
    ok, _ = run_step("Step 1: 增量元数据采集",
                      [PYTHON, str(SCRIPTS / "step1_fetch_metadata.py")],
                      timeout=600)
    results["step1"] = ok
    if not ok:
        log("Step 1 失败，Pipeline 终止", "ERROR")
        write_summary(results, start, started_at)
        return 1

    # Step 2: Extract subtitles
    ok, _ = run_step("Step 2: 字幕提取",
                      [PYTHON, str(SCRIPTS / "step2_extract_subtitles.py")],
                      timeout=3600)
    results["step2"] = ok
    if not ok:
        log("Step 2 失败，Pipeline 终止", "ERROR")
        write_summary(results, start, started_at)
        return 1

    # Step 3: ASR (optional, quota-aware)
    if prepare_asr_queue():
        ok, tail = run_step("Step 3: ASR 转写",
                            [PYTHON, str(SCRIPTS / "step3_asr_groq.py")],
                            timeout=7200,
                            soft_fail_patterns=["All keys exhausted"])
        results["step3"] = ok
        if "All keys exhausted" in tail:
            log("ASR 额度耗尽，已保存进度，下次继续", "WARN")
    else:
        results["step3"] = True
        log("无需 ASR 处理")

    # Step 4: Classify (subtitle transcripts)
    if has_new_transcripts():
        ok, _ = run_step("Step 4a: 分类（字幕文稿）",
                          [PYTHON, str(SCRIPTS / "step4_classify.py")],
                          timeout=3600)
        results["step4a"] = ok

        # Step 4b: Classify ASR transcripts (soft-fail if no ASR transcripts exist yet)
        ok, _ = run_step("Step 4b: 分类（ASR 文稿）",
                          [PYTHON, str(SCRIPTS / "step4_classify_asr.py")],
                          timeout=3600,
                          soft_fail_patterns=["All classified", "no metadata"])
        results["step4b"] = ok
    else:
        results["step4a"] = True
        results["step4b"] = True
        log("无新文稿需要分类")

    # Step 5: Generate docs (only if classification succeeded)
    if results.get("step4a", True):
        ok, _ = run_step("Step 5: 文档库生成",
                          [PYTHON, str(SCRIPTS / "step5_generate_docs.py")],
                          timeout=300)
        results["step5"] = ok
    else:
        log("Step 4a 分类失败，跳过文档生成以避免发布残缺数据", "WARN")
        results["step5"] = False

    # Step 6: Generate MDX for Fumadocs (only if step5 succeeded)
    if results.get("step5"):
        ok, _ = run_step("Step 6: MDX 生成",
                          [PYTHON, str(DOCS_V2 / "scripts/generate-mdx.py")],
                          timeout=300)
        results["step6_mdx"] = ok
    else:
        log("Step 5 失败，跳过 MDX 生成", "WARN")
        results["step6_mdx"] = False

    # Step 7: Git push
    if results.get("step6_mdx"):
        try:
            results["step7_git"] = git_push()
        except Exception as e:
            log(f"Git push 异常: {e}", "ERROR")
            results["step7_git"] = False
    else:
        log("MDX 生成失败，跳过 git push", "WARN")
        results["step7_git"] = False

    # Step 8: Build & Deploy
    if results.get("step7_git"):
        results["step8_deploy"] = build_and_deploy()
    else:
        log("Git push 失败/跳过，跳过 build", "WARN")
        results["step8_deploy"] = False

    write_summary(results, start, started_at)
    return 0 if all(results.values()) else 1


def write_summary(results, start, started_at):
    elapsed = time.time() - start
    log("=" * 60)
    log(f"Pipeline 完成 ({elapsed:.0f}s)")
    for step, ok in results.items():
        status = "✓" if ok else "✗"
        log(f"  {status} {step}")
    log(f"日志: {LOG_PATH}")
    log("=" * 60)

    # Write machine-readable summary (atomic)
    summary_path = PROJECT / "data/raw/.pipeline_last_run.json"
    summary = {
        "run_id": RUN_ID,
        "started_at": started_at,
        "elapsed_seconds": round(elapsed),
        "results": {k: v for k, v in results.items()},
        "all_ok": all(results.values()),
        "log_path": str(LOG_PATH),
    }
    atomic_write_json(summary_path, summary)


if __name__ == "__main__":
    sys.exit(main())
