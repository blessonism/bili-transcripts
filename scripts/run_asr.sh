#!/bin/bash
# Daily ASR runner — called by cron or manual
# On completion: writes summary to /tmp/bili-asr-result.txt
# OpenClaw heartbeat picks it up and reports to Boss

LOCK=/tmp/bili-asr.lock
if [ -f "$LOCK" ] && kill -0 $(cat "$LOCK") 2>/dev/null; then
    echo "Already running (PID $(cat $LOCK)), skip"
    exit 0
fi

cd /root/projects/bili-transcripts
source .venv/bin/activate
export HTTPS_PROXY=http://127.0.0.1:7890
export HTTP_PROXY=http://127.0.0.1:7890

echo $$ > "$LOCK"
python3 scripts/step3_asr_groq.py >> /tmp/bili-asr-daily.log 2>&1
RET=$?
rm -f "$LOCK"

# Generate failure report
python3 scripts/gen_failure_report.py >> /tmp/bili-asr-daily.log 2>&1

# Write result file for OpenClaw to pick up
SUMMARY=$(grep -E "Run done|Key \.\.| total_files|All keys|10 consecutive" /tmp/bili-asr-daily.log | tail -8)
echo "exit=$RET time=$(date -u +%Y-%m-%dT%H:%M:%SZ)" > /tmp/bili-asr-result.txt
echo "$SUMMARY" >> /tmp/bili-asr-result.txt

exit $RET
