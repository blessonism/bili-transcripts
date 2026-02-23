#!/bin/bash
# ASR Runner — loops until daily limit exhausted
# Runs step3_asr_groq.py, waits for hourly reset, repeats
# Stops when: all keys daily-exhausted OR no remaining videos

PROJECT=/root/projects/bili-transcripts
LOG=/tmp/asr-runner.log
MAX_ROUNDS=24  # safety cap: max 24 rounds (~24h)
WAIT_SEC=3660  # wait 61 min between rounds for hourly reset

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG"; }

log "=== ASR Runner started ==="

for round in $(seq 1 $MAX_ROUNDS); do
    log "--- Round $round/$MAX_ROUNDS ---"
    
    # Run ASR
    cd "$PROJECT"
    HTTPS_PROXY=http://127.0.0.1:7890 HTTP_PROXY=http://127.0.0.1:7890 \
        .venv/bin/python3 scripts/step3_asr_groq.py >> "$LOG" 2>&1
    EXIT_CODE=$?
    
    # Check result
    LAST_LINE=$(tail -5 "$LOG" | grep -o 'this_run=[0-9]*' | head -1)
    THIS_RUN=$(echo "$LAST_LINE" | grep -oP '\d+')
    
    # Check if all keys daily exhausted
    if tail -20 "$LOG" | grep -q 'All keys exhausted'; then
        # Check if it's daily (not just hourly)
        DAILY_HOURS=$(tail -10 "$LOG" | grep -oP 'daily=\K[0-9.]+' | sort -rn | head -1)
        if [ -n "$DAILY_HOURS" ] && [ "$(echo "$DAILY_HOURS > 6.5" | bc)" = "1" ]; then
            log "Daily limit reached (max daily=${DAILY_HOURS}h). Done for today."
            break
        fi
    fi
    
    # If this_run=0 and no keys exhausted, nothing left to do
    if [ "$THIS_RUN" = "0" ] && ! tail -20 "$LOG" | grep -q 'exhausted'; then
        log "No more videos to process. Done."
        break
    fi
    
    # If this_run=0 and keys exhausted, it's hourly — wait and retry
    if [ "$THIS_RUN" = "0" ] || tail -20 "$LOG" | grep -q 'All keys exhausted'; then
        log "Hourly limit hit. Waiting ${WAIT_SEC}s for reset..."
        sleep $WAIT_SEC
    else
        # Ran some but didn't exhaust — small pause then continue
        sleep 10
    fi
done

log "=== ASR Runner finished ==="

# Summary
OK=$(grep -c '" ok |' "$LOG" 2>/dev/null || echo 0)
ERR=$(grep -c 'download failed' "$LOG" 2>/dev/null || echo 0)
TOTAL_FILES=$(ls "$PROJECT/data/transcripts_asr/" 2>/dev/null | wc -l)
log "Summary: new_ok=$OK new_err=$ERR total_asr_files=$TOTAL_FILES"
