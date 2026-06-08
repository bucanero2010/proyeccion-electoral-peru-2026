#!/bin/bash
# Run the segunda vuelta scraper + projection every 10 minutes and push.
# Vercel auto-deploys on push.
#
# Usage: ./run_loop_2v.sh [INTERVAL_SECONDS]
#   ./run_loop_2v.sh        -> 10 min loop (default)
#   ./run_loop_2v.sh 300    -> 5 min loop
#
# Stop with Ctrl+C.

set -u

# Move to repo root regardless of where the script is invoked from
cd "$(dirname "$0")"

INTERVAL=${1:-600}  # default: 600s = 10 min

# Don't try to push if the working tree is on a detached HEAD or unexpected branch.
BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")
if [ "$BRANCH" != "main" ]; then
    echo "WARNING: current branch is '$BRANCH', not 'main'. Pushes will go to that branch."
fi

echo "Loop starting. Interval: $((INTERVAL / 60)) min. Branch: $BRANCH."
echo "Stop with Ctrl+C."

cycle=0
while true; do
    cycle=$((cycle + 1))
    echo ""
    echo "=================================================="
    echo "Cycle $cycle | $(date '+%Y-%m-%d %H:%M:%S')"
    echo "=================================================="

    # 1) Scrape (skips districts already at 100% from previous CSV)
    if ! python3 -u -m src.scraper_2v; then
        echo "Scraper failed. Skipping push this cycle."
        sleep "$INTERVAL"
        continue
    fi

    # 2) Trim older CSVs once we have a non-empty fresh one (keeps repo lean)
    LATEST=$(ls -t data/resultados_2v_*.csv 2>/dev/null | head -1)
    if [ -n "${LATEST:-}" ] && [ "$(wc -l < "$LATEST")" -gt 100 ]; then
        for f in data/resultados_2v_*.csv; do
            [ "$f" != "$LATEST" ] && rm -f "$f"
        done
    else
        echo "Latest CSV looks empty/small. Keeping previous data."
        [ -n "${LATEST:-}" ] && rm -f "$LATEST"
        sleep "$INTERVAL"
        continue
    fi

    # 3) Project (regenerates summary_2v.json + appends snapshot row)
    if ! python3 -u -m src.projection_2v; then
        echo "Projection failed. Skipping push this cycle."
        sleep "$INTERVAL"
        continue
    fi

    # 4) Stage just the data we generate
    git add data/resultados_2v_*.csv data/summary_2v.json data/snapshots_2v.csv 2>/dev/null

    # Bail early if nothing to commit
    if git diff --cached --quiet; then
        echo "No data changes this cycle; nothing to push."
    else
        STAMP=$(date '+%Y-%m-%d %H:%M')
        if git commit -m "data(2v): update results + projection $STAMP" >/dev/null 2>&1; then
            if git push origin "$BRANCH" 2>&1 | sed 's/^/  /'; then
                echo "Pushed to origin/$BRANCH. Vercel will redeploy."
            else
                echo "Push failed (network or auth). Will retry next cycle."
            fi
        else
            echo "Nothing committed (no diff after staging)."
        fi
    fi

    echo "Next run in $((INTERVAL / 60)) min ($(date -v +"$((INTERVAL / 60))M" '+%H:%M' 2>/dev/null || date -d "+$INTERVAL seconds" '+%H:%M' 2>/dev/null || echo unknown))."
    sleep "$INTERVAL"
done
