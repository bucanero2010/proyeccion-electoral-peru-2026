#!/bin/bash
# Run scraper every 30 minutes and push to GitHub.
# Streamlit Cloud auto-reloads on push.
#
# Usage: ./run_loop.sh
# Stop with Ctrl+C

INTERVAL=1800  # 30 minutes in seconds

while true; do
    echo ""
    echo "=========================================="
    echo "$(date): Starting scraper..."
    echo "=========================================="

    # Run scraper
    python3 -m src.scraper

    # Only keep the latest CSV (delete older ones to avoid bloating the repo)
    LATEST=$(ls -t data/resultados_presidenciales_*.csv 2>/dev/null | head -1)
    if [ -n "$LATEST" ]; then
        for f in data/resultados_presidenciales_*.csv; do
            [ "$f" != "$LATEST" ] && rm -f "$f"
        done
    fi

    # Commit and push
    git add data/
    git commit -m "data: update results $(date '+%Y-%m-%d %H:%M')" 2>/dev/null

    if git push 2>/dev/null; then
        echo "$(date): Pushed to GitHub. Streamlit will reload."
    else
        echo "$(date): Push failed (no changes or network issue)."
    fi

    echo "$(date): Next run in $((INTERVAL / 60)) minutes..."
    sleep $INTERVAL
done
