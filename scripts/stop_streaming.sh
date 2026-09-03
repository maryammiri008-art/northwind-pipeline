#!/usr/bin/env bash
set +e
cd "$PROJECT_HOME" || cd "$HOME/de_final_project"

for f in logs/producer.pid logs/consumer.pid; do
    if [ -f "$f" ]; then
        kill "$(cat "$f")" 2>/dev/null
        rm -f "$f"
    fi
done

echo "✅ Streaming processes stopped."
