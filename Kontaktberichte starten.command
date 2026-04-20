#!/bin/zsh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
APP_FILE="$SCRIPT_DIR/app.py"
PORT="8501"
URL="http://localhost:${PORT}"

cd "$SCRIPT_DIR"

if ! command -v python3 >/dev/null 2>&1; then
  osascript -e 'display alert "Python 3 nicht gefunden" message "Bitte Python 3 installieren oder im PATH verfügbar machen." as critical'
  exit 1
fi

if ! python3 -c "import streamlit" >/dev/null 2>&1; then
  osascript -e 'display alert "Streamlit nicht gefunden" message "Bitte zuerst die Abhängigkeiten installieren." as critical'
  exit 1
fi

python3 -m streamlit run "$APP_FILE" \
  --server.headless true \
  --server.port "$PORT" \
  --browser.serverAddress localhost &

STREAMLIT_PID=$!
trap 'kill "$STREAMLIT_PID" >/dev/null 2>&1 || true' EXIT

for _ in {1..60}; do
  if curl -s "http://127.0.0.1:${PORT}/_stcore/health" >/dev/null 2>&1; then
    open "$URL"
    wait "$STREAMLIT_PID"
    exit 0
  fi
  sleep 1
done

osascript -e 'display alert "Streamlit konnte nicht gestartet werden" message "Die App hat innerhalb von 60 Sekunden nicht auf localhost:8501 reagiert." as critical'
exit 1
