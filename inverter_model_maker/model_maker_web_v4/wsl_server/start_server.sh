#!/usr/bin/env bash
# Qwen3-VL WSL 추론 서버 시작 스크립트
# Windows bat에서: start "QwenVL-WSL" wsl -e bash /mnt/c/.../.../start_server.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PORT="${QWEN_SERVER_PORT:-8084}"
MODEL_PATH="${QWEN_MODEL_PATH:-/mnt/c/models/Qwen3-VL-32B-4bit}"

echo "========================================"
echo "  Qwen3-VL WSL 추론 서버"
echo "  포트: $PORT"
echo "  모델: $MODEL_PATH"
echo "========================================"

# pip 의존성 설치 (최초 1회 또는 업데이트 시)
if ! python3 -c "import fastapi, autoawq" 2>/dev/null; then
    echo "[설치] 패키지 설치 중..."
    pip3 install -r requirements.txt -q
fi

echo "[시작] uvicorn 서버 실행 중..."
export QWEN_MODEL_PATH="$MODEL_PATH"
export QWEN_SERVER_PORT="$PORT"

exec python3 -m uvicorn qwen_server:app --host 0.0.0.0 --port "$PORT"
