@echo off
chcp 65001 > nul
title Model Maker Web v2

cd /d "%~dp0.."
echo ============================================
echo   Model Maker Web v2
echo   Stage 1→2→3 파이프라인 웹 UI
echo ============================================
echo.

echo [1/3] 캐시 정리 중...
for /d /r "model_maker_web_v2" %%d in (__pycache__) do @if exist "%%d" rd /s /q "%%d"
echo   __pycache__ 삭제 완료

echo [2/3] 의존성 확인 중...
pip install fastapi uvicorn python-multipart openpyxl PyMuPDF -q 2>nul

echo [3/3] 서버 시작 중...
echo.
echo   http://localhost:8082
echo.
echo   PDF 업로드 → Stage 1 추출 → Stage 2 필터링 → Stage 3 코드 생성
echo   Ctrl+C로 종료
echo.

start "" http://localhost:8082
python -m uvicorn model_maker_web_v2.backend.main:app --host 0.0.0.0 --port 8082 --reload
pause
