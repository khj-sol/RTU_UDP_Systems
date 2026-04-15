@echo off
chcp 65001 > nul
title Model Maker Web v4 - Phi + Qwen3-VL - Stage 1/2/3
cd /d "%~dp0"

set "PYTHON=C:\Program Files\Python312\python.exe"
if not exist "%PYTHON%" set "PYTHON=python"

echo ============================================
echo   Model Maker Web v4  -  http://localhost:8083
echo   Phi-mini-MoE + Qwen3-VL (HuggingFace)
echo   Stage 1 -^> Stage 2 -^> Stage 3
echo ============================================
echo.

echo [1/4] Clearing pycache...
for /d /r "model_maker_web_v4" %%d in (__pycache__) do @if exist "%%d" rd /s /q "%%d"

echo [2/4] Installing dependencies...
"%PYTHON%" -m pip install fastapi "uvicorn[standard]" python-multipart openpyxl PyMuPDF transformers torch accelerate bitsandbytes Pillow websockets -q

echo [3/4] Checking HuggingFace models...
echo   Note: Phi-4-mini-instruct and Qwen3-VL-32B-4bit should be downloaded to:
echo   - C:/models/Phi-4-mini-instruct
echo   - C:/models/Qwen3-VL-32B-4bit
echo   If not available, models will be downloaded on first use (requires ~30GB disk space).
echo.

echo [4/4] Starting server...
echo.
start "" http://localhost:8083

:LOOP
"%PYTHON%" -m uvicorn model_maker_web_v4.backend.main:app --host 0.0.0.0 --port 8083 --reload --reload-dir model_maker_web_v4/backend --reload-dir model_maker_web_v4/static
echo.
echo [%date% %time%] Server stopped. Restarting in 5s... (Ctrl+C to quit)
timeout /t 5 /nobreak >nul
goto LOOP
