@echo off
chcp 65001 > nul
title Model Maker Web v4 - Nemotron OCR - Stage 1/2/3
cd /d "%~dp0"

set "PYTHON=C:\Program Files\Python312\python.exe"
if not exist "%PYTHON%" set "PYTHON=python"

echo ============================================
echo   Model Maker Web v4  -  http://localhost:8083
echo   Nemotron OCR (nvidia/Llama-3.1-Nemotron-Nano-VL-8B-V1)
echo   Stage 1 -^> Stage 2 -^> Stage 3
echo ============================================
echo.

echo [1/3] Clearing pycache...
for /d /r "model_maker_web_v4" %%d in (__pycache__) do @if exist "%%d" rd /s /q "%%d"

echo [2/3] Installing dependencies...
"%PYTHON%" -m pip install fastapi "uvicorn[standard]" python-multipart openpyxl PyMuPDF transformers torch accelerate Pillow websockets requests -q

echo [3/3] Starting server...
echo   Note: Nemotron model should be at C:/models/Nemotron-Nano-VL-8B
echo   Note: Or configure NIM API in config/ai_settings.ini [nemotron_ocr]
echo.
start "" http://localhost:8083

:LOOP
"%PYTHON%" -m uvicorn model_maker_web_v4.backend.main:app --host 0.0.0.0 --port 8083 --reload --reload-dir model_maker_web_v4/backend --reload-dir model_maker_web_v4/static
echo.
echo [%date% %time%] Server stopped. Restarting in 5s... (Ctrl+C to quit)
timeout /t 5 /nobreak >nul
goto LOOP
