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

echo [1/5] Clearing pycache...
for /d /r "model_maker_web_v4" %%d in (__pycache__) do @if exist "%%d" rd /s /q "%%d"

echo [2/5] Installing Windows dependencies (Phi + FastAPI)...
"%PYTHON%" -m pip install fastapi "uvicorn[standard]" python-multipart openpyxl PyMuPDF transformers torch accelerate bitsandbytes Pillow websockets requests -q

echo [3/5] Starting Qwen3-VL WSL inference server (port 8084)...
echo   Qwen3-VL runs in WSL to avoid gptqmodel/sys.abiflags build error on Windows.
set "WSL_SCRIPT=/mnt/c/Users/user/Desktop/CODE/RTU_UDP_Systems/inverter_model_maker/model_maker_web_v4/wsl_server/start_server.sh"
start "QwenVL-WSL-8084" wsl -e bash -c "bash '%WSL_SCRIPT%'"
echo   WSL server starting in background...
timeout /t 3 /nobreak > nul

echo [4/5] Checking HuggingFace models...
echo   Note: Phi-4-mini-instruct should be at C:/models/Phi-4-mini-instruct
echo   Note: Qwen3-VL-32B-4bit should be at /mnt/c/models/Qwen3-VL-32B-4bit (WSL path)
echo.

echo [5/5] Starting server...
echo.
start "" http://localhost:8083

:LOOP
"%PYTHON%" -m uvicorn model_maker_web_v4.backend.main:app --host 0.0.0.0 --port 8083 --reload --reload-dir model_maker_web_v4/backend --reload-dir model_maker_web_v4/static
echo.
echo [%date% %time%] Server stopped. Restarting in 5s... (Ctrl+C to quit)
timeout /t 5 /nobreak >nul
goto LOOP
