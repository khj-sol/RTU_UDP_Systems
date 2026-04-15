@echo off
chcp 65001 > nul
title Model Maker V4 - HuggingFace Models Setup
cd /d "%~dp0"

set "PYTHON=C:\Program Files\Python312\python.exe"
if not exist "%PYTHON%" set "PYTHON=python"

echo ================================================
echo   Model Maker V4 - HuggingFace Models Setup
echo   Phi-4-mini-instruct + Qwen3-VL-32B-4bit
echo ================================================
echo.

echo [1/3] Installing Python dependencies...
"%PYTHON%" -m pip install "transformers>=4.50.0" "torch>=2.3.0" accelerate bitsandbytes pillow "huggingface-hub>=0.24.0" -q
echo   Done.
echo.

echo [2/3] Downloading Phi-4-mini-instruct...
echo   Location: C:/models/Phi-4-mini-instruct
echo   Size: ~8GB
echo.

"%PYTHON%" download_models.py phi

echo.
echo [3/3] Downloading Qwen3-VL-32B-4bit...
echo   Location: C:/models/Qwen3-VL-32B-4bit
echo   Size: ~22GB (4-bit quantized)
echo.

"%PYTHON%" download_models.py qwen

echo.
echo ================================================
echo   Setup Complete!
echo.
echo   Now run: START_모델메이커_WEB_v4.bat
echo ================================================
pause
