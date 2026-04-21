@echo off
chcp 65001 > nul
title Model Maker V4 - Nemotron OCR Model Setup
cd /d "%~dp0"

set "PYTHON=C:\Program Files\Python312\python.exe"
if not exist "%PYTHON%" set "PYTHON=python"

echo ================================================
echo   Model Maker V4 - Nemotron OCR Model Setup
echo   nvidia/Llama-3.1-Nemotron-Nano-VL-8B-V1
echo   Dest: C:\models\Nemotron-Nano-VL-8B
echo   Size: ~16GB
echo ================================================
echo.

echo [1/2] Installing Python dependencies...
"%PYTHON%" -m pip install "transformers>=4.50.0" "torch>=2.3.0" accelerate pillow "huggingface-hub>=0.24.0" -q
echo   Done.
echo.

echo [2/2] Downloading Nemotron-Nano-VL-8B (~16GB)...
echo   This may take 30-60 minutes.
echo.

"%PYTHON%" download_models.py nemotron

echo.
echo ================================================
echo   Setup Complete!
echo.
echo   모델 경로(C:\models\Nemotron-Nano-VL-8B)가 mm_settings.json에 기본 설정됩니다.
echo   웹 UI에서 경로 확인/변경 후 사용하세요.
echo.
echo   Now run: START_모델메이커_WEB_v4.bat
echo ================================================
pause
