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

echo [1/2] Installing huggingface-hub...
"%PYTHON%" -m pip install "huggingface-hub>=0.24.0" -q
echo   Done.
echo.

echo [2/2] Downloading Nemotron-Nano-VL-8B (~16GB)...
echo   This may take 30-60 minutes.
echo.

"%PYTHON%" download_models.py nemotron

echo.
echo ================================================
echo   Download Complete!
echo.
echo   Set in config\ai_settings.ini:
echo     [nemotron_ocr]
echo     model_path = C:/models/Nemotron-Nano-VL-8B
echo ================================================
pause
