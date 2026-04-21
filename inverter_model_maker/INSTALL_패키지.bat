@echo off
chcp 65001 > nul
title Install Model Maker Dependencies and OCR Models
cd /d "%~dp0"

set "PYTHON=C:\Program Files\Python312\python.exe"
if not exist "%PYTHON%" set "PYTHON=python"

if /I "%SKIP_AI_MODELS%"=="1" goto :install_packages_only

echo ============================================
echo   Installing Model Maker dependencies...
echo ============================================
echo.

"%PYTHON%" -m pip install --upgrade pip
if errorlevel 1 goto :pip_error

"%PYTHON%" -m pip install -r requirements.txt
if errorlevel 1 goto :pip_error

echo.
echo ============================================
echo   Downloading v4.2 RapidOCR ONNX models...
echo ============================================
echo   Target: C:\models\paddleocr-onnx
echo   Models: PP-OCRv5 detection + English/Korean/Latin recognition
echo   Note  : Large VLMs (Nemotron/Qwen) are NOT downloaded by default.
echo.

"%PYTHON%" download_models.py rapidocr
if errorlevel 1 goto :model_error

echo.
echo [DONE] Packages and RapidOCR models installed.
echo.
echo Legacy large model commands, only if you really need them:
echo   "%PYTHON%" download_models.py nemotron
echo   "%PYTHON%" download_models.py qwen
echo   "%PYTHON%" download_models.py legacy_all
goto :end

:install_packages_only
echo ============================================
echo   Installing Model Maker dependencies only...
echo ============================================
echo.
"%PYTHON%" -m pip install --upgrade pip
if errorlevel 1 goto :pip_error
"%PYTHON%" -m pip install -r requirements.txt
if errorlevel 1 goto :pip_error
echo.
echo [DONE] Packages installed. AI model download skipped by SKIP_AI_MODELS=1.
goto :end

:pip_error
echo.
echo [ERROR] Package installation failed.
echo         Check Python/pip and network connection.
goto :end

:model_error
echo.
echo [ERROR] RapidOCR model download failed.
echo         Check internet connection or Hugging Face access.
echo         You can retry with:
echo         "%PYTHON%" download_models.py rapidocr
goto :end

:end
pause
