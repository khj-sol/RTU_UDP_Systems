@echo off
chcp 65001 > nul
title Model Maker v1.3.0
cd /d "%~dp0.."

:: Use explicit Python path to avoid Windows Store alias errors
set "PYTHON=C:\Program Files\Python312\python.exe"

:: Check and install dependencies
"%PYTHON%" -c "import subprocess,sys;[subprocess.check_call([sys.executable,'-m','pip','install',p,'--quiet']) for m,p in [('fitz','PyMuPDF'),('openpyxl','openpyxl'),('anthropic','anthropic')] if not __import__('importlib').util.find_spec(m)]" 2>nul

echo [INFO] Starting Model Maker...
"%PYTHON%" -m model_maker.modbus_to_udp_mapper
if errorlevel 1 (
    echo.
    echo [ERROR] Model Maker exited with an error.
    pause
)
