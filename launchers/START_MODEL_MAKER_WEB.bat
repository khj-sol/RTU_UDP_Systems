@echo off
chcp 65001 > nul
title Model Maker Web v1.0.0
cd /d "%~dp0.."

:: Use explicit Python path to avoid Windows Store alias errors
set "PYTHON=C:\Program Files\Python312\python.exe"

echo ============================================================
echo   Model Maker Web v1.0.0
echo   Web UI: http://localhost:8181
echo   Auto-installs required packages on first run
echo ============================================================
echo.

:: Check and install dependencies
echo [INFO] Checking dependencies...
"%PYTHON%" -c "import subprocess,sys;[subprocess.check_call([sys.executable,'-m','pip','install',p,'--quiet']) for m,p in [('fastapi','fastapi'),('uvicorn','uvicorn[standard]'),('multipart','python-multipart'),('openpyxl','openpyxl'),('fitz','PyMuPDF'),('anthropic','anthropic')] if not __import__('importlib').util.find_spec(m)]" 2>nul

echo [INFO] Starting Model Maker Web...
echo.

:LOOP
"%PYTHON%" -m model_maker_web.backend.main %*
echo.
echo [%date% %time%] Server exited (code: %ERRORLEVEL%)
echo   Restarting in 5 seconds... (Ctrl+C to stop)
timeout /t 5 /nobreak >nul
goto LOOP
