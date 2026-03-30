@echo off
title RTU Web Dashboard (Dev)
cd /d "%~dp0.."
echo ============================================================
echo   RTU Dashboard - Development Server
echo   Web: http://localhost:8080
echo   UDP: 0.0.0.0:13132
echo ============================================================
if exist "web_server\rtu_dashboard.db" (
    del /q "web_server\rtu_dashboard.db"
    echo   DB cleared.
)
echo.
python web_server/main.py
pause
