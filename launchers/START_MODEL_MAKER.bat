@echo off
chcp 65001 > nul
title Model Maker v1.3.0

:: Check Python
python --version > nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python not found. Install Python 3.8+ and add to PATH.
    pause
    exit /b 1
)

:: Check required packages
python -c "import fitz" > nul 2>&1
if errorlevel 1 (
    echo [INFO] Installing PyMuPDF...
    python -m pip install PyMuPDF --quiet
)

:: Check openpyxl (required for 3-Stage Pipeline Excel export/import)
python -c "import openpyxl" > nul 2>&1
if errorlevel 1 (
    echo [INFO] Installing openpyxl (required for Stage Pipeline Excel features)...
    python -m pip install openpyxl --quiet
)

:: Check anthropic package for AI Generate / AI Assist features
python -c "import anthropic" > nul 2>&1
if errorlevel 1 (
    echo [INFO] Installing anthropic (required for AI Generate / AI Assist buttons)...
    python -m pip install anthropic --quiet
)

cd /d "%~dp0..\model_maker"
python modbus_to_udp_mapper.py
