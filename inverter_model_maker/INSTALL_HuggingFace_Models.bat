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
"%PYTHON%" -m pip install transformers torch accelerate bitsandbytes pillow huggingface-hub -q
echo   Done.
echo.

echo [2/3] Downloading Phi-4-mini-instruct...
echo   Location: C:/models/Phi-4-mini-instruct
echo   Size: ~8GB
echo.

"%PYTHON%" -c "^
import os; ^
from transformers import AutoTokenizer, AutoModelForCausalLM; ^
models_dir = 'C:/models'; ^
os.makedirs(models_dir, exist_ok=True); ^
phi_path = os.path.join(models_dir, 'Phi-4-mini-instruct'); ^
if not os.path.exists(phi_path): ^
    print('[Downloading] Phi-4-mini-instruct...'); ^
    AutoTokenizer.from_pretrained('microsoft/Phi-4-mini-instruct', trust_remote_code=True, cache_dir=models_dir); ^
    AutoModelForCausalLM.from_pretrained('microsoft/Phi-4-mini-instruct', trust_remote_code=True, device_map='cpu', torch_dtype='auto', cache_dir=models_dir); ^
    print('[Done] Phi-4-mini-instruct downloaded'); ^
else: ^
    print('[Skip] Phi-4-mini-instruct already exists'); ^
"

echo.
echo [3/3] Downloading Qwen3-VL-32B-4bit...
echo   Location: C:/models/Qwen3-VL-32B-4bit
echo   Size: ~22GB (4-bit quantized)
echo.

"%PYTHON%" -c "^
import os; ^
from transformers import AutoProcessor, AutoModelForVision2Seq; ^
models_dir = 'C:/models'; ^
os.makedirs(models_dir, exist_ok=True); ^
qwen_path = os.path.join(models_dir, 'Qwen3-VL-32B-4bit'); ^
if not os.path.exists(qwen_path): ^
    print('[Downloading] Qwen3-VL-32B-4bit...'); ^
    try: ^
        AutoProcessor.from_pretrained('Qwen/Qwen3-VL-32B-4bit', trust_remote_code=True, cache_dir=models_dir); ^
        AutoModelForVision2Seq.from_pretrained('Qwen/Qwen3-VL-32B-4bit', trust_remote_code=True, device_map='auto', torch_dtype='auto', cache_dir=models_dir); ^
        print('[Done] Qwen3-VL-32B-4bit downloaded'); ^
    except Exception as e: ^
        print(f'[Error] Qwen3-VL download failed: {e}'); ^
        print('[Info] Try downloading manually from HuggingFace: https://huggingface.co/Qwen/Qwen3-VL-32B-4bit'); ^
else: ^
    print('[Skip] Qwen3-VL-32B-4bit already exists'); ^
"

echo.
echo ================================================
echo   Setup Complete!
echo.
echo   Now run: START_모델메이커_WEB_v4.bat
echo ================================================
pause
