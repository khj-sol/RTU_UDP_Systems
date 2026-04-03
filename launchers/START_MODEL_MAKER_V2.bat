@echo off
chcp 65001 > nul
title Model Maker Web v2
cd /d "%~dp0.."

:: Python 경로 명시 (Windows Store alias 충돌 방지)
set "PYTHON=C:\Program Files\Python312\python.exe"
if not exist "%PYTHON%" set "PYTHON=python"

echo ============================================
echo   Model Maker Web v2
echo   Stage 1→2→3 파이프라인 웹 UI
echo ============================================
echo.

echo [1/3] 캐시 정리 중...
for /d /r "model_maker_web_v2" %%d in (__pycache__) do @if exist "%%d" rd /s /q "%%d"
echo   __pycache__ 삭제 완료

echo [2/3] 의존성 확인 중...
"%PYTHON%" -c "import subprocess,sys;[subprocess.check_call([sys.executable,'-m','pip','install',p,'--quiet']) for m,p in [('fastapi','fastapi'),('uvicorn','uvicorn[standard]'),('multipart','python-multipart'),('openpyxl','openpyxl'),('fitz','PyMuPDF')] if not __import__('importlib').util.find_spec(m)]" 2>nul

:: 8082 포트 충돌 정리
echo [2/3] 포트 8082 확인 중...
"%PYTHON%" -c "import subprocess; r=subprocess.run('netstat -ano',capture_output=True,text=True,shell=True); lines=[l for l in r.stdout.splitlines() if ':8082' in l and 'LISTENING' in l]; [subprocess.run(f'taskkill /PID {l.split()[-1]} /F',shell=True,capture_output=True) or print(f'[INFO] Killed PID {l.split()[-1]}') for l in lines]" 2>nul

echo [3/3] 서버 시작 중...
echo.
echo   http://localhost:8082
echo.
echo   PDF 업로드 → Stage 1 추출 → Stage 2 필터링 → Stage 3 코드 생성
echo   Ctrl+C로 종료
echo.

start "" http://localhost:8082

:LOOP
"%PYTHON%" -m uvicorn model_maker_web_v2.backend.main:app --host 0.0.0.0 --port 8082 --reload
echo.
echo [%date% %time%] 서버 종료 (코드: %ERRORLEVEL%)
echo   5초 후 재시작... (Ctrl+C로 중단)
timeout /t 5 /nobreak >nul
goto LOOP
