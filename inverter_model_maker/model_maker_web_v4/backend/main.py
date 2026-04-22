# -*- coding: utf-8 -*-
"""
Model Maker Web v4 — FastAPI 진입점 (Phi-mini-MoE + Qwen3-VL-32B / HuggingFace)
실행: uvicorn model_maker_web_v4.backend.main:app --host 0.0.0.0 --port 8083 --reload
"""
import os
import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, Response

from .api_routes import router, _load_ai_settings

logger = logging.getLogger(__name__)


async def _preload_ai_models():
    """서버 시작 직후 AI 모델을 백그라운드에서 미리 로드."""
    try:
        cfg = _load_ai_settings()
        phi_path = cfg.get('phi_model_path', '')
        phi_device = cfg.get('phi_device', 'auto')
        qwen_path = cfg.get('qwen_model_path', '')
        qwen_device = cfg.get('qwen_device', 'auto')
        qwen_wsl_url = cfg.get('qwen_wsl_url', '')

        if not phi_path and not qwen_path:
            logger.info('[AI Preload] ai_settings.ini 경로 미설정 — 스킵')
            return

        from .pipeline.ai_phi import get_phi_model
        from .pipeline.ai_qwen_vl import get_qwen_vl_model

        loop = asyncio.get_running_loop()
        tasks = []
        if phi_path:
            logger.info('[AI Preload] Phi 모델 프리로드: %s', phi_path)
            tasks.append(loop.run_in_executor(None, get_phi_model, phi_path, phi_device))
        if qwen_path:
            logger.info('[AI Preload] Qwen-VL 모델 프리로드: %s (%s)',
                        qwen_path, 'WSL' if qwen_wsl_url else 'local')
            tasks.append(loop.run_in_executor(None, get_qwen_vl_model, qwen_path, qwen_device, qwen_wsl_url))

        await asyncio.gather(*tasks)
        logger.info('[AI Preload] 완료')
    except Exception as e:
        logger.warning('[AI Preload] 실패 (무시): %s', e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(_preload_ai_models())
    yield


app = FastAPI(
    title='Model Maker Web v4',
    description='Inverter Modbus PDF → RTU register map 3-stage pipeline (Phi-mini-MoE + Qwen3-VL)',
    version='4.0.0',
    lifespan=lifespan,
)

app.include_router(router, prefix='/api')

# 브라우저 캐싱 방지: HTML/JS/CSS 변경 즉시 반영
_NO_CACHE_HEADERS = {
    'Cache-Control': 'no-store, no-cache, must-revalidate, max-age=0',
    'Pragma': 'no-cache',
    'Expires': '0',
}


@app.middleware('http')
async def add_no_cache_headers(request: Request, call_next):
    response: Response = await call_next(request)
    path = request.url.path
    # API 는 캐싱 무관, static/HTML 만 no-cache
    if path == '/' or path.startswith('/static/') or path.endswith(('.html', '.js', '.css')):
        for k, v in _NO_CACHE_HEADERS.items():
            response.headers[k] = v
    return response


# 정적 파일
_STATIC = os.path.join(os.path.dirname(__file__), '..', 'static')
app.mount('/static', StaticFiles(directory=_STATIC), name='static')


@app.get('/')
def index():
    return FileResponse(
        os.path.join(_STATIC, 'index.html'),
        headers=_NO_CACHE_HEADERS,
    )
