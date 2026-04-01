# -*- coding: utf-8 -*-
"""
REST API + WebSocket 라우트 — 파이프라인 연결
"""
from fastapi import APIRouter, UploadFile, File, Form, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import FileResponse
import os
import shutil
import asyncio
import traceback
import json

from .session_store import SessionStore
from .ws_manager import ws_manager

router = APIRouter()

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
COMMON_DIR = os.path.join(PROJECT_ROOT, 'common')


# ─── 비동기 파이프라인 실행 헬퍼 ─────────────────────────────────────────────

async def _run_in_thread(func, *args, **kwargs):
    """동기 함수를 스레드 풀에서 실행"""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: func(*args, **kwargs))


def _make_progress_callback(sid: str, stage: str, loop: asyncio.AbstractEventLoop):
    """WebSocket 로그 전송 콜백 생성 — loop을 캡처하여 스레드 안전"""
    def callback(msg: str, level: str = 'info'):
        loop.call_soon_threadsafe(
            asyncio.ensure_future,
            ws_manager.send_json(sid, {
                'stage': stage,
                'text': msg,
                'level': level,
            })
        )
    return callback


# ─── 세션 ────────────────────────────────────────────────────────────────────

@router.post('/session/new')
def new_session():
    sid = SessionStore.create()
    return {'session_id': sid}


@router.get('/session/{sid}')
def get_session(sid: str):
    s = SessionStore.get(sid)
    if not s:
        raise HTTPException(404, 'session not found')
    return s


# ─── Stage 1 ─────────────────────────────────────────────────────────────────

@router.post('/stage1/upload')
async def stage1_upload(
    session_id: str = Form(...),
    file: UploadFile = File(...),
):
    """PDF 또는 Excel 업로드 → 작업 디렉토리에 저장"""
    s = SessionStore.get(session_id)
    if not s:
        raise HTTPException(404, 'session not found')

    work_dir = SessionStore.get_work_dir(session_id)
    dest = os.path.join(work_dir, file.filename)
    with open(dest, 'wb') as f:
        shutil.copyfileobj(file.file, f)

    SessionStore.update(session_id, uploaded_file=dest)
    return {'saved': dest, 'filename': file.filename}


@router.post('/stage1/run')
async def stage1_run(body: dict):
    """
    Stage 1 실행
    body: {session_id, filename, device_type}
    """
    sid = body.get('session_id')
    s = SessionStore.get(sid)
    if not s:
        raise HTTPException(404, 'session not found')

    device_type = body.get('device_type', 'inverter')
    uploaded = s.get('uploaded_file')
    if not uploaded or not os.path.exists(uploaded):
        raise HTTPException(400, 'no file uploaded')

    work_dir = SessionStore.get_work_dir(sid)

    # 비동기 실행
    async def _run():
        try:
            from .pipeline.stage1 import run_stage1
            progress = _make_progress_callback(sid, 's1', asyncio.get_running_loop())
            result = await _run_in_thread(
                run_stage1, uploaded, work_dir, device_type, progress)

            SessionStore.update(sid,
                                stage=1,
                                stage1_excel=result['output_path'],
                                meta=result['meta'])

            await ws_manager.send_json(sid, {
                'event': 'stage1_done',
                'stage': 's1',
                'text': f'Stage 1 완료: {result["output_name"]}',
                'level': 'ok',
                'counts': result['counts'],
                'review_count': result['review_count'],
            })
        except Exception as e:
            await ws_manager.send_json(sid, {
                'stage': 's1',
                'text': f'오류: {str(e)}',
                'level': 'err',
            })
            traceback.print_exc()

    asyncio.ensure_future(_run())
    return {'status': 'running', 'session_id': sid}


# ─── Stage 2 ─────────────────────────────────────────────────────────────────

@router.post('/stage2/run')
async def stage2_run(body: dict):
    """
    body: {session_id, mppt, strings_per_mppt, capacity}
    """
    sid = body.get('session_id')
    s = SessionStore.get(sid)
    if not s:
        raise HTTPException(404, 'session not found')

    stage1_excel = s.get('stage1_excel')
    if not stage1_excel or not os.path.exists(stage1_excel):
        raise HTTPException(400, 'Stage 1 not completed')

    mppt = int(body.get('mppt', 4))
    strings_per_mppt = int(body.get('strings_per_mppt', 2))
    capacity = body.get('capacity', '')
    work_dir = SessionStore.get_work_dir(sid)

    async def _run():
        try:
            from .pipeline.stage2 import run_stage2
            progress = _make_progress_callback(sid, 's2', asyncio.get_running_loop())
            result = await _run_in_thread(
                run_stage2, stage1_excel, work_dir,
                mppt, strings_per_mppt, capacity, progress)

            SessionStore.update(sid,
                                stage=2,
                                stage2_excel=result['output_path'])

            await ws_manager.send_json(sid, {
                'event': 'stage2_done',
                'stage': 's2',
                'text': f'Stage 2 완료: {result["output_name"]}',
                'level': 'ok',
                'h01_matched': result['h01_matched'],
                'h01_total': result['h01_total'],
                'der_matched': result['der_matched'],
                'der_total': result['der_total'],
                'review_count': result['review_count'],
            })
        except Exception as e:
            await ws_manager.send_json(sid, {
                'stage': 's2',
                'text': f'오류: {str(e)}',
                'level': 'err',
            })
            traceback.print_exc()

    asyncio.ensure_future(_run())
    return {'status': 'running', 'session_id': sid}


# ─── Stage 3 ─────────────────────────────────────────────────────────────────

@router.post('/stage3/run')
async def stage3_run(body: dict):
    """
    body: {session_id}
    """
    sid = body.get('session_id')
    s = SessionStore.get(sid)
    if not s:
        raise HTTPException(404, 'session not found')

    stage2_excel = s.get('stage2_excel')
    if not stage2_excel or not os.path.exists(stage2_excel):
        raise HTTPException(400, 'Stage 2 not completed')

    work_dir = SessionStore.get_work_dir(sid)

    async def _run():
        try:
            from .pipeline.stage3 import run_stage3
            progress = _make_progress_callback(sid, 's3', asyncio.get_running_loop())
            result = await _run_in_thread(
                run_stage3, stage2_excel, work_dir, progress)

            SessionStore.update(sid,
                                stage=3,
                                registers_py=result['output_path'])

            await ws_manager.send_json(sid, {
                'event': 'stage3_done',
                'stage': 's3',
                'text': f'Stage 3 완료: {result["filename"]}',
                'level': 'ok',
                'filename': result['filename'],
                'validation': result['validation'],
                'synonym_added': result['synonym_added'],
                'review_recorded': result['review_recorded'],
            })
        except Exception as e:
            await ws_manager.send_json(sid, {
                'stage': 's3',
                'text': f'오류: {str(e)}',
                'level': 'err',
            })
            traceback.print_exc()

    asyncio.ensure_future(_run())
    return {'status': 'running', 'session_id': sid}


# ─── 파일 다운로드 ───────────────────────────────────────────────────────────

@router.get('/download/{session_id}/{filename}')
def download_file(session_id: str, filename: str):
    work_dir = SessionStore.get_work_dir(session_id)
    path = os.path.join(work_dir, filename)
    if not os.path.exists(path):
        raise HTTPException(404, 'file not found')
    return FileResponse(path, filename=filename)


@router.get('/download-stage/{session_id}/{stage}')
def download_stage_file(session_id: str, stage: int):
    """Stage별 결과 파일 다운로드"""
    s = SessionStore.get(session_id)
    if not s:
        raise HTTPException(404, 'session not found')

    if stage == 1:
        path = s.get('stage1_excel')
    elif stage == 2:
        path = s.get('stage2_excel')
    elif stage == 3:
        path = s.get('registers_py')
    else:
        raise HTTPException(400, 'invalid stage')

    if not path or not os.path.exists(path):
        raise HTTPException(404, 'file not found')
    return FileResponse(path, filename=os.path.basename(path))


# ─── WebSocket ───────────────────────────────────────────────────────────────

@router.websocket('/ws/{session_id}')
async def websocket_endpoint(ws: WebSocket, session_id: str):
    await ws_manager.connect(session_id, ws)
    try:
        while True:
            data = await ws.receive_text()
            # 클라이언트 메시지 처리 (REVIEW 판정 등)
            try:
                msg = json.loads(data)
                if msg.get('type') == 'review_verdicts':
                    # REVIEW 판정을 Stage 2 Excel에 반영
                    s = SessionStore.get(session_id)
                    if s and s.get('stage2_excel'):
                        _update_review_verdicts(s['stage2_excel'], msg.get('verdicts', []))
                        await ws.send_json({'event': 'verdicts_saved', 'count': len(msg.get('verdicts', []))})
            except (json.JSONDecodeError, KeyError):
                pass
    except WebSocketDisconnect:
        ws_manager.disconnect(session_id)


def _update_review_verdicts(excel_path: str, verdicts: list):
    """Stage 2 Excel REVIEW 시트에 사용자 판정 기록"""
    try:
        import openpyxl
        wb = openpyxl.load_workbook(excel_path)
        if 'REVIEW' not in wb.sheetnames:
            wb.close()
            return

        ws = wb['REVIEW']
        header = [str(c.value).strip() if c.value else '' for c in ws[1]]

        # '사용자 판정' 컬럼 인덱스
        verdict_col = None
        for i, h in enumerate(header):
            if '판정' in h:
                verdict_col = i + 1
                break
        if verdict_col is None:
            wb.close()
            return

        for v in verdicts:
            row_idx = v.get('row', 0) + 2  # 1-indexed + header
            verdict_val = v.get('verdict', '')
            ws.cell(row=row_idx, column=verdict_col, value=verdict_val)

        wb.save(excel_path)
        wb.close()
    except Exception:
        pass
