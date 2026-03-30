// Model Maker Web v1.0.0 — React 18 CDN (React.createElement, no JSX)
'use strict';

const { useState, useEffect, useRef, useCallback, useMemo } = React;
const h = React.createElement;

// ---------------------------------------------------------------------------
// Constants & Helpers
// ---------------------------------------------------------------------------
const API = window.location.origin;

async function apiFetch(path, opts = {}) {
  const r = await fetch(API + path, opts);
  if (!r.ok) {
    let msg = `HTTP ${r.status}`;
    try { const j = await r.json(); msg = j.detail || JSON.stringify(j) || msg; } catch {}
    throw new Error(msg);
  }
  return r.json();
}

async function apiGet(path) { return apiFetch(path); }
async function apiPost(path, body) {
  return apiFetch(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
}
async function apiPut(path, body) {
  return apiFetch(path, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
}
async function apiDelete(path) {
  return apiFetch(path, { method: 'DELETE' });
}

// Session ID persisted in sessionStorage
function getSessionId() {
  let sid = sessionStorage.getItem('mm_session_id');
  if (!sid) {
    sid = crypto.randomUUID();
    sessionStorage.setItem('mm_session_id', sid);
  }
  return sid;
}

function cls(...args) { return args.filter(Boolean).join(' '); }

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

function Badge({ text, color = 'bg-blue-600' }) {
  return h('span', { className: cls('px-2 py-0.5 rounded text-xs font-mono', color) }, text);
}

function Spinner() {
  return h('svg', {
    className: 'animate-spin h-4 w-4 text-blue-400',
    fill: 'none', viewBox: '0 0 24 24',
    xmlns: 'http://www.w3.org/2000/svg',
  },
    h('circle', { className: 'opacity-25', cx: 12, cy: 12, r: 10, stroke: 'currentColor', strokeWidth: 4 }),
    h('path', { className: 'opacity-75', fill: 'currentColor', d: 'M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z' })
  );
}

function ProgressBar({ pct, label }) {
  const p = Math.max(0, Math.min(100, pct || 0));
  return h('div', { className: 'w-full' },
    label && h('div', { className: 'text-xs text-gray-400 mb-1 truncate' }, label),
    h('div', { className: 'w-full bg-gray-700 rounded-full h-2' },
      h('div', {
        className: 'progress-bar-inner bg-blue-500 h-2 rounded-full',
        style: { width: `${p}%` },
      })
    )
  );
}

function Alert({ type = 'info', children }) {
  const colors = {
    info: 'bg-blue-900/50 border-blue-700 text-blue-200',
    success: 'bg-green-900/50 border-green-700 text-green-200',
    error: 'bg-red-900/50 border-red-700 text-red-200',
    warning: 'bg-yellow-900/50 border-yellow-700 text-yellow-200',
  };
  return h('div', { className: cls('border rounded p-3 text-sm', colors[type] || colors.info) }, children);
}

function Button({ onClick, disabled, children, variant = 'primary', size = 'md', className = '' }) {
  const variants = {
    primary: 'bg-blue-600 hover:bg-blue-700 text-white disabled:bg-blue-900 disabled:text-blue-500',
    secondary: 'bg-gray-700 hover:bg-gray-600 text-gray-200 disabled:bg-gray-800 disabled:text-gray-600',
    success: 'bg-green-600 hover:bg-green-700 text-white disabled:bg-green-900 disabled:text-green-600',
    danger: 'bg-red-600 hover:bg-red-700 text-white disabled:bg-red-900 disabled:text-red-600',
    outline: 'border border-gray-600 hover:bg-gray-700 text-gray-300 disabled:opacity-40',
  };
  const sizes = { sm: 'px-2 py-1 text-xs', md: 'px-3 py-1.5 text-sm', lg: 'px-4 py-2 text-base' };
  return h('button', {
    onClick,
    disabled,
    className: cls('rounded font-medium transition-colors disabled:cursor-not-allowed flex items-center gap-1.5',
      variants[variant] || variants.primary, sizes[size] || sizes.md, className),
  }, children);
}

function Toggle({ label, checked, onChange }) {
  return h('label', { className: 'flex items-center gap-2 cursor-pointer' },
    h('div', { className: 'relative' },
      h('input', { type: 'checkbox', className: 'sr-only', checked, onChange }),
      h('div', { className: cls('w-10 h-5 rounded-full transition-colors', checked ? 'bg-blue-600' : 'bg-gray-600') }),
      h('div', {
        className: cls('absolute top-0.5 left-0.5 w-4 h-4 rounded-full bg-white transition-transform',
          checked ? 'translate-x-5' : 'translate-x-0'),
      })
    ),
    h('span', { className: 'text-sm text-gray-300' }, label)
  );
}

function LogPanel({ messages, maxH = 'max-h-40' }) {
  const ref = useRef(null);
  useEffect(() => {
    if (ref.current) ref.current.scrollTop = ref.current.scrollHeight;
  }, [messages]);
  return h('div', {
    ref,
    className: cls('overflow-y-auto bg-gray-800 rounded p-2 font-mono text-xs text-gray-300 space-y-0.5', maxH),
  },
    messages.length === 0
      ? h('div', { className: 'text-gray-500' }, '대기 중...')
      : messages.map((m, i) => h('div', { key: i, className: 'leading-relaxed' }, m))
  );
}

// ---------------------------------------------------------------------------
// Drop Zone for PDF Upload
// ---------------------------------------------------------------------------
function PDFDropZone({ sessionId, onUploaded }) {
  const [dragging, setDragging] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [error, setError] = useState('');
  const inputRef = useRef(null);

  async function uploadFile(file) {
    if (!file || !file.name.toLowerCase().endsWith('.pdf')) {
      setError('PDF 파일만 업로드 가능합니다.');
      return;
    }
    setError('');
    setUploading(true);
    try {
      const fd = new FormData();
      fd.append('file', file);
      const r = await fetch(`${API}/api/upload-pdf?session_id=${sessionId}`, {
        method: 'POST', body: fd,
      });
      if (!r.ok) {
        const j = await r.json().catch(() => ({}));
        throw new Error(j.detail || `HTTP ${r.status}`);
      }
      const data = await r.json();
      onUploaded(data);
    } catch (e) {
      setError(e.message);
    } finally {
      setUploading(false);
    }
  }

  function onDrop(e) {
    e.preventDefault();
    setDragging(false);
    const file = e.dataTransfer.files[0];
    if (file) uploadFile(file);
  }

  function onInputChange(e) {
    const file = e.target.files[0];
    if (file) uploadFile(file);
    e.target.value = '';
  }

  return h('div', { className: 'space-y-2' },
    h('div', {
      className: cls('drop-zone rounded-lg p-8 text-center cursor-pointer', dragging && 'drag-over'),
      onDragOver: e => { e.preventDefault(); setDragging(true); },
      onDragLeave: () => setDragging(false),
      onDrop,
      onClick: () => inputRef.current?.click(),
    },
      uploading
        ? h('div', { className: 'flex flex-col items-center gap-2' },
            h(Spinner),
            h('div', { className: 'text-gray-400 text-sm' }, '업로드 중...')
          )
        : h('div', { className: 'flex flex-col items-center gap-2' },
            h('div', { className: 'text-4xl text-gray-500' }, '📄'),
            h('div', { className: 'text-gray-400 text-sm' }, 'PDF 파일을 여기에 드래그하거나 클릭하여 선택'),
            h('div', { className: 'text-gray-600 text-xs' }, 'Modbus Protocol PDF')
          )
    ),
    h('input', { ref: inputRef, type: 'file', accept: '.pdf', className: 'hidden', onChange: onInputChange }),
    error && h(Alert, { type: 'error' }, error)
  );
}

// ---------------------------------------------------------------------------
// Data Table (virtualized-style, fixed header)
// ---------------------------------------------------------------------------
function DataTable({ rows, columns, editMode = false, onCellEdit, maxH = 'max-h-96' }) {
  const [editCell, setEditCell] = useState(null); // {row, col}
  const [editVal, setEditVal] = useState('');

  if (!rows || rows.length === 0) {
    return h('div', { className: 'text-gray-500 text-sm p-4 text-center' }, '데이터 없음');
  }

  const cols = columns || Object.keys(rows[0] || {});

  function startEdit(ri, col) {
    if (!editMode) return;
    setEditCell({ ri, col });
    setEditVal(rows[ri][col] || '');
  }

  async function commitEdit() {
    if (!editCell) return;
    const { ri, col } = editCell;
    if (onCellEdit) await onCellEdit(ri, col, editVal);
    setEditCell(null);
  }

  function onKeyDown(e) {
    if (e.key === 'Enter') commitEdit();
    if (e.key === 'Escape') setEditCell(null);
  }

  const MATCH_COLORS = {
    'Addr-Match': 'text-green-400',
    'Name-Match': 'text-blue-400',
    'Reference': 'text-purple-400',
    'Heuristic': 'text-yellow-400',
    'Supplement': 'text-orange-400',
    'Unmapped': 'text-red-400',
  };

  return h('div', { className: cls('overflow-auto rounded border border-gray-700', maxH) },
    h('table', { className: 'w-full text-xs border-collapse' },
      h('thead', { className: 'sticky top-0 bg-gray-800 z-10' },
        h('tr', null,
          cols.map(col => h('th', {
            key: col,
            className: 'px-2 py-1.5 text-left text-gray-400 border-b border-gray-700 whitespace-nowrap',
          }, col))
        )
      ),
      h('tbody', null,
        rows.map((row, ri) => h('tr', {
          key: ri,
          className: cls('border-b border-gray-800 hover:bg-gray-800/50',
            ri % 2 === 0 ? 'bg-gray-900' : 'bg-gray-900/50'),
        },
          cols.map(col => {
            const val = row[col] || '';
            const isEditing = editMode && editCell && editCell.ri === ri && editCell.col === col;
            const matchColor = col === 'Match Type' ? (MATCH_COLORS[val] || 'text-gray-300') : '';
            return h('td', {
              key: col,
              className: cls('px-2 py-1 max-w-xs truncate', matchColor,
                editMode && 'cursor-pointer hover:bg-blue-900/30'),
              onClick: () => startEdit(ri, col),
              title: val,
            },
              isEditing
                ? h('input', {
                    type: 'text',
                    value: editVal,
                    onChange: e => setEditVal(e.target.value),
                    onBlur: commitEdit,
                    onKeyDown,
                    autoFocus: true,
                    className: 'cell-edit w-full bg-gray-800 border border-blue-500 rounded px-1 text-gray-100',
                  })
                : val
            );
          })
        ))
      )
    )
  );
}

// ---------------------------------------------------------------------------
// Stage 1 Tab
// ---------------------------------------------------------------------------
function Stage1Tab({ sessionId, pdfInfo, onPdfUploaded, wsMessages }) {
  const [mode, setMode] = useState('offline');
  const [running, setRunning] = useState(false);
  const [done, setDone] = useState(false);
  const [error, setError] = useState('');
  const [rows, setRows] = useState([]);
  const [logs, setLogs] = useState([]);

  // Filter ws messages for stage1
  useEffect(() => {
    const msgs = wsMessages.filter(m => m.stage === 'stage1');
    if (msgs.length === 0) return;
    const newLogs = msgs.map(m => {
      if (m.type === 'progress') return `[진행] ${m.message}`;
      if (m.type === 'done') return m.success ? `✓ ${m.detail}` : `✗ ${m.detail}`;
      if (m.type === 'error') return `✗ 오류: ${m.error}`;
      return '';
    }).filter(Boolean);
    if (newLogs.length > 0) setLogs(prev => [...prev, ...newLogs]);

    const last = msgs[msgs.length - 1];
    if (last.type === 'done') {
      setRunning(false);
      if (last.success) {
        setDone(true);
        loadResult();
      } else {
        setError(last.detail || '실패');
      }
    } else if (last.type === 'error') {
      setRunning(false);
      setError(last.error);
    }
  }, [wsMessages]);

  async function loadResult() {
    try {
      const data = await apiGet(`/api/stage1/result?session_id=${sessionId}`);
      setRows(data.rows || []);
    } catch (e) {
      setError(e.message);
    }
  }

  async function runStage1() {
    if (!pdfInfo) { setError('먼저 PDF를 업로드하세요.'); return; }
    setRunning(true);
    setDone(false);
    setError('');
    setLogs([`[시작] PDF: ${pdfInfo.filename}, 모드: ${mode}`]);
    try {
      await apiPost('/api/stage1/run', { session_id: sessionId, mode });
    } catch (e) {
      setRunning(false);
      setError(e.message);
    }
  }

  const COLS = ['No', 'Section', 'Addr(Hex)', 'Addr(Dec)', 'Definition', 'Data Type',
                'FC', 'Regs', 'Unit', 'Scale', 'R/W', 'Comment'];

  return h('div', { className: 'space-y-4' },
    h('div', { className: 'bg-gray-800 rounded-lg p-4 space-y-4' },
      h('h2', { className: 'text-base font-semibold text-gray-200' }, 'Stage 1 — PDF → Excel 추출'),

      h(PDFDropZone, { sessionId, onUploaded: onPdfUploaded }),

      pdfInfo && h(Alert, { type: 'success' },
        `✓ 업로드됨: ${pdfInfo.filename} (${(pdfInfo.size / 1024).toFixed(1)} KB)`
      ),

      h('div', { className: 'flex items-center gap-4 flex-wrap' },
        h('div', { className: 'flex items-center gap-2' },
          h('span', { className: 'text-sm text-gray-400' }, '모드:'),
          h('div', { className: 'flex rounded overflow-hidden border border-gray-600' },
            ['offline', 'ai'].map(m =>
              h('button', {
                key: m,
                onClick: () => setMode(m),
                className: cls('px-3 py-1 text-sm transition-colors',
                  mode === m ? 'bg-blue-600 text-white' : 'bg-gray-700 text-gray-300 hover:bg-gray-600'),
              }, m === 'offline' ? '오프라인' : 'AI (Claude)')
            )
          )
        ),

        h(Button, {
          onClick: runStage1,
          disabled: running || !pdfInfo,
          variant: 'primary',
        },
          running ? h(Spinner) : null,
          running ? '실행 중...' : '자동 추출 실행'
        ),

        done && rows.length > 0 && h('a', {
          href: `/api/stage1/download-excel?session_id=${sessionId}`,
          className: 'px-3 py-1.5 text-sm rounded bg-green-700 hover:bg-green-600 text-white transition-colors',
        }, '📥 Excel 다운로드'),
      ),

      error && h(Alert, { type: 'error' }, error),

      h('div', { className: 'space-y-1' },
        h('div', { className: 'text-xs text-gray-500 font-medium' }, '진행 로그'),
        h(LogPanel, { messages: logs })
      ),
    ),

    done && rows.length > 0 && h('div', { className: 'bg-gray-800 rounded-lg p-4 space-y-2' },
      h('div', { className: 'flex items-center justify-between' },
        h('h3', { className: 'text-sm font-semibold text-gray-300' },
          `추출 결과 (${rows.length}개 레지스터)`),
      ),
      h(DataTable, { rows, columns: COLS, maxH: 'max-h-80' })
    )
  );
}

// ---------------------------------------------------------------------------
// Stage 2 Tab
// ---------------------------------------------------------------------------
function Stage2Tab({ sessionId, stage1Done, wsMessages }) {
  const [mode, setMode] = useState('offline');
  const [mppt, setMppt] = useState(4);
  const [strings, setStrings] = useState(8);
  const [running, setRunning] = useState(false);
  const [done, setDone] = useState(false);
  const [error, setError] = useState('');
  const [rows, setRows] = useState([]);
  const [logs, setLogs] = useState([]);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    const msgs = wsMessages.filter(m => m.stage === 'stage2');
    if (msgs.length === 0) return;
    const newLogs = msgs.map(m => {
      if (m.type === 'progress') return `[진행] ${m.message}`;
      if (m.type === 'done') return m.success ? `✓ ${m.detail}` : `✗ ${m.detail}`;
      if (m.type === 'error') return `✗ 오류: ${m.error}`;
      return '';
    }).filter(Boolean);
    if (newLogs.length > 0) setLogs(prev => [...prev, ...newLogs]);
    const last = msgs[msgs.length - 1];
    if (last.type === 'done') {
      setRunning(false);
      if (last.success) { setDone(true); loadResult(); }
      else setError(last.detail || '실패');
    } else if (last.type === 'error') {
      setRunning(false);
      setError(last.error);
    }
  }, [wsMessages]);

  async function loadResult() {
    try {
      const data = await apiGet(`/api/stage2/result?session_id=${sessionId}`);
      setRows(data.rows || []);
    } catch (e) {}
  }

  async function runStage2() {
    setRunning(true); setDone(false); setError('');
    setLogs([`[시작] 자동 매핑, 모드: ${mode}`]);
    try {
      await apiPost('/api/stage2/run', {
        session_id: sessionId, mode, mppt_count: mppt, string_count: strings,
      });
    } catch (e) {
      setRunning(false);
      setError(e.message);
    }
  }

  async function onCellEdit(ri, col, val) {
    setSaving(true);
    try {
      await apiPut('/api/stage2/update-row', {
        session_id: sessionId, row_index: ri, col_name: col, value: val,
      });
      setRows(prev => {
        const next = [...prev];
        next[ri] = { ...next[ri], [col]: val };
        return next;
      });
    } catch (e) {
      setError(e.message);
    } finally {
      setSaving(false);
    }
  }

  const COLS = ['Section', 'Src Addr(Hex)', 'Src Addr(Dec)', 'Src Name', 'Data Type',
                'FC', 'Unit', 'R/W', 'Regs', 'Match Score', 'Sol Name', 'Scale',
                'Match Type', 'Notes'];

  return h('div', { className: 'space-y-4' },
    h('div', { className: 'bg-gray-800 rounded-lg p-4 space-y-4' },
      h('h2', { className: 'text-base font-semibold text-gray-200' }, 'Stage 2 — 자동 매핑'),

      !stage1Done && h(Alert, { type: 'warning' }, 'Stage 1을 먼저 완료하세요.'),

      h('div', { className: 'flex items-center gap-4 flex-wrap' },
        h('div', { className: 'flex items-center gap-2' },
          h('span', { className: 'text-sm text-gray-400' }, '모드:'),
          h('div', { className: 'flex rounded overflow-hidden border border-gray-600' },
            ['offline', 'ai'].map(m =>
              h('button', {
                key: m,
                onClick: () => setMode(m),
                className: cls('px-3 py-1 text-sm transition-colors',
                  mode === m ? 'bg-blue-600 text-white' : 'bg-gray-700 text-gray-300 hover:bg-gray-600'),
              }, m === 'offline' ? '오프라인' : 'AI (Claude)')
            )
          )
        ),

        h('div', { className: 'flex items-center gap-2' },
          h('span', { className: 'text-sm text-gray-400' }, 'MPPT:'),
          h('input', {
            type: 'number', min: 1, max: 9, value: mppt,
            onChange: e => setMppt(parseInt(e.target.value) || 4),
            className: 'w-16 bg-gray-700 border border-gray-600 rounded px-2 py-1 text-sm text-center',
          })
        ),

        h('div', { className: 'flex items-center gap-2' },
          h('span', { className: 'text-sm text-gray-400' }, '스트링:'),
          h('input', {
            type: 'number', min: 1, max: 24, value: strings,
            onChange: e => setStrings(parseInt(e.target.value) || 8),
            className: 'w-16 bg-gray-700 border border-gray-600 rounded px-2 py-1 text-sm text-center',
          })
        ),

        h(Button, {
          onClick: runStage2,
          disabled: running || !stage1Done,
          variant: 'primary',
        },
          running ? h(Spinner) : null,
          running ? '매핑 중...' : '자동 매핑 실행'
        ),

        done && rows.length > 0 && h('a', {
          href: `/api/stage2/download-excel?session_id=${sessionId}`,
          className: 'px-3 py-1.5 text-sm rounded bg-green-700 hover:bg-green-600 text-white transition-colors',
        }, '📥 Excel 다운로드'),
      ),

      error && h(Alert, { type: 'error' }, error),
      saving && h('div', { className: 'text-xs text-blue-400' }, '저장 중...'),

      h('div', { className: 'space-y-1' },
        h('div', { className: 'text-xs text-gray-500 font-medium' }, '진행 로그'),
        h(LogPanel, { messages: logs })
      ),
    ),

    done && rows.length > 0 && h('div', { className: 'bg-gray-800 rounded-lg p-4 space-y-2' },
      h('div', { className: 'flex items-center justify-between' },
        h('h3', { className: 'text-sm font-semibold text-gray-300' },
          `매핑 결과 (${rows.length}개 레지스터) — 셀 클릭으로 편집 가능`),
      ),
      h('div', { className: 'flex gap-2 flex-wrap text-xs mb-2' },
        [
          ['Addr-Match', 'text-green-400', '주소 직접 매핑'],
          ['Name-Match', 'text-blue-400', '이름 유사도 매핑'],
          ['Reference', 'text-purple-400', '레퍼런스 매핑'],
          ['Heuristic', 'text-yellow-400', '휴리스틱 매핑'],
          ['Supplement', 'text-orange-400', '보완 매핑'],
          ['Unmapped', 'text-red-400', '미매핑'],
        ].map(([k, c, desc]) =>
          h('span', { key: k, className: cls(c) }, `■ ${k} (${desc})`)
        )
      ),
      h(DataTable, { rows, columns: COLS, editMode: true, onCellEdit, maxH: 'max-h-96' })
    )
  );
}

// ---------------------------------------------------------------------------
// Stage 3 Tab
// ---------------------------------------------------------------------------
function Stage3Tab({ sessionId, stage2Done, wsMessages }) {
  const [mode, setMode] = useState('offline');
  const [protocol, setProtocol] = useState('custom');
  const [manufacturer, setManufacturer] = useState('');
  const [mppt, setMppt] = useState(4);
  const [strings, setStrings] = useState(8);
  const [ivScan, setIvScan] = useState(false);
  const [derAvm, setDerAvm] = useState(true);
  const [deaAvm, setDeaAvm] = useState(true);
  const [fcCode, setFcCode] = useState('FC03');
  const [running, setRunning] = useState(false);
  const [done, setDone] = useState(false);
  const [error, setError] = useState('');
  const [code, setCode] = useState('');
  const [results, setResults] = useState([]);
  const [success, setSuccess] = useState(false);
  const [logs, setLogs] = useState([]);
  const [saving, setSaving] = useState(false);
  const [saveMsg, setSaveMsg] = useState('');
  const [showCode, setShowCode] = useState(false);

  useEffect(() => {
    const msgs = wsMessages.filter(m => m.stage === 'stage3');
    if (msgs.length === 0) return;
    const newLogs = msgs.map(m => {
      if (m.type === 'progress') return `[진행] ${m.message}`;
      if (m.type === 'done') return m.success ? `✓ ${m.detail}` : `✗ ${m.detail}`;
      if (m.type === 'error') return `✗ 오류: ${m.error}`;
      return '';
    }).filter(Boolean);
    if (newLogs.length > 0) setLogs(prev => [...prev, ...newLogs]);
    const last = msgs[msgs.length - 1];
    if (last.type === 'done') {
      setRunning(false);
      if (last.success !== undefined) {
        setDone(true);
        loadResult();
      }
    } else if (last.type === 'error') {
      setRunning(false);
      setError(last.error);
    }
  }, [wsMessages]);

  async function loadResult() {
    try {
      const data = await apiGet(`/api/stage3/result?session_id=${sessionId}`);
      setCode(data.code || '');
      setResults(data.results || []);
      setSuccess(data.success || false);
      setDone(true);
    } catch (e) {}
  }

  async function runStage3() {
    if (!protocol.trim()) { setError('프로토콜 이름을 입력하세요.'); return; }
    setRunning(true); setDone(false); setError(''); setSaveMsg('');
    setLogs([`[시작] 코드 생성 — protocol: ${protocol}, 모드: ${mode}`]);
    try {
      await apiPost('/api/stage3/run', {
        session_id: sessionId, mode,
        protocol_name: protocol.trim(),
        manufacturer,
        mppt_count: mppt,
        string_count: strings,
        iv_scan: ivScan,
        der_avm: derAvm,
        dea_avm: deaAvm,
        class_name: 'RegisterMap',
        fc_code: fcCode,
      });
    } catch (e) {
      setRunning(false);
      setError(e.message);
    }
  }

  async function saveToCommon() {
    setSaving(true); setSaveMsg('');
    try {
      const data = await apiPost('/api/stage3/save', {
        session_id: sessionId,
        protocol_name: protocol.trim(),
        save_as_reference: true,
      });
      setSaveMsg(`✓ 저장됨: ${data.path}`);
    } catch (e) {
      setSaveMsg(`✗ 오류: ${e.message}`);
    } finally {
      setSaving(false);
    }
  }

  const passCount = results.filter(r => r[0] === 'PASS').length;

  return h('div', { className: 'space-y-4' },
    h('div', { className: 'bg-gray-800 rounded-lg p-4 space-y-4' },
      h('h2', { className: 'text-base font-semibold text-gray-200' }, 'Stage 3 — 코드 생성 & 검증'),

      !stage2Done && h(Alert, { type: 'warning' }, 'Stage 2를 먼저 완료하세요.'),

      h('div', { className: 'grid grid-cols-1 sm:grid-cols-2 gap-3' },

        h('div', { className: 'space-y-1' },
          h('label', { className: 'text-xs text-gray-400' }, '프로토콜 이름 (protocol_name)'),
          h('input', {
            type: 'text', value: protocol,
            onChange: e => setProtocol(e.target.value),
            placeholder: 'newbrand',
            className: 'w-full bg-gray-700 border border-gray-600 rounded px-2 py-1.5 text-sm',
          })
        ),

        h('div', { className: 'space-y-1' },
          h('label', { className: 'text-xs text-gray-400' }, '제조사 (manufacturer)'),
          h('input', {
            type: 'text', value: manufacturer,
            onChange: e => setManufacturer(e.target.value),
            placeholder: 'NewBrand Inc.',
            className: 'w-full bg-gray-700 border border-gray-600 rounded px-2 py-1.5 text-sm',
          })
        ),

        h('div', { className: 'flex items-center gap-4' },
          h('div', { className: 'space-y-1' },
            h('label', { className: 'text-xs text-gray-400' }, 'MPPT 수'),
            h('input', {
              type: 'number', min: 1, max: 9, value: mppt,
              onChange: e => setMppt(parseInt(e.target.value) || 4),
              className: 'w-20 bg-gray-700 border border-gray-600 rounded px-2 py-1 text-sm text-center',
            })
          ),
          h('div', { className: 'space-y-1' },
            h('label', { className: 'text-xs text-gray-400' }, '스트링 수'),
            h('input', {
              type: 'number', min: 1, max: 24, value: strings,
              onChange: e => setStrings(parseInt(e.target.value) || 8),
              className: 'w-20 bg-gray-700 border border-gray-600 rounded px-2 py-1 text-sm text-center',
            })
          ),
          h('div', { className: 'space-y-1' },
            h('label', { className: 'text-xs text-gray-400' }, 'FC 코드'),
            h('select', {
              value: fcCode, onChange: e => setFcCode(e.target.value),
              className: 'bg-gray-700 border border-gray-600 rounded px-2 py-1 text-sm',
            },
              h('option', { value: 'FC03' }, 'FC03'),
              h('option', { value: 'FC04' }, 'FC04')
            )
          )
        ),

        h('div', { className: 'flex flex-col gap-2' },
          h(Toggle, { label: 'IV Scan 지원', checked: ivScan, onChange: e => setIvScan(e.target.checked) }),
          h(Toggle, { label: 'DER-AVM 제어 레지스터', checked: derAvm, onChange: e => setDerAvm(e.target.checked) }),
          h(Toggle, { label: 'DEA-AVM 모니터링 레지스터', checked: deaAvm, onChange: e => setDeaAvm(e.target.checked) }),
        ),
      ),

      h('div', { className: 'flex items-center gap-3 flex-wrap' },
        h('div', { className: 'flex items-center gap-2' },
          h('span', { className: 'text-sm text-gray-400' }, '모드:'),
          h('div', { className: 'flex rounded overflow-hidden border border-gray-600' },
            ['offline', 'ai'].map(m =>
              h('button', {
                key: m,
                onClick: () => setMode(m),
                className: cls('px-3 py-1 text-sm transition-colors',
                  mode === m ? 'bg-blue-600 text-white' : 'bg-gray-700 text-gray-300 hover:bg-gray-600'),
              }, m === 'offline' ? '오프라인' : 'AI (Claude + 자동재시도)')
            )
          )
        ),

        h(Button, {
          onClick: runStage3,
          disabled: running || !stage2Done,
          variant: 'primary',
        },
          running ? h(Spinner) : null,
          running ? '생성 중...' : '코드 생성 실행'
        ),
      ),

      error && h(Alert, { type: 'error' }, error),

      h('div', { className: 'space-y-1' },
        h('div', { className: 'text-xs text-gray-500 font-medium' }, '진행 로그'),
        h(LogPanel, { messages: logs })
      ),
    ),

    done && h('div', { className: 'bg-gray-800 rounded-lg p-4 space-y-4' },

      // Validation Results
      h('div', { className: 'space-y-2' },
        h('div', { className: 'flex items-center justify-between' },
          h('h3', { className: 'text-sm font-semibold text-gray-300' }, '검증 결과 (12항목)'),
          h('div', { className: 'flex items-center gap-2' },
            h(Badge, {
              text: `${passCount}/${results.length} 통과`,
              color: success ? 'bg-green-700' : 'bg-red-700',
            }),
            success
              ? h(Badge, { text: '✓ 생성 성공', color: 'bg-green-700' })
              : h(Badge, { text: '✗ 검증 실패', color: 'bg-red-700' })
          )
        ),

        h('div', { className: 'grid grid-cols-1 gap-1' },
          results.map(([status, msg], i) =>
            h('div', {
              key: i,
              className: cls('flex items-start gap-2 px-3 py-1.5 rounded text-xs',
                status === 'PASS' ? 'bg-green-900/30 text-green-300' : 'bg-red-900/30 text-red-300'),
            },
              h('span', { className: 'font-bold w-8 shrink-0' }, status),
              h('span', { className: 'text-gray-200' }, msg)
            )
          )
        )
      ),

      // Action Buttons
      h('div', { className: 'flex items-center gap-3 flex-wrap' },
        h(Button, {
          onClick: saveToCommon,
          disabled: saving || !code,
          variant: 'success',
        },
          saving ? h(Spinner) : '💾',
          saving ? '저장 중...' : `common/${protocol}_registers.py 저장`
        ),

        h('a', {
          href: `/api/stage3/download-py?session_id=${sessionId}&protocol_name=${protocol}`,
          className: 'px-3 py-1.5 text-sm rounded bg-gray-700 hover:bg-gray-600 text-gray-200 transition-colors',
        }, '📥 .py 다운로드'),

        h('button', {
          onClick: () => setShowCode(v => !v),
          className: 'px-3 py-1.5 text-sm rounded bg-gray-700 hover:bg-gray-600 text-gray-200 transition-colors',
        }, showCode ? '코드 숨기기' : '코드 보기'),
      ),

      saveMsg && h(Alert, { type: saveMsg.startsWith('✓') ? 'success' : 'error' }, saveMsg),

      showCode && code && h('div', { className: 'space-y-1' },
        h('div', { className: 'text-xs text-gray-500 font-medium' }, '생성된 코드'),
        h('pre', {
          className: 'bg-gray-900 border border-gray-700 rounded p-3 text-xs text-green-300 overflow-x-auto max-h-96 overflow-y-auto',
        }, code)
      ),
    )
  );
}

// ---------------------------------------------------------------------------
// Management Tab (References + AI Settings)
// ---------------------------------------------------------------------------
function ManagementTab({ sessionId }) {
  const [refs, setRefs] = useState([]);
  const [aiKey, setAiKey] = useState('');
  const [aiModel, setAiModel] = useState('claude-opus-4-6');
  const [aiKeySet, setAiKeySet] = useState(false);
  const [aiPreview, setAiPreview] = useState('');
  const [saving, setSaving] = useState(false);
  const [saveMsg, setSaveMsg] = useState('');
  const [deleting, setDeleting] = useState('');

  const MODELS = [
    'claude-opus-4-6',
    'claude-sonnet-4-6',
    'claude-haiku-4-5-20251001',
  ];

  useEffect(() => {
    loadRefs();
    loadAiSettings();
  }, []);

  async function loadRefs() {
    try {
      const data = await apiGet('/api/references');
      setRefs(data.references || []);
    } catch (e) {}
  }

  async function loadAiSettings() {
    try {
      const data = await apiGet('/api/ai-settings');
      setAiKeySet(data.api_key_set);
      setAiPreview(data.api_key_preview || '');
      setAiModel(data.model || 'claude-opus-4-6');
    } catch (e) {}
  }

  async function saveAiSettings() {
    setSaving(true); setSaveMsg('');
    try {
      await apiPut('/api/ai-settings', {
        api_key: aiKey || undefined,
        model: aiModel,
      });
      setSaveMsg('✓ AI 설정 저장 완료');
      setAiKey('');
      await loadAiSettings();
    } catch (e) {
      setSaveMsg(`✗ 오류: ${e.message}`);
    } finally {
      setSaving(false);
    }
  }

  async function deleteRef(name) {
    if (!confirm(`"${name}" 레퍼런스를 삭제하시겠습니까?`)) return;
    setDeleting(name);
    try {
      await apiDelete(`/api/references/${name}`);
      await loadRefs();
    } catch (e) {
      alert(e.message);
    } finally {
      setDeleting('');
    }
  }

  return h('div', { className: 'space-y-6' },

    // AI Settings
    h('div', { className: 'bg-gray-800 rounded-lg p-4 space-y-4' },
      h('h2', { className: 'text-base font-semibold text-gray-200' }, 'AI 설정 (Claude API)'),

      h('div', { className: 'space-y-3' },
        h('div', { className: 'space-y-1' },
          h('label', { className: 'text-xs text-gray-400' },
            aiKeySet ? `API 키 설정됨: ${aiPreview} — 변경하려면 아래에 입력` : 'Anthropic API 키'),
          h('input', {
            type: 'password',
            value: aiKey,
            onChange: e => setAiKey(e.target.value),
            placeholder: aiKeySet ? '변경하지 않으려면 비워두세요' : 'sk-ant-...',
            className: 'w-full bg-gray-700 border border-gray-600 rounded px-3 py-1.5 text-sm font-mono',
          })
        ),

        h('div', { className: 'space-y-1' },
          h('label', { className: 'text-xs text-gray-400' }, 'Claude 모델'),
          h('select', {
            value: aiModel,
            onChange: e => setAiModel(e.target.value),
            className: 'bg-gray-700 border border-gray-600 rounded px-3 py-1.5 text-sm w-full max-w-xs',
          },
            MODELS.map(m => h('option', { key: m, value: m }, m))
          )
        ),

        h('div', { className: 'flex items-center gap-3' },
          h(Button, {
            onClick: saveAiSettings,
            disabled: saving,
            variant: 'primary',
          },
            saving ? h(Spinner) : null,
            saving ? '저장 중...' : 'AI 설정 저장'
          ),
          saveMsg && h('span', {
            className: cls('text-sm', saveMsg.startsWith('✓') ? 'text-green-400' : 'text-red-400'),
          }, saveMsg)
        ),

        h(Alert, { type: 'info' },
          'AI 모드는 Claude API를 사용하여 PDF 파싱 정확도를 향상시킵니다. ' +
          'API 키가 없으면 오프라인 모드만 사용 가능합니다.'
        )
      )
    ),

    // Reference Library
    h('div', { className: 'bg-gray-800 rounded-lg p-4 space-y-4' },
      h('div', { className: 'flex items-center justify-between' },
        h('h2', { className: 'text-base font-semibold text-gray-200' }, '레퍼런스 라이브러리'),
        h('button', {
          onClick: loadRefs,
          className: 'text-xs text-blue-400 hover:text-blue-300',
        }, '↻ 새로고침')
      ),

      refs.length === 0
        ? h('div', { className: 'text-gray-500 text-sm' }, '레퍼런스 없음')
        : h('div', { className: 'overflow-auto rounded border border-gray-700' },
            h('table', { className: 'w-full text-xs' },
              h('thead', { className: 'bg-gray-700' },
                h('tr', null,
                  ['이름', '제조사', '설명', 'MPPT', '스트링', 'FC', '유형', ''].map(col =>
                    h('th', {
                      key: col,
                      className: 'px-3 py-1.5 text-left text-gray-400 whitespace-nowrap',
                    }, col)
                  )
                )
              ),
              h('tbody', null,
                refs.map(ref =>
                  h('tr', {
                    key: ref.name,
                    className: 'border-t border-gray-700 hover:bg-gray-700/50',
                  },
                    h('td', { className: 'px-3 py-1.5 font-mono text-blue-300' }, ref.name),
                    h('td', { className: 'px-3 py-1.5 text-gray-300' }, ref.manufacturer || '-'),
                    h('td', { className: 'px-3 py-1.5 text-gray-400 max-w-xs truncate' },
                      ref.description || '-'),
                    h('td', { className: 'px-3 py-1.5 text-center' }, ref.mppt_count),
                    h('td', { className: 'px-3 py-1.5 text-center' }, ref.string_count),
                    h('td', { className: 'px-3 py-1.5 text-center' }, ref.fc_code),
                    h('td', { className: 'px-3 py-1.5' },
                      ref.builtin
                        ? h(Badge, { text: '내장', color: 'bg-gray-600' })
                        : h(Badge, { text: '사용자', color: 'bg-blue-700' })
                    ),
                    h('td', { className: 'px-3 py-1.5' },
                      !ref.builtin && h(Button, {
                        onClick: () => deleteRef(ref.name),
                        disabled: deleting === ref.name,
                        variant: 'danger',
                        size: 'sm',
                      },
                        deleting === ref.name ? h(Spinner) : '삭제'
                      )
                    )
                  )
                )
              )
            )
          )
    )
  );
}

// ---------------------------------------------------------------------------
// App Root
// ---------------------------------------------------------------------------
function App() {
  const [tab, setTab] = useState(0);
  const sessionId = useMemo(() => getSessionId(), []);
  const [pdfInfo, setPdfInfo] = useState(null);
  const [stage1Done, setStage1Done] = useState(false);
  const [stage2Done, setStage2Done] = useState(false);
  const [wsConnected, setWsConnected] = useState(false);
  const [wsMessages, setWsMessages] = useState([]);
  const wsRef = useRef(null);
  const reconnectRef = useRef(null);

  // WebSocket setup
  function connectWS() {
    const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const url = `${proto}//${window.location.host}/ws`;
    const ws = new WebSocket(url);
    wsRef.current = ws;

    ws.onopen = () => {
      setWsConnected(true);
      if (reconnectRef.current) { clearTimeout(reconnectRef.current); reconnectRef.current = null; }
    };

    ws.onclose = () => {
      setWsConnected(false);
      reconnectRef.current = setTimeout(connectWS, 3000);
    };

    ws.onerror = () => ws.close();

    ws.onmessage = e => {
      try {
        const msg = JSON.parse(e.data);
        if (msg.session_id && msg.session_id !== sessionId) return;
        setWsMessages(prev => [...prev.slice(-200), msg]);

        // Track stage completion
        if (msg.type === 'done' && msg.success) {
          if (msg.stage === 'stage1') setStage1Done(true);
          if (msg.stage === 'stage2') setStage2Done(true);
        }
      } catch {}
    };
  }

  useEffect(() => {
    connectWS();
    return () => {
      if (reconnectRef.current) clearTimeout(reconnectRef.current);
      wsRef.current?.close();
    };
  }, []);

  // Keep only messages for the active stage context
  const tabMessages = useMemo(() => {
    const stageMap = ['stage1', 'stage2', 'stage3', null];
    const stage = stageMap[tab];
    if (!stage) return [];
    return wsMessages.filter(m => m.stage === stage);
  }, [wsMessages, tab]);

  const TABS = [
    { label: 'Stage 1 — PDF 추출', icon: '📄' },
    { label: 'Stage 2 — 자동 매핑', icon: '🔗' },
    { label: 'Stage 3 — 코드 생성', icon: '⚙️' },
    { label: '관리', icon: '🔧' },
  ];

  return h('div', { className: 'min-h-screen bg-gray-900 text-gray-100' },

    // Header
    h('header', { className: 'bg-gray-800 border-b border-gray-700 px-4 py-3 flex items-center justify-between' },
      h('div', { className: 'flex items-center gap-3' },
        h('div', { className: 'text-lg font-bold text-blue-400' }, 'Model Maker Web'),
        h('span', { className: 'text-xs text-gray-500' }, 'v1.0.0'),
        h('span', { className: 'text-xs text-gray-600 hidden sm:block' },
          '— RTU UDP System 인버터 레지스터맵 생성기')
      ),
      h('div', { className: 'flex items-center gap-3' },
        pdfInfo && h('span', { className: 'text-xs text-gray-400 hidden sm:block truncate max-w-xs' },
          `📄 ${pdfInfo.filename}`
        ),
        h('div', { className: 'flex items-center gap-1.5' },
          h('div', {
            className: cls('w-2 h-2 rounded-full', wsConnected ? 'bg-green-500' : 'bg-red-500'),
          }),
          h('span', { className: 'text-xs text-gray-500' },
            wsConnected ? 'WS 연결됨' : 'WS 재연결 중...')
        )
      )
    ),

    // Pipeline Status Bar
    h('div', { className: 'bg-gray-800 border-b border-gray-700 px-4 py-2 flex items-center gap-4 text-xs' },
      h('span', { className: 'text-gray-500' }, '진행 상태:'),
      [
        { label: 'PDF 업로드', done: !!pdfInfo },
        { label: 'Stage 1', done: stage1Done },
        { label: 'Stage 2', done: stage2Done },
      ].map(({ label, done }, i) =>
        h('div', { key: i, className: 'flex items-center gap-1' },
          i > 0 && h('span', { className: 'text-gray-600' }, '→'),
          h('span', {
            className: cls(done ? 'text-green-400' : 'text-gray-500'),
          }, done ? `✓ ${label}` : label)
        )
      )
    ),

    // Tabs
    h('div', { className: 'bg-gray-800 border-b border-gray-700 px-4' },
      h('div', { className: 'flex gap-0' },
        TABS.map((t, i) =>
          h('button', {
            key: i,
            onClick: () => setTab(i),
            className: cls(
              'px-4 py-3 text-sm font-medium transition-colors border-b-2 whitespace-nowrap',
              tab === i
                ? 'border-blue-500 text-blue-400 bg-gray-900/50'
                : 'border-transparent text-gray-400 hover:text-gray-200 hover:border-gray-500'
            ),
          },
            h('span', { className: 'mr-1.5' }, t.icon),
            t.label
          )
        )
      )
    ),

    // Tab Content
    h('main', { className: 'max-w-5xl mx-auto px-4 py-6' },
      tab === 0 && h(Stage1Tab, {
        sessionId,
        pdfInfo,
        onPdfUploaded: info => {
          setPdfInfo(info);
          setStage1Done(false);
          setStage2Done(false);
        },
        wsMessages: tabMessages,
      }),
      tab === 1 && h(Stage2Tab, {
        sessionId,
        stage1Done,
        wsMessages: tabMessages,
      }),
      tab === 2 && h(Stage3Tab, {
        sessionId,
        stage2Done,
        wsMessages: tabMessages,
      }),
      tab === 3 && h(ManagementTab, { sessionId }),
    ),

    // Footer
    h('footer', { className: 'border-t border-gray-800 px-4 py-3 text-center text-xs text-gray-600' },
      'RTU UDP System V1.1.0 — Model Maker Web v1.0.0'
    )
  );
}

// ---------------------------------------------------------------------------
// Mount
// ---------------------------------------------------------------------------
const rootEl = document.getElementById('root');
ReactDOM.createRoot(rootEl).render(h(App));
