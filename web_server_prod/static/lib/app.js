const {
  useState,
  useEffect,
  useRef,
  useCallback,
  useMemo
} = React;

// ---- Helpers ----
const API = window.location.origin + '/api';
const fetcher = async (url, opts) => {
  const r = await fetch(API + url, opts);
  if (!r.ok) {
    let msg = `HTTP ${r.status}`;
    try { const j = await r.json(); msg = j.detail || JSON.stringify(j) || msg; } catch {}
    throw new Error(msg);
  }
  return r.json();
};
const post = (url, body) => fetcher(url, {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json'
  },
  body: JSON.stringify(body)
});
const fmt = (v, d = 1) => v != null ? Number(v).toFixed(d) : '--';
const sanitizeRtuHost = value => String(value || '').trim();
const isValidRtuHost = value => {
  const host = sanitizeRtuHost(value);
  if (!host) return false;
  const lowered = host.toLowerCase();
  if (['localhost', '127.0.0.1', 'local'].includes(lowered)) return true;
  if (/\s|:|\/|\\/.test(host)) return false;
  const ipv4Match = host.match(/^(\d{1,3})(?:\.(\d{1,3})){3}$/);
  if (ipv4Match) {
    return host.split('.').every(part => {
      const n = Number(part);
      return Number.isInteger(n) && n >= 0 && n <= 255;
    });
  }
  return /^(?=.{1,253}$)(?!-)(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)*[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])$/.test(host);
};
const fmtTime = ts => {
  if (!ts) return '--';
  const d = new Date(typeof ts === 'number' ? ts * 1000 : ts);
  return d.toLocaleString('ko-KR');
};
const statusColor = s => s === 'online' ? 'bg-green-500' : s === 'offline' ? 'bg-red-500' : 'bg-yellow-500';
// Model number → display name (matches common/*_registers.py and device_models.ini)
// Naming rule: {Manufacturer}_{kW}_{phase}
const MODEL_NAMES = {
  1:  'Solarize_50_3',
  2:  'Huawei_50_3',
  3:  'Kstar_60_3',
  4:  'Sungrow_50_3',
  5:  'Ekos_10_3',
  6:  'Senergy_50_3',
  7:  'Sofar_50_3',
  8:  'Solis_50_3',
  9:  'Growatt_30_3',
  10: 'CPS_50_3',
  11: 'Sunways_30_3',
  12: 'ABB_50_3',
  13: 'Goodwe_50_3',
};
const MODEL_COLORS = {
  1:  'bg-blue-500',
  2:  'bg-orange-500',
  3:  'bg-purple-500',
  4:  'bg-teal-600',
  5:  'bg-green-500',
  6:  'bg-cyan-500',
  7:  'bg-yellow-500',
  8:  'bg-red-500',
  9:  'bg-lime-500',
  10: 'bg-indigo-500',
  11: 'bg-pink-500',
  12: 'bg-amber-600',
  13: 'bg-rose-500',
};
const MODEL_NAME_MANUFACTURERS = [{
  match: /huawei|sun2000|huaweinew/i,
  name: 'Huawei'
}, {
  match: /kstar/i,
  name: 'Kstar'
}, {
  match: /sungrow|\bsg\d+/i,
  name: 'Sungrow'
}, {
  match: /solarize/i,
  name: 'Solarize'
}, {
  match: /senergy/i,
  name: 'Senergy'
}, {
  match: /growatt/i,
  name: 'Growatt'
}, {
  match: /solis/i,
  name: 'Solis'
}, {
  match: /sofar/i,
  name: 'Sofar'
}, {
  match: /sunways/i,
  name: 'Sunways'
}, {
  match: /cps/i,
  name: 'CPS'
}, {
  match: /ekos/i,
  name: 'Ekos'
}];
const INVERTER_STATUS = {
  0x00: 'Initial',
  0x01: 'Standby',
  0x03: 'On-Grid',
  0x05: 'Fault',
  0x09: 'Shutdown'
};
const INV_STATUS_COLOR = {
  0x00: 'bg-gray-500',
  0x01: 'bg-yellow-500',
  0x03: 'bg-green-500',
  0x05: 'bg-red-500',
  0x09: 'bg-red-700'
};
const EVT_COLORS = {
  CONNECT: 'text-green-400',
  DISCONNECT: 'text-red-400',
  H03_SENT: 'text-blue-400',
  H04_RECV: 'text-purple-400',
  H05_RECV: 'text-orange-400'
};
const getInverterManufacturer = device => {
  const explicit = String(device?.manufacturer || '').trim();
  if (explicit) return explicit;
  const modelName = String(device?.model_name || '').trim();
  if (modelName) {
    const matched = MODEL_NAME_MANUFACTURERS.find(entry => entry.match.test(modelName));
    if (matched) return matched.name;
    const firstToken = modelName.replace(/[_-]+/g, ' ').trim().split(/\s+/)[0];
    if (firstToken) return firstToken;
  }
  return MODEL_NAMES[device?.model] || 'Unknown';
};

// ---- Badge ----
function Badge({
  text,
  color = 'bg-gray-600'
}) {
  return /*#__PURE__*/React.createElement("span", {
    className: `${color} text-white text-xs px-2 py-0.5 rounded-full`
  }, text);
}

// ---- Card wrapper ----
function Card({
  children,
  className = '',
  onClick
}) {
  return /*#__PURE__*/React.createElement("div", {
    onClick: onClick,
    className: `bg-gray-800 rounded-lg shadow p-4 ${className}`
  }, children);
}

// ---- Simple SVG Line Chart ----
function MultiLineChart({
  series,
  labels,
  height = 280
}) {
  // series: [{name,color,data:[{time,value}]}]
  if (!series || !series.length || series[0].data.length < 2) return /*#__PURE__*/React.createElement("div", {
    className: "text-gray-500 text-sm p-4"
  }, "Not enough data");
  const W = 900,
    H = height;
  const pad = {
    t: 25,
    r: 15,
    b: 50,
    l: 55
  };
  const pw = W - pad.l - pad.r,
    ph = H - pad.t - pad.b;
  const n = series[0].data.length;
  // Y range across all series
  let allVals = [];
  series.forEach(s => s.data.forEach(d => allVals.push(d.value)));
  let yMin = Math.min(...allVals),
    yMax = Math.max(...allVals);
  if (yMax === yMin) {
    yMax += 1;
    yMin -= 1;
  }
  const yMargin = (yMax - yMin) * 0.05;
  yMin -= yMargin;
  yMax += yMargin;
  const yRange = yMax - yMin;
  // Grid lines
  const ySteps = 5;
  const gridLines = [];
  const yLabels = [];
  for (let i = 0; i <= ySteps; i++) {
    const y = pad.t + ph * i / ySteps;
    const val = yMax - i / ySteps * yRange;
    gridLines.push(/*#__PURE__*/React.createElement("line", {
      key: `gy${i}`,
      x1: pad.l,
      y1: y,
      x2: pad.l + pw,
      y2: y,
      stroke: "#374151",
      strokeWidth: "0.5"
    }));
    yLabels.push(/*#__PURE__*/React.createElement("text", {
      key: `yl${i}`,
      x: pad.l - 5,
      y: y + 4,
      fill: "#9CA3AF",
      fontSize: "9",
      textAnchor: "end"
    }, val >= 1000 ? (val / 1000).toFixed(1) + 'k' : val.toFixed(1)));
  }
  // X labels (time)
  const xLabels = [];
  const xSteps = Math.min(6, n - 1);
  for (let i = 0; i <= xSteps; i++) {
    const idx = Math.round(i * ((n - 1) / xSteps));
    const x = pad.l + idx / (n - 1) * pw;
    const t = series[0].data[idx]?.time;
    const d = t ? new Date(typeof t === 'number' ? t * 1000 : t) : null;
    const label = d ? `${d.getHours()}:${String(d.getMinutes()).padStart(2, '0')}` : '';
    gridLines.push(/*#__PURE__*/React.createElement("line", {
      key: `gx${i}`,
      x1: x,
      y1: pad.t,
      x2: x,
      y2: pad.t + ph,
      stroke: "#374151",
      strokeWidth: "0.5",
      strokeDasharray: "3,3"
    }));
    xLabels.push(/*#__PURE__*/React.createElement("text", {
      key: `xl${i}`,
      x: x,
      y: H - pad.b + 18,
      fill: "#9CA3AF",
      fontSize: "9",
      textAnchor: "middle"
    }, label));
  }
  // Lines + fills
  const paths = series.map((s, si) => {
    const pts = s.data.map((d, i) => {
      const x = pad.l + i / (n - 1) * pw;
      const y = pad.t + ph - (d.value - yMin) / yRange * ph;
      return {
        x,
        y
      };
    });
    const line = pts.map((p, i) => `${i === 0 ? 'M' : 'L'}${p.x},${p.y}`).join(' ');
    const fill = line + ` L${pts[pts.length - 1].x},${pad.t + ph} L${pts[0].x},${pad.t + ph} Z`;
    return /*#__PURE__*/React.createElement("g", {
      key: si
    }, /*#__PURE__*/React.createElement("path", {
      d: fill,
      fill: s.color,
      opacity: "0.08"
    }), /*#__PURE__*/React.createElement("path", {
      d: line,
      fill: "none",
      stroke: s.color,
      strokeWidth: "1.8"
    }));
  });
  // Legend
  const legend = series.map((s, i) => /*#__PURE__*/React.createElement("g", {
    key: `lg${i}`,
    transform: `translate(${pad.l + i * 100},${H - 12})`
  }, /*#__PURE__*/React.createElement("rect", {
    width: "12",
    height: "3",
    fill: s.color,
    y: "-1"
  }), /*#__PURE__*/React.createElement("text", {
    x: "16",
    y: "3",
    fill: "#D1D5DB",
    fontSize: "9"
  }, s.name)));
  return /*#__PURE__*/React.createElement("svg", {
    viewBox: `0 0 ${W} ${H}`,
    className: "w-full",
    style: {
      maxHeight: H
    }
  }, /*#__PURE__*/React.createElement("rect", {
    width: W,
    height: H,
    fill: "#111827",
    rx: "4"
  }), gridLines, yLabels, xLabels, paths, legend);
}

// ---- WebSocket Hook ----
function useWebSocket(onMessage) {
  const wsRef = useRef(null);
  useEffect(() => {
    const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const url = `${proto}//${window.location.host}/ws`;
    let ws;
    const connect = () => {
      ws = new WebSocket(url);
      ws.onmessage = e => {
        try {
          onMessage(JSON.parse(e.data));
        } catch (err) {}
      };
      ws.onclose = () => setTimeout(connect, 3000);
      ws.onerror = () => ws.close();
      wsRef.current = ws;
    };
    connect();
    return () => {
      if (wsRef.current) wsRef.current.close();
    };
  }, []);
  return wsRef;
}

// ==== OVERVIEW TAB ====
function OverviewTab({
  rtus,
  onSelectRtu
}) {
  const sshIpMap = (() => {
    try {
      return JSON.parse(localStorage.getItem('rtu_ssh_ip_map_v1') || '{}') || {};
    } catch {
      return {};
    }
  })();
  const getMgmtIp = r => (sshIpMap[String(r?.rtu_id)] || '').trim();
  const getDisplayIp = r => getMgmtIp(r) || (r?.ip || '--');
  const getDisplayPort = r => getMgmtIp(r) ? '' : (r?.port || '--');
  const activeRtus = rtus.filter(r => !r.hidden);
  const online = activeRtus.filter(r => r.status === 'online').length;
  const totalSolar = activeRtus.reduce((s, r) => s + (r.total_solar_power || 0), 0);
  const totalGrid = activeRtus.reduce((s, r) => s + (r.total_grid_power || 0), 0);
  // Total Grid Power is shown only when at least one RTU has a power meter
  // or protection relay device (DEVICE_POWER_METER=3 / DEVICE_PROTECTION_RELAY=4).
  // Without any grid measurement device the value is meaningless, so the card
  // is hidden entirely instead of falling back to the solar total.
  const hasGridDevice = activeRtus.some(r => r.has_grid_device);
  const [infoRtu, setInfoRtu] = useState(null);
  const [infoData, setInfoData] = useState(null);
  const showRtuInfo = async (r) => {
    setInfoRtu(r);
    try {
      const d = await fetcher(`/rtus/${r.rtu_id}`);
      setInfoData(d);
    } catch(e) { setInfoData(null); }
  };
  return /*#__PURE__*/React.createElement("div", null,
    infoRtu && /*#__PURE__*/React.createElement("div", {
      className: "fixed inset-0 bg-black/60 z-50 flex items-center justify-center",
      onClick: () => setInfoRtu(null)
    }, /*#__PURE__*/React.createElement("div", {
      className: "bg-gray-800 rounded-lg p-6 max-w-md w-full mx-4 border border-gray-600",
      onClick: e => e.stopPropagation()
    }, /*#__PURE__*/React.createElement("div", {
      className: "flex justify-between items-center mb-4"
    }, /*#__PURE__*/React.createElement("h3", {className: "text-lg font-bold"}, "RTU ", infoRtu.rtu_id, " ", infoRtu.rtu_type && /*#__PURE__*/React.createElement("span", {
      className: `ml-1 text-xs px-1.5 py-0.5 rounded ${infoRtu.rtu_type === 'RIP' ? 'bg-purple-600' : 'bg-teal-600'}`
    }, infoRtu.rtu_type)), /*#__PURE__*/React.createElement("button", {
      className: "text-gray-400 hover:text-white text-xl",
      onClick: () => setInfoRtu(null)
    }, "\u2715")), /*#__PURE__*/React.createElement("div", {className: "space-y-2 text-sm"},
      [['Status', infoRtu.status === 'online' ? 'Online' : 'Offline'],
       ['IP', getDisplayIp(infoRtu)],
       ['UDP Src', `${infoRtu.ip || '--'}:${infoRtu.port || '--'}`],
       ['Model', infoRtu.rtu_info?.model || '--'],
       ['Serial', infoRtu.rtu_info?.serial || '--'],
       ['Phone', infoRtu.rtu_info?.phone || '--'],
       ['Firmware', infoRtu.rtu_info?.firmware || '--'],
       ['Devices', infoRtu.device_count || 0],
       ['Period', infoRtu.avg_interval > 0 ? Math.round(infoRtu.avg_interval / 60) + '\uBD84' : '--'],
       ['Last Seen', fmtTime(infoRtu.last_seen)],
       ['Power', fmt((infoRtu.total_solar_power || 0) / 1000, 2) + ' kW'],
      ].map(([k, v]) => /*#__PURE__*/React.createElement("div", {key: k, className: "flex justify-between border-b border-gray-700/50 pb-1"},
        /*#__PURE__*/React.createElement("span", {className: "text-gray-400"}, k),
        /*#__PURE__*/React.createElement("span", {className: "text-white"}, v)
      ))
    ))), /*#__PURE__*/React.createElement("div", {
    className: "grid grid-cols-2 lg:grid-cols-4 gap-4 mb-6"
  }, /*#__PURE__*/React.createElement(Card, null, /*#__PURE__*/React.createElement("div", {
    className: "text-gray-400 text-sm"
  }, "Total RTUs"), /*#__PURE__*/React.createElement("div", {
    className: "text-3xl font-bold"
  }, activeRtus.length)), /*#__PURE__*/React.createElement(Card, null, /*#__PURE__*/React.createElement("div", {
    className: "text-gray-400 text-sm"
  }, "Online RTUs"), /*#__PURE__*/React.createElement("div", {
    className: "text-3xl font-bold text-green-400"
  }, online)), /*#__PURE__*/React.createElement(Card, null, /*#__PURE__*/React.createElement("div", {
    className: "text-gray-400 text-sm"
  }, "Total Solar Power"), /*#__PURE__*/React.createElement("div", {
    className: "text-3xl font-bold text-yellow-400"
  }, fmt(totalSolar / 1000, 2), " kW")), hasGridDevice && /*#__PURE__*/React.createElement(Card, null, /*#__PURE__*/React.createElement("div", {
    className: "text-gray-400 text-sm"
  }, "Total Grid Power"), /*#__PURE__*/React.createElement("div", {
    className: "text-3xl font-bold text-blue-400"
  }, fmt(totalGrid / 1000, 2), " kW"))), /*#__PURE__*/React.createElement(Card, null, /*#__PURE__*/React.createElement("table", {
    className: "w-full text-sm"
  }, /*#__PURE__*/React.createElement("thead", null, /*#__PURE__*/React.createElement("tr", {
    className: "text-gray-400 border-b border-gray-700"
  }, /*#__PURE__*/React.createElement("th", {
    className: "py-2 text-center",
    style: {width: '60px'}
  }, "Status"), /*#__PURE__*/React.createElement("th", {
    className: "text-left"
  }, "RTU ID"), /*#__PURE__*/React.createElement("th", {
    className: "text-left"
  }, "IP"), /*#__PURE__*/React.createElement("th", {
    className: "text-center"
  }, "Period"), /*#__PURE__*/React.createElement("th", {
    className: "text-left"
  }, "Last Seen"), /*#__PURE__*/React.createElement("th", {
    className: "text-left"
  }, "ON/OFF"), /*#__PURE__*/React.createElement("th", {
    className: "text-left"
  }, "Power"), /*#__PURE__*/React.createElement("th", {
    className: "text-center",
    style: {width: '50px'}
  }))), /*#__PURE__*/React.createElement("tbody", null, rtus.map(r => /*#__PURE__*/React.createElement("tr", {
    key: r.rtu_id,
    className: `border-b border-gray-700/50 hover:bg-gray-700/30 ${r.hidden ? 'opacity-40' : ''}`
  }, /*#__PURE__*/React.createElement("td", {
    className: "py-2 text-center cursor-pointer",
    onClick: () => showRtuInfo(r)
  }, /*#__PURE__*/React.createElement("span", {
    className: `inline-block w-2.5 h-2.5 rounded-full ${statusColor(r.status)}`,
    title: r.hidden ? 'Deleted — permanently offline' : 'Click for RTU info'
  })), /*#__PURE__*/React.createElement("td", {
    className: "font-mono cursor-pointer hover:text-blue-400",
    onClick: () => onSelectRtu(r.rtu_id)
  }, r.rtu_id, " ", r.hidden ? /*#__PURE__*/React.createElement("span", {
    className: "ml-1 text-xs px-1.5 py-0.5 rounded bg-gray-600 text-gray-300"
  }, "DELETED") : r.rtu_type && /*#__PURE__*/React.createElement("span", {
    className: `ml-1 text-xs px-1.5 py-0.5 rounded ${r.rtu_type === 'RIP' ? 'bg-purple-600' : 'bg-teal-600'}`
  }, r.rtu_type)), /*#__PURE__*/React.createElement("td", {
    className: "cursor-pointer hover:text-blue-400",
    onClick: () => onSelectRtu(r.rtu_id)
  }, getDisplayIp(r), getDisplayPort(r) ? ":" : "", getDisplayPort(r)), /*#__PURE__*/React.createElement("td", {
    className: "text-center text-gray-400"
  }, r.avg_interval > 0 ? Math.round(r.avg_interval / 60) + '\uBD84' : '-'), /*#__PURE__*/React.createElement("td", null, fmtTime(r.last_seen)), /*#__PURE__*/React.createElement("td", null,
    /*#__PURE__*/React.createElement("span", { className: "text-green-400" }, r.devices_on || 0),
    /*#__PURE__*/React.createElement("span", { className: "text-gray-500" }, "/"),
    /*#__PURE__*/React.createElement("span", { className: (r.devices_off || 0) > 0 ? "text-red-400" : "text-gray-500" }, r.devices_off || 0)), /*#__PURE__*/React.createElement("td", null, fmt((r.total_solar_power || 0) / 1000, 2), " kW"), /*#__PURE__*/React.createElement("td", {
    className: "text-center"
  }, /*#__PURE__*/React.createElement("button", {
    className: "text-gray-500 hover:text-red-400 text-xs px-1",
    title: "Delete RTU",
    onClick: (e) => {
      e.stopPropagation();
      if (confirm(`RTU ${r.rtu_id} 를 삭제하시겠습니까?`)) {
        fetch(`/api/rtus/${r.rtu_id}`, {method:'DELETE'}).then(res => {
          if (res.ok) location.reload();
          else res.json().then(d => alert(d.detail || 'Error'));
        });
      }
    }
  }, "\u2716"))))))));
}

// ==== DEVICES TAB ====
function InverterCard({
  dev
}) {
  const d = dev.data || dev || {};
  const m = d.model || dev.model || 1;
  const manufacturer = getInverterManufacturer(d);
  const isHuawei = m === 2 || String(d.protocol || dev.protocol || '').toLowerCase() === 'huawei' || manufacturer === 'Huawei';
  // Monitor 값이 있으면 AC 데이터를 Monitor로 오버라이드 + PV 비례 조정
  const monRatio = d.mon && d.ac_power > 0 ? (d.mon.active_power_kw * 1000) / d.ac_power : 1;
  const acI_r = d.mon ? fmt(d.mon.current_r, 1) : fmt((d.r_current || 0) / 10, 1);
  const acI_s = d.mon ? fmt(d.mon.current_s, 1) : fmt((d.s_current || 0) / 10, 1);
  const acI_t = d.mon ? fmt(d.mon.current_t, 1) : fmt((d.t_current || 0) / 10, 1);
  const acV_r = d.mon ? fmt(d.mon.voltage_rs, 1) : fmt((d.r_voltage || 0) / (isHuawei ? 10 : 1), 1);
  const acV_s = d.mon ? fmt(d.mon.voltage_st, 1) : fmt((d.s_voltage || 0) / (isHuawei ? 10 : 1), 1);
  const acV_t = d.mon ? fmt(d.mon.voltage_tr, 1) : fmt((d.t_voltage || 0) / (isHuawei ? 10 : 1), 1);
  const acPower = d.mon ? fmt(d.mon.active_power_kw, 2) : fmt((d.ac_power || 0) / 1000, 2);
  const acPF = d.mon ? fmt(d.mon.power_factor, 3) : fmt(d.power_factor, 3);
  const acFreq = d.mon ? fmt(d.mon.frequency, 1) : fmt(d.frequency);
  const pvPower = d.mon ? fmt((d.pv_power || 0) / 1000 * monRatio, 2) : fmt((d.pv_power || 0) / 1000, 2);
  const pvCurrent = d.mon ? fmt((d.pv_current || 0) / 10 * monRatio, 1) : fmt((d.pv_current || 0) / 10, 1);
  return /*#__PURE__*/React.createElement(Card, null, /*#__PURE__*/React.createElement("div", {
    className: "flex justify-between items-center mb-3"
  }, /*#__PURE__*/React.createElement("span", {
    className: "font-bold"
  }, "Inverter #", dev.device_number), /*#__PURE__*/React.createElement("div", {
    className: "flex gap-1"
  }, /*#__PURE__*/React.createElement(Badge, {
    text: manufacturer,
    color: MODEL_COLORS[m] || 'bg-gray-600'
  }), d.body_type === -4 ? /*#__PURE__*/React.createElement(Badge, {
    text: '야간대기',
    color: 'bg-yellow-600'
  }) : d.error || d.body_type < 0 ? /*#__PURE__*/React.createElement(Badge, {
    text: 'Comm Fail',
    color: 'bg-red-600'
  }) : /*#__PURE__*/React.createElement(Badge, {
    text: INVERTER_STATUS[d.status] || 'Unknown',
    color: INV_STATUS_COLOR[d.status] || 'bg-gray-600'
  }), d.der_avm && /*#__PURE__*/React.createElement(Badge, {
    text: 'DER',
    color: 'bg-emerald-600'
  }), d.iv_scan && /*#__PURE__*/React.createElement(Badge, {
    text: 'IV',
    color: 'bg-violet-600'
  }), d.ctrl && d.ctrl.active_power_pct > 0 && d.ctrl.active_power_pct < 1000 && /*#__PURE__*/React.createElement(Badge, {
    text: `P${fmt((d.ctrl.active_power_pct || 0) / 10, 0)}%`,
    color: 'bg-orange-600'
  }), d.ctrl && d.ctrl.power_factor > 0 && d.ctrl.power_factor < 1000 && /*#__PURE__*/React.createElement(Badge, {
    text: `PF${fmt((d.ctrl.power_factor || 0) / 1000, 2)}`,
    color: 'bg-cyan-600'
  }), d.ctrl && d.ctrl.reactive_power_pct > 0 && /*#__PURE__*/React.createElement(Badge, {
    text: `Q${fmt((d.ctrl.reactive_power_pct || 0) / 10, 0)}%`,
    color: 'bg-purple-600'
  }))), /*#__PURE__*/React.createElement("div", {
    className: "grid grid-cols-3 gap-2 text-xs"
  }, /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "PV V:"), " ", fmt(d.pv_voltage), " V"), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "PV I:"), " ", pvCurrent, " A"), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "PV P:"), " ", pvPower, " kW"), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "AC R V:"), " ", acV_r, " V"), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "AC S V:"), " ", acV_s, " V"), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "AC T V:"), " ", acV_t, " V"), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "AC R I:"), " ", acI_r, " A"), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "AC S I:"), " ", acI_s, " A"), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "AC T I:"), " ", acI_t, " A"), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "AC Power:"), " ", acPower, " kW"), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "PF:"), " ", acPF), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "Freq:"), " ", acFreq, " Hz"), /*#__PURE__*/React.createElement("div", {
    className: "col-span-3"
  }, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "Cumulative Energy:"), " ", fmt((d.cumulative_energy || 0) / 1000, 1), " kWh")), d.mppt && d.mppt.length > 0 && /*#__PURE__*/React.createElement("div", {
    className: "mt-2 text-xs"
  }, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "MPPT:"), d.mppt.map((m, i) => /*#__PURE__*/React.createElement("span", {
    key: i,
    className: "ml-2 text-cyan-400"
  }, "CH", i + 1, ": ", fmt(m.voltage, 1), "V/", fmt(m.current, 1), "A"))), d.strings && d.strings.length > 0 && /*#__PURE__*/React.createElement("div", {
    className: "mt-1 text-xs"
  }, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "Strings(", d.strings.length, "):"), d.strings.map((v, i) => /*#__PURE__*/React.createElement("span", {
    key: i,
    className: "ml-1 text-green-400"
  }, fmt(v, 1), "A"))), d.ctrl && /*#__PURE__*/React.createElement("div", {
    className: "mt-2 text-xs border-t border-gray-700 pt-2"
  }, /*#__PURE__*/React.createElement("span", {
    className: "text-yellow-400"
  }, "Control:"), " ", d.ctrl.active_power_pct === 0 && d.ctrl.reactive_power_pct === 0 && d.ctrl.operation_mode === 0 && d.ctrl.power_factor === 0 ? /*#__PURE__*/React.createElement("span", {className: "text-gray-500"}, "Not Supported") : [d.ctrl.on_off === 0 ? 'ON' : 'OFF', ", PF=", fmt((d.ctrl.power_factor || 1000) / 1000, 3), ", Active=", fmt((d.ctrl.active_power_pct || 0) / 10, 1), "%, Reactive=", fmt((d.ctrl.reactive_power_pct || 0) / 10, 1), "%, Mode=", d.ctrl.operation_mode === 2 ? 'DER-AVM' : 'Self']), d.mon && /*#__PURE__*/React.createElement("div", {
    className: "mt-1 text-xs"
  }, /*#__PURE__*/React.createElement("span", {
    className: "text-purple-400"
  }, "Monitor:"), " I=", fmt(d.mon.current_r, 1), "/", fmt(d.mon.current_s, 1), "/", fmt(d.mon.current_t, 1), "A, V=", fmt(d.mon.voltage_rs, 1), "/", fmt(d.mon.voltage_st, 1), "/", fmt(d.mon.voltage_tr, 1), "V, P=", fmt(d.mon.active_power_kw, 1), "kW, Q=", fmt(d.mon.reactive_power_var, 0), "Var, PF=", fmt(d.mon.power_factor, 3), ", F=", fmt(d.mon.frequency, 1), "Hz"));
}
function RelayCard({
  dev
}) {
  const d = dev.data || dev || {};
  return /*#__PURE__*/React.createElement(Card, null, /*#__PURE__*/React.createElement("div", {
    className: "flex justify-between items-center mb-3"
  }, /*#__PURE__*/React.createElement("span", {
    className: "font-bold"
  }, "Relay #", dev.device_number), /*#__PURE__*/React.createElement(Badge, {
    text: "KDU-300",
    color: "bg-teal-600"
  })), /*#__PURE__*/React.createElement("div", {
    className: "grid grid-cols-3 gap-2 text-xs"
  }, /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "R V:"), " ", fmt(d.r_voltage), " V"), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "S V:"), " ", fmt(d.s_voltage), " V"), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "T V:"), " ", fmt(d.t_voltage), " V"), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "R I:"), " ", fmt(d.r_current), " A"), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "S I:"), " ", fmt(d.s_current), " A"), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "T I:"), " ", fmt(d.t_current), " A"), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "R P:"), " ", fmt((d.r_power || 0) / 1000, 2), " kW"), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "S P:"), " ", fmt((d.s_power || 0) / 1000, 2), " kW"), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "T P:"), " ", fmt((d.t_power || 0) / 1000, 2), " kW"), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "Total P:"), " ", fmt((d.total_active_power || 0) / 1000, 2), " kW"), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "PF:"), " ", fmt(d.avg_power_factor, 3)), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "Freq:"), " ", fmt(d.frequency), " Hz"), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "Import:"), " ", fmt((d.received_energy || 0) / 1000, 1), " kWh"), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "Export:"), " ", fmt((d.sent_energy || 0) / 1000, 1), " kWh")), /*#__PURE__*/React.createElement("div", {
    className: "mt-2 text-xs flex gap-4"
  }, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "DO: ", /*#__PURE__*/React.createElement("span", {
    className: "text-white font-mono"
  }, d.do_status != null ? '0x' + d.do_status.toString(16).toUpperCase() : '--')), /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "DI: ", /*#__PURE__*/React.createElement("span", {
    className: "text-white font-mono"
  }, d.di_status != null ? '0x' + d.di_status.toString(16).toUpperCase() : '--'))));
}
function WeatherCard({
  dev
}) {
  const d = dev.data || dev || {};
  return /*#__PURE__*/React.createElement(Card, null, /*#__PURE__*/React.createElement("div", {
    className: "flex justify-between items-center mb-3"
  }, /*#__PURE__*/React.createElement("span", {
    className: "font-bold"
  }, "Weather #", dev.device_number), /*#__PURE__*/React.createElement(Badge, {
    text: "SEM5046",
    color: "bg-green-700"
  })), /*#__PURE__*/React.createElement("div", {
    className: "grid grid-cols-3 gap-2 text-xs"
  }, /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "Radiation:"), " ", fmt(d.horizontal_radiation, 0), " W/m\xB2"), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "Inclined:"), " ", fmt(d.inclined_radiation, 0), " W/m\xB2"), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "Air Temp:"), " ", fmt(d.air_temp), " \xB0C"), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "Module T1:"), " ", fmt(d.module_temp_1), " \xB0C"), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "Module T2:"), " ", fmt(d.module_temp_2), " \xB0C"), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "Module T3:"), " ", fmt(d.module_temp_3), " \xB0C"), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "Module T4:"), " ", fmt(d.module_temp_4), " \xB0C"), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "Humidity:"), " ", fmt(d.air_humidity), " %"), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "Pressure:"), " ", fmt(d.air_pressure, 0), " hPa"), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "Wind:"), " ", fmt(d.wind_speed), " m/s"), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "Direction:"), " ", fmt(d.wind_direction, 0), "\xB0"), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("span", {
    className: "text-gray-400"
  }, "Accum H:"), " ", fmt(d.horizontal_accum, 0), " Wh/m\xB2")));
}
function DevicesTab({
  selectedRtu,
  rtus,
  wsUpdateCounter,
  onRtuChange
}) {
  const [rtuId, setRtuId] = useState(selectedRtu || '');
  const [devices, setDevices] = useState([]);
  useEffect(() => {
    if (selectedRtu) setRtuId(selectedRtu);
  }, [selectedRtu]);
  useEffect(() => {
    if (!rtuId) return;
    const parseDevices = d => {
      if (d?.devices && typeof d.devices === 'object' && !Array.isArray(d.devices)) {
        return Object.values(d.devices).map(v => ({
          device_type: v.device_type,
          device_number: v.device_number,
          model: v.data?.model,
          ...v.data
        }));
      }
      return Array.isArray(d) ? d : [];
    };
    fetcher(`/rtus/${rtuId}/devices`).then(d => setDevices(parseDevices(d))).catch(() => setDevices([]));
  }, [rtuId, wsUpdateCounter]);
  // Show only devices with actual data (ac_r_voltage or frequency > 0, or has cumulative energy)
  const hasData = d => d.error || d.body_type < 0 || d.r_voltage > 0 || d.frequency > 0 || d.cumulative_energy > 0 || d.pv_voltage > 0 || d.status > 0;
  const inverters = devices.filter(d => d.device_type === 1 && hasData(d)).sort((a, b) => a.device_number - b.device_number);
  const relays = devices.filter(d => d.device_type === 4 && hasData(d)).sort((a, b) => a.device_number - b.device_number);
  const weathers = devices.filter(d => d.device_type === 5).sort((a, b) => a.device_number - b.device_number);
  return /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("div", {
    className: "mb-4"
  }, /*#__PURE__*/React.createElement("select", {
    className: "bg-gray-700 text-white rounded px-3 py-2",
    value: rtuId,
    onChange: e => { setRtuId(e.target.value); onRtuChange && onRtuChange(e.target.value); }
  }, /*#__PURE__*/React.createElement("option", {
    value: ""
  }, "-- Select RTU --"), rtus.map(r => /*#__PURE__*/React.createElement("option", {
    key: r.rtu_id,
    value: r.rtu_id
  }, r.rtu_id, " (", r.status, ")")))), inverters.length > 0 && /*#__PURE__*/React.createElement("h3", {
    className: "text-lg font-semibold mb-2"
  }, "Inverters"), /*#__PURE__*/React.createElement("div", {
    className: "grid grid-cols-1 lg:grid-cols-2 gap-4 mb-4"
  }, inverters.map(d => /*#__PURE__*/React.createElement(InverterCard, {
    key: d.device_number,
    dev: d
  }))), relays.length > 0 && /*#__PURE__*/React.createElement("h3", {
    className: "text-lg font-semibold mb-2"
  }, "Relays"), /*#__PURE__*/React.createElement("div", {
    className: "grid grid-cols-1 lg:grid-cols-2 gap-4 mb-4"
  }, relays.map(d => /*#__PURE__*/React.createElement(RelayCard, {
    key: d.device_number,
    dev: d
  }))), weathers.length > 0 && /*#__PURE__*/React.createElement("h3", {
    className: "text-lg font-semibold mb-2"
  }, "Weather Stations"), /*#__PURE__*/React.createElement("div", {
    className: "grid grid-cols-1 lg:grid-cols-2 gap-4"
  }, weathers.map(d => /*#__PURE__*/React.createElement(WeatherCard, {
    key: d.device_number,
    dev: d
  }))), !rtuId && /*#__PURE__*/React.createElement("div", {
    className: "text-gray-500 text-center py-8"
  }, "Select an RTU to view devices"), rtuId && devices.length === 0 && /*#__PURE__*/React.createElement("div", {
    className: "text-gray-500 text-center py-8"
  }, "No devices found"));
}

// ==== CONTROL TAB ====
function ControlTab({
  rtus,
  selectedRtu,
  wsEvents = [],
  onRtuChange
}) {
  const [rtuId, setRtuId] = useState(selectedRtu || '');
  useEffect(() => {
    if (selectedRtu) setRtuId(selectedRtu);
  }, [selectedRtu]);
  // (RTU change cleanup moved after prevEvtLen/lastLoggedRef declarations)
  const [deviceNum, setDeviceNum] = useState('');  // for IV Scan only
  const [selectedDevs, setSelectedDevs] = useState(new Set());
  const [devices, setDevices] = useState([]);
  const [onOff, setOnOff] = useState(0);
  const [ivData, setIvData] = useState(null);
  const ivCanvasRef = useRef(null);
  const [activePower, setActivePower] = useState(1000);
  const [powerFactor, setPowerFactor] = useState(1000);
  const [reactivePower, setReactivePower] = useState(0);
  const [logs, setLogs] = useState([]);
  const logRef = useRef(null);
  useEffect(() => {
    if (!rtuId) return;
    const pd = d => {
      if (d?.devices && typeof d.devices === 'object' && !Array.isArray(d.devices)) return Object.values(d.devices).map(v => ({
        device_type: v.device_type,
        device_number: v.device_number,
        ...v.data
      }));
      return Array.isArray(d) ? d : [];
    };
    fetcher(`/rtus/${rtuId}/devices`).then(d => {
      const devs = pd(d);
      setDevices(devs);
      // Sort by device_number so the initial default picks INV#1 (not
      // whichever inverter happens to be first in the API response).
      const invs = devs.filter(dd => dd.device_type === 1)
        .sort((a, b) => (a.device_number || 0) - (b.device_number || 0));
      if (invs.length > 0) {
        setSelectedDevs(new Set([invs[0].device_number]));
        if (!deviceNum) setDeviceNum(String(invs[0].device_number));
      } else {
        setSelectedDevs(new Set());
      }
    }).catch(() => {});
  }, [rtuId]);

  // Keep the IV Scan target dropdown (deviceNum) in sync with the first
  // inverter selected via the checkboxes. If the user checks INV#1, IV Scan
  // should target INV#1 even though it has its own single-target dropdown.
  useEffect(() => {
    if (selectedDevs.size === 0) return;
    const first = [...selectedDevs].sort((a, b) => a - b)[0];
    setDeviceNum(String(first));
  }, [selectedDevs]);

  // Load control values from DB ONCE when RTU changes (not on selectedDevs change)
  const lastSendTime = useRef(0);
  const controlLoaded = useRef(false);
  useEffect(() => {
    controlLoaded.current = false;  // reset on RTU change
  }, [rtuId]);
  useEffect(() => {
    if (!rtuId || selectedDevs.size === 0 || controlLoaded.current) return;
    controlLoaded.current = true;
    const devNum = [...selectedDevs][0];
    fetcher(`/control/status/${rtuId}/${devNum}`).then(d => {
      if (d) {
        const ap = d.active_power_pct;
        setActivePower((ap !== null && ap !== undefined && ap > 0) ? ap : 1000);
        const pf = d.power_factor;
        setPowerFactor((pf !== null && pf !== undefined && pf !== 0) ? pf : 1000);
        setReactivePower(d.reactive_power_pct ?? 0);
      }
    }).catch(() => {});
  }, [rtuId, selectedDevs]);

  const addLog = msg => {
    setLogs(p => {
      const next = [...p, {
        time: new Date().toLocaleTimeString(),
        msg
      }];
      return next.length > 500 ? next.slice(-500) : next;
    });
    setTimeout(() => {
      if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight;
    }, 50);
  };

  // H04/H05 이벤트를 Response Log에 자동 추가
  const prevEvtLen = useRef(0);
  const formatResponseLog = (e) => {
    const EVT_COLORS = {
      'h04_response': 'text-purple-400',
      'control_check': 'text-cyan-400',
      'control_result': 'text-green-400',
      'comm_fail': 'text-red-400',
      'nighttime_standby': 'text-yellow-400',
      'comm_restored': 'text-emerald-400',
      'inverter_model': 'text-blue-400',
      'H03_SENT': 'text-orange-400',
      'rtu_event': 'text-red-400',
    };
    const CTRL_NAMES = {1:'Reboot',2:'RTU Info',11:'INV Model',12:'IV Scan',13:'Status Check',14:'Init Reset',15:'On/Off',16:'Active Power',17:'Power Factor',18:'Reactive Power'};
    const RESP_NAMES = {0:'SUCCESS',1:'FAIL',2:'BUSY',3:'UNSUPPORTED'};
    const color = EVT_COLORS[e.event_type] || 'text-yellow-400';
    const tag = `<span class="${color}">[${e.event_type}]</span>`;
    // Parse H04 response dict string
    if (e.event_type === 'h04_response' && e.detail) {
      try {
        const d = typeof e.detail === 'string' ? JSON.parse(e.detail.replace(/'/g,'"')) : e.detail;
        if (d && d.control_type !== undefined) {
          const ctrlName = CTRL_NAMES[d.control_type] || `Type${d.control_type}`;
          const respName = RESP_NAMES[d.response] || `Code${d.response}`;
          const respColor = d.response === 0 ? 'text-green-400' : 'text-red-400';
          return `${tag} <span class="text-gray-300">INV#${d.device_number||'?'}</span> ${ctrlName} → <span class="${respColor}">${respName}</span> (val=${d.control_value})`;
        }
      } catch(_) {}
      // Fallback: already formatted string like "Inverter Model: value=0, resp=SUCCESS"
      return `${tag} ${e.detail}`;
    }
    return `${tag} ${e.detail}`;
  };
  // Control Response Log shows ONLY user-initiated power-control round-trips
  // (Active Power / PF / Q / ON-OFF / Init Reset / IV Scan / RTU Info).
  // These system / metadata events go ONLY to the Events tab, not here:
  //   - rtu_event       (RTU First Connection, port open, etc.)
  //   - inverter_model  (per-inverter model name / serial metadata — now
  //                      only emitted on user click, but user requested
  //                      that it be visible in Events tab instead of
  //                      cluttering the Control Response Log)
  const RESPONSE_LOG_TYPES = new Set([
    'h04_response', 'H03_SENT',
    'control_check', 'control_result',
    'iv_scan_success', 'iv_scan_data', 'iv_scan_complete',
    'rtu_info', 'inverter_model',
    'modbus_test_result',
  ]);
  // Deduplicate: track last logged event per type to suppress repeats
  const lastLoggedRef = useRef({});
  // Clear Response Log and reset event pointer when RTU changes
  useEffect(() => {
    setLogs([]);
    prevEvtLen.current = wsEvents.length;
    lastLoggedRef.current = {};
  }, [rtuId]);
  useEffect(() => {
    if (wsEvents.length > prevEvtLen.current) {
      const newEvts = wsEvents.slice(prevEvtLen.current);
      newEvts.forEach(e => {
        if (String(e.rtu_id) === rtuId && RESPONSE_LOG_TYPES.has(e.event_type)) {
          // Deduplicate: skip if same type+detail within 2 seconds
          // Control responses (control_check, control_result, h04_response) are never deduped
          const NO_DEDUP = new Set(['control_check', 'control_result', 'h04_response', 'H03_SENT', 'iv_scan_data', 'iv_scan_success', 'iv_scan_complete']);
          if (!NO_DEDUP.has(e.event_type)) {
            const dedupKey = `${e.event_type}:${e.detail}`;
            const now = Date.now();
            if (lastLoggedRef.current[dedupKey] && now - lastLoggedRef.current[dedupKey] < 2000) {
              return; // skip duplicate
            }
            lastLoggedRef.current[dedupKey] = now;
          }
          addLog(formatResponseLog(e));
        }
      });
      prevEvtLen.current = wsEvents.length;
    }
  }, [wsEvents, rtuId]);

  // control_check events are shown in Response Log only.
  // Slider values are ONLY changed by: user input, Send button, Init Reset.

  // IV Scan complete → fetch data
  useEffect(() => {
    if (wsEvents.length === 0 || !rtuId) return;
    const last = wsEvents[wsEvents.length - 1];
    if (last.event_type === 'iv_scan_complete' && String(last.rtu_id) === rtuId) {
      fetcher(`/rtus/${rtuId}/iv_scan`).then(d => {
        if (d?.available) setIvData(d);
      }).catch(() => {});
    }
  }, [wsEvents]);

  // Draw graph when ivData changes and canvas is ready
  useEffect(() => {
    if (ivData) setTimeout(() => drawIVCurve(ivData), 100);
  }, [ivData]);
  const drawIVCurve = data => {
    const canvas = ivCanvasRef.current;
    if (!canvas || !data?.strings) return;
    const dpr = window.devicePixelRatio || 1;
    const dispW = canvas.offsetWidth;
    const dispH = 350;
    canvas.width = dispW * dpr;
    canvas.height = dispH * dpr;
    canvas.style.height = dispH + 'px';
    const ctx = canvas.getContext('2d');
    ctx.scale(dpr, dpr);
    const w = dispW;
    const h = dispH;
    ctx.fillStyle = '#111827';
    ctx.fillRect(0, 0, w, h);
    const pad = {
      t: 35,
      r: 25,
      b: 45,
      l: 55
    };
    const pw = w - pad.l - pad.r;
    const ph = h - pad.t - pad.b;
    // Find min/max V/I
    let minV = Infinity,
      maxV = 0,
      maxI = 0;
    const strKeys = Object.keys(data.strings).sort((a, b) => Number(a) - Number(b));
    strKeys.forEach(k => {
      data.strings[k].forEach(p => {
        if (p.voltage > 0 && p.voltage < minV) minV = p.voltage;
        if (p.voltage > maxV) maxV = p.voltage;
        if (p.current > maxI) maxI = p.current;
      });
    });
    minV = Math.floor(minV / 50) * 50;
    maxV = Math.ceil(maxV / 50) * 50 || 700;
    maxI = Math.ceil(maxI / 2) * 2 || 14;
    const vRange = maxV - minV;
    // Grid
    ctx.strokeStyle = '#374151';
    ctx.lineWidth = 0.5;
    ctx.font = '11px monospace';
    ctx.fillStyle = '#9CA3AF';
    ctx.textAlign = 'center';
    const vSteps = 6;
    const iSteps = 5;
    for (let i = 0; i <= vSteps; i++) {
      const x = pad.l + pw * i / vSteps;
      ctx.beginPath();
      ctx.moveTo(x, pad.t);
      ctx.lineTo(x, pad.t + ph);
      ctx.stroke();
      ctx.fillText((minV + vRange * i / vSteps).toFixed(0), x, pad.t + ph + 18);
    }
    ctx.textAlign = 'right';
    for (let i = 0; i <= iSteps; i++) {
      const y = pad.t + ph * i / iSteps;
      ctx.beginPath();
      ctx.moveTo(pad.l, y);
      ctx.lineTo(pad.l + pw, y);
      ctx.stroke();
      ctx.fillText((maxI * (iSteps - i) / iSteps).toFixed(1), pad.l - 6, y + 4);
    }
    // Axis labels
    ctx.fillStyle = '#9CA3AF';
    ctx.font = '12px sans-serif';
    ctx.textAlign = 'center';
    ctx.fillText('Voltage (V)', pad.l + pw / 2, h - 8);
    ctx.save();
    ctx.translate(14, pad.t + ph / 2);
    ctx.rotate(-Math.PI / 2);
    ctx.fillText('Current (A)', 0, 0);
    ctx.restore();
    // Curves
    const colors = ['#3B82F6', '#EF4444', '#10B981', '#F59E0B', '#8B5CF6', '#EC4899', '#06B6D4', '#F97316'];
    strKeys.forEach((k, si) => {
      const pts = data.strings[k];
      if (!pts.length) return;
      ctx.strokeStyle = colors[si % colors.length];
      ctx.lineWidth = 2;
      ctx.beginPath();
      pts.forEach((p, i) => {
        const x = pad.l + (p.voltage - minV) / vRange * pw;
        const y = pad.t + ph - p.current / maxI * ph;
        i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
      });
      ctx.stroke();
    });
    // Legend
    ctx.font = '11px sans-serif';
    ctx.textAlign = 'left';
    strKeys.forEach((k, si) => {
      const lx = pad.l + 8 + si * 72;
      const ly = pad.t + 10;
      ctx.fillStyle = colors[si % colors.length];
      ctx.fillRect(lx, ly - 2, 14, 4);
      ctx.fillStyle = '#D1D5DB';
      ctx.fillText(`STR ${k}`, lx + 18, ly + 3);
    });
    // Title
    ctx.fillStyle = '#F3F4F6';
    ctx.font = 'bold 13px sans-serif';
    const mName = data.model_name || MODEL_NAMES[data.model] || 'Unknown';
    ctx.fillText(`IV Curve - INV#${data.device_number} (${mName})`, pad.l, pad.t - 12);
  };
  const downloadIVCsv = () => {
    if (!ivData?.strings) return;
    // Save to server IVscandata/ directory
    if (rtuId) {
      post(`/rtus/${rtuId}/iv_scan/save`, {}).then(d => {
        addLog(`IV CSV saved: ${d.filename}`);
        // Also download to browser
        const blob = new Blob([_buildCsv()], {
          type: 'text/csv'
        });
        const a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        a.download = d.filename;
        a.click();
      }).catch(() => {
        // Fallback: browser-only download
        const fname = _buildFilename();
        const blob = new Blob([_buildCsv()], {
          type: 'text/csv'
        });
        const a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        a.download = fname;
        a.click();
      });
    } else {
      const fname = _buildFilename();
      const blob = new Blob([_buildCsv()], {
        type: 'text/csv'
      });
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = fname;
      a.click();
    }
  };
  const _buildFilename = () => {
    const now = new Date();
    const ts = now.getFullYear() + String(now.getMonth() + 1).padStart(2, '0') + String(now.getDate()).padStart(2, '0') + '_' + String(now.getHours()).padStart(2, '0') + String(now.getMinutes()).padStart(2, '0') + String(now.getSeconds()).padStart(2, '0');
    const model = (ivData.model_name || MODEL_NAMES[ivData.model] || 'Unknown').replace(/\s+/g, '_');
    return `${ts}-${rtuId || 0}-${model}_IV.csv`;
  };
  const _buildCsv = () => {
    let csv = 'String,Point,Voltage(V),Current(A)\n';
    Object.keys(ivData.strings).sort((a, b) => Number(a) - Number(b)).forEach(k => {
      ivData.strings[k].forEach((p, i) => {
        csv += `${k},${i + 1},${p.voltage},${p.current}\n`;
      });
    });
    return csv;
  };
  const [ivFiles, setIvFiles] = useState([]);
  const loadIvFileList = () => {
    fetcher('/iv_scan/files').then(d => setIvFiles(d?.files || [])).catch(() => {});
  };
  const loadIvFile = fname => {
    fetcher(`/iv_scan/files/${fname}`).then(d => {
      if (d?.available) {
        setIvData(d);
        addLog(`IV loaded: ${fname}`);
      }
    }).catch(() => addLog('Failed to load IV file'));
  };
  const refreshDevices = () => {
    if (!rtuId) return;
    const pd = d => {
      if (d?.devices && typeof d.devices === 'object' && !Array.isArray(d.devices)) return Object.values(d.devices).map(v => ({
        device_type: v.device_type,
        device_number: v.device_number,
        ...v.data
      }));
      return Array.isArray(d) ? d : [];
    };
    fetcher(`/rtus/${rtuId}/devices`).then(d => {
      setDevices(pd(d));
    }).catch(() => {});
  };
  const sendControl = async (endpoint, body, opts = {}) => {
    const ENDPOINT_NAMES = {on_off:'On/Off',active_power:'Active Power',power_factor:'Power Factor',reactive_power:'Reactive Power',rtu_info:'RTU Info',model_info:'INV Model',reboot:'Reboot',iv_scan:'IV Scan'};
    const name = ENDPOINT_NAMES[endpoint] || endpoint;
    try {
      addLog(`<span class="text-orange-400">▶ ${name}</span> INV#${body.device_num||'ALL'} → Sent`);
      const r = await post(`/control/${endpoint}`, body);
      if (r.status !== 'sent') addLog(`<span class="text-yellow-400">⚠ ${r.status || JSON.stringify(r)}</span>`);
      // Preserve sent value in UI (prevent race condition with DB fetch)
      lastSendTime.current = Date.now();
      if (endpoint === 'active_power' && body.value !== undefined) setActivePower(body.value);
      if (endpoint === 'power_factor' && body.value !== undefined) setPowerFactor(body.value);
      if (endpoint === 'reactive_power' && body.value !== undefined) setReactivePower(body.value);
      if (endpoint === 'on_off' && body.value !== undefined) setOnOff(body.value);
      // Skip redundant polling when sending to multiple devices — the final
      // refreshDevices at the end of sendToSelected handles it once.
      if (!opts.skipRefresh) {
        setTimeout(refreshDevices, 1500);
      }
    } catch (e) {
      addLog(`<span class="text-red-400">✗ ${name} Error: ${e.message}</span>`);
    }
  };
  const sendToSelected = async (endpoint, makeBody) => {
    const devNums = [...selectedDevs].sort((a, b) => a - b);
    if (devNums.length === 0) { addLog('No inverters selected'); return; }
    const multi = devNums.length > 1;
    for (const dn of devNums) {
      // Skip per-command refreshDevices polling when sending to many devices —
      // 3x polls per inverter (11 inv → 33 fetches) would thrash the UI and
      // interfere with WebSocket event processing, dropping control responses.
      await sendControl(endpoint, makeBody(dn), { skipRefresh: multi });
      if (multi) await new Promise(r => setTimeout(r, 250));
    }
    // Single final refresh after all commands complete
    if (multi) setTimeout(refreshDevices, 1500);
  };
  const inverters = devices.filter(d => d.device_type === 1)
    .sort((a, b) => (a.device_number || 0) - (b.device_number || 0));
  const allSelected = inverters.length > 0 && inverters.every(d => selectedDevs.has(d.device_number));
  const toggleDev = (num) => {
    setSelectedDevs(prev => { const s = new Set(prev); s.has(num) ? s.delete(num) : s.add(num); return s; });
  };
  const toggleAll = () => {
    if (allSelected) setSelectedDevs(new Set());
    else setSelectedDevs(new Set(inverters.map(d => d.device_number)));
  };
  const selLabel = () => { const n = [...selectedDevs].sort((a,b)=>a-b); return n.length === 0 ? '' : n.map(d=>'#'+d).join(', '); };
  return /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("div", {
    className: "flex gap-4 mb-4 flex-wrap items-center"
  }, /*#__PURE__*/React.createElement("select", {
    className: "bg-gray-700 rounded px-3 py-2",
    value: rtuId,
    onChange: e => {
      setRtuId(e.target.value);
      onRtuChange && onRtuChange(e.target.value);
      setDeviceNum('');
      setSelectedDevs(new Set());
    }
  }, /*#__PURE__*/React.createElement("option", {
    value: ""
  }, "-- RTU --"), rtus.map(r => /*#__PURE__*/React.createElement("option", {
    key: r.rtu_id,
    value: r.rtu_id
  }, r.rtu_id))), inverters.length > 0 && React.createElement('div', {className: 'flex gap-3 items-center flex-wrap'},
    React.createElement('label', {className: 'flex items-center gap-1 text-sm cursor-pointer text-yellow-400'},
      React.createElement('input', {type: 'checkbox', checked: allSelected, onChange: toggleAll, className: 'accent-yellow-400'}), 'All'),
    inverters.map(d => React.createElement('label', {key: d.device_number, className: 'flex items-center gap-1 text-sm cursor-pointer'},
      React.createElement('input', {type: 'checkbox', checked: selectedDevs.has(d.device_number), onChange: () => toggleDev(d.device_number), className: 'accent-blue-500'}),
      'INV#' + d.device_number))
  )), rtuId && selectedDevs.size > 0 && /*#__PURE__*/React.createElement("div", {
    className: "grid grid-cols-1 lg:grid-cols-2 gap-4 mb-4"
  }, /*#__PURE__*/React.createElement(Card, null, /*#__PURE__*/React.createElement("div", {
    className: "text-gray-400 text-sm mb-2"
  }, "ON / OFF Control"), /*#__PURE__*/React.createElement("div", {
    className: "flex items-center gap-4"
  }, /*#__PURE__*/React.createElement("button", {
    onClick: () => {
      if (!confirm(`INV [${selLabel()}] ON 명령을 전송하시겠습니까?`)) return;
      setOnOff(0);
      sendToSelected('on_off', dn => ({ rtu_id: Number(rtuId), device_num: dn, value: 0 }));
    },
    className: `px-6 py-3 rounded-lg text-lg font-bold ${onOff === 0 ? 'bg-green-600' : 'bg-gray-600'}`
  }, "ON"), /*#__PURE__*/React.createElement("button", {
    onClick: () => {
      if (!confirm(`INV [${selLabel()}] OFF 명령을 전송하시겠습니까?`)) return;
      setOnOff(1);
      sendToSelected('on_off', dn => ({ rtu_id: Number(rtuId), device_num: dn, value: 1 }));
    },
    className: `px-6 py-3 rounded-lg text-lg font-bold ${onOff === 1 ? 'bg-red-600' : 'bg-gray-600'}`
  }, "OFF"))), /*#__PURE__*/React.createElement(Card, null, /*#__PURE__*/React.createElement("div", {
    className: "text-gray-400 text-sm mb-2"
  }, "Active Power Limit (", (activePower / 10).toFixed(1), "%)"), /*#__PURE__*/React.createElement("div", {
    className: "flex items-center gap-2"
  }, /*#__PURE__*/React.createElement("input", {
    type: "range",
    min: "0",
    max: "1000",
    value: activePower,
    onChange: e => setActivePower(Number(e.target.value)),
    className: "slider flex-1"
  }), /*#__PURE__*/React.createElement("input", {
    type: "number",
    min: "0",
    max: "1000",
    value: activePower,
    onChange: e => setActivePower(Number(e.target.value)),
    className: "bg-gray-700 rounded px-2 py-1 w-20 text-sm"
  }), /*#__PURE__*/React.createElement("button", {
    onClick: () => sendToSelected('active_power', dn => ({ rtu_id: Number(rtuId), device_num: dn, value: activePower })),
    className: "bg-blue-600 hover:bg-blue-500 px-3 py-1 rounded text-sm"
  }, "Send"))), /*#__PURE__*/React.createElement(Card, null, /*#__PURE__*/React.createElement("div", {
    className: "text-gray-400 text-sm mb-2"
  }, "Power Factor (", (powerFactor / 1000).toFixed(3), ")"), /*#__PURE__*/React.createElement("div", {
    className: "flex items-center gap-2"
  }, /*#__PURE__*/React.createElement("input", {
    type: "range",
    min: "-1000",
    max: "1000",
    value: powerFactor,
    onChange: e => setPowerFactor(Number(e.target.value)),
    className: "slider flex-1"
  }), /*#__PURE__*/React.createElement("input", {
    type: "number",
    min: "-1000",
    max: "1000",
    value: powerFactor,
    onChange: e => setPowerFactor(Number(e.target.value)),
    className: "bg-gray-700 rounded px-2 py-1 w-20 text-sm"
  }), /*#__PURE__*/React.createElement("button", {
    onClick: () => sendToSelected('power_factor', dn => ({ rtu_id: Number(rtuId), device_num: dn, value: powerFactor })),
    className: "bg-blue-600 hover:bg-blue-500 px-3 py-1 rounded text-sm"
  }, "Send"))), /*#__PURE__*/React.createElement(Card, null, /*#__PURE__*/React.createElement("div", {
    className: "text-gray-400 text-sm mb-2"
  }, "Reactive Power (", (reactivePower / 10).toFixed(1), "%)"), /*#__PURE__*/React.createElement("div", {
    className: "flex items-center gap-2"
  }, /*#__PURE__*/React.createElement("input", {
    type: "range",
    min: "-1000",
    max: "1000",
    value: reactivePower,
    onChange: e => setReactivePower(Number(e.target.value)),
    className: "slider flex-1"
  }), /*#__PURE__*/React.createElement("input", {
    type: "number",
    min: "-1000",
    max: "1000",
    value: reactivePower,
    onChange: e => setReactivePower(Number(e.target.value)),
    className: "bg-gray-700 rounded px-2 py-1 w-20 text-sm"
  }), /*#__PURE__*/React.createElement("button", {
    onClick: () => sendToSelected('reactive_power', dn => ({ rtu_id: Number(rtuId), device_num: dn, value: reactivePower })),
    className: "bg-blue-600 hover:bg-blue-500 px-3 py-1 rounded text-sm"
  }, "Send")))), rtuId && /*#__PURE__*/React.createElement(Card, {
    className: "mb-4"
  }, /*#__PURE__*/React.createElement("div", {
    className: "text-gray-400 text-sm mb-2"
  }, "Actions"), /*#__PURE__*/React.createElement("div", {
    className: "flex flex-wrap gap-2"
  }, /*#__PURE__*/React.createElement("button", {
    onClick: () => {
      sendToSelected('init', dn => ({ rtu_id: Number(rtuId), device_num: dn }));
      setActivePower(1000); setPowerFactor(1000); setReactivePower(0); setOnOff(0);
    },
    className: "bg-gray-600 hover:bg-gray-500 px-3 py-2 rounded text-sm"
  }, "Init Reset"), /*#__PURE__*/React.createElement("button", {
    onClick: () => sendToSelected('check', dn => ({ rtu_id: Number(rtuId), device_num: dn })),
    className: "bg-gray-600 hover:bg-gray-500 px-3 py-2 rounded text-sm"
  }, "Status Check"), /*#__PURE__*/React.createElement("select", {
    className: "bg-gray-700 rounded px-2 py-2 text-sm",
    value: deviceNum,
    onChange: e => setDeviceNum(e.target.value)
  }, inverters.map(d => React.createElement('option', {key: d.device_number, value: d.device_number}, 'INV#' + d.device_number))),
  /*#__PURE__*/React.createElement("button", {
    onClick: () => {
      sendControl('iv_scan', { rtu_id: Number(rtuId), device_num: Number(deviceNum) });
      setIvData(null);
    },
    className: "bg-gray-600 hover:bg-gray-500 px-3 py-2 rounded text-sm"
  }, "IV Scan"), /*#__PURE__*/React.createElement("button", {
    onClick: () => sendToSelected('model_info', dn => ({ rtu_id: Number(rtuId), device_num: dn })),
    className: "bg-indigo-600 hover:bg-indigo-500 px-3 py-2 rounded text-sm"
  }, "Model Info"), /*#__PURE__*/React.createElement("button", {
    onClick: () => sendControl('rtu_info', {
      rtu_id: Number(rtuId)
    }),
    className: "bg-indigo-600 hover:bg-indigo-500 px-3 py-2 rounded text-sm"
  }, "RTU Info"), /*#__PURE__*/React.createElement("button", {
    onClick: () => {
      if (!confirm(`RTU ${rtuId}를 재부팅하시겠습니까? 통신이 일시 중단됩니다.`)) return;
      sendControl('reboot', {
        rtu_id: Number(rtuId)
      });
    },
    className: "bg-red-700 hover:bg-red-600 px-3 py-2 rounded text-sm"
  }, "Reboot RTU"))), /*#__PURE__*/React.createElement(Card, null, /*#__PURE__*/React.createElement("div", {
    className: "text-gray-400 text-sm mb-2"
  }, /*#__PURE__*/React.createElement("span", null, "Response Log"), " ", /*#__PURE__*/React.createElement("button", {
    onClick: () => setLogs([]),
    className: "text-xs bg-red-700 hover:bg-red-600 px-2 py-0.5 rounded ml-2"
  }, "Clear")), /*#__PURE__*/React.createElement("div", {
    ref: logRef,
    className: "bg-gray-900 rounded p-2 h-48 overflow-y-auto font-mono text-xs"
  }, logs.map((l, i) => /*#__PURE__*/React.createElement("div", {
    key: i,
    dangerouslySetInnerHTML: { __html: `<span class="text-gray-500">[${l.time}]</span> ${l.msg}` }
  })), logs.length === 0 && /*#__PURE__*/React.createElement("div", {
    className: "text-gray-600"
  }, "No logs yet"))), /*#__PURE__*/React.createElement(Card, {
    className: "mt-4"
  }, /*#__PURE__*/React.createElement("div", {
    className: "flex justify-between items-center mb-2"
  }, /*#__PURE__*/React.createElement("div", {
    className: "text-gray-400 text-sm"
  }, ivData ? `IV Curve — INV#${ivData.device_number} (${ivData.model_name || MODEL_NAMES[ivData.model] || 'Unknown'}), ${ivData.total_strings} strings` : 'IV Curve — No data'), /*#__PURE__*/React.createElement("div", {
    className: "flex gap-2 items-center"
  }, /*#__PURE__*/React.createElement("select", {
    className: "bg-gray-700 text-xs rounded px-2 py-1",
    defaultValue: "",
    onClick: loadIvFileList,
    onChange: e => {
      if (e.target.value) loadIvFile(e.target.value);
      e.target.value = '';
    }
  }, /*#__PURE__*/React.createElement("option", {
    value: ""
  }, "CSV Load..."), ivFiles.map(f => /*#__PURE__*/React.createElement("option", {
    key: f,
    value: f
  }, f))), ivData && /*#__PURE__*/React.createElement("button", {
    onClick: downloadIVCsv,
    className: "bg-green-700 hover:bg-green-600 px-3 py-1 rounded text-xs"
  }, "CSV Save"))), /*#__PURE__*/React.createElement("canvas", {
    ref: ivCanvasRef,
    style: {
      width: '100%',
      height: ivData ? '350px' : '50px'
    },
    className: "rounded"
  })));
}

// ==== HISTORY TAB ====
function HistoryTab({
  rtus,
  selectedRtu,
  onRtuChange
}) {
  const [rtuId, setRtuId] = useState(selectedRtu || '');
  useEffect(() => {
    if (selectedRtu) setRtuId(selectedRtu);
  }, [selectedRtu]);
  const [devices, setDevices] = useState([]);
  const [deviceNum, setDeviceNum] = useState('');
  const [deviceType, setDeviceType] = useState('inverter');
  // Time-range filter state. Default 1 hour back from now. Options:
  //   '1h','6h','1d','7d','30d' = preset rolling windows ending now
  //   'custom' = use fromDt/toDt manually entered datetimes
  const [timeRange, setTimeRange] = useState('1h');
  const [fromDt, setFromDt] = useState('');
  const [toDt, setToDt] = useState('');
  const [data, setData] = useState([]);
  const [clearTs, setClearTs] = useState(() => sessionStorage.getItem('history_clear_ts') || '');
  useEffect(() => {
    if (!rtuId) return;
    const pd = d => {
      if (d?.devices && typeof d.devices === 'object' && !Array.isArray(d.devices)) return Object.values(d.devices).map(v => ({
        device_type: v.device_type,
        device_number: v.device_number,
        ...v.data
      }));
      return Array.isArray(d) ? d : [];
    };
    fetcher(`/rtus/${rtuId}/devices`).then(d => {
      const devs = pd(d);
      setDevices(devs);
      // Sort numerically so the default pick is the lowest device number
      // (e.g. INV#1 instead of whichever device the API happened to list first).
      const sorted = devs.slice().sort((a, b) => (a.device_number || 0) - (b.device_number || 0));
      if (sorted.length > 0 && !deviceNum) {
        setDeviceNum(String(sorted[0].device_number));
        setDeviceType(sorted[0].device_type === 1 ? 'inverter' : sorted[0].device_type === 5 ? 'weather' : 'relay');
      }
    }).catch(() => {});
  }, [rtuId]);
  // KST timestamp formatter — matches DB column format `YYYY-MM-DD HH:MM:SS`.
  const fmtKstTs = (date) => {
    const k = new Date(date.getTime() + 9 * 3600 * 1000);
    return k.toISOString().replace('T', ' ').substring(0, 19);
  };
  // datetime-local input value (YYYY-MM-DDTHH:MM in local tz) → KST DB string
  const dtLocalToKst = (s) => s ? s.replace('T', ' ') + (s.length === 16 ? ':00' : '') : '';
  const loadData = () => {
    if (!rtuId || !deviceNum) return;
    let from_ts = '';
    let to_ts = '';
    if (timeRange === 'custom') {
      from_ts = dtLocalToKst(fromDt);
      to_ts = dtLocalToKst(toDt);
    } else {
      const ms = {
        '1h': 1*3600e3, '6h': 6*3600e3,
        '1d': 24*3600e3, '7d': 7*24*3600e3, '30d': 30*24*3600e3,
      }[timeRange] || 3600e3;
      const now = new Date();
      from_ts = fmtKstTs(new Date(now.getTime() - ms));
      to_ts = fmtKstTs(now);
    }
    // Honor the Clear button: never load data older than the clear timestamp.
    if (clearTs && (!from_ts || from_ts < clearTs)) from_ts = clearTs;
    const params = [`rtu_id=${rtuId}`, `device_num=${deviceNum}`, `limit=10000`];
    if (from_ts) params.push(`from_ts=${encodeURIComponent(from_ts)}`);
    if (to_ts) params.push(`to_ts=${encodeURIComponent(to_ts)}`);
    fetcher(`/data/${deviceType}?${params.join('&')}`).then(d => setData(Array.isArray(d?.data) ? d.data : Array.isArray(d) ? d : [])).catch(() => setData([]));
  };
  useEffect(() => {
    loadData();
  }, [rtuId, deviceNum, deviceType, timeRange, fromDt, toDt, clearTs]);
  const rev = useMemo(() => data.slice().reverse(), [data]);
  const powerSeries = useMemo(() => deviceType === 'inverter' ? [{
    name: 'PV Power',
    color: '#F59E0B',
    data: rev.map(d => ({
      time: d.timestamp,
      value: (d.pv_power || 0) / 1000
    }))
  }, {
    name: 'AC Power',
    color: '#3B82F6',
    data: rev.map(d => ({
      time: d.timestamp,
      value: (d.ac_power || 0) / 1000
    }))
  }] : deviceType === 'relay' ? [{
    name: 'Inverter',
    color: '#F59E0B',
    data: rev.map(d => ({
      time: d.timestamp,
      value: (d.inverter_power || 0) / 1000
    }))
  }, {
    name: 'Load',
    color: '#10B981',
    data: rev.map(d => ({
      time: d.timestamp,
      value: (d.load_power || 0) / 1000
    }))
  }, {
    name: 'Grid',
    color: '#3B82F6',
    data: rev.map(d => ({
      time: d.timestamp,
      value: (d.total_active_power || 0) / 1000
    }))
  }] : [], [rev, deviceType]);
  const relayEnergySeries = useMemo(() => deviceType === 'relay' ? [{
    name: 'Import (+WH)',
    color: '#10B981',
    data: rev.map(d => ({
      time: d.timestamp,
      value: (d.received_energy || 0) / 1000
    }))
  }, {
    name: 'Export (-WH)',
    color: '#EF4444',
    data: rev.map(d => ({
      time: d.timestamp,
      value: (d.sent_energy || 0) / 1000
    }))
  }] : [], [rev, deviceType]);
  const voltageSeries = useMemo(() => deviceType === 'inverter' ? [{
    name: 'R Voltage',
    color: '#EF4444',
    data: rev.map(d => ({
      time: d.timestamp,
      value: d.r_voltage || 0
    }))
  }, {
    name: 'S Voltage',
    color: '#10B981',
    data: rev.map(d => ({
      time: d.timestamp,
      value: d.s_voltage || 0
    }))
  }, {
    name: 'T Voltage',
    color: '#8B5CF6',
    data: rev.map(d => ({
      time: d.timestamp,
      value: d.t_voltage || 0
    }))
  }] : [], [rev, deviceType]);
  const currentSeries = useMemo(() => deviceType === 'inverter' ? [{
    name: 'R Current',
    color: '#EF4444',
    data: rev.map(d => ({
      time: d.timestamp,
      value: (d.r_current || 0) / 10
    }))
  }, {
    name: 'S Current',
    color: '#10B981',
    data: rev.map(d => ({
      time: d.timestamp,
      value: (d.s_current || 0) / 10
    }))
  }, {
    name: 'T Current',
    color: '#8B5CF6',
    data: rev.map(d => ({
      time: d.timestamp,
      value: (d.t_current || 0) / 10
    }))
  }] : [], [rev, deviceType]);
  const energySeries = useMemo(() => deviceType === 'inverter' ? [{
    name: 'Energy',
    color: '#06B6D4',
    data: rev.map(d => ({
      time: d.timestamp,
      value: (d.cumulative_energy || 0) / 1000
    }))
  }] : [], [rev, deviceType]);
  const radiationSeries = useMemo(() => deviceType === 'weather' ? [{
    name: 'Horizontal',
    color: '#F59E0B',
    data: rev.map(d => ({ time: d.timestamp, value: d.horizontal_radiation || 0 }))
  }, {
    name: 'Inclined',
    color: '#EF4444',
    data: rev.map(d => ({ time: d.timestamp, value: d.inclined_radiation || 0 }))
  }] : [], [rev, deviceType]);
  const tempSeries = useMemo(() => deviceType === 'weather' ? [{
    name: 'Air Temp',
    color: '#3B82F6',
    data: rev.map(d => ({ time: d.timestamp, value: d.air_temp || 0 }))
  }, {
    name: 'Module 1',
    color: '#EF4444',
    data: rev.map(d => ({ time: d.timestamp, value: d.module_temp_1 || 0 }))
  }, {
    name: 'Module 2',
    color: '#10B981',
    data: rev.map(d => ({ time: d.timestamp, value: d.module_temp_2 || 0 }))
  }] : [], [rev, deviceType]);
  return /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("div", {
    className: "flex gap-4 mb-4 flex-wrap items-end"
  }, /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("label", {
    className: "text-gray-400 text-xs block mb-1"
  }, "RTU"), /*#__PURE__*/React.createElement("select", {
    className: "bg-gray-700 rounded px-3 py-2",
    value: rtuId,
    onChange: e => {
      setRtuId(e.target.value);
      onRtuChange && onRtuChange(e.target.value);
      setDeviceNum('');
    }
  }, /*#__PURE__*/React.createElement("option", {
    value: ""
  }, "-- RTU --"), rtus.map(r => /*#__PURE__*/React.createElement("option", {
    key: r.rtu_id,
    value: r.rtu_id
  }, r.rtu_id)))), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("label", {
    className: "text-gray-400 text-xs block mb-1"
  }, "Device"), /*#__PURE__*/React.createElement("select", {
    className: "bg-gray-700 rounded px-3 py-2",
    value: deviceNum,
    onChange: e => {
      setDeviceNum(e.target.value);
      const dev = devices.find(d => String(d.device_number) === e.target.value);
      if (dev) setDeviceType(dev.device_type === 1 || dev.device_type === 'inverter' ? 'inverter' : dev.device_type === 5 ? 'weather' : 'relay');
    }
  }, /*#__PURE__*/React.createElement("option", {
    value: ""
  }, "-- Device --"), devices.slice().sort((a, b) => (a.device_number || 0) - (b.device_number || 0)).map(d => /*#__PURE__*/React.createElement("option", {
    key: d.device_number,
    value: d.device_number
  }, "#", d.device_number, " (", d.device_type === 1 ? MODEL_NAMES[d.model] || 'Inverter' : d.device_type === 5 ? 'Weather' : 'Relay', ")")))), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("label", {
    className: "text-gray-400 text-xs block mb-1"
  }, "Time Range"), /*#__PURE__*/React.createElement("select", {
    className: "bg-gray-700 rounded px-3 py-2",
    value: timeRange,
    onChange: e => setTimeRange(e.target.value)
  }, [
    {v:'1h',  l:'Last 1 hour'},
    {v:'6h',  l:'Last 6 hours'},
    {v:'1d',  l:'Last 1 day'},
    {v:'7d',  l:'Last 7 days'},
    {v:'30d', l:'Last 30 days'},
    {v:'custom', l:'Custom range'},
  ].map(o => /*#__PURE__*/React.createElement("option", {
    key: o.v, value: o.v
  }, o.l)))), timeRange === 'custom' && /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("label", {
    className: "text-gray-400 text-xs block mb-1"
  }, "From"), /*#__PURE__*/React.createElement("input", {
    type: "datetime-local",
    className: "bg-gray-700 rounded px-3 py-2",
    value: fromDt,
    onChange: e => setFromDt(e.target.value)
  })), timeRange === 'custom' && /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("label", {
    className: "text-gray-400 text-xs block mb-1"
  }, "To"), /*#__PURE__*/React.createElement("input", {
    type: "datetime-local",
    className: "bg-gray-700 rounded px-3 py-2",
    value: toDt,
    onChange: e => setToDt(e.target.value)
  })), /*#__PURE__*/React.createElement("button", {
    onClick: loadData,
    className: "bg-blue-600 hover:bg-blue-500 px-4 py-2 rounded"
  }, "Refresh"), data.length > 0 && /*#__PURE__*/React.createElement("button", {
    onClick: () => { const now = new Date(); const kst = new Date(now.getTime() + 9*60*60*1000); const ts = kst.toISOString().replace('T',' ').substring(0,19); sessionStorage.setItem('history_clear_ts', ts); setClearTs(ts); setData([]); },
    className: "bg-red-600 hover:bg-red-500 px-4 py-2 rounded text-sm"
  }, "Clear")), rev.length > 1 && /*#__PURE__*/React.createElement("div", {
    className: "grid grid-cols-1 lg:grid-cols-2 gap-4 mb-4"
  }, /*#__PURE__*/React.createElement(Card, null, /*#__PURE__*/React.createElement("div", {
    className: "text-gray-400 text-sm mb-1"
  }, deviceType === 'weather' ? "Radiation (W/m\u00B2)" : deviceType === 'inverter' ? "Power (kW)" : "Power Flow (kW)"), /*#__PURE__*/React.createElement(MultiLineChart, {
    series: deviceType === 'weather' ? radiationSeries : powerSeries,
    height: 220
  })), deviceType === 'weather' ? /*#__PURE__*/React.createElement(Card, null, /*#__PURE__*/React.createElement("div", {
    className: "text-gray-400 text-sm mb-1"
  }, "Temperature (\u00B0C)"), /*#__PURE__*/React.createElement(MultiLineChart, {
    series: tempSeries,
    height: 220
  })) : null, relayEnergySeries.length > 0 && /*#__PURE__*/React.createElement(Card, null, /*#__PURE__*/React.createElement("div", {
    className: "text-gray-400 text-sm mb-1"
  }, "Energy (kWh)"), /*#__PURE__*/React.createElement(MultiLineChart, {
    series: relayEnergySeries,
    height: 220
  })), voltageSeries.length > 0 && /*#__PURE__*/React.createElement(Card, null, /*#__PURE__*/React.createElement("div", {
    className: "text-gray-400 text-sm mb-1"
  }, "AC Voltage (V)"), /*#__PURE__*/React.createElement(MultiLineChart, {
    series: voltageSeries,
    height: 220
  })), currentSeries.length > 0 && /*#__PURE__*/React.createElement(Card, null, /*#__PURE__*/React.createElement("div", {
    className: "text-gray-400 text-sm mb-1"
  }, "AC Current (A)"), /*#__PURE__*/React.createElement(MultiLineChart, {
    series: currentSeries,
    height: 220
  })), energySeries.length > 0 && /*#__PURE__*/React.createElement(Card, null, /*#__PURE__*/React.createElement("div", {
    className: "text-gray-400 text-sm mb-1"
  }, "Cumulative Energy (kWh)"), /*#__PURE__*/React.createElement(MultiLineChart, {
    series: energySeries,
    height: 220
  }))), /*#__PURE__*/React.createElement(Card, null, /*#__PURE__*/React.createElement("div", {
    className: "overflow-x-auto"
  }, /*#__PURE__*/React.createElement("table", {
    className: "w-full text-xs"
  }, /*#__PURE__*/React.createElement("thead", null, /*#__PURE__*/React.createElement("tr", {
    className: "text-gray-400 border-b border-gray-700"
  }, /*#__PURE__*/React.createElement("th", {
    className: "py-1 text-left"
  }, "Time"), deviceType === 'inverter' ? /*#__PURE__*/React.createElement(React.Fragment, null, /*#__PURE__*/React.createElement("th", {
    className: "text-right"
  }, "PV V (V)"), /*#__PURE__*/React.createElement("th", {
    className: "text-right"
  }, "PV I (A)"), /*#__PURE__*/React.createElement("th", {
    className: "text-right"
  }, "PV P (kW)"), /*#__PURE__*/React.createElement("th", {
    className: "text-right"
  }, "AC R/S/T (V)"), /*#__PURE__*/React.createElement("th", {
    className: "text-right"
  }, "AC R/S/T (A)"), /*#__PURE__*/React.createElement("th", {
    className: "text-right"
  }, "AC P (kW)"), /*#__PURE__*/React.createElement("th", {
    className: "text-right"
  }, "PF"), /*#__PURE__*/React.createElement("th", {
    className: "text-right"
  }, "Freq (Hz)"), /*#__PURE__*/React.createElement("th", {
    className: "text-right"
  }, "Energy (kWh)"), /*#__PURE__*/React.createElement("th", {
    className: "text-right"
  }, "Status")) : deviceType === 'weather' ? /*#__PURE__*/React.createElement(React.Fragment, null, /*#__PURE__*/React.createElement("th", {
    className: "text-right"
  }, "Temp(\u00B0C)"), /*#__PURE__*/React.createElement("th", {
    className: "text-right"
  }, "Hum(%)"), /*#__PURE__*/React.createElement("th", {
    className: "text-right"
  }, "Press(hPa)"), /*#__PURE__*/React.createElement("th", {
    className: "text-right"
  }, "Wind(m/s)"), /*#__PURE__*/React.createElement("th", {
    className: "text-right"
  }, "Dir(\u00B0)"), /*#__PURE__*/React.createElement("th", {
    className: "text-right"
  }, "H.Rad(W/m\u00B2)"), /*#__PURE__*/React.createElement("th", {
    className: "text-right"
  }, "I.Rad(W/m\u00B2)"), /*#__PURE__*/React.createElement("th", {
    className: "text-right"
  }, "Mod1(\u00B0C)"), /*#__PURE__*/React.createElement("th", {
    className: "text-right"
  }, "Mod2(\u00B0C)")) : /*#__PURE__*/React.createElement(React.Fragment, null, /*#__PURE__*/React.createElement("th", {
    className: "text-right"
  }, "R (V)"), /*#__PURE__*/React.createElement("th", {
    className: "text-right"
  }, "S (V)"), /*#__PURE__*/React.createElement("th", {
    className: "text-right"
  }, "T (V)"), /*#__PURE__*/React.createElement("th", {
    className: "text-right"
  }, "Grid (kW)"), /*#__PURE__*/React.createElement("th", {
    className: "text-right"
  }, "INV (kW)"), /*#__PURE__*/React.createElement("th", {
    className: "text-right"
  }, "Load (kW)"), /*#__PURE__*/React.createElement("th", {
    className: "text-right"
  }, "PF"), /*#__PURE__*/React.createElement("th", {
    className: "text-right"
  }, "Freq (Hz)"), /*#__PURE__*/React.createElement("th", {
    className: "text-right"
  }, "Import (kWh)"), /*#__PURE__*/React.createElement("th", {
    className: "text-right"
  }, "Export (kWh)"), /*#__PURE__*/React.createElement("th", {
    className: "text-right"
  }, "DO"), /*#__PURE__*/React.createElement("th", {
    className: "text-right"
  }, "DI")))), /*#__PURE__*/React.createElement("tbody", null, data.slice(0, 200).map((d, i) => /*#__PURE__*/React.createElement("tr", {
    key: i,
    className: "border-b border-gray-700/30 hover:bg-gray-800/50"
  }, /*#__PURE__*/React.createElement("td", {
    className: "py-1"
  }, fmtTime(d.timestamp)), deviceType === 'inverter' ? /*#__PURE__*/React.createElement(React.Fragment, null, /*#__PURE__*/React.createElement("td", {
    className: "text-right"
  }, fmt(d.pv_voltage)), /*#__PURE__*/React.createElement("td", {
    className: "text-right"
  }, fmt((d.pv_current || 0) / 10, 1)), /*#__PURE__*/React.createElement("td", {
    className: "text-right"
  }, fmt((d.pv_power || 0) / 1000, 2)), /*#__PURE__*/React.createElement("td", {
    className: "text-right text-xs"
  }, fmt(d.r_voltage), "/", fmt(d.s_voltage), "/", fmt(d.t_voltage)), /*#__PURE__*/React.createElement("td", {
    className: "text-right text-xs"
  }, fmt((d.r_current || 0) / 10, 1), "/", fmt((d.s_current || 0) / 10, 1), "/", fmt((d.t_current || 0) / 10, 1)), /*#__PURE__*/React.createElement("td", {
    className: "text-right font-medium"
  }, fmt((d.ac_power || 0) / 1000, 2)), /*#__PURE__*/React.createElement("td", {
    className: "text-right"
  }, fmt(d.power_factor, 3)), /*#__PURE__*/React.createElement("td", {
    className: "text-right"
  }, fmt(d.frequency)), /*#__PURE__*/React.createElement("td", {
    className: "text-right"
  }, fmt((d.cumulative_energy || 0) / 1000, 1)), /*#__PURE__*/React.createElement("td", {
    className: `text-right ${d.status === 3 ? 'text-green-400' : d.status === 5 ? 'text-red-400' : 'text-yellow-400'}`
  }, INVERTER_STATUS[d.status] || d.status)) : deviceType === 'weather' ? /*#__PURE__*/React.createElement(React.Fragment, null, /*#__PURE__*/React.createElement("td", {
    className: "text-right"
  }, fmt(d.air_temp, 1)), /*#__PURE__*/React.createElement("td", {
    className: "text-right"
  }, fmt(d.air_humidity, 1)), /*#__PURE__*/React.createElement("td", {
    className: "text-right"
  }, fmt(d.air_pressure, 1)), /*#__PURE__*/React.createElement("td", {
    className: "text-right"
  }, fmt(d.wind_speed, 1)), /*#__PURE__*/React.createElement("td", {
    className: "text-right"
  }, fmt(d.wind_direction, 0)), /*#__PURE__*/React.createElement("td", {
    className: "text-right font-medium"
  }, fmt(d.horizontal_radiation, 0)), /*#__PURE__*/React.createElement("td", {
    className: "text-right font-medium"
  }, fmt(d.inclined_radiation, 0)), /*#__PURE__*/React.createElement("td", {
    className: "text-right"
  }, fmt(d.module_temp_1, 1)), /*#__PURE__*/React.createElement("td", {
    className: "text-right"
  }, fmt(d.module_temp_2, 1))) : /*#__PURE__*/React.createElement(React.Fragment, null, /*#__PURE__*/React.createElement("td", {
    className: "text-right"
  }, fmt(d.r_voltage)), /*#__PURE__*/React.createElement("td", {
    className: "text-right"
  }, fmt(d.s_voltage)), /*#__PURE__*/React.createElement("td", {
    className: "text-right"
  }, fmt(d.t_voltage)), /*#__PURE__*/React.createElement("td", {
    className: "text-right font-medium"
  }, fmt((d.total_active_power || 0) / 1000, 2)), /*#__PURE__*/React.createElement("td", {
    className: "text-right text-yellow-400"
  }, fmt((d.inverter_power || 0) / 1000, 2)), /*#__PURE__*/React.createElement("td", {
    className: "text-right text-green-400"
  }, fmt((d.load_power || 0) / 1000, 2)), /*#__PURE__*/React.createElement("td", {
    className: "text-right"
  }, fmt(d.avg_power_factor, 3)), /*#__PURE__*/React.createElement("td", {
    className: "text-right"
  }, fmt(d.frequency)), /*#__PURE__*/React.createElement("td", {
    className: "text-right"
  }, fmt((d.received_energy || 0) / 1000, 1)), /*#__PURE__*/React.createElement("td", {
    className: "text-right"
  }, fmt((d.sent_energy || 0) / 1000, 1)), /*#__PURE__*/React.createElement("td", {
    className: "text-right text-xs"
  }, `0x${((d.do_status || 0) & 0xFFFF).toString(16).toUpperCase().padStart(4, '0')}`), /*#__PURE__*/React.createElement("td", {
    className: "text-right text-xs"
  }, `0x${((d.di_status || 0) & 0xFFFF).toString(16).toUpperCase().padStart(4, '0')}`)))))), data.length === 0 && /*#__PURE__*/React.createElement("div", {
    className: "text-gray-500 text-center py-4"
  }, "No data"))));
}

// ==== EVENTS TAB ====
function EventsTab({
  wsUpdateCounter
}) {
  const [events, setEvents] = useState([]);
  const [filter, setFilter] = useState('');
  const [clearTs, setClearTs] = useState(() => sessionStorage.getItem('events_clear_ts') || '');
  const scrollRef = useRef(null);
  useEffect(() => {
    const parseEv = d => Array.isArray(d?.events) ? d.events : Array.isArray(d) ? d : [];
    const fromParam = clearTs ? `&from_ts=${encodeURIComponent(clearTs)}` : '';
    fetcher(`/events?limit=200${fromParam}`).then(d => setEvents(parseEv(d))).catch(() => {});
  }, [wsUpdateCounter, clearTs]);
  useEffect(() => {
    if (scrollRef.current) scrollRef.current.scrollTop = 0;
  }, [events]);
  const filtered = filter ? events.filter(e => String(e.rtu_id).includes(filter)) : events;
  return /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("div", {
    className: "mb-4 flex items-center gap-4"
  }, /*#__PURE__*/React.createElement("input", {
    placeholder: "Filter by RTU ID...",
    className: "bg-gray-700 rounded px-3 py-2 text-sm",
    value: filter,
    onChange: e => setFilter(e.target.value)
  }), /*#__PURE__*/React.createElement("button", {
    onClick: () => { const now = new Date(); const kst = new Date(now.getTime() + 9*60*60*1000); const ts = kst.toISOString().replace('T',' ').substring(0,19); sessionStorage.setItem('events_clear_ts', ts); setClearTs(ts); setEvents([]); },
    className: "px-4 py-2 rounded text-sm font-medium bg-red-700 hover:bg-red-600"
  }, "Clear")), /*#__PURE__*/React.createElement(Card, null, /*#__PURE__*/React.createElement("div", {
    ref: scrollRef,
    className: "overflow-y-auto",
    style: {
      maxHeight: '65vh'
    }
  }, /*#__PURE__*/React.createElement("table", {
    className: "w-full text-sm"
  }, /*#__PURE__*/React.createElement("thead", {
    className: "sticky top-0 bg-gray-800"
  }, /*#__PURE__*/React.createElement("tr", {
    className: "text-gray-400 border-b border-gray-700"
  }, /*#__PURE__*/React.createElement("th", {
    className: "py-2 text-left"
  }, "Timestamp"), /*#__PURE__*/React.createElement("th", {
    className: "text-left"
  }, "RTU ID"), /*#__PURE__*/React.createElement("th", {
    className: "text-left"
  }, "Event"), /*#__PURE__*/React.createElement("th", {
    className: "text-left"
  }, "Detail"))), /*#__PURE__*/React.createElement("tbody", null, filtered.map((e, i) => /*#__PURE__*/React.createElement("tr", {
    key: i,
    className: "border-b border-gray-700/30"
  }, /*#__PURE__*/React.createElement("td", {
    className: "py-1 text-xs"
  }, fmtTime(e.timestamp)), /*#__PURE__*/React.createElement("td", {
    className: "font-mono"
  }, e.rtu_id), /*#__PURE__*/React.createElement("td", {
    className: EVT_COLORS[e.event_type] || 'text-gray-300'
  }, e.event_type), /*#__PURE__*/React.createElement("td", {
    className: "text-gray-400 text-xs"
  }, typeof e.detail === 'object' ? JSON.stringify(e.detail) : e.detail))))), filtered.length === 0 && /*#__PURE__*/React.createElement("div", {
    className: "text-gray-500 text-center py-4"
  }, "No events"))));
}

// ==== FIRMWARE TAB ====
function FirmwareTab({
  rtus
}) {
  const [files, setFiles] = useState([]);
  const [rtuId, setRtuId] = useState('');
  const [selFile, setSelFile] = useState('');
  const [status, setStatus] = useState('');
  const fileRef = useRef(null);
  const loadFiles = () => fetcher('/firmware/list').then(d => setFiles(Array.isArray(d?.files) ? d.files : Array.isArray(d) ? d : [])).catch(() => {});
  useEffect(() => {
    loadFiles();
  }, []);
  // Single-step update: if a local file is picked, upload it first then
  // dispatch to the selected RTU. If a server library file is picked from
  // the dropdown, dispatch directly. The previous separate "Upload Firmware"
  // panel was redundant — users always followed Upload with Send Update.
  const update = async () => {
    if (!rtuId) { setStatus('Pick an RTU first.'); return; }
    let filename = selFile;
    const localFile = fileRef.current && fileRef.current.files[0];
    if (localFile) {
      // Upload the locally picked file first.
      setStatus(`Uploading ${localFile.name}...`);
      try {
        const fd = new FormData();
        fd.append('file', localFile);
        const r = await fetch(API + '/firmware/upload', { method: 'POST', body: fd });
        const j = await r.json();
        if (!r.ok) { setStatus('Upload failed: ' + JSON.stringify(j)); return; }
        filename = j.filename || localFile.name;
        await loadFiles();
      } catch (e) {
        setStatus('Upload error: ' + e.message);
        return;
      }
    }
    if (!filename) { setStatus('Pick a firmware file (local or library).'); return; }
    setStatus(`Sending ${filename} to RTU ${rtuId}...`);
    try {
      const r = await post('/firmware/update', {
        rtu_id: Number(rtuId),
        filename: filename
      });
      setStatus('Update: ' + JSON.stringify(r));
    } catch (e) {
      setStatus('Error: ' + e.message);
    }
  };
  return /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement(Card, {
    className: "mb-4"
  }, /*#__PURE__*/React.createElement("div", {
    className: "text-gray-400 text-sm mb-2"
  }, "Send Firmware Update"), /*#__PURE__*/React.createElement("div", {
    className: "flex gap-3 flex-wrap items-end"
  }, /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("label", {
    className: "text-gray-500 text-xs block mb-1"
  }, "RTU"), /*#__PURE__*/React.createElement("select", {
    className: "bg-gray-700 rounded px-3 py-2 text-sm",
    value: rtuId,
    onChange: e => setRtuId(e.target.value)
  }, /*#__PURE__*/React.createElement("option", {
    value: ""
  }, "-- RTU --"), rtus.filter(r => r.rtu_type !== 'RIP').map(r => /*#__PURE__*/React.createElement("option", {
    key: r.rtu_id,
    value: r.rtu_id
  }, r.rtu_id)))), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("label", {
    className: "text-gray-500 text-xs block mb-1"
  }, "Library File"), /*#__PURE__*/React.createElement("select", {
    className: "bg-gray-700 rounded px-3 py-2 text-sm",
    value: selFile,
    onChange: e => setSelFile(e.target.value)
  }, /*#__PURE__*/React.createElement("option", {
    value: ""
  }, "-- File --"), files.map(f => /*#__PURE__*/React.createElement("option", {
    key: f.filename,
    value: f.filename
  }, f.filename)))), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("label", {
    className: "text-gray-500 text-xs block mb-1"
  }, "or Upload Local File"), /*#__PURE__*/React.createElement("input", {
    type: "file",
    ref: fileRef,
    accept: ".tar.gz,.tgz,.zip",
    className: "text-sm bg-gray-700 rounded px-2 py-1.5"
  })), /*#__PURE__*/React.createElement("button", {
    onClick: update,
    className: "bg-green-600 hover:bg-green-500 px-5 py-2 rounded text-sm font-medium"
  }, "Update"))), status && /*#__PURE__*/React.createElement(Card, {
    className: "mb-4"
  }, /*#__PURE__*/React.createElement("div", {
    className: "text-sm font-mono"
  }, status)), /*#__PURE__*/React.createElement(Card, null, /*#__PURE__*/React.createElement("table", {
    className: "w-full text-sm"
  }, /*#__PURE__*/React.createElement("thead", null, /*#__PURE__*/React.createElement("tr", {
    className: "text-gray-400 border-b border-gray-700"
  }, /*#__PURE__*/React.createElement("th", {
    className: "py-2 text-left"
  }, "Filename"), /*#__PURE__*/React.createElement("th", {
    className: "text-left"
  }, "Size"), /*#__PURE__*/React.createElement("th", {
    className: "text-left"
  }, "Modified"))), /*#__PURE__*/React.createElement("tbody", null, files.map(f => /*#__PURE__*/React.createElement("tr", {
    key: f.filename,
    className: "border-b border-gray-700/30"
  }, /*#__PURE__*/React.createElement("td", {
    className: "py-1 font-mono"
  }, f.filename), /*#__PURE__*/React.createElement("td", null, f.size != null ? (f.size / 1024).toFixed(1) + ' KB' : ''), /*#__PURE__*/React.createElement("td", {
    className: "text-xs"
  }, fmtTime(f.modified)))))), files.length === 0 && /*#__PURE__*/React.createElement("div", {
    className: "text-gray-500 text-center py-4"
  }, "No firmware files")));
}

// ==== CONFIG TAB ====
function ConfigTab({
  rtus,
  selectedRtu
}) {
  const [rtuIp, setRtuIp] = useState('');
  const [files, setFiles] = useState([]);
  const [selPath, setSelPath] = useState('');
  const [content, setContent] = useState('');
  const [status, setStatus] = useState('');
  const [statusErr, setStatusErr] = useState(false);
  const [loading, setLoading] = useState(false);
  const [pushPanel, setPushPanel] = useState(false);
  const [pushFiles, setPushFiles] = useState([]);
  const [pushLoading, setPushLoading] = useState(false);
  const [pushResult, setPushResult] = useState(null);
<<<<<<< HEAD
  const [groupedFromApi, setGroupedFromApi] = useState(null);
  const [localMode, setLocalMode] = useState(() => localStorage.getItem('rtu_config_local_mode') === 'true');
  const normalizedRtuIp = sanitizeRtuHost(rtuIp);
  const effectiveIp = localMode ? 'localhost' : normalizedRtuIp;
=======
  // localMode state removed — effectiveIp is now computed directly from
  // the RTU IP input: empty or localhost → local, otherwise → SSH push.
>>>>>>> 9837d06791ee1e2c66fd4a43108878afbb4ff0a1

  // Load saved RTU SSH IP from localStorage (NAT 환경에서 UDP source IP와 다를 수 있음)
  useEffect(() => {
    let ipMap = {};
    try {
      ipMap = JSON.parse(localStorage.getItem('rtu_ssh_ip_map_v1') || '{}') || {};
    } catch {}
    const selectedSaved = sanitizeRtuHost(selectedRtu ? (ipMap[String(selectedRtu)] || '') : '');
    const globalSaved = sanitizeRtuHost(localStorage.getItem('rtu_ssh_ip'));
    const saved = isValidRtuHost(selectedSaved) ? selectedSaved : isValidRtuHost(globalSaved) ? globalSaved : '';
    if (saved) {
      setRtuIp(saved);
    } else if (selectedRtu) {
      const rtu = rtus.find(r => String(r.rtu_id) === String(selectedRtu));
      if (rtu) setRtuIp(sanitizeRtuHost(rtu.ip?.split(':')[0] || rtu.ip || ''));
    }
  }, [selectedRtu, rtus]);
  useEffect(() => {
<<<<<<< HEAD
    if (!normalizedRtuIp) return;
    if (!isValidRtuHost(normalizedRtuIp)) return;
    localStorage.setItem('rtu_ssh_ip', normalizedRtuIp);
    if (!selectedRtu) return;
    let ipMap = {};
    try {
      ipMap = JSON.parse(localStorage.getItem('rtu_ssh_ip_map_v1') || '{}') || {};
    } catch {}
    ipMap[String(selectedRtu)] = normalizedRtuIp;
    localStorage.setItem('rtu_ssh_ip_map_v1', JSON.stringify(ipMap));
  }, [normalizedRtuIp, selectedRtu]);
  useEffect(() => {
    localStorage.setItem('rtu_config_local_mode', String(localMode));
  }, [localMode]);
=======
    if (rtuIp) localStorage.setItem('rtu_ssh_ip', rtuIp);
  }, [rtuIp]);
  // localMode useEffect removed — no more localStorage toggle.
>>>>>>> 9837d06791ee1e2c66fd4a43108878afbb4ff0a1

  // Auto-load files on tab entry
  useEffect(() => { loadFiles(); }, []);

  // Open Files: 항상 PC 로컬 파일 읽기 (SSH 불필요)
  const loadFiles = async () => {
    setLoading(true);
    setStatusErr(false);
    setStatus('Loading files...');
    try {
      const d = await fetcher('/config/files?rtu_ip=localhost');
      const rawFiles = Array.isArray(d.files) ? d.files : [];
      const normalized = rawFiles.map(f => {
        if (typeof f === 'string') return f;
        if (f && typeof f === 'object') {
          return f.path || f.remote || f.local || f.filename || '';
        }
        return '';
      }).filter(Boolean);
      setFiles(normalized);
      setGroupedFromApi(d.grouped && typeof d.grouped === 'object' ? d.grouped : null);
      setStatusErr(false);
      setStatus(`${normalized.length}개 파일 로드됨`);
    } catch (e) {
      setFiles([]);
      setGroupedFromApi(null);
      setStatusErr(true);
      setStatus('Error: ' + e.message);
    }
    setLoading(false);
  };

  // Load File: 항상 PC 로컬에서 읽기
  const loadFile = async path => {
    if (!path) return;
    setSelPath(path);
    setLoading(true);
    setStatus('Reading...');
    try {
      const d = await fetcher(`/config/read?rtu_ip=localhost&path=${encodeURIComponent(path)}`);
      setContent(d.content || '');
      setStatus(`Loaded: ${path}`);
    } catch (e) {
      setStatus('Error: ' + e.message);
    }
    setLoading(false);
  };

  // Save: 항상 PC 로컬에 저장
  const saveFile = async () => {
    if (!selPath) return;
    setStatus('Saving...');
    try {
      await post('/config/write', {
        rtu_ip: 'localhost',
        path: selPath,
        content
      });
      setStatus(`Saved: ${selPath}`);
    } catch (e) {
      setStatus('Error: ' + e.message);
    }
  };

  // Push to RTU: uses rtuIp if filled, otherwise localhost (local sim)
  const effectiveIp = rtuIp.trim() || 'localhost';
  const openPushPanel = async () => {
    setPushPanel(true);
    setPushResult(null);
    setPushLoading(true);
    setPushFiles([]);
    try {
      const d = await fetcher('/config/push_preview');
      setPushFiles(d.files || []);
    } catch (e) {
      setPushFiles([]);
    }
    setPushLoading(false);
  };
  const doPushToRtu = async () => {
    if (!effectiveIp) return;
    if (!localMode && !isValidRtuHost(effectiveIp)) {
      setStatusErr(true);
      setStatus('Error: RTU IP/host must be a plain IPv4 address or hostname without spaces or port');
      return;
    }
    setPushLoading(true);
    setPushResult(null);
    try {
      const d = await post('/config/push_to_rtu', {
        rtu_ip: effectiveIp
      });
      setPushResult(d);
      setStatus('PC\u2192RTU: ' + d.ok_count + '/' + d.results.length + '\uac1c \uc804\uc1a1\uc644\ub8cc, \uc7ac\uc2dc\uc791: ' + d.restart);
    } catch (e) {
      setStatus('Error: ' + e.message);
    }
    setPushLoading(false);
  };

<<<<<<< HEAD
  // Group files by directory (supports both "/home/pi/config/x.ini" and "config/x.ini")
  const grouped = {
    config: [],
    common: []
  };
  // Prefer backend grouping when available to avoid duplicate rendering.
  if (groupedFromApi && Object.keys(groupedFromApi).length > 0) {
    for (const [k, v] of Object.entries(groupedFromApi)) {
      if (!Array.isArray(v)) continue;
      const key = String(k || '').trim();
      if (!grouped[key]) grouped[key] = [];
      grouped[key] = Array.from(new Set(v.map(x => String(x)).filter(Boolean)));
    }
  } else {
    files.forEach(f => {
      if (typeof f !== 'string') return;
      const norm = f.replace(/\\/g, '/');
      let dir = '';
      const m = norm.match(/(?:^|\/)(config|common)(?:\/|$)/);
      if (m) {
        dir = m[1];
      } else {
        const parts = norm.split('/').filter(Boolean);
        if (parts.length >= 2) dir = parts[parts.length - 2];
      }
      if (!grouped[dir]) grouped[dir] = [];
      grouped[dir].push(norm);
    });
    for (const k of Object.keys(grouped)) {
      grouped[k] = Array.from(new Set(grouped[k]));
    }
  }
=======
  // Open any local file via browser file picker (for files outside project)
  const openFileRef = useRef(null);
  const openLocalFile = () => {
    if (openFileRef.current) openFileRef.current.click();
  };
  const handleLocalFileOpen = async (e) => {
    const file = e.target.files && e.target.files[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => {
      setContent(reader.result);
      setSelPath('[local] ' + file.name);
      setStatus(`Opened: ${file.name} (${(file.size / 1024).toFixed(1)} KB)`);
    };
    reader.readAsText(file);
    e.target.value = '';  // reset so same file can be re-opened
  };

  // Group files by directory
  const grouped = {};
  files.forEach(f => {
    const parts = f.split('/');
    const dir = parts.slice(0, -2).length ? parts[parts.length - 2] : '';
    if (!grouped[dir]) grouped[dir] = [];
    grouped[dir].push(f);
  });
>>>>>>> 9837d06791ee1e2c66fd4a43108878afbb4ff0a1

  return /*#__PURE__*/React.createElement("div", null,
    /*#__PURE__*/React.createElement("input", {
      type: "file", ref: openFileRef, style: { display: 'none' },
      accept: ".py,.ini,.json,.txt,.cfg,.yaml,.yml,.md,.csv",
      onChange: handleLocalFileOpen
    }),
    /*#__PURE__*/React.createElement("div", {
      className: "flex gap-3 mb-2 items-center flex-wrap"
    },
<<<<<<< HEAD
      /*#__PURE__*/React.createElement("span", { className: "text-gray-400 text-sm" }, "RTU IP:"),
      /*#__PURE__*/React.createElement("input", {
        className: localMode ? "bg-gray-700 rounded px-3 py-1.5 text-sm w-40 opacity-40 cursor-not-allowed" : "bg-gray-700 rounded px-3 py-1.5 text-sm w-40",
        value: localMode ? 'localhost' : rtuIp,
        onChange: e => { if (!localMode) setRtuIp(sanitizeRtuHost(e.target.value)); },
        placeholder: "172.30.1.40",
        disabled: localMode
      }),
      /*#__PURE__*/React.createElement("button", {
        onClick: () => setLocalMode(m => !m),
        className: localMode ? "bg-orange-600 hover:bg-orange-500 px-3 py-1.5 rounded text-sm font-semibold" : "bg-gray-600 hover:bg-gray-500 px-3 py-1.5 rounded text-sm font-semibold",
        title: localMode ? "\uB85C\uCEEC \ubaa8\uB4DC: PC \ub85c\ucec8 \ud30c\uc77c \uc9c1\uc811 \uc77d\uae30" : "\uc6d0\uaca9 \ubaa8\uB4DC: SSH\ub85c RTU\uc5d0 \uc811\uc18d"
      }, localMode ? "\uD83C\uDFE0 Local" : "\uD83C\uDF10 Remote")
=======
      /*#__PURE__*/React.createElement("span", { className: "text-gray-400 text-sm" }, "RTU:"),
      /*#__PURE__*/React.createElement("select", {
        className: "bg-gray-700 rounded px-3 py-1.5 text-sm",
        value: rtuIp,
        onChange: e => setRtuIp(e.target.value)
      },
        /*#__PURE__*/React.createElement("option", { value: "" }, "-- Push \uB300\uC0C1 RTU --"),
        /*#__PURE__*/React.createElement("option", { value: "localhost" }, "localhost (\uB85C\uCEEC \uC2DC\uBBAC\uB808\uC774\uD130)"),
        rtus.filter(r => r.status === 'online' && r.rtu_type !== 'RIP').map(r => /*#__PURE__*/React.createElement("option", {
          key: r.rtu_id,
          value: (r.ip || '').split(':')[0] || r.ip || ''
        }, r.rtu_id, " (", (r.ip || '').split(':')[0] || '-', ")"))
      )
>>>>>>> 9837d06791ee1e2c66fd4a43108878afbb4ff0a1
    ),
    /*#__PURE__*/React.createElement("div", {
      className: "flex gap-3 mb-4 items-center flex-wrap"
    },
      /*#__PURE__*/React.createElement("button", {
        onClick: openLocalFile,
        className: "bg-blue-600 hover:bg-blue-500 px-4 py-2 rounded text-sm"
      }, "\uD83D\uDCC2 Open File"),
      /*#__PURE__*/React.createElement("button", {
        onClick: saveFile,
        disabled: !selPath || selPath.startsWith('[local]'),
        className: "bg-green-600 hover:bg-green-500 px-4 py-2 rounded text-sm disabled:opacity-50"
      }, "\uD83D\uDCBE Save"),
      /*#__PURE__*/React.createElement("button", {
        onClick: openPushPanel,
        className: "bg-teal-600 hover:bg-teal-500 px-4 py-2 rounded text-sm font-semibold"
      }, "\uD83D\uDCE4 Push to RTU & Restart")
    ),
    status && /*#__PURE__*/React.createElement("div", {
      className: "mb-3 text-sm " + (statusErr ? "text-red-400" : "text-yellow-400")
    }, status),
    pushPanel && /*#__PURE__*/React.createElement("div", {
      className: "mb-4 bg-gray-800 border border-teal-700 rounded p-3"
    },
      /*#__PURE__*/React.createElement("div", {
        className: "flex items-center justify-between mb-2"
      },
        /*#__PURE__*/React.createElement("span", {
          className: "text-sm font-bold text-teal-400"
        }, "\uD83D\uDCE4 PC \u2192 RTU \uc804\uc1a1 \ubaa9\ub85d  (", effectiveIp === 'localhost' ? "\uD83C\uDFE0 Local" : "\uD83C\uDF10 " + effectiveIp, ")"),
        /*#__PURE__*/React.createElement("div", { className: "flex gap-2" },
          !pushResult && /*#__PURE__*/React.createElement("button", {
            onClick: doPushToRtu,
<<<<<<< HEAD
            disabled: pushLoading || (!localMode && !isValidRtuHost(effectiveIp)) || pushFiles.length === 0,
=======
            disabled: pushLoading || pushFiles.length === 0,
>>>>>>> 9837d06791ee1e2c66fd4a43108878afbb4ff0a1
            className: "bg-teal-600 hover:bg-teal-500 px-3 py-1 rounded text-xs disabled:opacity-50"
          }, pushLoading ? "\uc804\uc1a1 \uc911..." : "\uc804\uc1a1 & \uc7ac\uc2dc\uc791"),
          /*#__PURE__*/React.createElement("button", {
            onClick: () => { setPushPanel(false); setPushResult(null); },
            className: "bg-gray-600 hover:bg-gray-500 px-3 py-1 rounded text-xs"
          }, "\ub2eb\uae30")
        )
      ),
      pushLoading && !pushResult && /*#__PURE__*/React.createElement("div", {
        className: "text-xs text-gray-400 mb-2"
      }, "\ud30c\uc77c \ubaa9\ub85d \ub85c\ub529 \uc911..."),
      !pushLoading && pushFiles.length === 0 && !pushResult && /*#__PURE__*/React.createElement("div", {
        className: "text-xs text-gray-400"
      }, "\uc804\uc1a1\ud560 \ud30c\uc77c\uc774 \uc5c6\uc2b5\ub2c8\ub2e4"),
      !pushResult && pushFiles.length > 0 && /*#__PURE__*/React.createElement("div", {
        className: "text-xs space-y-1"
      }, pushFiles.map((f, i) => /*#__PURE__*/React.createElement("div", {
        key: i,
        className: "flex justify-between text-gray-300 px-1 py-0.5 hover:bg-gray-700 rounded"
      },
        /*#__PURE__*/React.createElement("span", { className: "font-mono" }, f.local),
        /*#__PURE__*/React.createElement("span", { className: "text-gray-500 ml-4" }, f.size ? (f.size / 1024).toFixed(1) + ' KB' : '')
      ))),
      pushResult && /*#__PURE__*/React.createElement("div", {
        className: "text-xs space-y-1"
      },
        pushResult.results.map((r, i) => /*#__PURE__*/React.createElement("div", {
          key: i,
          className: "flex items-center gap-2 px-1 py-0.5"
        },
          /*#__PURE__*/React.createElement("span", {
            className: r.status === 'ok' ? 'text-green-400' : 'text-red-400'
          }, r.status === 'ok' ? '\u2713' : '\u2717'),
          /*#__PURE__*/React.createElement("span", { className: "font-mono text-gray-300" }, r.file),
          r.error && /*#__PURE__*/React.createElement("span", { className: "text-red-400 ml-2" }, r.error)
        )),
        /*#__PURE__*/React.createElement("div", {
          className: "mt-2 pt-2 border-t border-gray-700 text-xs " + (pushResult.restart === 'ok' ? 'text-green-400' : 'text-yellow-400')
        }, "\uc7ac\uc2dc\uc791: " + pushResult.restart + " (" + pushResult.ok_count + "/" + pushResult.results.length + "\uac1c \uc131\ub3d9)")
      )
    ),
    /*#__PURE__*/React.createElement("div", {
      className: "grid grid-cols-4 gap-4"
    },
      /*#__PURE__*/React.createElement("div", { className: "col-span-1" },
        /*#__PURE__*/React.createElement(Card, null,
          /*#__PURE__*/React.createElement("div", { className: "text-sm font-bold mb-2" }, "Files"),
          Object.entries(grouped).filter(([, flist]) => flist.length > 0).map(([dir, flist]) => /*#__PURE__*/React.createElement("div", {
            key: dir,
            className: "mb-2"
          },
            /*#__PURE__*/React.createElement("div", { className: "text-xs text-blue-400 font-bold mb-1" }, "\uD83D\uDCC1 ", dir, "/"),
            flist.map(f => {
              const fname = f.split('/').pop();
              const active = f === selPath;
              return /*#__PURE__*/React.createElement("div", {
                key: f,
                onClick: () => loadFile(f),
                className: `text-xs cursor-pointer px-2 py-1 rounded hover:bg-gray-600 ${active ? 'bg-gray-600 text-white' : 'text-gray-300'}`
              }, fname);
            })
          )),
          files.length === 0 && /*#__PURE__*/React.createElement("div", {
            className: "text-gray-500 text-xs"
          }, "Open Files\ub97c \ud074\ub9ad\ud558\uc138\uc694")
        )
      ),
      /*#__PURE__*/React.createElement("div", { className: "col-span-3" },
        /*#__PURE__*/React.createElement(Card, null,
          /*#__PURE__*/React.createElement("div", { className: "text-sm text-gray-400 mb-2" }, selPath || 'Select a file'),
          /*#__PURE__*/React.createElement("textarea", {
            className: "w-full h-[500px] bg-gray-900 text-green-300 font-mono text-xs p-3 rounded border border-gray-700 resize-y",
            value: content,
            onChange: e => setContent(e.target.value),
            placeholder: "Select a file from the tree to edit...",
            spellCheck: false
          })
        )
      )
    )
  );
}

// ==== STATS TAB ====
function StatsTab() {
  const [stats, setStats] = useState({});
  useEffect(() => {
    const load = () => fetcher('/stats').then(setStats).catch(() => {});
    load();
    const iv = setInterval(load, 5000);
    return () => clearInterval(iv);
  }, []);
  const fmtUptime = s => { if (s == null) return '--'; const d = Math.floor(s/86400), h = Math.floor((s%86400)/3600), m = Math.floor((s%3600)/60), sec = Math.floor(s%60); return d > 0 ? `${d}d ${h}h ${String(m).padStart(2,'0')}m` : `${h}h ${String(m).padStart(2,'0')}m ${String(sec).padStart(2,'0')}s`; };
  const pktItems = [
    ['H01 Received', stats.h01_received], ['H02 Sent', stats.h02_sent],
    ['H03 Sent', stats.h03_sent], ['H04 Received', stats.h04_received],
    ['H05 Received', stats.h05_received], ['Total Packets', stats.total_packets],
    ['RTU Count', stats.rtu_count], ['WS Clients', stats.ws_clients],
    ['Uptime', fmtUptime(stats.uptime)], ['IV Scans', stats.iv_scan_count ?? 0],
  ];
  const cpuColor = (stats.cpu_percent||0) > 80 ? 'text-red-400' : (stats.cpu_percent||0) > 50 ? 'text-yellow-400' : 'text-green-400';
  const memColor = (stats.mem_percent||0) > 85 ? 'text-red-400' : (stats.mem_percent||0) > 60 ? 'text-yellow-400' : 'text-green-400';
  const diskColor = (stats.disk_percent||0) > 90 ? 'text-red-400' : (stats.disk_percent||0) > 70 ? 'text-yellow-400' : 'text-green-400';
  const srvItems = [
    ['CPU (System)', stats.cpu_percent != null ? `${stats.cpu_percent}%` : '--', cpuColor],
    ['RAM (System)', stats.mem_percent != null ? `${stats.mem_used_mb}/${stats.mem_total_mb} MB (${stats.mem_percent}%)` : '--', memColor],
    ['RAM (Server)', stats.proc_mem_mb != null ? `${stats.proc_mem_mb} MB` : '--', ''],
    ['Disk', stats.disk_free_gb != null ? `${stats.disk_free_gb}/${stats.disk_total_gb} GB (${stats.disk_percent}%)` : '--', diskColor],
    ['DB Size', stats.db_size_mb != null ? `${stats.db_size_mb} MB` : '--', ''],
  ];
  const dbTables = stats.db_tables || {};
  return /*#__PURE__*/React.createElement("div", null,
    /*#__PURE__*/React.createElement("div", { className: "text-gray-400 text-sm mb-2 font-semibold" }, "Server Resources"),
    /*#__PURE__*/React.createElement("div", {
      className: "grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-4 mb-6"
    }, srvItems.map(([label, val, color]) => /*#__PURE__*/React.createElement(Card, {
      key: label
    }, /*#__PURE__*/React.createElement("div", {
      className: "text-gray-400 text-xs"
    }, label), /*#__PURE__*/React.createElement("div", {
      className: "text-lg font-bold mt-1 " + (color || '')
    }, val)))),
    Object.keys(dbTables).length > 0 && /*#__PURE__*/React.createElement(Card, {
      className: "mb-6"
    }, /*#__PURE__*/React.createElement("div", { className: "text-gray-400 text-sm mb-2" }, "DB Table Rows"),
    /*#__PURE__*/React.createElement("div", { className: "grid grid-cols-2 md:grid-cols-4 gap-x-6 gap-y-1 text-sm" },
      Object.entries(dbTables).map(([t, c]) => /*#__PURE__*/React.createElement("div", { key: t, className: "flex justify-between" },
        /*#__PURE__*/React.createElement("span", { className: "text-gray-400 font-mono" }, t),
        /*#__PURE__*/React.createElement("span", { className: c > 100000 ? "text-yellow-400 font-bold" : "" }, (c||0).toLocaleString())
      ))
    )),
    /*#__PURE__*/React.createElement("div", { className: "text-gray-400 text-sm mb-2 font-semibold" }, "UDP Protocol Counters"),
    /*#__PURE__*/React.createElement("div", {
      className: "grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-4 mb-6"
    }, pktItems.map(([label, val]) => /*#__PURE__*/React.createElement(Card, {
      key: label
    }, /*#__PURE__*/React.createElement("div", {
      className: "text-gray-400 text-xs"
    }, label), /*#__PURE__*/React.createElement("div", {
      className: "text-2xl font-bold mt-1"
    }, val != null ? val : '--')))), stats.per_rtu && /*#__PURE__*/React.createElement(Card, {
    className: "mt-4"
  }, /*#__PURE__*/React.createElement("div", {
    className: "text-gray-400 text-sm mb-2"
  }, "Per-RTU Stats"), /*#__PURE__*/React.createElement("table", {
    className: "w-full text-sm"
  }, /*#__PURE__*/React.createElement("thead", null, /*#__PURE__*/React.createElement("tr", {
    className: "text-gray-400 border-b border-gray-700"
  }, /*#__PURE__*/React.createElement("th", {
    className: "py-1 text-left"
  }, "RTU ID"), /*#__PURE__*/React.createElement("th", {
    className: "text-right"
  }, "H01"), /*#__PURE__*/React.createElement("th", {
    className: "text-right"
  }, "H04"), /*#__PURE__*/React.createElement("th", {
    className: "text-right"
  }, "H05"))), /*#__PURE__*/React.createElement("tbody", null, Object.entries(stats.per_rtu).map(([id, s]) => /*#__PURE__*/React.createElement("tr", {
    key: id,
    className: "border-b border-gray-700/30"
  }, /*#__PURE__*/React.createElement("td", {
    className: "py-1 font-mono"
  }, id), /*#__PURE__*/React.createElement("td", {
    className: "text-right"
  }, s.h01 || 0), /*#__PURE__*/React.createElement("td", {
    className: "text-right"
  }, s.h04 || 0), /*#__PURE__*/React.createElement("td", {
    className: "text-right"
  }, s.h05 || 0)))))));
}

// ==== H1 LOG TAB ====
function H1LogTab({packets, rtus, onClear}) {
  const [filter, setFilter] = useState('');
  const [paused, setPaused] = useState(false);
  const [expanded, setExpanded] = useState(null);
  const pausedRef = useRef([]);
  const displayPackets = paused ? pausedRef.current : packets;
  const filtered = useMemo(() => {
    const list = filter ? displayPackets.filter(p => String(p.rtu_id) === filter) : displayPackets;
    return list.slice().reverse();
  }, [displayPackets, filter, packets]);
  const togglePause = () => {
    if (!paused) pausedRef.current = [...packets];
    setPaused(p => !p);
  };
  const seenSeqs = {};
  filtered.forEach(p => {
    const k = p.rtu_id + '_' + p.seq;
    seenSeqs[k] = (seenSeqs[k] || 0) + 1;
  });
  const getRowColor = (p) => {
    const k = p.rtu_id + '_' + p.seq;
    if (seenSeqs[k] > 1) return 'bg-red-900/30 text-red-300';
    if (p.backup === 1) return 'bg-yellow-900/30 text-yellow-300';
    if (p.combined) return 'bg-blue-900/30 text-blue-300';
    return '';
  };
  const getType = (p) => {
    const k = p.rtu_id + '_' + p.seq;
    if (seenSeqs[k] > 1) return 'RETRY';
    if (p.backup === 1) return 'BACKUP';
    if (p.combined) return 'COMBINED';
    return 'SINGLE';
  };
  const formatHex = (hex) => {
    if (!hex) return '';
    const bytes = hex.match(/.{1,2}/g) || [];
    const line1 = bytes.slice(0, 38).join(' ');
    const line2 = bytes.slice(38, 76).join(' ');
    const line3 = bytes.length > 76 ? bytes.slice(76).join(' ') : '';
    return [line1, line2, line3].filter(Boolean).join('\n');
  };
  const devName = (p) => {
    if (p.dev_type === 1) return 'INV' + p.dev_num;
    if (p.dev_type === 2) return 'RLY' + p.dev_num;
    if (p.dev_type === 3) return 'WTH' + p.dev_num;
    return 'DEV' + p.dev_num;
  };
  return React.createElement('div', null,
    React.createElement('div', {className: 'flex items-center gap-4 mb-4'},
      React.createElement('select', {
        className: 'bg-gray-700 text-white px-3 py-2 rounded text-sm',
        value: filter, onChange: e => setFilter(e.target.value)
      },
        React.createElement('option', {value: ''}, 'All RTUs'),
        rtus.map(r => React.createElement('option', {key: r.rtu_id, value: String(r.rtu_id)}, r.rtu_id + (r.status === 'online' ? ' (online)' : ' (offline)')))
      ),
      React.createElement('button', {
        onClick: togglePause,
        className: 'px-4 py-2 rounded text-sm font-medium ' + (paused ? 'bg-green-600 hover:bg-green-500' : 'bg-yellow-600 hover:bg-yellow-500')
      }, paused ? 'Resume' : 'Pause'),
      React.createElement('button', {
        onClick: onClear,
        className: 'px-4 py-2 rounded text-sm font-medium bg-red-700 hover:bg-red-600'
      }, 'Clear'),
      React.createElement('span', {className: 'text-sm text-gray-400'}, filtered.length + ' packets'),
      React.createElement('div', {className: 'flex gap-3 ml-auto text-xs'},
        React.createElement('span', {className: 'text-gray-400'}, 'SINGLE'),
        React.createElement('span', {className: 'text-blue-400'}, 'COMBINED'),
        React.createElement('span', {className: 'text-yellow-400'}, 'BACKUP'),
        React.createElement('span', {className: 'text-red-400'}, 'RETRY')
      )
    ),
    React.createElement('div', {className: 'overflow-auto', style: {maxHeight: '70vh'}},
      React.createElement('table', {className: 'w-full text-sm'},
        React.createElement('thead', null,
          React.createElement('tr', {className: 'text-gray-400 border-b border-gray-700'},
            ['Time', 'RTU ID', 'Device', 'Seq', 'BK', 'Size', 'Type', 'Source'].map(h =>
              React.createElement('th', {key: h, className: 'px-3 py-2 text-left font-medium'}, h)
            )
          )
        ),
        React.createElement('tbody', null,
          filtered.map((p, i) => React.createElement(React.Fragment, {key: i},
            React.createElement('tr', {
              className: 'border-b border-gray-800 cursor-pointer hover:bg-gray-700/50 ' + getRowColor(p),
              onClick: () => setExpanded(expanded === i ? null : i)
            },
              React.createElement('td', {className: 'px-3 py-1.5 font-mono text-xs'}, p._time || ''),
              React.createElement('td', {className: 'px-3 py-1.5'}, p.rtu_id),
              React.createElement('td', {className: 'px-3 py-1.5'}, devName(p)),
              React.createElement('td', {className: 'px-3 py-1.5 font-mono'}, p.seq),
              React.createElement('td', {className: 'px-3 py-1.5'}, p.backup),
              React.createElement('td', {className: 'px-3 py-1.5'}, p.body_size + 'B'),
              React.createElement('td', {className: 'px-3 py-1.5 font-medium'}, getType(p)),
              React.createElement('td', {className: 'px-3 py-1.5 text-xs text-gray-400'}, p.src_addr || '')
            ),
            expanded === i && React.createElement('tr', null,
              React.createElement('td', {colSpan: 8, className: 'px-3 py-2 bg-gray-800/80'},
                React.createElement('pre', {className: 'font-mono text-xs text-green-400 whitespace-pre-wrap'}, formatHex(p.raw_hex))
              )
            )
          ))
        )
      )
    )
  );
}

// ==== MODBUS TEST TAB ====
function ModbusTestTab({ rtus, selectedRtu }) {
  const [rtuId, setRtuId] = useState(selectedRtu || '');
  useEffect(() => { if (selectedRtu) setRtuId(selectedRtu); }, [selectedRtu]);
  const [devices, setDevices] = useState([]);
  const [slaveId, setSlaveId] = useState(1);
  const [customSlave, setCustomSlave] = useState(false);
  const [fc, setFc] = useState(3);
  const [addr, setAddr] = useState('0x0000');
  const [count, setCount] = useState(10);
  const [dataType, setDataType] = useState('U16');
  const [scale, setScale] = useState('1');
  const [writeVal, setWriteVal] = useState('0');
  const [writeVals, setWriteVals] = useState('');
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState(null);
  const [logs, setLogs] = useState([]);
  const logRef = useRef(null);

  // Load devices when RTU changes
  useEffect(() => {
    if (!rtuId) { setDevices([]); return; }
    fetcher(`/rtus/${rtuId}/devices`).then(d => {
      const devs = d?.devices ? Object.values(d.devices).map(v => ({
        device_type: v.device_type, device_number: v.device_number, ...v.data
      })) : Array.isArray(d) ? d : [];
      const invs = devs.filter(dd => dd.device_type === 1)
        .sort((a, b) => (a.device_number || 0) - (b.device_number || 0));
      setDevices(invs);
      if (invs.length > 0 && !customSlave) setSlaveId(invs[0].device_number || 1);
    }).catch(() => {});
  }, [rtuId]);

  const addLog = (msg) => {
    setLogs(p => [...p.slice(-99), { time: new Date().toLocaleTimeString(), msg }]);
    setTimeout(() => { if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight; }, 50);
  };

  const parseAddr = (s) => {
    s = s.trim();
    return s.startsWith('0x') || s.startsWith('0X') ? parseInt(s, 16) : parseInt(s, 10);
  };

  const execute = async () => {
    const a = parseAddr(addr);
    if (isNaN(a) || a < 0 || a > 0xFFFF) { addLog('Invalid address'); return; }
    setLoading(true);
    setResult(null);
    const body = { rtu_id: parseInt(rtuId), slave_id: slaveId, function_code: fc, register_address: a };
    if (fc === 3 || fc === 4) {
      body.count = count;
    } else if (fc === 6) {
      body.count = 1;
      body.values = [parseInt(writeVal) & 0xFFFF];
    } else if (fc === 16) {
      body.values = writeVals.split(',').map(v => parseInt(v.trim()) & 0xFFFF).filter(v => !isNaN(v));
      body.count = body.values.length;
    }
    try {
      const op = fc <= 4 ? 'READ' : 'WRITE';
      addLog(`> FC${String(fc).padStart(2,'0')} ${op} slave=${slaveId} addr=0x${a.toString(16).toUpperCase().padStart(4,'0')} count=${body.count || count}`);
      const sendResult = await post('/control/modbus_test', body);
      if (sendResult?.tx_packet) addLog(`  TX: [${sendResult.tx_packet}]`);
      // Poll for result (RTU responds via H05, takes ~1-3s)
      let tries = 0;
      const poll = setInterval(async () => {
        tries++;
        try {
          const r = await fetcher(`/modbus_test/result/${rtuId}`);
          if (r?.result && r.result.address === a && r.result.fc === fc) {
            clearInterval(poll);
            setResult(r.result);
            setLoading(false);
            const rc = r.result.result_code;
            if (rc === 0) {
              const regs = r.result.registers || [];
              addLog(`< OK ${regs.length} registers: [${regs.map(v => '0x'+v.toString(16).toUpperCase().padStart(4,'0')).join(' ')}]`);
              if (r.result.raw_hex) addLog(`  RX: [${r.result.raw_hex}]`);
            } else {
              addLog(`< ${rc === -1 ? 'TIMEOUT' : 'ERROR'} (rc=${rc})`);
            }
          }
        } catch (e) {}
        if (tries >= 10) { clearInterval(poll); setLoading(false); addLog('< No response (timeout)'); }
      }, 500);
    } catch (e) {
      addLog(`Error: ${e.message}`);
      setLoading(false);
    }
  };

  const isRead = fc === 3 || fc === 4;
  const isWrite = fc === 6 || fc === 16;

  return /*#__PURE__*/React.createElement("div", null,
    // RTU + Inverter selector row
    /*#__PURE__*/React.createElement("div", { className: "flex gap-3 mb-3 items-center flex-wrap" },
      /*#__PURE__*/React.createElement("label", { className: "text-sm text-gray-400" }, "RTU:"),
      /*#__PURE__*/React.createElement("select", {
        value: rtuId, onChange: e => setRtuId(e.target.value),
        className: "bg-gray-800 border border-gray-600 rounded px-3 py-1.5 text-sm"
      }, /*#__PURE__*/React.createElement("option", { value: "" }, "-- Select RTU --"),
        rtus.map(r => /*#__PURE__*/React.createElement("option", { key: r.rtu_id, value: r.rtu_id },
          `RTU ${r.rtu_id} (${r.ip || '?'})`))),

      /*#__PURE__*/React.createElement("label", { className: "text-sm text-gray-400" }, "Inverter:"),
      !customSlave && /*#__PURE__*/React.createElement("select", {
        value: slaveId, onChange: e => setSlaveId(parseInt(e.target.value)),
        className: "bg-gray-800 border border-gray-600 rounded px-3 py-1.5 text-sm"
      }, devices.length === 0
        ? /*#__PURE__*/React.createElement("option", { value: 1 }, "No inverters")
        : devices.map(d => /*#__PURE__*/React.createElement("option", {
            key: d.device_number, value: d.device_number
          }, `INV#${d.device_number} (slave=${d.device_number})`))),
      customSlave && /*#__PURE__*/React.createElement("input", {
        type: "number", min: 1, max: 247, value: slaveId,
        onChange: e => setSlaveId(parseInt(e.target.value) || 1),
        className: "bg-gray-800 border border-gray-600 rounded px-3 py-1.5 text-sm w-20"
      }),
      /*#__PURE__*/React.createElement("label", { className: "text-xs text-gray-500 flex items-center gap-1" },
        /*#__PURE__*/React.createElement("input", {
          type: "checkbox", checked: customSlave,
          onChange: e => setCustomSlave(e.target.checked)
        }), "Manual slave ID")
    ),

    // Modbus command row
    /*#__PURE__*/React.createElement("div", { className: "flex gap-3 mb-3 items-end flex-wrap" },
      /*#__PURE__*/React.createElement("div", null,
        /*#__PURE__*/React.createElement("div", { className: "text-xs text-gray-500 mb-1" }, "Function Code"),
        /*#__PURE__*/React.createElement("select", {
          value: fc, onChange: e => setFc(parseInt(e.target.value)),
          className: "bg-gray-800 border border-gray-600 rounded px-3 py-1.5 text-sm"
        },
          /*#__PURE__*/React.createElement("option", { value: 3 }, "FC03 Read Holding"),
          /*#__PURE__*/React.createElement("option", { value: 4 }, "FC04 Read Input"),
          /*#__PURE__*/React.createElement("option", { value: 6 }, "FC06 Write Single"),
          /*#__PURE__*/React.createElement("option", { value: 16 }, "FC16 Write Multiple"))),

      /*#__PURE__*/React.createElement("div", null,
        /*#__PURE__*/React.createElement("div", { className: "text-xs text-gray-500 mb-1" }, "Address (hex/dec)"),
        /*#__PURE__*/React.createElement("input", {
          type: "text", value: addr, onChange: e => setAddr(e.target.value),
          className: "bg-gray-800 border border-gray-600 rounded px-3 py-1.5 text-sm w-28",
          placeholder: "0x1000"
        })),

      isRead && /*#__PURE__*/React.createElement("div", null,
        /*#__PURE__*/React.createElement("div", { className: "text-xs text-gray-500 mb-1" }, "Count"),
        /*#__PURE__*/React.createElement("input", {
          type: "number", min: 1, max: 125, value: count,
          onChange: e => setCount(parseInt(e.target.value) || 1),
          className: "bg-gray-800 border border-gray-600 rounded px-3 py-1.5 text-sm w-20"
        })),

      /*#__PURE__*/React.createElement("div", null,
        /*#__PURE__*/React.createElement("div", { className: "text-xs text-gray-500 mb-1" }, "Data Type"),
        /*#__PURE__*/React.createElement("select", {
          value: dataType, onChange: e => setDataType(e.target.value),
          className: "bg-gray-800 border border-gray-600 rounded px-3 py-1.5 text-sm"
        },
          ['U16', 'S16', 'U32', 'S32', 'STRING'].map(t =>
            /*#__PURE__*/React.createElement("option", { key: t, value: t }, t)))),

      /*#__PURE__*/React.createElement("div", null,
        /*#__PURE__*/React.createElement("div", { className: "text-xs text-gray-500 mb-1" }, "Scale"),
        /*#__PURE__*/React.createElement("input", {
          type: "text", value: scale, onChange: e => setScale(e.target.value),
          className: "bg-gray-800 border border-gray-600 rounded px-3 py-1.5 text-sm w-20",
          placeholder: "0.1"
        })),

      fc === 6 && /*#__PURE__*/React.createElement("div", null,
        /*#__PURE__*/React.createElement("div", { className: "text-xs text-gray-500 mb-1" }, "Value (U16)"),
        /*#__PURE__*/React.createElement("input", {
          type: "number", min: 0, max: 65535, value: writeVal,
          onChange: e => setWriteVal(e.target.value),
          className: "bg-gray-800 border border-gray-600 rounded px-3 py-1.5 text-sm w-24"
        })),

      fc === 16 && /*#__PURE__*/React.createElement("div", null,
        /*#__PURE__*/React.createElement("div", { className: "text-xs text-gray-500 mb-1" }, "Values (comma-sep)"),
        /*#__PURE__*/React.createElement("input", {
          type: "text", value: writeVals, onChange: e => setWriteVals(e.target.value),
          className: "bg-gray-800 border border-gray-600 rounded px-3 py-1.5 text-sm w-48",
          placeholder: "100, 200, 300"
        })),

      /*#__PURE__*/React.createElement("button", {
        onClick: execute, disabled: loading || !rtuId,
        className: `${isWrite ? 'bg-orange-600 hover:bg-orange-500' : 'bg-blue-600 hover:bg-blue-500'} px-5 py-1.5 rounded text-sm font-medium disabled:opacity-50`
      }, loading ? 'Waiting...' : isWrite ? 'Write' : 'Read')
    ),

    // Result table
    result && result.result_code === 0 && result.registers && /*#__PURE__*/React.createElement(Card, null,
      /*#__PURE__*/React.createElement("div", { className: "text-sm font-medium mb-2 text-green-400" },
        `FC${String(result.fc).padStart(2,'0')} Response — ${result.registers.length} registers from slave ${result.slave_id}`,
        `  [${dataType} × ${scale}]`),
      /*#__PURE__*/React.createElement("div", { className: "overflow-auto", style: { maxHeight: '400px' } },
        /*#__PURE__*/React.createElement("table", { className: "w-full text-sm font-mono" },
          /*#__PURE__*/React.createElement("thead", null,
            /*#__PURE__*/React.createElement("tr", { className: "text-gray-400 border-b border-gray-700" },
              ['Addr', 'Hex', 'Raw', 'Parsed', 'ASCII'].map(h =>
                /*#__PURE__*/React.createElement("th", { key: h, className: "text-left px-2 py-1" }, h)))),
          /*#__PURE__*/React.createElement("tbody", null,
            (() => {
              const regs = result.registers;
              const sc = parseFloat(scale) || 1;
              const rows = [];
              const is32 = dataType === 'U32' || dataType === 'S32';
              const isStr = dataType === 'STRING';
              const step = is32 ? 2 : 1;
              for (let i = 0; i < regs.length; i += step) {
                const regAddr = result.address + i;
                const v = regs[i];
                let raw, parsed, hex, ascii;
                if (is32 && i + 1 < regs.length) {
                  const u32 = ((regs[i] & 0xFFFF) << 16) | (regs[i+1] & 0xFFFF);
                  hex = `0x${u32.toString(16).toUpperCase().padStart(8, '0')}`;
                  if (dataType === 'S32') {
                    raw = u32 >= 0x80000000 ? u32 - 0x100000000 : u32;
                  } else {
                    raw = u32;
                  }
                  parsed = (raw * sc).toFixed(sc < 1 ? Math.max(1, -Math.floor(Math.log10(sc))) : 0);
                  const c1 = (regs[i]>>8)&0xFF, c2 = regs[i]&0xFF, c3 = (regs[i+1]>>8)&0xFF, c4 = regs[i+1]&0xFF;
                  ascii = [c1,c2,c3,c4].map(c => c>=32&&c<127?String.fromCharCode(c):'.').join('');
                } else if (isStr) {
                  hex = `0x${v.toString(16).toUpperCase().padStart(4,'0')}`;
                  const c1 = (v>>8)&0xFF, c2 = v&0xFF;
                  ascii = (c1>=32&&c1<127?String.fromCharCode(c1):'.')+(c2>=32&&c2<127?String.fromCharCode(c2):'.');
                  raw = v;
                  parsed = ascii;
                } else {
                  hex = `0x${v.toString(16).toUpperCase().padStart(4,'0')}`;
                  if (dataType === 'S16') {
                    raw = v >= 0x8000 ? v - 0x10000 : v;
                  } else {
                    raw = v;
                  }
                  parsed = (raw * sc).toFixed(sc < 1 ? Math.max(1, -Math.floor(Math.log10(sc))) : 0);
                  const c1 = (v>>8)&0xFF, c2 = v&0xFF;
                  ascii = (c1>=32&&c1<127?String.fromCharCode(c1):'.')+(c2>=32&&c2<127?String.fromCharCode(c2):'.');
                }
                rows.push(/*#__PURE__*/React.createElement("tr", {
                  key: i, className: "border-b border-gray-800 hover:bg-gray-800"
                },
                  /*#__PURE__*/React.createElement("td", { className: "px-2 py-0.5 text-gray-400" },
                    `0x${regAddr.toString(16).toUpperCase().padStart(4,'0')}${is32 ? '-'+(regAddr+1).toString(16).toUpperCase().padStart(4,'0') : ''}`),
                  /*#__PURE__*/React.createElement("td", { className: "px-2 py-0.5 text-yellow-300" }, hex),
                  /*#__PURE__*/React.createElement("td", { className: "px-2 py-0.5" }, raw),
                  /*#__PURE__*/React.createElement("td", { className: "px-2 py-0.5 text-cyan-300 font-bold" }, parsed),
                  /*#__PURE__*/React.createElement("td", { className: "px-2 py-0.5 text-gray-500" }, ascii)));
              }
              return rows;
            })())))),

    result && result.result_code !== 0 && /*#__PURE__*/React.createElement(Card, null,
      /*#__PURE__*/React.createElement("div", { className: "text-red-400 text-sm" },
        `Error: ${result.result_code === -1 ? 'TIMEOUT — No response from inverter' : 'Modbus communication error'}`)),

    // Log
    /*#__PURE__*/React.createElement("div", {
      ref: logRef,
      className: "mt-3 bg-gray-900 border border-gray-700 rounded p-3 text-xs font-mono overflow-auto",
      style: { maxHeight: '200px' }
    }, logs.length === 0
      ? /*#__PURE__*/React.createElement("span", { className: "text-gray-500" }, "Modbus test log...")
      : logs.map((l, i) => /*#__PURE__*/React.createElement("div", { key: i, className: "mb-0.5" },
          /*#__PURE__*/React.createElement("span", { className: "text-gray-500" }, l.time, " "),
          /*#__PURE__*/React.createElement("span", {
            className: l.msg.startsWith('>') ? 'text-blue-400' : l.msg.startsWith('<') ? 'text-green-400' : l.msg.startsWith('  TX:') || l.msg.startsWith('  RX:') ? 'text-gray-500 text-[10px]' : 'text-red-400'
          }, l.msg))))
  );
}

// ==== MODEL MAKER TAB ====
function ModelMakerTab() {
  const [running, setRunning] = useState(false);
  const [loading, setLoading] = useState(false);
  const [status, setStatus] = useState('');
  const [aiMode, setAiMode] = useState(() => {
    const saved = localStorage.getItem('mm2_ai_mode');
    return saved === null ? true : saved === 'true';  // default ON
  });
  const [aiKey, setAiKey] = useState('');
  const [aiModel, setAiModel] = useState('claude-sonnet-4-6');
  const [aiHasKey, setAiHasKey] = useState(false);
  const [aiMasked, setAiMasked] = useState('');
  const [aiSaving, setAiSaving] = useState(false);
  const MM2_PORT = 8082;
  // Use reverse proxy through dashboard port (same origin) so DDNS/NAT
  // access works without forwarding port 8082.
  const mm2Url = `${API}/mm2-app/`;

  const checkStatus = async () => {
    try {
      const d = await fetcher('/mm2/status');
      setRunning(d.running);
    } catch (e) { setRunning(false); }
  };
  useEffect(() => { checkStatus(); const iv = setInterval(checkStatus, 5000); return () => clearInterval(iv); }, []);

  // Load AI settings
  useEffect(() => {
    fetcher('/mm2/ai-settings').then(d => {
      setAiHasKey(d.has_key);
      setAiMasked(d.masked_key || '');
      setAiModel(d.model || 'claude-sonnet-4-6');
    }).catch(() => {});
  }, []);
  useEffect(() => { localStorage.setItem('mm2_ai_mode', String(aiMode)); }, [aiMode]);

  const saveAiKey = async () => {
    if (!aiKey.trim()) return;
    setAiSaving(true);
    try {
      await post('/mm2/ai-settings', { api_key: aiKey, model: aiModel });
      setAiHasKey(true);
      setAiMasked('*'.repeat(Math.max(0, aiKey.length - 8)) + aiKey.slice(-8));
      setAiKey('');
      setStatus('API key saved');
    } catch (e) { setStatus('Save failed: ' + e.message); }
    setAiSaving(false);
  };

  const startMM2 = async () => {
    setLoading(true);
    setStatus('Starting Model Maker v2...');
    try {
      const d = await post('/mm2/start', {});
      setStatus(d.status === 'started' ? `Started (PID ${d.pid})` : d.status === 'already_running' ? 'Already running' : `Failed: ${d.status}`);
      await checkStatus();
    } catch (e) { setStatus('Error: ' + e.message); }
    setLoading(false);
  };

  const stopMM2 = async () => {
    setLoading(true);
    try {
      const d = await post('/mm2/stop', {});
      setStatus('Stopped');
      await checkStatus();
    } catch (e) { setStatus('Error: ' + e.message); }
    setLoading(false);
  };

  return /*#__PURE__*/React.createElement("div", null,
    /*#__PURE__*/React.createElement("div", { className: "flex gap-3 mb-3 items-center flex-wrap" },
      /*#__PURE__*/React.createElement("span", { className: "text-sm" },
        "Model Maker Web v2 ",
        /*#__PURE__*/React.createElement("span", {
          className: running ? "text-green-400 font-bold" : "text-red-400 font-bold"
        }, running ? "Running" : "Stopped"),
        running && /*#__PURE__*/React.createElement("span", { className: "text-gray-400 ml-2" }, `(port ${MM2_PORT})`)
      ),
      !running && /*#__PURE__*/React.createElement("button", {
        onClick: startMM2, disabled: loading,
        className: "bg-green-600 hover:bg-green-500 px-4 py-2 rounded text-sm disabled:opacity-50"
      }, loading ? "Starting..." : "Start"),
      running && /*#__PURE__*/React.createElement("button", {
        onClick: stopMM2, disabled: loading,
        className: "bg-red-600 hover:bg-red-500 px-4 py-2 rounded text-sm disabled:opacity-50"
      }, "Stop"),
      running && /*#__PURE__*/React.createElement("a", {
        href: mm2Url, target: "_blank", rel: "noopener",
        className: "bg-blue-600 hover:bg-blue-500 px-4 py-2 rounded text-sm"
      }, "Open in New Tab"),
      /*#__PURE__*/React.createElement("button", {
        onClick: () => setAiMode(m => !m),
        className: aiMode
          ? "bg-purple-600 hover:bg-purple-500 px-4 py-2 rounded text-sm font-semibold"
          : "bg-gray-600 hover:bg-gray-500 px-4 py-2 rounded text-sm"
      }, aiMode ? "\uD83E\uDD16 AI Mode ON" : "\uD83E\uDD16 AI Mode"),
      status && /*#__PURE__*/React.createElement("span", { className: "text-xs text-yellow-400" }, status)
    ),
    aiMode && !aiHasKey && /*#__PURE__*/React.createElement("div", { className: "flex gap-3 mb-4 items-center flex-wrap" },
      /*#__PURE__*/React.createElement("select", {
        className: "bg-gray-700 rounded px-2 py-1.5 text-xs",
        value: aiModel,
        onChange: e => setAiModel(e.target.value)
      },
        ['claude-sonnet-4-6', 'claude-opus-4-6', 'claude-haiku-4-5-20251001'].map(m =>
          /*#__PURE__*/React.createElement("option", { key: m, value: m }, m))
      ),
      /*#__PURE__*/React.createElement("span", { className: "text-red-400 text-xs" },
        "\u26A0 API Key \uBBF8\uC124\uC815"),
      /*#__PURE__*/React.createElement("input", {
        type: "password",
        className: "bg-gray-700 rounded px-2 py-1.5 text-xs w-64",
        placeholder: "sk-ant-api03-...",
        value: aiKey,
        onChange: e => setAiKey(e.target.value)
      }),
      /*#__PURE__*/React.createElement("button", {
        onClick: saveAiKey,
        disabled: !aiKey.trim() || aiSaving,
        className: "bg-purple-600 hover:bg-purple-500 px-3 py-1.5 rounded text-xs disabled:opacity-50"
      }, aiSaving ? "Saving..." : "Save Key")
    ),
    // Always show iframe for Stage 1/2/3 — AI mode passes ?ai_mode=1
    // so the MM2 frontend can send use_ai:true to stage1/run
    running && /*#__PURE__*/React.createElement("div", {
      className: "border border-gray-700 rounded overflow-hidden",
      style: { height: 'calc(100vh - 180px)' }
    },
      /*#__PURE__*/React.createElement("iframe", {
        src: mm2Url + (aiMode && aiHasKey ? '?ai_mode=1' : ''),
        className: "w-full h-full border-0",
        title: "Model Maker Web v2",
        key: aiMode ? 'ai' : 'rule'  // re-mount iframe on mode change
      })
    ),
    !running && /*#__PURE__*/React.createElement(Card, null,
      /*#__PURE__*/React.createElement("div", { className: "text-center text-gray-400 py-8" },
        /*#__PURE__*/React.createElement("div", { className: "text-lg mb-2" }, "Model Maker Web v2"),
        /*#__PURE__*/React.createElement("div", { className: "text-sm mb-4" }, "PDF \u2192 Stage1 \u2192 Stage2 \u2192 Stage3 \u2192 *_registers.py"),
        /*#__PURE__*/React.createElement("div", { className: "text-xs text-gray-500" }, "Start \uBC84\uD2BC\uC744 \uB20C\uB7EC Model Maker \uC11C\uBC84\uB97C \uC2E4\uD589\uD558\uC138\uC694")
      )
    )
  );
}

// ==== MAIN APP ====
function App() {
  const TABS = ['Overview', 'Devices', 'Control', 'History', 'Events', 'Firmware', 'Config', 'Stats', 'H1 Log', 'Model Maker', 'Modbus Test'];
  const [mmEnabled, setMmEnabled] = useState(false);
  const [mtEnabled, setMtEnabled] = useState(false);
  const [tab, setTab] = useState('Overview');
  const [rtus, setRtus] = useState([]);
  const [selectedRtu, setSelectedRtu] = useState('');
  const [rawPackets, setRawPackets] = useState([]);
  const [serverVersion, setServerVersion] = useState('');

  useEffect(() => {
    fetcher('/health').then(d => { if (d?.version) setServerVersion('v' + d.version); }).catch(() => {});
    // Check modelmaker/modbustest flags from ai_settings.ini
    fetcher('/mm2/ai-settings').then(d => {
      setMmEnabled(!!d?.modelmaker_enabled);
      setMtEnabled(!!d?.modbustest_enabled);
    }).catch(() => {});
  }, []);

  // Load RTUs
  useEffect(() => {
    const load = () => fetcher('/rtus').then(d => setRtus(Array.isArray(d?.rtus) ? d.rtus : Array.isArray(d) ? d : [])).catch(() => {});
    load();
    const iv = setInterval(load, 10000);
    return () => clearInterval(iv);
  }, []);

  // WebSocket for real-time
  const [wsEvents, setWsEvents] = useState([]);
  const [wsUpdateCounter, setWsUpdateCounter] = useState(0);
  useWebSocket(useCallback(msg => {
    if (msg.type === 'rtu_status' || msg.type === 'h01_data' || msg.type === 'rtu_offline') {
      fetcher('/rtus').then(d => setRtus(Array.isArray(d?.rtus) ? d.rtus : Array.isArray(d) ? d : [])).catch(() => {});
      setWsUpdateCounter(c => c + 1);
    }
    if (msg.type === 'event') {
      // Buffer up to 499 prior events + 1 new. Bulk control (11 inverters ×
      // 4 events = 44) fits easily with headroom for concurrent H01/H05
      // background traffic.
      setWsEvents(p => [...p.slice(-499), {
        time: new Date().toLocaleTimeString(),
        rtu_id: msg.rtu_id,
        event_type: msg.event_type,
        detail: msg.detail
      }]);
      setWsUpdateCounter(c => c + 1);
    }
    if (msg.type === 'raw_packet') {
      setRawPackets(p => [...p.slice(-199), {...msg, _time: new Date().toLocaleTimeString()}]);
    }
  }, []));
  const handleSelectRtu = id => {
    setSelectedRtu(String(id));
    setTab('Devices');
  };
  return /*#__PURE__*/React.createElement("div", {
    className: "min-h-screen"
  }, /*#__PURE__*/React.createElement("header", {
    className: "bg-gray-800 border-b border-gray-700 px-4 py-3"
  }, /*#__PURE__*/React.createElement("div", {
    className: "max-w-7xl mx-auto flex items-center justify-between"
  }, /*#__PURE__*/React.createElement("h1", {
    className: "text-xl font-bold"
  }, /*#__PURE__*/React.createElement("span", {
    className: "text-blue-400"
  }, "RTU"), " UDP System Dashboard"), /*#__PURE__*/React.createElement("div", {
    className: "flex items-center gap-4 text-sm text-gray-400"
  }, /*#__PURE__*/React.createElement("span", {
    className: "text-sm text-gray-300 font-mono font-semibold"
  }, serverVersion), /*#__PURE__*/React.createElement("span", {
    id: "header-clock"
  }), /*#__PURE__*/React.createElement("span", {
    className: "flex items-center gap-2"
  }, /*#__PURE__*/React.createElement("span", {
    className: "inline-block w-2 h-2 rounded-full bg-green-500"
  }), " Connected")))), /*#__PURE__*/React.createElement("nav", {
    className: "bg-gray-800/50 border-b border-gray-700"
  }, /*#__PURE__*/React.createElement("div", {
    className: "max-w-7xl mx-auto flex gap-1 px-4 overflow-x-auto"
  }, (() => {
    const selRtuObj = rtus.find(r => String(r.rtu_id) === String(selectedRtu));
    const isRIP = selRtuObj && selRtuObj.rtu_type === 'RIP';
    const hiddenTabs = isRIP ? ['Firmware', 'Config'] : [];
    return TABS.filter(t => !hiddenTabs.includes(t)).map(t => {
      const isDisabled = (t === 'Model Maker' && !mmEnabled) || (t === 'Modbus Test' && !mtEnabled);
      const disabledHint = t === 'Model Maker' ? 'modelmaker=NO' : t === 'Modbus Test' ? 'modbustest=NO' : '';
      return /*#__PURE__*/React.createElement("button", {
        key: t,
        onClick: isDisabled ? undefined : () => setTab(t),
        disabled: isDisabled,
        className: `px-4 py-2.5 text-sm font-medium whitespace-nowrap border-b-2 transition-colors ${isDisabled ? 'border-transparent text-gray-600 cursor-not-allowed opacity-50' : tab === t ? 'border-blue-500 text-blue-400' : 'border-transparent text-gray-400 hover:text-gray-200 hover:border-gray-500'}`,
        title: isDisabled ? `Disabled (config/ai_settings.ini: ${disabledHint})` : ''
      }, t);
    });
  })())), /*#__PURE__*/React.createElement("main", {
    className: "max-w-7xl mx-auto p-4"
  }, tab === 'Overview' && /*#__PURE__*/React.createElement(OverviewTab, {
    rtus: rtus,
    onSelectRtu: handleSelectRtu
  }), tab === 'Devices' && /*#__PURE__*/React.createElement(DevicesTab, {
    selectedRtu: selectedRtu,
    rtus: rtus,
    wsUpdateCounter: wsUpdateCounter,
    onRtuChange: id => setSelectedRtu(String(id))
  }), tab === 'Control' && /*#__PURE__*/React.createElement(ControlTab, {
    rtus: rtus,
    selectedRtu: selectedRtu,
    wsEvents: wsEvents,
    onRtuChange: id => setSelectedRtu(String(id))
  }), tab === 'History' && /*#__PURE__*/React.createElement(HistoryTab, {
    rtus: rtus,
    selectedRtu: selectedRtu,
    onRtuChange: id => setSelectedRtu(String(id))
  }), tab === 'Events' && /*#__PURE__*/React.createElement(EventsTab, {
    wsUpdateCounter: wsUpdateCounter
  }), tab === 'Firmware' && /*#__PURE__*/React.createElement(FirmwareTab, {
    rtus: rtus
  }), tab === 'Config' && /*#__PURE__*/React.createElement(ConfigTab, {
    rtus: rtus,
    selectedRtu: selectedRtu
  }), tab === 'Stats' && /*#__PURE__*/React.createElement(StatsTab, null), tab === 'H1 Log' && /*#__PURE__*/React.createElement(H1LogTab, {
    packets: rawPackets,
    rtus: rtus,
    onClear: () => setRawPackets([])
  }), tab === 'Model Maker' && /*#__PURE__*/React.createElement(ModelMakerTab, null),
  tab === 'Modbus Test' && /*#__PURE__*/React.createElement(ModbusTestTab, { rtus: rtus, selectedRtu: selectedRtu })));
}
ReactDOM.createRoot(document.getElementById('root')).render(/*#__PURE__*/React.createElement(App, null));

// Header clock
setInterval(() => {
  const el = document.getElementById('header-clock');
  if (el) {
    const now = new Date();
    const y = now.getFullYear(),
      mo = String(now.getMonth() + 1).padStart(2, '0'),
      d = String(now.getDate()).padStart(2, '0');
    const h = String(now.getHours()).padStart(2, '0'),
      mi = String(now.getMinutes()).padStart(2, '0'),
      s = String(now.getSeconds()).padStart(2, '0');
    el.textContent = `${y}-${mo}-${d} ${h}:${mi}:${s}`;
  }
}, 1000);
