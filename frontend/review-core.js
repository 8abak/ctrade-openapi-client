// PATH: frontend/review-core.js
// Historical review page
// - loads NOTHING by default
// - when user enters a tick id and clicks "Go":
//     * loads ~windowSize ticks to the right (default 5000)
//     * draws mid + kal (if present) lines
//     * overlays kal zones as coloured markAreas
// Depends on:
//   - <div id="chart"></div>
//   - <input id="fromId">
//   - <input id="windowSize">  (optional, default 5000)
//   - <button id="btnLoad">
//   - (optional) <span id="status">

const SQL_API = '/api/sql';
const chartEl = document.getElementById('chart');
const chart = echarts.init(chartEl);

// ----- State -----
let ticks = [];   // {id, ts: Date, mid, kal}
let zones = [];   // {id, start_id, end_id, start_ts, end_ts, direction, zone_type}
let currentFromId = null;
let currentWindow = 5000;

// ----- Helpers -----
async function fetchSqlRows(sql) {
  const res = await fetch(SQL_API, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ sql })
  });
  if (!res.ok) {
    const txt = await res.text();
    console.error('SQL error', txt);
    throw new Error('SQL error: ' + txt);
  }
  const json = await res.json();
  return json.rows || [];
}

function setStatus(msg) {
  const el = document.getElementById('status');
  if (el) el.textContent = msg || '';
}

function parseIntSafe(val, def) {
  const n = parseInt(val, 10);
  return Number.isFinite(n) && n > 0 ? n : def;
}

// Find nearest ts for a tick id in our loaded window
function findTsForIdOrNearest(id) {
  if (!ticks.length) return null;
  // ticks are sorted by id ascending
  // exact or first >= id
  for (let i = 0; i < ticks.length; i++) {
    if (ticks[i].id >= id) return ticks[i].ts;
  }
  // otherwise last one
  return ticks[ticks.length - 1].ts;
}

// ----- Chart setup -----
function setupChart() {
  chart.setOption({
    backgroundColor: '#0d1117',
    animation: false,
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'line' },
      formatter: (params) => {
        if (!params || !params.length) return '';
        // Prefer mid series for meta
        const p = params.find(x => x.seriesName === 'Mid') || params[0];
        const d = p && p.data && p.data.meta ? p.data.meta : null;
        if (!d) return '';
        const dt = d.ts;
        const lines = [
          `id: ${d.id}`,
          dt.toLocaleString(),
          `mid: ${d.mid != null ? d.mid.toFixed(2) : ''}`,
          `kal: ${d.kal != null ? d.kal.toFixed(2) : ''}`
        ];
        return lines.join('<br/>');
      }
    },
    grid: { left: 48, right: 24, top: 24, bottom: 48 },
    xAxis: {
      type: 'time',
      axisLabel: { color: '#c9d1d9' },
      axisLine: { lineStyle: { color: '#30363d' } }
    },
    yAxis: {
      type: 'value',
      scale: true,
      minInterval: 1,
      splitNumber: 8,
      axisLabel: {
        color: '#c9d1d9',
        formatter: v => String(Math.round(v))
      },
      splitLine: { lineStyle: { color: '#30363d' } }
    },
    dataZoom: [
      { type: 'inside', xAxisIndex: 0, filterMode: 'weakFilter' },
      { type: 'slider', xAxisIndex: 0, bottom: 6 }
    ],
    legend: {
      top: 0,
      textStyle: { color: '#c9d1d9' }
    },
    series: [
      {
        name: 'Mid',
        type: 'line',
        showSymbol: false,
        lineStyle: { width: 1.5 },
        data: []
      },
      {
        name: 'Kalman',
        type: 'line',
        showSymbol: false,
        lineStyle: { width: 1, type: 'dashed' },
        data: []
      },
      {
        name: 'Zones',
        type: 'line',
        data: [],
        markArea: { data: [] }
      }
    ]
  });
}

function rebuildChart() {
  if (!ticks.length) {
    chart.setOption({
      series: [
        { data: [] },
        { data: [] },
        { data: [], markArea: { data: [] } }
      ]
    });
    return;
  }

  const midSeries = ticks.map(t => ({
    value: [t.ts, t.mid],
    meta: t
  }));

  const hasKal = ticks.some(t => t.kal != null);
  const kalSeries = hasKal
    ? ticks
        .filter(t => t.kal != null)
        .map(t => ({ value: [t.ts, t.kal], meta: t }))
    : [];

  // Build zone markAreas
  const zoneColors = {
    'TREND': 'rgba(34,197,94,0.18)',       // green-ish
    'WEAK_TREND': 'rgba(59,130,246,0.16)', // blue-ish
    'CHOP': 'rgba(248,113,113,0.18)',      // red-ish
    'OTHER': 'rgba(148,163,184,0.12)'      // gray-ish
  };

  const areas = zones.map(z => {
    const startTs = z.start_ts || findTsForIdOrNearest(z.start_id);
    const endTs   = z.end_ts   || findTsForIdOrNearest(z.end_id);
    if (!startTs || !endTs) return null;

    const color = zoneColors[z.zone_type] || zoneColors['OTHER'];
    const label = `${z.zone_type || 'ZONE'} (${z.direction > 0 ? '↑' : z.direction < 0 ? '↓' : '≈'})`;

    return [
      {
        name: label,
        xAxis: startTs,
        itemStyle: { color }
      },
      {
        xAxis: endTs
      }
    ];
  }).filter(Boolean);

  chart.setOption({
    series: [
      { name: 'Mid', data: midSeries },
      { name: 'Kalman', data: kalSeries },
      { name: 'Zones', data: [], markArea: { data: areas } }
    ]
  });
}

// ----- Loading logic -----
async function loadWindow(fromId, windowSize) {
  currentFromId = fromId;
  currentWindow = windowSize;

  const startId = fromId;
  const endId = fromId + windowSize - 1;

  setStatus(`Loading ticks ${startId} – ${endId} ...`);

  // 1) Ticks with mid + kal
  const tickSql = `
    SELECT id, ts, mid, kal
    FROM ticks
    WHERE id BETWEEN ${startId} AND ${endId}
    ORDER BY id
  `;
  // 2) Zones overlapping this id window
  const zoneSql = `
    SELECT id, start_id, end_id,
           start_ts, end_ts,
           direction,
           zone_type
    FROM zones
    WHERE NOT (end_id < ${startId} OR start_id > ${endId})
    ORDER BY start_id
  `;

  const [tickRows, zoneRows] = await Promise.all([
    fetchSqlRows(tickSql),
    fetchSqlRows(zoneSql)
  ]);

  // Normalize ticks
  ticks = tickRows.map(r => ({
    id: Number(r.id),
    ts: new Date(r.ts),
    mid: r.mid != null ? Number(r.mid) : null,
    kal: r.kal != null ? Number(r.kal) : null
  }));

  zones = zoneRows.map(r => ({
    id: Number(r.id),
    start_id: Number(r.start_id),
    end_id: Number(r.end_id),
    start_ts: r.start_ts ? new Date(r.start_ts) : null,
    end_ts: r.end_ts ? new Date(r.end_ts) : null,
    direction: r.direction != null ? Number(r.direction) : 0,
    zone_type: r.zone_type || 'OTHER'
  }));

  setStatus(`Loaded ${ticks.length} ticks, ${zones.length} zones.`);
  rebuildChart();
}

// ----- UI wiring -----
function wireUi() {
  const btn = document.getElementById('btnLoad');
  const fromInput = document.getElementById('fromId');
  const winInput = document.getElementById('windowSize');

  if (winInput && !winInput.value) {
    winInput.value = String(currentWindow);
  }

  if (btn) {
    btn.addEventListener('click', async () => {
      const fromVal = parseIntSafe(fromInput && fromInput.value, null);
      if (!fromVal) {
        alert('Please enter a valid starting tick id.');
        return;
      }
      const winVal = parseIntSafe(winInput && winInput.value, currentWindow);
      try {
        await loadWindow(fromVal, winVal);
      } catch (err) {
        console.error(err);
        setStatus('Error: ' + err.message);
        alert('Error loading data – see console for details.');
      }
    });
  }
}

// ----- Bootstrap -----
window.addEventListener('resize', () => chart.resize());
setupChart();
wireUi();
// NOTE: no auto-load; page starts empty on purpose.
