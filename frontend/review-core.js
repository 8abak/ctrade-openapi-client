// PATH: frontend/review-core.js
// Review window viewer:
// - No live streaming, purely "static window" for any historical tick_id range.
// - Data comes from /api/review/window?from_id=...&window=...
//   and is expected to have: { ticks: [...], segs: [...], zones: [...] }.
// - Mid  = blue line
// - Kal  = green line
// - Zones = colored rectangular bands (from local min->max price in that zone)
// - Segs  = arrows at top/bottom of the price area (direction up/down).

/* global echarts */

const chartEl = document.getElementById('chart');
const chart   = echarts.init(chartEl);

let ticks = [];   // [{id, ts, mid, kal, bid, ask, spread}]
let segs  = [];   // [{id, start_id, end_id, direction, ...}]
let zones = [];   // [{id, start_id, end_id, direction, zone_type, ...}]

let globalMinPrice = null;
let globalMaxPrice = null;

// UI elements (defensive: they might be null if HTML differs slightly)
const fromInput   = document.getElementById('fromId');
const windowInput = document.getElementById('window');
const goBtn       = document.getElementById('goBtn');
const statusEl    = document.getElementById('status');

const chkKal     = document.getElementById('chkKal');
const chkZones   = document.getElementById('chkZones');
const chkSegs    = document.getElementById('chkSegs');

// ------------------------ helpers ------------------------

function setStatus(msg) {
  if (statusEl) statusEl.textContent = msg;
}

// find first / last tick by id inside a [start_id, end_id] range
function sliceTicksById(startId, endId) {
  if (!ticks.length) return [];
  return ticks.filter(t => t.id >= startId && t.id <= endId);
}

function recomputeGlobalMinMax() {
  if (!ticks.length) {
    globalMinPrice = null;
    globalMaxPrice = null;
    return;
  }
  let mn = Infinity;
  let mx = -Infinity;
  for (const t of ticks) {
    const v = (t.mid != null) ? t.mid : t.kal;
    if (v == null) continue;
    if (v < mn) mn = v;
    if (v > mx) mx = v;
  }
  if (!Number.isFinite(mn) || !Number.isFinite(mx)) {
    globalMinPrice = null;
    globalMaxPrice = null;
  } else {
    globalMinPrice = mn;
    globalMaxPrice = mx;
  }
}

// -------------- build ECharts series from data ------------

function buildMidSeries() {
  const data = ticks.map(t => ({
    value: [t.ts, t.mid],
    meta: t
  }));
  return {
    name: 'Mid',
    type: 'line',
    showSymbol: false,
    lineStyle: { width: 1.5 },
    data
  };
}

function buildKalSeries() {
  const enabled = !chkKal || chkKal.checked;
  const data = ticks
    .filter(t => t.kal != null)
    .map(t => ({
      value: [t.ts, t.kal],
      meta: t
    }));
  return {
    name: 'Kalman',
    type: 'line',
    showSymbol: false,
    lineStyle: { width: 1, opacity: enabled ? 1 : 0 },
    data,
    z: 3
  };
}

function zoneColor(z) {
  // Simple mapping based on zone_type + direction
  const base = (z.zone_type || '').toUpperCase();
  if (base === 'TREND') {
    return z.direction > 0 ? 'rgba(25, 135, 84, 0.18)'   // strong green
                           : 'rgba(220, 53, 69, 0.18)';  // strong red
  }
  if (base === 'WEAK_TREND') {
    return z.direction > 0 ? 'rgba(25, 135, 84, 0.10)'
                           : 'rgba(220, 53, 69, 0.10)';
  }
  if (base === 'CHOP') {
    return 'rgba(108, 117, 125, 0.12)'; // gray
  }
  return 'rgba(255, 193, 7, 0.10)'; // OTHER = amber-ish
}

function buildZoneMarkAreas() {
  const enabled = !chkZones || chkZones.checked;
  if (!enabled || !zones.length || !ticks.length || globalMinPrice == null) {
    return [];
  }

  const data = [];

  for (const z of zones) {
    const zTicks = sliceTicksById(z.start_id, z.end_id);
    if (!zTicks.length) continue;

    let localMin = Infinity;
    let localMax = -Infinity;
    for (const t of zTicks) {
      const v = (t.mid != null) ? t.mid : t.kal;
      if (v == null) continue;
      if (v < localMin) localMin = v;
      if (v > localMax) localMax = v;
    }
    if (!Number.isFinite(localMin) || !Number.isFinite(localMax)) {
      localMin = globalMinPrice;
      localMax = globalMaxPrice;
    }

    const startTs = zTicks[0].ts;
    const endTs   = zTicks[zTicks.length - 1].ts;

    data.push([
      {
        coord: [startTs, localMin],
        itemStyle: { color: zoneColor(z) }
      },
      {
        coord: [endTs, localMax]
      }
    ]);
  }

  return data;
}

function buildSegSeries() {
  const enabled = !chkSegs || chkSegs.checked;
  if (!enabled || !segs.length || !ticks.length || globalMinPrice == null) {
    return {
      name: 'Segs',
      type: 'scatter',
      data: [],
      z: 6
    };
  }

  const midPrice = (globalMinPrice + globalMaxPrice) / 2;
  const topY     = globalMaxPrice + (globalMaxPrice - midPrice) * 0.06;
  const bottomY  = globalMinPrice - (midPrice - globalMinPrice) * 0.06;

  const data = [];

  for (const s of segs) {
    const sTicks = sliceTicksById(s.start_id, s.end_id);
    if (!sTicks.length) continue;
    const midIndex = Math.floor(sTicks.length / 2);
    const midTick  = sTicks[midIndex];

    const y = s.direction > 0 ? topY : bottomY;
    data.push({
      value: [midTick.ts, y],
      symbol: 'triangle',
      symbolSize: 10,
      symbolRotate: s.direction > 0 ? 0 : 180,
      itemStyle: {
        color: s.direction > 0 ? '#2ea043' : '#f85149'
      }
    });
  }

  return {
    name: 'Segs',
    type: 'scatter',
    data,
    z: 6
  };
}

// --------------------- main chart option --------------------

function rebuildChart() {
  recomputeGlobalMinMax();

  const midSeries = buildMidSeries();
  const kalSeries = buildKalSeries();
  const segSeries = buildSegSeries();
  const zoneAreas = buildZoneMarkAreas();

  const showKal   = !chkKal   || chkKal.checked;
  const showZones = !chkZones || chkZones.checked;
  const showSegs  = !chkSegs  || chkSegs.checked;

  chart.setOption({
    backgroundColor: '#0d1117',
    animation: false,
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'line' },
      formatter(params) {
        // Find Mid point first, otherwise first series
        const p = params.find(x => x.seriesName === 'Mid') || params[0];
        if (!p || !p.data || !p.data.meta) return '';
        const d  = p.data.meta;
        const dt = new Date(d.ts);
        const date = dt.toLocaleDateString();
        const time = dt.toLocaleTimeString();
        const fmt = v => (v == null ? '' : (+v).toFixed(2));

        const lines = [
          `id: ${d.id}`,
          `${date} ${time}`,
          `mid: ${fmt(d.mid)}`,
          `kal: ${fmt(d.kal)}`,
          `bid: ${fmt(d.bid)}`,
          `ask: ${fmt(d.ask)}`,
          `spread: ${fmt(d.spread)}`
        ];
        return lines.join('<br/>');
      }
    },
    grid: { left: 48, right: 24, top: 40, bottom: 48 },
    xAxis: {
      type: 'time',
      axisLabel: { color: '#c9d1d9' },
      axisLine: { lineStyle: { color: '#30363d' } },
      splitLine: { lineStyle: { color: '#161b22' } }
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
    series: [
      midSeries,
      showKal   ? kalSeries : { ...kalSeries, data: [], lineStyle: { opacity: 0 } },
      showSegs  ? segSeries : { ...segSeries, data: [] },
      {
        name: 'Zones',
        type: 'line',        // dummy, we only use markArea
        data: [],
        markArea: {
          silent: true,
          itemStyle: { opacity: 1 },
          data: showZones ? zoneAreas : []
        },
        z: 1
      }
    ]
  });
}

// ----------------------- data loading -----------------------

async function loadWindow(fromId, winSize) {
  if (!fromId || !winSize) return;
  try {
    setStatus('Loading...');
    const resp = await fetch(`/api/review/window?from_id=${fromId}&window=${winSize}`);
    if (!resp.ok) {
      throw new Error(`HTTP ${resp.status}`);
    }
    const payload = await resp.json();
    ticks = (payload.ticks || []).map(t => ({
      id: t.id,
      ts: t.ts,           // already ISO string
      mid: t.mid,
      kal: t.kal,
      bid: t.bid,
      ask: t.ask,
      spread: t.spread
    }));
    segs  = payload.segs  || [];
    zones = payload.zones || [];

    setStatus(`Loaded ${ticks.length} ticks, ${segs.length} segs, ${zones.length} zones`);
    rebuildChart();
  } catch (err) {
    console.error('Error loading review window:', err);
    alert('Error loading data — see console for details.');
    setStatus('Error.');
  }
}

// ----------------------- UI wiring --------------------------

if (goBtn) {
  goBtn.addEventListener('click', () => {
    const fromId   = parseInt(fromInput ? fromInput.value : '0', 10) || 0;
    const winSize  = parseInt(windowInput ? windowInput.value : '5000', 10) || 5000;
    loadWindow(fromId, winSize);
  });
}

// allow pressing Enter in either input to trigger Go
if (fromInput) {
  fromInput.addEventListener('keydown', e => {
    if (e.key === 'Enter' && goBtn) goBtn.click();
  });
}
if (windowInput) {
  windowInput.addEventListener('keydown', e => {
    if (e.key === 'Enter' && goBtn) goBtn.click();
  });
}

if (chkKal)   chkKal.addEventListener('change', rebuildChart);
if (chkZones) chkZones.addEventListener('change', rebuildChart);
if (chkSegs)  chkSegs.addEventListener('change', rebuildChart);

window.addEventListener('resize', () => chart.resize());

// Initial empty chart (no data until you press Go)
rebuildChart();
