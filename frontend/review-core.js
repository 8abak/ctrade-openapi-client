// Review window viewer:
// - Nothing loads by default.
// - When you hit "Go", it fetches /api/review/window?from_id=&window=
//   and draws:
//     * mid line (blue)
//     * kalman line (green)
//     * zones as rectangles from min→max price
//     * kalseg segs as arrows at top/bottom of price area.

const API = '/api';

const chart = echarts.init(document.getElementById('chart'));

const state = {
  ticks: [],
  segs: [],
  zones: [],
};

const fromInput  = document.getElementById('fromId');
const winInput   = document.getElementById('win');
const goButton   = document.getElementById('btnGo');
const statusSpan = document.getElementById('status');

const showKal    = document.getElementById('showKal');
const showZones  = document.getElementById('showZones');
const showSegs   = document.getElementById('showSegs');

function setStatus(msg) {
  statusSpan.textContent = msg || '';
}

// ------------------------ chart base option --------------------------

function initChart() {
  chart.setOption({
    backgroundColor: '#0d1117',
    animation: false,
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'cross' },
      formatter: params => {
        // Find mid line point
        const midP = params.find(p => p.seriesName === 'Mid');
        if (!midP) return '';
        const d = midP.data && midP.data.meta;
        if (!d) return '';

        const dt = new Date(d.ts);
        const fmtPrice = v => (v == null ? '' : (+v).toFixed(2));

        const lines = [
          `id: ${d.id}`,
          `${dt.toLocaleDateString()} ${dt.toLocaleTimeString()}`,
          `mid: ${fmtPrice(d.mid)}`,
          d.kal != null ? `kal: ${fmtPrice(d.kal)}` : '',
          d.bid != null ? `bid: ${fmtPrice(d.bid)}` : '',
          d.ask != null ? `ask: ${fmtPrice(d.ask)}` : '',
          d.spread != null ? `spread: ${fmtPrice(d.spread)}` : '',
        ].filter(Boolean);

        return lines.join('<br>');
      },
    },
    grid: { left: 50, right: 20, top: 24, bottom: 40 },
    xAxis: {
      type: 'value',
      name: 'tick id',
      boundaryGap: false,
      axisLine: { lineStyle: { color: '#30363d' } },
      axisLabel: { color: '#c9d1d9' },
    },
    yAxis: {
      type: 'value',
      scale: true,
      axisLine: { lineStyle: { color: '#30363d' } },
      splitLine: { lineStyle: { color: '#30363d' } },
      axisLabel: {
        color: '#c9d1d9',
        formatter: v => String(Math.round(v)),
      },
    },
    dataZoom: [
      { type: 'inside', xAxisIndex: 0, filterMode: 'weakFilter' },
      { type: 'slider', xAxisIndex: 0, bottom: 6 },
    ],
    series: [
      { // 0: mid
        name: 'Mid',
        type: 'line',
        showSymbol: false,
        lineStyle: { width: 1.2 },
        data: [],
        z: 10,
      },
      { // 1: kalman
        name: 'Kalman',
        type: 'line',
        showSymbol: false,
        lineStyle: { width: 1.4 },
        data: [],
        z: 11,
      },
      { // 2: zones (rectangles via custom series)
        name: 'Zones',
        type: 'custom',
        renderItem: renderZoneItem,
        data: [],
        silent: true,
        z: 1,
      },
      { // 3: seg arrows
        name: 'Segs',
        type: 'scatter',
        symbol: 'triangle',
        symbolSize: 12,
        data: [],
        z: 20,
        itemStyle: {
          borderWidth: 1,
        },
        // rotate up/down
        symbolRotate: function (value, params) {
          const d = params.data || {};
          return d.dir === 1 ? 0 : 180;
        },
      },
    ],
  });
}

initChart();
window.addEventListener('resize', () => chart.resize());

// ----------------------------- helpers -------------------------------

function computeExtents(ticks) {
  let min = Infinity, max = -Infinity;
  for (const t of ticks || []) {
    if (t.mid == null) continue;
    const v = +t.mid;
    if (v < min) min = v;
    if (v > max) max = v;
  }
  if (!isFinite(min) || !isFinite(max)) {
    min = 0;
    max = 1;
  }
  return { min, max };
}

function buildSeries() {
  const ticks = state.ticks;
  const segs  = state.segs;
  const zones = state.zones;

  if (!ticks.length) {
    chart.setOption({
      series: [
        { data: [] },
        { data: [] },
        { data: [] },
        { data: [] },
      ],
    });
    return;
  }

  const { min, max } = computeExtents(ticks);
  const pad = (max - min) * 0.05;
  const bottomY = min + pad;
  const topY    = max - pad;

  // mid + kal lines
  const midData = [];
  const kalData = [];

  for (const t of ticks) {
    const point = {
      value: [t.id, t.mid],
      meta: t,
    };
    midData.push(point);
    if (t.kal != null) {
      kalData.push({ value: [t.id, t.kal], meta: t });
    }
  }

  // zones => rectangle data: [start_id, end_id, minPrice, maxPrice, direction, zone_type]
  const zoneData = [];
  if (zones.length) {
    for (const z of zones) {
      const start = z.start_id;
      const end   = z.end_id;
      let zMin = Infinity;
      let zMax = -Infinity;

      for (const t of ticks) {
        if (t.id < start || t.id > end) continue;
        const y = t.kal != null ? t.kal : t.mid;
        if (y == null) continue;
        if (y < zMin) zMin = y;
        if (y > zMax) zMax = y;
      }
      if (!isFinite(zMin) || !isFinite(zMax) || zMin === zMax) continue;

      zoneData.push({
        value: [
          start,
          end,
          zMin,
          zMax,
          z.direction || 0,
          z.zone_type || '',
        ],
      });
    }
  }

  // segs => arrows
  const segData = [];
  if (segs.length) {
    for (const s of segs) {
      const centerX = (s.start_id + s.end_id) / 2;
      const y = s.direction === 1 ? bottomY : topY;
      segData.push({
        value: [centerX, y],
        dir: s.direction,
        itemStyle: {
          color: s.direction === 1 ? '#22c55e' : '#f97373',
          borderColor: '#111827',
        },
      });
    }
  }

  chart.setOption({
    series: [
      { data: midData },
      { data: showKal.checked ? kalData : [] },
      { data: showZones.checked ? zoneData : [] },
      { data: showSegs.checked ? segData : [] },
    ],
  });
}

// Custom renderer for zone rectangles
function renderZoneItem(params, api) {
  const startId = api.value(0);
  const endId   = api.value(1);
  const minP    = api.value(2);
  const maxP    = api.value(3);
  const dir     = api.value(4);

  const startCoord = api.coord([startId, minP]);
  const endCoord   = api.coord([endId,   maxP]);

  const x = startCoord[0];
  const y = endCoord[1];
  const w = endCoord[0] - startCoord[0];
  const h = startCoord[1] - endCoord[1];

  const isUp   = dir === 1;
  const isDown = dir === -1;

  const fill = isUp
    ? 'rgba(56, 189, 248, 0.10)'     // up zone
    : isDown
      ? 'rgba(248, 113, 113, 0.10)'  // down zone
      : 'rgba(148, 163, 184, 0.08)'; // neutral

  const stroke = isUp
    ? '#0ea5e9'
    : isDown
      ? '#f97373'
      : '#9ca3af';

  return {
    type: 'rect',
    shape: { x, y, width: w, height: h },
    style: api.style({
      fill,
      stroke,
      lineWidth: 1,
    }),
  };
}

// -------------------------- data loading -----------------------------

async function loadWindow() {
  const from = parseInt(fromInput.value, 10);
  const win  = parseInt(winInput.value, 10);

  if (!from || !win || from < 1 || win <= 0) {
    alert('Please enter a valid start tick id and window.');
    return;
  }

  setStatus('Loading…');

  try {
    const resp = await fetch(
      `${API}/review/window?from_id=${from}&window=${win}`
    );
    if (!resp.ok) {
      throw new Error(`HTTP ${resp.status}`);
    }
    const data = await resp.json();
    state.ticks = data.ticks || [];
    state.segs  = data.segs || [];
    state.zones = data.zones || [];

    setStatus(
      `Loaded ${state.ticks.length} ticks, ` +
      `${state.segs.length} segs, ${state.zones.length} zones`
    );
    buildSeries();
  } catch (err) {
    console.error('Error loading review window', err);
    alert('Error loading data – see console for details.');
    setStatus('Error.');
  }
}

// ------------------------- event bindings ----------------------------

goButton.addEventListener('click', loadWindow);
showKal.addEventListener('change', buildSeries);
showZones.addEventListener('change', buildSeries);
showSegs.addEventListener('change', buildSeries);

// Optionally: hitting Enter in inputs triggers Go
[fromInput, winInput].forEach(el => {
  el.addEventListener('keydown', ev => {
    if (ev.key === 'Enter') loadWindow();
  });
});
