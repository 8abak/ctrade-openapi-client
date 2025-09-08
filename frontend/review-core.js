// frontend/review-core.js
// Review page wired to backend/main.py routes. SAFE single-file version.
// Restores segm list, keeps segment click -> load, and adds:
// - Smart Y auto-fit to visible X window (with padding)
// - "No thinning" and "Show ticks" toggles
// - ATR (atr1) on right axis, shown if present in ticks or separately loaded
// - Chunk-based thinning remains; "No thinning" disables pixel sampling
// Does NOT change your routes. Uses /api/segm/recent (if present) with a SQL fallback,
// and /api/segm?id=...

(() => {
  // ---------- DOM helpers (defensive selectors so we don't "lose" the list) ----------
  const qs  = (s, r=document) => r.querySelector(s);
  const qsa = (s, r=document) => Array.from(r.querySelectorAll(s));

  // likely ids in your page (we try several)
  const segmTableBody =
      qs('#journal tbody') ||
      qs('#segmTable tbody') ||
      qs('.segm-table tbody') ||
      createFallbackSegmTable();

  const segInfo  = qs('#seginfo') || ensureInfoLine();
  const chunkInp = qs('#chunkInput') || qs('input[name="chunk"]');
  const chartEl  = qs('#reviewChart') || qs('#chart');

  if (!chartEl) {
    console.error('review-core: chart element not found (#reviewChart or #chart).');
    return;
  }

  const API = '/api';
  const chart = echarts.init(chartEl);

  // ---------- State ----------
  const State = {
    chunk: 2000,
    noThinning: false,
    showTicks: true,
    lastSegm: null,       // last loaded segm object from /api/segm?id=...
    xMin: null,           // current X window min (ms) if focused/zoomed
    xMax: null            // current X window max (ms)
  };

  // ---------- Fallback small scaffolding if page lacks a table/info ----------
  function createFallbackSegmTable() {
    const wrap = document.createElement('div');
    wrap.style.margin = '8px 0';
    wrap.innerHTML = `
      <table id="segmTable" class="segm-table">
        <thead>
          <tr>
            <th>ID</th><th>Start TS</th><th>Duration(s)</th><th>Dir</th><th>Span</th><th>Len</th>
          </tr>
        </thead>
        <tbody></tbody>
      </table>`;
    const host = qs('#segments') || qs('.segments-panel') || document.body;
    host.appendChild(wrap);
    return wrap.querySelector('tbody');
  }

  function ensureInfoLine() {
    const el = document.createElement('div');
    el.id = 'seginfo';
    el.style.margin = '6px 0';
    el.textContent = 'Segment: —';
    const host = qs('#review') || document.body;
    host.appendChild(el);
    return el;
  }

  // ---------- Utils ----------
  const fmt2 = (x) =>
    x === null || x === undefined || isNaN(+x) ? '' : (+x).toFixed(2);

  function ensureArray(v) { return Array.isArray(v) ? v : (v ? [v] : []); }

  function thin(arr, n) {
    if (!arr || arr.length <= n) return arr || [];
    const stride = Math.ceil(arr.length / n);
    const out = [];
    for (let i = 0; i < arr.length; i += stride) out.push(arr[i]);
    if (arr.length && out[out.length - 1] !== arr[arr.length - 1]) out.push(arr[arr.length - 1]);
    return out;
  }

  // ---------- Fetch segm list ----------
  async function fetchRecentSegm(limit = 200) {
    // Prefer native route, fall back to SQL viewer (keeps your previous behavior).
    try {
      const r = await fetch(`${API}/segm/recent?limit=${Math.max(1, Math.min(limit, 500))}`);
      if (r.ok) return await r.json();
    } catch { /* ignore */ }

    const q = encodeURIComponent(`
      SELECT id, start_id, end_id, start_ts, end_ts, dir, span, len
      FROM segm ORDER BY id DESC
      LIMIT ${Math.max(1, Math.min(limit, 500))}
    `.trim());
    const r2 = await fetch(`/sqlvw/query?query=${q}`);
    if (!r2.ok) throw new Error(`segm list failed: ${r2.status}`);
    return await r2.json();
  }

  async function loadSegmList() {
    const rows = await fetchRecentSegm(200);
    segmTableBody.innerHTML = '';
    for (const s of rows) {
      const tr = document.createElement('tr');
      const durSec = Math.max(0, (new Date(s.end_ts) - new Date(s.start_ts)) / 1000) | 0;
      tr.setAttribute('data-start', s.start_ts);
      tr.setAttribute('data-end', s.end_ts);
      tr.innerHTML = `
        <td>${s.id}</td>
        <td>${new Date(s.start_ts).toLocaleString()}</td>
        <td>${durSec}</td>
        <td>${s.dir ?? ''}</td>
        <td>${fmt2(s.span)}</td>
        <td>${s.len ?? ''}</td>
      `;
      tr.addEventListener('click', () => {
        focusWindow(s.start_ts, s.end_ts);   // x focus first (so y can fit nicely)
        loadSegment(s.id);
      });
      segmTableBody.appendChild(tr);
    }
    segInfo.textContent = 'Segment: —';
  }

  // ---------- Chart setup ----------
  function setupChart() {
    chart.setOption({
      backgroundColor: '#0d1117',
      animation: false,
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'cross' },
        formatter: (params) => {
          const midP = params.find(p => p.seriesName === 'mid');
          const d = midP?.data?.meta;
          if (!d) return '';
          const dt = new Date(d.ts);
          const lines = [
            `${dt.toLocaleDateString()} ${dt.toLocaleTimeString()}`,
            `id: ${d.id}`,
            `mid: ${fmt2(d.mid)}`
          ];
          return lines.join('<br/>');
        }
      },
      grid: { left: 56, right: 48, top: 24, bottom: 56 },
      xAxis: [{ type: 'time', axisPointer: { show: true } }],
      yAxis: [
        { type: 'value', scale: true, name: 'Price', axisLabel: { formatter: v => (+v).toFixed(2) } },
        { type: 'value', scale: true, name: 'ATR', position: 'right', axisLabel: { formatter: v => (+v).toFixed(4) } }
      ],
      dataZoom: [
        { type: 'inside', throttle: 0 },
        { type: 'slider', height: 20 }
      ],
      series: [
        // base mid line (toggleable)
        { name: 'mid', type: 'line', showSymbol: false, yAxisIndex: 0, data: [], large: true, largeThreshold: 200000, sampling: 'lttb' },
        // ATR line (right axis). Only visible if we load data for it.
        { name: 'atr1', type: 'line', showSymbol: false, yAxisIndex: 1, data: [], large: true, largeThreshold: 200000, sampling: 'lttb', lineStyle: { width: 1.5 } },
        // Big moves shading
        { name: 'bigm', type: 'line', data: [], markArea: { itemStyle: { color: 'rgba(234,179,8,0.18)' }, data: [] } },
        // Small moves as red connecting lines
        { name: 'smal', type: 'line', showSymbol: false, data: [], markLine: { silent: true, symbol: ['none','none'], lineStyle: { width: 2 }, data: [] } },
        // Predictions as scatter (✓/✗ in label if present)
        { name: 'pred', type: 'scatter', symbolSize: 10, data: [], label: { show: true, formatter: (p)=> p.data?.p?.hit===true?'✓':(p.data?.p?.hit===false?'✗':'') } }
      ]
    });

    // Refit Y when the user zooms/pans
    chart.on('dataZoom', refitYAxes);
  }

  // ---------- Data mappers ----------
  function makeTimeIndex(ticks) {
    const pairs = ticks.map(t => [+new Date(t.ts), +t.mid]).sort((a, b) => a[0] - b[0]);
    function yAt(ts) {
      if (!pairs.length) return null;
      const x = +new Date(ts);
      let lo = 0, hi = pairs.length - 1, best = pairs[0];
      while (lo <= hi) {
        const m = (lo + hi) >> 1, dx = pairs[m][0] - x;
        if (Math.abs(dx) < Math.abs(best[0] - x)) best = pairs[m];
        if (dx === 0) break;
        if (dx < 0) lo = m + 1; else hi = m - 1;
      }
      return best[1];
    }
    return { yAt };
  }

  function mapTicksForSeries(ticks) {
    const pts = ticks.map(t => ({
      value: [new Date(t.ts), +t.mid],
      meta: t
    }));
    return pts;
    // (If you want a rolling mean again, we can add it back, but keeping it simple/fast here)
  }

  function buildBigAreas(bigm) {
    const areas = [];
    for (const b of ensureArray(bigm)) {
      const a = b?.a_ts, c = b?.b_ts;
      if (!a || !c) continue;
      areas.push([{ xAxis: new Date(a) }, { xAxis: new Date(c) }]);
    }
    return areas;
  }

  function buildSmallMarkLines(smal, idx) {
    const data = [];
    for (const s of ensureArray(smal)) {
      const a = s?.a_ts, b = s?.b_ts;
      if (!a || !b) continue;
      const y1 = idx.yAt(a), y2 = idx.yAt(b);
      if (y1 == null || y2 == null) continue;
      data.push([{ coord: [new Date(a), +y1] }, { coord: [new Date(b), +y2] }]);
    }
    return data;
  }

  function buildPredScatter(pred, idx) {
    const dots = [];
    for (const p of ensureArray(pred)) {
      const x = p?.at_ts ? new Date(p.at_ts) : null;
      if (!x) continue;
      const y = idx.yAt(p.at_ts);
      if (y == null) continue;
      dots.push({
        value: [x, +y],
        p,
        itemStyle: {
          color: p?.hit === true ? '#2ea043' : (p?.hit === false ? '#f85149' : '#8b949e')
        },
        symbol: p?.hit == null ? 'circle' : (p?.hit ? 'triangle' : 'rect')
      });
    }
    return dots;
  }

  // Try to extract ATR from ticks (tick.atr1 or tick.atr)
  function extractAtrFromTicks(ticks) {
    const arr = [];
    for (const t of ensureArray(ticks)) {
      const v = (t.atr1 != null ? t.atr1 : t.atr);
      if (v == null) continue;
      arr.push([new Date(t.ts), +v]);
    }
    return arr;
  }

  // ---------- Load a single segment ----------
  async function loadSegment(segmId) {
    try {
      const r = await fetch(`${API}/segm?id=${segmId}`);
      if (!r.ok) throw new Error(`segm ${segmId} fetch failed: ${r.status}`);
      const data = await r.json();

      const ticks = Array.isArray(data.ticks) ? data.ticks : [];
      const midSeries = mapTicksForSeries(ticks);
      const timeIdx   = makeTimeIndex(ticks);

      // series data
      const bigAreas  = buildBigAreas(data.bigm || []);
      const smalLines = buildSmallMarkLines(data.smal || [], timeIdx);
      const predDots  = buildPredScatter(data.pred || [], timeIdx);

      // ATR values (from ticks if present)
      let atrSeries = extractAtrFromTicks(ticks);

      // Draw
      const midData  = State.noThinning ? midSeries : thin(midSeries, State.chunk);
      const atrData  = State.noThinning ? atrSeries : thin(atrSeries, State.chunk);

      // keep current X window if already focused; otherwise use segm bounds
      const xMin = State.xMin != null ? State.xMin : (ticks[0] ? +new Date(ticks[0].ts) : null);
      const xMax = State.xMax != null ? State.xMax : (ticks[ticks.length - 1] ? +new Date(ticks[ticks.length - 1].ts) : null);

      chart.setOption({
        xAxis: [{ min: xMin ?? 'dataMin', max: xMax ?? 'dataMax' }],
        series: [
          { name: 'mid',  data: State.showTicks ? midData : [] , sampling: State.noThinning ? undefined : 'lttb' },
          { name: 'atr1', data: atrData, yAxisIndex: 1, sampling: State.noThinning ? undefined : 'lttb' },
          { name: 'bigm', data: [], markArea: { itemStyle: { color: 'rgba(234,179,8,0.18)' }, data: bigAreas } },
          { name: 'smal', data: [], markLine: { silent: true, symbol: ['none','none'], data: smalLines } },
          { name: 'pred', data: predDots }
        ]
      }, true);

      // Stats line
      const s = data.segm ?? {};
      const statsLine = ticks.length
        ? `Segment #${s.id ?? segmId} | ticks ${s.start_id ?? ''}..${s.end_id ?? ''} | ${s.dir ?? ''} span=${fmt2(s.span)} | small=${(data.smal||[]).length} big=${(data.bigm||[]).length} preds=${(data.pred||[]).length}`
        : `Segment #${s.id ?? segmId} — no ticks returned`;
      segInfo.textContent = statsLine;

      State.lastSegm = s;

      // After drawing, fit Y to the *visible* X window
      refitYAxes();
    } catch (err) {
      console.error(err);
      segInfo.textContent = `Segment ${segmId}: failed to load`;
    }
  }

  // ---------- Smart Y auto-fit ----------
  function currentXRange() {
    const opt = chart.getOption();
    const xa = opt?.xAxis?.[0];
    const mn = xa && xa.min != null ? +xa.min : null;
    const mx = xa && xa.max != null ? +xa.max : null;
    return [mn, mx];
  }

  function extent(seriesData, xMin, xMax) {
    let lo = Infinity, hi = -Infinity, found = false;
    for (const item of seriesData || []) {
      const [x, y] = item.value || item; // support [x,y] or {value:[x,y]}
      if (xMin != null && x < xMin) continue;
      if (xMax != null && x > xMax) continue;
      if (y < lo) lo = y;
      if (y > hi) hi = y;
      found = true;
    }
    if (!found) return null;
    if (lo === hi) { const p = Math.max(1e-6, Math.abs(hi) * 0.001); lo -= p; hi += p; }
    const pad = (hi - lo) * 0.08;
    return [lo - pad, hi + pad];
  }

  function refitYAxes() {
    const opt = chart.getOption();
    const [xMin, xMax] = currentXRange();

    const series = opt?.series || [];
    const mid = series.find(s => s.name === 'mid' && Array.isArray(s.data))?.data || [];
    const atr = series.find(s => String(s.name).toLowerCase() === 'atr1' && Array.isArray(s.data))?.data || [];

    const leftExt  = State.showTicks ? extent(mid, xMin, xMax) : null;
    const rightExt = atr.length ? extent(atr, xMin, xMax) : null;

    const yAxis = opt.yAxis || [{type:'value',scale:true},{type:'value',scale:true,position:'right'}];
    if (leftExt)  { yAxis[0].min = leftExt[0];  yAxis[0].max = leftExt[1];  yAxis[0].name='Price'; yAxis[0].axisLabel={formatter:v=>(+v).toFixed(2)}; }
    if (rightExt) { yAxis[1] = yAxis[1] || {type:'value',scale:true,position:'right'}; yAxis[1].min = rightExt[0]; yAxis[1].max = rightExt[1]; yAxis[1].name='ATR'; yAxis[1].axisLabel={formatter:v=>(+v).toFixed(4)}; }

    chart.setOption({ yAxis }, false);
  }

  // ---------- Focus helpers ----------
  function focusWindow(startTs, endTs) {
    const s = isFinite(+startTs) ? +startTs : Date.parse(startTs);
    const e = isFinite(+endTs) ? +endTs : Date.parse(endTs);
    if (!isFinite(s) || !isFinite(e)) return;
    State.xMin = s; State.xMax = e;
    chart.setOption({ xAxis: [{ min: s, max: e }] });
    refitYAxes();
  }

  // ---------- UI toggles (non-invasive) ----------
  function mountToggles() {
    // mount into the "Layers:" row if present; otherwise append near chart
    const host = qs('.layers-row') || qs('#layersRow') || qs('.layers') || qs('#review') || document.body;

    if (!qs('#toggleNoThin')) {
      const label = document.createElement('label');
      label.style.marginLeft = '12px';
      label.innerHTML = `<input id="toggleNoThin" type="checkbox"> No thinning`;
      host.appendChild(label);
      label.firstElementChild.addEventListener('change', () => {
        State.noThinning = !!qs('#toggleNoThin').checked;
        // just reload the current segm view with sampling updated
        if (State.lastSegm?.id) loadSegment(State.lastSegm.id);
      });
    }

    if (!qs('#toggleShowTicks')) {
      const label = document.createElement('label');
      label.style.marginLeft = '12px';
      label.innerHTML = `<input id="toggleShowTicks" type="checkbox" checked> Show ticks`;
      host.appendChild(label);
      label.firstElementChild.addEventListener('change', () => {
        State.showTicks = !!qs('#toggleShowTicks').checked;
        if (State.lastSegm?.id) loadSegment(State.lastSegm.id);
      });
    }
  }

  function wireChunkInput() {
    if (!chunkInp) return;
    const apply = () => {
      const v = +chunkInp.value;
      if (v > 0 && Number.isFinite(v)) {
        State.chunk = v;
        if (!State.noThinning && State.lastSegm?.id) loadSegment(State.lastSegm.id);
      }
    };
    chunkInp.addEventListener('change', apply);
    // initialize
    apply();
  }

  // ---------- Boot ----------
  async function boot() {
    setupChart();
    mountToggles();
    wireChunkInput();
    await loadSegmList();
    // resize hooks
    window.addEventListener('resize', () => {
      chart.resize();
      refitYAxes();
    });
  }

  // Start
  if (document.readyState === 'complete' || document.readyState === 'interactive') {
    setTimeout(boot, 0);
  } else {
    document.addEventListener('DOMContentLoaded', boot);
  }
})();