// PATH: frontend/review-core.js
(() => {
  const $ = (id) => document.getElementById(id);

  // ---------------- ECharts ----------------
  const chart = echarts.init($('chart'));
  chart.setOption({
    animation: false,
    backgroundColor: '#0f172a',
    grid: { left: 50, right: 20, top: 10, bottom: 30 },
    // --- tooltip: now reads p.data.meta.id so id always shows ---
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'cross' },
      formatter: (params) => {
        if (!params || !params.length) return '';
        const base = params.find(p => (p.seriesId || '').startsWith('ticks:')) || params[0];
        const point = base && base.data ? base.data : null;
        const ts = point ? new Date(point.value[0]) : (params[0].axisValueLabel);
        const mid = point ? point.value[1] : (params[0].value && params[0].value[1]);
        const id  = point && point.meta ? point.meta.id : '—';
        const dt = new Date(ts);
        const date = dt.toLocaleDateString(); const time = dt.toLocaleTimeString();
        let lines = [`<b>${date} ${time}</b>`, `id: ${id}`, `mid: ${mid}`];
        params.forEach(p => {
          if ((p.seriesId || '').startsWith('ticks:')) return;
          if (typeof p.value === 'number') lines.push(`${p.seriesName}: ${p.value}`);
          else if (Array.isArray(p.value) && typeof p.value[1] === 'number') lines.push(`${p.seriesName}: ${p.value[1]}`);
        });
        return lines.join('<br/>');
      }
    },
    // IMPORTANT: give dataZoom entries IDs so we can target them
    dataZoom: [
      { id: 'dzIn',  type: 'inside', zoomOnMouseWheel: true },
      { id: 'dzSl',  type: 'slider', height: 18 }
    ],
    xAxis: { type: 'time', axisLabel: { color: '#cbd5e1' } },
    yAxis: {
      type: 'value', scale: true,
      axisLabel: { color:'#cbd5e1', formatter:(v)=> (typeof v==='number'? v.toFixed(2): v) },
      splitLine: { lineStyle:{ color:'#233047' } }
    },
    series: []
  });

  // ---------------- State ----------------
  const state = {
    chunk: 2000,
    segms: [],                 // {id,start_id,end_id,start_ts,end_ts,dir, loadedMaxId}
    selectedSegmIds: new Set(),
    selectedTables: new Set(), // overlay tables
    seriesMap: new Map(),      // key -> series
    streaming: new Map()       // segmId -> boolean
  };

  const keyS = (type, segmId, name='') => `${type}:${segmId}:${name}`;

  // ---------------- API helpers ----------------
  async function api(url) { const r = await fetch(url); if (!r.ok) throw new Error(await r.text()); return r.json(); }
  const listTables = () => api('/api/sql/tables');
  const getSegms = (limit=600) => api(`/api/segm/recent?limit=${limit}`);
  const getSegmTicksChunk = (id, fromId, limit) => api(`/api/segm/ticks?id=${id}&from=${fromId}&limit=${limit}`);
  const getSegmLayers = (id, tablesCSV) => api(`/api/segm/layers?id=${id}&tables=${encodeURIComponent(tablesCSV)}`);

  // ---------------- Labels bar ----------------
  async function populateLabelsBar() {
    const tables = await listTables();
    const blacklist = new Set(['ticks']);
    const host = $('labelsGroup'); host.innerHTML = '';
    tables.filter(t => !blacklist.has(t)).forEach(t => {
      const box = document.createElement('label');
      const id = `lbl_${t}`;
      box.innerHTML = `<input id="${id}" type="checkbox" data-t="${t}" /> ${t}`;
      box.style.cursor = 'pointer';
      box.querySelector('input').addEventListener('change', async (e) => {
        const name = e.target.dataset.t;
        if (e.target.checked) state.selectedTables.add(name);
        else state.selectedTables.delete(name);
        for (const segmId of state.selectedSegmIds) await loadSegmLayers(segmId);
      });
      host.appendChild(box);
    });
  }

  // ---------------- Segm list ----------------
  function durationHM(aTs, bTs) {
    const ms = Math.max(0, (new Date(bTs) - new Date(aTs)));
    const m = Math.floor(ms / 60000);
    const h = Math.floor(m / 60);
    const mm = (m % 60).toString().padStart(2,'0');
    return `${h}:${mm}`;
  }
  async function populateSegms() {
    const rows = await getSegms(800);
    state.segms = rows.map(r => ({ ...r, loadedMaxId: null }));
    const host = $('segmList'); host.innerHTML = '';

    const hdr = document.createElement('div');
    hdr.className = 'segm-row small';
    hdr.style.position = 'sticky'; hdr.style.top = '0'; hdr.style.background = '#0b1220'; hdr.style.zIndex = '2';
    hdr.innerHTML = `<div></div><div>Segm</div><div>Start TS</div><div>Start ID</div><div>Dur (h:m)</div>`;
    host.appendChild(hdr);

    rows.forEach(r => {
      const row = document.createElement('div');
      row.className = 'segm-row';
      const dur = durationHM(r.start_ts, r.end_ts);
      row.innerHTML = `
        <input type="checkbox" data-id="${r.id}"/>
        <div class="small">${r.id}</div>
        <div class="small">${new Date(r.start_ts).toLocaleString()}</div>
        <div class="small">${r.start_id}</div>
        <div class="small">${dur}</div>
      `;
      row.querySelector('input').addEventListener('change', (e) => {
        const id = r.id;
        if (e.target.checked) { state.selectedSegmIds.add(id); initialLoadSegm(id).catch(console.error); }
        else { state.selectedSegmIds.delete(id); removeSegmSeries(id); }
      });
      host.appendChild(row);
    });
  }

  // ---------------- Zoom preservation (absolute values) ----------------
  function getTicksExtent() {
    // Use the first ticks series as the extent reference
    const ticksSeries = [...state.seriesMap.values()].find(s => (s.id||'').startsWith('ticks:'));
    if (!ticksSeries || !ticksSeries.data || !ticksSeries.data.length) return null;
    const first = ticksSeries.data[0][0];
    const last  = ticksSeries.data[ticksSeries.data.length-1][0];
    return [new Date(first).getTime(), new Date(last).getTime()];
  }

  function getAllTicksExtent() {
    let minT = Infinity, maxT = -Infinity;
    for (const [k, s] of state.seriesMap) {
      if (!k.startsWith('ticks:') || !s || !s.data || !s.data.length) continue;
      const firstVal = s.data[0].value?.[0];
      const lastVal  = s.data[s.data.length - 1].value?.[0];
      const f = +new Date(firstVal);
      const l = +new Date(lastVal);
      if (isFinite(f) && f < minT) minT = f;
      if (isFinite(l) && l > maxT) maxT = l;
    }
    return (isFinite(minT) && isFinite(maxT)) ? [minT, maxT] : null;
  }


  function getZoomWindowValues() {
    const opt = chart.getOption();
    const dz = (opt.dataZoom && opt.dataZoom.length) ? opt.dataZoom[0] : null;
    const extent = getAllTicksExtent();
    if (!dz || !extent) return null;

    if (dz.startValue != null && dz.endValue != null) {
      const s = +new Date(dz.startValue), e = +new Date(dz.endValue);
      return (isFinite(s) && isFinite(e)) ? [s, e] : null;
    }
    // convert percent → absolute using current extent
    const [minT, maxT] = extent;
    const span = Math.max(1, maxT - minT);
    const pStart = (dz.start ?? 0) / 100;
    const pEnd   = (dz.end   ?? 100) / 100;
    const absStart = Math.round(minT + pStart * span);
    const absEnd   = Math.round(minT + pEnd   * span);
    return [absStart, absEnd];
  }


  function applyZoomWindowValues(absStart, absEnd) {
    if (!isFinite(absStart) || !isFinite(absEnd)) return;
    const sISO = new Date(absStart).toISOString();
    const eISO = new Date(absEnd).toISOString();
    chart.setOption({
      dataZoom: [
        { id: 'dzIn', startValue: sISO, endValue: eISO },
        { id: 'dzSl', startValue: sISO, endValue: eISO }
      ]
    });
  }


  // Helper to convert a tick record to a point for ECharts
  function toPoint(r) {
    return { value: [r.ts, r.mid], meta: { id: r.id } };
  }

  // ---------------- Series ops (now preserving window) ----------------
  function setSeriesPreserveWindow() {
    const zw = getZoomWindowValues();           // absolute [start,end] in ms
    chart.setOption({ series: [...state.seriesMap.values()] }, { replaceMerge: ['series'] });
    if (zw) applyZoomWindowValues(zw[0], zw[1]); // keep exactly the same window
  }


  function addOrUpdateTickSeries(segmId, ticks) {
    const k = keyS('ticks', segmId);
    const data = ticks.map(toPoint);
    const s = { id:k, name:`mid #${segmId}`, type:'line', showSymbol:false, sampling:'lttb', large:true, data };
    state.seriesMap.set(k, s);
    setSeriesPreserveWindow();
  }

  function appendTickSeries(segmId, ticks) {
    const k = keyS('ticks', segmId);
    const s = state.seriesMap.get(k);
    if (!s) return addOrUpdateTickSeries(segmId, ticks);
    s.data = s.data.concat(ticks.map(toPoint));
    setSeriesPreserveWindow();
  }

  function prependTickSeries(segmId, ticks) {
    const k = keyS('ticks', segmId);
    const s = state.seriesMap.get(k);
    if (!s) return addOrUpdateTickSeries(segmId, ticks);
    s.data = ticks.map(toPoint).concat(s.data);
    setSeriesPreserveWindow();
  }


  function getLastDrawnId(segmId) {
    const s = state.seriesMap.get(keyS('ticks', segmId));
    if (!s || !s.data || !s.data.length) return null;
    const last = s.data[s.data.length - 1];
    return last && last.meta ? last.meta.id : null;
  }


  function removeSegmSeries(segmId) {
    for (const [k] of state.seriesMap) if (k.includes(`:${segmId}:`)) state.seriesMap.delete(k);
    setSeriesPreserveWindow();
  }

  function addOrUpdateLayerSeries(segmId, table, rows) {
    const key = keyS('layer', segmId, table);
    let series;
    if (table === 'atr1') {
      const segs = rows.map(r => ({
        coords: [
          [r.start_ts, r.start_mid ?? r.a_mid ?? null],
          [r.end_ts,   r.end_mid   ?? r.b_mid ?? null]
        ]
      }));
      series = { id:key, name:`${table} #${segmId}`, type:'lines', data:segs, lineStyle:{ width:1.5 } };
    } else if (table === 'level') {
      series = { id:key, name:`${table} #${segmId}`, type:'scatter', symbolSize:6,
                 data: rows.map(r => [r.ts, r.price]) };
    } else if (table === 'bigm' || table === 'smal' || table === 'pred') {
      const segs = rows.map(r => ({
        coords: [[(r.a_ts||r.start_ts), (r.a_mid||r.a_price||r.price||null)],
                 [(r.b_ts||r.end_ts),   (r.b_mid||r.b_price||r.price||null)]]
      }));
      series = { id:key, name:`${table} #${segmId}`, type:'lines', data:segs, lineStyle:{ width:2 } };
    } else {
      const guess = rows.map(r => [r.ts || r.a_ts || r.start_ts || r.time || r.created_at,
                                   r.mid ?? r.value ?? r.price ?? r.span ?? null])
                        .filter(v => v[0] && v[1] != null);
      series = { id:key, name:`${table} #${segmId}`, type:'line', showSymbol:false, data:guess };
    }
    state.seriesMap.set(key, series);
    setSeriesPreserveWindow();
  }

  // ---------------- Load logic ----------------
  async function initialLoadSegm(segmId) {
    const seg = state.segms.find(s => s.id === segmId); if (!seg) return;
    const first = await getSegmTicksChunk(segmId, seg.start_id, state.chunk);
    if (first.length) {
      seg.loadedMaxId = first[first.length-1].id;
      addOrUpdateTickSeries(segmId, first);
      await loadSegmLayers(segmId);
      streamRight(segmId).catch(console.error); // background, preserves window each append
    }
  }

  async function loadMoreLeft() {
    for (const segmId of state.selectedSegmIds) {
      const seg = state.segms.find(s => s.id === segmId); if (!seg) continue;
      const k = keyS('ticks', segmId); const s = state.seriesMap.get(k);
      if (!s || !s.data || !s.data.length) continue;
      const earliestId = s.data[0].__id ?? seg.start_id;
      const from = Math.max(seg.start_id, earliestId - state.chunk);
      if (from >= earliestId) continue;
      const chunk = await getSegmTicksChunk(segmId, from, state.chunk);
      if (chunk.length) prependTickSeries(segmId, chunk); // zoom preserved inside
    }
  }

  async function streamRight(segmId) {
  if (state.streaming.get(segmId)) return;
  state.streaming.set(segmId, true);
  try {
    const seg = state.segms.find(s => s.id === segmId); if (!seg) return;
    while (state.selectedSegmIds.has(segmId)) {
      const lastId = getLastDrawnId(segmId) ?? seg.start_id;
      const from = lastId + 1;
      if (from > seg.end_id) break;

      const chunk = await getSegmTicksChunk(segmId, from, state.chunk);
      if (!chunk.length) break;

      appendTickSeries(segmId, chunk); // zoom preserved inside
      await new Promise(r => setTimeout(r, 40)); // keep UI responsive
    }
  } finally {
    state.streaming.delete(segmId);
  }
}


  async function loadSegmLayers(segmId) {
    if (!state.selectedTables.size) return;
    const tablesCSV = [...state.selectedTables].join(',');
    const payload = await getSegmLayers(segmId, tablesCSV);
    for (const [tname, rows] of Object.entries(payload.layers || {})) {
      rows.forEach(r => { if (r.start_ts) r.start_ts = new Date(r.start_ts).toISOString();
                          if (r.end_ts)   r.end_ts   = new Date(r.end_ts).toISOString();
                          if (r.a_ts)     r.a_ts     = new Date(r.a_ts).toISOString();
                          if (r.b_ts)     r.b_ts     = new Date(r.b_ts).toISOString();
                          if (r.ts)       r.ts       = new Date(r.ts).toISOString(); });
      addOrUpdateLayerSeries(segmId, tname, rows);
    }
  }

  // ---------------- Wire up ----------------
  $('btnReload').addEventListener('click', async () => {
    state.chunk = Math.max(500, Math.min(20000, Number($('chunk').value || 2000)));
    state.seriesMap.clear();
    state.selectedSegmIds.clear();
    chart.setOption({ series: [] });
    await populateLabelsBar();
    await populateSegms();
  });
  $('btnLoadMore').addEventListener('click', () => loadMoreLeft().catch(console.error));

  $('chunk').value = String(state.chunk);
  $('btnReload').click();
  window.addEventListener('resize', () => chart.resize());
})();
