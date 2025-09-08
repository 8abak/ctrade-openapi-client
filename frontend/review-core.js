(() => {
  const $ = (id) => document.getElementById(id);

  // --------- Chart ---------
  const chart = echarts.init($('chart'));
  chart.setOption({
    animation: false,
    backgroundColor: '#0f172a',
    grid: { left: 50, right: 20, top: 10, bottom: 30 },
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'cross' },
      formatter: (params) => {
        if (!params || !params.length) return '';
        const base = params.find(p => (p.seriesId || '').startsWith('ticks:')) || params[0];
        const point = base?.data;
        const ts   = point ? new Date(point.value[0]) : (params[0].axisValueLabel);
        const mid  = point ? point.value[1] : (params[0].value && params[0].value[1]);
        const id   = point?.meta?.id ?? '—';
        const date = new Date(ts).toLocaleDateString();
        const time = new Date(ts).toLocaleTimeString();
        const lines = [`<b>${date} ${time}</b>`, `id: ${id}`, `mid: ${mid}`];
        params.forEach(p => {
          if ((p.seriesId || '').startsWith('ticks:')) return;
          if (typeof p.value === 'number') lines.push(`${p.seriesName}: ${p.value}`);
          else if (Array.isArray(p.value) && typeof p.value[1] === 'number') lines.push(`${p.seriesName}: ${p.value[1]}`);
        });
        return lines.join('<br/>');
      }
    },
    // IMPORTANT: keep all data; don't filter out-of-window points
    dataZoom: [
      { id:'dzIn', type:'inside', zoomOnMouseWheel:true, filterMode:'none', rangeMode:['value','value'] },
      { id:'dzSl', type:'slider', height:18,            filterMode:'none', rangeMode:['value','value'] }
    ],
    xAxis: { type:'time', axisLabel:{ color:'#cbd5e1' } },
    yAxis: {
      type:'value', scale:true,
      axisLabel:{ color:'#cbd5e1', formatter:(v)=> (typeof v==='number'? v.toFixed(2): v) },
      splitLine:{ lineStyle:{ color:'#233047' } }
    },
    series: []
  });

  // --------- State ---------
  const state = {
    chunk: 2000,
    segms: [],                 // {id,start_id,end_id,start_ts,end_ts,dir}
    selectedSegmIds: new Set(),
    selectedTables: new Set(),
    seriesMap: new Map(),      // key -> series object
    streaming: new Map()       // segmId -> boolean
  };
  const keyS = (type, segmId, name='') => `${type}:${segmId}:${name}`;

  // --------- API ---------
  async function api(url) {
    const r = await fetch(url);
    if (!r.ok) throw new Error(await r.text());
    return r.json();
  }
  // robust segm fetcher (tries both shapes)
  async function fetchSegms(limit=600) {
    try {
      // preferred
      return await api(`/api/segm/recent?limit=${limit}`);
    } catch {
      // fallback to /api/segms (if your backend exposes it)
      const rows = await api(`/api/segms?limit=${limit}`);
      // Normalize shape if needed
      return rows.map(r => ({
        id: r.id,
        start_id: r.start_id, end_id: r.end_id,
        start_ts: r.start_ts || r.a_ts || r.ts,
        end_ts:   r.end_ts   || r.b_ts || r.ts,
        dir: r.dir
      }));
    }
  }
  const listTables = () => api('/api/sql/tables'); // your helper endpoint
  const getSegmTicksChunk = (id, fromId, limit) => api(`/api/segm/ticks?id=${id}&from=${fromId}&limit=${limit}`);
  const getSegmLayers = (id, tablesCSV) => api(`/api/segm/layers?id=${id}&tables=${encodeURIComponent(tablesCSV)}`);

  // --------- Labels bar ---------
  async function populateLabelsBar() {
    const host = $('labelsGroup'); host.innerHTML = '';
    let tables = [];
    try { tables = await listTables(); } catch { tables = []; }
    const blacklist = new Set(['ticks']);
    tables.filter(t => !blacklist.has(t)).forEach(t => {
      const box = document.createElement('label');
      const id = `lbl_${t}`;
      box.innerHTML = `<input id="${id}" type="checkbox" data-t="${t}" /> ${t}`;
      box.querySelector('input').addEventListener('change', async (e) => {
        const name = e.target.dataset.t;
        if (e.target.checked) state.selectedTables.add(name);
        else state.selectedTables.delete(name);
        for (const segmId of state.selectedSegmIds) await loadSegmLayers(segmId);
      });
      host.appendChild(box);
    });
  }

  // --------- Segments list ---------
  function durationHM(aTs, bTs) {
    const ms = Math.max(0, (new Date(bTs) - new Date(aTs)));
    const m = Math.floor(ms / 60000), h = Math.floor(m / 60);
    return `${h}:${String(m % 60).padStart(2,'0')}`;
  }
  async function populateSegms() {
    const rows = await fetchSegms(800);
    state.segms = rows;
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

  // --------- Zoom preservation (absolute) ---------
  function getAllTicksExtent() {
    let minT = Infinity, maxT = -Infinity;
    for (const [k, s] of state.seriesMap) {
      if (!k.startsWith('ticks:') || !s?.data?.length) continue;
      const firstVal = s.data[0].value?.[0];
      const lastVal  = s.data[s.data.length-1].value?.[0];
      const f = +new Date(firstVal), l = +new Date(lastVal);
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
    const [minT, maxT] = extent, span = Math.max(1, maxT - minT);
    const pStart = (dz.start ?? 0)/100, pEnd = (dz.end ?? 100)/100;
    return [Math.round(minT + pStart*span), Math.round(minT + pEnd*span)];
  }
  function applyZoomWindowValues(absStart, absEnd) {
    if (!isFinite(absStart) || !isFinite(absEnd)) return;
    const sISO = new Date(absStart).toISOString(), eISO = new Date(absEnd).toISOString();
    chart.setOption({
      dataZoom: [
        { id:'dzIn', startValue:sISO, endValue:eISO },
        { id:'dzSl', startValue:sISO, endValue:eISO }
      ]
    });
  }
  function setSeriesPreserveWindow() {
    const zw = getZoomWindowValues();
    chart.setOption({ series: [...state.seriesMap.values()] }, { replaceMerge:['series'] });
    if (zw) applyZoomWindowValues(zw[0], zw[1]);
  }

  // --------- Series helpers ---------
  const toPoint = (r) => ({ value:[r.ts, r.mid], meta:{ id:r.id } });

  function addOrUpdateTickSeries(segmId, ticks) {
    const k = keyS('ticks', segmId);
    const s = { id:k, name:`mid #${segmId}`, type:'line', showSymbol:false, sampling:'lttb', large:true,
                data: ticks.map(toPoint) };
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
  function removeSegmSeries(segmId) {
    for (const [k] of state.seriesMap) if (k.includes(`:${segmId}:`)) state.seriesMap.delete(k);
    setSeriesPreserveWindow();
  }
  function getLastDrawnId(segmId) {
    const s = state.seriesMap.get(keyS('ticks', segmId));
    if (!s?.data?.length) return null;
    return s.data[s.data.length-1]?.meta?.id ?? null;
  }

  // --------- Layers ---------
  function addOrUpdateLayerSeries(segmId, table, rows) {
    const key = keyS('layer', segmId, table);
    let series;
    if (table === 'atr1') {
      const segs = rows.map(r => ({ coords: [[r.start_ts, r.start_mid ?? r.a_mid ?? null],
                                              [r.end_ts,   r.end_mid   ?? r.b_mid ?? null]] }));
      series = { id:key, name:`${table} #${segmId}`, type:'lines', data:segs, lineStyle:{ width:1.5 } };
    } else if (table === 'level') {
      series = { id:key, name:`${table} #${segmId}`, type:'scatter', symbolSize:6,
                 data: rows.map(r => [r.ts, r.price]) };
    } else if (['bigm','smal','pred','segm','stat','outcome','atr1_work'].includes(table)) {
      const segs = rows.map(r => ({ coords: [[(r.a_ts||r.start_ts), (r.a_mid||r.a_price||r.price||null)],
                                             [(r.b_ts||r.end_ts),   (r.b_mid||r.b_price||r.price||null)]] }));
      series = { id:key, name:`${table} #${segmId}`, type:'lines', data:segs, lineStyle:{ width:2 } };
    } else {
      const guess = rows.map(r => [r.ts || r.a_ts || r.start_ts || r.time || r.created_at,
                                   r.mid ?? r.value ?? r.price ?? r.span ?? null]).filter(v => v[0] && v[1]!=null);
      series = { id:key, name:`${table} #${segmId}`, type:'line', showSymbol:false, data:guess };
    }
    state.seriesMap.set(key, series);
    setSeriesPreserveWindow();
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

  // --------- Load logic ---------
  async function initialLoadSegm(segmId) {
    const seg = state.segms.find(s => s.id === segmId); if (!seg) return;
    const first = await getSegmTicksChunk(segmId, seg.start_id, state.chunk);
    if (!first.length) return;
    addOrUpdateTickSeries(segmId, first);
    await loadSegmLayers(segmId);
    streamRight(segmId).catch(console.error); // background
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
        appendTickSeries(segmId, chunk); // zoom preserved
        await new Promise(r => setTimeout(r, 40));
      }
    } finally {
      state.streaming.delete(segmId);
    }
  }

  async function loadMoreLeft() {
    for (const segmId of state.selectedSegmIds) {
      const seg = state.segms.find(s => s.id === segmId); if (!seg) continue;
      const s = state.seriesMap.get(keyS('ticks', segmId));
      if (!s?.data?.length) continue;
      const earliestId = s.data[0]?.meta?.id ?? seg.start_id;
      const from = Math.max(seg.start_id, earliestId - state.chunk);
      if (from >= earliestId) continue;
      const chunk = await getSegmTicksChunk(segmId, from, state.chunk);
      if (chunk.length) prependTickSeries(segmId, chunk);
    }
  }

  // --------- Wire up ---------
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
  window.addEventListener('resize', () => chart.resize(), { passive: true });
})();