(() => {
  const $ = (id) => document.getElementById(id);

  // --------- config: caps (no downsampling) ---------
  const MAX_TICKS_DESKTOP = 50_000;
  const MAX_TICKS_MOBILE  = 15_000;
  const isMobile = /Android|iPhone|iPad|iPod|Mobile/i.test(navigator.userAgent);
  const MAX_TICKS = isMobile ? MAX_TICKS_MOBILE : MAX_TICKS_DESKTOP;

  // chunk when fetching
  let CHUNK = 2000;

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
    // keep all data; do not filter out-of-window points
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
    segms: [],                       // {id,start_id,end_id,start_ts,end_ts,dir}
    selectedSegmIds: new Set(),
    selectedTables: new Set(),       // includes 'ticks' now
    seriesMap: new Map(),            // key -> series object
    loadedRanges: new Map()          // segmId -> {minId, maxId}
  };
  const keyS = (type, segmId, name='') => `${type}:${segmId}:${name}`;

  // --------- API ---------
  async function api(url) {
    const r = await fetch(url);
    if (!r.ok) throw new Error(await r.text());
    return r.json();
  }
  async function fetchSegms(limit=600) {
    try {
      return await api(`/api/segm/recent?limit=${limit}`);
    } catch {
      const rows = await api(`/api/segms?limit=${limit}`);
      return rows.map(r => ({
        id: r.id, start_id: r.start_id, end_id: r.end_id,
        start_ts: r.start_ts || r.a_ts || r.ts,
        end_ts:   r.end_ts   || r.b_ts || r.ts,
        dir: r.dir
      }));
    }
  }
  const listTables = () => api('/api/sql/tables');
  const getSegmTicksChunk = (id, fromId, limit) => api(`/api/segm/ticks?id=${id}&from=${fromId}&limit=${limit}`);
  const getSegmLayers = (id, tablesCSV) => api(`/api/segm/layers?id=${id}&tables=${encodeURIComponent(tablesCSV)}`);

  // --------- Labels bar (include ticks as a layer) ---------
  async function populateLabelsBar() {
    const host = $('labelsGroup'); host.innerHTML = '';
    let tables = [];
    try { tables = await listTables(); } catch { tables = []; }

    // Ensure 'ticks' appears as a layer (first, checked by default)
    const names = ['ticks', ...tables.filter(t => t !== 'ticks')];
    names.forEach((t, i) => {
      const box = document.createElement('label');
      const id = `lbl_${t}`;
      const checked = (t === 'ticks'); // ticks on by default
      box.innerHTML = `<input id="${id}" type="checkbox" data-t="${t}" ${checked?'checked':''}/> ${t}`;
      box.querySelector('input').addEventListener('change', async (e) => {
        const name = e.target.dataset.t;
        if (e.target.checked) state.selectedTables.add(name);
        else {
          state.selectedTables.delete(name);
          // remove rendered series for that layer
          for (const segmId of state.selectedSegmIds) {
            const k = (name === 'ticks') ? keyS('ticks', segmId) : keyS('layer', segmId, name);
            state.seriesMap.delete(k);
          }
          setSeriesPreserveWindow();
        }
        // load layer data if needed
        if (e.target.checked) {
          for (const segmId of state.selectedSegmIds) {
            if (name === 'ticks') await ensureInitialTicks(segmId);
            else await loadSegmLayers(segmId);
          }
        }
      });
      host.appendChild(box);
      if (checked) state.selectedTables.add(t);
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
      row.querySelector('input').addEventListener('change', async (e) => {
        const id = r.id;
        if (e.target.checked) {
          state.selectedSegmIds.add(id);
          if (state.selectedTables.has('ticks')) await ensureInitialTicks(id);
          if (state.selectedTables.size) await loadSegmLayers(id);
        } else {
          state.selectedSegmIds.delete(id);
          // remove all series for that segm
          for (const [k] of state.seriesMap) if (k.includes(`:${id}:`)) state.seriesMap.delete(k);
          state.loadedRanges.delete(id);
          setSeriesPreserveWindow();
        }
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
    chart.setOption({ dataZoom: [
      { id:'dzIn', startValue:sISO, endValue:eISO },
      { id:'dzSl', startValue:sISO, endValue:eISO }
    ]});
  }
  function setSeriesPreserveWindow() {
    const zw = getZoomWindowValues();
    chart.setOption({ series: [...state.seriesMap.values()] }, { replaceMerge:['series'] });
    if (zw) applyZoomWindowValues(zw[0], zw[1]);
  }

  // --------- Series helpers (no downsampling) ---------
  const toPoint = (r) => ({ value:[r.ts, r.mid], meta:{ id:r.id } });

  function addOrUpdateTickSeries(segmId, ticks) {
    const k = keyS('ticks', segmId);
    const s = {
      id:k, name:`mid #${segmId}`, type:'line',
      showSymbol:false, // symbols hidden to keep it light
      // turn OFF any built-in downsampling:
      sampling: undefined, progressive: 0, large: true,
      lineStyle:{ width:1 },
      data: ticks.map(toPoint)
    };
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
    } else {
      const segs = rows.map(r => ({
        coords: [[(r.a_ts||r.start_ts||r.ts), (r.a_mid||r.a_price||r.price||r.mid||null)],
                 [(r.b_ts||r.end_ts||r.ts),   (r.b_mid||r.b_price||r.price||r.mid||null)]]
      }));
      series = { id:key, name:`${table} #${segmId}`, type:'lines', data:segs, lineStyle:{ width:2 } };
    }
    state.seriesMap.set(key, series);
    setSeriesPreserveWindow();
  }

  async function loadSegmLayers(segmId) {
    const tables = [...state.selectedTables].filter(t => t !== 'ticks');
    if (!tables.length) return;
    const tablesCSV = tables.join(',');
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

  // --------- Loading ticks with caps (no auto-stream) ---------
  function getLoaded(segmId){ return state.loadedRanges.get(segmId) || null; }
  function setLoaded(segmId, minId, maxId){
    const cur = state.loadedRanges.get(segmId);
    if (!cur) state.loadedRanges.set(segmId, {minId, maxId});
    else state.loadedRanges.set(segmId, {minId: Math.min(cur.minId,minId), maxId: Math.max(cur.maxId,maxId)});
  }

  async function ensureInitialTicks(segmId) {
    // Load from segment start up to MAX_TICKS (in chunks)
    const seg = state.segms.find(s => s.id === segmId); if (!seg) return;
    const already = getLoaded(segmId);
    if (already) return; // already have some data

    let want = Math.min(MAX_TICKS, seg.end_id - seg.start_id + 1);
    let from = seg.start_id;
    let firstBatch = true;
    while (want > 0) {
      const take = Math.min(CHUNK, want);
      const batch = await getSegmTicksChunk(segmId, from, take);
      if (!batch.length) break;
      if (firstBatch) { addOrUpdateTickSeries(segmId, batch); firstBatch = false; }
      else            { appendTickSeries(segmId, batch); }
      from = batch[batch.length-1].id + 1;
      want -= batch.length;
    }
    // track loaded range
    const s = state.seriesMap.get(keyS('ticks', segmId));
    if (s?.data?.length){
      const minId = s.data[0].meta.id;
      const maxId = s.data[s.data.length-1].meta.id;
      setLoaded(segmId, minId, maxId);
    }
  }

  async function loadMoreRight() {
    for (const segmId of state.selectedSegmIds) {
      const seg = state.segms.find(s => s.id === segmId); if (!seg) continue;
      const lr = getLoaded(segmId); if (!lr) continue;
      const remainingCap = MAX_TICKS - (lr.maxId - lr.minId + 1);
      if (remainingCap <= 0) continue;
      const from = lr.maxId + 1;
      if (from > seg.end_id) continue;

      const take = Math.min(CHUNK, remainingCap);
      const batch = await getSegmTicksChunk(segmId, from, take);
      if (!batch.length) continue;
      appendTickSeries(segmId, batch);
      setLoaded(segmId, lr.minId, batch[batch.length-1].id);
    }
  }

  async function loadMoreLeft() {
    for (const segmId of state.selectedSegmIds) {
      const seg = state.segms.find(s => s.id === segmId); if (!seg) continue;
      const lr = getLoaded(segmId); if (!lr) continue;
      const remainingCap = MAX_TICKS - (lr.maxId - lr.minId + 1);
      if (remainingCap <= 0) continue;

      const earliestWant = Math.max(seg.start_id, lr.minId - CHUNK);
      if (earliestWant >= lr.minId) continue;
      const batch = await getSegmTicksChunk(segmId, earliestWant, Math.min(CHUNK, remainingCap));
      if (!batch.length) continue;
      prependTickSeries(segmId, batch);
      setLoaded(segmId, batch[0].id, lr.maxId);
    }
  }

  // --------- Wire up ---------
  $('btnReload').addEventListener('click', async () => {
    CHUNK = Math.max(1000, Math.min(20000, Number($('chunk').value || 2000)));
    state.seriesMap.clear();
    state.selectedSegmIds.clear();
    state.loadedRanges.clear();
    chart.setOption({ series: [] });
    await populateLabelsBar();
    await populateSegms();
  });
  $('btnMoreLeft').addEventListener('click', () => loadMoreLeft().catch(console.error));
  $('btnMoreRight').addEventListener('click', () => loadMoreRight().catch(console.error));
  $('btnTogglePanel').addEventListener('click', () => {
    document.body.classList.toggle('collapsed');
    setTimeout(() => chart.resize(), 50);
  });

  $('chunk').value = String(CHUNK);
  $('btnReload').click();
  window.addEventListener('resize', () => chart.resize(), { passive: true });
})();