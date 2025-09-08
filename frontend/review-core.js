// PATH: frontend/review-core.js
(() => {
  const $ = (id) => document.getElementById(id);

  // ---------------- ECharts ----------------
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
        // prefer the base tick series item for metadata
        const base = params.find(p => (p.seriesId || '').startsWith('ticks:')) || params[0];
        const d = base && base.data ? base.data : null;
        const ts = d ? new Date(d[0]) : (params[0].axisValueLabel);
        const mid = d ? d[1] : (params[0].value && params[0].value[1]);
        const id = d && d.__id ? d.__id : '—';
        const dt = new Date(ts);
        const date = dt.toLocaleDateString(); const time = dt.toLocaleTimeString();
        let lines = [`<b>${date} ${time}</b>`, `id: ${id}`, `mid: ${mid}`];
        // include overlay lines values if they have numeric
        params.forEach(p => {
          if ((p.seriesId || '').startsWith('ticks:')) return;
          if (typeof p.value === 'number') lines.push(`${p.seriesName}: ${p.value}`);
          else if (Array.isArray(p.value) && typeof p.value[1] === 'number') lines.push(`${p.seriesName}: ${p.value[1]}`);
        });
        return lines.join('<br/>');
      }
    },
    dataZoom: [
      { type: 'inside', zoomOnMouseWheel: true },
      { type: 'slider', height: 18 }
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
    streaming: new Map()       // segmId -> boolean (background right-stream in progress)
  };

  const keyS = (type, segmId, name='') => `${type}:${segmId}:${name}`;

  // ---------------- API helpers ----------------
  async function api(url) { const r = await fetch(url); if (!r.ok) throw new Error(await r.text()); return r.json(); }
  const listTables = () => api('/api/sql/tables');     // alias /api/tables is fine too
  const getSegms = (limit=600) => api(`/api/segm/recent?limit=${limit}`);
  const getSegmTicksChunk = (id, fromId, limit) => api(`/api/segm/ticks?id=${id}&from=${fromId}&limit=${limit}`);
  const getSegmLayers = (id, tablesCSV) => api(`/api/segm/layers?id=${id}&tables=${encodeURIComponent(tablesCSV)}`);

  // ---------------- UI: labels bar ----------------
  async function populateLabelsBar() {
    const tables = await listTables();
    const blacklist = new Set(['ticks']); // we draw ticks anyway
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
        // refresh overlays for currently selected segms
        for (const segmId of state.selectedSegmIds) await loadSegmLayers(segmId);
      });
      host.appendChild(box);
    });
  }

  // ---------------- UI: segm list ----------------
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

    // header row
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

  // ---------------- Series ops ----------------
  function setSeries() {
    const currentZoom = chart.getOption().dataZoom || null; // preserve zoom
    chart.setOption({ series: [...state.seriesMap.values()] }, { replaceMerge: ['series'] });
    if (currentZoom) chart.setOption({ dataZoom: currentZoom.map(z => ({...z})) });
  }
  function addOrUpdateTickSeries(segmId, ticks) {
    const k = keyS('ticks', segmId);
    const data = ticks.map(r => {
      const d = [r.ts, r.mid];
      d.__id = r.id;                 // stash id for tooltip
      return d;
    });
    const s = {
      id: k, name: `mid #${segmId}`, type: 'line',
      showSymbol: false, sampling: 'lttb', large: true, data
    };
    state.seriesMap.set(k, s);
    setSeries();
  }
  function appendTickSeries(segmId, ticks) {
    const k = keyS('ticks', segmId);
    const s = state.seriesMap.get(k);
    if (!s) return addOrUpdateTickSeries(segmId, ticks);
    const more = ticks.map(r => { const d=[r.ts, r.mid]; d.__id=r.id; return d; });
    s.data = s.data.concat(more);
    setSeries();
  }
  function prependTickSeries(segmId, ticks) {
    const k = keyS('ticks', segmId);
    const s = state.seriesMap.get(k);
    if (!s) return addOrUpdateTickSeries(segmId, ticks);
    const more = ticks.map(r => { const d=[r.ts, r.mid]; d.__id=r.id; return d; });
    s.data = more.concat(s.data);
    setSeries();
  }
  function removeSegmSeries(segmId) {
    for (const [k] of state.seriesMap) if (k.includes(`:${segmId}:`)) state.seriesMap.delete(k);
    setSeries();
  }

  function addOrUpdateLayerSeries(segmId, table, rows) {
    const key = keyS('layer', segmId, table);
    let series;
    if (table === 'atr1') {
      // draw legs as 'lines' segments
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
    setSeries();
  }

  // ---------------- Load logic ----------------
  async function initialLoadSegm(segmId) {
    const seg = state.segms.find(s => s.id === segmId); if (!seg) return;
    // first chunk from the start
    const first = await getSegmTicksChunk(segmId, seg.start_id, state.chunk);
    if (first.length) {
      seg.loadedMaxId = first[first.length-1].id;
      addOrUpdateTickSeries(segmId, first);
      await loadSegmLayers(segmId);
      // start background stream to the RIGHT (no zoom change)
      streamRight(segmId).catch(console.error);
    }
  }
  async function loadMoreLeft() {
    for (const segmId of state.selectedSegmIds) {
      const seg = state.segms.find(s => s.id === segmId); if (!seg) continue;
      // we currently store only loadedMaxId for right; for left we always fetch older than the earliest drawn point
      const k = keyS('ticks', segmId); const s = state.seriesMap.get(k);
      if (!s || !s.data || !s.data.length) continue;
      // earliest id we have:
      const earliestId = s.data[0].__id ?? seg.start_id;
      const from = Math.max(seg.start_id, earliestId - state.chunk);
      if (from >= earliestId) continue;
      const chunk = await getSegmTicksChunk(segmId, from, state.chunk);
      if (chunk.length) prependTickSeries(segmId, chunk);
    }
  }
  async function streamRight(segmId) {
    if (state.streaming.get(segmId)) return; // already streaming
    state.streaming.set(segmId, true);
    try {
      const seg = state.segms.find(s => s.id === segmId); if (!seg) return;
      while (state.selectedSegmIds.has(segmId)) {
        const from = (seg.loadedMaxId || seg.start_id) + 1;
        if (from > seg.end_id) break;
        const chunk = await getSegmTicksChunk(segmId, from, state.chunk);
        if (!chunk.length) break;
        seg.loadedMaxId = chunk[chunk.length-1].id;
        appendTickSeries(segmId, chunk);
        // tiny pause to keep UI responsive
        await new Promise(r => setTimeout(r, 40));
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
      // normalize iso for chart + keep numeric
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
