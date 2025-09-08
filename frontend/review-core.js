// PATH: frontend/review-core.js
(() => {
  const el = (id) => document.getElementById(id);

  // --- ECharts setup ---------------------------------------------------
  const chart = echarts.init(el('chart'));
  const opt = {
    animation: false,
    backgroundColor: '#0f172a',
    grid: { left: 40, right: 18, top: 18, bottom: 28 },
    tooltip: { trigger: 'axis', axisPointer: { type: 'cross' } },
    dataZoom: [
      { type: 'inside', zoomOnMouseWheel: true },
      { type: 'slider', height: 18 }
    ],
    xAxis: { type: 'time', axisLabel: { color: '#cbd5e1' } },
    yAxis: {
      type: 'value',
      scale: true,
      axisLabel: {
        color: '#cbd5e1',
        formatter: (v) => (typeof v === 'number' ? v.toFixed(2) : v) // stick to real numbers like live.html
      },
      splitLine: { lineStyle: { color: '#233047' } }
    },
    series: []
  };
  chart.setOption(opt);

  // --- State -----------------------------------------------------------
  const state = {
    chunk: 2000,
    segms: [],           // [{id,start_id,end_id,..., loadedFromId}]
    selectedSegmIds: new Set(),
    selectedTables: new Set(), // user-chosen overlay tables
    seriesMap: new Map() // key -> series object
  };

  // --- Helpers ---------------------------------------------------------
  function fmtTs(s) { return new Date(s); }
  function chip(txt) {
    const d = document.createElement('div'); d.className = 'chip'; d.textContent = txt; return d;
  }
  function setStat() {
    const S = el('stat'); S.innerHTML = '';
    S.appendChild(chip(`segms: ${state.selectedSegmIds.size}`));
    S.appendChild(chip(`tables: ${state.selectedTables.size}`));
  }
  function seriesKey(kind, segmId, extra='') { return `${kind}:${segmId}:${extra}`; }

  // --- API -------------------------------------------------------------
  async function api(url) {
    const r = await fetch(url);
    if (!r.ok) throw new Error(await r.text());
    return r.json();
  }
  const getSegms = (limit=400) => api(`/api/segm/recent?limit=${limit}`);
  const getSegmTicksChunk = (id, fromId, limit) =>
    api(`/api/segm/ticks?id=${id}&from=${fromId}&limit=${limit}`);
  const getSegmLayers = (id, tablesCSV) =>
    api(`/api/segm/layers?id=${id}&tables=${encodeURIComponent(tablesCSV)}`);
  const listTables = () => api(`/api/sql/tables`); // alias exists as /api/tables

  // --- UI: segments list ----------------------------------------------
  async function populateSegms() {
    const rows = await getSegms(800);
    state.segms = rows.map(r => ({ ...r, loadedFromId: r.start_id })); // nothing loaded yet
    const host = el('segmList'); host.innerHTML = '';
    rows.forEach(r => {
      const row = document.createElement('div');
      row.className = 'segm-row';
      row.innerHTML = `
        <input type="checkbox" data-id="${r.id}"/>
        <div class="small">${r.start_id}</div>
        <div class="small">${r.end_id}</div>
        <div class="small">${r.dir}</div>
        <div class="small">${new Date(r.start_ts).toLocaleString()}</div>`;
      row.querySelector('input').addEventListener('change', (e) => {
        if (e.target.checked) state.selectedSegmIds.add(r.id);
        else state.selectedSegmIds.delete(r.id);
        setStat();
        // initial load for this segm
        if (e.target.checked) initialLoadSegm(r.id).catch(console.error);
        else removeSegmSeries(r.id);
      });
      host.appendChild(row);
    });
  }

  function removeSegmSeries(segmId) {
    // remove all series for this segm
    for (const [k] of state.seriesMap) {
      if (k.includes(`:${segmId}:`)) {
        state.seriesMap.delete(k);
      }
    }
    chart.setOption({ series: [...state.seriesMap.values()] }, { replaceMerge: ['series'] });
  }

  // --- UI: table list (layers) ----------------------------------------
  async function populateTables() {
    const tables = await listTables();
    const host = el('tblList'); host.innerHTML = '';
    // ignore obvious system/large raw tables; keep user ones visible
    const blacklist = new Set(['ticks']); // we always draw ticks as base
    tables.filter(t => !blacklist.has(t)).forEach(t => {
      const row = document.createElement('div'); row.className = 'tbl-check';
      const id = `tbl_${t}`;
      row.innerHTML = `
        <label for="${id}">${t}</label>
        <input id="${id}" type="checkbox" data-t="${t}" />
      `;
      row.querySelector('input').addEventListener('change', async (e) => {
        const name = e.target.dataset.t;
        if (e.target.checked) state.selectedTables.add(name);
        else state.selectedTables.delete(name);
        setStat();
        // reload layers for currently selected segms
        for (const segmId of state.selectedSegmIds) {
          await loadSegmLayers(segmId);
        }
      });
      host.appendChild(row);
    });
  }

  // --- Loading logic ---------------------------------------------------
  async function initialLoadSegm(segmId) {
    const seg = state.segms.find(s => s.id === segmId);
    if (!seg) return;

    // base ticks: first chunk from segment start
    const ticks = await getSegmTicksChunk(segmId, seg.start_id, state.chunk);
    seg.loadedFromId = (ticks.length ? ticks[0].id : seg.start_id); // earliest in this chunk
    addOrUpdateTickSeries(segmId, ticks);

    // overlays
    await loadSegmLayers(segmId);
    chart.resize();
  }

  async function loadMore() {
    const tasks = [];
    for (const segmId of state.selectedSegmIds) {
      const seg = state.segms.find(s => s.id === segmId);
      if (!seg) continue;
      const from = Math.max(seg.start_id, (seg.loadedFromId || seg.start_id) - 1_000_000); // guard
      const nextFrom = Math.max(seg.start_id, (seg.loadedFromId || seg.start_id) - state.chunk);
      if (nextFrom >= seg.loadedFromId) continue; // nothing to do
      tasks.push((async () => {
        const chunk = await getSegmTicksChunk(segmId, nextFrom, state.chunk);
        if (chunk.length) {
          seg.loadedFromId = chunk[0].id;
          prependTickSeries(segmId, chunk);
        }
      })());
    }
    await Promise.all(tasks);
  }

  async function loadSegmLayers(segmId) {
    if (!state.selectedTables.size) return;
    const tablesCSV = [...state.selectedTables].join(',');
    const payload = await getSegmLayers(segmId, tablesCSV);
    Object.entries(payload.layers || {}).forEach(([tname, rows]) => {
      addOrUpdateLayerSeries(segmId, tname, rows);
    });
  }

  // --- Chart series builders ------------------------------------------
  function addOrUpdateTickSeries(segmId, ticks) {
    const k = seriesKey('ticks', segmId);
    const data = ticks.map(r => [fmtTs(r.ts), r.mid]);
    const s = {
      id: k, name: `mid #${segmId}`, type: 'line',
      showSymbol: false, sampling: 'lttb', large: true,
      data
    };
    state.seriesMap.set(k, s);
    chart.setOption({ series: [...state.seriesMap.values()] }, { replaceMerge: ['series'] });
  }
  function prependTickSeries(segmId, ticks) {
    const k = seriesKey('ticks', segmId);
    const s = state.seriesMap.get(k); if (!s) return addOrUpdateTickSeries(segmId, ticks);
    const more = ticks.map(r => [fmtTs(r.ts), r.mid]);
    s.data = more.concat(s.data);
    chart.setOption({ series: [...state.seriesMap.values()] }, { replaceMerge: ['series'] });
  }

  function addOrUpdateLayerSeries(segmId, table, rows) {
    // Special shapes:
    // - atr1: has start_ts/end_ts and span/dir -> draw as step segments
    // - bigm/smal: a_id/b_id or a_ts/b_ts -> draw as segment overlays
    // - level: draw as horizontal markers
    const key = seriesKey(table, segmId);
    let series;

    if (table === 'atr1') {
      // convert each leg into [ [ts, price], [ts, price] ] segments
      const segs = [];
      rows.forEach(r => {
        segs.push([[fmtTs(r.start_ts), r.start_mid ?? null], [fmtTs(r.end_ts), r.end_mid ?? null]]);
      });
      series = {
        id: key, name: `atr1 #${segmId}`, type: 'lines',
        polyline: false, coordinateSystem: 'cartesian2d',
        lineStyle: { width: 1.5 },
        effect: { show: false },
        data: segs.map(([a,b]) => ({ coords: [a, b] }))
      };
    } else if (table === 'level') {
      series = {
        id: key, name: `level #${segmId}`, type: 'scatter',
        symbolSize: 6,
        data: rows.map(r => [fmtTs(r.ts), r.price])
      };
    } else if (table === 'bigm' || table === 'smal' || table === 'pred') {
      const segs = rows.map(r => ({
        coords: [[fmtTs(r.a_ts || r.start_ts), r.a_mid ?? r.a_price ?? r.price ?? null],
                 [fmtTs(r.b_ts || r.end_ts),   r.b_mid ?? r.b_price ?? r.price ?? null]]
      }));
      series = {
        id: key, name: `${table} #${segmId}`, type: 'lines', lineStyle: { width: 2 }, data: segs
      };
    } else {
      // generic fallback: time+value columns if present
      const guess = rows.map(r => [fmtTs(r.ts || r.a_ts || r.start_ts || r.time || r.created_at),
                                   r.mid ?? r.value ?? r.price ?? r.span ?? null]).filter(x => x[0] && x[1] != null);
      series = {
        id: key, name: `${table} #${segmId}`, type: 'line', showSymbol:false, data: guess
      };
    }

    state.seriesMap.set(key, series);
    chart.setOption({ series: [...state.seriesMap.values()] }, { replaceMerge: ['series'] });
  }

  // --- Wire up ---------------------------------------------------------
  el('btnReload').addEventListener('click', async () => {
    state.chunk = Math.max(500, Math.min(20000, Number(el('chunk').value||2000)));
    state.seriesMap.clear();
    chart.setOption({ series: [] });
    await populateSegms();
    await populateTables();
    setStat();
  });
  el('btnLoadMore').addEventListener('click', () => loadMore().catch(console.error));

  // initial
  el('chunk').value = String(state.chunk);
  el('btnReload').click();

  // responsive
  window.addEventListener('resize', () => chart.resize());
})();
