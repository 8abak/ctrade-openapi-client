/* frontend/review-core.js
 * Walk-forward review UI:
 * - Loads a natural tick window
 * - Preserves zoom/pan on Load/Run
 * - Journal with progress messages
 * - Renders Macro (bands), Micro events, Outcomes, Predictions
 * - Integer y-axis labels; exact prices plotted
 *
 * Assumes backend routes:
 *   GET  /ticks?start=<id>&end=<id>
 *   POST /walkforward/step
 *   GET  /walkforward/snapshot
 *
 * If your tick route differs, adjust fetchTicks().
 */

(() => {
  // ---------------------- Config ----------------------
  const DEFAULT_WINDOW = 6000;       // "Natural" window size
  const Y_PAD = 0.15;                // vertical pad as % of visible range
  const CACHE_BUSTER = () => `&_=${Date.now()}`; // dev cache buster
  const LS = window.localStorage;

  // ---------------------- State -----------------------
  const el = {
    start: document.getElementById('start-tick'),
    end: document.getElementById('end-tick'),
    load: document.getElementById('btn-load'),
    run: document.getElementById('btn-run'),
    jumpTo: document.getElementById('jump-tick'),
    jumpBtn: document.getElementById('btn-jump'),
    chkMacro: document.getElementById('chk-macro'),
    chkEvents: document.getElementById('chk-events'),
    chkPred: document.getElementById('chk-preds'),
    chkOut: document.getElementById('chk-outs'),
    chkMid: document.getElementById('chk-mid'),
    chkBid: document.getElementById('chk-bid'),
    chkAsk: document.getElementById('chk-ask'),
    journal: document.getElementById('journal'),
    journalToggle: document.getElementById('journal-toggle'),
    status: document.getElementById('status'),
    chart: document.getElementById('chart')
  };

  const state = {
    ticks: [],        // [{id, ts, mid, bid, ask}]
    macro: [],        // [{segment_id, start_tick_id, end_tick_id, direction, confidence}]
    events: [],       // [{event_id, tick_id, event_type, features}]
    outcomes: [],     // [{event_id, outcome}]
    preds: [],        // [{event_id, p_tp, threshold, decided, model_version}]
    zoom: null        // {xStart, xEnd, yMin, yMax}
  };

  // ---------------------- ECharts ---------------------
  const chart = echarts.init(el.chart, null, { renderer: 'canvas' });

  function intTickFormatter(val) {
    // Show integers only on y-axis labels
    return Math.round(val).toString();
  }

  const baseOption = {
    darkMode: true,
    animation: false,
    grid: { left: 40, right: 20, top: 40, bottom: 80 },
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'cross' },
      formatter: (params) => {
        if (!params || !params.length) return '';
        const idx = params[0].dataIndex;
        const t = state.ticks[idx];
        if (!t) return '';
        const dt = new Date(t.ts);
        const d = dt.toISOString().slice(0, 10);
        const time = dt.toISOString().slice(11, 19);
        const lines = [
          `tick: <b>${t.id}</b>`,
          `date: ${d} time (UTC): ${time}`,
          `Mid: <b>${(+t.mid).toFixed(2)}</b> Bid: <b>${(+t.bid).toFixed(2)}</b> Ask: <b>${(+t.ask).toFixed(2)}</b>`
        ];
        return lines.join('<br/>');
      }
    },
    xAxis: {
      type: 'category',
      boundaryGap: false,
      // category = tick id as string
      data: [],
      axisLabel: { showMinLabel: true, showMaxLabel: true }
    },
    yAxis: {
      type: 'value',
      scale: true,
      axisLabel: { formatter: intTickFormatter },
      splitNumber: 6
    },
    dataZoom: [
      { id: 'xInside', type: 'inside', xAxisIndex: 0, filterMode: 'none' },
      { id: 'xSlider', type: 'slider', xAxisIndex: 0, height: 28, bottom: 40 },
      { id: 'yInside', type: 'inside', yAxisIndex: 0, filterMode: 'none' }
    ],
    legend: { top: 10, left: 'center' },
    series: [
      {
        name: 'Mid',
        type: 'line',
        smooth: false,
        showSymbol: false,
        data: []
      },
      {
        name: 'Bid',
        type: 'line',
        smooth: false,
        showSymbol: false,
        data: []
      },
      {
        name: 'Ask',
        type: 'line',
        smooth: false,
        showSymbol: false,
        data: []
      },
      // Macro bands (markAreas)
      {
        name: 'Macro',
        type: 'line',
        data: [],
        lineStyle: { opacity: 0 },
        areaStyle: { opacity: 0.10 },
        markArea: { silent: true, itemStyle: { color: '#2e7d32', opacity: 0.12 }, data: [] }
      },
      // Events (points)
      {
        name: 'Events',
        type: 'scatter',
        symbolSize: 8,
        data: []
      },
      // Outcomes halo
      {
        name: 'Outcomes',
        type: 'scatter',
        symbolSize: 12,
        data: []
      }
    ]
  };

  chart.setOption(baseOption);

  // When user zooms horizontally, recompute visible y-range (natural min/max)
  chart.on('dataZoom', () => {
    autoAdjustY();
    captureZoom();
  });

  function captureZoom() {
    const opt = chart.getOption();
    const dzX = (opt.dataZoom || []).find(z => z.id === 'xInside') || (opt.dataZoom || [])[0];
    const dzY = (opt.dataZoom || []).find(z => z.id === 'yInside');
    const yAxis = (opt.yAxis && opt.yAxis[0]) || {};

    state.zoom = {
      xStart: dzX ? (dzX.start ?? 0) : 0,
      xEnd: dzX ? (dzX.end ?? 100) : 100,
      yMin: (typeof yAxis.min === 'number') ? yAxis.min : null,
      yMax: (typeof yAxis.max === 'number') ? yAxis.max : null
    };
  }

  function restoreZoom() {
    if (!state.zoom) return;
    chart.setOption({
      dataZoom: [
        { id: 'xInside', start: state.zoom.xStart, end: state.zoom.xEnd },
        { id: 'xSlider', start: state.zoom.xStart, end: state.zoom.xEnd },
        { id: 'yInside' } // keep
      ],
      yAxis: {
        min: state.zoom.yMin,
        max: state.zoom.yMax
      }
    }, false, false);
  }

  function autoAdjustY() {
    // Compute visible x range and adjust y-axis bounds with padding, integers on labels only
    const opt = chart.getOption();
    const dz = (opt.dataZoom || []).find(z => z.id === 'xInside');
    const data = state.ticks;
    if (!dz || !data.length) return;

    // dataZoom.start/end are percentages (0..100)
    const startIdx = Math.max(0, Math.floor((dz.start / 100) * (data.length - 1)));
    const endIdx = Math.min(data.length - 1, Math.ceil((dz.end / 100) * (data.length - 1)));

    let min = Infinity, max = -Infinity;
    for (let i = startIdx; i <= endIdx; i++) {
      const v = data[i].mid;
      if (v < min) min = v;
      if (v > max) max = v;
    }
    if (!isFinite(min) || !isFinite(max)) return;

    const pad = Math.max(0.01, (max - min) * Y_PAD);
    const yMin = Math.floor(min - pad);
    const yMax = Math.ceil(max + pad);

    chart.setOption({ yAxis: { min: yMin, max: yMax } }, false, false);
    // Keep record for restore
    state.zoom = state.zoom || {};
    state.zoom.yMin = yMin; state.zoom.yMax = yMax;
  }

  // ---------------------- Fetchers --------------------
  async function fetchTicks(startId, endId) {
    const url = `/ticks?start=${encodeURIComponent(startId)}&end=${encodeURIComponent(endId)}${CACHE_BUSTER()}`;
    const res = await fetch(url);
    if (!res.ok) throw new Error(`ticks HTTP ${res.status}`);
    return res.json(); // [{id, timestamp, bid, ask, mid}]
  }

  async function fetchSnapshot() {
    const res = await fetch(`/walkforward/snapshot${CACHE_BUSTER()}`);
    if (!res.ok) throw new Error(`snapshot HTTP ${res.status}`);
    return res.json();
  }

  async function postStep() {
    const res = await fetch(`/walkforward/step${CACHE_BUSTER()}`, { method: 'POST' });
    if (!res.ok) throw new Error(`step HTTP ${res.status}`);
    return res.json();
  }

  // ---------------------- Journal ---------------------
  function j(msg) {
    const now = new Date().toISOString().replace('T', ' ').replace('Z', '');
    const line = `[${now}] ${msg}`;
    if (el.journal) {
      const p = document.createElement('div');
      p.textContent = line;
      el.journal.prepend(p);
    }
    if (el.status) el.status.textContent = msg;
    console.log(line);
  }

  function initJournalToggle() {
    if (!el.journal || !el.journalToggle) return;
    const key = 'review.journal.open';
    const apply = (open) => {
      el.journal.style.display = open ? 'block' : 'none';
      el.journalToggle.textContent = open ? 'Journal ▾' : 'Journal ▸';
    };
    const saved = LS.getItem(key);
    let open = saved ? saved === '1' : false;
    apply(open);
    el.journalToggle.addEventListener('click', () => {
      open = !open;
      LS.setItem(key, open ? '1' : '0');
      apply(open);
    });
  }

  // ---------------------- Renderers -------------------
  function setSeriesVisibility() {
    chart.setOption({
      series: [
        { name: 'Mid',   show: el.chkMid?.checked ?? true },
        { name: 'Bid',   show: el.chkBid?.checked ?? false },
        { name: 'Ask',   show: el.chkAsk?.checked ?? false },
        { name: 'Macro', show: el.chkMacro?.checked ?? true },
        { name: 'Events', show: el.chkEvents?.checked ?? true },
        { name: 'Outcomes', show: el.chkOut?.checked ?? true }
      ]
    });
  }

  function renderAll() {
    const xCats = state.ticks.map(t => String(t.id));
    const mid = state.ticks.map(t => t.mid);
    const bid = state.ticks.map(t => t.bid);
    const ask = state.ticks.map(t => t.ask);

    // Macro bands
    const markAreas = [];
    for (const seg of state.macro) {
      const startIdx = state.ticks.findIndex(t => t.id === seg.start_tick_id);
      const endIdx = state.ticks.findIndex(t => t.id === seg.end_tick_id);
      if (startIdx === -1 || endIdx === -1) continue;
      const color = seg.direction > 0 ? '#2e7d32' : '#b71c1c';
      const opacity = Math.min(0.25, Math.max(0.08, seg.confidence || 0.12));
      markAreas.push([
        { xAxis: String(state.ticks[startIdx].id), itemStyle: { color, opacity } },
        { xAxis: String(state.ticks[endIdx].id) }
      ]);
    }

    // Events
    const evPts = [];
    for (const ev of state.events) {
      const t = state.ticks.find(tk => tk.id === ev.tick_id);
      if (!t) continue;
      const color = ev.event_type === 'pullback_end' ? '#00e676'
                  : ev.event_type === 'breakout'     ? '#29b6f6'
                  : '#ef5350';
      evPts.push({ value: [String(t.id), t.mid], itemStyle: { color } });
    }

    // Outcomes halo (use outcome color ring)
    const ocPts = [];
    for (const oc of state.outcomes) {
      const ev = state.events.find(e => e.event_id === oc.event_id);
      if (!ev) continue;
      const t = state.ticks.find(tk => tk.id === ev.tick_id);
      if (!t) continue;
      const color = oc.outcome === 'TP' ? '#00e676' : oc.outcome === 'SL' ? '#ff5252' : '#9e9e9e';
      ocPts.push({ value: [String(t.id), t.mid], itemStyle: { color, opacity: 0.9 } });
    }

    // Apply data & try to keep zoom
    captureZoom();
    chart.setOption({
      xAxis: { data: xCats },
      series: [
        { name: 'Mid', data: mid },
        { name: 'Bid', data: bid },
        { name: 'Ask', data: ask },
        {
          name: 'Macro',
          data: [], // invisible line
          markArea: { data: markAreas }
        },
        { name: 'Events', data: evPts },
        { name: 'Outcomes', data: ocPts }
      ]
    }, false, false);
    setSeriesVisibility();
    if (state.zoom) restoreZoom();
    autoAdjustY();
  }

  // ---------------------- Loading logic ----------------
  function currentWindow() {
    // A natural window using end tick (input) minus DEFAULT_WINDOW
    const startId = Math.max(1, parseInt(el.start.value || '1', 10));
    let endId = parseInt(el.end.value || '0', 10);
    if (!endId || endId < startId) endId = startId + DEFAULT_WINDOW - 1;
    const n = endId - startId + 1;
    if (n > DEFAULT_WINDOW) {
      const newStart = endId - DEFAULT_WINDOW + 1;
      return { start: newStart, end: endId };
    }
    return { start: startId, end: endId };
  }

  async function loadNaturalWindow() {
    try {
      const win = currentWindow();
      j(`Load window [${win.start}, ${win.end}]`);
      const rows = await fetchTicks(win.start, win.end);
      state.ticks = rows.map(r => ({
        id: r.id,
        ts: r.timestamp, mid: +r.mid, bid: +r.bid, ask: +r.ask
      }));
      // Snapshot layers
      const snap = await fetchSnapshot();
      state.macro = snap.macro_segments || [];
      state.events = snap.micro_events || [];
      state.outcomes = snap.outcomes || [];
      state.preds = snap.predictions || [];
      renderAll();

      // If first load, set view to the last DEFAULT_WINDOW region
      if (!state.zoom) {
        const n = state.ticks.length;
        if (n > 0) {
          const startPct = Math.max(0, (n - Math.min(n, DEFAULT_WINDOW)) / n) * 100;
          chart.setOption({
            dataZoom: [
              { id: 'xInside', start: startPct, end: 100 },
              { id: 'xSlider', start: startPct, end: 100 }
            ]
          }, false, false);
          autoAdjustY();
        }
      }
    } catch (e) {
      j(`ERROR loading: ${e.message}`);
    }
  }

  async function doRun() {
    try {
      captureZoom();
      j('Run: start');
      const r = await postStep();
      if (r && r.journal) {
        (Array.isArray(r.journal) ? r.journal : [r.journal]).forEach(x => x && j(x));
      }
      // pull fresh layers but keep current focus
      const snap = await fetchSnapshot();
      state.macro = snap.macro_segments || [];
      state.events = snap.micro_events || [];
      state.outcomes = snap.outcomes || [];
      state.preds = snap.predictions || [];
      renderAll();
      j('Run: done');
    } catch (e) {
      j(`Run error: ${e.message}`);
    }
  }

  async function doJump() {
    const tid = parseInt(el.jumpTo.value || '0', 10);
    if (!tid) return;
    const half = Math.floor(DEFAULT_WINDOW / 2);
    const start = Math.max(1, tid - half);
    const end = start + DEFAULT_WINDOW - 1;
    el.start.value = start;
    el.end.value = end;
    await loadNaturalWindow();
    // Center the slider on the requested tick if it exists in window
    const idx = state.ticks.findIndex(t => t.id === tid);
    if (idx >= 0) {
      const n = state.ticks.length;
      const windowSpan = Math.max(10, Math.floor(n * 0.3)); // show ~30% span around center
      const left = Math.max(0, idx - Math.floor(windowSpan / 2));
      const right = Math.min(n - 1, left + windowSpan);
      const startPct = (left / (n - 1)) * 100;
      const endPct = (right / (n - 1)) * 100;
      chart.setOption({
        dataZoom: [
          { id: 'xInside', start: startPct, end: endPct },
          { id: 'xSlider', start: startPct, end: endPct }
        ]
      }, false, false);
      autoAdjustY();
    }
  }

  // ---------------------- Wire up UI -------------------
  function initControls() {
    el.load?.addEventListener('click', () => {
      captureZoom();
      loadNaturalWindow();
    });
    el.run?.addEventListener('click', async () => {
      await doRun();
    });
    el.jumpBtn?.addEventListener('click', doJump);

    [el.chkMacro, el.chkEvents, el.chkPred, el.chkOut, el.chkMid, el.chkBid, el.chkAsk]
      .forEach(ch => ch && ch.addEventListener('change', setSeriesVisibility));
  }

  // ---------------------- Init ------------------------
  function boot() {
    initJournalToggle();
    initControls();
    // sensible defaults
    if (!el.start.value) el.start.value = '1';
    if (!el.end.value) el.end.value = String(DEFAULT_WINDOW);
    loadNaturalWindow().then(() => j('Ready'));
  }

  document.addEventListener('DOMContentLoaded', boot);
})();
