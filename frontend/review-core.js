(() => {
  // ------------------------------------------------------------
  // Small helpers
  // ------------------------------------------------------------
  const $ = (id) => document.getElementById(id);
  const sleep = (ms) => new Promise(r => setTimeout(r, ms));
  const nowIso = () => new Date().toISOString();
  const status = (msg) => { $('status').textContent = msg; };

  // Journal helpers
  const jwrap = $('journalWrap');
  const jbox  = $('journal');
  const journalToggle = () => {
    const open = jwrap.style.display !== 'none';
    jwrap.style.display = open ? 'none' : 'block';
    $('btnJournal').textContent = open ? 'Journal ▸' : 'Journal ▾';
  };
  const j = (line) => {
    const t = new Date().toISOString().replace('T',' ').replace('Z','');
    jbox.textContent += `[${t}] ${line}\n`;
    jbox.scrollTop = jbox.scrollHeight;
  };

  $('btnJournal').addEventListener('click', journalToggle);
  $('btnHideJ').addEventListener('click', journalToggle);
  $('btnClearJ').addEventListener('click', () => jbox.textContent = '');
  // Start collapsed
  jwrap.style.display = 'none';
  $('btnJournal').textContent = 'Journal ▸';

  // ------------------------------------------------------------
  // API endpoints (with /walkforward first, then /api fallback)
  // ------------------------------------------------------------
  async function wfFetch(path, opts = {}) {
    // try /walkforward/*
    let res = await fetch(path.startsWith('/') ? path : `/${path}`, opts);
    if (res.status === 404 && path.startsWith('walkforward/')) {
      // try /api/walkforward/*
      res = await fetch(`/api/${path}`, opts);
    }
    return res;
  }

  // ------------------------------------------------------------
  // State
  // ------------------------------------------------------------
  const state = {
    // data
    ticks: [],        // [{id, timestamp, bid, ask, mid}]
    segments: [],     // [{segment_id, start_tick_id, end_tick_id, direction, confidence, ...}]
    events: [],       // [{event_id, segment_id, tick_id, event_type, features, event_ts, event_price}]
    outcomes: [],     // [{event_id, outcome, ...}]
    preds: [],        // [{event_id, model_version, p_tp, threshold, decided, predicted_at}]
    // lookups
    eventsByTick: new Map(),   // tick_id -> array of events
    outcomeByEvent: new Map(), // event_id -> outcome row
    predByEvent: new Map(),    // event_id -> prediction row
    // UI / view
    view: { xStart: null, xEnd: null, yMin: null, yMax: null }, // persisted view window
  };

  // ------------------------------------------------------------
  // ECharts setup
  // ------------------------------------------------------------
  const el = $('chart');
  const chart = echarts.init(el, null, { renderer: 'canvas' });

  function intTickFormatter(val) {
    // y-axis labels as integers only (no decimals on the axis labels)
    return Math.round(val).toString();
  }

  const baseOption = {
    darkMode: true,
    animation: false,
    backgroundColor: '#0f141a',
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'cross' },
      confine: true,
      formatter: function (params) {
        // params: array (Mid/Bid/Ask) at the same index
        if (!params || !params.length) return '';
        const idx = params[0].dataIndex;
        const row = state.ticks[idx];
        if (!row) return '';

        const dt = new Date(row.timestamp);
        const date = dt.toISOString().slice(0,10);
        const time = dt.toISOString().slice(11,19);

        // events/preds/outcomes on this tick
        const evs = state.eventsByTick.get(row.id) || [];
        const lines = [
          `<b>tick:</b> ${row.id}`,
          `<b>date:</b> ${date} <b>time (UTC):</b> ${time}`,
          `<b>Mid:</b> ${row.mid} <b>Bid:</b> ${row.bid} <b>Ask:</b> ${row.ask}`
        ];

        if (evs.length) {
          for (const e of evs) {
            let pred = state.predByEvent.get(e.event_id);
            let outc = state.outcomeByEvent.get(e.event_id);
            lines.push(
              `<hr style="border:none;border-top:1px solid #23354a;margin:.3rem 0;" />` +
              `<b>Event</b> #${e.event_id} <i>${e.event_type}</i>` +
              (pred ? `<br/><b>Pred:</b> p_tp=${(+pred.p_tp).toFixed(6)}, th=${pred.threshold}, decided=${pred.decided}, model=${pred.model_version}` : '') +
              (outc ? `<br/><b>Outcome:</b> ${outc.outcome}` : '')
            );
          }
        } else {
          lines.push(`<i>No events on this tick.</i>`);
        }

        // macro segment membership
        const seg = state.segments.find(s => row.id >= s.start_tick_id && row.id <= s.end_tick_id);
        if (seg) {
          lines.push(
            `<hr style="border:none;border-top:1px solid #23354a;margin:.3rem 0;" />` +
            `<b>Macro:</b> seg ${seg.segment_id} dir=${seg.direction} conf=${(+seg.confidence).toFixed(3)}`
          );
        }

        return lines.join('<br/>');
      }
    },
    grid: { left: 56, right: 16, top: 28, bottom: 48, containLabel: false },
    xAxis: {
      type: 'category',
      boundaryGap: false,
      axisLine: { lineStyle: { color: '#3a4856' } },
      axisLabel: { color: '#95a4b3' },
      axisTick: { show: false },
      data: [], // tick ids
    },
    yAxis: {
      type: 'value',
      scale: true,
      minInterval: 1,              // do not label fractional numbers
      axisLabel: { color: '#95a4b3', formatter: intTickFormatter },
      splitLine: { lineStyle: { color: '#1e2833' } },
    },
    dataZoom: [
      // X inside (drag left/right & wheel to zoom)
      { type: 'inside', xAxisIndex: 0, filterMode: 'none', zoomOnMouseWheel: true, moveOnMouseMove: true, moveOnMouseWheel: true, throttle: 50 },
      // X slider
      { type: 'slider', xAxisIndex: 0, height: 22, top: null, bottom: 12, showDetail: false, brushSelect: true },
      // Y inside (wheel to zoom; hold Shift to zoom Y with wheel; drag to pan when zoomed)
      { type: 'inside', yAxisIndex: 0, filterMode: 'none', zoomOnMouseWheel: 'shift', moveOnMouseMove: true, moveOnMouseWheel: true }
    ],
    legend: {
      top: 4,
      textStyle: { color: '#aab9c8' },
      data: ['Mid','Bid','Ask','Macro','Events','Outcomes'],
    },
    series: []
  };

  chart.setOption(baseOption);

  // ------------------------------------------------------------
  // Series builders
  // ------------------------------------------------------------
  function buildPriceSeries() {
    const ids = state.ticks.map(r => r.id);
    const mid = state.ticks.map(r => r.mid);
    const bid = state.ticks.map(r => r.bid);
    const ask = state.ticks.map(r => r.ask);

    return [
      {
        name: 'Mid', type: 'line', showSymbol: false, smooth: false,
        lineStyle: { width: 1.2, color: '#9dbbff' },
        emphasis: { focus: 'series' },
        data: mid, xAxisIndex: 0, yAxisIndex: 0,
        visible: $('chkMid').checked
      },
      {
        name: 'Bid', type: 'line', showSymbol: false, smooth: false,
        lineStyle: { width: .9, color: '#4aa3ff' },
        emphasis: { focus: 'series' },
        data: bid, xAxisIndex: 0, yAxisIndex: 0,
        visible: $('chkBid').checked
      },
      {
        name: 'Ask', type: 'line', showSymbol: false, smooth: false,
        lineStyle: { width: .9, color: '#eab66c' },
        emphasis: { focus: 'series' },
        data: ask, xAxisIndex: 0, yAxisIndex: 0,
        visible: $('chkAsk').checked
      }
    ];
  }

  function buildMacroAreas() {
    if (!$('chkMacro').checked || !state.segments.length) return [];
    // Draw shaded areas over macro segments using markArea on a dummy line
    const areas = state.segments.map(s => ({
      name: `Seg ${s.segment_id}`,
      itemStyle: {
        color: s.direction > 0 ? 'rgba(39,211,142,0.10)' : 'rgba(255,107,107,0.10)'
      },
      label: { show: false },
      xAxis: s.start_tick_id,
      xAxis2: s.end_tick_id
    }));
    return [{
      name: 'Macro',
      type: 'line',
      data: state.ticks.map(_ => null),
      showSymbol: false,
      lineStyle: { width: 0 },
      markArea: {
        itemStyle: { opacity: 1 },
        data: areas.map(a => [{ xAxis: a.xAxis }, { xAxis: a.xAxis2, itemStyle: a.itemStyle }])
      }
    }];
  }

  function buildEventsScatter() {
    if (!$('chkEvents').checked || !state.events.length) return [];
    return [{
      name: 'Events',
      type: 'scatter',
      symbolSize: 7,
      itemStyle: { color: '#ff7d7d' },
      data: state.events.map(e => {
        const x = state.ticks.findIndex(t => t.id === e.tick_id);
        if (x < 0) return null;
        return [x, state.ticks[x].mid];
      }).filter(Boolean)
    }];
  }

  function buildOutcomesScatter() {
    if (!$('chkOutcomes').checked || !state.outcomes.length) return [];
    // show at the event tick too
    return [{
      name: 'Outcomes',
      type: 'scatter',
      symbolSize: 7,
      itemStyle: { color: '#b784ff' },
      data: state.outcomes.map(o => {
        const ev = state.events.find(e => e.event_id === o.event_id);
        if (!ev) return null;
        const x = state.ticks.findIndex(t => t.id === ev.tick_id);
        if (x < 0) return null;
        return [x, state.ticks[x].mid];
      }).filter(Boolean)
    }];
  }

  function rebuildLookups() {
    state.eventsByTick.clear();
    state.outcomeByEvent.clear();
    state.predByEvent.clear();
    for (const e of state.events) {
      const arr = state.eventsByTick.get(e.tick_id) || [];
      arr.push(e);
      state.eventsByTick.set(e.tick_id, arr);
    }
    for (const o of state.outcomes) state.outcomeByEvent.set(o.event_id, o);
    for (const p of state.preds)    state.predByEvent.set(p.event_id, p);
  }

  // ------------------------------------------------------------
  // Rendering with view preservation
  // ------------------------------------------------------------
  function currentViewFromChart() {
    const opt = chart.getOption();
    const xData = state.ticks.map(t => t.id);
    // dataZoom slider is opt.dataZoom[1] (the slider)
    if (opt.dataZoom && opt.dataZoom.length) {
      const dz = opt.dataZoom[0]; // inside-x
      const start = (dz.startValue !== undefined) ? dz.startValue :
                    (dz.start !== undefined) ? Math.floor(dz.start / 100 * (xData.length-1)) : 0;
      const end   = (dz.endValue   !== undefined) ? dz.endValue   :
                    (dz.end   !== undefined) ? Math.floor(dz.end / 100 * (xData.length-1))   : xData.length-1;
      state.view.xStart = start;
      state.view.xEnd   = end;
    }
    if (opt.yAxis && opt.yAxis.length) {
      state.view.yMin = opt.yAxis[0].min ?? null;
      state.view.yMax = opt.yAxis[0].max ?? null;
    }
  }

  function applyViewToOption(option) {
    const xLen = state.ticks.length;
    const defaultSpan = Math.min(xLen - 1, Math.max(500, Math.floor(xLen * 0.3))); // show ~30% or ≥500 points
    let start = state.view.xStart, end = state.view.xEnd;

    if (start == null || end == null) {
      // first load: focus last defaultSpan points
      end   = xLen - 1;
      start = Math.max(0, end - defaultSpan);
    } else {
      // clamp to new length
      start = Math.max(0, Math.min(start, xLen - 2));
      end   = Math.max(start + 1, Math.min(end, xLen - 1));
    }

    option.dataZoom = option.dataZoom || baseOption.dataZoom;
    option.dataZoom[0].startValue = start;
    option.dataZoom[0].endValue   = end;
    option.dataZoom[1].startValue = start;
    option.dataZoom[1].endValue   = end;

    // y auto-fit for the visible window unless user locked with manual min/max
    if (state.view.yMin != null && state.view.yMax != null) {
      option.yAxis.min = state.view.yMin;
      option.yAxis.max = state.view.yMax;
    } else {
      const slice = state.ticks.slice(start, end + 1);
      if (slice.length) {
        const lo = Math.min(...slice.map(r => r.mid));
        const hi = Math.max(...slice.map(r => r.mid));
        const pad = (hi - lo) * 0.05 || 1;
        option.yAxis.min = Math.floor(lo - pad);
        option.yAxis.max = Math.ceil(hi + pad);
      }
    }
  }

  function renderAll(preserve=true) {
    if (preserve) currentViewFromChart();

    const xCats = state.ticks.map(r => r.id);
    const series = [
      ...buildPriceSeries(),
      ...buildMacroAreas(),
      ...buildEventsScatter(),
      ...buildOutcomesScatter()
    ];

    const option = {
      ...baseOption,
      xAxis: { ...baseOption.xAxis, data: xCats },
      yAxis: { ...baseOption.yAxis },
      series
    };
    applyViewToOption(option);
    chart.setOption(option, true); // replace – keeps dataZoom handles coherent
  }

  // ------------------------------------------------------------
  // Data loading
  // ------------------------------------------------------------
  async function loadTicksRange(startId, endId) {
    const url = `/ticks/range?start=${encodeURIComponent(startId)}&end=${encodeURIComponent(endId)}&limit=200000`;
    const res = await fetch(url);
    if (!res.ok) throw new Error(`GET ${url} → ${res.status}`);
    const rows = await res.json();
    state.ticks = rows || [];
  }

  async function refreshSnapshot() {
    const res = await wfFetch('walkforward/snapshot');
    if (!res.ok) throw new Error(`snapshot → ${res.status}`);
    const snap = await res.json();
    state.segments = snap.segments || [];
    state.events   = snap.events   || [];
    state.outcomes = snap.outcomes || [];
    state.preds    = snap.predictions || [];
    rebuildLookups();
  }

  // ------------------------------------------------------------
  // UI wiring
  // ------------------------------------------------------------
  async function doLoad() {
    try {
      status('Loading…');
      const a = parseInt($('startTick').value, 10);
      const b = parseInt($('endTick').value, 10);
      await loadTicksRange(a, b);
      await refreshSnapshot();
      renderAll(true);
      status(`Loaded ${state.ticks.length} ticks`);
      j(`Load window [${a}, ${b}]`);
    } catch (e) {
      console.error(e);
      status('Load failed');
      j('ERROR load: ' + (e?.message || e));
    }
  }

  async function doRun() {
    try {
      status('Run: start');
      j('Run: start');

      const res = await wfFetch('walkforward/step', { method: 'POST' });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err?.error || ('HTTP ' + res.status));
      }
      const body = await res.json();
      if (Array.isArray(body.journal)) {
        for (const line of body.journal) j(line);
      } else {
        j('Run ok.');
      }

      // Always get a fresh snapshot, and DO NOT change current focus
      await refreshSnapshot();
      renderAll(true);

      status('Run: done');
      j('Run: done');
    } catch (e) {
      console.error(e);
      status('Run: error');
      j('Run error: ' + (e?.message || e));
    }
  }

  function onJump() {
    let t = parseInt($('jumpTick').value, 10);
    if (!Number.isFinite(t) || !state.ticks.length) return;

    // Choose a healthy window around the tick (~3000 each side, clamped)
    const span = 3000;
    const start = Math.max(1, t - span);
    const end   = t + span;

    $('startTick').value = start;
    $('endTick').value   = end;
    doLoad();
  }

  // toggles
  for (const id of ['chkMid','chkBid','chkAsk','chkMacro','chkEvents','chkPreds','chkOutcomes']) {
    $(id).addEventListener('change', () => renderAll(true));
  }

  $('btnLoad').addEventListener('click', doLoad);
  $('btnRun').addEventListener('click', doRun);
  $('btnJump').addEventListener('click', onJump);

  // First paint: if someone lands and hits Run before Load, we still show snapshot overlays after first data load.
  status('Ready');
})();
