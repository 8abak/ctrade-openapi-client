// frontend/review-core.js
// Drop-in: smart y-extents + segment-focus + no-thinning + show/hide ticks + ATR on 2nd axis.

(() => {
  const $ = (sel, root = document) => root.querySelector(sel);
  const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

  const State = {
    chunk: 2000,
    noThinning: false,
    showTicks: true,
    segmListBound: false,     // bind segment table clicks once
    data: {
      segm: null,
      layers: {}              // name -> [{ts, value}] etc.
    },
    echarts: null,
    // x-window (ms) we want to display; null = auto (full data)
    xMin: null,
    xMax: null
  };

  // ---------------- UI toggles ----------------
  function ensureExtraToggles() {
    const layersRow = document.querySelector('.layers-row') || document.querySelector('.layers') || document.body;

    if (!$('#toggleNoThin')) {
      const lbl = document.createElement('label');
      lbl.style.marginLeft = '12px';
      lbl.innerHTML = `<input type="checkbox" id="toggleNoThin" /> No thinning`;
      layersRow.appendChild(lbl);
      $('#toggleNoThin').addEventListener('change', (e) => {
        State.noThinning = e.target.checked;
        render();
      });
    }

    if (!$('#toggleShowTicks')) {
      const lbl = document.createElement('label');
      lbl.style.marginLeft = '12px';
      lbl.innerHTML = `<input type="checkbox" id="toggleShowTicks" checked /> Show ticks`;
      layersRow.appendChild(lbl);
      $('#toggleShowTicks').addEventListener('change', (e) => {
        State.showTicks = e.target.checked;
        render();
      });
    }
  }

  function wireChunkInput() {
    const el = document.querySelector('#chunkInput') || document.querySelector('input[name="chunk"]');
    if (!el) return;
    const apply = () => {
      const v = parseInt(el.value, 10);
      if (!Number.isNaN(v) && v > 0) {
        State.chunk = v;
        if (!State.noThinning) render();
      }
    };
    el.addEventListener('change', apply);
    apply();
  }

  // ---------------- Downsampling ----------------
  function thinToChunk(points, chunk) {
    if (!points || points.length <= chunk) return points;
    const stride = Math.ceil(points.length / chunk);
    const out = [];
    for (let i = 0; i < points.length; i += stride) out.push(points[i]);
    // include the last point for proper tooltips
    if (out.length && out[out.length - 1][0] !== points[points.length - 1][0]) out.push(points[points.length - 1]);
    return out;
  }

  // ---------------- Adapters ----------------
  function mapTicksToSeries(segm) {
    return (segm?.ticks || []).map(t => [new Date(t.ts).getTime(), t.mid]);
  }
  function mapAtrToSeries(arr) {
    return (arr || []).map(r => [new Date(r.ts).getTime(), r.value ?? r.atr ?? r.atr1]).filter(p => p[1] != null);
  }

  // ---------------- ECharts ----------------
  function getChart() {
    if (State.echarts) return State.echarts;
    const el = document.getElementById('chart') || document.getElementById('reviewChart');
    if (!el) return null;
    State.echarts = echarts.init(el);
    window.addEventListener('resize', () => State.echarts && State.echarts.resize());

    // Re-fit Y on any zoom/pan
    State.echarts.on('dataZoom', () => {
      const [xMin, xMax] = getCurrentXWindow();
      State.xMin = xMin; State.xMax = xMax;
      refitYAxes();
    });
    return State.echarts;
  }

  function getCurrentXWindow() {
    // Derive from dataZoom or fall back to State.xMin/xMax/full range
    const chart = getChart();
    const option = chart?.getOption?.();
    const xAxis = option?.xAxis?.[0];
    const min = (xAxis && xAxis.min != null) ? +xAxis.min : State.xMin;
    const max = (xAxis && xAxis.max != null) ? +xAxis.max : State.xMax;

    if (min != null && max != null) return [min, max];

    // fallback to full data range
    const ticks = mapTicksToSeries(State.data.segm);
    if (!ticks.length) return [null, null];
    return [ticks[0][0], ticks[ticks.length - 1][0]];
  }

  // Compute y-extent within the visible x-window
  function computeExtent(series, xMin, xMax) {
    let lo = +Infinity, hi = -Infinity;
    for (const [x, y] of series) {
      if (xMin != null && x < xMin) continue;
      if (xMax != null && x > xMax) continue;
      if (y < lo) lo = y;
      if (y > hi) hi = y;
    }
    if (!isFinite(lo) || !isFinite(hi)) return null;
    if (lo === hi) { // protect against flat lines
      const pad = Math.max(1e-6, Math.abs(hi) * 0.001);
      lo -= pad; hi += pad;
    }
    // add padding proportional to range
    const pad = (hi - lo) * 0.08; // 8% headroom/footroom
    return [lo - pad, hi + pad];
  }

  // Recompute Y axes based on visible window & visible series
  function refitYAxes() {
    const chart = getChart();
    if (!chart) return;

    const [xMin, xMax] = getCurrentXWindow();

    // Visible series
    let ticks = mapTicksToSeries(State.data.segm);
    let atr = mapAtrToSeries(State.data.layers['atr1']);

    if (!State.noThinning) {
      ticks = thinToChunk(ticks, State.chunk);
      atr   = thinToChunk(atr,   State.chunk);
    }

    const leftExtent  = State.showTicks ? computeExtent(ticks, xMin, xMax) : null;
    const rightExtent = isAtrWanted() && atr.length ? computeExtent(atr, xMin, xMax) : null;

    const option = {};
    option.yAxis = [
      {
        type: 'value', scale: true, name: 'Price',
        axisLabel: { formatter: v => (+v).toFixed(2) },
        ...(leftExtent ? { min: leftExtent[0], max: leftExtent[1] } : {})
      },
      {
        type: 'value', scale: true, name: 'ATR', position: 'right',
        axisLabel: { formatter: v => (+v).toFixed(4) },
        ...(rightExtent ? { min: rightExtent[0], max: rightExtent[1] } : {})
      }
    ];

    chart.setOption(option);
  }

  function isAtrWanted() {
    const cb = document.querySelector('input[type=checkbox][data-layer="atr1"]')
           || document.querySelector('input[type=checkbox][name="atr1"]');
    return cb ? cb.checked : true;
  }

  // ---------------- Render ----------------
  function render() {
    const chart = getChart();
    if (!chart) return;

    let tickSeries = mapTicksToSeries(State.data.segm);
    let atrSeries  = mapAtrToSeries(State.data.layers['atr1']);

    if (!State.noThinning) {
      tickSeries = thinToChunk(tickSeries, State.chunk);
      atrSeries  = thinToChunk(atrSeries,  State.chunk);
    }

    const series = [];
    if (State.showTicks) {
      series.push({
        name: 'mid',
        type: 'line',
        showSymbol: false,
        yAxisIndex: 0,
        data: tickSeries,
        sampling: State.noThinning ? undefined : 'lttb',
        large: true, largeThreshold: 200000
      });
    }
    if (isAtrWanted() && atrSeries.length) {
      series.push({
        name: 'atr1',
        type: 'line',
        showSymbol: false,
        yAxisIndex: 1,
        data: atrSeries,
        sampling: State.noThinning ? undefined : 'lttb',
        large: true, largeThreshold: 200000,
        lineStyle: { width: 1.5 }
      });
    }

    // x-axis window (stay put when user has focused a segment)
    const [xMin, xMax] = (State.xMin != null && State.xMax != null)
      ? [State.xMin, State.xMax]
      : (tickSeries.length ? [tickSeries[0][0], tickSeries[tickSeries.length - 1][0]] : [null, null]);

    const option = {
      animation: false,
      tooltip: { trigger: 'axis', axisPointer: { type: 'cross' } },
      legend: { show: true },
      xAxis: [{ type: 'time', min: xMin ?? 'dataMin', max: xMax ?? 'dataMax' }],
      yAxis: [
        { type: 'value', scale: true, name: 'Price', axisLabel: { formatter: v => (+v).toFixed(2) } },
        { type: 'value', scale: true, name: 'ATR',   axisLabel: { formatter: v => (+v).toFixed(4) }, position: 'right' }
      ],
      dataZoom: [
        { type: 'inside', throttle: 0 },
        { type: 'slider', height: 20 }
      ],
      series
    };

    chart.setOption(option, true);
    // after drawing, fit Y to the current X-window
    refitYAxes();
    // ensure we’re bound to segment list clicks
    bindSegmListClicks();
  }

  // ---------------- Segment focus ----------------
  // Try to auto-bind: rows must carry data-start and data-end (ISO ts or ms)
  function bindSegmListClicks() {
    if (State.segmListBound) return;
    const table = document.querySelector('#segmTable') || document.querySelector('.segm-table');
    if (!table) return;

    table.addEventListener('click', (e) => {
      const tr = e.target.closest('tr');
      if (!tr) return;
      const s = tr.getAttribute('data-start') || tr.dataset.start;
      const eend = tr.getAttribute('data-end') || tr.dataset.end;
      if (!s || !eend) return;

      const startMs = isFinite(+s) ? +s : Date.parse(s);
      const endMs   = isFinite(+eend) ? +eend : Date.parse(eend);
      if (!isFinite(startMs) || !isFinite(endMs)) return;

      focusWindow(startMs, endMs);
    });

    State.segmListBound = true;
  }

  // Public way to focus: call from your existing row handler if you prefer
  function focusWindow(xMin, xMax) {
    State.xMin = xMin;
    State.xMax = xMax;

    const chart = getChart();
    if (!chart) return;

    chart.setOption({
      xAxis: [{ min: xMin, max: xMax }]
    });
    refitYAxes();
  }

  // ---------------- Public API ----------------
  window.ReviewCore = {
    setSegmData(segmObj) {
      State.data.segm = segmObj;
      // If the segm object includes start_ts/end_ts, adopt them on first load so clicking the row isn’t mandatory
      if (segmObj?.start_ts && segmObj?.end_ts) {
        const s = Date.parse(segmObj.start_ts);
        const e = Date.parse(segmObj.end_ts);
        if (isFinite(s) && isFinite(e)) {
          State.xMin = s; State.xMax = e;
        }
      }
      render();
    },
    setLayerData(name, rows) {
      State.data.layers[name] = rows || [];
      render();
    },
    focusSegment(startTs, endTs) { // optional external call
      const s = isFinite(+startTs) ? +startTs : Date.parse(startTs);
      const e = isFinite(+endTs) ? +endTs : Date.parse(endTs);
      if (isFinite(s) && isFinite(e)) focusWindow(s, e);
    },
    init() {
      ensureExtraToggles();
      wireChunkInput();
      render();
    }
  };

  if (document.readyState === 'complete' || document.readyState === 'interactive') {
    setTimeout(() => window.ReviewCore.init(), 0);
  } else {
    document.addEventListener('DOMContentLoaded', () => window.ReviewCore.init());
  }
})();