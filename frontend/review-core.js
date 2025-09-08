// frontend/review-core.js
// Drop-in: adds "No thinning", "Show ticks", and fixes ATR1 visibility via a 2nd y-axis.
// Assumes existing APIs and data shapes (ticks with {id, ts, mid,...}, optional atr1 layer data).
// No route names changed.

(() => {
  // --- Simple DOM helpers ---------------------------------------------------
  const $ = (sel, root = document) => root.querySelector(sel);
  const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

  // --- State ----------------------------------------------------------------
  const State = {
    chunk: 2000,          // controlled by your existing Chunk input
    noThinning: false,    // new: "No thinning" toggle
    showTicks: true,      // new: show/hide base tick/mid series
    // layer flags are still driven by your existing checkboxes (e.g., 'atr1')
    data: {
      segm: null,         // last loaded segment object (with .ticks)
      layers: {}          // layerName -> array of {ts, value, ...}
    },
    echarts: null
  };

  // --- UI wiring ------------------------------------------------------------
  function ensureExtraToggles() {
    // Attach into the same "Layers:" row you already render
    const layersRow = document.querySelector('.layers-row') || document.body; // fallback

    // No thinning
    if (!$('#toggleNoThin')) {
      const lbl = document.createElement('label');
      lbl.style.marginLeft = '12px';
      lbl.title = 'Draw every point (no JS thinning; ECharts sampling disabled)';
      lbl.innerHTML = `<input type="checkbox" id="toggleNoThin" /> No thinning`;
      layersRow.appendChild(lbl);
      $('#toggleNoThin').addEventListener('change', (e) => {
        State.noThinning = e.target.checked;
        render();
      });
    }

    // Show ticks
    if (!$('#toggleShowTicks')) {
      const lbl = document.createElement('label');
      lbl.style.marginLeft = '12px';
      lbl.title = 'Show/hide base tick (mid) line';
      lbl.innerHTML = `<input type="checkbox" id="toggleShowTicks" checked /> Show ticks`;
      layersRow.appendChild(lbl);
      $('#toggleShowTicks').addEventListener('change', (e) => {
        State.showTicks = e.target.checked;
        render();
      });
    }
  }

  // Expect an <input> for chunk already exists; keep listening to it
  function wireChunkInput() {
    const el = document.querySelector('#chunkInput') || document.querySelector('input[name="chunk"]');
    if (!el) return;
    el.addEventListener('change', () => {
      const v = parseInt(el.value, 10);
      if (!Number.isNaN(v) && v > 0) {
        State.chunk = v;
        if (!State.noThinning) render();
      }
    });
    // Initialize from current UI
    const v = parseInt(el.value, 10);
    if (!Number.isNaN(v) && v > 0) State.chunk = v;
  }

  // --- Downsampling ---------------------------------------------------------
  function thinToChunk(points, chunk) {
    if (!points || points.length <= chunk) return points;
    const stride = Math.ceil(points.length / chunk);
    const out = [];
    for (let i = 0; i < points.length; i += stride) out.push(points[i]);
    // Make sure last point is included (cursor/tooltip correctness)
    if (out[out.length - 1] !== points[points.length - 1]) out.push(points[points.length - 1]);
    return out;
  }

  // --- Data adapters --------------------------------------------------------
  function mapTicksToSeries(segm) {
    // segm.ticks: [{id, ts, mid, ...}]
    const pts = (segm?.ticks || []).map(t => [new Date(t.ts).getTime(), t.mid]);
    return pts;
  }

  // If your ATR data comes in a separate payload, make sure State.data.layers.atr1 = [{ts, value}, ...]
  function mapAtrToSeries(arr) {
    return (arr || []).map(r => [new Date(r.ts).getTime(), r.value ?? r.atr ?? r.atr1]);
  }

  // --- ECharts init ---------------------------------------------------------
  function getChart() {
    if (State.echarts) return State.echarts;
    const el = document.getElementById('chart') || document.getElementById('reviewChart');
    if (!el) return null;
    State.echarts = echarts.init(el);
    window.addEventListener('resize', () => State.echarts && State.echarts.resize());
    return State.echarts;
  }

  // --- Main render ----------------------------------------------------------
  function render() {
    const chart = getChart();
    if (!chart) return;

    // Source series
    let tickSeries = mapTicksToSeries(State.data.segm);
    let atrSeries  = mapAtrToSeries(State.data.layers['atr1']);

    // Thinning toggle
    if (!State.noThinning) {
      tickSeries = thinToChunk(tickSeries, State.chunk);
      atrSeries  = thinToChunk(atrSeries,  State.chunk);
    }

    // Axis build: left for price, right for ATR
    const yAxes = [
      {
        type: 'value',
        scale: true,
        axisLabel: { formatter: v => v.toFixed(2) },
        name: 'Price'
      },
      {
        type: 'value',
        scale: true,
        axisLabel: { formatter: v => v.toFixed(3) },
        name: 'ATR',
        position: 'right'
      }
    ];

    // Series options
    const series = [];

    if (State.showTicks) {
      series.push({
        name: 'mid',
        type: 'line',
        showSymbol: false,
        yAxisIndex: 0,
        data: tickSeries,
        // When noThinning: disable sampling; also allow very large datasets
        sampling: State.noThinning ? undefined : 'lttb',
        large: true,
        largeThreshold: 200000
      });
    }

    // Only add ATR if user checked the layer checkbox (keeps your existing UI logic)
    const atrLayerCheckbox = document.querySelector('input[type=checkbox][data-layer="atr1"]')
      || document.querySelector('input[type=checkbox][name="atr1"]');
    const atrWanted = atrLayerCheckbox ? atrLayerCheckbox.checked : true; // default show if toggled "Layers: atr1" exists

    if (atrWanted && atrSeries && atrSeries.length) {
      series.push({
        name: 'atr1',
        type: 'line',
        showSymbol: false,
        yAxisIndex: 1,              // <- right axis so it’s visible
        data: atrSeries,
        sampling: State.noThinning ? undefined : 'lttb',
        step: false,
        smooth: false,
        large: true,
        largeThreshold: 200000,
        lineStyle: { width: 1.5 }
      });
    }

    const option = {
      animation: false,
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'cross' },
        valueFormatter: (v) => (typeof v === 'number' ? v.toFixed(5) : v)
      },
      xAxis: {
        type: 'time'
      },
      yAxis: yAxes,
      legend: { show: true },
      dataZoom: [
        { type: 'inside', throttle: 0 },
        { type: 'slider', height: 20 }
      ],
      series
    };

    chart.setOption(option, true);
  }

  // --- Public hooks you likely already call --------------------------------
  // Call this after fetching /api/segm (and any extra layers) as before.
  // Keep your existing fetch code; just make sure to fill State.data.segm and State.data.layers['atr1'].
  window.ReviewCore = {
    setSegmData(segmObj) {
      State.data.segm = segmObj;
      render();
    },
    setLayerData(name, rows) {
      State.data.layers[name] = rows || [];
      render();
    },
    init() {
      ensureExtraToggles();
      wireChunkInput();
      render();
    }
  };

  // Auto-init if chart exists
  if (document.readyState === 'complete' || document.readyState === 'interactive') {
    setTimeout(() => window.ReviewCore.init(), 0);
  } else {
    document.addEventListener('DOMContentLoaded', () => window.ReviewCore.init());
  }
})();