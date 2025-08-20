// review-core.js — split-pane ML Review with fixed zoom on "Load more" and left-pinned Jump
(() => {
  const $  = (s, el=document) => el.querySelector(s);
  const $$ = (s, el=document) => Array.from(el.querySelectorAll(s));

  // UI refs
  const startInput = $('#startInput');
  const limitInput = $('#limitInput');
  const btnLoad    = $('#btnLoad');
  const btnMore    = $('#btnMore');
  const jumpInput  = $('#jumpInput');
  const btnJump    = $('#btnJump');

  const chkRaw     = $('#chkRaw');
  const chkKalman  = $('#chkKalman');
  const chkProb    = $('#chkProb');
  const chkLabels  = $('#chkLabels');
  const btnConfirm = $('#btnConfirm');

  const titleText  = $('#titleText');
  const rangeInfo  = $('#rangeInfo');
  const kLoaded    = $('#kLoaded');
  const kRun       = $('#kRun');
  const kTrain     = $('#kTrain');
  const kTest      = $('#kTest');

  const chartEl    = document.getElementById('chart');

  // State
  const state = {
    start: 1,         // training start
    offset: 0,        // current offset within [1..200000]
    limit:  5000,     // chunk size
    leftAnchor: null, // x0 of current viewport
    viewWidth: null,  // x1-x0 of current viewport
    traces: { raw:true, kalman:true, prob:false, labels:false },
    data:   { x:[], raw:[], kalman:[], prob:[], labels:[] },
    run: null,        // {run_id, confirmed, model_id}
  };

  // -------------- helpers --------------
  const api = {
    review: (start, offset, limit) =>
      `/ml/review?start=${start}&offset=${offset}&limit=${limit}`,
    confirm: (runId) => `/ml/confirm?run_id=${encodeURIComponent(runId)}`
  };

  async function getJSON(url) {
    const r = await fetch(url);
    if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
    return r.json();
  }

  function pick(val, ...keys) {
    for (const k of keys) if (k in val) return val[k];
    return undefined;
  }

  // The review bundle can vary; extract series defensively.
  function parseBundle(bundle) {
    // x-axis = local tick ids (1..200000 within this window)
    // Try common container keys
    const rawArr    = bundle.ticks || bundle.raw || bundle.ml_ticks || [];
    const kalArr    = bundle.kalman || bundle.kalman_states || [];
    const predArr   = bundle.predictions || bundle.preds || [];
    const labelsArr = bundle.labels || bundle.trend_labels || [];

    const X = [];
    const RAW = [];
    const KAL = [];
    const PROB = [];
    const LABS = [];

    // Raw mid/price
    for (const r of rawArr) {
      const x = pick(r, 'tickid', 'id', 'x');
      const p = pick(r, 'mid', 'price', 'p', 'y');
      if (x != null && p != null) { X.push(x); RAW.push(p); }
    }

    // Kalman level/price
    const xK=[], yK=[];
    for (const k of kalArr) {
      const x = pick(k, 'tickid', 'id', 'x');
      const v = pick(k, 'level', 'price', 'y');
      if (x != null && v != null) { xK.push(x); yK.push(v); }
    }

    // Probability p_up
    const xP=[], yP=[];
    for (const p of predArr) {
      const x = pick(p, 'tickid', 'id', 'x');
      const pu = pick(p, 'p_up', 'prob', 'p');
      if (x != null && pu != null) { xP.push(x); yP.push(pu); }
    }

    // Labels (just points at x; use y from RAW/KAL if exists)
    for (const l of labelsArr) {
      const x = pick(l, 'tickid', 'id', 'x');
      if (x != null) LABS.push(x);
    }

    return {
      x: X.length ? X : xK.length ? xK : xP, // prefer raw X
      raw: RAW,
      kalman: { x: xK, y: yK },
      prob:   { x: xP, y: yP },
      labels: LABS
    };
  }

  function appendData(dst, src) {
    // assumes non-overlapping increasing x
    dst.x.push(...src.x);
    dst.raw.push(...src.raw);
    dst.kalman.x.push(...src.kalman.x);
    dst.kalman.y.push(...src.kalman.y);
    dst.prob.x.push(...src.prob.x);
    dst.prob.y.push(...src.prob.y);
    dst.labels.push(...src.labels);
  }

  // -------------- chart --------------
  function drawChart() {
    const d = state.data;
    const traces = [];

    if (state.traces.raw && d.x.length) {
      traces.push({
        type:'scattergl', mode:'markers',
        name:'Raw', x:d.x, y:d.raw,
        marker:{ size:3 },
        hovertemplate:'tick %{x}<br>raw %{y:.5f}<extra></extra>'
      });
    }
    if (state.traces.kalman && d.kalman.x.length) {
      traces.push({
        type:'scattergl', mode:'lines',
        name:'Kalman', x:d.kalman.x, y:d.kalman.y,
        line:{ width:2 },
        hovertemplate:'tick %{x}<br>kalman %{y:.5f}<extra></extra>'
      });
    }
    if (state.traces.prob && d.prob.x.length) {
      traces.push({
        type:'scattergl', mode:'lines',
        name:'p_up', x:d.prob.x, y:d.prob.y,
        yaxis:'y2', line:{ width:1 },
        hovertemplate:'tick %{x}<br>p_up %{y:.3f}<extra></extra>'
      });
    }

    // Layout
    const layout = {
      dragmode:'pan',
      margin:{l:60,r:60,t:10,b:30},
      paper_bgcolor:'#0d0f12',
      plot_bgcolor:'#0d0f12',
      font:{color:'#e6e6e6'},
      xaxis:{ showgrid:true, gridcolor:'#1f2633', title:'tick' },
      yaxis:{ showgrid:true, gridcolor:'#1f2633', title:'price' },
      yaxis2:{ overlaying:'y', side:'right', rangemode:'tozero', title:'prob' }
    };

    // Preserve zoom if we have it
    if (state.leftAnchor != null && state.viewWidth != null) {
      layout.xaxis.range = [state.leftAnchor, state.leftAnchor + state.viewWidth];
    }

    Plotly.newPlot(chartEl, traces, layout, {displayModeBar:false, responsive:true});

    chartEl.on('plotly_relayout', ev => {
      const r0 = ev['xaxis.range[0]'], r1 = ev['xaxis.range[1]'];
      if (typeof r0 === 'number' && typeof r1 === 'number') {
        state.leftAnchor = r0;
        state.viewWidth  = Math.max(1, r1 - r0);
      }
    });

    // Header info
    if (d.x.length) {
      const first = d.x[0];
      const last  = d.x[d.x.length - 1];
      rangeInfo.textContent = `Loaded ${first}…${last}`;
      kLoaded.textContent = `${first}…${last}`;
    } else {
      rangeInfo.textContent = '–';
      kLoaded.textContent = '–';
    }
  }

  // -------------- load/jump/more --------------
  async function doLoad({ reset=false } = {}) {
    state.start = parseInt(startInput.value, 10) || 1;
    state.limit = Math.min(Math.max(parseInt(limitInput.value || '5000', 10), 100), 20000);

    if (reset) {
      state.offset = 0;
      state.data   = { x:[], raw:[], kalman:{x:[],y:[]}, prob:{x:[],y:[]}, labels:[] };
      state.leftAnchor = null; state.viewWidth = null;
    }

    const url = api.review(state.start, state.offset, state.limit);
    const bundle = await getJSON(url);

    // Attach run info if present
    state.run = bundle.run || null;
    if (state.run) {
      kRun.textContent   = `${state.run.run_id}${state.run.confirmed ? ' ✓' : ''}`;
      kTrain.textContent = `${state.start}…${state.start + 100000 - 1}`;
      kTest.textContent  = `${state.start + 100000}…${state.start + 200000 - 1}`;
    } else {
      kRun.textContent = '–'; kTrain.textContent = '–'; kTest.textContent = '–';
    }

    // Parse and append
    const parsed = parseBundle(bundle);
    if (state.data.x.length === 0) {
      state.data = { x:[], raw:[], kalman:{x:[],y:[]}, prob:{x:[],y:[]}, labels:[] };
    }
    appendData(state.data, parsed);

    // If no zoom yet, set a sensible window
    if (state.leftAnchor == null && parsed.x.length) {
      const x0 = parsed.x[0];
      const width = Math.max(500, Math.round(state.limit * 0.9));
      state.leftAnchor = x0;
      state.viewWidth  = width;
    }

    drawChart();
  }

  async function doMore() {
    // Keep current zoom: leftAnchor/viewWidth stay as-is
    state.offset += state.limit;
    await doLoad({ reset:false });
  }

  async function doJump() {
    const j = Math.max(1, parseInt(jumpInput.value, 10) || 1);
    state.offset = Math.max(0, j - 1);  // left pin
    // Reset data but preserve desired leftAnchor = jump id
    state.data = { x:[], raw:[], kalman:{x:[],y:[]}, prob:{x:[],y:[]}, labels:[] };
    state.leftAnchor = j;
    // Keep current viewWidth if set; otherwise compute after load
    await doLoad({ reset:false });
  }

  // -------------- confirm --------------
  async function doConfirm() {
    if (!state.run?.run_id) return;
    try {
      const r = await fetch(api.confirm(state.run.run_id), { method:'POST' });
      if (!r.ok) throw new Error(await r.text());
      kRun.textContent = `${state.run.run_id} ✓`;
    } catch (e) {
      alert('Confirm failed: ' + (e.message || e));
    }
  }

  // -------------- wire UI --------------
  btnLoad.addEventListener('click', () => doLoad({ reset:true }).catch(console.error));
  btnMore.addEventListener('click', () => doMore().catch(console.error));
  btnJump.addEventListener('click', () => doJump().catch(console.error));
  btnConfirm.addEventListener('click', () => doConfirm().catch(console.error));

  chkRaw   .addEventListener('change', () => { state.traces.raw    = chkRaw.checked;   drawChart(); });
  chkKalman.addEventListener('change', () => { state.traces.kalman = chkKalman.checked;drawChart(); });
  chkProb  .addEventListener('change', () => { state.traces.prob   = chkProb.checked;  drawChart(); });
  chkLabels.addEventListener('change', () => { state.traces.labels = chkLabels.checked;drawChart(); });

  // -------------- first render --------------
  // Autoload initial view
  doLoad({ reset:true }).catch(err => { console.error(err); });
})();
