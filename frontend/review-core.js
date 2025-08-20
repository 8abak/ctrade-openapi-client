// review-core.js — zoom-scoped y-range, wheel zoom, axis drag scaling, rich tooltips
(() => {
  const $  = (s, el=document) => el.querySelector(s);

  // UI
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

  const rangeInfo  = $('#rangeInfo');
  const kLoaded    = $('#kLoaded');
  const kRun       = $('#kRun');
  const kTrain     = $('#kTrain');
  const kTest      = $('#kTest');

  const chartEl    = document.getElementById('chart');

  // State
  const state = {
    start: 1,
    offset: 0,
    limit:  5000,
    leftAnchor: null,     // x0 of view
    viewWidth:  null,     // x1 - x0
    manualYLock: false,   // set when user resizes Y via axis drag
    traces: { raw:true, kalman:true, prob:false, labels:false },
    data:   { x:[], raw:[], ts:[], kalman:{x:[],y:[]}, prob:{x:[],y:[]}, labelsRaw:[] },
    run: null,
  };

  // API helpers
  const api = {
    review: (start, offset, limit) =>
      `/ml/review?start=${start}&offset=${offset}&limit=${limit}`,
    confirm: (runId) => `/ml/confirm?run_id=${encodeURIComponent(runId)}`
  };
  async function getJSON(url){ const r = await fetch(url); if(!r.ok) throw new Error(`${r.status} ${r.statusText}`); return r.json(); }
  const pick = (o, ...ks) => { for (const k of ks) if (k in o) return o[k]; };

  // ---------- Parse server bundle defensively ----------
  function parseBundle(b){
    const rawArr    = b.ticks || b.raw || b.ml_ticks || [];
    const kalArr    = b.kalman || b.kalman_states || [];
    const predArr   = b.predictions || b.preds || [];
    const labelsArr = b.labels || b.trend_labels || [];

    const X=[], RAW=[], TS=[], KX=[], KY=[], PX=[], PY=[], LABS=[];
    for(const r of rawArr){
      const x=pick(r,'tickid','id','x'); const p=pick(r,'mid','price','p','y');
      const ts=pick(r,'timestamp','ts');
      if(x!=null&&p!=null){X.push(x); RAW.push(p); TS.push(ts??null);}
    }
    for(const k of kalArr){
      const x=pick(k,'tickid','id','x'); const v=pick(k,'level','price','y');
      if(x!=null&&v!=null){KX.push(x); KY.push(v);}
    }
    for(const p of predArr){
      const x=pick(p,'tickid','id','x'); const v=pick(p,'p_up','prob','p');
      if(x!=null&&v!=null){PX.push(x); PY.push(v);}
    }
    for(const l of labelsArr){
      // store raw label object; we will render a readable summary later
      LABS.push(l);
    }
    return { x:X, raw:RAW, ts:TS, kalman:{x:KX,y:KY}, prob:{x:PX,y:PY}, labelsRaw:LABS };
  }

  function appendData(dst, src){
    dst.x.push(...src.x);
    dst.raw.push(...src.raw);
    dst.ts.push(...src.ts);
    dst.kalman.x.push(...src.kalman.x); dst.kalman.y.push(...src.kalman.y);
    dst.prob.x.push(...src.prob.x);     dst.prob.y.push(...src.prob.y);
    dst.labelsRaw.push(...src.labelsRaw);
  }

  // ---------- Helpers for ranges ----------
  function lowerBound(arr, target){
    let lo=0, hi=arr.length;
    while(lo<hi){ const mid=(lo+hi>>1); if(arr[mid] < target) lo=mid+1; else hi=mid; }
    return lo;
  }
  function upperBound(arr, target){
    let lo=0, hi=arr.length;
    while(lo<hi){ const mid=(lo+hi>>1); if(arr[mid] <= target) lo=mid+1; else hi=mid; }
    return lo;
  }
  function minmaxSlice(x, y, x0, x1){
    if(!x?.length || !y?.length) return null;
    const s = lowerBound(x, x0);
    const e = upperBound(x, x1);
    if(e - s <= 0) return null;
    let mn=Infinity, mx=-Infinity;
    for(let i=s;i<e;i++){ const v=y[i]; if(v<mn) mn=v; if(v>mx) mx=v; }
    if(!isFinite(mn) || !isFinite(mx)) return null;
    if(mx === mn){ const pad = (Math.abs(mx) || 1)*0.01; mn -= pad; mx += pad; }
    const span = mx - mn;
    return [mn - span*0.05, mx + span*0.05];
  }

  // ---------- Tooltip plumbing ----------
  function fmtDate(ts){
    if(!ts) return ['—','—'];
    const d = new Date(ts);
    if(isNaN(d)) return [String(ts), ''];
    const pad = n=>String(n).padStart(2,'0');
    const date = `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}`;
    const time = `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
    return [date,time];
  }

  function buildMapsForHover(){
    // Map tickid -> {ts, kal, labelText}
    const map = new Map();
    for(let i=0;i<state.data.x.length;i++){
      map.set(state.data.x[i], { ts: state.data.ts[i] || null, raw: state.data.raw[i] });
    }
    for(let i=0;i<state.data.kalman.x.length;i++){
      const x = state.data.kalman.x[i];
      const y = state.data.kalman.y[i];
      const m = map.get(x) || {};
      m.kal = y;
      map.set(x, m);
    }
    // Build label text per tick if label objects exist
    const labByTick = new Map();
    for(const l of state.data.labelsRaw){
      const x = pick(l,'tickid','id','x');
      if(x==null) continue;
      // make a compact readable string from label object (excluding id-ish keys)
      const keys = Object.keys(l).filter(k=>!['tickid','id','x','timestamp','ts'].includes(k));
      const parts = keys.map(k=>`${k}:${l[k]}`);
      const text  = parts.join(', ');
      if(!labByTick.has(x)) labByTick.set(x, text);
      else if (text) labByTick.set(x, labByTick.get(x) + '; ' + text);
    }
    for(const [x, t] of labByTick.entries()){
      const m = map.get(x) || {};
      m.label = t;
      map.set(x, m);
    }
    return map;
  }

  // ---------- Chart ----------
  function currentXRange(){
    if(state.leftAnchor!=null && state.viewWidth!=null){
      return [state.leftAnchor, state.leftAnchor + state.viewWidth];
    }
    const fl = chartEl._fullLayout;
    if(fl?.xaxis?.range) return fl.xaxis.range.slice();
    return null;
  }

  function updateYForCurrentView(){
    if(state.manualYLock) return; // user is controlling Y scale manually
    const xr = currentXRange();
    if(!xr) return;
    const [x0,x1] = xr;
    const d = state.data;

    let rngPrice = null;
    if(state.traces.raw)    rngPrice = minmaxSlice(d.x, d.raw, x0, x1) || rngPrice;
    if(state.traces.kalman){
      const r = minmaxSlice(d.kalman.x, d.kalman.y, x0, x1);
      if(r) rngPrice = rngPrice ? [Math.min(rngPrice[0], r[0]), Math.max(rngPrice[1], r[1])] : r;
    }

    let rngProb = null;
    if(state.traces.prob) rngProb = minmaxSlice(d.prob.x, d.prob.y, x0, x1);

    const rel = {};
    if(rngPrice){ rel['yaxis.autorange']=false; rel['yaxis.range']=rngPrice; } else { rel['yaxis.autorange']=true; }
    if(rngProb){ rel['yaxis2.autorange']=false; rel['yaxis2.range']=rngProb; } else { rel['yaxis2.autorange']=true; }
    if(Object.keys(rel).length) Plotly.relayout(chartEl, rel);
  }

  function drawChart(){
    const d = state.data;
    const maps = buildMapsForHover();

    // Build customdata for RAW points (date, time, kalman at same id, labels text)
    const cdRaw = d.x.map(x => {
      const m = maps.get(x) || {};
      const [date,time] = fmtDate(m.ts);
      return [date, time, m.kal ?? null, m.label ?? ''];
    });

    const traces = [];
    if(state.traces.raw && d.x.length){
      traces.push({
        type:'scattergl', mode:'markers', name:'Raw',
        x:d.x, y:d.raw,
        marker:{ size:3 },
        customdata: cdRaw,
        hovertemplate:
          'date: %{customdata[0]}<br>' +
          'time: %{customdata[1]}<br>' +
          'id: %{x}<br>' +
          'price: %{y:.5f}<br>' +
          'kalman: %{customdata[2]:.5f}<br>' +
          '%{customdata[3]}<extra></extra>'
      });
    }
    if(state.traces.kalman && d.kalman.x.length){
      traces.push({
        type:'scattergl', mode:'lines', name:'Kalman',
        x:d.kalman.x, y:d.kalman.y,
        line:{ width:2 },
        hovertemplate: 'id: %{x}<br>kalman: %{y:.5f}<extra></extra>'
      });
    }
    if(state.traces.prob && d.prob.x.length){
      traces.push({
        type:'scattergl', mode:'lines', name:'p_up',
        x:d.prob.x, y:d.prob.y, yaxis:'y2',
        line:{ width:1 },
        hovertemplate:'id: %{x}<br>p_up: %{y:.3f}<extra></extra>'
      });
    }

    const layout = {
      uirevision: 'ml-review-v1',   // preserve zoom when data updates
      dragmode:'pan',
      margin:{l:60,r:60,t:30,b:30},
      paper_bgcolor:'#0d0f12',
      plot_bgcolor:'#0d0f12',
      font:{color:'#e6e6e6'},
      hovermode:'x unified',
      hoverlabel:{bgcolor:'#101520', bordercolor:'#253044'},
      xaxis:{
        title:'tick', showgrid:true, gridcolor:'#1f2633',
        showspikes:true, spikemode:'across', spikethickness:1, spikecolor:'#9aa4b2',
      },
      yaxis:{
        title:'price', showgrid:true, gridcolor:'#1f2633',
        showspikes:true, spikemode:'across', spikethickness:1, spikecolor:'#9aa4b2',
        autorange:true
      },
      yaxis2:{
        overlaying:'y', side:'right', rangemode:'tozero',
        title:'prob', showspikes:true, spikethickness:1, spikecolor:'#9aa4b2',
        autorange:true
      },
      legend:{ orientation:'v', x:1.02, xanchor:'left', y:1 }
    };

    if(state.leftAnchor!=null && state.viewWidth!=null){
      layout.xaxis.range = [state.leftAnchor, state.leftAnchor + state.viewWidth];
    }

    Plotly.newPlot(chartEl, traces, layout, {
      displayModeBar:false,
      responsive:true,
      scrollZoom:true   // wheel zoom enabled
    });

    // Keep y-range tied to current x-range
    const updateNow = () => updateYForCurrentView();
    chartEl.on('plotly_relayout', updateNow);
    chartEl.on('plotly_relayouting', updateNow); // live while panning/zooming

    // Header info
    if (d.x.length){
      const first=d.x[0], last=d.x[d.x.length-1];
      rangeInfo.textContent = `Loaded ${first}…${last}`;
      kLoaded.textContent   = `${first}…${last}`;
    } else {
      rangeInfo.textContent = '–'; kLoaded.textContent='–';
    }

    // Axis-drag scaling (TradingView-like)
    enableAxisDragScaling();
    // Initial y-range for the current view
    updateYForCurrentView();
  }

  // ---------- Axis drag scaling ----------
  function enableAxisDragScaling(){
    let dragging = null; // 'y' | 'x' | null
    let start = null;

    const onDown = (e) => {
      const fl = chartEl._fullLayout; if(!fl) return;
      const rect = chartEl.getBoundingClientRect();
      const x = e.clientX - rect.left;
      const y = e.clientY - rect.top;
      const L = fl._size.l, T = fl._size.t, W = fl._size.w, H = fl._size.h;
      const rightZone  = x > (L + W) && x < rect.width;   // over right margin → y-axis
      const bottomZone = y > (T + H) && y < rect.height;  // over bottom margin → x-axis
      if (rightZone) {
        dragging = 'y';
        start = {y0: y, range: (chartEl._fullLayout.yaxis.range||[]).slice()};
        state.manualYLock = true; // user is adjusting Y manually
        e.preventDefault();
      } else if (bottomZone) {
        dragging = 'x';
        start = {x0: x, range: (chartEl._fullLayout.xaxis.range||[]).slice()};
        e.preventDefault();
      }
    };
    const onMove = (e) => {
      if(!dragging || !start) return;
      const fl = chartEl._fullLayout;
      const rect = chartEl.getBoundingClientRect();
      if(dragging==='y'){
        const dy = e.clientY - start.y0; // drag up → negative → zoom in
        const factor = Math.pow(1.0025, dy); // sensitivity
        const [a,b] = start.range;
        const mid = (a+b)/2;
        const half = (b-a)/2 * factor;
        const newRange = [mid - half, mid + half];
        Plotly.relayout(chartEl, {'yaxis.autorange': false, 'yaxis.range': newRange});
      } else if (dragging==='x'){
        const dx = e.clientX - start.x0; // drag right → positive → zoom out
        const factor = Math.pow(1.0025, -dx); // invert so right drag zooms out
        const [a,b] = start.range;
        const mid = (a+b)/2;
        const half = (b-a)/2 * factor;
        const newRange = [mid - half, mid + half];
        state.leftAnchor = newRange[0];
        state.viewWidth  = newRange[1]-newRange[0];
        Plotly.relayout(chartEl, {'xaxis.range': newRange});
      }
    };
    const onUp = () => { dragging=null; start=null; };

    chartEl.addEventListener('mousedown', onDown);
    window.addEventListener('mousemove', onMove);
    window.addEventListener('mouseup', onUp);
  }

  // ---------- Load / More / Jump ----------
  async function doLoad({ reset=false } = {}){
    state.start = parseInt(startInput.value,10) || 1;
    state.limit = Math.min(Math.max(parseInt(limitInput.value||'5000',10),100),20000);

    if(reset){
      state.offset=0;
      state.data   = { x:[], raw:[], ts:[], kalman:{x:[],y:[]}, prob:{x:[],y:[]}, labelsRaw:[] };
      state.leftAnchor=null; state.viewWidth=null;
      state.manualYLock = false;
    }

    const bundle = await getJSON(api.review(state.start, state.offset, state.limit));
    state.run = bundle.run || null;
    if(state.run){
      kRun.textContent=`${state.run.run_id}${state.run.confirmed?' ✓':''}`;
      kTrain.textContent=`${state.start}…${state.start+100000-1}`;
      kTest.textContent =`${state.start+100000}…${state.start+200000-1}`;
    } else {
      kRun.textContent='–'; kTrain.textContent='–'; kTest.textContent='–';
    }

    const parsed = parseBundle(bundle);
    if(state.data.x.length===0){
      state.data = { x:[], raw:[], ts:[], kalman:{x:[],y:[]}, prob:{x:[],y:[]}, labelsRaw:[] };
    }
    appendData(state.data, parsed);

    if(state.leftAnchor==null && parsed.x.length){
      const x0 = parsed.x[0];
      const width = Math.max(500, Math.round(state.limit*0.9));
      state.leftAnchor = x0; state.viewWidth = width;
    }

    drawChart();
  }

  async function doMore(){
    state.offset += state.limit;
    await doLoad({ reset:false });
  }

  async function doJump(){
    const j = Math.max(1, parseInt(jumpInput.value,10) || 1);
    state.offset = Math.max(0, j - 1);
    state.data = { x:[], raw:[], ts:[], kalman:{x:[],y:[]}, prob:{x:[],y:[]}, labelsRaw:[] };
    state.leftAnchor = j; // pin left edge to jump id
    state.manualYLock = false; // let y auto-fit to new view initially
    await doLoad({ reset:false });
  }

  async function doConfirm(){
    if(!state.run?.run_id) return;
    try{
      const r = await fetch(api.confirm(state.run.run_id), {method:'POST'});
      if(!r.ok) throw new Error(await r.text());
      kRun.textContent = `${state.run.run_id} ✓`;
    }catch(e){ alert('Confirm failed: ' + (e.message || e)); }
  }

  // ---------- Wire UI ----------
  btnLoad.addEventListener('click', () => doLoad({reset:true}).catch(console.error));
  btnMore.addEventListener('click', () => doMore().catch(console.error));
  btnJump.addEventListener('click', () => doJump().catch(console.error));
  btnConfirm.addEventListener('click', () => doConfirm().catch(console.error));

  chkRaw   .addEventListener('change', () => { state.traces.raw    = chkRaw.checked;    state.manualYLock=false; drawChart(); });
  chkKalman.addEventListener('change', () => { state.traces.kalman = chkKalman.checked; state.manualYLock=false; drawChart(); });
  chkProb  .addEventListener('change', () => { state.traces.prob   = chkProb.checked;   drawChart(); });
  chkLabels.addEventListener('change', () => { state.traces.labels = chkLabels.checked; drawChart(); });

  // First render
  doLoad({ reset:true }).catch(console.error);
})();
