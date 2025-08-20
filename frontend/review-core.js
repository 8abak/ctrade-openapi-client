// review-core.js — current-zoom-based y min/max
(() => {
  const $  = (s, el=document) => el.querySelector(s);

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
    leftAnchor: null,   // x0 of view
    viewWidth:  null,   // x1 - x0
    traces: { raw:true, kalman:true, prob:false, labels:false },
    data:   { x:[], raw:[], kalman:{x:[],y:[]}, prob:{x:[],y:[]}, labels:[] },
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

  // Parse server bundle defensively
  function parseBundle(b){
    const rawArr    = b.ticks || b.raw || b.ml_ticks || [];
    const kalArr    = b.kalman || b.kalman_states || [];
    const predArr   = b.predictions || b.preds || [];
    const labelsArr = b.labels || b.trend_labels || [];

    const X=[], RAW=[], KX=[], KY=[], PX=[], PY=[], LABS=[];
    for(const r of rawArr){ const x=pick(r,'tickid','id','x'); const p=pick(r,'mid','price','p','y'); if(x!=null&&p!=null){X.push(x); RAW.push(p);} }
    for(const k of kalArr){ const x=pick(k,'tickid','id','x'); const v=pick(k,'level','price','y'); if(x!=null&&v!=null){KX.push(x); KY.push(v);} }
    for(const p of predArr){ const x=pick(p,'tickid','id','x'); const v=pick(p,'p_up','prob','p');   if(x!=null&&v!=null){PX.push(x); PY.push(v);} }
    for(const l of labelsArr){ const x=pick(l,'tickid','id','x'); if(x!=null) LABS.push(x); }
    return { x: X.length?X:(KX.length?KX:PX), raw: RAW, kalman:{x:KX,y:KY}, prob:{x:PX,y:PY}, labels: LABS };
  }
  function appendData(dst, src){
    dst.x.push(...src.x);
    dst.raw.push(...src.raw);
    dst.kalman.x.push(...src.kalman.x); dst.kalman.y.push(...src.kalman.y);
    dst.prob.x.push(...src.prob.x);     dst.prob.y.push(...src.prob.y);
    dst.labels.push(...src.labels);
  }

  // ---------- Chart ----------
  function drawChart(){
    const d = state.data;
    const traces = [];
    if(state.traces.raw && d.x.length){
      traces.push({type:'scattergl',mode:'markers',name:'Raw',x:d.x,y:d.raw,marker:{size:3},hovertemplate:'tick %{x}<br>raw %{y:.5f}<extra></extra>'});
    }
    if(state.traces.kalman && d.kalman.x.length){
      traces.push({type:'scattergl',mode:'lines',name:'Kalman',x:d.kalman.x,y:d.kalman.y,line:{width:2},hovertemplate:'tick %{x}<br>kalman %{y:.5f}<extra></extra>'});
    }
    if(state.traces.prob && d.prob.x.length){
      traces.push({type:'scattergl',mode:'lines',name:'p_up',x:d.prob.x,y:d.prob.y,yaxis:'y2',line:{width:1},hovertemplate:'tick %{x}<br>p_up %{y:.3f}<extra></extra>'});
    }

    const layout = {
      dragmode:'pan', margin:{l:60,r:60,t:10,b:30},
      paper_bgcolor:'#0d0f12', plot_bgcolor:'#0d0f12', font:{color:'#e6e6e6'},
      xaxis:{ showgrid:true, gridcolor:'#1f2633', title:'tick' },
      yaxis:{ showgrid:true, gridcolor:'#1f2633', title:'price', autorange:true },
      yaxis2:{ overlaying:'y', side:'right', rangemode:'tozero', title:'prob', autorange:true },
    };

    if(state.leftAnchor!=null && state.viewWidth!=null){
      layout.xaxis.range = [state.leftAnchor, state.leftAnchor + state.viewWidth];
    }

    Plotly.newPlot(chartEl, traces, layout, {displayModeBar:false, responsive:true});

    chartEl.on('plotly_relayout', ev => {
      const r0 = ev['xaxis.range[0]'], r1 = ev['xaxis.range[1]'];
      if (typeof r0 === 'number' && typeof r1 === 'number') {
        state.leftAnchor = r0;
        state.viewWidth  = Math.max(1, r1 - r0);
        updateYForCurrentView();
      }
    });

    // Initial y-range set to match current view
    updateYForCurrentView();

    // Header info
    if (d.x.length){
      const first=d.x[0], last=d.x[d.x.length-1];
      rangeInfo.textContent = `Loaded ${first}…${last}`;
      kLoaded.textContent = `${first}…${last}`;
    } else {
      rangeInfo.textContent = '–'; kLoaded.textContent='–';
    }
  }

  // ---------- Current-zoom y-scaling ----------
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
    return [mn - span*0.05, mx + span*0.05]; // add 5% headroom
  }

  function updateYForCurrentView(){
    let x0, x1;
    if(state.leftAnchor!=null && state.viewWidth!=null){
      x0 = state.leftAnchor; x1 = state.leftAnchor + state.viewWidth;
    } else if (chartEl._fullLayout?.xaxis?.range){
      [x0, x1] = chartEl._fullLayout.xaxis.range;
    } else {
      return;
    }

    const d = state.data;
    // Price axis from visible RAW and/or KALMAN (whichever is toggled on)
    let rngPrice = null;
    if(state.traces.raw)    rngPrice = minmaxSlice(d.x, d.raw, x0, x1) || rngPrice;
    if(state.traces.kalman) {
      const r = minmaxSlice(d.kalman.x, d.kalman.y, x0, x1);
      if(r) rngPrice = rngPrice ? [Math.min(rngPrice[0], r[0]), Math.max(rngPrice[1], r[1])] : r;
    }

    // Prob axis from visible p_up
    let rngProb = null;
    if(state.traces.prob) rngProb = minmaxSlice(d.prob.x, d.prob.y, x0, x1);

    const rel = {};
    if(rngPrice){ rel['yaxis.autorange']=false; rel['yaxis.range']=rngPrice; }
    else { rel['yaxis.autorange']=true; }
    if(rngProb){ rel['yaxis2.autorange']=false; rel['yaxis2.range']=rngProb; }
    else { rel['yaxis2.autorange']=true; }

    if(Object.keys(rel).length){
      Plotly.relayout(chartEl, rel);
    }
  }

  // ---------- Load / More / Jump ----------
  async function doLoad({ reset=false } = {}){
    state.start = parseInt(startInput.value,10) || 1;
    state.limit = Math.min(Math.max(parseInt(limitInput.value||'5000',10),100),20000);

    if(reset){
      state.offset=0;
      state.data   = { x:[], raw:[], kalman:{x:[],y:[]}, prob:{x:[],y:[]}, labels:[] };
      state.leftAnchor=null; state.viewWidth=null;
    }

    const bundle = await getJSON(api.review(state.start, state.offset, state.limit));
    state.run = bundle.run || null;
    if(state.run){ kRun.textContent=`${state.run.run_id}${state.run.confirmed?' ✓':''}`; kTrain.textContent=`${state.start}…${state.start+100000-1}`; kTest.textContent=`${state.start+100000}…${state.start+200000-1}`; }
    else { kRun.textContent='–'; kTrain.textContent='–'; kTest.textContent='–'; }

    const parsed = parseBundle(bundle);
    if(state.data.x.length===0){
      state.data = { x:[], raw:[], kalman:{x:[],y:[]}, prob:{x:[],y:[]}, labels:[] };
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
    state.offset += state.limit;   // keep zoom; y-range will recompute from current x-range
    await doLoad({ reset:false });
  }

  async function doJump(){
    const j = Math.max(1, parseInt(jumpInput.value,10) || 1);
    state.offset = Math.max(0, j - 1);
    state.data = { x:[], raw:[], kalman:{x:[],y:[]}, prob:{x:[],y:[]}, labels:[] };
    state.leftAnchor = j; // pin left edge to jump id
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

  chkRaw   .addEventListener('change', () => { state.traces.raw    = chkRaw.checked;    drawChart(); });
  chkKalman.addEventListener('change', () => { state.traces.kalman = chkKalman.checked; drawChart(); });
  chkProb  .addEventListener('change', () => { state.traces.prob   = chkProb.checked;   drawChart(); });
  chkLabels.addEventListener('change', () => { state.traces.labels = chkLabels.checked; drawChart(); });

  // First render
  doLoad({ reset:true }).catch(console.error);
})();
