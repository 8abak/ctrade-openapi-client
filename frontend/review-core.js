// frontend/review-core.js — Walk-Forward UI (intraday-style)
// Drag-zoom: L/R → X zoom, U/D → Y zoom (cursor-anchored)
// Wheel + slider supported for X
// Y auto-fit recalculated after any zoom
// Y labels/grid are integers; series plot exact floats

(function () {
  const $ = (sel) => document.querySelector(sel);
  const asInt = (v) => (v == null ? '' : String(Math.round(Number(v))));
  const fmt2  = (v) => (v == null ? '-' : Number(v).toFixed(2));
  const JUMP_WINDOW = 6000;

  let chart;
  let currentTicks = [];  // [{id, timestamp, bid, ask, mid}]
  let currentSnap  = null;

  // lookup maps for tooltip enrichment
  let evByTick = new Map(), prByTick = new Map(), ocByTick = new Map();

  function setStatus(text, kind='info'){
    const el = $('#status'); if (!el) return;
    el.textContent = text;
    el.style.color = kind==='ok' ? '#7ee787' : kind==='err' ? '#ffa198' : '#8b949e';
  }
  function log(msg){
    const j = $('#journal'); if (!j) return;
    const ts = new Date().toISOString().replace('T',' ').replace('Z','');
    j.textContent += `[${ts}] ${msg}\n`;
    j.scrollTop = j.scrollHeight;
  }

  // journal toggle
  (function(){
    const btn = $('#toggleJournal'), j = $('#journal'), key='reviewJournalOpen';
    function apply(open){ if(open){ j.classList.remove('collapsed'); btn.textContent='Journal ▾';}
                          else    { j.classList.add('collapsed');    btn.textContent='Journal ▸';}
                          localStorage.setItem(key, open?'1':'0'); }
    apply(localStorage.getItem(key)==='1');
    btn.addEventListener('click', ()=> apply(j.classList.contains('collapsed')));
  })();

  async function sqlRange(startId, endId){
    const sql = `SELECT id, timestamp, bid, ask, mid FROM ticks WHERE id BETWEEN ${startId} AND ${endId} ORDER BY id`;
    const r = await fetch('/sqlvw/query?query=' + encodeURIComponent(sql));
    if(!r.ok) throw new Error(`ticks HTTP ${r.status}`); return r.json();
  }
  async function wfStep(){ const r = await fetch('/walkforward/step',{method:'POST'}); if(!r.ok) throw new Error(`step HTTP ${r.status}`); return r.json(); }
  async function wfSnap(){ const r = await fetch('/walkforward/snapshot'); if(!r.ok) throw new Error(`snap HTTP ${r.status}`); return r.json(); }

  const xy = (rows, x, y)=> rows.map(r=>[r[x], Number(r[y])]);

  function ensureChart(){
    if(chart) return chart;
    if(typeof echarts==='undefined'){ setStatus('ECharts not loaded','err'); return null; }
    chart = echarts.init($('#chart'), null, {renderer:'canvas'});
    return chart;
  }

  function enrichMaps(snap){
    evByTick.clear(); prByTick.clear(); ocByTick.clear();
    const ev = snap?.events || [];
    for(const e of ev){ if(!evByTick.has(e.tick_id)) evByTick.set(e.tick_id,[]); evByTick.get(e.tick_id).push(e); }
    const evById = new Map(ev.map(e=>[e.event_id, e]));
    for(const p of (snap?.predictions||[])){ const e=evById.get(p.event_id); if(!e) continue;
      if(!prByTick.has(e.tick_id)) prByTick.set(e.tick_id,[]); prByTick.get(e.tick_id).push({...p, event:e}); }
    for(const o of (snap?.outcomes||[])){ const e=evById.get(o.event_id); if(!e) continue;
      if(!ocByTick.has(e.tick_id)) ocByTick.set(e.tick_id,[]); ocByTick.get(e.tick_id).push({...o, event:e}); }
  }

  function baseOption(ticks, snap){
    const mid = xy(ticks,'id','mid'), bid = xy(ticks,'id','bid'), ask = xy(ticks,'id','ask');

    return {
      useUTC: true,
      backgroundColor: '#0d1117',
      animation: false,
      progressive: 4000, progressiveThreshold: 3000,
      textStyle: { color: '#c9d1d9' },
      color: ['#7aa6ff', '#7ad3ff', '#ffd37a', '#65cc9a', '#f27370', '#b981f5'],
      legend: { top: 6, textStyle: { color: '#aeb9cc' }, selectedMode: 'multiple' },
      grid: { left: 48, right: 20, top: 32, bottom: 64 },
      tooltip: {
        show: true, trigger: 'axis', axisPointer: { type: 'cross', snap: true },
        backgroundColor: '#101826', borderColor: '#26314a', textStyle: { color: '#dce6f2' },
        formatter: (params)=>{
          const id = Math.round(params[0].axisValue);
          const row = currentTicks.find(r=>r.id===id);
          const out = [];
          out.push(`<b>tick:</b> ${id}`);
          if(row?.timestamp){
            const t=new Date(row.timestamp);
            const dt=`${t.getUTCFullYear()}-${String(t.getUTCMonth()+1).padStart(2,'0')}-${String(t.getUTCDate()).padStart(2,'0')}`;
            const tm=`${String(t.getUTCHours()).padStart(2,'0')}:${String(t.getUTCMinutes()).padStart(2,'0')}:${String(t.getUTCSeconds()).padStart(2,'0')}`;
            out.push(`<b>date:</b> ${dt}  <b>time (UTC):</b> ${tm}`);
          }
          if(row){ out.push(`<b>Mid:</b> ${fmt2(row.mid)}  <b>Bid:</b> ${fmt2(row.bid)}  <b>Ask:</b> ${fmt2(row.ask)}`); }
          const evs = evByTick.get(id); if(evs) for(const e of evs) out.push(`• <b>Event</b> ${e.event_type}`);
          const prs = prByTick.get(id); if(prs) for(const p of prs) out.push(`• <b>Pred</b> p_tp=${(p.p_tp??0).toFixed(3)} τ=${(p.threshold??0).toFixed(3)} [${p.model_version||'-'}]`);
          const ocs = ocByTick.get(id); if(ocs) for(const o of ocs) out.push(`• <b>Outcome</b> ${o.outcome}`);
          return out.join('<br/>');
        }
      },
      xAxis: {
        type: 'value', name: 'tick',
        nameTextStyle: { color: '#8b949e' }, axisLabel: { color: '#8b949e' },
        axisLine: { lineStyle: { color: '#30363d' } },
        splitLine: { show: true, lineStyle: { color: '#21262d', type: 'dashed' } }
      },
      yAxis: {
        type: 'value',
        min: 'dataMin', max: 'dataMax',
        minInterval: 1, axisLabel: { color:'#8b949e', formatter:v=>asInt(v) },
        axisLine: { lineStyle: { color: '#30363d' } },
        splitLine: { show: true, lineStyle: { color: '#21262d' } }
      },
      dataZoom: [
        // inside X (wheel zoom + drag pan with Shift)
        { id:'dzx', type:'inside', xAxisIndex:0, filterMode:'none',
          zoomOnMouseWheel:true, moveOnMouseWheel:'shift', moveOnMouseMove:'shift', throttle:22 },
        { type:'slider', xAxisIndex:0, height:18, bottom:26, backgroundColor:'#0f1524', borderColor:'#2a3654' }
      ],
      series: [
        { name:'Mid', type:'line', smooth:0.15, showSymbol:false, sampling:'lttb', large:true, largeThreshold:10000, lineStyle:{width:1.3}, data:mid },
        { name:'Bid', type:'line', smooth:0.15, showSymbol:false, sampling:'lttb', large:true, largeThreshold:10000, lineStyle:{width:1.0, opacity:0.7}, data:bid },
        { name:'Ask', type:'line', smooth:0.15, showSymbol:false, sampling:'lttb', large:true, largeThreshold:10000, lineStyle:{width:1.0, opacity:0.7}, data:ask }
      ]
    };
  }

  function addLayers(opt, snap, startId, endId){
    const segs = (snap?.segments||[]).filter(s=>s.end_tick_id>=startId && s.start_tick_id<=endId);
    if(segs.length){
      const data = segs.map(s=>{
        const dir=s.direction>0?1:-1;
        const color = dir>0 ? `rgba(0,160,100,${0.10+0.20*(s.confidence??0.5)})`
                            : `rgba(200,50,60,${0.10+0.20*(s.confidence??0.5)})`;
        return [{xAxis:Math.max(s.start_tick_id,startId), yAxis:'min', itemStyle:{color}},
                {xAxis:Math.min(s.end_tick_id,endId),     yAxis:'max'}];
      });
      opt.series.push({name:'Macro', type:'line', data:[], markArea:{silent:true, itemStyle:{opacity:1}, data}});
    }

    const evsWin = (snap?.events||[]).filter(e=>e.tick_id>=startId && e.tick_id<=endId);
    if(evsWin.length){
      const sym = (t)=> t==='pullback_end'?'triangle': t==='breakout'?'diamond':'circle';
      const col = (t)=> t==='pullback_end'?'#58a6ff': t==='breakout'?'#f2cc60':'#b981f5';
      opt.series.push({name:'Events', type:'scatter', symbolSize:9,
        data: evsWin.map(e=>({value:[e.tick_id, Number(e.event_price)], symbol:sym(e.event_type), itemStyle:{color:col(e.event_type)}}))});
    }

    if((snap?.predictions||[]).length && evsWin.length){
      const evByIdWin = new Map(evsWin.map(e=>[e.event_id,e]));
      const rows = [];
      for(const p of snap.predictions){ const e=evByIdWin.get(p.event_id); if(!e) continue;
        rows.push({ value:[e.tick_id, Number(e.event_price)], p_tp:p.p_tp??null, threshold:p.threshold??null,
                    model_version:p.model_version??'', decided:!!p.decided, predicted_at:p.predicted_at }); }
      if(rows.length){
        opt.series.push({name:'Predictions', type:'scatter', symbol:'circle', symbolSize:8,
          itemStyle:{ color:(prm)=>{ const p=prm.data.p_tp??0, a=0.25+0.45*Math.max(0,Math.min(1,p));
                                     const g=Math.round(120+120*p), r=Math.round(60*(1-p)); return `rgba(${r},${g},120,${a})`; } },
          data:rows });
      }
    }

    if((snap?.outcomes||[]).length && evsWin.length){
      const evByIdWin = new Map(evsWin.map(e=>[e.event_id,e]));
      const rows=[];
      for(const o of snap.outcomes){ const e=evByIdWin.get(o.event_id); if(!e) continue;
        const c = o.outcome==='TP'?'#2ea043': o.outcome==='SL'?'#f85149':'#8b949e';
        rows.push({value:[e.tick_id, Number(e.event_price)], itemStyle:{color:c, borderColor:c}});
      }
      if(rows.length){
        opt.series.push({name:'Outcomes', type:'effectScatter', rippleEffect:{brushType:'stroke', scale:2.2}, symbolSize:11, showEffectOn:'render', data:rows});
      }
    }

    // legend -> toggles
    const sel={};
    sel['Mid']=$('#chkMid').checked; sel['Bid']=$('#chkBid').checked; sel['Ask']=$('#chkAsk').checked;
    sel['Macro']=$('#chkMacro').checked; sel['Events']=$('#chkEvents').checked;
    sel['Predictions']=$('#chkPreds').checked; sel['Outcomes']=$('#chkOutcomes').checked;
    opt.legend.selected = sel;
  }

  function applyLegendFromToggles(){
    const map={chkMid:'Mid',chkBid:'Bid',chkAsk:'Ask',chkMacro:'Macro',chkEvents:'Events',chkPreds:'Predictions',chkOutcomes:'Outcomes'};
    for(const id in map){ const el=$('#'+id); if(!el) continue; const n=map[id];
      chart?.dispatchAction({type: el.checked?'legendSelect':'legendUnSelect', name:n}); }
  }

  // --- helpers for zoom math ---
  function xDomain(){ if(!currentTicks.length) return [0,1]; return [currentTicks[0].id, currentTicks[currentTicks.length-1].id]; }
  function getZoomX(){ const opt=chart.getOption(); const dz=(opt.dataZoom||[]).find(z=>z.id==='dzx');
    if(!dz) return [0,100]; return [dz.start??0, dz.end??100]; }
  function setZoomX(startPct, endPct){ startPct=Math.max(0,Math.min(100,startPct)); endPct=Math.max(0,Math.min(100,endPct));
    if(endPct-startPct<0.1){ const c=(startPct+endPct)/2; startPct=c-0.05; endPct=c+0.05; }
    chart.setOption({ dataZoom:[{id:'dzx', start:startPct, end:endPct}] }, false, true);
  }
  function pctFromVal(val){ const [xmin,xmax]=xDomain(); if(xmax===xmin) return 0;
    return ( (val - xmin) / (xmax - xmin) ) * 100; }
  function valFromPct(p){ const [xmin,xmax]=xDomain(); return xmin + (p/100)*(xmax-xmin); }

  function currentXWindowVals(){
    const [s,e]=getZoomX(); return [valFromPct(s), valFromPct(e)];
  }

  function autoFitY(){
    const [vmin,vmax]=currentXWindowVals();
    const rows = currentTicks.filter(r=> r.id>=vmin && r.id<=vmax);
    if(!rows.length) return;
    let mn=+Infinity, mx=-Infinity;
    for(const r of rows){ if(r.mid<mn) mn=r.mid; if(r.mid>mx) mx=r.mid; }
    if(!isFinite(mn)||!isFinite(mx)) return;
    const pad = (mx-mn)*0.06 + 0.25;
    chart.setOption({ yAxis: [{ min: mn - pad, max: mx + pad }] }, false, true);
  }

  async function renderWindow(startId, endId){
    const c = ensureChart(); if(!c) return;
    setStatus('Loading data…'); log(`Load window [${startId}, ${endId}]`);

    currentSnap = await wfSnap().catch(()=>({segments:[],events:[],predictions:[],outcomes:[]}));
    const nSeg=currentSnap?.segments?.length||0, nEv=currentSnap?.events?.length||0, nPr=currentSnap?.predictions?.length||0, nOc=currentSnap?.outcomes?.length||0;

    currentTicks = await sqlRange(startId, endId);
    enrichMaps(currentSnap);

    const opt = baseOption(currentTicks, currentSnap);
    addLayers(opt, currentSnap, startId, endId);
    chart.setOption(opt, true);
    autoFitY(); // initial auto-fit

    setStatus(`Loaded ${currentTicks.length} ticks · seg:${nSeg} ev:${nEv} pr:${nPr} oc:${nOc}`, 'ok');
  }

  // --- drag zoom implementation (works even on older ECharts) ---
  (function installDragZoom(){
    let dragging=false, mode=null; // 'x' or 'y'
    let anchorPixel=null, anchorData=null; // [xpx, ypx], [xval, yval]
    let anchorStartPct=0, anchorEndPct=100;

    function onDown(e){
      if(!chart || e.event?.zrDelta) return;
      const pt=[e.offsetX, e.offsetY];
      if(!chart.containPixel('grid', pt)) return;
      dragging=true; mode=null;
      anchorPixel=pt;
      anchorData=chart.convertFromPixel({gridIndex:0}, pt);
      [anchorStartPct,anchorEndPct]=getZoomX();
    }
    function onMove(e){
      if(!dragging) return;
      const pt=[e.offsetX, e.offsetY];
      const dx=pt[0]-anchorPixel[0], dy=pt[1]-anchorPixel[1];
      if(!mode){
        if(Math.hypot(dx,dy)<6) return;
        mode = Math.abs(dx) >= Math.abs(dy) ? 'x' : 'y';
      }
      if(mode==='x'){
        // zoom horizontally around anchorData[0]
        const width = chart.getWidth();
        const scale = Math.exp(-dx / (width*0.35)); // right -> zoom in
        const curWidthPct = Math.max(0.1, (anchorEndPct - anchorStartPct));
        const newWidthPct = Math.max(0.1, Math.min(100, curWidthPct * scale));
        const centerVal = anchorData[0];
        const centerPct = pctFromVal(centerVal);
        let start = centerPct - newWidthPct/2, end = centerPct + newWidthPct/2;
        if(start<0){ end -= start; start=0; }
        if(end>100){ const over=end-100; start -= over; end=100; if(start<0) start=0; }
        setZoomX(start,end);
        autoFitY();
      } else {
        // zoom vertically around anchorData[1]
        const h = chart.getHeight();
        const scale = Math.exp(-dy / (h*0.35)); // up -> zoom in
        // get current y extent from option (min/max)
        const opt = chart.getOption();
        let ymin = Number(opt.yAxis[0].min), ymax = Number(opt.yAxis[0].max);
        if(!isFinite(ymin) || !isFinite(ymax)) { autoFitY(); const o2=chart.getOption(); ymin=Number(o2.yAxis[0].min); ymax=Number(o2.yAxis[0].max); }
        const curRange = Math.max(1e-6, ymax - ymin);
        const newRange = Math.max(0.01, Math.min(1e6, curRange * scale));
        const cy = anchorData[1];
        const newMin = cy - newRange/2, newMax = cy + newRange/2;
        chart.setOption({ yAxis:[{min:newMin, max:newMax}] }, false, true);
      }
    }
    function onUp(){ dragging=false; mode=null; }

    // defer install until chart exists
    const iv = setInterval(()=>{
      if(!chart) return;
      clearInterval(iv);
      const zr = chart.getZr();
      zr.on('mousedown', onDown);
      zr.on('mousemove', onMove);
      zr.on('mouseup', onUp);
      zr.on('globalout', onUp);
      chart.on('dataZoom', autoFitY);
    }, 50);
  })();

  // ----- UI wiring -----
  $('#loadBtn').addEventListener('click', async ()=>{
    const s=parseInt($('#startTick').value||'1',10);
    const e=parseInt($('#endTick').value||(s+12000),10);
    try{ await renderWindow(s,e); applyLegendFromToggles(); }
    catch(err){ console.error(err); setStatus('Load failed: '+(err.message||err),'err'); log('Load failed: '+err); }
  });

  $('#jumpBtn').addEventListener('click', async ()=>{
    const t=parseInt($('#jumpTick').value||'1',10);
    const s=Math.max(1, t-JUMP_WINDOW), e=t+JUMP_WINDOW;
    $('#startTick').value=s; $('#endTick').value=e;
    try{ await renderWindow(s,e); applyLegendFromToggles(); }
    catch(err){ console.error(err); setStatus('Jump failed: '+(err.message||err),'err'); log('Jump failed: '+err); }
  });
  $('#jumpTick').addEventListener('keydown', ev=>{ if(ev.key==='Enter') $('#jumpBtn').click(); });

  ['chkMid','chkBid','chkAsk','chkMacro','chkEvents','chkPreds','chkOutcomes'].forEach(id=>{
    const el=$('#'+id); if(el) el.addEventListener('change', applyLegendFromToggles);
  });

  $('#runBtn').addEventListener('click', async ()=>{
    const btn=$('#runBtn'); btn.disabled=true; btn.textContent='Running…';
    setStatus('Working…'); log('Run: start');
    try{
      const res=await wfStep();
      if(res?.journal) res.journal.forEach(line=>log(line));
      if(res?.ok===false){ setStatus('Error: '+(res.error||'unknown'),'err'); log('Run error: '+(res.error||'unknown')); }
      else { setStatus(res?.message||'Working','ok'); log('Run: done'); }
      const s=parseInt($('#startTick').value||'1',10), e=parseInt($('#endTick').value||(s+12000),10);
      await renderWindow(s,e); applyLegendFromToggles();
    }catch(err){
      setStatus('Run failed: '+(err.message||err),'err'); log('Run failed: '+err);
    }finally{ btn.disabled=false; btn.textContent='Run'; }
  });

  setStatus('Ready — use Jump or Load to fetch a window.');
})();
