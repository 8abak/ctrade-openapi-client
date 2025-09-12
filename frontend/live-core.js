(() => {
  const INIT_CAP = 60000;    // keep at most this many points in memory
  const VIEW_DEFAULT = 3000; // visible points when pinned-right

  const el = document.getElementById('chart');
  const chart = echarts.init(el, null, { renderer: 'canvas' });

  const COLORS = {
    ask: '#FF6B6B', mid_tick: '#FFD166', bid: '#4ECDC4',
    max_lbl: '#A78BFA', mid_lbl: '#60A5FA', min_lbl: '#34D399',
  };

  const state = {
    x:[], ts:[], ask:[], bid:[], mid_tick:[],
    max_lbl:[], mid_lbl:[], min_lbl:[],
    lastId: 0,
    viewSize: VIEW_DEFAULT,
    seg: null, // {id,start_id,end_id,...}
    fetching: false,
    labelsCursor: { max: 0, mid: 0, min: 0 }, // last fetched label start id
  };

  // ---------- chart option ----------
  function yAxisInt(){ return {
    type:'value', min:(e)=>Math.floor(e.min), max:(e)=>Math.ceil(e.max),
    interval:1, minInterval:1, scale:false,
    axisLabel:{ color:'#9ca3af', formatter:(v)=>Number.isInteger(v)?v:'' },
    axisLine:{ lineStyle:{ color:'#1f2937' } },
    splitLine:{ show:true, lineStyle:{ color:'rgba(148,163,184,0.08)' } },
  };}

  function mkLine(name,key,color,connect=false){
    return { name, type:'line', showSymbol:true, symbolSize:3.5, smooth:false,
      data:state[key], itemStyle:{ color }, lineStyle:{ color, width:1.6 }, connectNulls:connect };
  }

  function baseOption(){
    return {
      backgroundColor:'#0b0f14', animation:false,
      grid:{ left:42, right:18, top:10, bottom:28 },
      tooltip:{ trigger:'axis', axisPointer:{type:'line'}, backgroundColor:'rgba(17,24,39,.95)',
        formatter:(ps)=>{
          if(!ps?.length) return '';
          const i=ps[0].dataIndex, id=state.x[i], ts=state.ts[i]||'';
          const dt=ts?new Date(ts):null, d=dt?`${dt.toLocaleDateString()} ${dt.toLocaleTimeString()}`:'(no time)';
          const out=[`<div><b>ID</b>: ${id}</div><div><b>Time</b>: ${d}</div><hr>`];
          for(const p of ps) if(p.value!=null) out.push(`<div><span style="display:inline-block;width:8px;height:8px;background:${p.color};margin-right:6px;border-radius:2px"></span>${p.seriesName}: ${p.value}</div>`);
          return out.join('');
        }},
      xAxis:{ type:'category', data:state.x,
        axisLabel:{ color:'#9ca3af' }, axisLine:{ lineStyle:{ color:'#1f2937' } },
        splitLine:{ show:true, lineStyle:{ color:'rgba(148,163,184,0.08)' } } },
      yAxis: yAxisInt(),
      dataZoom:[{type:'inside'},{type:'slider',height:16,bottom:4}],
      series:[
        mkLine('Ask','ask',COLORS.ask,false),
        mkLine('Mid (ticks)','mid_tick',COLORS.mid_tick,false),
        mkLine('Bid','bid',COLORS.bid,false),
        mkLine('Max (labels)','max_lbl',COLORS.max_lbl,true),
        mkLine('Mid (labels)','mid_lbl',COLORS.mid_lbl,true),
        mkLine('Min (labels)','min_lbl',COLORS.min_lbl,true),
      ]
    };
  }
  chart.setOption(baseOption(), { notMerge:true });
  chart.on('dataZoom', () => { if(!pinnedRight()) return; const [s,e]=idxWindow(); if(s!=null&&e!=null) state.viewSize=Math.max(1,e-s+1); });

  // ---------- utils ----------
  async function j(url){ const r=await fetch(url); if(!r.ok) throw new Error(await r.text()); return r.json(); }
  function status(t){ const el=document.getElementById('status'); if(el) el.textContent=t; }
  function pinnedRight(){ const dz=chart.getOption().dataZoom?.[0]; if(!dz) return true; if(dz.endValue!=null){ const last=state.x.length?state.x.length-1:0; return dz.endValue>=last; } return (dz.end??100)>99.5; }
  function idxWindow(){ const dz=chart.getOption().dataZoom?.[0]; if(!dz) return [null,null]; if(dz.startValue!=null) return [dz.startValue,dz.endValue]; const n=state.x.length; return [Math.floor(((dz.start??0)/100)*(n-1)), Math.floor(((dz.end??100)/100)*(n-1))]; }
  function keepRight(){ const n=Math.max(1,state.viewSize); const len=state.x.length; const e=Math.max(0,len-1), s=Math.max(0,e-n+1); chart.dispatchAction({type:'dataZoom',startValue:s,endValue:e}); }

  // ---------- append ----------
  function cap(){
    if(state.x.length<=INIT_CAP) return;
    const cut=state.x.length-INIT_CAP;
    for(const k of ['x','ts','ask','bid','mid_tick','max_lbl','mid_lbl','min_lbl']) state[k].splice(0,cut);
  }
  function appendTicks(rows){
    if(!rows?.length) return 0;
    const atRight=pinnedRight();
    for(const r of rows){
      state.x.push(r.id); state.ts.push(r.ts||null);
      state.mid_tick.push(r.mid!=null?+r.mid:null);
      state.ask.push(r.ask!=null?+r.ask:null);
      state.bid.push(r.bid!=null?+r.bid:null);
      state.max_lbl.push(null); state.mid_lbl.push(null); state.min_lbl.push(null);
      state.lastId=r.id;
    }
    cap();
    chart.setOption({
      xAxis:{ data:state.x }, yAxis:yAxisInt(),
      series:[
        {name:'Ask',data:state.ask},{name:'Mid (ticks)',data:state.mid_tick},{name:'Bid',data:state.bid},
        {name:'Max (labels)',data:state.max_lbl},{name:'Mid (labels)',data:state.mid_lbl},{name:'Min (labels)',data:state.min_lbl},
      ]
    },{lazyUpdate:true});
    if(atRight) keepRight();
    return rows.length;
  }

  function overlayLabelChunk(rows,key){
    if(!rows?.length) return;
    const idx = new Map(state.x.map((id,i)=>[id,i]));
    const arr = state[key];
    for(const r of rows){
      const id = r.id ?? r.tick_id ?? r.start_id;
      const val = r.value ?? r.start_price ?? r.price ?? r.mid;
      if(id==null || val==null) continue;
      const i = idx.get(id); if(i!=null) arr[i]=+val;
    }
    chart.setOption({ series:[
      {name:'Max (labels)',data:state.max_lbl},
      {name:'Mid (labels)',data:state.mid_lbl},
      {name:'Min (labels)',data:state.min_lbl},
    ]},{lazyUpdate:true});
  }

  async function fetchLabelsWindow(startId, endId){
    let cursor = startId;
    while(cursor <= endId){
      const lim = Math.min(20000, endId - cursor + 1);
      const [mx,md,mn] = await Promise.allSettled([
        j(`/api/labels/max/range?start_id=${cursor}&limit=${lim}`),
        j(`/api/labels/mid/range?start_id=${cursor}&limit=${lim}`),
        j(`/api/labels/min/range?start_id=${cursor}&limit=${lim}`),
      ]);
      if(mx.status==='fulfilled'){ overlayLabelChunk(mx.value,'max_lbl'); if(mx.value.length) cursor = (mx.value.at(-1).id ?? mx.value.at(-1).start_id) + 1; }
      if(md.status==='fulfilled'){ overlayLabelChunk(md.value,'mid_lbl'); }
      if(mn.status==='fulfilled'){ overlayLabelChunk(mn.value,'min_lbl'); }
      if((mx.status!=='fulfilled'||!mx.value.length) &&
         (md.status!=='fulfilled'||!md.value.length) &&
         (mn.status!=='fulfilled'||!mn.value.length)) break; // nothing more
    }
  }

  async function seedAnchors(atId){
    const [mx,md,mn]=await Promise.allSettled([
      j(`/api/labels/max/prev?before_id=${atId}`),
      j(`/api/labels/mid/prev?before_id=${atId}`),
      j(`/api/labels/min/prev?before_id=${atId}`),
    ]);
    const i0 = state.x.indexOf(atId);
    if(i0>=0){
      if(mx.status==='fulfilled'&&mx.value?.value!=null) state.max_lbl[i0]=+mx.value.value;
      if(md.status==='fulfilled'&&md.value?.value!=null) state.mid_lbl[i0]=+md.value.value;
      if(mn.status==='fulfilled'&&mn.value?.value!=null) state.min_lbl[i0]=+mn.value.value;
      chart.setOption({ series:[
        {name:'Max (labels)',data:state.max_lbl},
        {name:'Mid (labels)',data:state.mid_lbl},
        {name:'Min (labels)',data:state.min_lbl},
      ]},{lazyUpdate:true});
    }
  }

  function setSeriesVisibility(){
    const boxes=document.querySelectorAll('input[type=checkbox][data-series]');
    const show={}; boxes.forEach(b=>show[b.dataset.series]=b.checked);
    const opt=chart.getOption();
    opt.series.forEach(s=>{
      const key=({'Ask':'ask','Bid':'bid','Mid (ticks)':'mid_tick','Max (labels)':'max_lbl','Mid (labels)':'mid_lbl','Min (labels)':'min_lbl'})[s.name];
      s.data=state[key]; const vis=show[key]; s.itemStyle.opacity=vis?1:0; s.lineStyle.opacity=vis?1:0;
    });
    chart.setOption(opt,{notMerge:true,lazyUpdate:true});
  }

  // ---------- loading ----------
  async function fetchRange(startId, endId){
    const size=Math.max(1,endId-startId+1);
    const tries=[
      `/api/ticks?from_id=${startId}&to_id=${endId}`,
      `/api/ticks/range?start_id=${startId}&limit=${size}`,
      `/api/ticks/after?since_id=${Math.max(0,startId-1)}&limit=${size}`
    ];
    for(const u of tries){ try{ const rows=await j(u); if(Array.isArray(rows)&&rows.length) return rows; }catch{} }
    return [];
  }

  async function loadInitialFromLastMax(){
    status('finding last max…');
    const seg = await j('/api/maxline/last');
    if(!seg?.start_id){ status('no max'); return; }
    state.seg = seg;

    // get latest tick id
    let latest = 0;
    try { const r = await j('/api/ticks/latestN?limit=1'); latest = r?.[0]?.id || 0; } catch {}
    const from = seg.start_id, to = Math.max(seg.end_id || 0, latest);

    status(`loading ticks ${from}..${to}`);
    const rows = await fetchRange(from, to);
    appendTicks(rows);
    await seedAnchors(from);
    await fetchLabelsWindow(from, to);
    keepRight();
    setSeriesVisibility();
    status('ready');
  }

  async function loadNew(){
    if(state.fetching) return;
    state.fetching = true;
    status('fetching…');
    try{
      const rows = await j(`/api/ticks/after?since_id=${state.lastId||0}&limit=5000`);
      const added = appendTicks(rows);
      if(added) await fetchLabelsWindow(state.lastId-added+1, state.lastId);
      status(added?`+${added}`:'no new ticks');
    }catch(e){ console.error(e); status('error'); }
    finally{ state.fetching=false; }
  }

  // UI
  document.querySelectorAll('input[type=checkbox][data-series]').forEach(cb=>cb.addEventListener('change', setSeriesVisibility));
  document.getElementById('btnLoadNew').addEventListener('click', loadNew);
  (function wireAuto(){
    const box=document.getElementById('autoFetch'); let t=null;
    box.addEventListener('change',()=>{ if(box.checked){ if(t)clearInterval(t); t=setInterval(loadNew,1000);} else { if(t)clearInterval(t); t=null; } });
  })();

  // boot
  loadInitialFromLastMax();
})();
