(() => {
  const el = document.getElementById('chart');
  const chart = echarts.init(el, null, { renderer: 'canvas' });

  const COLORS = {
    ask:'#FF6B6B', bid:'#4ECDC4', mid_tick:'#FFD166',
    max_lbl:'#A78BFA', mid_lbl:'#60A5FA', min_lbl:'#34D399',
    max_seg:'#F472B6', mid_seg:'#F59E0B', min_seg:'#10B981'
  };

  const S = {
    x:[], ts:[],
    ask:[], bid:[], mid_tick:[],
    max_lbl:[], mid_lbl:[], min_lbl:[],
    max_seg:[], mid_seg:[], min_seg:[],
    viewSize:3000,
    lastRowId: null,     // last loaded row id for current leg
    lastSegEndId: null,  // end tick id of last loaded segment
    currentLeg: 'max',   // 'max' | 'mid' | 'min'
  };

  const status = (t)=>document.getElementById('status').textContent=t;
  const j = async(u)=>{ const r=await fetch(u); if(!r.ok) throw new Error(await r.text()); return r.json(); };

  function yAxisInt(){
    return { type:'value',
      min:e=>Math.floor(e.min), max:e=>Math.ceil(e.max),
      interval:1, minInterval:1, scale:false,
      axisLabel:{ color:'#9ca3af', formatter:v=>Number.isInteger(v)?v:'' },
      axisLine:{ lineStyle:{ color:'#1f2937' } },
      splitLine:{ show:true, lineStyle:{ color:'rgba(148,163,184,0.08)' } },
    };
  }
  const mkLine=(name,key,color,connect=false)=>({ name, type:'line', showSymbol:true, symbolSize:3.5,
    data:S[key], itemStyle:{ color }, lineStyle:{ color, width:1.6 }, smooth:false, connectNulls:connect });

  function baseOption(){
    return {
      backgroundColor:'#0b0f14', animation:false, grid:{ left:42, right:18, top:10, bottom:28 },
      tooltip:{ trigger:'axis', axisPointer:{type:'line'},
        backgroundColor:'rgba(17,24,39,.95)',
        formatter:(ps)=>{
          if(!ps?.length) return '';
          const i=ps[0].dataIndex, id=S.x[i], ts=S.ts[i]||'';
          const dt=ts?new Date(ts):null, d=dt?`${dt.toLocaleDateString()} ${dt.toLocaleTimeString()}`:'(no time)';
          const out=[`ID: ${id}\n\nTime: ${d}\n\n* * *\n`];
          for(const p of ps) if(p.value!=null) out.push(`${p.seriesName}: ${p.value}\n`);
          return out.join('');
        } },
      xAxis:{ type:'category', data:S.x, axisLabel:{ color:'#9ca3af' },
              axisLine:{ lineStyle:{ color:'#1f2937' } },
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
        mkLine('Max Segment','max_seg',COLORS.max_seg,true),
        mkLine('Mid Segment','mid_seg',COLORS.mid_seg,true),
        mkLine('Min Segment','min_seg',COLORS.min_seg,true),
      ]
    };
  }
  chart.setOption(baseOption(),{notMerge:true});

  const pinnedRight=()=>{
    const dz=chart.getOption().dataZoom?.[0];
    if(!dz) return false;
    if(dz.endValue!=null){ const last=S.x.length?S.x.length-1:0; return dz.endValue>=last; }
    return (dz.end??100)>99.5;
  };
  function keepRight(){
    const n=Math.max(1,S.viewSize), len=S.x.length, e=Math.max(0,len-1), s=Math.max(0,e-n+1);
    chart.dispatchAction({type:'dataZoom',startValue:s,endValue:e});
  }
  chart.on('dataZoom',()=>{
    if(!pinnedRight()) return;
    const dz=chart.getOption().dataZoom?.[0]; if(!dz) return;
    if(dz.endValue!=null && dz.startValue!=null) S.viewSize = Math.max(1, dz.endValue - dz.startValue + 1);
  });

  // ---------- appenders / overlays ----------
  function ensureAlignment(){ const keys=['ask','bid','mid_tick','max_lbl','mid_lbl','min_lbl','max_seg','mid_seg','min_seg','ts'];
    for(const k of keys) while(S[k].length < S.x.length) S[k].push(null); }

  function appendTicks(rows){
    if(!rows?.length) return 0;
    const atRight=pinnedRight();
    for(const r of rows){
      S.x.push(r.id);
      S.ts.push(r.ts||null);
      S.mid_tick.push(r.mid!=null?+r.mid:null);
      S.ask.push(r.ask!=null?+r.ask:null);
      S.bid.push(r.bid!=null?+r.bid:null);
    }
    ensureAlignment();
    chart.setOption({
      xAxis:{ data:S.x }, yAxis:yAxisInt(),
      series:[
        {name:'Ask',data:S.ask},{name:'Mid (ticks)',data:S.mid_tick},{name:'Bid',data:S.bid},
        {name:'Max (labels)',data:S.max_lbl},{name:'Mid (labels)',data:S.mid_lbl},{name:'Min (labels)',data:S.min_lbl},
        {name:'Max Segment',data:S.max_seg},{name:'Mid Segment',data:S.mid_seg},{name:'Min Segment',data:S.min_seg}
      ]
    },{lazyUpdate:true});
    if(atRight) keepRight();
    return rows.length;
  }

  function overlayLabelChunk(rows,key){
    if(!rows?.length) return;
    const idx=new Map(S.x.map((id,i)=>[id,i]));
    const arr=S[key];
    for(const r of rows){
      const id=r.id ?? r.tick_id ?? r.start_id;
      const val=r.value ?? r.start_price ?? r.price ?? r.mid;
      if(id==null||val==null) continue;
      const i=idx.get(id); if(i!=null) arr[i]=+val;
    }
    chart.setOption({
      series:[
        {name:'Max (labels)',data:S.max_lbl},
        {name:'Mid (labels)',data:S.mid_lbl},
        {name:'Min (labels)',data:S.min_lbl}
      ]
    },{lazyUpdate:true});
  }

  function overlaySegment(kind, startId, startPrice, endId, endPrice){
    const idx=new Map(S.x.map((id,i)=>[id,i]));
    const i0=idx.get(startId), i1=idx.get(endId);
    if(i0==null||i1==null||i1<=i0) return;
    const n=i1-i0, arr = kind==='max'?S.max_seg:kind==='mid'?S.mid_seg:S.min_seg;
    for(let k=0;k<=n;k++){
      const y = startPrice + ((endPrice-startPrice)*(k/n));
      arr[i0+k]=y;
    }
    chart.setOption({ series:[
      {name:'Max Segment',data:S.max_seg},
      {name:'Mid Segment',data:S.mid_seg},
      {name:'Min Segment',data:S.min_seg}
    ] },{lazyUpdate:true});
  }

  // ---------- fetchers ----------
  async function fetchTicksRange(startId,endId){
    const size=Math.max(1,endId-startId+1);
    const tries=[
      `/api/ticks?from_id=${startId}&to_id=${endId}`,
      `/api/ticks/range?start_id=${startId}&limit=${size}`,
      `/api/ticks/after?since_id=${Math.max(0,startId-1)}&limit=${size}`
    ];
    for(const u of tries){ try{ const rows=await j(u); if(Array.isArray(rows)&&rows.length) return rows; }catch{} }
    return [];
  }

  async function fetchLeg(kind, id){
    return await j(`/api/${kind}line/by_id?id=${id}`);
  }
  async function fetchLegNext(kind, afterRowId){
    return await j(`/api/${kind}line/next?after_id=${afterRowId}`);
  }
  async function lastTickId(){
    const r=await j('/api/ticks/last_id'); return r?.last_id ?? null;
  }

  async function fetchLabelsWindow(startId,endId){
    let cursor=startId;
    while(cursor<=endId){
      const lim=Math.min(20000,endId-cursor+1);
      const results = await Promise.allSettled([
        j(`/api/labels/max/range?start_id=${cursor}&limit=${lim}`),
        j(`/api/labels/mid/range?start_id=${cursor}&limit=${lim}`),
        j(`/api/labels/min/range?start_id=${cursor}&limit=${lim}`),
      ]);
      let progressed=false;
      if(results[0].status==='fulfilled') overlayLabelChunk(results[0].value,'max_lbl');
      if(results[1].status==='fulfilled') overlayLabelChunk(results[1].value,'mid_lbl');
      if(results[2].status==='fulfilled') overlayLabelChunk(results[2].value,'min_lbl');
      for(const res of results){
        if(res.status==='fulfilled' && Array.isArray(res.value) && res.value.length){
          const last = res.value.at(-1); const lastId = (last.id ?? last.tick_id ?? last.start_id);
          if(lastId!=null){ cursor=Math.max(cursor,lastId+1); progressed=true; }
        }
      }
      if(!progressed) break;
    }
  }

  // ---------- flows ----------
  async function loadInitial(kind,rowId){
    status(`loading ${kind}#${rowId}…`);
    S.currentLeg=kind;

    const seg = await fetchLeg(kind,rowId);
    if(!seg?.start_id){ status('not found'); return; }
    S.lastRowId = seg.id;
    S.lastSegEndId = seg.end_id;

    // ticks
    const rows = await fetchTicksRange(seg.start_id, seg.end_id);
    appendTicks(rows);

    // labels + segments
    await fetchLabelsWindow(seg.start_id, seg.end_id);

    const sp = seg.start_price ?? seg.start ?? seg.price_start ?? seg.value_start;
    const ep = seg.end_price   ?? seg.end   ?? seg.price_end   ?? seg.value_end;
    if(sp!=null && ep!=null) overlaySegment(kind, seg.start_id, +sp, seg.end_id, +ep);

    keepRight(); status('ready');
  }

  async function loadMore(){
    if(!S.lastRowId){ status('nothing loaded'); return; }
    const kind=S.currentLeg;

    try{
      const nx = await fetchLegNext(kind, S.lastRowId);
      if(nx?.id){
        // append next same-kind leg
        const rows = await fetchTicksRange(nx.start_id, nx.end_id);
        appendTicks(rows);
        await fetchLabelsWindow(nx.start_id, nx.end_id);
        const sp = nx.start_price ?? nx.start ?? nx.price_start ?? nx.value_start;
        const ep = nx.end_price   ?? nx.end   ?? nx.price_end   ?? nx.value_end;
        if(sp!=null && ep!=null) overlaySegment(kind, nx.start_id, +sp, nx.end_id, +ep);
        S.lastRowId = nx.id; S.lastSegEndId = nx.end_id;
        keepRight(); status('ready'); return;
      }
    }catch(e){ /* fallthrough to tail mode */ }

    // --- tail mode: no next leg of this kind -> load everything to the last tick and overlay smaller legs ---
    status('tail… loading to last tick');
    const last = await lastTickId();
    if(!last || !S.lastSegEndId){ status('ready'); return; }

    let cursor = S.lastSegEndId + 1;
    while(cursor <= last){
      const chunk = Math.min(20000, last - cursor + 1);
      const rows = await j(`/api/ticks/after?since_id=${cursor-1}&limit=${chunk}`);
      if(!rows?.length) break;
      appendTicks(rows);
      cursor = rows.at(-1).id + 1;
    }
    // overlay labels (all kinds) over the tail so “smaller” legs appear
    await fetchLabelsWindow(S.lastSegEndId, last);
    keepRight(); status('ready');
  }

  // ---------- UI ----------
  function applyVisibility(){
    const boxes=document.querySelectorAll('input[type=checkbox][data-series]');
    const show={}; boxes.forEach(b=>show[b.dataset.series]=b.checked);
    const opt=chart.getOption();
    opt.series.forEach(s=>{
      const key=({
        'Ask':'ask','Bid':'bid','Mid (ticks)':'mid_tick',
        'Max (labels)':'max_lbl','Mid (labels)':'mid_lbl','Min (labels)':'min_lbl',
        'Max Segment':'max_seg','Mid Segment':'mid_seg','Min Segment':'min_seg'
      })[s.name];
      s.data=S[key];
      const vis=show[key]; s.itemStyle.opacity=vis?1:0; s.lineStyle.opacity=vis?1:0;
    });
    chart.setOption(opt,{notMerge:true,lazyUpdate:true});
  }

  document.getElementById('btnLoad').addEventListener('click', async ()=>{
    // reset
    Object.assign(S,{ x:[],ts:[],ask:[],bid:[],mid_tick:[],
      max_lbl:[],mid_lbl:[],min_lbl:[], max_seg:[],mid_seg:[],min_seg:[],
      viewSize:3000, lastRowId:null, lastSegEndId:null,
      currentLeg: document.getElementById('legKind').value });
    chart.clear(); chart.setOption(baseOption(),{notMerge:true});
    const id = parseInt(document.getElementById('rowId').value||'0',10);
    if(!id){ status('enter Row ID'); return; }
    try{ await loadInitial(S.currentLeg, id); applyVisibility(); }catch(e){ status(`error: ${String(e).slice(0,160)}`); }
  });

  document.getElementById('btnMore').addEventListener('click', async ()=>{
    try{ await loadMore(); applyVisibility(); }catch(e){ status(`error: ${String(e).slice(0,160)}`); }
  });

  document.getElementById('btnReset').addEventListener('click', ()=>{
    Object.assign(S,{ x:[],ts:[],ask:[],bid:[],mid_tick:[],
      max_lbl:[],mid_lbl:[],min_lbl:[], max_seg:[],mid_seg:[],min_seg:[],
      viewSize:3000, lastRowId:null, lastSegEndId:null, currentLeg:'max' });
    chart.clear(); chart.setOption(baseOption(),{notMerge:true});
    status('idle');
  });

  document.querySelectorAll('input[type=checkbox][data-series]')
    .forEach(cb=>cb.addEventListener('change', applyVisibility));
})();
