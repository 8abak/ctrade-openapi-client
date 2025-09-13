(() => {
  // --- chart bootstrap ---
  const el = document.getElementById('chart');
  const chart = echarts.init(el, null, { renderer: 'canvas' });

  const COLORS = {
    ask:'#FF6B6B',
    mid_tick:'#FFD166',
    bid:'#4ECDC4',
    max_lbl:'#A78BFA',
    mid_lbl:'#60A5FA',
    min_lbl:'#34D399',
    max_seg:'#F472B6'
  };

  const state = {
    x:[], ts:[],
    ask:[], bid:[], mid_tick:[],
    max_lbl:[], mid_lbl:[], min_lbl:[],
    max_seg:[],                       // diagonal zigzag leg overlay
    lastId:0,
    viewSize:3000,
    lastMaxRowId:null,
  };

  // --- helpers ---
  function yAxisInt(){
    return {
      type:'value',
      min:(e)=>Math.floor(e.min),
      max:(e)=>Math.ceil(e.max),
      interval:1, minInterval:1, scale:false,
      axisLabel:{ color:'#9ca3af', formatter:(v)=>Number.isInteger(v)?v:'' },
      axisLine:{ lineStyle:{ color:'#1f2937' } },
      splitLine:{ show:true, lineStyle:{ color:'rgba(148,163,184,0.08)' } },
    };
  }

  function mkLine(name,key,color,connect=false){
    return {
      name, type:'line', showSymbol:true, symbolSize:3.5, smooth:false,
      data:state[key], itemStyle:{ color }, lineStyle:{ color, width:1.6 },
      connectNulls:connect
    };
  }

  function baseOption(){
    return {
      backgroundColor:'#0b0f14',
      animation:false,
      grid:{ left:42, right:18, top:10, bottom:28 },
      tooltip:{
        trigger:'axis', axisPointer:{type:'line'},
        backgroundColor:'rgba(17,24,39,.95)',
        formatter:(ps)=>{
          if(!ps?.length) return '';
          const i=ps[0].dataIndex, id=state.x[i], ts=state.ts[i]||'';
          const dt=ts?new Date(ts):null, d=dt?`${dt.toLocaleDateString()} ${dt.toLocaleTimeString()}`:'(no time)';
          const out=[`ID: ${id}\n\nTime: ${d}\n\n* * *\n`];
          for(const p of ps) if(p.value!=null) out.push(`${p.seriesName}: ${p.value}\n`);
          return out.join('');
        }
      },
      xAxis:{ type:'category', data:state.x,
        axisLabel:{ color:'#9ca3af' },
        axisLine:{ lineStyle:{ color:'#1f2937' } },
        splitLine:{ show:true, lineStyle:{ color:'rgba(148,163,184,0.08)' } }
      },
      yAxis: yAxisInt(),
      dataZoom:[{type:'inside'},{type:'slider',height:16,bottom:4}],
      series:[
        mkLine('Ask','ask',COLORS.ask,false),
        mkLine('Mid (ticks)','mid_tick',COLORS.mid_tick,false),
        mkLine('Bid','bid',COLORS.bid,false),
        mkLine('Max (labels)','max_lbl',COLORS.max_lbl,true),
        mkLine('Mid (labels)','mid_lbl',COLORS.mid_lbl,true),
        mkLine('Min (labels)','min_lbl',COLORS.min_lbl,true),
        mkLine('Max Segment','max_seg',COLORS.max_seg,true), // NEW
      ]
    };
  }

  chart.setOption(baseOption(), { notMerge:true });

  chart.on('dataZoom',()=>{
    if(!pinnedRight()) return;
    const [s,e]=idxWin();
    if(s!=null&&e!=null) state.viewSize=Math.max(1,e-s+1);
  });

  // --- tiny utils & IO ---
  async function j(u){ const r=await fetch(u); if(!r.ok) throw new Error(await r.text()); return r.json(); }
  function status(t){ const el=document.getElementById('status'); if(el) el.textContent=t; }

  function pinnedRight(){
    const dz=chart.getOption().dataZoom?.[0];
    if(!dz) return false;
    if(dz.endValue!=null){
      const last=state.x.length?state.x.length-1:0;
      return dz.endValue>=last;
    }
    return (dz.end??100)>99.5;
  }

  function idxWin(){
    const dz=chart.getOption().dataZoom?.[0];
    if(!dz) return [null,null];
    if(dz.startValue!=null) return [dz.startValue,dz.endValue];
    const n=state.x.length;
    return [Math.floor(((dz.start??0)/100)*(n-1)), Math.floor(((dz.end??100)/100)*(n-1))];
  }

  function keepRight(){
    const n=Math.max(1,state.viewSize), len=state.x.length, e=Math.max(0,len-1), s=Math.max(0,e-n+1);
    chart.dispatchAction({type:'dataZoom',startValue:s,endValue:e});
  }

  function setSeriesVisibility(){
    const boxes=document.querySelectorAll('input[type=checkbox][data-series]');
    const show={};
    boxes.forEach(b=>show[b.dataset.series]=b.checked);

    const opt=chart.getOption();
    opt.series.forEach(s=>{
      const key=({
        'Ask':'ask','Bid':'bid','Mid (ticks)':'mid_tick',
        'Max (labels)':'max_lbl','Mid (labels)':'mid_lbl','Min (labels)':'min_lbl',
        'Max Segment':'max_seg',
      })[s.name];
      s.data=state[key];
      const vis=show[key];
      s.itemStyle.opacity=vis?1:0;
      s.lineStyle.opacity=vis?1:0;
    });
    chart.setOption(opt,{notMerge:true,lazyUpdate:true});
  }

  // --- data assembly ---
  function ensureArraysExtended(nNew){
    // keep arrays aligned when appending ticks
    const keys=['ask','bid','mid_tick','max_lbl','mid_lbl','min_lbl','max_seg','ts'];
    for(const k of keys) while(state[k].length < state.x.length) state[k].push(null);
  }

  function appendTicks(rows){
    if(!rows?.length) return 0;
    const atRight=pinnedRight();
    for(const r of rows){
      state.x.push(r.id);
      state.ts.push(r.ts||null);
      state.mid_tick.push(r.mid!=null?+r.mid:null);
      state.ask.push(r.ask!=null?+r.ask:null);
      state.bid.push(r.bid!=null?+r.bid:null);
    }
    ensureArraysExtended();
    state.lastId=state.x[state.x.length-1];

    chart.setOption({
      xAxis:{ data:state.x },
      yAxis:yAxisInt(),
      series:[
        {name:'Ask',data:state.ask},
        {name:'Mid (ticks)',data:state.mid_tick},
        {name:'Bid',data:state.bid},
        {name:'Max (labels)',data:state.max_lbl},
        {name:'Mid (labels)',data:state.mid_lbl},
        {name:'Min (labels)',data:state.min_lbl},
        {name:'Max Segment',data:state.max_seg},
      ]
    },{lazyUpdate:true});

    if(atRight) keepRight();
    return rows.length;
  }

  function overlayLabelChunk(rows,key){
    if(!rows?.length) return;
    const idx=new Map(state.x.map((id,i)=>[id,i]));
    const arr=state[key];
    for(const r of rows){
      const id=r.id ?? r.tick_id ?? r.start_id;
      const val=r.value ?? r.start_price ?? r.price ?? r.mid;
      if(id==null||val==null) continue;
      const i=idx.get(id);
      if(i!=null) arr[i]=+val;
    }
    chart.setOption({
      series:[
        {name:'Max (labels)',data:state.max_lbl},
        {name:'Mid (labels)',data:state.mid_lbl},
        {name:'Min (labels)',data:state.min_lbl},
      ]
    },{lazyUpdate:true});
  }

  // NEW: draw a diagonal “max leg” between start/end, even if labels are missing
  function overlayMaxSegment(startId,startPrice,endId,endPrice){
    const idx=new Map(state.x.map((id,i)=>[id,i]));
    const i0=idx.get(startId), i1=idx.get(endId);
    if(i0==null || i1==null) return;

    // linear interpolation across indices so tooltip works everywhere
    const n = i1 - i0;
    if(n <= 0) return;

    for(let k=0;k<=n;k++){
      const y = startPrice + ( (endPrice - startPrice) * (k/n) );
      state.max_seg[i0+k] = y;
    }
    chart.setOption({ series:[ {name:'Max Segment', data:state.max_seg} ] }, {lazyUpdate:true});
  }

  // label fetcher for sparse tables
  async function fetchLabelsWindow(startId,endId){
    let cursor=startId;
    while(cursor<=endId){
      const lim=Math.min(20000,endId-cursor+1);
      const tries = [
        [`/api/labels/max/range?start_id=${cursor}&limit=${lim}`, 'max_lbl'],
        [`/api/labels/mid/range?start_id=${cursor}&limit=${lim}`, 'mid_lbl'],
        [`/api/labels/min/range?start_id=${cursor}&limit=${lim}`, 'min_lbl'],
      ];
      const results = await Promise.allSettled(tries.map(t=>j(t[0])));
      let progressed=false;
      for(let r=0;r<results.length;r++){
        const res=results[r];
        const key=tries[r][1];
        if(res.status==='fulfilled' && Array.isArray(res.value) && res.value.length){
          overlayLabelChunk(res.value,key);
          const last = res.value.at(-1);
          const lastId = (last.id ?? last.tick_id ?? last.start_id);
          if(lastId!=null) { cursor = Math.max(cursor, lastId+1); progressed=true; }
        }
      }
      if(!progressed) break;
    }
  }

  async function seedAnchors(atId){
    const [mx,md,mn]=await Promise.allSettled([
      j(`/api/labels/max/prev?before_id=${atId}`),
      j(`/api/labels/mid/prev?before_id=${atId}`),
      j(`/api/labels/min/prev?before_id=${atId}`)
    ]);
    const i0=state.x.indexOf(atId);
    if(i0>=0){
      if(mx.status==='fulfilled'&&mx.value?.value!=null) state.max_lbl[i0]=+mx.value.value;
      if(md.status==='fulfilled'&&md.value?.value!=null) state.mid_lbl[i0]=+md.value.value;
      if(mn.status==='fulfilled'&&mn.value?.value!=null) state.min_lbl[i0]=+mn.value.value;
      chart.setOption({
        series:[
          {name:'Max (labels)',data:state.max_lbl},
          {name:'Mid (labels)',data:state.mid_lbl},
          {name:'Min (labels)',data:state.min_lbl}
        ]
      },{lazyUpdate:true});
    }
  }

  async function fetchRange(startId,endId){
    const size=Math.max(1,endId-startId+1);
    const tries=[
      `/api/ticks?from_id=${startId}&to_id=${endId}`,
      `/api/ticks/range?start_id=${startId}&limit=${size}`,
      `/api/ticks/after?since_id=${Math.max(0,startId-1)}&limit=${size}`
    ];
    for(const u of tries){
      try{
        const rows=await j(u);
        if(Array.isArray(rows)&&rows.length) return rows;
      }catch{}
    }
    return [];
  }

  // --- high-level actions ---
  async function loadMaxId(maxRowId){
    status(`loading max#${maxRowId}…`);
    const seg = await j(`/api/maxline/by_id?id=${maxRowId}`);
    if(!seg?.start_id){
      status('not found');
      return;
    }
    state.lastMaxRowId = seg.id;

    // base ticks
    const rows = await fetchRange(seg.start_id, seg.end_id);
    appendTicks(rows);

    // seed + labels
    await seedAnchors(seg.start_id);
    await fetchLabelsWindow(seg.start_id, seg.end_id);

    // draw the leg even if labels are empty
    const sp = seg.start_price ?? seg.start ?? seg.price_start ?? seg.max_start ?? seg.value_start;
    const ep = seg.end_price   ?? seg.end   ?? seg.price_end   ?? seg.max_end   ?? seg.value_end;
    if(sp!=null && ep!=null) overlayMaxSegment(seg.start_id, +sp, seg.end_id, +ep);

    keepRight();
    setSeriesVisibility();
    status('ready');
  }

  async function loadNextMax(){
    if(!state.lastMaxRowId){ status('no current max'); return; }
    const nx = await j(`/api/maxline/next?after_id=${state.lastMaxRowId}`);
    if(!nx?.id){ status('no more'); return; }
    // extend the visible window with next leg ticks
    const rows = await fetchRange(nx.start_id, nx.end_id);
    appendTicks(rows);
    await fetchLabelsWindow(nx.start_id, nx.end_id);
    const sp = nx.start_price ?? nx.start ?? nx.price_start ?? nx.max_start ?? nx.value_start;
    const ep = nx.end_price   ?? nx.end   ?? nx.price_end   ?? nx.max_end   ?? nx.value_end;
    if(sp!=null && ep!=null) overlayMaxSegment(nx.start_id, +sp, nx.end_id, +ep);
    state.lastMaxRowId = nx.id;
    keepRight(); setSeriesVisibility(); status('ready');
  }

  // --- UI wiring ---
  document.getElementById('btnLoadMax').addEventListener('click', async ()=>{
    // reset first
    Object.assign(state,{
      x:[],ts:[],ask:[],bid:[],mid_tick:[],
      max_lbl:[],mid_lbl:[],min_lbl:[], max_seg:[],
      lastId:0, viewSize:3000, lastMaxRowId:null
    });
    chart.clear(); chart.setOption(baseOption(),{notMerge:true});

    const id = parseInt(document.getElementById('maxId').value||'0',10);
    if(!id){ status('enter Max ID'); return; }
    try{
      await loadMaxId(id);
    }catch(e){
      status(`error: ${String(e).slice(0,160)}`);
    }
  });

  document.getElementById('btnMoreMax').addEventListener('click', async ()=>{
    try{ await loadNextMax(); }catch(e){ status(`error: ${String(e).slice(0,160)}`); }
  });

  document.getElementById('btnReset').addEventListener('click', ()=>{
    Object.assign(state,{
      x:[],ts:[],ask:[],bid:[],mid_tick:[],
      max_lbl:[],mid_lbl:[],min_lbl:[], max_seg:[],
      lastId:0, viewSize:3000, lastMaxRowId:null
    });
    chart.clear(); chart.setOption(baseOption(),{notMerge:true});
    status('idle');
  });

  document.querySelectorAll('input[type=checkbox][data-series]')
    .forEach(cb=>cb.addEventListener('change', setSeriesVisibility));
})();
