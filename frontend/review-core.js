(() => {
  const DEFAULT_CHUNK = 20000;  // balanced chunk size

  const el = document.getElementById('chart');
  const chart = echarts.init(el, null, { renderer: 'canvas' });

  const COLORS = {
    ask:'#FF6B6B', mid_tick:'#FFD166', bid:'#4ECDC4',
    max_lbl:'#A78BFA', mid_lbl:'#60A5FA', min_lbl:'#34D399'
  };

  const state = {
    x:[], ts:[],
    ask:[], bid:[], mid_tick:[],
    max_lbl:[], mid_lbl:[], min_lbl:[],
    rightmostId:0,
    viewSize: 3000,
    labelsLoadedUntil: { max: 0, mid: 0, min: 0 },
  };

  function yAxisInt(){ return {
    type:'value',
    min:(e)=>Math.floor(e.min), max:(e)=>Math.ceil(e.max),
    interval:1, minInterval:1,
    axisLabel:{ color:'#9ca3af', formatter:(v)=>Number.isInteger(v)?v:'' },
    axisLine:{ lineStyle:{ color:'#1f2937' } },
    splitLine:{ show:true, lineStyle:{ color:'rgba(148,163,184,0.08)' } },
    scale:false
  };}

  function mkLine(name,key,color,connect=false){
    return { name, type:'line', showSymbol:false, smooth:false,
      data:state[key], itemStyle:{ color }, lineStyle:{ color, width:1.6 }, connectNulls:connect };
  }

  function baseOption(){
    return {
      backgroundColor:'#0b0f14', animation:false,
      grid:{ left:42, right:18, top:10, bottom:28 },
      tooltip:{
        trigger:'axis', axisPointer:{ type:'line' }, backgroundColor:'rgba(17,24,39,.95)',
        formatter:(params)=>{
          if(!params||!params.length) return '';
          const i=params[0].dataIndex, id=state.x[i], ts=state.ts[i]||'';
          const dt=ts?new Date(ts):null, d=dt?`${dt.toLocaleDateString()} ${dt.toLocaleTimeString()}`:'(no time)';
          const out=[`<div><b>ID</b>: ${id}</div><div><b>Time</b>: ${d}</div><hr>`];
          for(const p of params) if(p.value!=null) out.push(
            `<div><span style="display:inline-block;width:8px;height:8px;background:${p.color};margin-right:6px;border-radius:2px"></span>${p.seriesName}: ${p.value}</div>`
          );
          return out.join('');
        }
      },
      xAxis:{ type:'category', data:state.x,
        axisLabel:{ color:'#9ca3af' }, axisLine:{ lineStyle:{ color:'#1f2937' } },
        splitLine:{ show:true, lineStyle:{ color:'rgba(148,163,184,0.08)' } },
      },
      yAxis: yAxisInt(),
      dataZoom:[{ type:'inside' },{ type:'slider', height:16, bottom:4 }],
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
  chart.setOption(baseOption(), { notMerge:true, lazyUpdate:true });

  // Update remembered view when pinned-right and user zooms
  chart.on('dataZoom', () => {
    if (!pinnedRight()) return;
    const [s,e]=currentIndexWindow();
    if (s!=null && e!=null) state.viewSize = Math.max(1, e - s + 1);
  });

  function pinnedRight(){
    const dz=chart.getOption().dataZoom?.[0]; if(!dz) return false;
    if (dz.endValue != null) { const last = state.x.length ? state.x.length - 1 : 0; return dz.endValue >= last; }
    return (dz.end ?? 100) > 99.5;
  }
  function currentIndexWindow(){
    const dz=chart.getOption().dataZoom?.[0];
    if(!dz) return [null,null];
    if(dz.startValue!=null && dz.endValue!=null) return [dz.startValue, dz.endValue];
    const len=state.x.length;
    const s=Math.floor(((dz.start??0)/100)*(len-1));
    const e=Math.floor(((dz.end??100)/100)*(len-1));
    return [s,e];
  }
  function keepRightWithView(){
    const n=Math.max(1, state.viewSize);
    const len=state.x.length;
    const endVal=Math.max(0, len-1);
    const startVal=Math.max(0, endVal-n+1);
    chart.dispatchAction({ type:'dataZoom', startValue:startVal, endValue:endVal });
  }

  async function fetchJSON(u){ const r=await fetch(u); if(!r.ok) throw new Error(await r.text()); return r.json(); }
  function setStatus(s){ const el=document.getElementById('status'); if(el) el.textContent=s; }

  function appendTicks(rows){
    if(!rows||!rows.length) return 0;
    const atRight=pinnedRight();
    for(const r of rows){
      state.x.push(r.id);
      state.ts.push(r.ts||null);
      state.mid_tick.push(r.mid!=null?+r.mid:null);
      state.ask.push(r.ask!=null?+r.ask:null);
      state.bid.push(r.bid!=null?+r.bid:null);
      state.max_lbl.push(null); state.mid_lbl.push(null); state.min_lbl.push(null);
      state.rightmostId=r.id;
    }
    chart.setOption({
      xAxis:{ data:state.x }, yAxis:yAxisInt(),
      series:[
        {name:'Ask',data:state.ask},{name:'Mid (ticks)',data:state.mid_tick},{name:'Bid',data:state.bid},
        {name:'Max (labels)',data:state.max_lbl},{name:'Mid (labels)',data:state.mid_lbl},{name:'Min (labels)',data:state.min_lbl},
      ]
    },{ lazyUpdate:true });
    if(atRight) keepRightWithView();
    return rows.length;
  }

  function overlayLabels(rows,key){
    if(!rows||!rows.length) return;
    const map=new Map(state.x.map((id,i)=>[id,i])); const arr=state[key];
    for(const r of rows){
      const id = r.id ?? r.tick_id ?? r.start_id;
      const val = (r.value ?? r.price ?? r.mid ?? r.start_price);
      if(id==null || val==null) continue;
      const i=map.get(id); if(i!=null) arr[i]=+val;
    }
    chart.setOption({
      series:[
        {name:'Max (labels)',data:state.max_lbl},{name:'Mid (labels)',data:state.mid_lbl},{name:'Min (labels)',data:state.min_lbl},
      ]
    },{ lazyUpdate:true });
  }

  async function fetchLabelsFrom(startId, limit){
    const lim=Math.max(limit,1000);
    const [mx,md,mn]=await Promise.allSettled([
      fetchJSON(`/api/labels/max/range?start_id=${startId}&limit=${lim}`),
      fetchJSON(`/api/labels/mid/range?start_id=${startId}&limit=${lim}`),
      fetchJSON(`/api/labels/min/range?start_id=${startId}&limit=${lim}`),
    ]);
    if(mx.status==='fulfilled') overlayLabels(mx.value,'max_lbl');
    if(md.status==='fulfilled') overlayLabels(md.value,'mid_lbl');
    if(mn.status==='fulfilled') overlayLabels(mn.value,'min_lbl');
    state.labelsLoadedUntil.max = Math.max(state.labelsLoadedUntil.max, state.rightmostId);
    state.labelsLoadedUntil.mid = Math.max(state.labelsLoadedUntil.mid, state.rightmostId);
    state.labelsLoadedUntil.min = Math.max(state.labelsLoadedUntil.min, state.rightmostId);
  }

  async function fetchRange(startId, size){
    const endId = startId + size - 1;
    const tryUrls=[
      `/api/ticks?from_id=${startId}&to_id=${endId}`,
      `/api/ticks/range?start_id=${startId}&limit=${size}`,
      `/api/ticks/latestN?limit=${size}`,
      `/api/ticks/after?since_id=${Math.max(0,startId-1)}&limit=${size}`
    ];
    for(const u of tryUrls){
      try{ const rows=await fetchJSON(u); if(Array.isArray(rows)&&rows.length) return rows; }catch{}
    }
    return [];
  }

  async function loadChunk(startId, size){
    setStatus('loading…');
    const rows = await fetchRange(startId, size);
    const added = appendTicks(rows);
    if(added){
      await fetchLabelsFrom(state.x[0], state.x.length);
      keepRightWithView();
    }
    setStatus(`loaded ${added}`);
  }

  // UI
  function valNum(id, fallback){ const v=parseInt(document.getElementById(id).value||`${fallback}`,10); return Number.isFinite(v)?v:fallback; }

  document.getElementById('btnLoad').addEventListener('click', async ()=>{
    const startId = valNum('startId', 1);
    const size = valNum('chunkSize', DEFAULT_CHUNK);
    Object.assign(state,{
      x:[], ts:[], ask:[], bid:[], mid_tick:[],
      max_lbl:[], mid_lbl:[], min_lbl:[],
      rightmostId:0, viewSize: 3000,
      labelsLoadedUntil:{ max:0, mid:0, min:0 }
    });
    chart.clear(); chart.setOption(baseOption(),{ notMerge:true });
    await loadChunk(startId, size);
  });

  document.getElementById('btnMore').addEventListener('click', async ()=>{
    if(!state.rightmostId) return;
    const size = valNum('chunkSize', DEFAULT_CHUNK);
    await loadChunk(state.rightmostId + 1, size);
  });
})();
