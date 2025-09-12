(() => {
  const el = document.getElementById('chart');
  const chart = echarts.init(el, null, { renderer: 'canvas' });

  const COLORS = {
    ask:'#FF6B6B', mid_tick:'#FFD166', bid:'#4ECDC4',
    max_lbl:'#A78BFA', mid_lbl:'#60A5FA', min_lbl:'#34D399'
  };

  const state = {
    x:[], ts:[], ask:[], bid:[], mid_tick:[],
    max_lbl:[], mid_lbl:[], min_lbl:[], rightmostId:0
  };

  function yAxisInt(){ return {
    type:'value',
    min:(e)=>Math.floor(e.min),
    max:(e)=>Math.ceil(e.max),
    interval:1, minInterval:1,
    axisLabel:{ color:'#9ca3af', formatter:(v)=>Number.isInteger(v)?v:'' },
    axisLine:{ lineStyle:{ color:'#1f2937' } },
    splitLine:{ show:true, lineStyle:{ color:'rgba(148,163,184,0.08)' } },
    scale:false
  };}

  function mkLine(name,key,color){
    return { name, type:'line', showSymbol:false, smooth:false,
      data:state[key], itemStyle:{ color }, lineStyle:{ color, width:1.6 }, connectNulls:false };
  }

  function baseOption(){
    return {
      backgroundColor:'#0b0f14', animation:false,
      grid:{ left:42, right:18, top:10, bottom:28 },
      tooltip:{
        trigger:'axis', axisPointer:{ type:'line' },
        backgroundColor:'rgba(17,24,39,.95)',
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
        axisLabel:{ color:'#9ca3af' },
        axisLine:{ lineStyle:{ color:'#1f2937' } },
        splitLine:{ show:true, lineStyle:{ color:'rgba(148,163,184,0.08)' } },
      },
      yAxis: yAxisInt(),
      dataZoom:[{ type:'inside' },{ type:'slider', height:16, bottom:4 }],
      series:[
        mkLine('Ask','ask',COLORS.ask),
        mkLine('Mid (ticks)','mid_tick',COLORS.mid_tick),
        mkLine('Bid','bid',COLORS.bid),
        mkLine('Max (labels)','max_lbl',COLORS.max_lbl),
        mkLine('Mid (labels)','mid_lbl',COLORS.mid_lbl),
        mkLine('Min (labels)','min_lbl',COLORS.min_lbl),
      ]
    };
  }
  chart.setOption(baseOption(), { notMerge:true, lazyUpdate:true });

  function pinnedRight(){ const dz=chart.getOption().dataZoom?.[0]; if(!dz) return false; return (dz.end??100) > 99.5; }
  function stickToRight(){ chart.dispatchAction({ type:'dataZoom', start:100, end:100 }); }

  function setSeriesVisibility(){
    const boxes=document.querySelectorAll('input[type=checkbox][data-series]');
    const show={}; boxes.forEach(b=>show[b.dataset.series]=b.checked);
    const opt=chart.getOption();
    opt.series.forEach(s=>{
      const key=keyFromName(s.name); s.data=state[key];
      const vis=show[key]; s.itemStyle.opacity=vis?1:0; s.lineStyle.opacity=vis?1:0;
    });
    chart.setOption(opt,{ notMerge:true, lazyUpdate:true });
  }
  document.querySelectorAll('input[type=checkbox][data-series]')
    .forEach(cb=>cb.addEventListener('change', setSeriesVisibility));
  function keyFromName(n){
    if(n==='Ask')return'ask'; if(n==='Bid')return'bid'; if(n==='Mid (ticks)')return'mid_tick';
    if(n==='Max (labels)')return'max_lbl'; if(n==='Mid (labels)')return'mid_lbl'; if(n==='Min (labels)')return'min_lbl';
  }

  async function fetchJSON(u){ const r=await fetch(u); if(!r.ok) throw new Error(await r.text()); return r.json(); }
  function setStatus(s){ document.getElementById('status').textContent=s; }

  function appendTicks(rows){
    if(!rows||!rows.length) return;
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
    if(atRight) stickToRight();
  }

  function overlayLabels(rows,key){
    if(!rows||!rows.length) return;
    const map=new Map(state.x.map((id,i)=>[id,i])); const arr=state[key];
    for(const r of rows){ const i=map.get(r.id); if(i!=null) arr[i]=(r.value!=null?+r.value:null); }
    chart.setOption({
      series:[
        {name:'Max (labels)',data:state.max_lbl},{name:'Mid (labels)',data:state.mid_lbl},{name:'Min (labels)',data:state.min_lbl},
      ]
    },{ lazyUpdate:true });
  }

  async function fetchRange(startId, size){
    const endId = startId + size - 1;
    const tryUrls=[
      `/api/ticks?from_id=${startId}&to_id=${endId}`,
      `/api/ticks/range?start_id=${startId}&limit=${size}`,
      `/api/ticks/latestN?limit=${size}`,
      `/api/ticks/after?since_id=${Math.max(0,startId-1)}&limit=${size}`
    ];
    for (const u of tryUrls) {
      try {
        const rows = await fetchJSON(u);
        if (Array.isArray(rows) && rows.length) return rows;
      } catch { /* next */ }
    }
    return [];
  }

  async function loadChunk(startId, size){
    setStatus('loading…');
    const rows = await fetchRange(startId, size);
    appendTicks(rows);
    if(rows.length){
      await Promise.all([
        fetchJSON(`/api/labels/max/range?start_id=${state.x[0]}&limit=${state.x.length}`).then(r=>overlayLabels(r,'max_lbl')).catch(()=>{}),
        fetchJSON(`/api/labels/mid/range?start_id=${state.x[0]}&limit=${state.x.length}`).then(r=>overlayLabels(r,'mid_lbl')).catch(()=>{}),
        fetchJSON(`/api/labels/min/range?start_id=${state.x[0]}&limit=${state.x.length}`).then(r=>overlayLabels(r,'min_lbl')).catch(()=>{}),
      ]);
    }
    setSeriesVisibility();
    setStatus(`loaded ${rows.length}`);
  }

  document.getElementById('btnLoad').addEventListener('click', async ()=>{
    const startId = parseInt(document.getElementById('startId').value||'1',10);
    const size = parseInt(document.getElementById('chunkSize').value||'20000',10);
    Object.assign(state,{ x:[], ts:[], ask:[], bid:[], mid_tick:[], max_lbl:[], mid_lbl:[], min_lbl:[], rightmostId:0 });
    chart.clear(); chart.setOption(baseOption(),{ notMerge:true });
    await loadChunk(startId, size);
  });

  document.getElementById('btnMore').addEventListener('click', async ()=>{
    if(!state.rightmostId) return;
    const size = parseInt(document.getElementById('chunkSize').value||'20000',10);
    await loadChunk(state.rightmostId+1, size);
  });

  // empty until user loads a range
})();
