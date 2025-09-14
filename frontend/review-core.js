/* global echarts */
(() => {
  // ===== DOM =====
  const chartEl = document.getElementById("chart");
  const btnLoad = document.getElementById("btnLoad");
  const btnMore = document.getElementById("btnMore");
  const btnReset = document.getElementById("btnReset");
  const rowInput = document.getElementById("rowId");
  const legSel   = document.getElementById("legKind"); // we anchor on 'max'
  const statusEl = document.getElementById("status");

  const cbs = Array.from(document.querySelectorAll('input[type=checkbox][data-series]'));

  // ===== State =====
  const COLORS = {
    ask:  "#FF6B6B",
    mid:  "#FFD166",
    bid:  "#4ECDC4",
    maxZ: "#F472B6",
    midZ: "#F59E0B",
    minZ: "#10B981",
  };

  const S = {
    ask: [], mid: [], bid: [],         // [[id, price], ...]
    maxZ: [], midZ: [], minZ: [],      // broken polylines with null separators
    tsById: new Map(),                 // id -> ts (for tooltip)
    lastMaxRowId: null,
    lastSpanEndId: null,               // last appended max.end_id
    lastZoom: { start: null, end: null }, // for keeping view span on Load More
  };

  // ===== Utils =====
  const setStatus = (t) => { if (statusEl) statusEl.textContent = t; };
  const j = async (u) => { const r = await fetch(u); if(!r.ok) throw new Error(await r.text()); return r.json(); };

  const ALL_SERIES = ["Ask","Mid (ticks)","Bid","Max (labels)","Mid (labels)","Min (labels)","Max Segment","Mid Segment","Min Segment"];
  const KEY_BY_NAME = {
    "Ask":"ask",
    "Mid (ticks)":"mid",
    "Bid":"bid",
    "Max Segment":"maxZ",
    "Mid Segment":"midZ",
    "Min Segment":"minZ",
    "Max (labels)":"max_lbl",
    "Mid (labels)":"mid_lbl",
    "Min (labels)":"min_lbl",
  };

  function yAxisInt(){
    return {
      type:'value',
      min:e=>Math.floor(e.min),
      max:e=>Math.ceil(e.max),
      interval:1, minInterval:1, scale:false,
      axisLabel:{ color:'#9ca3af', formatter:v=>Number.isInteger(v)?v:'' },
      axisLine:{ lineStyle:{ color:'#1f2937' } },
      splitLine:{ show:true, lineStyle:{ color:'rgba(148,163,184,0.08)' } },
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
        formatter: ps => {
          if(!ps?.length) return '';
          const p0 = ps.find(p=>Array.isArray(p.data));
          const id = p0 ? p0.data[0] : null;
          const ts = id!=null ? (S.tsById.get(id)||'') : '';
          const when = ts ? new Date(ts).toLocaleString() : '(no time)';
          const lines = ps.filter(p=>p.value!=null).map(p=>{
            const y = Array.isArray(p.data) ? p.data[1] : p.value;
            return `${p.seriesName}: ${y}`;
          });
          return `ID: ${id}\n\nTime: ${when}\n\n* * *\n` + lines.join('\n');
        }
      },
      xAxis:{ type:'value',
        axisLabel:{ color:'#9ca3af' },
        axisLine:{ lineStyle:{ color:'#1f2937' } },
        splitLine:{ show:true, lineStyle:{ color:'rgba(148,163,184,0.08)' } },
      },
      yAxis: yAxisInt(),
      // IMPORTANT: we will *always* set startValue/endValue explicitly
      dataZoom:[
        { type:'inside', startValue:0, endValue:1 },
        { type:'slider', height:16, bottom:4, startValue:0, endValue:1 }
      ],
      series:[
        { name:'Ask',         type:'line', showSymbol:false, lineStyle:{ color:COLORS.ask, width:1.4 }, data:[] },
        { name:'Mid (ticks)', type:'line', showSymbol:false, lineStyle:{ color:COLORS.mid, width:1.4 }, data:[] },
        { name:'Bid',         type:'line', showSymbol:false, lineStyle:{ color:COLORS.bid, width:1.4 }, data:[] },

        { name:'Max (labels)', type:'line', showSymbol:false, lineStyle:{ width:0.5, opacity:0 }, data:[] },
        { name:'Mid (labels)', type:'line', showSymbol:false, lineStyle:{ width:0.5, opacity:0 }, data:[] },
        { name:'Min (labels)', type:'line', showSymbol:false, lineStyle:{ width:0.5, opacity:0 }, data:[] },

        { name:'Max Segment', type:'line', connectNulls:false, showSymbol:false, lineStyle:{ color:COLORS.maxZ, width:1.8 }, data:[] },
        { name:'Mid Segment', type:'line', connectNulls:false, showSymbol:false, lineStyle:{ color:COLORS.midZ, width:1.6 }, data:[] },
        { name:'Min Segment', type:'line', connectNulls:false, showSymbol:false, lineStyle:{ color:COLORS.minZ, width:1.4 }, data:[] },
      ]
    };
  }

  const chart = echarts.init(chartEl, null, { renderer:'canvas' });
  chart.setOption(baseOption(), { notMerge:true });

  function seriesSet(name, data){
    const opt = chart.getOption();
    const idx = opt.series.findIndex(s => s.name === name);
    if(idx >= 0){ opt.series[idx].data = data; chart.setOption(opt, { notMerge:true }); }
  }

  function applyVisibility(){
    const showMap = Object.fromEntries(cbs.map(cb => [cb.dataset.series, cb.checked]));
    const opt = chart.getOption();
    opt.series.forEach(s=>{
      const key = KEY_BY_NAME[s.name];
      if(!key) return;
      let vis = true;
      switch (key) {
        case 'ask':   vis = !!showMap['ask']; break;
        case 'mid':   vis = !!(showMap['mid_tick'] ?? showMap['mid']); break;
        case 'bid':   vis = !!showMap['bid']; break;
        case 'maxZ':  vis = !!(showMap['max_seg'] ?? showMap['max_segment']); break;
        case 'midZ':  vis = !!(showMap['mid_seg'] ?? showMap['mid_segment']); break;
        case 'minZ':  vis = !!(showMap['min_seg'] ?? showMap['min_segment']); break;
        case 'max_lbl': vis = !!showMap['max_lbl']; break;
        case 'mid_lbl': vis = !!showMap['mid_lbl']; break;
        case 'min_lbl': vis = !!showMap['min_lbl']; break;
      }
      s.itemStyle = s.itemStyle || {};
      s.lineStyle = s.lineStyle || {};
      s.itemStyle.opacity = vis ? 1 : 0;
      s.lineStyle.opacity = vis ? 1 : 0;
    });
    chart.setOption(opt, { notMerge:true, lazyUpdate:true });
  }

  function extentXY(){
    const arrays = [S.ask,S.mid,S.bid,S.maxZ,S.midZ,S.minZ].filter(a=>a && a.length);
    if(!arrays.length) return null;
    let minX=Infinity, maxX=-Infinity, minY=Infinity, maxY=-Infinity;
    for(const arr of arrays){
      for(const pt of arr){
        if(!pt) continue;
        if(pt[0]==null || pt[1]==null) continue;
        const x=pt[0], y=pt[1];
        if(x<minX) minX=x; if(x>maxX) maxX=x;
        if(y<minY) minY=y; if(y>maxY) maxY=y;
      }
    }
    if(!isFinite(minX)||!isFinite(maxX)||minX===Infinity||maxX===-Infinity) return null;
    return { minX, maxX, minY, maxY };
  }

  function zoomToRange(startVal, endVal){
    const opt = chart.getOption();
    // clamp
    if(!(isFinite(startVal) && isFinite(endVal) && endVal>=startVal)) return;
    opt.dataZoom[0].startValue = startVal;
    opt.dataZoom[0].endValue   = endVal;
    opt.dataZoom[1].startValue = startVal;
    opt.dataZoom[1].endValue   = endVal;
    chart.setOption(opt, { notMerge:true });
    S.lastZoom = { start:startVal, end:endVal };
  }

  function zoomKeepRight(newMaxX){
    // keep same span if user was at the right
    const span = (S.lastZoom.start!=null && S.lastZoom.end!=null)
      ? (S.lastZoom.end - S.lastZoom.start)
      : 10000; // default span if unknown
    zoomToRange(Math.max(0, newMaxX - span), newMaxX);
  }

  function polyline(rows){
    const out = [];
    for(const r of rows||[]){
      if(r.start_id==null || r.end_id==null || r.start_price==null || r.end_price==null) continue;
      out.push([+r.start_id, +r.start_price], [+r.end_id, +r.end_price], null);
    }
    return out;
  }

  // ===== Data fetchers =====
  async function fetchMaxById(id){ return j(`/api/max/by_id?id=${id}`); }
  async function fetchNextMax(afterId){ return j(`/api/max/next?after_id=${afterId}`); }
  async function lastTickId(){ const r = await j('/api/ticks/last_id'); return r?.last_id ?? null; }

  async function fetchTicksWindow(fromId, toId){
    const OUT = { ask:[], mid:[], bid:[], tsById:new Map() };
    if(toId < fromId){ const t=fromId; fromId=toId; toId=t; }
    let cursor = fromId - 1;
    while(true){
      const limit = Math.min(20000, toId - cursor);
      if(limit <= 0) break;
      const rows = await j(`/api/ticks/after?since_id=${cursor}&limit=${limit}`);
      if(!Array.isArray(rows) || rows.length===0) break;
      for(const r of rows){
        const id = r.id;
        if(id < fromId) continue;
        if(id > toId){ cursor = toId; break; }
        if(r.ask!=null) OUT.ask.push([id, +r.ask]);
        if(r.mid!=null) OUT.mid.push([id, +r.mid]);
        if(r.bid!=null) OUT.bid.push([id, +r.bid]);
        const ts = r.ts || r.timestamp || null;
        if(ts) OUT.tsById.set(id, ts);
        cursor = id;
      }
      if(cursor >= toId) break;
    }
    return OUT;
  }

  async function fetchZigsWindow(fromId, toId){
    // unified endpoint that returns rows with .kind in {'max','mid','min'}
    const rows = await j(`/api/zigzag?from_id=${fromId}&to_id=${toId}`);
    const out = { max:[], mid:[], min:[] };
    if(Array.isArray(rows)){
      for(const r of rows){
        const k = (r.kind||'').toLowerCase();
        if(out[k]) out[k].push(r);
      }
    }
    return out;
  }

  // ===== Flows =====
  async function loadByMaxRow(rowId){
    setStatus(`loading max#${rowId}…`);
    // 1) anchor window
    const seg = await fetchMaxById(rowId);
    const fromId = seg.start_id, toId = seg.end_id;
    S.lastMaxRowId = seg.id;
    S.lastSpanEndId = toId;

    // 2) zigs first (fast feedback)
    const zz = await fetchZigsWindow(fromId, toId);
    S.maxZ = polyline(zz.max);
    S.midZ = polyline(zz.mid);
    S.minZ = polyline(zz.min);

    // 3) ticks for same window
    const T = await fetchTicksWindow(fromId, toId);
    S.ask = T.ask; S.mid = T.mid; S.bid = T.bid;
    T.tsById.forEach((v,k)=>S.tsById.set(k,v));

    // 4) paint all
    const opt = chart.getOption();
    opt.series.find(s=>s.name==='Ask').data = S.ask;
    opt.series.find(s=>s.name==='Mid (ticks)').data = S.mid;
    opt.series.find(s=>s.name==='Bid').data = S.bid;
    opt.series.find(s=>s.name==='Max Segment').data = S.maxZ;
    opt.series.find(s=>s.name==='Mid Segment').data = S.midZ;
    opt.series.find(s=>s.name==='Min Segment').data = S.minZ;
    chart.setOption(opt, { notMerge:true });

    // 5) compute extents and set absolute zoom
    const ex = extentXY();
    if(ex){
      // pad a little on Y
      const padY = Math.max(0.5, (ex.maxY - ex.minY) * 0.08);
      const yOpt = chart.getOption().yAxis;
      yOpt[0].min = Math.floor(ex.minY - padY);
      yOpt[0].max = Math.ceil(ex.maxY + padY);
      chart.setOption({ yAxis: yOpt }, { notMerge:true });

      zoomToRange(ex.minX, ex.maxX); // <-- key fix for value axis
    }

    applyVisibility();
    setStatus('ready');
  }

  async function loadMore(){
    if(!S.lastMaxRowId){ setStatus('nothing loaded'); return; }

    // try next max
    try{
      const nx = await fetchNextMax(S.lastMaxRowId);
      if(nx?.id){
        const fromId = nx.start_id, toId = nx.end_id;

        // ticks
        const T = await fetchTicksWindow(fromId, toId);
        S.ask = S.ask.concat(T.ask);
        S.mid = S.mid.concat(T.mid);
        S.bid = S.bid.concat(T.bid);
        T.tsById.forEach((v,k)=>S.tsById.set(k,v));

        // zigs
        const zz = await fetchZigsWindow(fromId, toId);
        S.maxZ = S.maxZ.concat(polyline(zz.max));
        S.midZ = S.midZ.concat(polyline(zz.mid));
        S.minZ = S.minZ.concat(polyline(zz.min));

        S.lastMaxRowId = nx.id;
        S.lastSpanEndId = toId;

        chart.setOption({
          series:[
            { name:'Ask', data:S.ask },
            { name:'Mid (ticks)', data:S.mid },
            { name:'Bid', data:S.bid },
            { name:'Max Segment', data:S.maxZ },
            { name:'Mid Segment', data:S.midZ },
            { name:'Min Segment', data:S.minZ },
          ]
        }, { lazyUpdate:true });

        const ex = extentXY();
        if(ex) zoomKeepRight(ex.maxX);
        applyVisibility();
        setStatus('ready');
        return;
      }
    }catch(_){ /* fallthrough */ }

    // tail mode: fill to last tick and overlay smaller legs
    setStatus('tail…');
    const last = await lastTickId();
    if(!last || !S.lastSpanEndId){ setStatus('ready'); return; }

    let cursor = S.lastSpanEndId + 1;
    while(cursor <= last){
      const chunk = Math.min(20000, last - cursor + 1);
      const rows = await j(`/api/ticks/after?since_id=${cursor-1}&limit=${chunk}`);
      if(!rows?.length) break;
      for(const r of rows){
        const id=r.id;
        if(r.ask!=null) S.ask.push([id,+r.ask]);
        if(r.mid!=null) S.mid.push([id,+r.mid]);
        if(r.bid!=null) S.bid.push([id,+r.bid]);
        const ts = r.ts || r.timestamp || null;
        if(ts) S.tsById.set(id, ts);
        cursor = id + 1;
      }
    }

    const zzTail = await fetchZigsWindow(S.lastSpanEndId, last);
    S.maxZ = S.maxZ.concat(polyline(zzTail.max));
    S.midZ = S.midZ.concat(polyline(zzTail.mid));
    S.minZ = S.minZ.concat(polyline(zzTail.min));

    chart.setOption({
      series:[
        { name:'Ask', data:S.ask },
        { name:'Mid (ticks)', data:S.mid },
        { name:'Bid', data:S.bid },
        { name:'Max Segment', data:S.maxZ },
        { name:'Mid Segment', data:S.midZ },
        { name:'Min Segment', data:S.minZ },
      ]
    }, { lazyUpdate:true });

    const ex = extentXY();
    if(ex) zoomKeepRight(ex.maxX);
    applyVisibility();
    setStatus('ready');
  }

  function resetAll(){
    S.ask=[]; S.mid=[]; S.bid=[];
    S.maxZ=[]; S.midZ=[]; S.minZ=[];
    S.tsById.clear();
    S.lastMaxRowId=null;
    S.lastSpanEndId=null;
    S.lastZoom={start:null,end:null};
    chart.clear();
    chart.setOption(baseOption(), { notMerge:true });
    setStatus('idle');
  }

  // ===== Wire UI =====
  if(btnLoad){
    btnLoad.addEventListener('click', async ()=>{
      const row = parseInt(rowInput?.value || '0', 10);
      const leg = (legSel?.value || 'max').toLowerCase();
      if(!row){ setStatus('enter Row ID'); return; }
      if(leg !== 'max'){
        // mid/min row ids don't map to max; we always anchor by max row id then fetch overlap by tick range
        setStatus('Anchoring by Max row id; fetching corresponding mid/min via tick range…');
      }
      try { resetAll(); await loadByMaxRow(row); } catch(e){ setStatus(`error: ${String(e).slice(0,160)}`); }
    });
  }
  if(btnMore)  btnMore.addEventListener('click', ()=>loadMore().catch(e=>setStatus(String(e).slice(0,160))));
  if(btnReset) btnReset.addEventListener('click', resetAll);
  cbs.forEach(cb => cb.addEventListener('change', applyVisibility));
})();
