/* global echarts */
(() => {
  // ---------- DOM ----------
  const chartEl = document.getElementById("chart");
  const btnLoad = document.getElementById("btnLoad");
  const btnMore = document.getElementById("btnMore");
  const btnReset = document.getElementById("btnReset");
  const rowInput = document.getElementById("rowId");
  const legSel   = document.getElementById("legKind"); // we anchor by 'max'
  const statusEl = document.getElementById("status");

  const cbs = Array.from(document.querySelectorAll('input[type=checkbox][data-series]'));

  // ---------- state ----------
  const COLORS = {
    ask:  "#FF6B6B",
    mid:  "#FFD166",
    bid:  "#4ECDC4",
    maxZ: "#F472B6",
    midZ: "#F59E0B",
    minZ: "#10B981",
  };

  const S = {
    ask: [], mid: [], bid: [],       // tick lines: [[id,price],...]
    maxZ: [], midZ: [], minZ: [],    // polyline legs: [[sid,spr],[eid,epr],null,...]
    tsById: new Map(),               // id -> timestamp
    lastMaxRowId: null,
    lastSpanEndId: null,
    lastZoomSpan: null,              // number of x units to keep when appending
  };

  // ---------- utils ----------
  const setStatus = (t) => { if (statusEl) statusEl.textContent = String(t); };
  const j = async (u) => { const r = await fetch(u); if(!r.ok) throw new Error(await r.text()); return r.json(); };

  function yAxisInt(minY, maxY){
    const pad = Math.max(0.5, (maxY - minY) * 0.08 || 1);
    const lo = Math.floor((minY ?? 0) - pad);
    const hi = Math.ceil ((maxY ?? 1) + pad);
    return {
      type:'value', min:lo, max:hi,
      interval:1, minInterval:1, scale:false,
      axisLabel:{ color:'#9ca3af', formatter:v=>Number.isInteger(v)?v:'' },
      axisLine:{ lineStyle:{ color:'#1f2937' } },
      splitLine:{ show:true, lineStyle:{ color:'rgba(148,163,184,0.08)' } },
    };
  }

  function visibilityMap(){
    const m = {};
    cbs.forEach(cb => m[cb.dataset.series] = cb.checked);
    return m;
  }

  function extentXY(){
    const arrays = [S.ask,S.mid,S.bid,S.maxZ,S.midZ,S.minZ].filter(a => a && a.length);
    if(!arrays.length) return null;
    let minX=Infinity,maxX=-Infinity,minY=Infinity,maxY=-Infinity;
    for(const arr of arrays){
      for(const p of arr){
        if(!p || p[0]==null || p[1]==null) continue;
        const x=p[0], y=p[1];
        if(x<minX) minX=x; if(x>maxX) maxX=x;
        if(y<minY) minY=y; if(y>maxY) maxY=y;
      }
    }
    if(!isFinite(minX) || !isFinite(maxX)) return null;
    return {minX,maxX,minY,maxY};
  }

  function polyline(rows){
    const out=[];
    for(const r of rows||[]){
      if(r.start_id==null || r.end_id==null || r.start_price==null || r.end_price==null) continue;
      out.push([+r.start_id, +r.start_price], [+r.end_id, +r.end_price], null);
    }
    return out;
  }

  // ---------- data ----------
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
        if(r.ask!=null) OUT.ask.push([id,+r.ask]);
        if(r.mid!=null) OUT.mid.push([id,+r.mid]);
        if(r.bid!=null) OUT.bid.push([id,+r.bid]);
        const ts = r.ts || r.timestamp; if(ts) OUT.tsById.set(id, ts);
        cursor = id;
      }
      if(cursor >= toId) break;
    }
    return OUT;
  }

  // unified zigzag (returns rows with .kind in {'max','mid','min'})
  async function fetchZigsWindow(fromId, toId){
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

  // ---------- rendering ----------
  const chart = echarts.init(chartEl, null, { renderer:'canvas' });

  function fullOption(){
    const vis = visibilityMap();
    return {
      backgroundColor:'#0b0f14',
      animation:false,
      grid:{ left:42, right:18, top:10, bottom:28 },
      tooltip:{
        trigger:'axis', axisPointer:{type:'line'},
        backgroundColor:'rgba(17,24,39,.95)',
        formatter: (ps)=>{
          if(!ps?.length) return '';
          const p0 = ps.find(p=>Array.isArray(p.data));
          const id = p0 ? p0.data[0] : null;
          const ts = id!=null ? S.tsById.get(id) : null;
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
      yAxis: yAxisInt(ext?.minY, ext?.maxY),
      dataZoom:[
        { type:'inside', startValue: dz.start, endValue: dz.end },
        { type:'slider', height:16, bottom:4, startValue: dz.start, endValue: dz.end },
      ],
      series:[
        { name:'Ask',         type:'line', showSymbol:false, lineStyle:{ color:COLORS.ask,  width:1.4, opacity: vis.ask?1:0 },  itemStyle:{ opacity: vis.ask?1:0 },  data:S.ask },
        { name:'Mid (ticks)', type:'line', showSymbol:false, lineStyle:{ color:COLORS.mid,  width:1.4, opacity: (vis.mid_tick??vis.mid)?1:0 }, itemStyle:{ opacity: (vis.mid_tick??vis.mid)?1:0 }, data:S.mid },
        { name:'Bid',         type:'line', showSymbol:false, lineStyle:{ color:COLORS.bid,  width:1.4, opacity: vis.bid?1:0 }, itemStyle:{ opacity: vis.bid?1:0 }, data:S.bid },

        { name:'Max (labels)', type:'line', showSymbol:false, lineStyle:{ width:0.5, opacity: vis.max_lbl?1:0 }, data:[] },
        { name:'Mid (labels)', type:'line', showSymbol:false, lineStyle:{ width:0.5, opacity: vis.mid_lbl?1:0 }, data:[] },
        { name:'Min (labels)', type:'line', showSymbol:false, lineStyle:{ width:0.5, opacity: vis.min_lbl?1:0 }, data:[] },

        { name:'Max Segment', type:'line', connectNulls:false, showSymbol:false, lineStyle:{ color:COLORS.maxZ, width:1.8, opacity:(vis.max_seg??true)?1:0 }, data:S.maxZ },
        { name:'Mid Segment', type:'line', connectNulls:false, showSymbol:false, lineStyle:{ color:COLORS.midZ, width:1.6, opacity:(vis.mid_seg??true)?1:0 }, data:S.midZ },
        { name:'Min Segment', type:'line', connectNulls:false, showSymbol:false, lineStyle:{ color:COLORS.minZ, width:1.4, opacity:(vis.min_seg??true)?1:0 }, data:S.minZ },
      ]
    };
  }

  // locals used by fullOption
  let ext = null;
  let dz  = { start: 0, end: 1 };

  function computeZoomToData(){
    ext = extentXY();
    if(!ext){ dz = { start: 0, end: 1 }; return; }
    // preserve span if we already had one (Load More), otherwise fit window
    if (S.lastZoomSpan != null) {
      const span = S.lastZoomSpan;
      dz.end   = ext.maxX;
      dz.start = Math.max(ext.minX, ext.maxX - span);
    } else {
      dz.start = ext.minX;
      dz.end   = ext.maxX;
      S.lastZoomSpan = dz.end - dz.start;
    }
  }

  function renderAll(){
    computeZoomToData();
    chart.clear();
    chart.setOption(fullOption(), { notMerge:true });
  }

  // ---------- flows ----------
  async function loadByMaxRow(rowId){
    setStatus(`loading max#${rowId}…`);
    const seg = await fetchMaxById(rowId);
    const fromId = seg.start_id, toId = seg.end_id;
    S.lastMaxRowId = seg.id;
    S.lastSpanEndId = toId;

    // zigs first (fast)
    const zz = await fetchZigsWindow(fromId, toId);
    S.maxZ = polyline(zz.max);
    S.midZ = polyline(zz.mid);
    S.minZ = polyline(zz.min);

    // ticks
    const T = await fetchTicksWindow(fromId, toId);
    S.ask = T.ask; S.mid = T.mid; S.bid = T.bid;
    T.tsById.forEach((v,k)=>S.tsById.set(k,v));

    renderAll();
    setStatus('ready');
  }

  async function loadMore(){
    if(!S.lastMaxRowId){ setStatus('nothing loaded'); return; }
    try{
      const nx = await fetchNextMax(S.lastMaxRowId);
      if(nx?.id){
        const fromId = nx.start_id, toId = nx.end_id;

        const T = await fetchTicksWindow(fromId, toId);
        S.ask = S.ask.concat(T.ask);
        S.mid = S.mid.concat(T.mid);
        S.bid = S.bid.concat(T.bid);
        T.tsById.forEach((v,k)=>S.tsById.set(k,v));

        const zz = await fetchZigsWindow(fromId, toId);
        S.maxZ = S.maxZ.concat(polyline(zz.max));
        S.midZ = S.midZ.concat(polyline(zz.mid));
        S.minZ = S.minZ.concat(polyline(zz.min));

        S.lastMaxRowId = nx.id;
        S.lastSpanEndId = toId;

        renderAll();
        setStatus('ready');
        return;
      }
    }catch(_){ /* fall to tail */ }

    // tail mode: extend to last tick and overlay remaining legs
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
        const ts = r.ts || r.timestamp; if(ts) S.tsById.set(id, ts);
        cursor = id + 1;
      }
    }

    const zzTail = await fetchZigsWindow(S.lastSpanEndId, last);
    S.maxZ = S.maxZ.concat(polyline(zzTail.max));
    S.midZ = S.midZ.concat(polyline(zzTail.mid));
    S.minZ = S.minZ.concat(polyline(zzTail.min));

    renderAll();
    setStatus('ready');
  }

  function resetAll(){
    S.ask=[]; S.mid=[]; S.bid=[];
    S.maxZ=[]; S.midZ=[]; S.minZ=[];
    S.tsById.clear();
    S.lastMaxRowId=null;
    S.lastSpanEndId=null;
    S.lastZoomSpan=null;
    ext=null; dz={start:0,end:1};
    chart.clear();
    chart.setOption(fullOption(), { notMerge:true });
    setStatus('idle');
  }

  // ---------- wire ----------
  if(btnLoad){
    btnLoad.addEventListener('click', async ()=>{
      const row = parseInt(rowInput?.value || '0', 10);
      const leg = (legSel?.value || 'max').toLowerCase();
      if(!row){ setStatus('enter Row ID'); return; }
      if(leg !== 'max'){ setStatus('Anchoring by Max row id; fetching mid/min by tick range…'); }
      try { resetAll(); await loadByMaxRow(row); } catch(e){ setStatus(`error: ${String(e).slice(0,160)}`); }
    });
  }
  if(btnMore)  btnMore.addEventListener('click', ()=>loadMore().catch(e=>setStatus(`error: ${String(e).slice(0,160)}`)));
  if(btnReset) btnReset.addEventListener('click', resetAll);
  cbs.forEach(cb => cb.addEventListener('change', ()=>renderAll()));

  // initial empty frame
  renderAll();
})();
