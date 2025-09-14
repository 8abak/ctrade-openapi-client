/* global echarts */
(() => {
  // ====== DOM ======
  const chartEl = document.getElementById("chart");
  const btnLoad = document.getElementById("btnLoad");
  const btnMore = document.getElementById("btnMore");
  const btnReset = document.getElementById("btnReset");
  const rowInput = document.getElementById("rowId");       // numeric
  const legSel   = document.getElementById("legKind");     // expects 'max' | 'mid' | 'min' (we anchor on 'max')
  const statusEl = document.getElementById("status");

  const seriesCbs = Array.from(
    document.querySelectorAll('input[type=checkbox][data-series]')
  );

  // ====== State ======
  const COLORS = {
    ask:  "#FF6B6B",
    mid:  "#FFD166",
    bid:  "#4ECDC4",
    maxZ: "#F472B6",
    midZ: "#F59E0B",
    minZ: "#10B981",
  };

  const S = {
    ask: [], mid: [], bid: [],         // tick lines -> [[id,price],...]
    maxZ: [], midZ: [], minZ: [],      // zigzag polylines with null breaks
    tsById: new Map(),                 // id -> ISO ts (for tooltip)
    lastMaxRowId: null,                // last loaded max row id (for Load More)
    lastSpanEndId: null,               // end_id of the last max span we appended
    viewSize: 3000
  };

  // ====== Helpers ======
  const setStatus = (t) => { if (statusEl) statusEl.textContent = t; };
  const j = async (u) => { const r = await fetch(u); if (!r.ok) throw new Error(await r.text()); return r.json(); };

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
        formatter: (ps)=>{
          if(!ps?.length) return '';
          const p0 = ps.find(p => Array.isArray(p.data));
          const id = p0 ? p0.data[0] : null;
          const ts = id!=null ? S.tsById.get(id) : null;
          const d  = ts ? new Date(ts).toLocaleString() : '(no time)';
          const lines = ps.filter(p=>p.value!=null).map(p=>{
            // For line series with [x,y], p.data[1] is the price
            const y = Array.isArray(p.data) ? p.data[1] : p.value;
            return `${p.seriesName}: ${y}`;
          });
          return `ID: ${id}\n\nTime: ${d}\n\n* * *\n` + lines.join('\n');
        }
      },
      xAxis:{
        type:'value', // tick id
        axisLabel:{ color:'#9ca3af' },
        axisLine:{ lineStyle:{ color:'#1f2937' } },
        splitLine:{ show:true, lineStyle:{ color:'rgba(148,163,184,0.08)' } }
      },
      yAxis: yAxisInt(),
      dataZoom:[{type:'inside'},{type:'slider',height:16,bottom:4}],
      series:[
        { name:'Ask',         type:'line', showSymbol:false, data:S.ask,  lineStyle:{ color:COLORS.ask,  width:1.4 } },
        { name:'Mid (ticks)', type:'line', showSymbol:false, data:S.mid,  lineStyle:{ color:COLORS.mid,  width:1.4 } },
        { name:'Bid',         type:'line', showSymbol:false, data:S.bid,  lineStyle:{ color:COLORS.bid,  width:1.4 } },
        // label series placeholders (kept so your checkboxes don’t break)
        { name:'Max (labels)', type:'line', data:[], showSymbol:false, lineStyle:{ width:0.5, opacity:0 } },
        { name:'Mid (labels)', type:'line', data:[], showSymbol:false, lineStyle:{ width:0.5, opacity:0 } },
        { name:'Min (labels)', type:'line', data:[], showSymbol:false, lineStyle:{ width:0.5, opacity:0 } },
        // zigzags:
        { name:'Max Segment', type:'line', connectNulls:false, showSymbol:false, data:S.maxZ, lineStyle:{ color:COLORS.maxZ, width:1.8 } },
        { name:'Mid Segment', type:'line', connectNulls:false, showSymbol:false, data:S.midZ, lineStyle:{ color:COLORS.midZ, width:1.6 } },
        { name:'Min Segment', type:'line', connectNulls:false, showSymbol:false, data:S.minZ, lineStyle:{ color:COLORS.minZ, width:1.4 } },
      ]
    };
  }

  const chart = echarts.init(chartEl, null, { renderer: 'canvas' });
  chart.setOption(baseOption(), { notMerge:true });

  function applyVisibility(){
    const show = Object.fromEntries(seriesCbs.map(cb => [cb.dataset.series, cb.checked]));
    const opt = chart.getOption();
    opt.series.forEach(s=>{
      const key = (
        s.name==='Ask'           ? 'ask'  :
        s.name==='Mid (ticks)'   ? 'mid'  :
        s.name==='Bid'           ? 'bid'  :
        s.name==='Max Segment'   ? 'maxZ' :
        s.name==='Mid Segment'   ? 'midZ' :
        s.name==='Min Segment'   ? 'minZ' :
        s.name==='Max (labels)'  ? 'max_lbl' :
        s.name==='Mid (labels)'  ? 'mid_lbl' :
        s.name==='Min (labels)'  ? 'min_lbl' : null
      );
      // map checkbox keys in your HTML to our series arrays:
      const vis = (
        key==='ask'   ? show.ask :
        key==='mid'   ? (show['mid_tick'] ?? show.mid) :
        key==='bid'   ? show.bid :
        key==='maxZ'  ? (show['max_seg'] ?? show['max_segment'] ?? show['Max Segment'.toLowerCase()]) :
        key==='midZ'  ? (show['mid_seg'] ?? show['mid_segment'] ?? show['Mid Segment'.toLowerCase()]) :
        key==='minZ'  ? (show['min_seg'] ?? show['min_segment'] ?? show['Min Segment'.toLowerCase()]) :
        key==='max_lbl' ? (show['max_lbl'] ?? show['Max (labels)'.toLowerCase()]) :
        key==='mid_lbl' ? (show['mid_lbl'] ?? show['Mid (labels)'.toLowerCase()]) :
        key==='min_lbl' ? (show['min_lbl'] ?? show['Min (labels)'.toLowerCase()]) :
        false
      );
      s.itemStyle = s.itemStyle || {};
      s.lineStyle = s.lineStyle || {};
      s.itemStyle.opacity = vis ? 1 : 0;
      s.lineStyle.opacity = vis ? 1 : 0;
    });
    chart.setOption(opt, { notMerge:true, lazyUpdate:true });
  }

  function keepRight(){
    const opt = chart.getOption();
    const dz = opt.dataZoom?.[0];
    if(!dz) return;
    const end = (dz.endValue != null) ? dz.endValue : dz.end;
    if(end != null && typeof end === 'number' && end < 99.5) return;

    const maxX = Math.max(
      ...[S.ask,S.mid,S.bid,S.maxZ,S.midZ,S.minZ]
        .filter(a => a && a.length)
        .map(a => a.reduce((m,p)=> (p && p[0]!=null) ? Math.max(m,p[0]) : m, -Infinity)),
      -Infinity
    );
    if (isFinite(maxX)) {
      const span = Math.max(1,S.viewSize);
      chart.dispatchAction({ type:'dataZoom', startValue:Math.max(0,maxX-span+1), endValue:maxX });
    }
  }

  // ====== Data layer ======
  async function fetchMaxById(id){
    return j(`/api/max/by_id?id=${id}`); // id,start_id,end_id,start_price,end_price
  }
  async function fetchNextMax(afterId){
    return j(`/api/max/next?after_id=${afterId}`);
  }
  async function lastTickId(){
    const r = await j(`/api/ticks/last_id`);
    return r?.last_id ?? null;
  }

  // Chunked ticks in [from..to]
  async function fetchTicksWindow(fromId, toId){
    const out = { ask:[], mid:[], bid:[], tsById:new Map() };
    if (toId < fromId) { const t=fromId; fromId=toId; toId=t; }
    let cursor = fromId - 1;
    while (true) {
      const limit = Math.min(20000, toId - cursor);
      if (limit <= 0) break;
      const rows = await j(`/api/ticks/after?since_id=${cursor}&limit=${limit}`);
      if (!Array.isArray(rows) || rows.length===0) break;
      for (const r of rows) {
        const id = r.id;
        if (id < fromId) continue;
        if (id > toId) { cursor = toId; break; }
        if (r.ask != null) out.ask.push([id, +r.ask]);
        if (r.mid != null) out.mid.push([id, +r.mid]);
        if (r.bid != null) out.bid.push([id, +r.bid]);
        if (r.ts) out.tsById.set(id, r.ts);
        cursor = id;
      }
      if (cursor >= toId) break;
    }
    return out;
  }

  // /api/zigzag?from_id=&to_id=[&kind] returns flat rows with a 'kind' field
  async function fetchZigsWindow(fromId, toId){
    const rows = await j(`/api/zigzag?from_id=${fromId}&to_id=${toId}`);
    const out = { max:[], mid:[], min:[] };
    for (const r of (rows||[])) {
      const k = (r.kind||'').toLowerCase();
      if (out[k]) out[k].push(r);
    }
    return out;
  }

  function polyline(segments){
    // [ [sid, sprice], [eid, eprice], null, ... ]
    const a = [];
    for (const s of segments||[]) {
      if (s.start_id==null || s.end_id==null || s.start_price==null || s.end_price==null) continue;
      a.push([+s.start_id, +s.start_price], [+s.end_id, +s.end_price], null);
    }
    return a;
  }

  // ====== Flows ======
  async function loadByMaxRow(rowId){
    setStatus(`loading max#${rowId}…`);
    // 1) anchor window by max row
    const seg = await fetchMaxById(rowId); // :contentReference[oaicite:1]{index=1}
    const fromId = seg.start_id, toId = seg.end_id;
    S.lastMaxRowId = seg.id;
    S.lastSpanEndId = toId;

    // 2) fetch all three zig kinds in that window
    const zz = await fetchZigsWindow(fromId, toId);       // :contentReference[oaicite:2]{index=2}
    S.maxZ = polyline(zz.max);
    S.midZ = polyline(zz.mid);
    S.minZ = polyline(zz.min);

    // 3) ticks (optional – shown by default because the checkboxes at bottom are on)
    const T = await fetchTicksWindow(fromId, toId);       // :contentReference[oaicite:3]{index=3}
    S.ask = T.ask; S.mid = T.mid; S.bid = T.bid;
    T.tsById.forEach((v,k)=>S.tsById.set(k,v));

    // 4) render
    chart.setOption({
      xAxis:[{type:'value'}],
      yAxis:yAxisInt(),
      series:[
        { name:'Ask', data:S.ask },
        { name:'Mid (ticks)', data:S.mid },
        { name:'Bid', data:S.bid },
        { name:'Max Segment', data:S.maxZ },
        { name:'Mid Segment', data:S.midZ },
        { name:'Min Segment', data:S.minZ },
      ]
    }, { notMerge:true });

    applyVisibility();
    keepRight();
    setStatus('ready');
  }

  async function loadMore(){
    if (!S.lastMaxRowId) { setStatus('nothing loaded'); return; }
    // try next max first
    try {
      const nx = await fetchNextMax(S.lastMaxRowId);      // :contentReference[oaicite:4]{index=4}
      if (nx && nx.id) {
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

        applyVisibility();
        keepRight();
        setStatus('ready');
        return;
      }
    } catch (_) { /* fallthrough to tail mode */ }

    // tail mode: no next max → extend to last tick & overlay zigs on tail
    setStatus('tail…');
    const last = await lastTickId();                       // :contentReference[oaicite:5]{index=5}
    if (!last || !S.lastSpanEndId) { setStatus('ready'); return; }

    let cursor = S.lastSpanEndId + 1;
    while (cursor <= last) {
      const chunk = Math.min(20000, last - cursor + 1);
      const rows = await j(`/api/ticks/after?since_id=${cursor-1}&limit=${chunk}`); // :contentReference[oaicite:6]{index=6}
      if (!rows?.length) break;
      for (const r of rows){
        const id=r.id;
        if (r.ask!=null) S.ask.push([id,+r.ask]);
        if (r.mid!=null) S.mid.push([id,+r.mid]);
        if (r.bid!=null) S.bid.push([id,+r.bid]);
        if (r.ts) S.tsById.set(id, r.ts);
        cursor = id + 1;
      }
    }
    // overlay mid/min (and max if any) for the tail window
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

    applyVisibility();
    keepRight();
    setStatus('ready');
  }

  function resetAll(){
    S.ask=[]; S.mid=[]; S.bid=[];
    S.maxZ=[]; S.midZ=[]; S.minZ=[];
    S.tsById.clear();
    S.lastMaxRowId=null;
    S.lastSpanEndId=null;
    chart.clear();
    chart.setOption(baseOption(),{notMerge:true});
    setStatus('idle');
  }

  // ====== Wire UI ======
  if (btnLoad) {
    btnLoad.addEventListener('click', async () => {
      const leg = (legSel?.value || 'max').toLowerCase();
      const row = parseInt(rowInput?.value || '0', 10);
      if (!row) { setStatus('enter Row ID'); return; }
      if (leg !== 'max') {
        // We’re anchoring by max row id (mid/min row ids are unrelated).
        setStatus('Only Max row-id is supported; retrieving corresponding mid/min by tick range…');
      }
      try { resetAll(); await loadByMaxRow(row); } catch (e) { setStatus(`error: ${String(e).slice(0,160)}`); }
    });
  }
  if (btnMore)  btnMore.addEventListener('click', () => loadMore().catch(e=>setStatus(String(e).slice(0,160))));
  if (btnReset) btnReset.addEventListener('click', resetAll);
  seriesCbs.forEach(cb => cb.addEventListener('change', applyVisibility));
})();
