/* global echarts */
(() => {
  // ---------- UI elements ----------
  const chartEl = document.getElementById('chart');
  const statusEl = document.getElementById('status') || (() => {
    const d=document.createElement('div'); d.id='status'; return d;
  })();

  // If your HTML didn't have these inputs yet, wire them by id:
  // <input id="fromId" type="number"> <input id="toId" type="number">
  // <button id="btnLoad">Load</button> <button id="btnReset">Reset</button>
  // <label><input id="zzOnly" type="checkbox"> Zigzag only</label>
  // And the legend checkboxes have data-series="ask|mid|bid|minZ|midZ|maxZ"

  const fromInput = document.getElementById('fromId');
  const toInput   = document.getElementById('toId');
  const btnLoad   = document.getElementById('btnLoad');
  const btnReset  = document.getElementById('btnReset');
  const zzOnlyCB  = document.getElementById('zzOnly');

  const checkboxes = Array.from(
    document.querySelectorAll('input[type=checkbox][data-series]')
  );

  // ---------- Chart bootstrap ----------
  const chart = echarts.init(chartEl, null, { renderer: 'canvas' });

  const COLORS = {
    ask: '#FF6B6B',
    mid: '#FFD166',
    bid: '#4ECDC4',
    maxZ: '#F472B6',
    midZ: '#F59E0B',
    minZ: '#10B981',
  };

  const S = {
    // we render everything in [x=id, y=price] format (value axis),
    // so both ticks and zigs use the same data layout.
    ask: [], mid: [], bid: [],
    maxZ: [], midZ: [], minZ: [],
    tsById: new Map(), // id -> ISO ts (for tooltip)
    viewSize: 3000,
    window: { from: null, to: null },
    zigzagOnly: false,
  };

  function setStatus(t) { statusEl.textContent = t; }

  function yAxisInt(){
    return {
      type:'value',
      min:(e)=>Math.floor(e.min),
      max:(e)=>Math.ceil(e.max),
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
        formatter: params => {
          if(!params || !params.length) return '';
          // all series at the same x; get the x (tick id) from the first point
          const p0 = params.find(p => p.data && Array.isArray(p.data));
          const id = p0 ? p0.data[0] : null;
          const ts = id != null ? S.tsById.get(id) : null;
          const d  = ts ? (new Date(ts)).toLocaleString() : '(no time)';
          const head = `ID: ${id}\n\nTime: ${d}\n\n* * *\n`;
          const lines = params
            .filter(p => p.value != null)
            .map(p => `${p.seriesName}: ${Array.isArray(p.data) ? p.data[1] : p.value}`);
          return head + lines.join('\n');
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
        { name:'Ask', type:'line', showSymbol:false, data:S.ask,   lineStyle:{ color:COLORS.ask, width:1.4 }, itemStyle:{ color:COLORS.ask } },
        { name:'Mid (ticks)', type:'line', showSymbol:false, data:S.mid,   lineStyle:{ color:COLORS.mid, width:1.4 }, itemStyle:{ color:COLORS.mid } },
        { name:'Bid', type:'line', showSymbol:false, data:S.bid,   lineStyle:{ color:COLORS.bid, width:1.4 }, itemStyle:{ color:COLORS.bid } },

        { name:'Max Zig', type:'line', connectNulls:false, showSymbol:false, data:S.maxZ, lineStyle:{ color:COLORS.maxZ, width:1.8 } },
        { name:'Mid Zig', type:'line', connectNulls:false, showSymbol:false, data:S.midZ, lineStyle:{ color:COLORS.midZ, width:1.6 } },
        { name:'Min Zig', type:'line', connectNulls:false, showSymbol:false, data:S.minZ, lineStyle:{ color:COLORS.minZ, width:1.4 } },
      ]
    };
  }

  chart.setOption(baseOption(), { notMerge:true });

  function applyVisibility(){
    const show = Object.fromEntries(checkboxes.map(cb => [cb.dataset.series, cb.checked]));
    const opt = chart.getOption();
    opt.series.forEach(s => {
      const key = (
        s.name === 'Ask' ? 'ask' :
        s.name === 'Mid (ticks)' ? 'mid' :
        s.name === 'Bid' ? 'bid' :
        s.name === 'Max Zig' ? 'maxZ' :
        s.name === 'Mid Zig' ? 'midZ' :
        s.name === 'Min Zig' ? 'minZ' : null
      );
      if(!key) return;
      const vis = S.zigzagOnly
        ? (key.endsWith('Z') ? show[key] : false)      // ticks hidden in zigzag-only
        : show[key];                                    // normal
      s.data = S[key];
      s.itemStyle = s.itemStyle || {};
      s.lineStyle = s.lineStyle || {};
      s.itemStyle.opacity = vis ? 1 : 0;
      s.lineStyle.opacity = vis ? 1 : 0;
    });
    chart.setOption(opt, { notMerge:true, lazyUpdate:true });
  }

  function keepRight(){
    // keep the right edge if user is already at the right
    const dz = chart.getOption().dataZoom?.[0];
    if(!dz) return;
    const end = (dz.endValue != null) ? dz.endValue : dz.end;
    if(end == null || (typeof end === 'number' && end < 99.5)) return;
    // compute rightmost tick id in current data
    const maxX = Math.max(
      ...[S.ask, S.mid, S.bid, S.maxZ, S.midZ, S.minZ]
         .filter(a => a && a.length)
         .map(a => a.reduce((m,p)=> p && p[0]!=null ? Math.max(m, p[0]) : m, -Infinity)),
      -Infinity
    );
    if (isFinite(maxX)) {
      const span = Math.max(1, S.viewSize);
      chart.dispatchAction({ type:'dataZoom', startValue: Math.max(0, maxX - span + 1), endValue: maxX });
    }
  }

  // ---------- Data helpers ----------
  async function j(url) {
    const r = await fetch(url);
    if(!r.ok) throw new Error(await r.text());
    return r.json();
  }

  // Fetch ticks in chunks using /api/ticks/after (backend guarantees ASC)
  // We also store ts in S.tsById for tooltips.
  async function fetchTicksWindow(fromId, toId){
    const OUT = { ask:[], mid:[], bid:[], tsById:new Map() };
    if(toId < fromId){ const t=fromId; fromId=toId; toId=t; }
    let cursor = fromId - 1;
    while (true) {
      const limit = Math.min(20000, toId - cursor);
      if (limit <= 0) break;
      const rows = await j(`/api/ticks/after?since_id=${cursor}&limit=${limit}`);
      if(!Array.isArray(rows) || rows.length === 0) break;
      for(const r of rows){
        const id = r.id;
        if(id < fromId) continue;
        if(id > toId) { cursor = toId; break; }
        if (r.ask != null) OUT.ask.push([id, +r.ask]);
        if (r.mid != null) OUT.mid.push([id, +r.mid]);
        if (r.bid != null) OUT.bid.push([id, +r.bid]);
        if (r.ts) OUT.tsById.set(id, r.ts);
        cursor = id;
      }
      if (cursor >= toId) break;
    }
    return OUT;
  }

  function buildPolyline(segments){
    // segments are rows: {start_id, end_id, start_price, end_price, kind, ...}
    // We render each leg as [ [sid, sprice], [eid, eprice], null, ... ]
    const out = [];
    for (const s of segments || []) {
      if (s.start_id==null || s.end_id==null || s.start_price==null || s.end_price==null) continue;
      out.push([+s.start_id, +s.start_price], [+s.end_id, +s.end_price], null);
    }
    return out;
  }

  // Fetch all zigs (or a single kind) via /api/zigzag
  async function fetchZigsWindow(fromId, toId, kind /* 'min'|'mid'|'max'|undefined */){
    const q = new URLSearchParams({ from_id:String(fromId), to_id:String(toId) });
    if (kind) q.set('kind', kind);
    const rows = await j(`/api/zigzag?${q.toString()}`);
    // rows can be a flat list (all kinds) or one-kind list; normalize into {min:[], mid:[], max:[]}
    const out = { min:[], mid:[], max:[] };
    if (!Array.isArray(rows)) return out;
    for (const s of rows) {
      const k = s.kind || kind || 'max';
      if (!out[k]) out[k]=[];
      out[k].push(s);
    }
    return out;
  }

  // ---------- High-level flows ----------
  async function loadWindow(fromId, toId){
    setStatus(`loading ${fromId}..${toId} …`);
    S.window.from = fromId; S.window.to = toId;

    // clear previous
    S.ask = []; S.mid = []; S.bid = [];
    S.maxZ = []; S.midZ = []; S.minZ = [];
    S.tsById.clear();

    // zigs first (fast visual)
    const zz = await fetchZigsWindow(fromId, toId); // /api/zigzag returns min+mid+max in one call
    S.maxZ = buildPolyline(zz.max);
    S.midZ = buildPolyline(zz.mid);
    S.minZ = buildPolyline(zz.min);

    // optionally load ticks
    if (!S.zigzagOnly) {
      const T = await fetchTicksWindow(fromId, toId); // uses /api/ticks/after in chunks
      S.ask = T.ask; S.mid = T.mid; S.bid = T.bid;
      T.tsById.forEach((v,k)=>S.tsById.set(k,v));
    }

    // apply
    chart.setOption({
      xAxis: [{ type:'value' }],
      yAxis: yAxisInt(),
      series: [
        { name:'Ask', data:S.ask },
        { name:'Mid (ticks)', data:S.mid },
        { name:'Bid', data:S.bid },
        { name:'Max Zig', data:S.maxZ },
        { name:'Mid Zig', data:S.midZ },
        { name:'Min Zig', data:S.minZ },
      ]
    }, { notMerge:true });

    applyVisibility();
    keepRight();
    setStatus('ready');
  }

  // ---------- Wiring ----------
  if (zzOnlyCB) {
    zzOnlyCB.addEventListener('change', () => {
      S.zigzagOnly = !!zzOnlyCB.checked;
      applyVisibility();
      // If ticks were previously loaded and user toggles off/on, no need to refetch.
    });
  }

  if (btnLoad) {
    btnLoad.addEventListener('click', async () => {
      const fromId = parseInt(fromInput?.value || '0', 10);
      const toId   = parseInt(toInput?.value   || '0', 10);
      if (!fromId || !toId) { setStatus('enter From/To tick ids'); return; }
      try { await loadWindow(fromId, toId); } catch (e) { setStatus(`error: ${String(e).slice(0,160)}`); }
    });
  }

  if (btnReset) {
    btnReset.addEventListener('click', () => {
      S.ask=[]; S.mid=[]; S.bid=[]; S.maxZ=[]; S.midZ=[]; S.minZ=[]; S.tsById.clear();
      chart.clear(); chart.setOption(baseOption(),{notMerge:true});
      setStatus('idle');
    });
  }

  checkboxes.forEach(cb => cb.addEventListener('change', applyVisibility));
})();
