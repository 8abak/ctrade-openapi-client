//# PATH: frontend/live-core.js
// Live chart with wheel-zoom, time x-axis, integer y-grid, rich tooltip,
// and optional overlays (pred, levels, small moves, big moves).
const API = '/api';

const chart = echarts.init(document.getElementById('chart'));
let rows = [];   // ticks [{id, ts, mid, bid?, ask?, spread?}]
let preds = [];  // live predictions
let levels = []; // {id, kind, price, used_at_id?}
let smals  = []; // [{a_ts,b_ts, a_id,b_id}]
let bigms  = []; // [{a_ts,b_ts, dir}]
let paused = false;
let win = 2000;
let currentSegId = null;
let overlaysTimer = null;

const ckPred   = document.getElementById('ckPred');
const ckLevels = document.getElementById('ckLevels');
const ckSmals  = document.getElementById('ckSmals');
const ckBigm   = document.getElementById('ckBigm');

function setupChart(){
  chart.setOption({
    backgroundColor:'#0d1117',
    animation:false,
    tooltip:{
      trigger:'axis',
      axisPointer:{type:'line'},
      formatter:(params)=>{
        const p = params.find(x=>x.seriesName==='mid') || params[0];
        const d = p?.data?.meta;
        if (!d) return '';
        const dt = new Date(d.ts);
        const fmt = v => (v==null?'':(+v).toFixed(2));
        return [
          `id: ${d.id}`,
          `${dt.toLocaleDateString()} ${dt.toLocaleTimeString()}`,
          `mid: ${fmt(d.mid)}`,
          `bid: ${fmt(d.bid)}`,
          `ask: ${fmt(d.ask)}`,
          `spread: ${fmt(d.spread)}`
        ].join('<br/>');
      }
    },
    grid:{left:56,right:24,top:16,bottom:56},
    xAxis:{ type:'time', axisLabel:{color:'#c9d1d9'}, axisLine:{lineStyle:{color:'#30363d'}}, axisPointer:{show:true} },
    yAxis:{ type:'value', scale:true, minInterval:1, splitNumber:8,
      axisLabel:{color:'#c9d1d9', formatter:v=>String(Math.round(v))},
      splitLine:{lineStyle:{color:'#30363d'}}, axisPointer:{show:false}
    },
    dataZoom:[ {type:'inside', xAxisIndex:0, filterMode:'weakFilter'}, {type:'slider', xAxisIndex:0, bottom:6} ],
    series:[
      {name:'mid', type:'line', showSymbol:false, lineStyle:{width:1.5}, data:[]},
      {name:'pred', type:'scatter', symbolSize:10, data:[], label:{show:ckPred.checked, formatter:p=>p.data?.p?.hit===true?'✓':(p.data?.p?.hit===false?'✗':'?')}},
      // helper series used to carry markArea/markLine overlays
      {name:'bigm', type:'line', data:[], markArea:{itemStyle:{color:'rgba(234,179,8,0.18)'}, data:[]}},
      {name:'smal', type:'lines', coordinateSystem:'cartesian2d', lineStyle:{width:2, color:'#ef4444'}, data:[]},
      {name:'levels', type:'line', showSymbol:false, data:[], markLine:{silent:true, symbol:['none','none'], data:[]}}
    ]
  });
}

function rebuildLine(){
  const data = rows.slice(-win).map(r=>({value:[new Date(r.ts), r.mid], meta:r}));
  chart.setOption({series:[
    {name:'mid', data},
    {name:'pred', data: ckPred.checked ? buildPredScatter() : []},
    {name:'bigm', markArea:{itemStyle:{color:'rgba(234,179,8,0.18)'},
             data: ckBigm.checked ? buildBigAreas() : []}},
    {name:'smal', data: ckSmals.checked ? buildSmallLines() : []},
    {name:'levels', markLine:{silent:true, symbol:['none','none'], data: ckLevels.checked ? buildLevelLines() : []}}
  ]});
}

function buildPredScatter(){
  const windowRows = rows.slice(-win);
  const tsToMid = new Map(windowRows.map(r=>[new Date(r.ts).getTime(), r.mid]));
  const startTs = windowRows.length ? new Date(windowRows[0].ts).getTime() : 0;
  const endTs   = windowRows.length ? new Date(windowRows[windowRows.length-1].ts).getTime() : 0;
  const items = [];
  for (const p of preds){
    const ts = p.at_ts ? new Date(p.at_ts).getTime() : null;
    if (!ts || ts < startTs || ts > endTs) continue;
    const y = tsToMid.get(ts);
    if (y === undefined) continue;
    items.push({ value:[ts, y],
      p, itemStyle:{ color: p.hit===true ? '#2ea043' : (p.hit===false ? '#f85149' : '#8b949e') },
      symbol: p.hit==null ? 'circle' : (p.hit ? 'triangle' : 'rect')
    });
  }
  return items;
}

function buildBigAreas(){
  const areas = [];
  for (const b of bigms){
    areas.push([{xAxis:new Date(b.a_ts)}, {xAxis:new Date(b.b_ts)}]);
  }
  return areas;
}

function buildSmallLines(){
  const lines = [];
  const idx = new Map(rows.slice(-win*2).map(r=>[r.id, r])); // local search
  for (const s of smals){
    const a = idx.get(s.a_id); const b = idx.get(s.b_id);
    if (!a || !b) continue;
    lines.push({coords:[[new Date(a.ts), a.mid],[new Date(b.ts), b.mid]]});
  }
  return lines;
}

function buildLevelLines(){
  if (!rows.length) return [];
  const xs = [new Date(rows[Math.max(0, rows.length-win)].ts), new Date(rows[rows.length-1].ts)];
  const out = [];
  for (const L of levels){
    const col = L.used_at_id ? (L.kind==='high' ? '#2ea043' : '#f85149') : '#8b949e';
    out.push([{coord:[xs[0], +L.price], lineStyle:{color:col, type:'dashed', width:1},
               label:{show:true, formatter:`${L.kind}@${(+L.price).toFixed(2)}${L.used_at_id?' • used':''}`, color:'#c9d1d9'}},
              {coord:[xs[1], +L.price]}]);
  }
  return out;
}

function pushTick(t){
  rows.push(t);
  if (rows.length>win*2){ rows = rows.slice(-win*2); }
  rebuildLine();
}

function pushPred(p){
  // If pred with same id exists, replace (to capture hit update)
  const i = preds.findIndex(x=>x.id===p.id);
  if (i>=0) preds[i]=p; else preds.push(p);
  rebuildLine();
}

async function bootstrap(){
  // Start with recent window
  const last = await (await fetch('/ticks/lastid')).json();
  const end = last.lastId;
  const start = Math.max(1, end - win + 1);
  const r = await fetch(`${API}/ticks?from_id=${start}&to_id=${end}`);
  const arr = await r.json();
  rows = arr.map(r=>({id:r.id, ts:r.ts, mid:r.mid, bid:r.bid, ask:r.ask, spread:r.spread}));
  await refreshOverlays(); // initial overlays for the tail segment
  rebuildLine();
}

async function getTailSegId(){
  const out = await (await fetch(`${API}/outcome?limit=1`)).json();
  if (!out || !out.length) return null;
  return out[0].segm_id || null;
}

async function refreshOverlays(){
  const segId = await getTailSegId();
  if (!segId) return;
  if (currentSegId !== segId){
    // reset caches when segment changes
    levels = []; smals = []; bigms = []; preds = [];
    currentSegId = segId;
  }
  const data = await (await fetch(`${API}/segm?id=${segId}`)).json();
  // Merge new overlay items (simple replace is fine)
  levels = data.level || [];
  smals  = data.smal  || [];
  bigms  = data.bigm  || [];
  // Also refresh preds snapshot (runner extends segment, so ids may change)
  preds  = (data.pred || []).map(p=>({id:p.id, ...p}));
  rebuildLine();
}

setupChart();
bootstrap();

// --- Live ticks via SSE ----
const es = new EventSource(`${API}/live`);
es.addEventListener('tick', ev=>{
  if (paused) return;
  const d = JSON.parse(ev.data);
  pushTick({ id:d.id, ts:d.ts, mid:d.mid, bid:d.bid, ask:d.ask, spread:d.spread });
});
es.addEventListener('pred', ev=>{
  if (paused) return;
  pushPred(JSON.parse(ev.data));
});

// --- UI wiring ---
document.getElementById('toggle').onclick = ()=>{
  paused = !paused;
  document.getElementById('toggle').textContent = paused ? 'Resume' : 'Pause';
};
document.getElementById('go').onclick = async ()=>{
  const val = +document.getElementById('jump').value;
  if (!val) return;
  const end = val, start = Math.max(1, end - win + 1);
  const r = await fetch(`${API}/ticks?from_id=${start}&to_id=${end}`);
  const arr = await r.json();
  rows = arr.map(r=>({id:r.id, ts:r.ts, mid:r.mid, bid:r.bid, ask:r.ask, spread:r.spread}));
  await refreshOverlays();
  rebuildLine();
};
document.getElementById('win').onchange = (e)=>{
  win = +e.target.value;
  rebuildLine();
};
ckPred.addEventListener('change', rebuildLine);
ckLevels.addEventListener('change', rebuildLine);
ckSmals.addEventListener('change', rebuildLine);
ckBigm.addEventListener('change', rebuildLine);

window.addEventListener('resize', ()=>chart.resize());

// Periodically refresh overlays for the current tail segment so you can
// watch labeling evolve live as the runner extends.
overlaysTimer = setInterval(refreshOverlays, 5000);
