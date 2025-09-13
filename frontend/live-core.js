// PATH: frontend/live-core.js
// Live chart anchored to the latest MAX leg start_id.
// - draws mid line from start_id → now (chunked load + SSE updates)
// - overlays the max leg as a separate line (start_ts→end_ts)
// - keeps your dark theme, integer y-grid, rich tooltip
// - provides "Load more ←" that never goes earlier than the anchor

const API = '/api';
const chart = echarts.init(document.getElementById('chart'));

// --- state ---
let rows = [];                 // [{id, ts, mid, bid?, ask?, spread?}]
let preds = [];                // [{at_ts, hit, dir, ...}]
let paused = false;
let win = 10000;               // visible window hint (doesn't force zoom)
let anchorStartId = null;      // start_id of the last MAX leg
let anchorEndId = null;        // end_id of that leg (for the overlay)
let maxlineData = [];          // [[ts,price],[ts,price]]
let initialLoadedCount = 0;    // used to keep the anchor body in memory
const CHUNK = 5000;

// --- UI bindings ---
document.getElementById('toggle').onclick = () => {
  paused = !paused;
  document.getElementById('toggle').textContent = paused ? 'Resume' : 'Pause';
};
document.getElementById('win').onchange = (e) => {
  win = +e.target.value;
  rebuildSeries();
};
document.getElementById('go').onclick = jumpToTick;
document.getElementById('labels').addEventListener('change', (e) => {
  chart.setOption({ series: [ {}, {}, { label:{show:e.target.checked} } ] });
});
document.getElementById('showMax').addEventListener('change', (e) => {
  chart.setOption({ series: [ {}, { show: e.target.checked }, {} ] });
});
document.getElementById('moreLeft').onclick = loadMoreLeft;
window.addEventListener('resize', () => chart.resize());

// --- chart setup ---
function setupChart(){
  chart.setOption({
    backgroundColor:'#0d1117',
    animation:false,
    tooltip:{
      trigger:'axis',
      axisPointer:{type:'line'},
      formatter:(params)=>{
        const p = params.find(x=>x.seriesName==='mid') || params[0];
        const d = p && p.data ? p.data.meta : null;
        if (!d) return '';
        const dt = new Date(d.ts);
        const date = dt.toLocaleDateString();
        const time = dt.toLocaleTimeString();
        const here = preds.filter(x=>x.at_ts && new Date(x.at_ts).getTime() === new Date(d.ts).getTime());
        const labelsHere = here.map(x=>`pred:${x.dir} ${x.hit===true?'✓':x.hit===false?'✗':'?'}`);
        const fmt = (v)=> (v===null || v===undefined) ? '' : (+v).toFixed(2);
        const lines = [
          `id: ${d.id}`,
          `${date} ${time}`,
          `mid: ${fmt(d.mid)}`,
          `bid: ${fmt(d.bid)}`,
          `ask: ${fmt(d.ask)}`,
          `spread: ${fmt(d.spread)}`
        ];
        if (labelsHere.length) lines.push(`labels: ${labelsHere.join(', ')}`);
        return lines.join('<br/>');
      }
    },
    grid:{left:48,right:24,top:24,bottom:48},
    xAxis:{ type:'time', axisLabel:{color:'#c9d1d9'}, axisLine:{lineStyle:{color:'#30363d'}}, axisPointer:{show:true} },
    yAxis:{
      type:'value', scale:true, minInterval:1, splitNumber:8,
      axisLabel:{color:'#c9d1d9', formatter:(v)=> String(Math.round(v))},
      splitLine:{lineStyle:{color:'#30363d'}}, axisPointer:{show:false}
    },
    dataZoom:[
      {type:'inside', xAxisIndex:0, filterMode:'weakFilter'},
      {type:'slider', xAxisIndex:0, bottom:6}
    ],
    series:[
      { // 0: mid
        name:'mid', type:'line', showSymbol:false, lineStyle:{width:1.5}, data:[]
      },
      { // 1: max leg overlay (two points)
        name:'max', type:'line', showSymbol:false, lineStyle:{width:2.5, type:'dashed'}, data:[], z:5
      },
      { // 2: prediction markers (optional)
        name:'pred', type:'scatter', symbolSize:10, data:[],
        label:{show:false, formatter:(p)=> p.data?.p?.hit===true?'✓':(p.data?.p?.hit===false?'✗':'?')}
      }
    ]
  });
}

// --- series builders ---
function rebuildSeries(){
  // Keep a reasonable right-side window for perf, but never drop below anchor body.
  let dataRows = rows;
  if (rows.length > win * 2) dataRows = rows.slice(-win * 2);

  const midData = dataRows.map(r => ({ value:[new Date(r.ts), r.mid], meta:r }));
  chart.setOption({
    series: [
      { data: midData },
      { data: maxlineData, show: document.getElementById('showMax').checked },
      { data: buildPredScatter(dataRows) }
    ]
  });
}

function buildPredScatter(windowRows){
  if (!windowRows.length) return [];
  const tsToMid = new Map(windowRows.map(r=>[new Date(r.ts).getTime(), r.mid]));
  const startTs = new Date(windowRows[0].ts).getTime();
  const endTs   = new Date(windowRows[windowRows.length-1].ts).getTime();
  const items = [];
  for (const p of preds){
    const ts = p.at_ts ? new Date(p.at_ts).getTime() : null;
    if (!ts || ts < startTs || ts > endTs) continue;
    const y = tsToMid.get(ts);
    if (y === undefined) continue;
    items.push({
      value:[ts, y],
      p,
      itemStyle:{ color: p.hit===true ? '#2ea043' : (p.hit===false ? '#f85149' : '#8b949e') },
      symbol: p.hit==null ? 'circle' : (p.hit ? 'triangle' : 'rect')
    });
  }
  return items;
}

// --- data helpers ---
function appendTicks(arr){
  if (!arr || !arr.length) return;
  rows = rows.concat(arr.map(r => ({
    id:r.id, ts:r.ts, mid:r.mid, bid:r.bid, ask:r.ask, spread:r.spread
  })));
  initialLoadedCount = Math.max(initialLoadedCount, rows.length);
  // Protect the anchored body from being trimmed away
  const cap = Math.max(win * 2, initialLoadedCount);
  if (rows.length > cap){
    // trim only if we won't cross the anchor
    let drop = rows.length - cap;
    while (drop > 0 && rows.length && anchorStartId && rows[0].id > anchorStartId){
      rows.shift(); drop--;
    }
  }
  rebuildSeries();
}

function prependTicks(arr){
  if (!arr || !arr.length) return;
  const left = arr.map(r => ({ id:r.id, ts:r.ts, mid:r.mid, bid:r.bid, ask:r.ask, spread:r.spread }));
  rows = left.concat(rows);
  initialLoadedCount = Math.max(initialLoadedCount, rows.length);
  rebuildSeries();
}

async function loadRangeInChunks(startId, endId){
  let from = startId;
  while (from <= endId){
    const to = Math.min(from + CHUNK - 1, endId);
    const r  = await fetch(`${API}/ticks?from_id=${from}&to_id=${to}`);
    const a  = await r.json();
    appendTicks(a);
    from = to + 1;
  }
}

async function jumpToTick(){
  const val = +document.getElementById('jump').value;
  if (!val) return;
  const end = val;
  const start = Math.max(1, end - win + 1);
  const r = await fetch(`${API}/ticks?from_id=${start}&to_id=${end}`);
  const arr = await r.json();
  rows = arr.map(r=>({id:r.id, ts:r.ts, mid:r.mid, bid:r.bid, ask:r.ask, spread:r.spread}));
  rebuildSeries();
}

async function loadMoreLeft(){
  if (!rows.length || anchorStartId == null) return;
  const firstId = rows[0].id;
  const to   = firstId - 1;
  if (to < anchorStartId) return;
  const start = Math.max(anchorStartId, to - CHUNK + 1);
  const r  = await fetch(`${API}/ticks?from_id=${start}&to_id=${to}`);
  const a  = await r.json();
  prependTicks(a);
}

// --- bootstrap ---
async function bootstrap(){
  // 1) get last max leg (anchor)
  const maxline = await (await fetch(`${API}/maxline/last`)).json();
  if (maxline && maxline.start_id){
    anchorStartId = +maxline.start_id;
    anchorEndId   = +(maxline.end_id ?? maxline.start_id);
    maxlineData = [
      [new Date(maxline.start_ts), +maxline.start_price],
      [new Date(maxline.end_ts),   +maxline.end_price]
    ];
  } else {
    anchorStartId = null;
    maxlineData = [];
  }

  // 2) find current last tick
  const last = await (await fetch('/ticks/lastid')).json();
  const end  = +last.lastId;

  // choose an initial end so the whole max leg is visible, but not insane
  let initialEnd = end;
  if (anchorStartId != null) {
    initialEnd = Math.max(anchorEndId || anchorStartId, Math.min(anchorStartId + win - 1, end));
  }

  // 3) initial load (from anchor or recent window) in chunks
  if (anchorStartId != null) {
    await loadRangeInChunks(anchorStartId, initialEnd);
  } else {
    const start = Math.max(1, end - win + 1);
    const r = await fetch(`${API}/ticks?from_id=${start}&to_id=${end}`);
    appendTicks(await r.json());
  }

  // 4) apply the overlay and draw once
  rebuildSeries();

  // 5) stream live updates
  const es = new EventSource(`${API}/live`);
  es.addEventListener('tick', ev=>{
    if (paused) return;
    const d = JSON.parse(ev.data);
    appendTicks([{ id:d.id, ts:d.ts, mid:d.mid, bid:d.bid, ask:d.ask, spread:d.spread }]);
  });
  es.addEventListener('pred', ev=>{
    if (paused) return;
    const d = JSON.parse(ev.data);
    preds.push(d);
    rebuildSeries();
  });
}

setupChart();
bootstrap();
