//# PATH: frontend/live-core.js
// Live chart with wheel-zoom, time x-axis, integer y-grid, rich tooltip, and optional label markers.
// NOTE: Historical labels (bigm/smal/pred) are best viewed on /review.html:
// open https://www.datavis.au/review.html, then click a Journal row to load its segment;
// the chart overlays big movements (shaded), small moves (markers), and predictions (✓/✗).
const API = '/api';

const chart = echarts.init(document.getElementById('chart'));
let rows = [];   // [{id, ts, mid, bid?, ask?, spread?}]
let preds = [];  // raw pred events
let paused = false;
let win = 2000;

function setupChart(){
  chart.setOption({
    backgroundColor:'#0d1117',
    animation:false,
    tooltip:{
      trigger:'axis',
      // Single vertical pointer (no horizontal line), cleaner measurement grid
      axisPointer:{type:'line'},
      formatter:(params)=>{
        const p = params.find(x=>x.seriesName==='mid') || params[0];
        const d = p && p.data ? p.data.meta : null;
        if (!d) return '';
        const dt = new Date(d.ts);
        const date = dt.toLocaleDateString();
        const time = dt.toLocaleTimeString();
        // collect any prediction labels at this timestamp
        const pHere = preds.filter(x=>x.at_ts && new Date(x.at_ts).getTime() === new Date(d.ts).getTime());
        const labelsHere = pHere.map(x=>`pred:${x.dir} ${x.hit===true?'✓':x.hit===false?'✗':'?'}`);
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
    xAxis:{
      type:'time',
      axisLabel:{color:'#c9d1d9'},
      axisLine:{lineStyle:{color:'#30363d'}},
      // keep vertical pointer only
      axisPointer:{show:true}
    },
    yAxis:{
      type:'value',
      scale:true,
      // Integer-spaced grid for stable vertical measurement
      minInterval: 1,
      splitNumber: 8,
      axisLabel:{color:'#c9d1d9', formatter:(v)=> String(Math.round(v))},
      splitLine:{lineStyle:{color:'#30363d'}},
      // hide horizontal axis pointer line to avoid "double" lines per price
      axisPointer:{show:false}
    },
    dataZoom:[
      {type:'inside', xAxisIndex:0, filterMode:'weakFilter'},
      {type:'slider',  xAxisIndex:0, bottom:6}
    ],
    series:[
      {
        name:'mid',
        type:'line',
        showSymbol:false,
        lineStyle:{width:1.5},
        data:[] // objects: {value:[ts, mid], meta:{...fullRow}}
      },
      {
        name:'pred',
        type:'scatter',
        symbolSize:10,
        data:[], // objects: {value:[ts,y], p:{...}}
        label:{show:false, formatter:(p)=> p.data?.p?.hit===true?'✓':(p.data?.p?.hit===false?'✗':'?')}
      }
    ]
  });
}

function rebuildLine(){
  const data = rows.slice(-win).map(r=>({
    value:[new Date(r.ts), r.mid],
    meta:r
  }));
  chart.setOption({series:[{data}, {data: buildPredScatter()}]});
}

function buildPredScatter(){
  if (!rows.length) return [];
  const windowRows = rows.slice(-win);
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

function pushTick(t){
  rows.push(t);
  if (rows.length>win*2){ rows = rows.slice(-win*2); } // bound memory
  rebuildLine();
}

function pushPred(p){
  preds.push(p);
  chart.setOption({series:[{data: chart.getOption().series[0].data}, {data: buildPredScatter()}]});
}

async function bootstrap(){
  // prime with a recent window
  const last = await (await fetch('/ticks/lastid')).json();
  const end = last.lastId;
  const start = Math.max(1, end - win + 1);
  const r = await fetch(`${API}/ticks?from_id=${start}&to_id=${end}`);
  const arr = await r.json();
  rows = arr.map(r=>({
    id:r.id, ts:r.ts, mid:r.mid, bid:r.bid, ask:r.ask, spread:r.spread
  }));
  rebuildLine();
}

setupChart();
bootstrap();

// --- Live SSE wiring (includes prediction markers when ML catches up) ---
const es = new EventSource(`${API}/live`);
es.addEventListener('tick', ev=>{
  if (paused) return;
  const d = JSON.parse(ev.data);
  pushTick({ id:d.id, ts:d.ts, mid:d.mid, bid:d.bid, ask:d.ask, spread:d.spread });
});
es.addEventListener('pred', ev=>{
  if (paused) return;
  const d = JSON.parse(ev.data); // {at_id, at_ts, dir, hit, ...}
  pushPred(d);
});

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
  rebuildLine();
};
document.getElementById('win').onchange = (e)=>{
  win = +e.target.value;
  rebuildLine();
};
// Toggle visible labels on pred scatter (✓/✗ text)
document.getElementById('labels')?.addEventListener('change', (e)=>{
  const on = e.target.checked;
  chart.setOption({ series: [ {}, { label:{show:on} } ] });
});

window.addEventListener('resize', ()=>chart.resize());
