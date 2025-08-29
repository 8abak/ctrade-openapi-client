//# PATH: frontend/live-core.js
const API = '/api';

const chart = echarts.init(document.getElementById('chart'));
let rows = [];                 // [{id, ts, mid, bid?, ask?, spread?}]
let preds = [];                // raw pred events
let paused = false;
let win = 2000;
let showLabels = false;

function setupChart(){
  chart.setOption({
    backgroundColor:'#0d1117',
    animation:false,
    tooltip:{
      trigger:'axis',
      axisPointer:{type:'cross', label:{backgroundColor:'#161b22'}},
      formatter:(params)=>{
        // params is array; we care about the line point (seriesIndex 0)
        const p = params.find(x=>x.seriesName==='mid') || params[0];
        const d = p && p.data ? p.data.meta : null;
        if (!d) return '';
        const dt = new Date(d.ts);
        const date = dt.toLocaleDateString();
        const time = dt.toLocaleTimeString();
        const labelsHere = [];
        // show any predictions at this tick
        const pHere = preds.filter(x=>x.at_id===d.id);
        if (pHere.length){
          pHere.forEach(x=>labelsHere.push(`pred:${x.dir} ${x.hit===true?'✓':x.hit===false?'✗':'?'}`));
        }
        // compose tooltip lines
        const lines = [
          `id: ${d.id}`,
          `${date} ${time}`,
          `mid: ${d.mid !== undefined ? d.mid : ''}`,
          `bid: ${d.bid !== undefined && d.bid !== null ? d.bid : ''}`,
          `ask: ${d.ask !== undefined && d.ask !== null ? d.ask : ''}`,
          `spread: ${d.spread !== undefined && d.spread !== null ? d.spread : ''}`,
        ];
        if (labelsHere.length) lines.push(`labels: ${labelsHere.join(', ')}`);
        return lines.filter(Boolean).join('<br/>');
      }
    },
    grid:{left:48,right:24,top:24,bottom:48},
    xAxis:{
      type:'time',
      axisLabel:{color:'#c9d1d9'},
      axisLine:{lineStyle:{color:'#30363d'}}
    },
    yAxis:{
      type:'value',
      scale:true,
      axisLabel:{color:'#c9d1d9', formatter:(v)=> String(Math.round(v))},  // no decimals on axis labels
      splitLine:{lineStyle:{color:'#30363d'}}
    },
    dataZoom: [
      {type:'inside', xAxisIndex:0, filterMode:'weakFilter'},
      {type:'slider', xAxisIndex:0, bottom:6}
    ],
    series:[
      {
        name:'mid',
        type:'line',
        showSymbol:false,
        lineStyle:{width:1.5},
        // we push objects: {value:[ts, mid], meta:{...fullRow}}
        data:[]
      },
      {
        name:'pred',
        type:'scatter',
        symbolSize:10,
        data:[], // elements: {value:[ts, y], p:{...}}
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
  const startTs = new Date(rows[Math.max(0, rows.length - win)].ts).getTime();
  const endTs = new Date(rows[rows.length - 1].ts).getTime();
  // map ts->mid for quick lookup
  const tsToMid = new Map(rows.slice(-win).map(r=>[new Date(r.ts).getTime(), r.mid]));
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
  // p has at_id, at_ts, dir, hit, etc. (from SSE)
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

const es = new EventSource(`${API}/live`);
es.onmessage = ()=>{};
es.addEventListener('tick', ev=>{
  if (paused) return;
  const d = JSON.parse(ev.data);
  pushTick({ id:d.id, ts:d.ts, mid:d.mid, bid:d.bid, ask:d.ask, spread:d.spread });
});
es.addEventListener('pred', ev=>{
  if (paused) return;
  const d = JSON.parse(ev.data); // contains at_id, at_ts, hit, etc.
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
document.getElementById('labels')?.addEventListener('change', (e)=>{
  // currently toggles label visibility for pred series
  const on = e.target.checked;
  chart.setOption({ series: [ {}, { label:{show:on} } ] });
});

// resize on window change
window.addEventListener('resize', ()=>chart.resize());
