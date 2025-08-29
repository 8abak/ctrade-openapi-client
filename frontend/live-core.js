//# PATH: frontend/live-core.js
const API = '/api';

const chart = echarts.init(document.getElementById('chart'));
let xs = [], ys = [];
let rawPreds = [];           // store all pred SSE payloads we receive
let predsPlotted = [];       // current scatter items plotted
let paused = false;
let win = 2000;
let showLabels = false;

function setupChart(){
  chart.setOption({
    backgroundColor:'#0d1117',
    animation:false,
    tooltip:{
      trigger:'axis',
      axisPointer:{type:'cross', label:{backgroundColor:'#161b22'}}
    },
    grid:{left:48,right:24,top:24,bottom:36},
    xAxis:{
      type:'category',
      axisLabel:{color:'#c9d1d9'},
      axisLine:{lineStyle:{color:'#30363d'}}
    },
    yAxis:{
      type:'value',scale:true,
      axisLabel:{color:'#c9d1d9'},
      splitLine:{lineStyle:{color:'#30363d'}}
    },
    // <<< mouse-wheel zoom & drag >>>
    dataZoom: [
      {type:'inside', xAxisIndex:0, filterMode:'weakFilter'}, // wheel, pinch, drag inside
      {type:'slider', xAxisIndex:0, bottom:6}                  // visible slider
    ],
    series:[
      {
        name:'mid',
        type:'line',
        data:[],
        showSymbol:false,
        label:{show:false},
        lineStyle:{width:1.5}
      },
      {
        name:'pred',
        type:'scatter',
        data:[],
        symbolSize:10,
        encode:{x:0,y:1},
        label:{show:false, formatter:(p)=> p.data?.p?.hit===true?'✓':(p.data?.p?.hit===false?'✗':'?')}
      }
    ]
  });
}

function setSeriesLabels(show){
  showLabels = !!show;
  chart.setOption({
    series: [
      {label:{show: showLabels}},                   // mid line values (usually off)
      {label:{show: showLabels}}                    // pred symbols
    ]
  });
}

function idToIndex(id){
  // xs is array of tick ids (category labels). For perf we can binary search, but indexOf is OK at 2k window.
  return xs.indexOf(id);
}

function rebuildPredScatter(){
  const minId = xs[0], maxId = xs[xs.length-1];
  const items = [];
  for (const p of rawPreds){
    if (p.at_id >= minId && p.at_id <= maxId){
      const idx = idToIndex(p.at_id);
      if (idx >= 0){
        const y = ys[idx];
        items.push({
          value:[idx, y],
          p,
          itemStyle:{ color: p.hit===true ? '#2ea043' : (p.hit===false ? '#f85149' : '#8b949e') },
          symbol: p.hit==null ? 'circle' : (p.hit ? 'triangle' : 'rect')
        });
      }
    }
  }
  predsPlotted = items;
  chart.setOption({series:[{data:ys}, {data:predsPlotted}]});
}

function pushTick(id, mid){
  xs.push(id); ys.push(mid);
  if (xs.length>win){ xs.shift(); ys.shift(); }
  chart.setOption({xAxis:{data:xs}, series:[{data:ys}]});
  // keep preds aligned with the visible window
  rebuildPredScatter();
}

function pushPred(p){
  rawPreds.push(p);
  // bail if not visible yet
  const idx = idToIndex(p.at_id);
  if (idx < 0) return;
  const y = ys[idx];
  predsPlotted.push({
    value:[idx, y],
    p,
    itemStyle:{ color: p.hit===true ? '#2ea043' : (p.hit===false ? '#f85149' : '#8b949e') },
    symbol: p.hit==null ? 'circle' : (p.hit ? 'triangle' : 'rect')
  });
  if (predsPlotted.length>win) predsPlotted.shift();
  chart.setOption({series:[{data:ys}, {data:predsPlotted}]});
}

async function bootstrap(){
  // prime with a recent window
  const last = await (await fetch('/ticks/lastid')).json();
  const end = last.lastId;
  const start = Math.max(1, end - win + 1);
  const r = await fetch(`${API}/ticks?from_id=${start}&to_id=${end}`);
  const rows = await r.json();
  xs = rows.map(r=>r.id);
  ys = rows.map(r=>r.mid);
  chart.setOption({xAxis:{data:xs}, series:[{data:ys}, {data:[]}]});
}

setupChart();
bootstrap();

const es = new EventSource(`${API}/live`);
es.onmessage = ()=>{};
es.addEventListener('tick', ev=>{
  if (paused) return;
  const d = JSON.parse(ev.data);
  pushTick(d.id, d.mid);
});
es.addEventListener('pred', ev=>{
  if (paused) return;
  const d = JSON.parse(ev.data); // contains at_id, hit, etc.
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
  const rows = await r.json();
  xs = rows.map(r=>r.id);
  ys = rows.map(r=>r.mid);
  chart.setOption({xAxis:{data:xs}, series:[{data:ys}]});
  rebuildPredScatter();
};
document.getElementById('win').onchange = (e)=>{
  win = +e.target.value;
  // shrink to new window immediately
  if (xs.length>win){ xs = xs.slice(-win); ys = ys.slice(-win); }
  chart.setOption({xAxis:{data:xs}, series:[{data:ys}]});
  rebuildPredScatter();
};
document.getElementById('labels').onchange = (e)=> setSeriesLabels(e.target.checked);

// Expose resize handler (useful if user resizes window)
window.addEventListener('resize', ()=>chart.resize());
