# PATH: frontend/live-core.js
const API = '/api';

const chart = echarts.init(document.getElementById('chart'));
chart.setOption({
  backgroundColor:'#0d1117', animation:false,
  tooltip:{trigger:'axis'},
  grid:{left:48,right:24,top:24,bottom:36},
  xAxis:{type:'category',axisLabel:{color:'#c9d1d9'},axisLine:{lineStyle:{color:'#30363d'}}},
  yAxis:{type:'value',scale:true,axisLabel:{color:'#c9d1d9'},splitLine:{lineStyle:{color:'#30363d'}}},
  series:[
    {name:'mid', type:'line', data:[], showSymbol:false},
    {name:'pred', type:'scatter', data:[], symbolSize:10}
  ]
});

let xs=[], ys=[], preds=[];
let paused=false;
let win = 2000;

function pushTick(id, mid){
  xs.push(id); ys.push(mid);
  if (xs.length>win){ xs.shift(); ys.shift(); }
  chart.setOption({xAxis:{data:xs}, series:[{data:ys}, {data:preds}]});
}
function pushPred(p){
  preds.push([p.at_id, null]); // mark on x; y auto via null (aligns with axis)
  if (preds.length>win) preds.shift();
  chart.setOption({series:[{data:ys}, {data:preds}]});
}

async function bootstrap(){
  const r = await fetch(`${API}/ticks?from_id=${Math.max(1, (await (await fetch('/ticks/lastid')).json()).lastId - win)}&to_id=${(await (await fetch('/ticks/lastid')).json()).lastId}`);
  const rows = await r.json();
  xs = rows.map(r=>r.id);
  ys = rows.map(r=>r.mid);
  chart.setOption({xAxis:{data:xs}, series:[{data:ys}, {data:preds}]});
}
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
  const d = JSON.parse(ev.data);
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
  preds = [];
  chart.setOption({xAxis:{data:xs}, series:[{data:ys}, {data:preds}]});
};
document.getElementById('win').onchange = (e)=>{
  win = +e.target.value;
};
