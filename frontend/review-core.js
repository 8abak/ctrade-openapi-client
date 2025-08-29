//# PATH: frontend/review-core.js
// Review page: dark UI, wheel-zoom, time x-axis, $1 y-grid. Shows outcomes table and per-segment overlays.
const API = '/api';

const journalBody = document.querySelector('#journal tbody');
const runBtn = document.getElementById('run');
const runStat = document.getElementById('runStat');
const segInfo = document.getElementById('seginfo');

const chart = echarts.init(document.getElementById('chart'));
let currentSeg = null;

function fmt2(x){ return (x===null||x===undefined)?'':(+x).toFixed(2); }
function pillClass(v){
  if (v===0) return 'pill zero';
  return v>0 ? 'pill good' : 'pill bad';
}

function setupChart(){
  chart.setOption({
    backgroundColor:'#0d1117',
    animation:false,
    tooltip:{
      trigger:'axis',
      axisPointer:{type:'line'},
      formatter:(params)=>{
        const midP = params.find(p=>p.seriesName==='mid');
        const d = midP?.data?.meta;
        if (!d) return '';
        const dt = new Date(d.ts);
        const lines = [
          `${d.id}`,
          `${dt.toLocaleDateString()} ${dt.toLocaleTimeString()}`,
          `mid: ${fmt2(d.mid)}`
        ];
        if (d.smooth!==undefined) lines.push(`smooth: ${fmt2(d.smooth)}`);
        if (d.smal!==undefined)   lines.push(`smal: ${fmt2(d.smal)}`);
        if (d.pred!==undefined)   lines.push(`pred: ${fmt2(d.pred)}`);
        return lines.join('<br/>');
      }
    },
    grid:{left:56,right:24,top:24,bottom:56},
    xAxis:{
      type:'time',
      axisLabel:{color:'#c9d1d9'},
      axisLine:{lineStyle:{color:'#30363d'}},
      axisPointer:{show:true}
    },
    yAxis:{
      type:'value',
      scale:true,
      minInterval:1,               // $1 separation
      splitNumber:8,
      axisLabel:{color:'#c9d1d9', formatter:(v)=> String(Math.round(v))},
      splitLine:{lineStyle:{color:'#30363d'}},
      axisPointer:{show:false}
    },
    dataZoom:[
      {type:'inside', xAxisIndex:0, filterMode:'weakFilter'},
      {type:'slider',  xAxisIndex:0, bottom:8}
    ],
    series:[
      {name:'mid',    type:'line', showSymbol:false, lineStyle:{width:1.3}, data:[]},
      {name:'smooth', type:'line', showSymbol:false, lineStyle:{width:2, opacity:.8}, data:[]},
      // big movements as markArea (yellow translucent)
      {name:'bigm', type:'line', data:[], markArea:{itemStyle:{color:'rgba(234,179,8,0.18)'}, data:[]}},
      // small moves as thin green lines
      {name:'smal', type:'lines', coordinateSystem:'cartesian2d', polyline:false, lineStyle:{width:2, color:'#ef4444'}, data:[]},
      // predictions as scatter ✓/✗
      {name:'pred', type:'scatter', symbolSize:10, data:[], label:{show:true, formatter:(p)=> p.data?.p?.hit===true?'✓':(p.data?.p?.hit===false?'✗':'?')}}
    ]
  });
}

function rollingMean(arr, n){
  const out = new Array(arr.length).fill(null);
  if (arr.length===0) return out;
  n = Math.max(1, Math.min(n, arr.length));
  let sum = 0;
  for (let i=0;i<arr.length;i++){
    sum += arr[i];
    if (i>=n) sum -= arr[i-n];
    out[i] = i>=n-1 ? sum / n : arr[i]; // warm up: echo value
  }
  return out;
}

async function loadOutcomes(){
  const r = await fetch(`${API}/outcome?limit=200`);
  const rows = await r.json();
  journalBody.innerHTML = '';
  for (const o of rows){
    const tr = document.createElement('tr');
    tr.dataset.id = o.segm_id;
    const dt = new Date(o.time);
    const ticks = (o.end_id - o.start_id + 1);
    tr.innerHTML = `
      <td>${o.id}</td>
      <td>${dt.toLocaleDateString()},<br>${dt.toLocaleTimeString()}</td>
      <td>${o.duration}</td>
      <td>${o.predictions}</td>
      <td><span class="${pillClass(+o.ratio)}">${(+o.ratio).toFixed(2)}</span></td>
      <td>${o.dir}</td>
      <td>${ticks}</td>
    `;
    tr.onclick = ()=> loadSegment(o.segm_id);
    journalBody.appendChild(tr);
  }
  if (rows.length) loadSegment(rows[0].segm_id);
}

function mapTicksForSeries(ticks){
  const mids = ticks.map(t=>t.mid);
  const smooth = rollingMean(mids, Math.min(100, Math.max(50, Math.floor(ticks.length*0.1))));
  const midSeries = ticks.map((t,i)=>({ value:[new Date(t.ts), t.mid], meta:{...t, smooth:smooth[i]} }));
  const smoothSeries = ticks.map((t,i)=>({ value:[new Date(t.ts), smooth[i]] }));
  return {midSeries, smoothSeries};
}

function buildSmallLines(smal, tickIndex){
  // Convert smal (a_ts,b_ts) to line segments using mid values at nearest ticks
  const data = [];
  for (const s of smal){
    const a = tickIndex.get(s.a_id);
    const b = tickIndex.get(s.b_id);
    if (!a || !b) continue;
    data.push({
      coords: [[new Date(a.ts), a.mid],[new Date(b.ts), b.mid]]
    });
  }
  return data;
}

function buildBigAreas(bigm){
  // markArea expects [{name, xAxis:ts1}, {xAxis:ts2}] pairs
  const areas = [];
  for (const b of bigm){
    areas.push([{xAxis: new Date(b.a_ts)}, {xAxis: new Date(b.b_ts)}]);
  }
  return areas;
}

function buildPredScatter(pred, tickIndex){
  const items = [];
  for (const p of pred){
    const t = tickIndex.get(p.at_id);
    if (!t) continue;
    items.push({
      value:[new Date(p.at_ts), t.mid],
      p,
      itemStyle:{ color: p.hit===true ? '#2ea043' : (p.hit===false ? '#f85149' : '#8b949e') },
      symbol: p.hit==null ? 'circle' : (p.hit ? 'triangle' : 'rect')
    });
  }
  return items;
}

async function loadSegment(segmId){
  const r = await fetch(`${API}/segm?id=${segmId}`);
  const data = await r.json();
  currentSeg = data.segm;

  // Index ticks by id for overlays
  const tickIndex = new Map(data.ticks.map(t=>[t.id, t]));

  const {midSeries, smoothSeries} = mapTicksForSeries(data.ticks);
  const smalLines = buildSmallLines(data.smal || [], tickIndex);
  const bigAreas  = buildBigAreas(data.bigm || []);
  const predDots  = buildPredScatter(data.pred || [], tickIndex);

  chart.setOption({
    series: [
      {name:'mid', data: midSeries},
      {name:'smooth', data: smoothSeries},
      {name:'bigm', data: [], markArea:{itemStyle:{color:'rgba(234,179,8,0.18)'}, data: bigAreas}},
      {name:'smal', data: smalLines},
      {name:'pred', data: predDots}
    ]
  });

  const meta = data.segm;
  const statsLine = `Segment #${meta.id} | ticks ${meta.start_id}..${meta.end_id} | ${meta.dir} span=${fmt2(meta.span)} | small=${(data.smal||[]).length} big=${(data.bigm||[]).length} preds=${(data.pred||[]).length}`;
  segInfo.textContent = statsLine;
}

runBtn.onclick = async ()=>{
  runBtn.disabled = true; runStat.textContent='running…';
  try{
    const r = await fetch(`${API}/run`, {method:'POST', headers:{'Content-Type':'application/json'}, body:'{}'});
    const j = await r.json();
    runStat.textContent = `segments=${j.segments ?? '?'} from=${j.from_tick ?? '?'} to=${j.to_tick ?? '?'}`;
    await loadOutcomes();
  }catch(e){
    runStat.textContent = 'error';
  }finally{
    runBtn.disabled = false;
    setTimeout(()=>runStat.textContent='idle', 4000);
  }
};

setupChart();
loadOutcomes();
window.addEventListener('resize', ()=>chart.resize());
