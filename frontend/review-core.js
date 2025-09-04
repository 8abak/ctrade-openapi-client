// PATH: frontend/review-core.js
// Review page: dark UI, wheel-zoom, time x-axis, $1 y-grid. Shows outcomes table and per-segment overlays.

const API = '/api';

// DOM refs
const journalBody = document.querySelector('#journal tbody');
const runBtn   = document.getElementById('run');
const runStat  = document.getElementById('runStat');
const segInfo  = document.getElementById('seginfo');
const chart    = echarts.init(document.getElementById('chart'));

let currentSeg = null;

// -------------------- utils --------------------
function fmt2(x){ return (x===null||x===undefined||isNaN(+x)) ? '' : (+x).toFixed(2); }
const ms = t => (typeof t === 'number' ? t : +new Date(t));

// -------------------- new: load recent segm for sidebar --------------------
async function fetchRecentSegm(limit = 200) {
  const r = await fetch(`${API}/segm/recent?limit=${limit}`);
  if (!r.ok) throw new Error(`segm fetch failed: ${r.status}`);
  return await r.json(); // [{id,start_id,end_id,start_ts,end_ts,dir,span,len}, ...]
}

async function loadSegmList() {
  const rows = await fetchRecentSegm(200);
  journalBody.innerHTML = '';
  for (const s of rows) {
    const tr = document.createElement('tr');
    const durSec = Math.max(0, (new Date(s.end_ts) - new Date(s.start_ts)) / 1000) | 0;
    tr.innerHTML = `
      <td>${s.id}</td>
      <td>${new Date(s.start_ts).toLocaleString()}</td>
      <td>${durSec}</td>
      <td>${''}</td>
      <td>${fmt2(s.span)}</td>
      <td>${s.dir}</td>
      <td>${s.len}</td>
    `;
    tr.addEventListener('click', () => loadSegment(s.id));
    journalBody.appendChild(tr);
  }
  segInfo.textContent = 'Segment: —';
}

// -------------------- chart scaffolding --------------------
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
    xAxis:{ type:'time', axisLabel:{color:'#c9d1d9'}, axisLine:{lineStyle:{color:'#30363d'}}, axisPointer:{show:true} },
    yAxis:{ type:'value', scale:true, minInterval:1, splitNumber:8,
      axisLabel:{color:'#c9d1d9', formatter:(v)=> String(Math.round(v))},
      splitLine:{lineStyle:{color:'#30363d'}}, axisPointer:{show:false}
    },
    dataZoom:[
      {type:'inside', xAxisIndex:0, filterMode:'weakFilter'},
      {type:'slider', xAxisIndex:0, bottom:8}
    ],
    series:[
      {name:'mid',    type:'line', showSymbol:false, lineStyle:{width:1.3}, data:[]},
      {name:'smooth', type:'line', showSymbol:false, lineStyle:{width:2, opacity:.8}, data:[]},
      // big movements as markArea (yellow translucent)
      {name:'bigm',   type:'line', data:[], markArea:{itemStyle:{color:'rgba(234,179,8,0.18)'}, data:[]}},
      // small moves as thin red lines
      {name:'smal',   type:'lines', coordinateSystem:'cartesian2d', polyline:false, lineStyle:{width:2, color:'#ef4444'}, data:[]},
      // predictions as scatter ✓/✗
      {name:'pred',   type:'scatter', symbolSize:10, data:[],
        label:{show:true, formatter:(p)=> p.data?.p?.hit===true?'✓':(p.data?.p?.hit===false?'✗':'?')}
      }
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
    out[i] = i>=n-1 ? sum / n : arr[i]; // warm-up
  }
  return out;
}

function mapTicksForSeries(ticks){
  const mids = ticks.map(t=>+t.mid);
  const smooth = rollingMean(mids, Math.min(100, Math.max(50, Math.floor(ticks.length*0.1))));
  const midSeries    = ticks.map((t,i)=>({ value:[new Date(t.ts), +t.mid], meta:{...t, smooth:smooth[i]} }));
  const smoothSeries = ticks.map((t,i)=>({ value:[new Date(t.ts), smooth[i]] }));
  return {midSeries, smoothSeries};
}

function buildSmallLines(smal, tickIndex){
  // small segments [a_id, b_id] or objects with a_id/b_id
  const data = [];
  for (const s of (smal||[])){
    const aId = s.a_id ?? s[0];
    const bId = s.b_id ?? s[1];
    const a = tickIndex.get(aId); const b = tickIndex.get(bId);
    if (!a || !b) continue;
    data.push({ coords: [[new Date(a.ts), +a.mid],[new Date(b.ts), +b.mid]] });
  }
  return data;
}

function buildBigAreas(bigm){
  // [{a_ts,b_ts,dir,...}] -> markArea ranges
  const areas = [];
  for (const b of (bigm||[])){
    const a = b.a_ts ?? b[0]; const c = b.b_ts ?? b[1];
    areas.push([{xAxis:new Date(a)}, {xAxis:new Date(c)}]);
  }
  return areas;
}

function buildPredScatter(pred, tickIndex){
  const items = [];
  for (const p of (pred||[])){
    const t = tickIndex.get(p.at_id);
    if (!t) continue;
    items.push({
      value:[new Date(p.at_ts), +t.mid],
      p,
      itemStyle:{ color: p.hit===true ? '#2ea043' : (p.hit===false ? '#f85149' : '#8b949e') },
      symbol: p.hit==null ? 'circle' : (p.hit ? 'triangle' : 'rect')
    });
  }
  return items;
}

// -------------------- fetch & draw a single segment --------------------
async function loadSegment(segmId){
  const r = await fetch(`${API}/segm?id=${segmId}`);
  const data = await r.json();

  currentSeg = data.segm;

  // Index ticks by id for overlays
  const tickIndex = new Map(data.ticks.map(t=>[t.id, t]));

  const {midSeries, smoothSeries} = mapTicksForSeries(data.ticks);
  const smalLines = buildSmallLines(data.smal || [], tickIndex);
  const bigAreas  = buildBigAreas (data.bigm || []);
  const predDots  = buildPredScatter(data.pred || [], tickIndex);

  chart.clear();
  chart.setOption({
    series: [
      {name:'mid',    data: midSeries},
      {name:'smooth', data: smoothSeries},
      {name:'bigm',   data: [], markArea:{itemStyle:{color:'rgba(234,179,8,0.18)'}, data: bigAreas}},
      {name:'smal',   data: smalLines},
      {name:'pred',   data: predDots}
    ],
    markLine: undefined
  });

  // Add horizontal level lines via helper series so they lay on top
  const xStart = data.ticks.length ? new Date(data.ticks[0].ts) : null;
  const xEnd   = data.ticks.length ? new Date(data.ticks[data.ticks.length-1].ts) : null;
  const levelLines = [];
  if (xStart && xEnd){
    for (const L of (data.level || [])){
      const used = !!L.used_at_id;
      levelLines.push({
        lineStyle:{color: used ? (L.kind==='high' ? '#2ea043' : '#f85149') : '#8b949e', type:'dashed', width:1},
        label:{show:true, formatter:`${L.kind}@${(+L.price).toFixed(2)}${used?' • used':''}`, position:'insideEndTop', color:'#c9d1d9'},
        data:[ [{coord:[xStart, +L.price]}], [{coord:[xEnd, +L.price]}] ]
      });
    }
  }
  chart.setOption({
    series: [
      {}, {}, {}, {}, {},
      { // helper transparent line to carry markLine
        name:'levels', type:'line', showSymbol:false, data:[],
        markLine:{ silent:true, symbol:['none','none'], data: levelLines.flatMap(x=>x.data),
          lineStyle:{type:'dashed', width:1, color:'#8b949e'}, label:{show:false}
        }
      }
    ]
  });

  const meta = data.segm;
  const statsLine = `Segment #${meta.id} | ticks ${meta.start_id}..${meta.end_id} | ${meta.dir ?? ''} span=${fmt2(meta.span)} | small=${(data.smal||[]).length} big=${(data.bigm||[]).length} preds=${(data.pred||[]).length}`;
  segInfo.textContent = statsLine;
}

// -------------------- Clean button (local clear only) --------------------
function clearReviewChart() {
  try {
    chart.clear();
    currentSeg = null;
    segInfo.textContent = 'Segment: —';
    runStat.textContent = 'cleared';
  } catch (e) {
    runStat.textContent = 'clear failed';
    console.error('clearReviewChart error:', e);
  }
}

runBtn?.addEventListener('click', (e) => {
  e.preventDefault();
  clearReviewChart();
  // keep the left list intact so you can click another segment right away
  setTimeout(()=> runStat.textContent='idle', 1200);
});

// -------------------- boot --------------------
setupChart();
loadSegmList().catch(err => {
  console.error(err);
  runStat.textContent = 'segm list error';
});

// keep chart responsive
window.addEventListener('resize', ()=>chart.resize());

// checkboxes to toggle overlays on next load (already respected in buildOption earlier if you choose)
document.getElementById('ckBigs')?.addEventListener('change', ()=> {
  if (currentSeg) loadSegment(currentSeg.id);
});
document.getElementById('ckSmals')?.addEventListener('change', ()=> {
  if (currentSeg) loadSegment(currentSeg.id);
});
