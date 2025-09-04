// Review page wired to existing backend routes in backend/main.py (no assumptions).
// Uses: GET /api/segm?id=... (present) and the list loader you already wired.
// Change: draw "smal" using markLine (not a 'lines' series) to avoid ECharts crash.

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
function ensureArray(v){ return Array.isArray(v) ? v : (v ? [v] : []); }

// -------------------- list segments (use your /api/segm/recent or SQL passthrough) --------------------
async function fetchRecentSegm(limit = 200) {
  // prefer your native route (you added it)
  try {
    const r = await fetch(`${API}/segm/recent?limit=${limit}`);
    if (r.ok) return await r.json();
  } catch(_) { /* ignore and fall back */ }

  // fallback via SQL view you already have in main.py: GET /sqlvw/query?query=...
  const q = encodeURIComponent(`
    SELECT id, start_id, end_id, start_ts, end_ts, dir, span, len
    FROM segm
    ORDER BY id DESC
    LIMIT ${Math.max(1, Math.min(limit, 500))}
  `.trim());
  const r2 = await fetch(`/sqlvw/query?query=${q}`);
  if (!r2.ok) throw new Error(`segm list failed: ${r2.status}`);
  return await r2.json();
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
      <td>${s.dir ?? ''}</td>
      <td>${s.len ?? ''}</td>
    `;
    tr.addEventListener('click', () => loadSegment(s.id));
    journalBody.appendChild(tr);
  }
  segInfo.textContent = 'Segment: —';
}

// -------------------- chart scaffolding (keeps your styling) --------------------
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
        return lines.join('<br/>');
      }
    },
    grid:{left:56,right:24,top:24,bottom:56},
    xAxis:{ type:'time', axisLabel:{color:'#c9d1d9'}, axisLine:{lineStyle:{color:'#30363d'}}, axisPointer:{show:true} },
    yAxis:{ type:'value', scale:true, minInterval:1, splitNumber:8,
      axisLabel:{color:'#c9d1d9', formatter:(v)=> String(Math.round(v))},
      splitLine:{lineStyle:{color:'#30363d'}}
    },
    dataZoom:[
      {type:'inside', xAxisIndex:0, filterMode:'weakFilter'},
      {type:'slider', xAxisIndex:0, bottom:8}
    ],
    series:[
      {name:'mid',    type:'line', showSymbol:false, lineStyle:{width:1.3}, data:[]},
      {name:'smooth', type:'line', showSymbol:false, lineStyle:{width:2, opacity:.8}, data:[]},
      // big movements shaded
      {name:'bigm',   type:'line', data:[], markArea:{itemStyle:{color:'rgba(234,179,8,0.18)'}, data:[]}},
      // NEW: small moves drawn as markLine on a helper empty series
      {name:'smalLines', type:'line', showSymbol:false, data:[],
        markLine:{silent:true, symbol:['none','none'],
          lineStyle:{width:2, color:'#ef4444'}, data:[]}
      },
      // predictions
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
  let sum=0;
  for (let i=0;i<arr.length;i++){
    sum += arr[i];
    if (i>=n) sum -= arr[i-n];
    out[i] = i>=n-1 ? sum / n : arr[i];
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

// ---- helper: index by time so we can map ts→mid for lines/marks
function makeTimeIndex(ticks){
  const pairs = ticks.map(t=>[+new Date(t.ts), +t.mid]).sort((a,b)=>a[0]-b[0]);
  function yAt(ts){
    if (!pairs.length) return null;
    const x = +new Date(ts);
    // binary search nearest
    let lo=0, hi=pairs.length-1, best=pairs[0];
    while (lo<=hi){
      const m=(lo+hi)>>1, dx=pairs[m][0]-x;
      if (Math.abs(dx) < Math.abs(best[0]-x)) best=pairs[m];
      if (dx===0) break;
      if (dx<0) lo=m+1; else hi=m-1;
    }
    return best[1];
  }
  return { yAt };
}

// ---- build visuals from your backend shapes ----
// small moves: backend returns {a_ts, b_ts, ...}
function buildSmallMarkLines(smal, idx){
  const data = [];
  for (const s of ensureArray(smal)){
    const a = s?.a_ts, b = s?.b_ts;
    if (!a || !b) continue;
    const y1 = idx.yAt(a), y2 = idx.yAt(b);
    if (y1==null || y2==null) continue;
    data.push([{coord:[new Date(a), +y1]}, {coord:[new Date(b), +y2]}]);
  }
  return data;
}

function buildBigAreas(bigm){
  const areas = [];
  for (const b of ensureArray(bigm)){
    const a=b?.a_ts, c=b?.b_ts;
    if (!a || !c) continue;
    areas.push([{xAxis:new Date(a)}, {xAxis:new Date(c)}]);
  }
  return areas;
}

function buildPredScatter(pred, idx){
  const dots = [];
  for (const p of ensureArray(pred)){
    const x = p?.at_ts ? new Date(p.at_ts) : null;
    if (!x) continue;
    const y = idx.yAt(p.at_ts);
    if (y==null) continue;
    dots.push({
      value:[x, +y],
      p,
      itemStyle:{ color: p?.hit===true ? '#2ea043' : (p?.hit===false ? '#f85149' : '#8b949e') },
      symbol: p?.hit==null ? 'circle' : (p?.hit ? 'triangle' : 'rect')
    });
  }
  return dots;
}

// -------------------- fetch & draw a single segment --------------------
async function loadSegment(segmId){
  try{
    const r = await fetch(`${API}/segm?id=${segmId}`);
    if (!r.ok) throw new Error(`segm ${segmId} fetch failed: ${r.status}`);
    const data = await r.json();

    data.smal  = Array.isArray(data.smal)  ? data.smal  : [];
    data.bigm  = Array.isArray(data.bigm)  ? data.bigm  : [];
    data.pred  = Array.isArray(data.pred)  ? data.pred  : [];
    data.level = Array.isArray(data.level) ? data.level : [];


    const ticks = Array.isArray(data.ticks) ? data.ticks : [];
    const {midSeries, smoothSeries} = mapTicksForSeries(ticks);
    const timeIdx = makeTimeIndex(ticks);

    const bigAreas  = buildBigAreas (data.bigm || []);
    const predDots  = buildPredScatter(data.pred || [], timeIdx);
    const smalLines = buildSmallMarkLines(data.smal || [], timeIdx);

    chart.clear();
    chart.setOption({
      series: [
        {name:'mid',        data: midSeries},
        {name:'smooth',     data: smoothSeries},
        {name:'bigm',       data: [], markArea:{itemStyle:{color:'rgba(234,179,8,0.18)'}, data: bigAreas}},
        {name:'smalLines',  data: [], markLine:{silent:true, symbol:['none','none'], lineStyle:{width:2, color:'#ef4444'}, data: smalLines}},
        {name:'pred',       data: predDots}
      ]
    });

    // horizontal levels (your shape: {price, kind, ts, [used_at_ts]})
    if (ticks.length && Array.isArray(data.level)){
      const xStart = new Date(ticks[0].ts);
      const xEnd   = new Date(ticks[ticks.length-1].ts);
      const levelData = [];
      for (const L of data.level){
        const used = !!L.used_at_ts;
        levelData.push([{coord:[xStart, +L.price]}, {coord:[xEnd, +L.price]}]);
      }
      chart.setOption({
        series: [
          {}, {}, {}, {},
          { // add a second markLine layer on the pred series to keep UI simple
            name:'pred',
            markLine:{silent:true, symbol:['none','none'],
              lineStyle:{type:'dashed', width:1, color:'#8b949e'},
              data: levelData
            }
          }
        ]
      });
    }

    const s = data.segm ?? {};
    const statsLine = ticks.length
      ? `Segment #${s.id ?? segmId} | ticks ${s.start_id ?? ''}..${s.end_id ?? ''} | ${s.dir ?? ''} span=${fmt2(s.span)} | small=${(data.smal||[]).length} big=${(data.bigm||[]).length} preds=${(data.pred||[]).length}`
      : `Segment #${s.id ?? segmId} — no ticks returned`;
    segInfo.textContent = statsLine;
    currentSeg = s;

  } catch (err){
    console.error(err);
    runStat.textContent = 'segment load error';
    segInfo.textContent = `Segment ${segmId}: failed to load`;
  }
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
  } finally {
    setTimeout(()=> runStat.textContent='idle', 900);
  }
}

runBtn?.addEventListener('click', (e) => {
  e.preventDefault();
  clearReviewChart();
});

// -------------------- boot --------------------
function boot(){
  chart.resize();
  setupChart();
  loadSegmList().catch(err => {
    console.error(err);
    runStat.textContent = 'segm list error';
  });
}
boot();
window.addEventListener('resize', ()=>chart.resize());
