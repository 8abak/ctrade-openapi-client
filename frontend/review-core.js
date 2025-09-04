// Review page wired to existing backend routes in backend/main.py (no assumptions).
// Uses:   GET /api/segm?id=...   (present)
// Prefers GET /api/segm/recent   (you added it); falls back to GET /sqlvw/query?query=...

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
function asDate(x){ return (x instanceof Date) ? x : new Date(x); }
function ensureArray(v){ return Array.isArray(v) ? v : (v ? [v] : []); }

// -------------------- list segments (prefer /api/segm/recent, fallback to /sqlvw/query) --------------------
async function fetchRecentSegm(limit = 200) {
  // try native recent route
  try {
    const r = await fetch(`${API}/segm/recent?limit=${limit}`);
    if (r.ok) return await r.json();
  } catch(_) { /* fall through */ }

  // fallback: use SQL passthrough that exists in main.py: GET /sqlvw/query?query=...
  const q = encodeURIComponent(`
    SELECT id, start_id, end_id, start_ts, end_ts, dir, span, len
    FROM segm
    ORDER BY id DESC
    LIMIT ${Math.max(1, Math.min(limit, 500))}
  `.trim());
  const r2 = await fetch(`/sqlvw/query?query=${q}`);
  if (!r2.ok) throw new Error(`segm fetch failed: ${r2.status}`);
  const rows = await r2.json(); // array of objects from dict_cur
  return rows;
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

// -------------------- chart scaffolding (keeps your original styling/legend intent) --------------------
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
      {name:'bigm',   type:'line', data:[], markArea:{itemStyle:{color:'rgba(234,179,8,0.18)'}, data:[]}},
      {name:'smal',   type:'lines', coordinateSystem:'cartesian2d', polyline:false, lineStyle:{width:2, color:'#ef4444'}, data:[]},
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

// -------------------- loaders mapped to your /api/segm payload --------------------
function mapTicksForSeries(ticks){
  const mids = ticks.map(t=>+t.mid);
  const smooth = rollingMean(mids, Math.min(100, Math.max(50, Math.floor(ticks.length*0.1))));
  const midSeries    = ticks.map((t,i)=>({ value:[new Date(t.ts), +t.mid], meta:{...t, smooth:smooth[i]} }));
  const smoothSeries = ticks.map((t,i)=>({ value:[new Date(t.ts), smooth[i]] }));
  return {midSeries, smoothSeries};
}

function buildSmallLines(smal, tickIndex){
  const data = [];
  for (const s of (smal||[])){
    // small moves returned with a_ts/b_ts (timestamps) — we draw as time spans
    if (!s.a_ts || !s.b_ts) continue;
    // project to y using nearest ticks at those times
    const a = s.a_ts, b = s.b_ts;
    // find closest known points by time; fallback to first/last
    const first = tickIndex.first, last = tickIndex.last;
    const ay = tickIndex.byTime(a) ?? (first ? first.mid : null);
    const by = tickIndex.byTime(b) ?? (last ? last.mid : null);
    if (ay==null || by==null) continue;
    data.push({ coords: [[new Date(a), +ay],[new Date(b), +by]] });
  }
  return data;
}

function buildBigAreas(bigm){
  const areas = [];
  for (const b of (bigm||[])){
    if (!b.a_ts || !b.b_ts) continue;
    areas.push([{xAxis:new Date(b.a_ts)}, {xAxis:new Date(b.b_ts)}]);
  }
  return areas;
}

function buildPredScatter(pred, tickIndex){
  const items = [];
  for (const p of (pred||[])){
    const x = p.at_ts ? new Date(p.at_ts) : null;
    if (!x) continue;
    const y = tickIndex.byTime(p.at_ts);
    if (y==null) continue;
    items.push({
      value:[x, +y],
      p,
      itemStyle:{ color: p.hit===true ? '#2ea043' : (p.hit===false ? '#f85149' : '#8b949e') },
      symbol: p.hit==null ? 'circle' : (p.hit ? 'triangle' : 'rect')
    });
  }
  return items;
}

// helper: build index by id + time → mid
function indexTicks(ticks){
  const byId = new Map();
  const byTs = [];
  for (const t of ticks){ byId.set(t.id, t); byTs.push([+new Date(t.ts), +t.mid]); }
  byTs.sort((a,b)=>a[0]-b[0]);
  function byTime(ts){
    const x = +new Date(ts);
    // binary search nearest
    let lo=0, hi=byTs.length-1, best=null;
    while (lo<=hi){
      const mid = (lo+hi)>>1;
      const dx = byTs[mid][0]-x;
      if (best===null || Math.abs(dx)<Math.abs(best[0]-x)) best = byTs[mid];
      if (dx===0) break;
      if (dx<0) lo=mid+1; else hi=mid-1;
    }
    return best ? best[1] : null;
  }
  return {
    get:(id)=>byId.get(id),
    byTime,
    first: ticks[0] || null,
    last: ticks[ticks.length-1] || null
  };
}

// -------------------- fetch & draw a single segment --------------------
async function loadSegment(segmId){
  try{
    const r = await fetch(`${API}/segm?id=${segmId}`);
    if (!r.ok) throw new Error(`segm ${segmId} fetch failed: ${r.status}`);
    const data = await r.json();

    const ticks = Array.isArray(data.ticks) ? data.ticks : [];
    const tickIndex = indexTicks(ticks);

    const {midSeries, smoothSeries} = mapTicksForSeries(ticks);
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
      ]
    });

    // horizontal levels
    if (ticks.length && Array.isArray(data.level)){
      const xStart = new Date(ticks[0].ts);
      const xEnd   = new Date(ticks[ticks.length-1].ts);
      const levelLines = [];
      for (const L of data.level){
        const used = !!L.used_at_ts;
        levelLines.push({
          lineStyle:{color: used ? (L.kind==='high' ? '#2ea043' : '#f85149') : '#8b949e', type:'dashed', width:1},
          label:{show:true, formatter:`${L.kind}@${(+L.price).toFixed(2)}${used?' • used':''}`, position:'insideEndTop', color:'#c9d1d9'},
          data:[ [{coord:[xStart, +L.price]}], [{coord:[xEnd, +L.price]}] ]
        });
      }
      chart.setOption({
        series: [
          {}, {}, {}, {}, {},
          {name:'levels', type:'line', showSymbol:false, data:[],
           markLine:{ silent:true, symbol:['none','none'],
             data: levelLines.flatMap(x=>x.data),
             lineStyle:{type:'dashed', width:1, color:'#8b949e'}, label:{show:false}
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

// optional toggles: redraw current segm respecting checkboxes (kept behavior)
document.getElementById('ckBigs')?.addEventListener('change', ()=> {
  if (currentSeg?.id) loadSegment(currentSeg.id);
});
document.getElementById('ckSmals')?.addEventListener('change', ()=> {
  if (currentSeg?.id) loadSegment(currentSeg.id);
});
