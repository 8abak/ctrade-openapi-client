// PATH: frontend/review-core.js
// ML Review chart: zoomable ticks + BIG/SMALL as zig-zag overlays.
// Uses backend routes: /api/segms, /api/segm?id=, /api/run

const API = '/api';
const chart = echarts.init(document.getElementById('chart'));

let currentSeg = null;

function toMs(t){ return typeof t === 'number' ? t : +new Date(t); }
function lb(arr, x){ let lo=0,hi=arr.length; while(lo<hi){const m=(lo+hi)>>1; if(arr[m]<x) lo=m+1; else hi=m;} return Math.min(Math.max(lo,0),arr.length-1); }

function indexers(ticks){
  const ts=new Array(ticks.length), mid=new Array(ticks.length), id2idx=new Map();
  for(let i=0;i<ticks.length;i++){
    ts[i]=toMs(ticks[i].ts);
    mid[i]=+ticks[i].mid;
    if(ticks[i].id!=null) id2idx.set(ticks[i].id, i);
  }
  return {ts,mid,id2idx};
}

function zzFromEdges(ticks, edges){
  const {ts,mid,id2idx} = indexers(ticks);
  const line=[];
  for(const e of (edges||[])){
    // Accept rows having a_ts/b_ts (strings) and optionally a_id/b_id
    const aTs = e.a_ts ?? e[0], bTs = e.b_ts ?? e[1];
    const aId = e.a_id ?? e[2],  bId = e.b_id ?? e[3];
    let i0 = (aId!=null && id2idx.has(aId)) ? id2idx.get(aId) : lb(ts, toMs(aTs));
    let i1 = (bId!=null && id2idx.has(bId)) ? id2idx.get(bId) : lb(ts, toMs(bTs));
    line.push([ts[i0], mid[i0]]);
    line.push([ts[i1], mid[i1]]);
    line.push([null,null]); // break
  }
  return line;
}

function buildOption(seg, ticks, bigm, smal, show){
  const T = ticks.map(t=>toMs(t.ts));
  const M = ticks.map(t=>+t.mid);

  const bigZig = zzFromEdges(ticks, bigm);
  const smlZig = zzFromEdges(ticks, smal);

  return {
    animation:false,
    backgroundColor:'#0d1117',
    legend:{top:0, selected:{
      'Ticks': !!show.ticks,
      'BIG zigzag': !!show.big,
      'SMALL zigzag': !!show.small,
    }},
    tooltip:{ trigger:'axis', axisPointer:{type:'cross'} },
    grid:{left:60,right:16,top:28,bottom:38},
    xAxis:[{type:'time'}],
    yAxis:[{type:'value', scale:true, axisLabel:{formatter:v=>Math.round(v)}}],
    dataZoom:[
      {type:'inside', xAxisIndex:0, filterMode:'filter'},
      {type:'slider', xAxisIndex:0, filterMode:'filter', height:22, bottom:6},
    ],
    series:[
      show.ticks && {
        name:'Ticks', type:'line', showSymbol:false, sampling:'lttb',
        xAxisIndex:0, yAxisIndex:0, lineStyle:{width:1},
        data: T.map((x,i)=>[x,M[i]]), z:1
      },
      show.big && {
        name:'BIG zigzag', type:'line', showSymbol:false, connectNulls:false,
        xAxisIndex:0, yAxisIndex:0, lineStyle:{width:3},
        data: bigZig, z:4
      },
      show.small && {
        name:'SMALL zigzag', type:'line', showSymbol:false, connectNulls:false,
        xAxisIndex:0, yAxisIndex:0, lineStyle:{width:2},
        data: smlZig, z:3
      },
    ].filter(Boolean),
  };
}

function renderSegment(segData){
  currentSeg = segData;
  const show = {
    ticks: document.getElementById('ckTicks')?.checked ?? true,
    big:   document.getElementById('ckBigs')?.checked  ?? true,
    small: document.getElementById('ckSmals')?.checked ?? true,
  };
  const opt = buildOption(segData.segm, segData.ticks, segData.bigm, segData.smal, show);
  chart.clear();
  chart.setOption(opt, { notMerge:true, replaceMerge:['series','xAxis','yAxis','dataZoom','legend'] });
}

async function getJSON(url){
  const r = await fetch(url);
  if(!r.ok) throw new Error(`${r.status} ${r.statusText}`);
  return r.json();
}
async function postJSON(url, body){
  const r = await fetch(url, {method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify(body||{})});
  if(!r.ok) throw new Error(`${r.status} ${r.statusText}`);
  return r.json();
}

// ----------------------- Left table (segments) -----------------------

async function loadSegmList(){
  const rows = await getJSON(`${API}/segms?limit=500`);
  const tbody = document.getElementById('journalBody') || document.querySelector('#journal tbody');
  if(!tbody) return;
  tbody.innerHTML = '';
  for(const r of rows){
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${r.id}</td>
      <td>${new Date(r.start_ts).toLocaleString()}</td>
      <td>${r.dur_s}</td>
      <td>${r.preds ?? 0}</td>
      <td>${(r.ratio ?? 0).toFixed(2)}</td>
      <td>${r.dir ?? ''}</td>
    `;
    tr.style.cursor = 'pointer';
    tr.onclick = async ()=> {
      const data = await getJSON(`${API}/segm?id=${r.id}`);
      renderSegment(data);
      document.getElementById('segInfo') && (document.getElementById('segInfo').textContent = `Segment: ${r.id}`);
    };
    tbody.appendChild(tr);
  }
}

// ---------------------------- Buttons --------------------------------

async function runServer(){
  try{
    const res = await postJSON(`${API}/run`, {});
    console.log('Run result', res);
    await loadSegmList();
  }catch(e){
    alert('Run failed: ' + e.message);
  }
}

function wireUI(){
  document.getElementById('btnRunServer')?.addEventListener('click', runServer);
  ['ckTicks','ckBigs','ckSmals'].forEach(id=>{
    const el=document.getElementById(id);
    if(el) el.addEventListener('change', ()=> currentSeg && renderSegment(currentSeg));
  });
}

// Boot
wireUI();
loadSegmList().catch(console.error);
