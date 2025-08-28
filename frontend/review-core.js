//# PATH: frontend/review-core.js
const API = '/api'; // behind Nginx now
const fmtTs = (s)=> new Date(s).toLocaleString();

const runBtn = document.getElementById('runBtn');
const runStatus = document.getElementById('runStatus');
const journalBody = document.querySelector('#journal tbody');
const chartEl = document.getElementById('chart');
const segInfo = document.getElementById('segInfo');

let chart;

function initChart() {
  chart = echarts.init(chartEl, null, {renderer:'canvas'});
  chart.setOption({
    backgroundColor: '#0d1117',
    animation: false,
    tooltip: { trigger: 'axis' },
    legend: { textStyle:{color:'#c9d1d9'} },
    grid: { left: 48, right: 24, top: 24, bottom: 36 },
    xAxis: { type:'category', axisLabel:{color:'#c9d1d9'}, axisLine:{lineStyle:{color:'#30363d'}} },
    yAxis: { type:'value', scale:true, axisLabel:{color:'#c9d1d9'}, splitLine:{lineStyle:{color:'#30363d'}} },
    series: []
  });
}

async function loadOutcomes() {
  const res = await fetch(`${API}/outcome?limit=100`);
  const rows = await res.json();
  journalBody.innerHTML = '';
  rows.forEach(r => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${r.id}</td>
      <td>${fmtTs(r.time)}</td>
      <td>${r.duration}</td>
      <td>${r.predictions}</td>
      <td><span class="pill ${r.ratio>0?'good': (r.ratio<0?'bad':'')}">${r.ratio.toFixed ? r.ratio.toFixed(2) : r.ratio}</span></td>
      <td>${r.dir}</td>
      <td>${r.len}</td>`;
    tr.onclick = ()=> loadSeg(r.segm_id);
    journalBody.appendChild(tr);
  });
}

function smooth(arr, n=20) {
  const out = [];
  let sum=0;
  for (let i=0;i<arr.length;i++){
    sum += arr[i];
    if (i>=n) sum -= arr[i-n];
    out.push(sum/Math.min(i+1,n));
  }
  return out;
}

async function loadSeg(segId) {
  segInfo.textContent = 'loading...';
  const res = await fetch(`${API}/segm?id=${segId}`);
  const data = await res.json();
  const xs = data.ticks.map(t=>t.id);
  const ys = data.ticks.map(t=>t.mid);
  const ysm = smooth(ys, 80);

  const bigBands = data.bigm.map(b => {
    const aidx = xs.indexOf(b.a_id);
    const bidx = xs.indexOf(b.b_id);
    return [[aidx, Math.min(ys[aidx], ys[bidx])], [bidx, Math.max(ys[aidx], ys[bidx])]];
  });

  const smMarks = data.smal.map(s => {
    const idx = xs.indexOf(s.b_id);
    return {name:`${s.dir} ${s.move.toFixed(2)}`, xAxis: idx, yAxis: ys[idx]};
  });

  const predMarks = data.pred.map(p => {
    const idx = xs.indexOf(p.at_id);
    const symbol = p.hit == null ? 'circle' : (p.hit ? 'path://M5 12l3 3 7-7' : 'path://M4 4l12 12M16 4L4 16');
    return {name:`${p.dir} ${p.hit===true?'✓':p.hit===false?'✗':'?'}`, xAxis: idx, yAxis: ys[idx], symbolSize: 12, symbol};
  });

  chart.setOption({
    xAxis: { data: xs },
    series: [
      {name:'mid', type:'line', data: ys, showSymbol:false},
      {name:'smooth', type:'line', data: ysm, showSymbol:false, lineStyle:{width:1, opacity:0.6}},
      // big movements as custom rectangles (ribbons)
      {
        name:'bigm', type:'custom', renderItem: (params, api) => {
          const band = bigBands[params.dataIndex];
          if (!band) return;
          const x0 = api.coord([band[0][0], band[0][1]]);
          const x1 = api.coord([band[1][0], band[1][1]]);
          const rect = echarts.graphic.clipRectByRect({
            x: x0[0], y: Math.min(x0[1], x1[1]),
            width: x1[0]-x0[0], height: Math.abs(x1[1]-x0[1])
          }, {x: params.coordSys.x, y: params.coordSys.y, width: params.coordSys.width, height: params.coordSys.height});
          return rect && { type:'rect', shape: rect, style: { fill:'#2d333b', opacity:0.25 }};
        },
        data: bigBands.map((_,i)=>i), z: -1
      },
      // small moves as mark points
      {name:'smal', type:'line', data: ys, showSymbol:false, markPoint:{data: smMarks}},
      // preds as mark points with icons
      {name:'pred', type:'line', data: ys, showSymbol:false, markPoint:{data: predMarks}}
    ]
  });

  segInfo.textContent = `Segment #${data.segm.id} | ticks ${data.segm.start_id}..${data.segm.end_id} | ${data.segm.dir} span=${(+data.segm.span).toFixed(2)} | small=${data.smal.length} big=${data.bigm.length} preds=${data.pred.length}`;
}

runBtn.onclick = async ()=>{
  runStatus.textContent = 'running...';
  const r = await fetch(`${API}/run`, {method:'POST', headers:{'Content-Type':'application/json'}, body:'{}'});
  const j = await r.json();
  runStatus.textContent = `done: ${j.segments} segs`;
  await loadOutcomes();
};

initChart();
loadOutcomes();
