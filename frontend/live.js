import { $, makeChart, priceSeries, rowsToZigzag, j, keepOrFollowRight } from './chart-core.js';

const chart = makeChart($('#chart'));
const WINDOW = 2000;
let paused = false;

let lastId = 0;
let midData=[], bidData=[], askData=[];

function trimToWindow(){
  const maxN = WINDOW + 200; // small cushion for zooming
  if(midData.length > maxN) midData = midData.slice(-WINDOW);
  if(bidData.length > maxN) bidData = bidData.slice(-WINDOW);
  if(askData.length > maxN) askData = askData.slice(-WINDOW);
}

async function initialLoad(){
  const last = await j('/api/ticks/latest');
  if(!last?.id) return;
  lastId = last.id;
  const start = Math.max(1, lastId - WINDOW + 1);
  await pullTicks(start, lastId, false);
  await renderAll(start, lastId);
}

async function pullTicks(fromId, toId, append=true){
  const rows = await j(`/api/ticks?from_id=${fromId}&to_id=${toId}`);
  if(!append){ midData=[]; bidData=[]; askData=[]; }
  for(const r of rows){
    const t = r.ts ?? r.timestamp;
    if(t==null) continue;
    if(r.mid != null) midData.push([t, r.mid]);
    if(r.bid != null) bidData.push([t, r.bid]);
    if(r.ask != null) askData.push([t, r.ask]);
  }
  trimToWindow();
}

async function fetchZigs(fromId, toId){
  const rows = await j(`/api/zigzag?from_id=${fromId}&to_id=${toId}`);
  return {
    min: rows.filter(r=>r.kind==='min'),
    mid: rows.filter(r=>r.kind==='mid'),
    max: rows.filter(r=>r.kind==='max')
  };
}

async function renderAll(fromId, toId){
  const z = await fetchZigs(fromId, toId);
  const update = () => {
    const series = [];
    if($('#midp').checked && midData.length) series.push(priceSeries('mid', midData, 1.4));
    if($('#bid').checked  && bidData.length) series.push(priceSeries('bid', bidData, 1.0));
    if($('#ask').checked  && askData.length) series.push(priceSeries('ask', askData, 1.0));
    if($('#minzz').checked) series.push(rowsToZigzag(z.min, 'min'));
    if($('#midzz').checked) series.push(rowsToZigzag(z.mid, 'mid'));
    if($('#maxzz').checked) series.push(rowsToZigzag(z.max, 'max'));
    chart.setOption({ series }, { replaceMerge: ['series'] });
  };
  keepOrFollowRight(chart, update);
}

async function tickLoop(){
  if(paused) return;
  try{
    // ask for the newest id, then fetch any gap since lastId
    const last = await j('/api/ticks/latest');
    if(last?.id && last.id > lastId){
      const from = lastId + 1;
      const to   = last.id;
      await pullTicks(from, to, true);
      lastId = to;

      const fromWin = Math.max(1, lastId - WINDOW + 1);
      await renderAll(fromWin, lastId);
    }
  }catch(err){
    console.error(err);
  }
}

$('#pause').addEventListener('click', () => {
  paused = !paused;
  $('#pause').textContent = paused ? 'Resume' : 'Pause';
});

['#ask','#midp','#bid','#minzz','#midzz','#maxzz'].forEach(id => $(id).addEventListener('change', async () => {
  if(lastId){
    const fromWin = Math.max(1, lastId - WINDOW + 1);
    await renderAll(fromWin, lastId);
  }
}));

// Start everything
await initialLoad();
// light polling; adjust if you want tighter updates
setInterval(tickLoop, 750);
