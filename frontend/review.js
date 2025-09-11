import { $, makeChart, priceSeries, rowsToZigzag, j, keepOrFollowRight } from './chart-core.js';

const chart = makeChart($('#chart'));
const PADDING = 800;

let startId = 1;
let chunk = 2000;
let endId = startId + chunk - 1;

let midData=[], bidData=[], askData=[];

function readUI(){
  startId = +$('#startId').value || 1;
  chunk   = +$('#chunk').value   || 2000;
  endId   = startId + chunk - 1;
}

async function loadTicks(from_id, to_id, append=false){
  const rows = await j(`/api/ticks?from_id=${from_id}&to_id=${to_id}`);
  if(!append){ midData=[]; bidData=[]; askData=[]; }
  for(const r of rows){
    const t = r.ts ?? r.timestamp;
    if(t==null) continue;
    if(r.mid != null) midData.push([t, r.mid]);
    if(r.bid != null) bidData.push([t, r.bid]);
    if(r.ask != null) askData.push([t, r.ask]);
  }
}

async function fetchZigs(from_id, to_id){
  // get all three in one call; we’ll bucket by kind
  const rows = await j(`/api/zigzag?from_id=${from_id}&to_id=${to_id}`);
  return {
    min: rows.filter(r=>r.kind==='min'),
    mid: rows.filter(r=>r.kind==='mid'),
    max: rows.filter(r=>r.kind==='max')
  };
}

async function render(){
  readUI();
  const f = Math.max(1, startId - PADDING);
  const t = endId + PADDING;

  await loadTicks(f, t, false);
  const z = await fetchZigs(f, t);

  const series = [];
  if($('#midp').checked && midData.length) series.push(priceSeries('mid', midData, 1.4));
  if($('#bid').checked  && bidData.length) series.push(priceSeries('bid', bidData, 1.0));
  if($('#ask').checked  && askData.length) series.push(priceSeries('ask', askData, 1.0));

  if($('#minzz').checked) series.push(rowsToZigzag(z.min, 'min'));
  if($('#midzz').checked) series.push(rowsToZigzag(z.mid, 'mid'));
  if($('#maxzz').checked) series.push(rowsToZigzag(z.max, 'max'));

  chart.setOption({ series }, { replaceMerge: ['series'] });
}

async function loadMoreRight(){
  const opt = chart.getOption();
  const update = async () => {
    const oldEnd = endId;
    const nextStart = oldEnd + 1;
    endId = oldEnd + chunk;

    const f = Math.max(1, nextStart - PADDING);
    const t = endId + PADDING;

    await loadTicks(f, t, true);
    const z = await fetchZigs(f, t);

    const series = [];
    if($('#midp').checked) series.push(priceSeries('mid', midData, 1.4));
    if($('#bid').checked)  series.push(priceSeries('bid', bidData, 1.0));
    if($('#ask').checked)  series.push(priceSeries('ask', askData, 1.0));

    if($('#minzz').checked) series.push(rowsToZigzag(z.min, 'min'));
    if($('#midzz').checked) series.push(rowsToZigzag(z.mid, 'mid'));
    if($('#maxzz').checked) series.push(rowsToZigzag(z.max, 'max'));

    chart.setOption({ series }, { replaceMerge: ['series'] });
  };

  // keep view unless at far right
  keepOrFollowRight(chart, update);
}

$('#load').addEventListener('click', render);
$('#moreR').addEventListener('click', loadMoreRight);
['#ask','#midp','#bid','#minzz','#midzz','#maxzz'].forEach(id => $(id).addEventListener('change', render));

// initial
render();
