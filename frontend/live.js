// ===== live.js =====
const {
  fetchJSON, makeChart, priceLineSeries, ticksToLine, legsToPath
} = window.ChartCore;

const el = (id) => document.getElementById(id);

let chart;
let paused = false;
let windowSize = 2000;
let ticks = [];                   // ascending by id
let legsMin = [], legsMid = [], legsMax = [];
let followTail = true;            // autoscroll only if we're at right edge

// UI
function wireUI() {
  el('btnPause').onclick = () => {
    paused = !paused;
    el('btnPause').textContent = paused ? 'Resume' : 'Pause';
  };

  el('selWindow').onchange = () => {
    windowSize = +el('selWindow').value;
    trimToWindow();
    redraw();
  };

  el('btnLeft').onclick = async () => {
    await loadLeft(2000);
  };
}

function atTail() {
  const opt = chart.getOption();
  const dz = (opt.dataZoom && opt.dataZoom[0]) || null;
  return !dz || dz.end >= 99.5;
}
function onZoom() { followTail = atTail(); }

// Data helpers
function lastId() { return ticks.length ? ticks[ticks.length - 1].id : 0; }
function firstId() { return ticks.length ? ticks[0].id : 0; }
function trimToWindow() {
  if (ticks.length > windowSize) ticks = ticks.slice(ticks.length - windowSize);
}

async function loadInitial() {
  const latest = await fetchJSON('/api/ticks/latest');
  if (!latest?.id) return;

  const from = Math.max(1, latest.id - windowSize + 1);
  const arr = await fetchJSON(`/api/ticks?from_id=${from}&to_id=${latest.id}`);
  ticks = arr.sort((a,b)=>a.id-b.id);
  await refreshZigs();
  redraw();
}

async function loadLeft(n) {
  if (!ticks.length) return;
  const from = Math.max(1, firstId() - n);
  const to   = firstId() - 1;
  if (to < from) return;

  const older = await fetchJSON(`/api/ticks?from_id=${from}&to_id=${to}`);
  older.sort((a,b)=>a.id-b.id);
  ticks = older.concat(ticks);
  trimToWindow();
  await refreshZigs();
  redraw();
}

async function refreshZigs() {
  if (!ticks.length) return;
  const from = firstId();
  const to   = lastId();
  const z = await fetchJSON(`/api/zigzag?from_id=${from}&to_id=${to}`);
  legsMin = z.filter(r=>r.kind==='min');
  legsMid = z.filter(r=>r.kind==='mid');
  legsMax = z.filter(r=>r.kind==='max');
}

function redraw() {
  const s = [];

  // Ticks -> lines
  if (el('chkAsk').checked) s.push(priceLineSeries('ask', ticksToLine(ticks,'ask'), 10));
  if (el('chkMid').checked) s.push(priceLineSeries('mid', ticksToLine(ticks,'mid'), 11));
  if (el('chkBid').checked) s.push(priceLineSeries('bid', ticksToLine(ticks,'bid'), 12));

  // Zigzags -> lines (no scatter)
  if (el('chkMin').checked) s.push(priceLineSeries('min', legsToPath(legsMin), 20));
  if (el('chkZMid').checked) s.push(priceLineSeries('mid(zig)', legsToPath(legsMid), 21));
  if (el('chkMax').checked) s.push(priceLineSeries('max', legsToPath(legsMax), 22));

  chart.setOption({ series: s }, true);

  if (followTail) chart.dispatchAction({ type: 'dataZoom', end: 100 });
}

async function liveLoop() {
  try {
    if (!paused) {
      const t = await fetchJSON('/api/ticks/latest');
      if (t?.id && (!ticks.length || t.id > lastId())) {
        ticks.push(t);
        trimToWindow();
        await refreshZigs();
        redraw();
      }
    }
  } catch (e) {
    console.error('live tick error', e);
  } finally {
    setTimeout(liveLoop, 900);
  }
}

function init() {
  wireUI();
  chart = makeChart(document.getElementById('chart'));
  chart.on('dataZoom', onZoom);
  loadInitial().then(()=>liveLoop());
}

window.addEventListener('load', init);
