// frontend/review-core.js
// Restores the Segments panel with checkboxes (multi-select) + smart chart.
// Routes used (unchanged): 
//   GET /api/segm/recent?limit=200
//   GET /api/segm?id=<segmId>
// Also reads Chunk input and respects "Load more" buttons you already have.

(() => {
  const qs  = (s, r=document) => r.querySelector(s);
  const qsa = (s, r=document) => Array.from(r.querySelectorAll(s));

  const API = '/api';

  // ------- DOM -------
  const chunkInput = qs('#chunkInput') || qs('input[name="chunk"]');
  const chartEl    = qs('#reviewChart') || qs('#chart');
  if (!chartEl || !window.echarts) return;

  // Build/ensure Segments panel (left)
  let segWrap = qs('#segments') || qs('.segments-panel');
  if (!segWrap) {
    segWrap = document.createElement('div');
    segWrap.id = 'segments';
    const leftCol = qs('#leftCol') || qs('.left-col') || document.body;
    leftCol.appendChild(segWrap);
  }

  // Header + table
  segWrap.innerHTML = `
    <div class="segm-header" style="display:flex;align-items:center;gap:8px;margin-bottom:8px">
      <strong>Segments</strong>
      <span style="opacity:.7;font-size:.9em">(Select segments, then pick layers e.g., <b>ticks</b>, <b>atr1</b>)</span>
    </div>
    <div class="segm-scroll" style="max-height:40vh;overflow:auto;border:1px solid rgba(255,255,255,.08);border-radius:6px;">
      <table id="segmTable" class="segm-table" style="width:100%;border-collapse:collapse">
        <thead style="position:sticky;top:0;background:#0f1620">
          <tr>
            <th style="padding:6px 8px;width:36px"></th>
            <th style="padding:6px 8px">ID</th>
            <th style="padding:6px 8px">Start TS</th>
            <th style="padding:6px 8px">Duration(s)</th>
            <th style="padding:6px 8px">Dir</th>
            <th style="padding:6px 8px">Span</th>
            <th style="padding:6px 8px">Len</th>
          </tr>
        </thead>
        <tbody></tbody>
      </table>
    </div>
    <div id="seginfo" style="margin-top:6px;opacity:.85">Segment: —</div>
  `;
  const segmTableBody = segWrap.querySelector('tbody');
  const segInfo = qs('#seginfo');

  // ------- State -------
  const chart = echarts.init(chartEl);
  const State = {
    chunk: 2000,
    noThinning: false,
    showTicks: true,
    selectedIds: new Set(),           // segm IDs checked
    segmCache: new Map(),             // id -> { segm, ticks, bigm, smal, pred }
    xMin: null, xMax: null
  };

  // ------- Helpers -------
  const fmt2 = (x) => (x==null || isNaN(+x)) ? '' : (+x).toFixed(2);

  function thin(arr, n) {
    if (!arr || arr.length <= n) return arr || [];
    const stride = Math.ceil(arr.length / n);
    const out = [];
    for (let i = 0; i < arr.length; i += stride) out.push(arr[i]);
    if (arr.length && out[out.length-1] !== arr[arr.length-1]) out.push(arr[arr.length-1]);
    return out;
  }

  function mapTicksForSeries(ticks) {
    return (ticks||[]).map(t => ({ value: [new Date(t.ts), +t.mid], meta: t }));
  }
  function extractAtrFromTicks(ticks) {
    const out = [];
    for (const t of (ticks||[])) {
      const v = (t.atr1 != null ? t.atr1 : t.atr);
      if (v != null) out.push([new Date(t.ts), +v]);
    }
    return out;
  }

  function makeTimeIndex(ticks) {
    const pairs = (ticks||[]).map(t => [+new Date(t.ts), +t.mid]).sort((a,b)=>a[0]-b[0]);
    function yAt(ts) {
      if (!pairs.length) return null;
      const x = +new Date(ts);
      let lo=0, hi=pairs.length-1, best=pairs[0];
      while (lo<=hi) {
        const m=(lo+hi)>>1, dx=pairs[m][0]-x;
        if (Math.abs(dx)<Math.abs(best[0]-x)) best=pairs[m];
        if (dx===0) break;
        if (dx<0) lo=m+1; else hi=m-1;
      }
      return best[1];
    }
    return { yAt };
  }

  function extent(series, xMin, xMax) {
    let lo=Infinity, hi=-Infinity, got=false;
    for (const item of series||[]) {
      const p = Array.isArray(item) ? item : item.value;
      if (!p) continue;
      const x = +p[0], y = +p[1];
      if (xMin!=null && x < xMin) continue;
      if (xMax!=null && x > xMax) continue;
      if (y < lo) lo=y;
      if (y > hi) hi=y;
      got = true;
    }
    if (!got) return null;
    if (lo===hi) { const pad=Math.max(1e-6,Math.abs(hi)*0.001); lo-=pad; hi+=pad; }
    const pad=(hi-lo)*0.08;
    return [lo-pad, hi+pad];
  }

  // ------- Fetch -------
  async function fetchSegmList(limit=200) {
    const r = await fetch(`${API}/segm/recent?limit=${Math.max(1,Math.min(limit,500))}`);
    if (!r.ok) throw new Error('segm list failed');
    return r.json();
  }
  async function fetchSegm(id) {
    if (State.segmCache.has(id)) return State.segmCache.get(id);
    const r = await fetch(`${API}/segm?id=${id}`);
    if (!r.ok) throw new Error(`segm ${id} failed`);
    const data = await r.json();
    State.segmCache.set(id, data);
    return data;
  }

  // ------- UI: build segments table with checkboxes -------
  async function loadSegmList() {
    const rows = await fetchSegmList(200);
    segmTableBody.innerHTML = '';
    for (const s of rows) {
      const tr = document.createElement('tr');
      tr.dataset.start = s.start_ts;
      tr.dataset.end   = s.end_ts;
      const durSec = Math.max(0, (new Date(s.end_ts) - new Date(s.start_ts))/1000) | 0;

      tr.innerHTML = `
        <td style="padding:6px 8px">
          <input type="checkbox" class="segm-check" data-id="${s.id}">
        </td>
        <td style="padding:6px 8px">${s.id}</td>
        <td style="padding:6px 8px">${new Date(s.start_ts).toLocaleString()}</td>
        <td style="padding:6px 8px">${durSec}</td>
        <td style="padding:6px 8px">${s.dir ?? ''}</td>
        <td style="padding:6px 8px">${fmt2(s.span)}</td>
        <td style="padding:6px 8px">${s.len ?? ''}</td>
      `;

      // row click focuses window (doesn't toggle checkbox)
      tr.addEventListener('click', (ev) => {
        if (ev.target.closest('input[type=checkbox]')) return; // ignore clicks on checkbox itself
        focusWindow(s.start_ts, s.end_ts);
      });

      // checkbox controls selection
      tr.querySelector('.segm-check').addEventListener('change', async (ev) => {
        const id = Number(ev.target.dataset.id);
        if (ev.target.checked) State.selectedIds.add(id);
        else State.selectedIds.delete(id);
        await reloadChartForSelection();
      });

      segmTableBody.appendChild(tr);
    }
    segInfo.textContent = 'Segment: —';
  }

  // ------- Chart base option -------
  function setupChart() {
    chart.setOption({
      backgroundColor: '#0d1117',
      animation: false,
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'cross' },
        formatter: (params) => {
          const midP = params.find(p => p.seriesName === 'mid');
          const d = midP?.data?.meta;
          if (!d) return '';
          const dt = new Date(d.ts);
          return `${dt.toLocaleDateString()} ${dt.toLocaleTimeString()}<br/>id: ${d.id}<br/>mid: ${fmt2(d.mid)}`;
        }
      },
      grid: { left: 56, right: 48, top: 24, bottom: 56 },
      xAxis: [{ type: 'time', axisPointer: { show: true }, min: 'dataMin', max: 'dataMax' }],
      yAxis: [
        { type: 'value', scale: true, name: 'Price', axisLabel: { formatter: v => (+v).toFixed(2) } },
        { type: 'value', scale: true, name: 'ATR',   axisLabel: { formatter: v => (+v).toFixed(4) }, position: 'right' }
      ],
      dataZoom: [
        { type: 'inside', throttle: 0 },
        { type: 'slider', height: 20 }
      ],
      legend: { show: true },
      series: [
        { name:'mid',  type:'line', yAxisIndex:0, showSymbol:false, data:[], large:true, largeThreshold:200000, sampling:'lttb' },
        { name:'atr1', type:'line', yAxisIndex:1, showSymbol:false, data:[], large:true, largeThreshold:200000, sampling:'lttb', lineStyle:{width:1.5} },
        { name:'bigm', type:'line', data:[], markArea:{ itemStyle:{ color:'rgba(234,179,8,0.18)' }, data:[] } },
        { name:'smal', type:'line', showSymbol:false, data:[], markLine:{ silent:true, symbol:['none','none'], data:[] } },
        { name:'pred', type:'scatter', symbolSize:10, data:[], label:{ show:true, formatter:(p)=> p.data?.p?.hit===true?'✓':(p.data?.p?.hit===false?'✗':'') } }
      ]
    });

    chart.on('dataZoom', refitYAxes);
    window.addEventListener('resize', () => { chart.resize(); refitYAxes(); });
  }

  function currentXRange() {
    const opt = chart.getOption();
    const xa = opt?.xAxis?.[0];
    const mn = xa && xa.min != null ? +xa.min : null;
    const mx = xa && xa.max != null ? +xa.max : null;
    return [mn, mx];
  }

  function refitYAxes() {
    const opt = chart.getOption();
    const [xMin, xMax] = currentXRange();
    const series = opt?.series || [];

    const mid = series.find(s => s.name==='mid')?.data || [];
    const atr = series.find(s => s.name==='atr1')?.data || [];

    const left  = extent(mid, xMin, xMax);
    const right = extent(atr, xMin, xMax);

    const yAxis = opt.yAxis || [{type:'value',scale:true},{type:'value',scale:true,position:'right'}];
    if (left)  { yAxis[0].min = left[0];  yAxis[0].max = left[1];  yAxis[0].name='Price'; yAxis[0].axisLabel={formatter:v=>(+v).toFixed(2)}; }
    if (right) { yAxis[1].min = right[0]; yAxis[1].max = right[1]; yAxis[1].name='ATR';   yAxis[1].axisLabel={formatter:v=>(+v).toFixed(4)}; }

    chart.setOption({ yAxis }, false);
  }

  function focusWindow(startTs, endTs) {
    const s = isFinite(+startTs) ? +startTs : Date.parse(startTs);
    const e = isFinite(+endTs)   ? +endTs   : Date.parse(endTs);
    if (!isFinite(s) || !isFinite(e)) return;
    State.xMin = s; State.xMax = e;
    chart.setOption({ xAxis: [{ min:s, max:e }] });
    refitYAxes();
  }

  // ------- Build/merge data for current selection -------
  async function reloadChartForSelection() {
    // Limit to a sensible number to avoid overload on mobile
    const ids = Array.from(State.selectedIds).slice(0, 5);

    // Fetch & cache
    const segms = [];
    for (const id of ids) segms.push(await fetchSegm(id));

    // Merge ticks/atr/preds/small/big
    const allTicks = [];
    const allAtr   = [];
    const areas    = [];
    const lines    = [];
    const dots     = [];

    for (const d of segms) {
      const ticks = d.ticks || [];
      allTicks.push(...mapTicksForSeries(ticks));
      allAtr.push(...extractAtrFromTicks(ticks));

      // decorations
      const idx = makeTimeIndex(ticks);
      // big moves
      for (const b of (d.bigm || [])) {
        if (b?.a_ts && b?.b_ts) areas.push([{xAxis:new Date(b.a_ts)}, {xAxis:new Date(b.b_ts)}]);
      }
      // small moves
      for (const s of (d.smal || [])) {
        if (!(s?.a_ts && s?.b_ts)) continue;
        const y1 = idx.yAt(s.a_ts), y2 = idx.yAt(s.b_ts);
        if (y1==null || y2==null) continue;
        lines.push([{coord:[new Date(s.a_ts), +y1]}, {coord:[new Date(s.b_ts), +y2]}]);
      }
      // predictions
      for (const p of (d.pred || [])) {
        const x = p?.at_ts ? new Date(p.at_ts) : null;
        if (!x) continue;
        const y = idx.yAt(p.at_ts);
        if (y==null) continue;
        dots.push({
          value:[x, +y],
          p,
          itemStyle:{ color: p?.hit===true?'#2ea043' : (p?.hit===false?'#f85149':'#8b949e') },
          symbol: p?.hit==null? 'circle' : (p?.hit? 'triangle':'rect')
        });
      }
    }

    // Thinning (chunk) or no-thinning
    const midData = State.noThinning ? allTicks : thin(allTicks, State.chunk);
    const atrData = State.noThinning ? allAtr   : thin(allAtr,   State.chunk);

    // X window: if user focused, keep; else auto
    let xMin = State.xMin, xMax = State.xMax;
    if (xMin==null || xMax==null) {
      const full = [...midData, ...atrData];
      if (full.length) {
        full.sort((a,b)=> (Array.isArray(a)?a[0]:a.value[0]) - (Array.isArray(b)?b[0]:b.value[0]));
        const first = Array.isArray(full[0]) ? full[0][0] : full[0].value[0];
        const last  = Array.isArray(full[full.length-1]) ? full[full.length-1][0] : full[full.length-1].value[0];
        xMin = first; xMax = last;
      }
    }

    chart.setOption({
      xAxis: [{ min: xMin ?? 'dataMin', max: xMax ?? 'dataMax' }],
      series: [
        { name:'mid',  data: State.showTicks ? midData : [], sampling: State.noThinning ? undefined : 'lttb' },
        { name:'atr1', data: atrData, yAxisIndex:1, sampling: State.noThinning ? undefined : 'lttb' },
        { name:'bigm', data: [], markArea: { itemStyle:{color:'rgba(234,179,8,0.18)'}, data: areas } },
        { name:'smal', data: [], markLine: { silent:true, symbol:['none','none'], data: lines } },
        { name:'pred', data: dots }
      ]
    }, true);

    // Info line
    segInfo.textContent = ids.length
      ? `Selected segm: ${ids.join(', ')}`
      : 'Segment: —';

    refitYAxes();
  }

  // ------- Toggles -------
  function mountToggles() {
    const host = qs('.layers-row') || qs('#layersRow') || qs('.layers') || qs('#review') || document.body;

    if (!qs('#toggleNoThin')) {
      const label = document.createElement('label');
      label.style.marginLeft = '12px';
      label.innerHTML = `<input id="toggleNoThin" type="checkbox"> No thinning`;
      host.appendChild(label);
      label.firstElementChild.addEventListener('change', () => {
        State.noThinning = !!qs('#toggleNoThin').checked;
        reloadChartForSelection();
      });
    }
    if (!qs('#toggleShowTicks')) {
      const label = document.createElement('label');
      label.style.marginLeft = '12px';
      label.innerHTML = `<input id="toggleShowTicks" type="checkbox" checked> Show ticks`;
      host.appendChild(label);
      label.firstElementChild.addEventListener('change', () => {
        State.showTicks = !!qs('#toggleShowTicks').checked;
        reloadChartForSelection();
      });
    }
  }

  function wireChunk() {
    if (!chunkInput) return;
    const apply = () => {
      const v = +chunkInput.value;
      if (v > 0 && Number.isFinite(v)) {
        State.chunk = v;
        if (!State.noThinning) reloadChartForSelection();
      }
    };
    chunkInput.addEventListener('change', apply);
    apply();
  }

  // ------- Boot -------
  async function boot() {
    setupChart();
    mountToggles();
    wireChunk();
    await loadSegmList();
  }

  if (document.readyState === 'complete' || document.readyState === 'interactive') {
    setTimeout(boot, 0);
  } else {
    document.addEventListener('DOMContentLoaded', boot);
  }
})();