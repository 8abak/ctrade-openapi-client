// frontend/review-core.js — REPLACE ENTIRE FILE
(function(){
  const urlParams = new URLSearchParams(location.search);
  const START = parseInt(urlParams.get("start") || "1");

  // NEW: lighter initial paint + moderate increments afterwards
  const INITIAL_LIMIT = 2000;   // first load
  const CHUNK_LIMIT   = 5000;   // "Load More" increments
  let OFFSET = 0;
  let LIMIT = INITIAL_LIMIT;
  const TOTAL = 200000;
  let RUN = null;

  const chart = echarts.init(document.getElementById("chart"));
  const levelMap = new Map();
  const priceMap = new Map();

  // ---- helpers -------------------------------------------------------------
  function thin(data, maxKeep = 4000) { // more aggressive than before
    if (data.length <= maxKeep) return data;
    const n = data.length, step = Math.max(1, Math.floor(n / maxKeep));
    const out = [];
    for (let i = 0; i < n; i += step) out.push(data[i]);
    if (out[out.length - 1][0] !== data[n - 1][0]) out.push(data[n - 1]);
    return out;
  }
  function binAvg(data, bin = 200) { // NEW: strong binning to kill “web”
    if (!data.length || bin <= 1) return data;
    const out = [];
    let sum = 0, cnt = 0, startX = data[0][0];
    for (let i = 0; i < data.length; i++) {
      const [x, y] = data[i];
      if (cnt === 0) startX = x;
      sum += (y ?? 0); cnt++;
      if (cnt === bin) { out.push([startX, sum / cnt]); sum = 0; cnt = 0; }
    }
    if (cnt > 0) out.push([startX, sum / cnt]);
    return out;
  }
  // STRICTLY increasing X append (prevents zig‑zags)
  function appendUniqueSorted(targetArr, newArr){
    if (!newArr || !newArr.length) return;
    let lastX = targetArr.length ? targetArr[targetArr.length - 1][0] : -Infinity;
    for (let i = 0; i < newArr.length; i++) {
      const pt = newArr[i];
      if (!pt || pt.length < 2) continue;
      const x = pt[0];
      if (!Number.isFinite(x)) continue;
      if (x > lastX) { targetArr.push(pt); lastX = x; }
    }
  }

  const opt = {
    backgroundColor: '#111',
    title: { text: '200k Review', textStyle:{color:'#ddd'}},
    // Lighter tooltip & axis pointer to reduce RAF work
    tooltip: { trigger: 'axis', axisPointer: { type:'cross', animation:false }, transitionDuration: 0, confine: true },
    legend: { data: [], textStyle:{color:'#ddd'}},
    grid: { left: 60, right: 60, top: 50, bottom: 60 },
    animation: false,
    xAxis: { type:'value', name:'tickid', axisLine:{lineStyle:{color:'#888'}}, axisLabel:{color:'#bbb'} },
    yAxis: [
      {
        type:'value',
        name:'price',
        scale:true,
        min: (v) => v.min - (v.max - v.min) * 0.05,
        max: (v) => v.max + (v.max - v.min) * 0.05,
        axisLine:{lineStyle:{color:'#888'}},
        axisLabel:{color:'#bbb'}
      },
      { type:'value', name:'prob', min:0, max:1, position:'right',
        axisLine:{lineStyle:{color:'#888'}}, axisLabel:{color:'#bbb'} }
    ],
    dataZoom: [
      {type:'inside', filterMode:'filter', throttle: 50},
      {type:'slider', filterMode:'filter', throttle: 50}
    ],
    series: []
  };
  chart.setOption(opt);

  const HD = {
    showSymbol:false, smooth:false, sampling:'lttb',
    progressive:4000, progressiveThreshold:20000,
    lineStyle:{ width:1, opacity:0.7 }, connectNulls:false, clip:true
  };

  // Cloud opt‑in (kept off by default)
  const layers = {
    raw:      {name:'Raw',        type:'scatter', data:[], yAxisIndex:0, symbolSize:1.5, large:true, largeThreshold:1500},
    kalman:   {name:'Kalman',     type:'line',    data:[], yAxisIndex:0, ...HD},
    labelsUp: {name:'Up starts',  type:'scatter', data:[], yAxisIndex:0, symbol:'triangle', symbolSize:8},
    labelsDn: {name:'Down starts',type:'scatter', data:[], yAxisIndex:0, symbol:'triangle', symbolRotate:180, symbolSize:8},
    preds:    {name:'p_up',       type:'line',    data:[], yAxisIndex:1, ...HD},
    cloud:    {name:'S(d) @ $2',  type:'line',    data:[], yAxisIndex:1, ...HD}
  };

  function setLegend() {
    const wanted = [];
    if (document.getElementById("cbRaw").checked)    wanted.push(layers.raw.name);
    if (document.getElementById("cbKalman").checked) wanted.push(layers.kalman.name);
    if (document.getElementById("cbLabels").checked) { wanted.push(layers.labelsUp.name); wanted.push(layers.labelsDn.name); }
    if (document.getElementById("cbPreds").checked)  wanted.push(layers.preds.name);
    if (document.getElementById("cbCloud")?.checked) wanted.push(layers.cloud.name);
    chart.setOption({legend: {data: wanted}}, false, true);
  }
  function refreshSeries() {
    const s = [];
    if (document.getElementById("cbRaw").checked)    s.push(layers.raw);
    if (document.getElementById("cbKalman").checked) s.push(layers.kalman);
    if (document.getElementById("cbLabels").checked) { s.push(layers.labelsUp); s.push(layers.labelsDn); }
    if (document.getElementById("cbPreds").checked)  s.push(layers.preds);
    if (document.getElementById("cbCloud")?.checked) s.push(layers.cloud);
    chart.setOption({series: s}, false, true); // lazyUpdate to avoid blocking frame
  }

  async function loadChunk(offset, limit) {
    const res = await fetch(`/ml/review?start=${START}&offset=${offset}&limit=${limit}`);
    const j = await res.json();
    if (j.run) RUN = j.run;

    document.getElementById("rangeInfo").textContent =
      `Train ${START}-${START+100000-1} / Test ${START+100000}-${START+200000-1} | Loaded ${j.range[0]}..${j.range[1]}`;

    // Price series
    let raw = j.ticks.map(r => { priceMap.set(r.tickid, r.price); return [r.tickid, r.price]; });
    let kal = j.kalman.map(r => { levelMap.set(r.tickid, r.level); return [r.tickid, r.level]; });
    raw = thin(raw, 3000);   // tighter thin for initial feel
    kal = thin(kal, 3000);

    // Labels anchored to price
    const yAt = (tid) => (levelMap.get(tid) ?? priceMap.get(tid) ?? null);
    const labsUp = j.labels.filter(x => x.is_segment_start && x.direction===1).map(x => [x.tickid, yAt(x.tickid)]);
    const labsDn = j.labels.filter(x => x.is_segment_start && x.direction===-1).map(x => [x.tickid, yAt(x.tickid)]);

    // Probabilities / cloud — bin hard to keep ~few hundred points per chunk
    let preds = binAvg(j.predictions.map(p => [p.tickid, p.p_up ?? 0]), 200);
    let cloud = binAvg(j.predictions.map(p => {
      let v = 0;
      if (p.s_curve && p.s_curve.length) {
        const at2 = p.s_curve.find(x => Math.abs(x[0]-2.0) < 1e-6);
        v = at2 ? at2[1] : 0;
      }
      return [p.tickid, v];
    }), 200);

    // Append strictly increasing X
    appendUniqueSorted(layers.raw.data, raw);
    appendUniqueSorted(layers.kalman.data, kal);
    appendUniqueSorted(layers.preds.data, preds);
    appendUniqueSorted(layers.cloud.data, cloud);
    layers.labelsUp.data.push(...labsUp);
    layers.labelsDn.data.push(...labsDn);

    setLegend(); refreshSeries();

    if (offset === 0) {
      chart.dispatchAction({type:'dataZoom', startValue: START, endValue: START + Math.min(2000, limit)});
    }
  }

  // UI
  if (!document.getElementById("cbCloud")) {
    const lbl = document.createElement('label');
    lbl.innerHTML = '<input type="checkbox" id="cbCloud"/> Cloud';
    document.querySelector('.toolbar').insertBefore(lbl, document.getElementById('jumpTick'));
  }
  document.getElementById("cbRaw").onchange =
  document.getElementById("cbKalman").onchange =
  document.getElementById("cbLabels").onchange =
  document.getElementById("cbPreds").onchange =
  document.getElementById("cbCloud").onchange = () => { setLegend(); refreshSeries(); };

  document.getElementById("btnMore").onclick = async () => {
    const remaining = TOTAL - (OFFSET + LIMIT);
    if (remaining <= 0) return;
    OFFSET += LIMIT;
    LIMIT = CHUNK_LIMIT; // after first page, use larger increments
    await loadChunk(OFFSET, Math.min(LIMIT, remaining));
  };

  document.getElementById("btnJump").onclick = async () => {
    const tid = parseInt(document.getElementById("jumpTick").value || "0");
    if (!tid) return;
    const rel = tid - START;
    const needOffset = Math.floor(rel / LIMIT) * LIMIT;
    if (needOffset >= 0 && needOffset < TOTAL && needOffset !== OFFSET) {
      await loadChunk(needOffset, LIMIT);
      OFFSET = needOffset;
    }
    chart.dispatchAction({type:'dataZoom', startValue: Math.max(START, tid-1000), endValue: tid+1000});
  };

  document.getElementById("btnConfirm").onclick = async () => {
    let runId = RUN?.run_id;
    if (!runId) {
      const status = await (await fetch('/ml/status')).json();
      runId = status?.[0]?.run_id;
    }
    if (!runId) { alert("No run found to confirm."); return; }
    const res = await fetch(`/ml/confirm?run_id=${encodeURIComponent(runId)}`, {method:'POST'});
    const j = await res.json();
    if (j.ok) alert(`Confirmed ${runId}. Launch the next step when ready.`);
    else alert(`Confirm failed: ${JSON.stringify(j)}`);
  };

  window.addEventListener('resize', () => chart.resize());
  loadChunk(0, LIMIT);
})();
