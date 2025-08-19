// frontend/review-core.js — REPLACE ENTIRE FILE
(function(){
  const urlParams = new URLSearchParams(location.search);
  const START = parseInt(urlParams.get("start") || "1");
  let OFFSET = 0, LIMIT = 10000;            // chunked loading
  const TOTAL = 200000;
  let RUN = null;

  const chart = echarts.init(document.getElementById("chart"));
  const levelMap = new Map();
  const priceMap = new Map();

  // ---- helpers -------------------------------------------------------------
  function thin(data, maxKeep = 6000) {
    if (data.length <= maxKeep) return data;
    const n = data.length, step = Math.max(1, Math.floor(n / maxKeep));
    const out = [];
    for (let i = 0; i < n; i += step) out.push(data[i]);
    if (out[out.length - 1][0] !== data[n - 1][0]) out.push(data[n - 1]);
    return out;
  }
  // Bin by tick id (equal-width bins), averaging y within each bin
  function binAvg(data, bin = 50) {
    if (!data.length || bin <= 1) return data;
    const out = [];
    let sum = 0, cnt = 0, startX = data[0][0];
    for (let i = 0; i < data.length; i++) {
      const [x, y] = data[i];
      if (cnt === 0) startX = x;
      sum += (y ?? 0); cnt++;
      if (cnt === bin) {
        out.push([startX, sum / cnt]);
        sum = 0; cnt = 0;
      }
    }
    if (cnt > 0) out.push([startX, sum / cnt]);
    return out;
  }

  const opt = {
    backgroundColor: '#111',
    title: { text: '200k Review', textStyle:{color:'#ddd'}},
    tooltip: { trigger: 'axis', axisPointer: { type:'cross' }},
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
    dataZoom: [{type:'inside', filterMode:'filter'},{type:'slider', filterMode:'filter'}],
    series: []
  };
  chart.setOption(opt);

  const HD = {
    showSymbol: false,
    smooth: false,
    sampling: 'lttb',
    progressive: 4000,
    progressiveThreshold: 20000,
    lineStyle: { width: 1, opacity: 0.8 },
    connectNulls: true,
    clip: true
  };

  // NOTE: Cloud is OFF by default to keep chart clean; user can toggle it on.
  const layers = {
    raw:      {name:'Raw',        type:'scatter', data:[], yAxisIndex:0, symbolSize: 1.5},
    kalman:   {name:'Kalman',     type:'line',    data:[], yAxisIndex:0, ...HD},
    labelsUp: {name:'Up starts',  type:'scatter', data:[], yAxisIndex:0, symbol: 'triangle', symbolSize: 8},
    labelsDn: {name:'Down starts',type:'scatter', data:[], yAxisIndex:0, symbol: 'triangle', symbolRotate: 180, symbolSize: 8},
    preds:    {name:'p_up',       type:'line',    data:[], yAxisIndex:1, ...HD},
    cloud:    {name:'S(d) @ $2',  type:'line',    data:[], yAxisIndex:1, ...HD}
  };

  function setLegend() {
    const wanted = [];
    if (document.getElementById("cbRaw").checked)    wanted.push(layers.raw.name);
    if (document.getElementById("cbKalman").checked) wanted.push(layers.kalman.name);
    if (document.getElementById("cbLabels").checked) { wanted.push(layers.labelsUp.name); wanted.push(layers.labelsDn.name); }
    if (document.getElementById("cbPreds").checked)  { wanted.push(layers.preds.name); /* cloud opt-in via checkbox below */ }
    if (document.getElementById("cbCloud")?.checked) wanted.push(layers.cloud.name);
    chart.setOption({legend: {data: wanted}});
  }

  function refreshSeries() {
    const s = [];
    if (document.getElementById("cbRaw").checked)    s.push(layers.raw);
    if (document.getElementById("cbKalman").checked) s.push(layers.kalman);
    if (document.getElementById("cbLabels").checked) { s.push(layers.labelsUp); s.push(layers.labelsDn); }
    if (document.getElementById("cbPreds").checked)  s.push(layers.preds);
    if (document.getElementById("cbCloud")?.checked) s.push(layers.cloud);
    chart.setOption({series: s});
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
    raw = thin(raw, 6000);
    kal = thin(kal, 6000);

    // Labels at price level (kalman preferred)
    const yAt = (tid) => (levelMap.get(tid) ?? priceMap.get(tid) ?? null);
    const labsUp = j.labels.filter(x => x.is_segment_start && x.direction===1).map(x => [x.tickid, yAt(x.tickid)]);
    const labsDn = j.labels.filter(x => x.is_segment_start && x.direction===-1).map(x => [x.tickid, yAt(x.tickid)]);

    // Probabilities / cloud: **bin** aggressively to kill spider-web
    const BIN = 50; // 50-tick bins; adjust to 100 if you still see clutter
    let preds = binAvg(j.predictions.map(p => [p.tickid, p.p_up ?? 0]), BIN);
    let cloud = binAvg(j.predictions.map(p => {
      let v = 0;
      if (p.s_curve && p.s_curve.length) {
        const at2 = p.s_curve.find(x => Math.abs(x[0]-2.0) < 1e-6);
        v = at2 ? at2[1] : 0;
      }
      return [p.tickid, v];
    }), BIN);

    // append
    layers.raw.data.push(...raw);
    layers.kalman.data.push(...(kal));
    layers.labelsUp.data.push(...labsUp);
    layers.labelsDn.data.push(...labsDn);
    layers.preds.data.push(...preds);
    layers.cloud.data.push(...cloud);

    setLegend();
    refreshSeries();

    if (offset === 0) {
      chart.dispatchAction({type:'dataZoom', startValue: START, endValue: START + Math.min(8000, limit)});
    }
  }

  // UI
  // Add a hidden checkbox for cloud opt-in if not present
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
    chart.dispatchAction({type:'dataZoom', startValue: Math.max(START, tid-4000), endValue: tid+4000});
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
