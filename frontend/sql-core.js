// frontend/review-core.js — REPLACE ENTIRE FILE
(function(){
  const urlParams = new URLSearchParams(location.search);
  const START = parseInt(urlParams.get("start") || "1");
  let OFFSET = 0, LIMIT = 10000; // chunked loading
  const TOTAL = 200000;
  let RUN = null;

  const chart = echarts.init(document.getElementById("chart"));
  const levelMap = new Map();
  const priceMap = new Map();

  const opt = {
    backgroundColor: '#111',
    title: { text: '200k Review', textStyle:{color:'#ddd'}},
    tooltip: { trigger: 'axis', axisPointer: { type:'cross' }},
    legend: { data: [], textStyle:{color:'#ddd'}},
    grid: { left: 60, right: 60, top: 50, bottom: 60 },
    xAxis: { type:'value', name:'tickid', axisLine:{lineStyle:{color:'#888'}}, axisLabel:{color:'#bbb'} },
    // FIX: let price axis ignore zero; compute min/max from data with a 5% pad
    yAxis: [
      {
        type:'value',
        name:'price',
        scale:true, // <— do not force zero into the range
        min: (v) => v.min - (v.max - v.min) * 0.05,
        max: (v) => v.max + (v.max - v.min) * 0.05,
        axisLine:{lineStyle:{color:'#888'}},
        axisLabel:{color:'#bbb'}
      },
      { type:'value', name:'prob', min:0, max:1, position:'right', axisLine:{lineStyle:{color:'#888'}}, axisLabel:{color:'#bbb'} }
    ],
    dataZoom: [{type:'inside'},{type:'slider'}],
    series: []
  };
  chart.setOption(opt);

  const layers = {
    raw:      {name:'Raw',        type:'scatter', data:[], yAxisIndex:0, symbolSize: 2},
    kalman:   {name:'Kalman',     type:'line',    data:[], yAxisIndex:0, smooth:true, showSymbol:false, lineStyle:{width:1}},
    labelsUp: {name:'Up starts',  type:'scatter', data:[], yAxisIndex:0, symbol: 'triangle', symbolSize: 8},
    labelsDn: {name:'Down starts',type:'scatter', data:[], yAxisIndex:0, symbol: 'triangle', symbolRotate: 180, symbolSize: 8},
    preds:    {name:'p_up',       type:'line',    data:[], yAxisIndex:1, showSymbol:false, lineStyle:{width:1, opacity:0.8}},
    cloud:    {name:'S(d) @ $2',  type:'line',    data:[], yAxisIndex:1, showSymbol:false, lineStyle:{width:1}}
  };

  function setLegend() {
    const wanted = [];
    if (document.getElementById("cbRaw").checked)    wanted.push(layers.raw.name);
    if (document.getElementById("cbKalman").checked) wanted.push(layers.kalman.name);
    if (document.getElementById("cbLabels").checked) { wanted.push(layers.labelsUp.name); wanted.push(layers.labelsDn.name); }
    if (document.getElementById("cbPreds").checked)  { wanted.push(layers.preds.name); wanted.push(layers.cloud.name); }
    chart.setOption({legend: {data: wanted}});
  }

  function refreshSeries() {
    const s = [];
    if (document.getElementById("cbRaw").checked)    s.push(layers.raw);
    if (document.getElementById("cbKalman").checked) s.push(layers.kalman);
    if (document.getElementById("cbLabels").checked) { s.push(layers.labelsUp); s.push(layers.labelsDn); }
    if (document.getElementById("cbPreds").checked)  { s.push(layers.preds); s.push(layers.cloud); }
    chart.setOption({series: s});
  }

  async function loadChunk(offset, limit) {
    const res = await fetch(`/ml/review?start=${START}&offset=${offset}&limit=${limit}`);
    const j = await res.json();
    if (j.run) RUN = j.run;

    document.getElementById("rangeInfo").textContent =
      `Train ${START}-${START+100000-1} / Test ${START+100000}-${START+200000-1} | Loaded ${j.range[0]}..${j.range[1]}`;

    const raw = j.ticks.map(r => {
      priceMap.set(r.tickid, r.price);
      return [r.tickid, r.price];
    });

    const kal = j.kalman.map(r => {
      levelMap.set(r.tickid, r.level);
      return [r.tickid, r.level];
    });

    const yAt = (tid) => (levelMap.get(tid) ?? priceMap.get(tid) ?? null);
    const labsUp = j.labels.filter(x => x.is_segment_start && x.direction===1).map(x => [x.tickid, yAt(x.tickid)]);
    const labsDn = j.labels.filter(x => x.is_segment_start && x.direction===-1).map(x => [x.tickid, yAt(x.tickid)]);

    const preds = j.predictions.map(p => [p.tickid, p.p_up ?? 0]);
    const cloud = j.predictions.map(p => {
      let v = 0;
      if (p.s_curve && p.s_curve.length) {
        const at2 = p.s_curve.find(x => Math.abs(x[0]-2.0) < 1e-6);
        v = at2 ? at2[1] : 0;
      }
      return [p.tickid, v];
    });

    layers.raw.data.push(...raw);
    layers.kalman.data.push(...kal);
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

  document.getElementById("cbRaw").onchange =
  document.getElementById("cbKalman").onchange =
  document.getElementById("cbLabels").onchange =
  document.getElementById("cbPreds").onchange = () => { setLegend(); refreshSeries(); };

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
