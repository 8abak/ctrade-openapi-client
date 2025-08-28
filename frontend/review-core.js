/* Price-Action Segments Review UI
 * - Shows latest outcomes (journal)
 * - Click an outcome to load its segment ticks/smal/pred
 * - "Run until now" triggers POST /api/run (server processes segments)
 */
(() => {
  const elRun = document.getElementById('runBtn');
  const elRunStatus = document.getElementById('runStatus');
  const elBody = document.getElementById('outcomeBody');
  const elSegTitle = document.getElementById('segTitle');
  const elSegMeta = document.getElementById('segMeta');
  const chart = echarts.init(document.getElementById('chart'));

  const fmt = (d) => new Date(d).toLocaleString();
  const fetchJSON = (u, opt) => fetch(u, opt).then(r => {
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return r.json();
  });

  function setStatus(t) { elRunStatus.textContent = t; }

  async function loadOutcomes() {
    const rows = await fetchJSON('/api/outcome?limit=100');
    elBody.innerHTML = '';
    for (const r of rows) {
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td>${r.id}</td>
        <td>${fmt(r.time)}</td>
        <td>${r.duration}</td>
        <td>${r.predictions}</td>
        <td>${r.ratio}</td>
        <td>${r.dir}</td>
        <td>${r.len}</td>
      `;
      tr.style.cursor = 'pointer';
      tr.addEventListener('click', () => loadSegment(r.segm_id));
      elBody.appendChild(tr);
    }
    if (rows.length) loadSegment(rows[0].segm_id);
  }

  function smoothMA(series, n) {
    const out = [];
    const q = [];
    let s = 0;
    for (let i = 0; i < series.length; i++) {
      q.push(series[i]); s += series[i];
      if (q.length > n) s -= q.shift();
      out.push(s / q.length);
    }
    return out;
  }

  function renderChart(segm, ticks, smal, pred) {
    const xs = ticks.map(t => t.id);
    const ts = ticks.map(t => new Date(t.ts));
    const ys = ticks.map(t => Number(t.mid));
    const ma = smoothMA(ys, Math.min(100, Math.max(10, Math.floor(ys.length / 10))));

    // Build scatter markers for predictions
    const predHit = pred.filter(p => p.hit === true).map(p => {
      const idx = xs.indexOf(p.at_id);
      return [ts[idx], ys[idx]];
    });
    const predMiss = pred.filter(p => p.hit === false).map(p => {
      const idx = xs.indexOf(p.at_id);
      return [ts[idx], ys[idx]];
    });
    const predWait = pred.filter(p => p.hit === null).map(p => {
      const idx = xs.indexOf(p.at_id);
      return [ts[idx], ys[idx]];
    });

    // smal ranges as lines
    const smalLines = smal.map(s => {
      const aIdx = xs.indexOf(s.a_id), bIdx = xs.indexOf(s.b_id);
      return [
        { coord: [ts[aIdx], ys[aIdx]] },
        { coord: [ts[bIdx], ys[bIdx]] }
      ];
    });

    chart.setOption({
      animation: false,
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'cross' }
      },
      xAxis: {
        type: 'time',
        boundaryGap: false,
      },
      yAxis: { type: 'value', scale: true },
      series: [
        {
          type: 'line',
          name: 'Mid',
          showSymbol: false,
          data: ts.map((t, i) => [t, ys[i]]),
        },
        {
          type: 'line',
          name: 'MA',
          showSymbol: false,
          data: ts.map((t, i) => [t, ma[i]]),
        },
        {
          type: 'scatter',
          name: 'Pred ✓',
          data: predHit,
          symbol: 'circle',
          symbolSize: 8,
        },
        {
          type: 'scatter',
          name: 'Pred ✗',
          data: predMiss,
          symbol: 'diamond',
          symbolSize: 8,
        },
        {
          type: 'scatter',
          name: 'Pred …',
          data: predWait,
          symbol: 'triangle',
          symbolSize: 8,
        },
        {
          type: 'lines',
          name: 'Small moves',
          coordinateSystem: 'cartesian2d',
          polyline: false,
          lineStyle: { width: 2 },
          data: smalLines
        }
      ],
      legend: { top: 10 },
      grid: { left: 10, right: 10, top: 40, bottom: 10, containLabel: true },
    });

    elSegTitle.textContent = `Segment #${segm.id} (${segm.dir})`;
    elSegMeta.textContent = `Ticks ${segm.start_id}→${segm.end_id} | ${fmt(segm.start_ts)} → ${fmt(segm.end_ts)} | span=${Number(segm.span).toFixed(2)} | len=${segm.len}`;
  }

  async function loadSegment(segmId) {
    const data = await fetchJSON(`/api/segm?id=${segmId}`);
    renderChart(data.segm, data.ticks, data.smal, data.pred);
  }

  elRun.addEventListener('click', async () => {
    setStatus('running…');
    try {
      const res = await fetchJSON('/api/run', { method: 'POST' });
      setStatus(`done (${res.segments} seg)`);
      await loadOutcomes();
    } catch (e) {
      console.error(e);
      setStatus('error');
      alert('Run failed. See console.');
    }
  });

  loadOutcomes().catch(console.error);
})();
