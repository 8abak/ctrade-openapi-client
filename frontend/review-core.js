(async function() {
  const chartEl = document.getElementById('chart');
  const chart = echarts.init(chartEl, null, { renderer: 'canvas' });

  async function fetchKalman(start, end) {
    // Adjust to your actual API route if different:
    const url = `/api/kalman_layers?start=${start}&end=${end}`;
    const res = await fetch(url);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json(); // [{tickid, k1, k1_rts, k2_cv}]
  }

  function toSeriesData(rows, key) {
    return rows.map(r => [r.tickid, r[key]]);
  }

  function render(rows) {
    const k1     = toSeriesData(rows, 'k1');      // old straight-edge
    const k1rts  = toSeriesData(rows, 'k1_rts');  // RTS
    const kbig   = toSeriesData(rows, 'k2_cv');   // big-move tracker

    const option = {
      backgroundColor: '#0b0f1a',
      animation: false,
      textStyle: { color: '#c7d2e1' },
      grid: { left: 45, right: 20, top: 20, bottom: 60 },
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'line' },
        backgroundColor: '#101826',
        borderColor: '#26314a',
        textStyle: { color: '#dce6f2' },
        valueFormatter: v => (v != null ? v.toFixed(2) : v)
      },
      xAxis: {
        type: 'value',
        name: 'tick',
        nameTextStyle: { color: '#6b7a99' },
        axisLabel: { color: '#98a7c7' },
        axisLine: { lineStyle: { color: '#24304a' } },
        splitLine: { show: true, lineStyle: { color: '#1b2438', type: 'dashed' } }
      },
      yAxis: {
        type: 'value',
        scale: true,
        axisLabel: { color: '#98a7c7' },
        axisLine: { lineStyle: { color: '#24304a' } },
        splitLine: { show: true, lineStyle: { color: '#1b2438' } }
      },
      dataZoom: [
        { type: 'inside', throttle: 50 },
        { type: 'slider', height: 18, bottom: 24, backgroundColor: '#0f1524', borderColor: '#2a3654' }
      ],
      series: [
        {
          name: 'k1 (old Kalman)',
          type: 'line',
          showSymbol: false,
          smooth: false,            // straight segments feel
          lineStyle: { width: 1.5 },
          data: k1
        },
        {
          name: 'k1_rts (RTS)',
          type: 'line',
          showSymbol: false,
          smooth: true,             // smoothed look
          lineStyle: { width: 1 },
          opacity: 0.9,
          data: k1rts
        },
        {
          name: 'Big-Move',
          type: 'line',
          showSymbol: false,
          smooth: false,
          lineStyle: { width: 2.2 },
          data: kbig
        }
      ]
    };
    chart.setOption(option, true);
  }

  async function loadRange() {
    const start = parseInt(document.getElementById('startTick').value, 10);
    const end   = parseInt(document.getElementById('endTick').value, 10);
    const rows  = await fetchKalman(start, end);
    render(rows);
    // auto zoom to the first ~5k points to avoid overdraw while still seeing detail
    if (rows.length > 5000) {
      chart.dispatchAction({
        type: 'dataZoom',
        startValue: rows[0].tickid,
        endValue: rows[0].tickid + 5000
      });
    }
  }

  document.getElementById('loadBtn').addEventListener('click', loadRange);

  document.getElementById('jumpBtn').addEventListener('click', () => {
    const v = parseInt(document.getElementById('jumpTick').value, 10);
    if (!Number.isFinite(v)) return;
    chart.dispatchAction({ type: 'dataZoom', startValue: v - 2000, endValue: v + 2000 });
  });

  // initial load
  loadRange().catch(err => {
    console.error(err);
    chart.setOption({
      title: { text: 'Failed to load data', left: 'center', top: 'middle', textStyle: { color: '#ee8888' } }
    });
  });
})();
