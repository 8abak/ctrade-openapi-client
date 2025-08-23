(async function() {
  const chartEl = document.getElementById('chart');
  const chart = echarts.init(chartEl, null, { renderer: 'canvas' });

  // --- API helpers ---
  async function fetchKalman(start, end) {
    const res = await fetch(`/kalman_layers?start=${start}&end=${end}`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json(); // [{tickid, k1, k1_rts, k2_cv}]
  }
  async function fetchTicks(start, end) {
    const sql = `SELECT id, bid, ask, mid FROM ticks WHERE id BETWEEN ${start} AND ${end} ORDER BY id`;
    const url = `/sqlvw/query?query=${encodeURIComponent(sql)}`; // uses existing endpoint
    const res = await fetch(url);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json(); // [{id,bid,ask,mid}]
  }

  // --- Transforms ---
  const xy = (rows, xKey, yKey) => rows.map(r => [r[xKey], r[yKey]]);
  const asXY = (rows, key) => rows.map(r => [r.tickid, r[key]]);

  function render(krows, trows) {
    const mid = xy(trows, 'id', 'mid');
    const bid = xy(trows, 'id', 'bid');
    const ask = xy(trows, 'id', 'ask');

    const k1    = asXY(krows, 'k1');
    const k1rts = asXY(krows, 'k1_rts');
    const kbig  = asXY(krows, 'k2_cv');

    const option = {
      backgroundColor: '#0b0f1a',
      animation: false,
      textStyle: { color: '#c7d2e1' },
      legend: {
        top: 4,
        textStyle: { color: '#aeb9cc' },
        selectedMode: 'multiple',
        selected: {
          'Mid': true, 'Bid': true, 'Ask': true,
          'k1 (old Kalman)': true, 'k1_rts (RTS)': true, 'Big-Move': true
        }
      },
      grid: { left: 45, right: 20, top: 28, bottom: 60 },
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'line' },
        backgroundColor: '#101826',
        borderColor: '#26314a',
        textStyle: { color: '#dce6f2' },
        valueFormatter: v => (v != null ? Number(v).toFixed(2) : v)
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
        { name: 'Mid', type: 'line', showSymbol: false, smooth: false, sampling: 'lttb', large: true, largeThreshold: 10000, lineStyle: { width: 1.1 }, data: mid },
        { name: 'Bid', type: 'line', showSymbol: false, smooth: false, sampling: 'lttb', large: true, largeThreshold: 10000, lineStyle: { width: 0.8, opacity: 0.7 }, data: bid },
        { name: 'Ask', type: 'line', showSymbol: false, smooth: false, sampling: 'lttb', large: true, largeThreshold: 10000, lineStyle: { width: 0.8, opacity: 0.7 }, data: ask },

        { name: 'k1 (old Kalman)', type: 'line', showSymbol: false, smooth: false, lineStyle: { width: 1.5 }, data: k1 },
        { name: 'k1_rts (RTS)',     type: 'line', showSymbol: false, smooth: true,  lineStyle: { width: 1 },   opacity: 0.9, data: k1rts },
        { name: 'Big-Move',         type: 'line', showSymbol: false, smooth: false, lineStyle: { width: 2.2 }, data: kbig }
      ]
    };
    chart.setOption(option, true);
  }

  async function loadRange() {
    const start = parseInt(document.getElementById('startTick').value, 10);
    const end   = parseInt(document.getElementById('endTick').value, 10);
    const [krows, trows] = await Promise.all([fetchKalman(start, end), fetchTicks(start, end)]);
    render(krows, trows);

    if (krows.length > 5000) {
      chart.dispatchAction({ type: 'dataZoom', startValue: krows[0].tickid, endValue: krows[0].tickid + 5000 });
    }
  }

  // toolbar checkboxes -> legend toggle
  const legendMap = {
    chkMid: 'Mid', chkBid: 'Bid', chkAsk: 'Ask',
    chkK1: 'k1 (old Kalman)', chkRTS: 'k1_rts (RTS)', chkBM: 'Big-Move'
  };
  for (const id in legendMap) {
    const el = document.getElementById(id);
    if (el) el.addEventListener('change', () => {
      const name = legendMap[id];
      chart.dispatchAction({ type: el.checked ? 'legendSelect' : 'legendUnSelect', name });
    });
  }

  document.getElementById('loadBtn').addEventListener('click', () => loadRange().catch(err => console.error(err)));
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
