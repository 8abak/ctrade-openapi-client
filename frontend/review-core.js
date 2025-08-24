// frontend/review-core.js — dark mode + integer-only rendering, same chart structure
// Keeps your current data endpoints (/kalman_layers and /sqlvw/query).
// No decimals anywhere: axis labels, tooltips, plotted y-values are rounded to whole dollars.

(function () {
  // ---------- helpers ----------
  const $  = (sel) => document.querySelector(sel);

  // integer helpers (no decimals)
  const iRound = (v) => (v == null ? v : Math.round(Number(v)));
  const asInt  = (v) => (v == null ? '' : String(iRound(v)));

  // status line
  function setStatus(text, kind = 'info') {
    const el = $('#status');
    if (!el) return;
    el.textContent = text;
    el.style.color =
      kind === 'ok'  ? '#7ee787' :
      kind === 'err' ? '#ffa198' : '#8b949e';
  }

  // ---------- data access ----------
  async function fetchKalman(start, end) {
    const res = await fetch(`/kalman_layers?start=${start}&end=${end}`);
    if (!res.ok) throw new Error(`kalman_layers: HTTP ${res.status}`);
    return await res.json(); // [{tickid, k1, k1_rts, k2_cv}]
  }
  async function fetchTicks(start, end) {
    const sql = `SELECT id, bid, ask, mid FROM ticks WHERE id BETWEEN ${start} AND ${end} ORDER BY id`;
    const url = `/sqlvw/query?query=${encodeURIComponent(sql)}`;
    const res = await fetch(url);
    if (!res.ok) throw new Error(`sqlvw: HTTP ${res.status}`);
    return await res.json(); // [{id,bid,ask,mid}]
  }

  // ---------- transforms (keep structure) ----------
  const xy    = (rows, xKey, yKey) => rows.map(r => [r[xKey], iRound(r[yKey])]);
  const asXY  = (rows, key)        => rows.map(r => [r.tickid, iRound(r[key])]);

  // ---------- chart init ----------
  const chartEl = $('#chart');
  const chart = echarts.init(chartEl, null, { renderer: 'canvas' });

  function render(krows, trows) {
    const mid  = xy(trows, 'id', 'mid');
    const bid  = xy(trows, 'id', 'bid');
    const ask  = xy(trows, 'id', 'ask');
    const k1   = asXY(krows, 'k1');
    const k1r  = asXY(krows, 'k1_rts');
    const kbig = asXY(krows, 'k2_cv');

    const option = {
      backgroundColor: '#0d1117',
      animation: false,
      textStyle: { color: '#c9d1d9' },
      legend: {
        top: 6,
        textStyle: { color: '#aeb9cc' },
        selectedMode: 'multiple',
        selected: {
          'Mid': true, 'Bid': false, 'Ask': false,
          'k1 (old Kalman)': true, 'k1_rts (RTS)': true, 'Big-Move': true
        }
      },
      grid: { left: 48, right: 20, top: 32, bottom: 64 },
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'cross' },
        backgroundColor: '#101826',
        borderColor: '#26314a',
        textStyle: { color: '#dce6f2' },
        valueFormatter: (v) => asInt(v) // integers in tooltip
      },
      xAxis: {
        type: 'value',
        name: 'tick',
        nameTextStyle: { color: '#8b949e' },
        axisLabel: { color: '#8b949e' },
        axisLine:  { lineStyle: { color: '#30363d' } },
        splitLine: { show: true, lineStyle: { color: '#21262d', type: 'dashed' } }
      },
      yAxis: {
        type: 'value',
        scale: true,
        minInterval: 1, // whole-dollar grid steps
        axisLabel: { color: '#8b949e', formatter: (v) => asInt(v) }, // integers on axis
        axisLine:  { lineStyle: { color: '#30363d' } },
        splitLine: { show: true, lineStyle: { color: '#21262d' } }
      },
      dataZoom: [
        { type: 'inside', throttle: 50 },
        { type: 'slider', height: 18, bottom: 26, backgroundColor: '#0f1524', borderColor: '#2a3654' }
      ],
      series: [
        { name: 'Mid', type: 'line', showSymbol: false, smooth: false, sampling: 'lttb', large: true, largeThreshold: 10000, lineStyle: { width: 1.2 }, data: mid },
        { name: 'Bid', type: 'line', showSymbol: false, smooth: false, sampling: 'lttb', large: true, largeThreshold: 10000, lineStyle: { width: 0.9, opacity: 0.7 }, data: bid },
        { name: 'Ask', type: 'line', showSymbol: false, smooth: false, sampling: 'lttb', large: true, largeThreshold: 10000, lineStyle: { width: 0.9, opacity: 0.7 }, data: ask },
        { name: 'k1 (old Kalman)', type: 'line', showSymbol: false, smooth: false, lineStyle: { width: 1.5 }, data: k1 },
        { name: 'k1_rts (RTS)',    type: 'line', showSymbol: false, smooth: true,  lineStyle: { width: 1.1 }, opacity: 0.95, data: k1r },
        { name: 'Big-Move',        type: 'line', showSymbol: false, smooth: false, lineStyle: { width: 2.2 }, data: kbig }
      ]
    };

    chart.setOption(option, true);
  }

  // ---------- UI wiring ----------
  async function loadRange() {
    const start = parseInt($('#startTick').value || '1', 10);
    const end   = parseInt($('#endTick').value || '100000', 10);
    try {
      setStatus('Loading data…');
      const [krows, trows] = await Promise.all([fetchKalman(start, end), fetchTicks(start, end)]);
      render(krows, trows);
      setStatus(`Loaded: ticks ${trows.length}, layers ${krows.length}`, 'ok');

      // Auto-zoom to first 5k points if huge
      if (krows && krows.length > 5000) {
        const first = krows[0].tickid;
        chart.dispatchAction({ type: 'dataZoom', startValue: first, endValue: first + 5000 });
      }
    } catch (err) {
      console.error(err);
      setStatus(`Failed to load data: ${err.message || err}`, 'err');
      chart.setOption({
        title: { text: 'Failed to load data', left: 'center', top: 'middle', textStyle: { color: '#ee8888' } }
      });
    }
  }

  // Legend toggles via toolbar checkboxes (preserve series names)
  const legendMap = {
    chkMid: 'Mid', chkBid: 'Bid', chkAsk: 'Ask',
    chkK1: 'k1 (old Kalman)', chkRTS: 'k1_rts (RTS)', chkBM: 'Big-Move'
  };
  for (const id in legendMap) {
    const el = $('#' + id);
    if (el) el.addEventListener('change', () => {
      const name = legendMap[id];
      chart.dispatchAction({ type: el.checked ? 'legendSelect' : 'legendUnSelect', name });
    });
  }

  // Buttons
  $('#loadBtn').addEventListener('click', () => loadRange().catch(console.error));
  $('#jumpBtn').addEventListener('click', () => {
    const v = parseInt($('#jumpTick').value || '', 10);
    if (!Number.isFinite(v)) return;
    chart.dispatchAction({ type: 'dataZoom', startValue: v - 2000, endValue: v + 2000 });
  });

  // “Run” is reserved for the ML pipeline; backend endpoints are not enabled on this host
  $('#runBtn').addEventListener('click', () => {
    setStatus('Run clicked — ML pipeline endpoints not enabled on this host (expected 404 on /api/walkforward/*).', 'err');
  });

  // Initial load on first paint
  loadRange().catch(console.error);
})();
