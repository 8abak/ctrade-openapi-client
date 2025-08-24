// frontend/review-core.js — dark + integer-only; Run calls /walkforward/*

(function () {
  const $ = (sel) => document.querySelector(sel);

  const iRound = (v) => (v == null ? v : Math.round(Number(v)));
  const asInt  = (v) => (v == null ? '' : String(iRound(v)));

  function setStatus(text, kind = 'info') {
    const el = $('#status'); if (!el) return;
    el.textContent = text;
    el.style.color = kind === 'ok' ? '#7ee787' : kind === 'err' ? '#ffa198' : '#8b949e';
  }

  // existing data sources
  async function fetchKalman(start, end) {
    const res = await fetch(`/kalman_layers?start=${start}&end=${end}`);
    if (!res.ok) throw new Error(`kalman_layers: HTTP ${res.status}`);
    return await res.json();
  }
  async function fetchTicks(start, end) {
    const sql = `SELECT id, bid, ask, mid FROM ticks WHERE id BETWEEN ${start} AND ${end} ORDER BY id`;
    const url = `/sqlvw/query?query=${encodeURIComponent(sql)}`;
    const res = await fetch(url);
    if (!res.ok) throw new Error(`sqlvw: HTTP ${res.status}`);
    return await res.json();
  }

  // walk-forward endpoints (root; nginx already proxies "/" to backend)
  async function wfStep() { const r = await fetch('/walkforward/step', { method: 'POST' }); if (!r.ok) throw new Error(`step: ${r.status}`); return r.json(); }
  async function wfSnap() { const r = await fetch('/walkforward/snapshot'); if (!r.ok) throw new Error(`snapshot: ${r.status}`); return r.json(); }

  const chart = echarts.init($('#chart'), null, { renderer: 'canvas' });

  const xy   = (rows, xKey, yKey) => rows.map(r => [r[xKey], iRound(r[yKey])]);
  const asXY = (rows, key)        => rows.map(r => [r.tickid, iRound(r[key])]);

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
        top: 6, textStyle: { color: '#aeb9cc' },
        selectedMode: 'multiple',
        selected: { 'Mid': true, 'Bid': false, 'Ask': false, 'k1 (old Kalman)': true, 'k1_rts (RTS)': true, 'Big-Move': true }
      },
      grid: { left: 48, right: 20, top: 32, bottom: 64 },
      tooltip: {
        trigger: 'axis', axisPointer: { type: 'cross' },
        backgroundColor: '#101826', borderColor: '#26314a', textStyle: { color: '#dce6f2' },
        valueFormatter: (v) => asInt(v)
      },
      xAxis: {
        type: 'value', name: 'tick',
        nameTextStyle: { color: '#8b949e' }, axisLabel: { color: '#8b949e' }, axisLine: { lineStyle: { color: '#30363d' } },
        splitLine: { show: true, lineStyle: { color: '#21262d', type: 'dashed' } }
      },
      yAxis: {
        type: 'value', scale: true, minInterval: 1,
        axisLabel: { color: '#8b949e', formatter: (v) => asInt(v) },
        axisLine: { lineStyle: { color: '#30363d' } }, splitLine: { show: true, lineStyle: { color: '#21262d' } }
      },
      dataZoom: [
        { type: 'inside', throttle: 50 },
        { type: 'slider', height: 18, bottom: 26, backgroundColor: '#0f1524', borderColor: '#2a3654' }
      ],
      series: [
        { name: 'Mid', type: 'line', showSymbol: false, sampling: 'lttb', large: true, largeThreshold: 10000, lineStyle: { width: 1.2 }, data: mid },
        { name: 'Bid', type: 'line', showSymbol: false, sampling: 'lttb', large: true, largeThreshold: 10000, lineStyle: { width: 0.9, opacity: 0.7 }, data: bid },
        { name: 'Ask', type: 'line', showSymbol: false, sampling: 'lttb', large: true, largeThreshold: 10000, lineStyle: { width: 0.9, opacity: 0.7 }, data: ask },
        { name: 'k1 (old Kalman)', type: 'line', showSymbol: false, lineStyle: { width: 1.5 }, data: k1 },
        { name: 'k1_rts (RTS)',    type: 'line', showSymbol: false, smooth: true,  lineStyle: { width: 1.1 }, opacity: 0.95, data: k1r },
        { name: 'Big-Move',        type: 'line', showSymbol: false, lineStyle: { width: 2.2 }, data: kbig }
      ]
    };

    chart.setOption(option, true);
  }

  async function loadRange() {
    const start = parseInt($('#startTick').value || '1', 10);
    const end   = parseInt($('#endTick').value || '100000', 10);
    try {
      setStatus('Loading data…');
      const [krows, trows] = await Promise.all([fetchKalman(start, end), fetchTicks(start, end)]);
      render(krows, trows);
      setStatus(`Loaded: ticks ${trows.length}, layers ${krows.length}`, 'ok');
      if (krows && krows.length > 5000) {
        const first = krows[0].tickid;
        chart.dispatchAction({ type: 'dataZoom', startValue: first, endValue: first + 5000 });
      }
    } catch (err) {
      console.error(err);
      setStatus(`Failed to load data: ${err.message || err}`, 'err');
      chart.setOption({ title: { text: 'Failed to load data', left: 'center', top: 'middle', textStyle: { color: '#ee8888' } } });
    }
  }

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

  $('#loadBtn').addEventListener('click', () => loadRange().catch(console.error));
  $('#jumpBtn').addEventListener('click', () => {
    const v = parseInt($('#jumpTick').value || '', 10);
    if (!Number.isFinite(v)) return;
    chart.dispatchAction({ type: 'dataZoom', startValue: v - 2000, endValue: v + 2000 });
  });

  $('#runBtn').addEventListener('click', async () => {
    const btn = $('#runBtn'); btn.disabled = true; btn.textContent = 'Running…';
    setStatus('Working…', 'info');
    try {
      const res = await wfStep();
      const m = res?.macro_segments ?? {};
      const e = res?.micro_events ?? {};
      const o = res?.outcomes ?? {};
      const p = res?.predictions ?? {};
      const summary = [
        `Segments +${m.segments_added ?? 0}`,
        `Events +${e.events_added ?? 0}`,
        `Outcomes +${o.outcomes_resolved ?? 0}`,
        p.trained ? `Preds ${p.written ?? 0} (τ=${(p.threshold ?? 0).toFixed(2)})` : 'Preds —'
      ].join(' · ');
      setStatus(summary, 'ok');
    } catch (err) {
      console.error(err);
      setStatus('Run failed: ' + (err.message || err), 'err');
    } finally {
      btn.disabled = false; btn.textContent = 'Run';
    }
  });

  loadRange().catch(console.error);
})();
