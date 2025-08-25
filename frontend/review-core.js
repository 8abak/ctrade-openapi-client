// frontend/review-core.js — Walk-Forward UI with journal + integer-only display

(function () {
  window.__reviewBoot = 'ok'; // tiny smoke flag to see in console

  const $ = (sel) => document.querySelector(sel);

  // integer helpers
  const iRound = (v) => (v == null ? v : Math.round(Number(v)));
  const asInt  = (v) => (v == null ? '' : String(iRound(v)));

  // window size for Jump (± window on each side)
  const JUMP_WINDOW = 6000;

  function setStatus(text, kind = 'info') {
    const el = $('#status');
    if (!el) return;
    el.textContent = text;
    el.style.color = kind === 'ok' ? '#7ee787' : kind === 'err' ? '#ffa198' : '#8b949e';
  }

  function log(msg) {
    const j = $('#journal');
    if (!j) return;
    const ts = new Date().toISOString().replace('T',' ').replace('Z','');
    j.textContent += `[${ts}] ${msg}\n`;
    j.scrollTop = j.scrollHeight;
  }

  // ----- data -----
  async function fetchTicksRange(startId, endId) {
    const sql = `SELECT id, bid, ask, mid FROM ticks WHERE id BETWEEN ${startId} AND ${endId} ORDER BY id`;
    const url = `/sqlvw/query?query=${encodeURIComponent(sql)}`;
    const res = await fetch(url);
    if (!res.ok) throw new Error(`ticks range: HTTP ${res.status}`);
    return await res.json();
  }

  // legacy layer (ignored if not present)
  async function fetchKalman(startId, endId) {
    try {
      const res = await fetch(`/kalman_layers?start=${startId}&end=${endId}`);
      if (!res.ok) return [];
      return await res.json();
    } catch { return []; }
  }

  async function wfStep()   { const r = await fetch('/walkforward/step', { method: 'POST' }); if (!r.ok) throw new Error(`step: ${r.status}`); return r.json(); }
  async function wfSnap()   { const r = await fetch('/walkforward/snapshot'); if (!r.ok) throw new Error(`snapshot: ${r.status}`); return r.json(); }

  // ----- transforms -----
  const xy   = (rows, xKey, yKey) => rows.map(r => [r[xKey], iRound(r[yKey])]);

  // ----- chart -----
  let chart;
  function ensureChart() {
    if (chart) return chart;
    if (typeof echarts === 'undefined') { setStatus('ECharts not loaded', 'err'); return null; }
    chart = echarts.init($('#chart'), null, { renderer: 'canvas' });
    return chart;
  }

  function baseOption() {
    return {
      backgroundColor: '#0d1117',
      animation: false,
      textStyle: { color: '#c9d1d9' },
      legend: { top: 6, textStyle: { color: '#aeb9cc' }, selectedMode: 'multiple' },
      grid: { left: 48, right: 20, top: 32, bottom: 64 },
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'cross' },
        backgroundColor: '#101826',
        borderColor: '#26314a',
        textStyle: { color: '#dce6f2' },
        valueFormatter: (v) => asInt(v)
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
        minInterval: 1,
        axisLabel: { color: '#8b949e', formatter: (v) => asInt(v) },
        axisLine:  { lineStyle: { color: '#30363d' } },
        splitLine: { show: true, lineStyle: { color: '#21262d' } }
      },
      dataZoom: [
        { type: 'inside', throttle: 50 },
        { type: 'slider', height: 18, bottom: 26, backgroundColor: '#0f1524', borderColor: '#2a3654' }
      ],
      series: []
    };
  }

  function seriesForPrice(ticks) {
    const mid = xy(ticks, 'id', 'mid');
    const bid = xy(ticks, 'id', 'bid');
    const ask = xy(ticks, 'id', 'ask');
    return [
      { name: 'Mid', type: 'line', showSymbol: false, sampling: 'lttb', large: true, largeThreshold: 10000, lineStyle: { width: 1.2 }, data: mid },
      { name: 'Bid', type: 'line', showSymbol: false, sampling: 'lttb', large: true, largeThreshold: 10000, lineStyle: { width: 0.9, opacity: 0.7 }, data: bid },
      { name: 'Ask', type: 'line', showSymbol: false, sampling: 'lttb', large: true, largeThreshold: 10000, lineStyle: { width: 0.9, opacity: 0.7 }, data: ask },
    ];
  }

  function markAreasForMacro(segments, startId, endId) {
    const segs = (segments || []).filter(s => s.end_tick_id >= startId && s.start_tick_id <= endId);
    if (!segs.length) return [];
    const data = segs.map(s => {
      const dir = s.direction > 0 ? 1 : -1;
      const color = dir > 0 ? 'rgba(0,160,100,' + (0.10 + 0.20 * (s.confidence ?? 0.5)) + ')'
                            : 'rgba(200,50,60,' + (0.10 + 0.20 * (s.confidence ?? 0.5)) + ')';
      return [{
        xAxis: Math.max(s.start_tick_id, startId),
        yAxis: 'min',
        itemStyle: { color }
      }, {
        xAxis: Math.min(s.end_tick_id, endId),
        yAxis: 'max'
      }];
    });
    return [{
      name: 'Macro',
      type: 'line',
      data: [],
      markArea: { silent: true, itemStyle: { opacity: 1 }, data }
    }];
  }

  function seriesForEvents(events, startId, endId) {
    const evs = (events || []).filter(e => e.tick_id >= startId && e.tick_id <= endId);
    if (!evs.length) return [];
    const mapSymbol = (t) => t === 'pullback_end' ? 'triangle' : t === 'breakout' ? 'diamond' : 'circle';
    const mapColor  = (t) => t === 'pullback_end' ? '#58a6ff' : t === 'breakout' ? '#f2cc60' : '#b981f5';
    const data = evs.map(e => ({
      value: [e.tick_id, iRound(e.event_price)],
      name: e.event_type,
      symbol: mapSymbol(e.event_type),
      itemStyle: { color: mapColor(e.event_type) }
    }));
    return [{ name: 'Events', type: 'scatter', symbolSize: 9, data }];
  }

  function seriesForPreds(preds, events, startId, endId) {
    if (!preds?.length || !events?.length) return [];
    const evById = new Map(events.map(e => [e.event_id, e]));
    const rows = [];
    for (const p of preds) {
      const e = evById.get(p.event_id);
      if (!e) continue;
      if (e.tick_id < startId || e.tick_id > endId) continue;
      rows.push({
        value: [e.tick_id, iRound(e.event_price)],
        p_tp: p.p_tp ?? null, threshold: p.threshold ?? null,
        model_version: p.model_version ?? '', decided: !!p.decided, predicted_at: p.predicted_at
      });
    }
    if (!rows.length) return [];
    return [{
      name: 'Predictions',
      type: 'scatter',
      symbol: 'circle',
      symbolSize: 8,
      itemStyle: {
        color: (params) => {
          const p = params.data.p_tp ?? 0;
          const a = 0.25 + 0.45 * Math.max(0, Math.min(1, p));
          const g = Math.round(120 + 120 * p);
          const r = Math.round(60 * (1 - p));
          return `rgba(${r},${g},120,${a})`;
        }
      },
      tooltip: {
        formatter: (params) => {
          const d = params.data;
          return [
            `<b>Prediction</b>`,
            `tick: ${asInt(d.value[0])}`,
            `price: ${asInt(d.value[1])}`,
            `p_tp: ${(d.p_tp ?? 0).toFixed(3)}`,
            `τ: ${(d.threshold ?? 0).toFixed(3)}`,
            `model: ${d.model_version || '-'}`,
            `decided: ${d.decided ? 'yes' : 'no'}`,
            `at: ${d.predicted_at || ''}`
          ].join('<br/>');
        }
      },
      data: rows
    }];
  }

  function seriesForOutcomes(outcomes, events, startId, endId) {
    if (!outcomes?.length || !events?.length) return [];
    const evById = new Map(events.map(e => [e.event_id, e]));
    const rows = [];
    for (const o of outcomes) {
      const e = evById.get(o.event_id); if (!e) continue;
      if (e.tick_id < startId || e.tick_id > endId) continue;
      const col = o.outcome === 'TP' ? '#2ea043' : o.outcome === 'SL' ? '#f85149' : '#8b949e';
      rows.push({ value: [e.tick_id, iRound(e.event_price)], itemStyle: { color: col, borderColor: col } });
    }
    if (!rows.length) return [];
    return [{
      name: 'Outcomes',
      type: 'effectScatter',
      rippleEffect: { brushType: 'stroke', scale: 2.2 },
      symbolSize: 11,
      showEffectOn: 'render',
      data: rows
    }];
  }

  async function renderWindow(startId, endId) {
    const c = ensureChart(); if (!c) return;
    setStatus('Loading data…'); log(`Load window [${startId}, ${endId}]`);
    const [ticks, , snap] = await Promise.all([
      fetchTicksRange(startId, endId),
      fetchKalman(startId, endId), // ignored if empty
      wfSnap().catch(() => ({segments:[],events:[],predictions:[],outcomes:[]})),
    ]);

    const opt = baseOption();
    opt.series = [
      ...seriesForPrice(ticks),
      ...markAreasForMacro(snap.segments || [], startId, endId),
      ...seriesForEvents(snap.events || [], startId, endId),
      ...seriesForPreds(snap.predictions || [], snap.events || [], startId, endId),
      ...seriesForOutcomes(snap.outcomes || [], snap.events || [], startId, endId),
    ];

    const sel = {};
    sel['Mid'] = $('#chkMid').checked; sel['Bid'] = $('#chkBid').checked; sel['Ask'] = $('#chkAsk').checked;
    sel['Macro'] = $('#chkMacro').checked; sel['Events'] = $('#chkEvents').checked;
    sel['Predictions'] = $('#chkPreds').checked; sel['Outcomes'] = $('#chkOutcomes').checked;
    opt.legend.selected = sel;

    c.setOption(opt, true);
    setStatus(`Loaded ${ticks.length} ticks`, 'ok');
  }

  // ----- UI wiring -----
  function applyLegendFromToggles() {
    const map = { chkMid:'Mid', chkBid:'Bid', chkAsk:'Ask', chkMacro:'Macro', chkEvents:'Events', chkPreds:'Predictions', chkOutcomes:'Outcomes' };
    for (const id in map) {
      const el = $('#' + id); if (!el) continue;
      const name = map[id];
      chart?.dispatchAction({ type: el.checked ? 'legendSelect' : 'legendUnSelect', name });
    }
  }

  $('#loadBtn').addEventListener('click', async () => {
    const s = parseInt($('#startTick').value || '1', 10);
    const e = parseInt($('#endTick').value || (s + 12000), 10);
    try { await renderWindow(s, e); applyLegendFromToggles(); }
    catch (err) { console.error(err); setStatus('Load failed: ' + (err.message || err), 'err'); log('Load failed: ' + err); }
  });

  $('#jumpBtn').addEventListener('click', async () => {
    const t = parseInt($('#jumpTick').value || '1', 10);
    const s = Math.max(1, t - JUMP_WINDOW);
    const e = t + JUMP_WINDOW;
    $('#startTick').value = s;
    $('#endTick').value = e;
    try { await renderWindow(s, e); applyLegendFromToggles(); }
    catch (err) { console.error(err); setStatus('Jump failed: ' + (err.message || err), 'err'); log('Jump failed: ' + err); }
  });
  $('#jumpTick').addEventListener('keydown', (ev) => { if (ev.key === 'Enter') $('#jumpBtn').click(); });

  ['chkMid','chkBid','chkAsk','chkMacro','chkEvents','chkPreds','chkOutcomes'].forEach(id => {
    const el = $('#' + id);
    if (el) el.addEventListener('change', applyLegendFromToggles);
  });

  $('#runBtn').addEventListener('click', async () => {
    const btn = $('#runBtn');
    btn.disabled = true; btn.textContent = 'Running…';
    setStatus('Working…'); log('Run: start');
    try {
      const res = await wfStep();
      if (res?.journal) res.journal.forEach(line => log(line));
      if (res?.ok === false) {
        setStatus('Error: ' + (res.error || 'unknown'), 'err');
        log('Run error: ' + (res.error || 'unknown'));
      } else {
        setStatus(res?.message || 'Working', 'ok');
        log('Run: done');
      }
      const s = parseInt($('#startTick').value || '1', 10);
      const e = parseInt($('#endTick').value || (s + 12000), 10);
      await renderWindow(s, e);
      applyLegendFromToggles();
    } catch (err) {
      setStatus('Run failed: ' + (err.message || err), 'err');
      log('Run failed: ' + err);
    } finally {
      btn.disabled = false; btn.textContent = 'Run';
    }
  });

  // initial hint
  setStatus('Ready — use Jump or Load to fetch a window.');
})();
