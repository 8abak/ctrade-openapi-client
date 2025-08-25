// frontend/review-core.js — Walk-Forward UI (ECharts intraday-style)
// - Exact float plotting; y-axis labels/grid at integers
// - Auto y min/max for visible window
// - Drag to zoom: X (left/right), Y (up/down); wheel zoom too
// - Dark, snappy, lttb sampling & progressive rendering
// - Collapsible journal; rich tooltip (tick id, UTC date, UTC time, Mid/Bid/Ask, Events/Preds/Outcomes)

(function () {
  window.__reviewBoot = 'ok';

  const $ = (sel) => document.querySelector(sel);

  // display helpers
  const asInt = (v) => (v == null ? '' : String(Math.round(Number(v))));
  const fmt2  = (v) => (v == null ? '-' : Number(v).toFixed(2));

  // Jump window (±)
  const JUMP_WINDOW = 6000;

  // current window state
  let currentTicks = [];       // [{id, timestamp, bid, ask, mid}]
  let currentSnap  = null;     // {segments, events, predictions, outcomes}
  let evByTick = new Map();    // tick_id -> [events]
  let prByTick = new Map();    // tick_id -> [preds joined]
  let ocByTick = new Map();    // tick_id -> [outcomes joined]
  let chart;

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

  // ---- Collapsible journal ----
  (function initJournalToggle(){
    const btn = $('#toggleJournal');
    const j = $('#journal');
    const key = 'reviewJournalOpen';
    function apply(open){
      if (open) { j.classList.remove('collapsed'); btn.textContent = 'Journal ▾'; }
      else      { j.classList.add('collapsed');    btn.textContent = 'Journal ▸'; }
      localStorage.setItem(key, open ? '1' : '0');
    }
    const saved = localStorage.getItem(key);
    apply(saved === '1');
    btn.addEventListener('click', () => apply(j.classList.contains('collapsed')));
  })();

  // ----- data access -----
  async function fetchTicksRange(startId, endId) {
    const sql = `SELECT id, timestamp, bid, ask, mid FROM ticks WHERE id BETWEEN ${startId} AND ${endId} ORDER BY id`;
    const url = `/sqlvw/query?query=${encodeURIComponent(sql)}`;
    const res = await fetch(url);
    if (!res.ok) throw new Error(`ticks range: HTTP ${res.status}`);
    return await res.json();
  }

  async function wfStep() {
    const r = await fetch('/walkforward/step', { method: 'POST' });
    if (!r.ok) throw new Error(`step: HTTP ${r.status}`);
    return r.json();
  }
  async function wfSnap() {
    const r = await fetch('/walkforward/snapshot');
    if (!r.ok) throw new Error(`snapshot: HTTP ${r.status}`);
    return r.json();
  }

  // ----- transforms -----
  const xyFloat = (rows, xKey, yKey) => rows.map(r => [r[xKey], Number(r[yKey])]);

  function ensureChart() {
    if (chart) return chart;
    if (typeof echarts === 'undefined') { setStatus('ECharts not loaded', 'err'); return null; }
    chart = echarts.init($('#chart'), null, { renderer: 'canvas' });
    return chart;
  }

  function buildOption(ticks, snap, startId, endId) {
    // group snap by tick id for tooltip enrichment
    evByTick.clear(); prByTick.clear(); ocByTick.clear();
    const ev = (snap?.events || []);
    for (const e of ev) {
      if (!evByTick.has(e.tick_id)) evByTick.set(e.tick_id, []);
      evByTick.get(e.tick_id).push(e);
    }
    const pr = (snap?.predictions || []);
    const evById = new Map(ev.map(e => [e.event_id, e]));
    for (const p of pr) {
      const e = evById.get(p.event_id);
      if (!e) continue;
      const t = e.tick_id;
      if (!prByTick.has(t)) prByTick.set(t, []);
      prByTick.get(t).push({ ...p, event: e });
    }
    const oc = (snap?.outcomes || []);
    for (const o of oc) {
      const e = evById.get(o.event_id);
      if (!e) continue;
      const t = e.tick_id;
      if (!ocByTick.has(t)) ocByTick.set(t, []);
      ocByTick.get(t).push({ ...o, event: e });
    }

    // price series (exact floats)
    const mid = xyFloat(ticks, 'id', 'mid');
    const bid = xyFloat(ticks, 'id', 'bid');
    const ask = xyFloat(ticks, 'id', 'ask');

    const base = {
      useUTC: true,                           // match the example feel
      backgroundColor: '#0d1117',
      animation: false,
      progressive: 4000,
      progressiveThreshold: 3000,
      textStyle: { color: '#c9d1d9' },
      color: ['#7aa6ff', '#7ad3ff', '#ffd37a', '#65cc9a', '#f27370', '#b981f5'], // pleasant dark palette
      legend: { top: 6, textStyle: { color: '#aeb9cc' }, selectedMode: 'multiple' },
      grid: { left: 48, right: 20, top: 32, bottom: 64 },
      tooltip: {
        show: true,
        trigger: 'axis',
        axisPointer: { type: 'cross', snap: true },
        backgroundColor: '#101826',
        borderColor: '#26314a',
        textStyle: { color: '#dce6f2' },
        formatter: (params) => {
          const tickId = Math.round(params[0].axisValue);
          const row = currentTicks.find(r => r.id === tickId);
          const lines = [];
          lines.push(`<b>tick:</b> ${tickId}`);
          if (row?.timestamp) {
            const ts = new Date(row.timestamp);
            const yyyy = ts.getUTCFullYear();
            const mm = String(ts.getUTCMonth()+1).padStart(2,'0');
            const dd = String(ts.getUTCDate()).padStart(2,'0');
            const hh = String(ts.getUTCHours()).padStart(2,'0');
            const mi = String(ts.getUTCMinutes()).padStart(2,'0');
            const ss = String(ts.getUTCSeconds()).padStart(2,'0');
            lines.push(`<b>date:</b> ${yyyy}-${mm}-${dd}  <b>time (UTC):</b> ${hh}:${mi}:${ss}`);
          }
          if (row) {
            lines.push(`<b>Mid:</b> ${fmt2(row.mid)}  <b>Bid:</b> ${fmt2(row.bid)}  <b>Ask:</b> ${fmt2(row.ask)}`);
          }
          const evs = evByTick.get(tickId);
          if (evs?.length) for (const e of evs) lines.push(`• <b>Event</b> ${e.event_type}`);
          const prs = prByTick.get(tickId);
          if (prs?.length) for (const p of prs) lines.push(`• <b>Pred</b> p_tp=${(p.p_tp ?? 0).toFixed(3)} τ=${(p.threshold ?? 0).toFixed(3)} [${p.model_version || '-'}]`);
          const ocs = ocByTick.get(tickId);
          if (ocs?.length) for (const o of ocs) lines.push(`• <b>Outcome</b> ${o.outcome}`);
          return lines.join('<br/>');
        }
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
        min: 'dataMin',
        max: 'dataMax',
        minInterval: 1,                                   // integer grid/labels
        axisLabel: { color: '#8b949e', formatter: (v) => asInt(v) },
        axisLine:  { lineStyle: { color: '#30363d' } },
        splitLine: { show: true, lineStyle: { color: '#21262d' } }
      },
      dataZoom: [
        // X (drag and wheel zoom)
        { type: 'inside', xAxisIndex: 0, filterMode: 'none',
          moveOnMouseMove: false, moveOnMouseWheel: false,
          zoomOnMouseMove: true,  zoomOnMouseWheel: true, throttle: 24 },
        // Y (drag and wheel zoom)
        { type: 'inside', yAxisIndex: 0, filterMode: 'none',
          moveOnMouseMove: false, moveOnMouseWheel: false,
          zoomOnMouseMove: true,  zoomOnMouseWheel: true, throttle: 24 },
        // X slider
        { type: 'slider', xAxisIndex: 0, height: 18, bottom: 26, backgroundColor: '#0f1524', borderColor: '#2a3654' }
      ],
      series: [
        { name: 'Mid', type: 'line', smooth: 0.15, showSymbol: false, sampling: 'lttb', large: true, largeThreshold: 10000, lineStyle: { width: 1.3 }, data: mid },
        { name: 'Bid', type: 'line', smooth: 0.15, showSymbol: false, sampling: 'lttb', large: true, largeThreshold: 10000, lineStyle: { width: 1.0, opacity: 0.7 }, data: bid },
        { name: 'Ask', type: 'line', smooth: 0.15, showSymbol: false, sampling: 'lttb', large: true, largeThreshold: 10000, lineStyle: { width: 1.0, opacity: 0.7 }, data: ask }
      ]
    };

    // macro markArea
    const segs = (snap?.segments || []).filter(s => s.end_tick_id >= startId && s.start_tick_id <= endId);
    if (segs.length) {
      const data = segs.map(s => {
        const dir = s.direction > 0 ? 1 : -1;
        const color = dir > 0 ? 'rgba(0,160,100,' + (0.10 + 0.20 * (s.confidence ?? 0.5)) + ')'
                              : 'rgba(200,50,60,' + (0.10 + 0.20 * (s.confidence ?? 0.5)) + ')';
        return [{
          xAxis: Math.max(s.start_tick_id, startId), yAxis: 'min', itemStyle: { color }
        }, {
          xAxis: Math.min(s.end_tick_id, endId),     yAxis: 'max'
        }];
      });
      base.series.push({ name: 'Macro', type: 'line', data: [], markArea: { silent: true, itemStyle: { opacity: 1 }, data } });
    }

    // events
    const evsWin = (snap?.events || []).filter(e => e.tick_id >= startId && e.tick_id <= endId);
    if (evsWin.length) {
      const mapSymbol = (t) => t === 'pullback_end' ? 'triangle' : t === 'breakout' ? 'diamond' : 'circle';
      const mapColor  = (t) => t === 'pullback_end' ? '#58a6ff' : t === 'breakout' ? '#f2cc60' : '#b981f5';
      base.series.push({
        name: 'Events', type: 'scatter', symbolSize: 9,
        data: evsWin.map(e => ({
          value: [e.tick_id, Number(e.event_price)],
          name: e.event_type,
          symbol: mapSymbol(e.event_type),
          itemStyle: { color: mapColor(e.event_type) }
        }))
      });
    }

    // predictions
    if ((snap?.predictions || []).length && evsWin.length) {
      const latest = snap.predictions;
      const rows = [];
      const evByIdWin = new Map(evsWin.map(e => [e.event_id, e]));
      for (const p of latest) {
        const e = evByIdWin.get(p.event_id); if (!e) continue;
        rows.push({
          value: [e.tick_id, Number(e.event_price)],
          p_tp: p.p_tp ?? null, threshold: p.threshold ?? null,
          model_version: p.model_version ?? '', decided: !!p.decided, predicted_at: p.predicted_at
        });
      }
      if (rows.length) {
        base.series.push({
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
          data: rows
        });
      }
    }

    // outcomes
    if ((snap?.outcomes || []).length && evsWin.length) {
      const rows = [];
      const evByIdWin = new Map(evsWin.map(e => [e.event_id, e]));
      for (const o of snap.outcomes) {
        const e = evByIdWin.get(o.event_id); if (!e) continue;
        const col = o.outcome === 'TP' ? '#2ea043' : o.outcome === 'SL' ? '#f85149' : '#8b949e';
        rows.push({ value: [e.tick_id, Number(e.event_price)], itemStyle: { color: col, borderColor: col } });
      }
      if (rows.length) {
        base.series.push({
          name: 'Outcomes',
          type: 'effectScatter',
          rippleEffect: { brushType: 'stroke', scale: 2.2 },
          symbolSize: 11,
          showEffectOn: 'render',
          data: rows
        });
      }
    }

    // legend states from toggles
    const sel = {};
    sel['Mid'] = $('#chkMid').checked; sel['Bid'] = $('#chkBid').checked; sel['Ask'] = $('#chkAsk').checked;
    sel['Macro'] = $('#chkMacro').checked; sel['Events'] = $('#chkEvents').checked;
    sel['Predictions'] = $('#chkPreds').checked; sel['Outcomes'] = $('#chkOutcomes').checked;
    base.legend.selected = sel;

    return base;
  }

  function applyLegendFromToggles() {
    const map = { chkMid:'Mid', chkBid:'Bid', chkAsk:'Ask', chkMacro:'Macro', chkEvents:'Events', chkPreds:'Predictions', chkOutcomes:'Outcomes' };
    for (const id in map) {
      const el = $('#' + id); if (!el) continue;
      const name = map[id];
      chart?.dispatchAction({ type: el.checked ? 'legendSelect' : 'legendUnSelect', name });
    }
  }

  async function renderWindow(startId, endId) {
    const c = ensureChart(); if (!c) return;
    setStatus('Loading data…'); log(`Load window [${startId}, ${endId}]`);

    currentSnap = await wfSnap().catch(() => ({segments:[],events:[],predictions:[],outcomes:[]}));
    const nSeg = currentSnap?.segments?.length || 0;
    const nEv  = currentSnap?.events?.length || 0;
    const nPr  = currentSnap?.predictions?.length || 0;
    const nOc  = currentSnap?.outcomes?.length || 0;

    currentTicks = await fetchTicksRange(startId, endId);

    const opt = buildOption(currentTicks, currentSnap, startId, endId);
    c.setOption(opt, true);

    setStatus(`Loaded ${currentTicks.length} ticks · seg:${nSeg} ev:${nEv} pr:${nPr} oc:${nOc}`, 'ok');
  }

  // ----- UI wiring -----
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
