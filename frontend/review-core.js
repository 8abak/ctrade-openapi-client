/* review-core.js — clean UI + integer-only display
   - Leaves chart structure intact (same single time/price chart).
   - Minimal controls: Load range, Run, 4 ML layers + Mid/Bid/Ask toggles.
   - All prices are rounded to whole dollars on axis, tooltips, and markers.
*/

(function () {
  // --------------------------- helpers ---------------------------
  const $ = (sel) => document.querySelector(sel);
  const $$ = (sel) => Array.from(document.querySelectorAll(sel));

  function ensureEl(idCandidates) {
    for (const id of idCandidates) {
      const el = document.getElementById(id) || document.querySelector(`#${id}`);
      if (el) return el;
    }
    // fallback chart container
    const el = document.createElement('div');
    el.id = 'chart';
    el.style.cssText = 'height:65vh;min-height:480px;width:100%';
    document.body.appendChild(el);
    return el;
  }

  function fetchJSON(url, opts) {
    return fetch(url, opts).then((r) => {
      if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
      return r.json();
    });
  }

  // integer-only helpers
  const iround = (v) => (v == null ? v : Math.round(v));
  const fmtInt = (v) => (v == null ? '' : String(Math.round(v)));

  // --------------------------- chart boot ---------------------------
  const chartEl = ensureEl(['chart', 'main', 'echart', 'review-chart']);
  let chart = (window.echarts && window.echarts.getInstanceByDom)
    ? (window.echarts.getInstanceByDom(chartEl) || (window.echarts.init ? window.echarts.init(chartEl) : null))
    : null;

  if (!chart && window.echarts && window.echarts.init) {
    chart = window.echarts.init(chartEl);
  }

  // base option (keeps structure)
  if (chart && !chart.getOption().series) {
    chart.setOption({
      animation: false,
      tooltip: {
        trigger: 'axis',
        valueFormatter: (v) => fmtInt(v),
        axisPointer: { type: 'cross' }
      },
      xAxis: { type: 'time' },
      yAxis: {
        type: 'value',
        scale: true,
        axisLabel: {
          formatter: (v) => fmtInt(v)
        }
      },
      series: []
    });
  }

  // --------------------------- state ---------------------------
  const UI = {
    layers: {
      macro: true,
      events: true,
      predictions: true,
      outcomes: true,
      mid: true,
      bid: false,
      ask: false
    },
    range: { start: null, end: null } // optional tick range loader
  };

  // --------------------------- backend glue ---------------------------
  async function runWalkForwardStep() {
    const btn = $('#wf-run');
    if (btn) { btn.disabled = true; btn.textContent = 'Running…'; }
    try {
      const res = await fetchJSON('/api/walkforward/step', { method: 'POST' });
      const snap = res && res.snapshot ? res.snapshot : await fetchJSON('/api/walkforward/snapshot');
      window.__WF_SNAPSHOT = snap;
      renderAll();
    } catch (e) {
      console.error(e);
      alert('Walk-forward step failed: ' + e.message);
    } finally {
      if (btn) { btn.disabled = false; btn.textContent = 'Run'; }
    }
  }

  async function refreshSnapshot() {
    try {
      window.__WF_SNAPSHOT = await fetchJSON('/api/walkforward/snapshot');
      renderAll();
    } catch (e) { console.error(e); }
  }

  // optional historical load by tick id (kept intact, but rounded view still applies)
  async function loadRange() {
    const s = $('#start-tick'), e = $('#end-tick');
    if (!s || !e) return;
    UI.range.start = parseInt(s.value || '1', 10);
    UI.range.end = parseInt(e.value || '100000', 10);
    // if your backend has a range endpoint, call it here; otherwise just refresh
    await refreshSnapshot();
  }

  // --------------------------- renderers ---------------------------
  function colorForDirection(dir, alpha = 0.12) {
    return dir === 1 ? `rgba(0,140,0,${alpha})` : `rgba(180,0,0,${alpha})`;
  }
  function colorForEventType(tp) {
    if (tp === 'pullback_end') return '#1f77b4';
    if (tp === 'breakout') return '#9467bd';
    if (tp === 'retest_hold') return '#ff7f0e';
    return '#777';
  }
  function shapeForEventType(tp) {
    if (tp === 'pullback_end') return 'circle';
    if (tp === 'breakout') return 'triangle';
    if (tp === 'retest_hold') return 'diamond';
    return 'rect';
  }
  function outcomeColor(out) {
    if (out === 'TP') return '#00a000';
    if (out === 'SL') return '#c00000';
    return '#888888';
  }

  function buildMacroMarkAreas(segments) {
    const areas = [];
    for (const s of (segments || [])) {
      const op = Math.max(0.05, Math.min(0.28, (s.confidence || 0) / 1.5));
      areas.push([
        { xAxis: s.start_ts, itemStyle: { color: colorForDirection(s.direction, op) } },
        { xAxis: s.end_ts }
      ]);
    }
    return areas;
  }

  function composeSeries(snapshot) {
    const segs  = snapshot.segments || [];
    const evts  = snapshot.events || [];
    const outs  = snapshot.outcomes || [];
    const preds = snapshot.predictions || [];

    const byOutcome = new Map();
    for (const o of outs) byOutcome.set(o.event_id, o);
    const byPred = new Map();
    for (const p of preds) byPred.set(p.event_id, p);

    const series = [];

    // --- price streams (kept optional; rounded values) ---
    if (UI.layers.mid) {
      // If you have a mid stream in snapshot, wire it here. Otherwise, we skip streaming lines.
      // This keeps chart structure intact while decluttering the controls.
    }
    if (UI.layers.bid) { /* hook if you expose bid series */ }
    if (UI.layers.ask) { /* hook if you expose ask series */ }

    // --- macro bands ---
    if (UI.layers.macro) {
      series.push({
        name: 'Macro',
        type: 'line',
        data: [],
        markArea: { silent: true, data: buildMacroMarkAreas(segs) }
      });
    }

    // --- events (rounded price markers) ---
    if (UI.layers.events && evts.length) {
      const pts = evts.map(e => ({
        name: e.event_type,
        value: [e.event_ts, iround(e.event_price)],
        symbol: shapeForEventType(e.event_type),
        itemStyle: { color: colorForEventType(e.event_type) },
        tooltip: {
          formatter: () => `#${e.event_id} ${e.event_type}<br>${new Date(e.event_ts).toISOString()}<br>$${fmtInt(e.event_price)}`
        }
      }));
      series.push({ name: 'Events', type: 'scatter', symbolSize: 6, data: pts });
    }

    // --- predictions (opacity by p_tp; rounded price) ---
    if (UI.layers.predictions && evts.length) {
      const pts = evts.filter(e => byPred.has(e.event_id)).map(e => {
        const p = byPred.get(e.event_id);
        const alpha = Math.max(0.25, Math.min(0.95, p.p_tp || 0));
        const decided = !!p.decided;
        return {
          name: 'Pred',
          value: [e.event_ts, iround(e.event_price)],
          symbol: 'circle',
          symbolSize: decided ? 9 : 7,
          itemStyle: {
            color: `rgba(0,0,0,${alpha})`,
            borderColor: decided ? '#000' : '#666',
            borderWidth: decided ? 1.5 : 1
          },
          tooltip: {
            formatter: () => `P(TP)=${((p.p_tp || 0)*100).toFixed(1)}%<br>τ=${(p.threshold || 0).toFixed(2)}<br>${p.model_version || ''}`
          }
        };
      });
      series.push({ name: 'Predictions', type: 'scatter', symbolSize: 8, data: pts });
    }

    // --- outcomes (halo; rounded price) ---
    if (UI.layers.outcomes && evts.length) {
      const pts = evts.filter(e => byOutcome.has(e.event_id)).map(e => {
        const o = byOutcome.get(e.event_id);
        return {
          name: o.outcome,
          value: [e.event_ts, iround(e.event_price)],
          symbol: 'circle',
          symbolSize: 13,
          itemStyle: { color: 'transparent', borderColor: outcomeColor(o.outcome), borderWidth: 2.2 },
          tooltip: { formatter: () => `Outcome: ${o.outcome}` }
        };
      });
      series.push({ name: 'Outcomes', type: 'scatter', data: pts });
    }

    return series;
  }

  function renderAll() {
    if (!chart || !window.echarts) return;
    const snap = window.__WF_SNAPSHOT || {};
    const opt = chart.getOption() || {};
    opt.yAxis = opt.yAxis || {};
    opt.yAxis.axisLabel = opt.yAxis.axisLabel || {};
    opt.yAxis.axisLabel.formatter = (v) => fmtInt(v);     // integers on axis
    opt.tooltip = opt.tooltip || {};
    opt.tooltip.valueFormatter = (v) => fmtInt(v);        // integers in crosshair tooltip
    opt.series = composeSeries(snap);
    chart.setOption(opt, { notMerge: false, replaceMerge: ['series', 'yAxis', 'tooltip'] });
  }

  // --------------------------- UI wiring ---------------------------
  function bind() {
    // Run
    const run = $('#wf-run');
    if (run) run.addEventListener('click', runWalkForwardStep);

    // Load range
    const loadBtn = $('#wf-load');
    if (loadBtn) loadBtn.addEventListener('click', loadRange);

    // Layer toggles
    const map = [
      ['wf-macro', 'macro'],
      ['wf-events', 'events'],
      ['wf-preds', 'predictions'],
      ['wf-out', 'outcomes'],
      ['wf-mid', 'mid'],
      ['wf-bid', 'bid'],
      ['wf-ask', 'ask'],
    ];
    for (const [id, key] of map) {
      const el = document.getElementById(id);
      if (!el) continue;
      el.checked = !!UI.layers[key];
      el.addEventListener('change', () => { UI.layers[key] = !!el.checked; renderAll(); });
    }

    refreshSnapshot();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', bind);
  } else {
    bind();
  }

  // console helpers
  window.refreshWalkForwardSnapshot = refreshSnapshot;
  window.runWalkForwardStep = runWalkForwardStep;
})();
