/* review-core.js — dark theme + visible Run workflow + integer-only axis
   - Dark UI enforced (no flashing white).
   - Run button shows progress, returns counts from backend, and refreshes chart.
   - If backend is busy/unreachable, shows a clear message (and logs details).
   - Y-axis, tooltips, and plotted prices are rounded to whole dollars.
*/

(function () {
  // --------------------------- helpers ---------------------------
  const $  = (sel) => document.querySelector(sel);
  const $$ = (sel) => Array.from(document.querySelectorAll(sel));

  // integer-only helpers
  const iround = (v) => (v == null ? v : Math.round(v));
  const fmtInt = (v) => (v == null ? '' : String(Math.round(v)));

  // hard-enforce dark page early, before chart init
  function enforceDarkTheme() {
    document.documentElement.style.colorScheme = 'dark';
    document.body.classList.add('wf-dark');
  }
  enforceDarkTheme();

  function ensureEl(idCandidates) {
    for (const id of idCandidates) {
      const el = document.getElementById(id) || document.querySelector(`#${id}`);
      if (el) return el;
    }
    const el = document.createElement('div');
    el.id = 'chart';
    el.style.cssText = 'height:65vh;min-height:480px;width:100%';
    document.body.appendChild(el);
    return el;
  }

  async function fetchJSON(url, opts = {}, timeoutMs = 45000) {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), timeoutMs);
    try {
      const r = await fetch(url, { ...opts, signal: ctrl.signal });
      if (!r.ok) {
        const txt = await r.text().catch(() => '');
        throw new Error(`${r.status} ${r.statusText} — ${txt}`);
      }
      return await r.json();
    } finally {
      clearTimeout(t);
    }
  }

  // status line
  function setStatus(html, kind = 'info') {
    const el = $('#wf-status');
    if (!el) return;
    el.innerHTML = html;
    el.dataset.kind = kind; // css color hook
  }

  // --------------------------- chart boot ---------------------------
  const chartEl = ensureEl(['chart', 'main', 'echart', 'review-chart']);
  chartEl.classList.add('wf-chart');
  let chart = (window.echarts && window.echarts.getInstanceByDom)
    ? (window.echarts.getInstanceByDom(chartEl) || (window.echarts.init ? window.echarts.init(chartEl) : null))
    : null;

  if (!chart && window.echarts && window.echarts.init) {
    chart = window.echarts.init(chartEl);
  }

  // base option (keep structure; integer-only)
  if (chart && !chart.getOption().series) {
    chart.setOption({
      backgroundColor: '#0d1117',
      animation: false,
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'cross' },
        valueFormatter: (v) => fmtInt(v),
        textStyle: { color: '#e6edf3' }
      },
      xAxis: {
        type: 'time',
        axisLine: { lineStyle: { color: '#30363d' } },
        axisLabel: { color: '#8b949e' },
        splitLine: { lineStyle: { color: '#21262d' } }
      },
      yAxis: {
        type: 'value',
        scale: true,
        minInterval: 1, // align grid to whole-dollars (safe; keeps scale=true)
        axisLine: { lineStyle: { color: '#30363d' } },
        axisLabel: { color: '#8b949e', formatter: (v) => fmtInt(v) },
        splitLine: { lineStyle: { color: '#21262d' } }
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
    }
  };

  // --------------------------- backend glue ---------------------------
  async function runWalkForwardStep() {
    const btn = $('#wf-run');
    if (btn) { btn.disabled = true; btn.textContent = 'Running…'; }
    setStatus('Working… starting step', 'info');

    try {
      // 1) kick the step
      const res = await fetchJSON('/api/walkforward/step', { method: 'POST' }, 120000);
      console.debug('[wf step]', res);

      // 2) summarize returned counts (helps you see if server actually did work)
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

      // 3) snapshot (returned inline or fetch fresh)
      const snap = res && res.snapshot ? res.snapshot : await fetchJSON('/api/walkforward/snapshot');
      window.__WF_SNAPSHOT = snap;

      renderAll();
    } catch (err) {
      console.error('[wf step] error', err);
      setStatus(`Run failed: ${err.message}`, 'err');
      // Try to at least show current snapshot so the screen isn’t blank
      try {
        window.__WF_SNAPSHOT = await fetchJSON('/api/walkforward/snapshot');
        renderAll();
      } catch {}
    } finally {
      if (btn) { btn.disabled = false; btn.textContent = 'Run'; }
    }
  }

  async function refreshSnapshot() {
    try {
      setStatus('Loading snapshot…', 'info');
      window.__WF_SNAPSHOT = await fetchJSON('/api/walkforward/snapshot');
      setStatus('Snapshot loaded', 'ok');
      renderAll();
    } catch (e) {
      console.error('[snapshot] error', e);
      setStatus('Cannot load snapshot. Is the backend up? ' + e.message, 'err');
    }
  }

  // --------------------------- renderers ---------------------------
  function colorForDirection(dir, alpha = 0.12) {
    return dir === 1 ? `rgba(0,140,0,${alpha})` : `rgba(180,0,0,${alpha})`;
  }
  function colorForEventType(tp) {
    if (tp === 'pullback_end') return '#58a6ff';
    if (tp === 'breakout')     return '#a371f7';
    if (tp === 'retest_hold')  return '#ffa657';
    return '#c9d1d9';
  }
  function shapeForEventType(tp) {
    if (tp === 'pullback_end') return 'circle';
    if (tp === 'breakout')     return 'triangle';
    if (tp === 'retest_hold')  return 'diamond';
    return 'rect';
  }
  function outcomeColor(out) {
    if (out === 'TP') return '#2ea043';
    if (out === 'SL') return '#f85149';
    return '#8b949e';
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

    // Macro bands
    if (UI.layers.macro) {
      series.push({
        name: 'Macro',
        type: 'line',
        data: [],
        markArea: { silent: true, data: buildMacroMarkAreas(segs) }
      });
    }

    // Events (rounded price markers)
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

    // Predictions
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
            color: `rgba(230,237,243,${alpha})`,
            borderColor: decided ? '#e6edf3' : '#8b949e',
            borderWidth: decided ? 1.5 : 1
          },
          tooltip: {
            formatter: () => `P(TP)=${((p.p_tp || 0)*100).toFixed(1)}%<br>τ=${(p.threshold || 0).toFixed(2)}<br>${p.model_version || ''}`
          }
        };
      });
      series.push({ name: 'Predictions', type: 'scatter', symbolSize: 8, data: pts });
    }

    // Outcomes (halo)
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

    // keep dark grid & integer labels each render
    opt.backgroundColor = '#0d1117';
    opt.yAxis = opt.yAxis || {};
    opt.yAxis.axisLabel = { color: '#8b949e', formatter: (v) => fmtInt(v) };
    opt.yAxis.minInterval = 1;
    opt.tooltip = opt.tooltip || {};
    opt.tooltip.valueFormatter = (v) => fmtInt(v);

    opt.series = composeSeries(snap);
    chart.setOption(opt, { notMerge: false, replaceMerge: ['series', 'yAxis', 'tooltip'] });
  }

  // --------------------------- UI wiring ---------------------------
  function bind() {
    // Run
    const run = $('#wf-run');
    if (run) run.addEventListener('click', runWalkForwardStep);

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

    // initial snapshot (also verifies backend connectivity)
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
