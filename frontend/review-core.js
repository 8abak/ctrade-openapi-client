/* frontend/review-core.js
 * Walk-forward UI glue for review.html.
 * - Adds a Run button handler
 * - Fetches /api/walkforward/snapshot
 * - Renders layers: Macro bands, Micro events, Predictions, Outcomes
 * - Non-destructive: if a chart exists, we reuse it; else we init one.
 */

(function () {
  // -------- DOM helpers --------
  function $(sel) { return document.querySelector(sel); }
  function EnsureEl(idCandidates) {
    for (const id of idCandidates) {
      const el = document.getElementById(id) || document.querySelector(`#${id}`);
      if (el) return el;
    }
    // fallback: create and append to body minimally
    const el = document.createElement('div');
    el.id = 'chart';
    el.style.cssText = 'height:65vh;min-height:480px;width:100%;';
    document.body.appendChild(el);
    return el;
  }

  // -------- Chart bootstrap --------
  const chartEl = EnsureEl(['chart','main','echart','review-chart']);
  let chart = (window.echarts && window.echarts.getInstanceByDom)
    ? (window.echarts.getInstanceByDom(chartEl) || (window.echarts.init ? window.echarts.init(chartEl) : null))
    : null;

  if (!chart && window.echarts && window.echarts.init) {
    chart = window.echarts.init(chartEl);
  }

  // Base option if empty
  if (chart && !chart.getOption().series) {
    chart.setOption({
      animation: false,
      tooltip: { trigger: 'axis' },
      xAxis: { type: 'time' },
      yAxis: { type: 'value', scale: true },
      series: []
    });
  }

  // -------- UI toggles --------
  const Layers = {
    macro: true,
    events: true,
    predictions: true,
    outcomes: true
  };

  function BindToggles() {
    const pairs = [
      ['wf-macro', 'macro'],
      ['wf-events', 'events'],
      ['wf-preds',  'predictions'],
      ['wf-out',    'outcomes'],
    ];
    for (const [id, key] of pairs) {
      const el = document.getElementById(id);
      if (el) {
        el.checked = Layers[key];
        el.addEventListener('change', () => { Layers[key] = !!el.checked; RenderLayers(window.__WF_SNAPSHOT || {}); });
      }
    }
  }

  // -------- Fetch helpers --------
  async function FetchJSON(url, opts) {
    const r = await fetch(url, opts);
    if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
    return r.json();
  }

  async function RunWalkForwardStep() {
    const btn = $('#wf-run');
    if (btn) { btn.disabled = true; btn.textContent = 'Running…'; }
    try {
      const res = await FetchJSON('/api/walkforward/step', { method: 'POST' });
      if (res && res.snapshot) {
        window.__WF_SNAPSHOT = res.snapshot;
      } else {
        window.__WF_SNAPSHOT = await FetchJSON('/api/walkforward/snapshot');
      }
      RenderLayers(window.__WF_SNAPSHOT);
    } catch (e) {
      console.error(e);
      alert('Walk-forward step failed: ' + e.message);
    } finally {
      if (btn) { btn.disabled = false; btn.textContent = 'Run'; }
    }
  }

  async function RefreshSnapshot() {
    try {
      window.__WF_SNAPSHOT = await FetchJSON('/api/walkforward/snapshot');
      RenderLayers(window.__WF_SNAPSHOT);
    } catch (e) {
      console.error(e);
    }
  }

  // -------- Renderers --------
  function ColorForDirection(dir, alpha=0.12) {
    return dir === 1 ? `rgba(0, 140, 0, ${alpha})` : `rgba(180, 0, 0, ${alpha})`;
  }
  function ColorForEventType(tp) {
    if (tp === 'pullback_end') return '#1f77b4';
    if (tp === 'breakout')     return '#9467bd';
    if (tp === 'retest_hold')  return '#ff7f0e';
    return '#777';
  }
  function ShapeForEventType(tp) {
    if (tp === 'pullback_end') return 'circle';
    if (tp === 'breakout')     return 'triangle';
    if (tp === 'retest_hold')  return 'diamond';
    return 'rect';
  }
  function OutcomeColor(out) {
    if (out === 'TP') return '#00a000';
    if (out === 'SL') return '#c00000';
    return '#888888';
  }

  function BuildMacroMarkAreas(segments) {
    // ECharts markArea requires data: [[{xAxis:.., yAxis:..},{xAxis:.., yAxis:..}], ...]
    const areas = [];
    for (const s of (segments||[])) {
      areas.push([
        { xAxis: s.start_ts, itemStyle: { color: ColorForDirection(s.direction, Math.max(0.05, Math.min(0.28, (s.confidence||0)/1.5))) } },
        { xAxis: s.end_ts }
      ]);
    }
    return areas;
  }

  function ComposeSeries(snapshot) {
    const segs  = snapshot.segments || [];
    const evts  = snapshot.events || [];
    const outs  = snapshot.outcomes || [];
    const preds = snapshot.predictions || [];

    // Map outcomes, predictions by event_id
    const byOutcome = new Map();
    for (const o of outs) byOutcome.set(o.event_id, o);
    const byPred = new Map();
    for (const p of preds) byPred.set(p.event_id, p);

    const series = [];

    // Macro bands via markArea on a dummy series
    if (Layers.macro) {
      series.push({
        name: 'Macro',
        type: 'line',
        data: [],
        markArea: {
          silent: true,
          data: BuildMacroMarkAreas(segs)
        }
      });
    }

    // Micro events
    if (Layers.events) {
      const pts = evts.map(e => {
        return {
          name: e.event_type,
          value: [e.event_ts, e.event_price],
          symbol: ShapeForEventType(e.event_type),
          itemStyle: { color: ColorForEventType(e.event_type) },
          event_id: e.event_id,
          tooltip: {
            formatter: () => {
              return `#${e.event_id} ${e.event_type}<br/>${new Date(e.event_ts).toISOString()}<br/>$${(e.event_price||0).toFixed(2)}`;
            }
          }
        };
      });
      series.push({
        name: 'Events',
        type: 'scatter',
        symbolSize: 6,
        data: pts
      });
    }

    // Predictions (overlay)
    if (Layers.predictions) {
      const pts = evts.filter(e => byPred.has(e.event_id)).map(e => {
        const p = byPred.get(e.event_id);
        const c = p.p_tp; // 0..1
        const alpha = Math.max(0.25, Math.min(0.95, c));
        const decided = !!p.decided;
        return {
          name: 'pred',
          value: [e.event_ts, e.event_price],
          symbol: 'circle',
          symbolSize: decided ? 9 : 7,
          itemStyle: { color: `rgba(0,0,0,${alpha})`, borderColor: decided ? '#000' : '#666', borderWidth: decided ? 1.5 : 1 },
          tooltip: {
            formatter: () => {
              return [
                `P(TP)=${(c*100).toFixed(1)}%`,
                `τ=${(p.threshold||0).toFixed(2)}`,
                `model=${p.model_version}`,
              ].join('<br/>');
            }
          }
        };
      });
      series.push({
        name: 'Predictions',
        type: 'scatter',
        symbolSize: 8,
        data: pts
      });
    }

    // Outcomes (ring/halo)
    if (Layers.outcomes) {
      const pts = evts.filter(e => byOutcome.has(e.event_id)).map(e => {
        const o = byOutcome.get(e.event_id);
        const col = OutcomeColor(o.outcome);
        return {
          name: o.outcome,
          value: [e.event_ts, e.event_price],
          symbol: 'circle',
          symbolSize: 13,
          itemStyle: { color: 'transparent', borderColor: col, borderWidth: 2.2 },
          tooltip: { formatter: () => `Outcome: ${o.outcome}` }
        };
      });
      series.push({
        name: 'Outcomes',
        type: 'scatter',
        data: pts
      });
    }

    return series;
  }

  function RenderLayers(snapshot) {
    if (!chart || !window.echarts) return;
    const opt = chart.getOption() || {};
    opt.series = ComposeSeries(snapshot);
    chart.setOption(opt, { notMerge: false, replaceMerge: ['series'] });
  }

  // -------- Wire up controls --------
  function Wire() {
    const btn = $('#wf-run');
    if (btn) btn.addEventListener('click', RunWalkForwardStep);
    BindToggles();
    RefreshSnapshot();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', Wire);
  } else {
    Wire();
  }

  // Expose for console debugging
  window.runWalkForwardStep = RunWalkForwardStep;
  window.refreshWalkForwardSnapshot = RefreshSnapshot;
})();
