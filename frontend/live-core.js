(() => {
  // Tunables
  const INIT_WINDOW = 20000;   // total points to keep loaded (balanced)
  const VIEW_DEFAULT = 3000;   // default visible window when pinned-right

  const el = document.getElementById('chart');
  const chart = echarts.init(el, null, { renderer: 'canvas' });

  const COLORS = {
    ask: '#FF6B6B',
    mid_tick: '#FFD166',
    bid: '#4ECDC4',
    max_lbl: '#A78BFA',
    mid_lbl: '#60A5FA',
    min_lbl: '#34D399',
  };

  const state = {
    x: [], ts: [],
    ask: [], bid: [], mid_tick: [],
    max_lbl: [], mid_lbl: [], min_lbl: [],
    lastId: 0,
    fetching: false,
    viewSize: VIEW_DEFAULT,            // how many points to keep visible when pinned-right
    labelsLoadedUntil: { max: 0, mid: 0, min: 0 }, // last tick id fetched for each label
  };

  // ----- Chart option helpers -----
  function yAxisInt() {
    return {
      type: 'value',
      min: (ext) => Math.floor(ext.min),
      max: (ext) => Math.ceil(ext.max),
      interval: 1,
      minInterval: 1,
      axisLabel: { color: '#9ca3af', formatter: (v) => Number.isInteger(v) ? v : '' },
      axisLine: { lineStyle: { color: '#1f2937' } },
      splitLine: { show: true, lineStyle: { color: 'rgba(148,163,184,0.08)' } },
      scale: false
    };
  }
  function mkLine(name, key, color, connect = false) {
    return {
      name, type: 'line', showSymbol: false, smooth: false,
      data: state[key], itemStyle: { color },
      lineStyle: { color, width: 1.6 },
      connectNulls: connect,   // true for labels so sparse points connect across gaps
    };
  }
  function baseOption() {
    return {
      backgroundColor: '#0b0f14',
      animation: false,
      grid: { left: 42, right: 18, top: 10, bottom: 28 },
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'line' },
        backgroundColor: 'rgba(17,24,39,.95)',
        formatter: (params) => {
          if (!params || !params.length) return '';
          const i = params[0].dataIndex;
          const id = state.x[i];
          const ts = state.ts[i] || '';
          const dt = ts ? new Date(ts) : null;
          const d = dt ? `${dt.toLocaleDateString()} ${dt.toLocaleTimeString()}` : '(no time)';
          const out = [`<div><b>ID</b>: ${id}</div><div><b>Time</b>: ${d}</div><hr>`];
          for (const p of params) if (p.value != null) {
            out.push(
              `<div><span style="display:inline-block;width:8px;height:8px;background:${p.color};margin-right:6px;border-radius:2px"></span>${p.seriesName}: ${p.value}</div>`
            );
          }
          return out.join('');
        }
      },
      xAxis: {
        type: 'category', data: state.x,
        axisLabel: { color: '#9ca3af' },
        axisLine: { lineStyle: { color: '#1f2937' } },
        splitLine: { show: true, lineStyle: { color: 'rgba(148,163,184,0.08)' } },
      },
      yAxis: yAxisInt(),
      dataZoom: [{ type: 'inside' }, { type: 'slider', height: 16, bottom: 4 }],
      series: [
        mkLine('Ask','ask',COLORS.ask,false),
        mkLine('Mid (ticks)','mid_tick',COLORS.mid_tick,false),
        mkLine('Bid','bid',COLORS.bid,false),
        mkLine('Max (labels)','max_lbl',COLORS.max_lbl,true),
        mkLine('Mid (labels)','mid_lbl',COLORS.mid_lbl,true),
        mkLine('Min (labels)','min_lbl',COLORS.min_lbl,true),
      ]
    };
  }

  chart.setOption(baseOption(), { notMerge: true, lazyUpdate: true });

  // Remember window size whenever user changes zoom while pinned-right
  chart.on('dataZoom', () => {
    if (!pinnedRight()) return;
    const [start, end] = currentIndexWindow();
    if (start != null && end != null) state.viewSize = Math.max(1, end - start + 1);
  });

  // ----- Utility -----
  async function fetchJSON(url) { const r = await fetch(url); if (!r.ok) throw new Error(await r.text()); return r.json(); }
  function setStatus(t) { const el = document.getElementById('status'); if (el) el.textContent = t; }

  function pinnedRight() {
    const dz = chart.getOption().dataZoom?.[0];
    if (!dz) return true;
    const end = dz.end ?? 100;
    // If using startValue/endValue, treat end at last index as pinned
    if (dz.endValue != null) {
      const last = state.x.length ? state.x.length - 1 : 0;
      return dz.endValue >= last;
    }
    return end > 99.5;
  }
  function currentIndexWindow() {
    const dz = chart.getOption().dataZoom?.[0];
    if (!dz) return [null, null];
    if (dz.startValue != null && dz.endValue != null) return [dz.startValue, dz.endValue];
    // percent-based → approximate to indices
    const len = state.x.length;
    const s = Math.floor(((dz.start ?? 0) / 100) * (len - 1));
    const e = Math.floor(((dz.end ?? 100) / 100) * (len - 1));
    return [s, e];
  }
  function keepRightWithView() {
    const n = Math.max(1, state.viewSize || VIEW_DEFAULT);
    const len = state.x.length;
    const endVal = Math.max(0, len - 1);
    const startVal = Math.max(0, endVal - n + 1);
    chart.dispatchAction({ type: 'dataZoom', startValue: startVal, endValue: endVal });
  }

  // ----- Append & overlay -----
  function appendTicks(rows) {
    if (!rows || !rows.length) return 0;
    const atRight = pinnedRight();

    for (const r of rows) {
      state.x.push(r.id);
      state.ts.push(r.ts || null);
      state.mid_tick.push(r.mid != null ? +r.mid : null);
      state.ask.push(r.ask != null ? +r.ask : null);
      state.bid.push(r.bid != null ? +r.bid : null);
      state.max_lbl.push(null);
      state.mid_lbl.push(null);
      state.min_lbl.push(null);
      state.lastId = r.id;
    }

    // Enforce retention cap (INIT_WINDOW) to avoid unbounded growth
    if (state.x.length > INIT_WINDOW) {
      const cut = state.x.length - INIT_WINDOW;
      for (const k of ['x','ts','ask','bid','mid_tick','max_lbl','mid_lbl','min_lbl']) state[k].splice(0, cut);
    }

    chart.setOption({
      xAxis: { data: state.x },
      yAxis: yAxisInt(),
      series: [
        { name: 'Ask', data: state.ask },
        { name: 'Mid (ticks)', data: state.mid_tick },
        { name: 'Bid', data: state.bid },
        { name: 'Max (labels)', data: state.max_lbl },
        { name: 'Mid (labels)', data: state.mid_lbl },
        { name: 'Min (labels)', data: state.min_lbl },
      ]
    }, { lazyUpdate: true });

    if (atRight) keepRightWithView();
    return rows.length;
  }

  // Accept label rows with id|tick_id|start_id and value|price|mid|start_price
  function overlayLabels(rows, key) {
    if (!rows || !rows.length) return;
    const map = new Map(state.x.map((id, i) => [id, i]));
    const arr = state[key];
    for (const r of rows) {
      const id = r.id ?? r.tick_id ?? r.start_id;
      const val = (r.value ?? r.price ?? r.mid ?? r.start_price);
      if (id == null || val == null) continue;
      const i = map.get(id);
      if (i != null) arr[i] = +val;
    }
    chart.setOption({
      series: [
        { name: 'Max (labels)', data: state.max_lbl },
        { name: 'Mid (labels)', data: state.mid_lbl },
        { name: 'Min (labels)', data: state.min_lbl },
      ]
    }, { lazyUpdate: true });
  }

  async function fetchLabelsFrom(startId, limit) {
    // modest overfetch to tolerate sparsity
    const lim = Math.max(limit, 1000);
    const [mx, md, mn] = await Promise.allSettled([
      fetchJSON(`/api/labels/max/range?start_id=${startId}&limit=${lim}`),
      fetchJSON(`/api/labels/mid/range?start_id=${startId}&limit=${lim}`),
      fetchJSON(`/api/labels/min/range?start_id=${startId}&limit=${lim}`)
    ]);
    if (mx.status === 'fulfilled') overlayLabels(mx.value, 'max_lbl');
    if (md.status === 'fulfilled') overlayLabels(md.value, 'mid_lbl');
    if (mn.status === 'fulfilled') overlayLabels(mn.value, 'min_lbl');
    state.labelsLoadedUntil.max = Math.max(state.labelsLoadedUntil.max, state.lastId);
    state.labelsLoadedUntil.mid = Math.max(state.labelsLoadedUntil.mid, state.lastId);
    state.labelsLoadedUntil.min = Math.max(state.labelsLoadedUntil.min, state.lastId);
  }

  function setSeriesVisibility() {
    const boxes = document.querySelectorAll('input[type=checkbox][data-series]');
    const showMap = {};
    boxes.forEach(b => showMap[b.dataset.series] = b.checked);
    const opt = chart.getOption();
    opt.series.forEach(s => {
      const key = ({
        'Ask':'ask', 'Bid':'bid', 'Mid (ticks)':'mid_tick',
        'Max (labels)':'max_lbl', 'Mid (labels)':'mid_lbl', 'Min (labels)':'min_lbl'
      })[s.name];
      s.data = state[key];
      const vis = showMap[key];
      s.itemStyle.opacity = vis ? 1 : 0;
      s.lineStyle.opacity = vis ? 1 : 0;
    });
    chart.setOption(opt, { lazyUpdate:true, notMerge:true });
  }

  // ----- Loading flow -----
  async function fetchRange(startId, endId) {
    const size = Math.max(1, endId - startId + 1);
    const tryUrls = [
      `/api/ticks?from_id=${startId}&to_id=${endId}`,
      `/api/ticks/range?start_id=${startId}&limit=${size}`,
      `/api/ticks/latestN?limit=${size}`,
      `/api/ticks/after?since_id=${Math.max(0,startId-1)}&limit=${size}`,
    ];
    for (const u of tryUrls) {
      try { const rows = await fetchJSON(u); if (Array.isArray(rows) && rows.length) return rows; } catch { /* next */ }
    }
    return [];
  }

  async function loadInitial() {
    setStatus('loading window…');
    let latest = 0;
    try { const last = await fetchJSON('/api/ticks/latestN?limit=1'); latest = last?.[0]?.id || 0; } catch {}
    const start = Math.max(1, latest - INIT_WINDOW + 1);
    const rows = await fetchRange(start, latest || (start + INIT_WINDOW - 1));
    const added = appendTicks(rows);
    setStatus(`window ${added}`);

    if (added) {
      await fetchLabelsFrom(state.x[0], state.x.length);
      // Default view: show last VIEW_DEFAULT
      keepRightWithView();
      setSeriesVisibility(); // respect default toggles from HTML
    }
  }

  async function loadNew() {
    if (state.fetching) return;
    state.fetching = true;
    setStatus('fetching…');
    try {
      const rows = await fetchJSON(`/api/ticks/after?since_id=${state.lastId || 0}&limit=5000`);
      const added = appendTicks(rows);
      if (added) {
        // get labels for the newly appended range only
        const fromId = Math.max(
          state.labelsLoadedUntil.max,
          state.labelsLoadedUntil.mid,
          state.labelsLoadedUntil.min
        ) + 1 || (state.x.length ? state.x[0] : 1);
        await fetchLabelsFrom(fromId, added * 4);
        setStatus(`+${added}`);
      } else {
        setStatus('no new ticks');
      }
    } catch (e) {
      console.error(e); setStatus('error');
    } finally {
      state.fetching = false;
    }
  }

  // ----- UI -----
  document.querySelectorAll('input[type=checkbox][data-series]')
    .forEach(cb => cb.addEventListener('change', setSeriesVisibility));

  document.getElementById('btnLoadNew').addEventListener('click', loadNew);

  const autoBox = document.getElementById('autoFetch');
  let autoTimer = null;
  autoBox.addEventListener('change', () => {
    if (autoBox.checked) {
      if (autoTimer) clearInterval(autoTimer);
      autoTimer = setInterval(loadNew, 1000);
    } else {
      if (autoTimer) clearInterval(autoTimer);
      autoTimer = null;
    }
  });

  // Boot
  loadInitial();
})();
