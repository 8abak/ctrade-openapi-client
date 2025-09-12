// Dark ECharts instance and shared helpers for live page
(() => {
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
    x: [],            // tick ids
    ts: [],           // ISO timestamp strings aligned to x
    ask: [],
    bid: [],
    mid_tick: [],
    max_lbl: [],      // sparse allowed
    mid_lbl: [],
    min_lbl: [],
    lastId: 0,
    auto: false,
    fetching: false,
  };

  function intAxisFormatter(v) { return Math.round(v); }

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
          // params share the same dataIndex
          const idx = params[0].dataIndex;
          const id = state.x[idx];
          const ts = state.ts[idx] || '';
          const dt = ts ? new Date(ts) : null;
          const d = dt
            ? `${dt.toLocaleDateString()} ${dt.toLocaleTimeString()}`
            : '(no time)';
          const lines = [`<div><b>ID</b>: ${id}</div><div><b>Time</b>: ${d}</div><hr>`];
          for (const p of params) {
            if (p.value == null) continue;
            lines.push(`<div><span style="display:inline-block;width:8px;height:8px;background:${p.color};margin-right:6px;border-radius:2px"></span>${p.seriesName}: ${p.value}</div>`);
          }
          return lines.join('');
        }
      },
      xAxis: {
        type: 'category',
        data: state.x,
        axisLabel: { color: '#9ca3af' },
        axisLine: { lineStyle: { color: '#1f2937' }},
        splitLine: { show: true, lineStyle: { color: 'rgba(148,163,184,0.08)' }},
      },
      yAxis: {
        type: 'value',
        axisLabel: { color: '#9ca3af', formatter: intAxisFormatter },
        axisLine: { lineStyle: { color: '#1f2937' }},
        splitLine: { show: true, lineStyle: { color: 'rgba(148,163,184,0.08)' }},
        scale: true,
      },
      dataZoom: [
        { type: 'inside' },
        { type: 'slider', height: 16, bottom: 4 }
      ],
      series: [
        mkLine('Ask', 'ask', COLORS.ask),
        mkLine('Mid (ticks)', 'mid_tick', COLORS.mid_tick),
        mkLine('Bid', 'bid', COLORS.bid),
        mkLine('Max (labels)', 'max_lbl', COLORS.max_lbl),
        mkLine('Mid (labels)', 'mid_lbl', COLORS.mid_lbl),
        mkLine('Min (labels)', 'min_lbl', COLORS.min_lbl),
      ]
    };
  }

  function mkLine(name, key, color) {
    return {
      name, type: 'line', showSymbol: false, smooth: false,
      data: state[key], itemStyle: { color }, lineStyle: { color, width: 1.2 },
      connectNulls: false,
    };
  }

  function pinnedRight() {
    const dz = chart.getOption().dataZoom?.[0];
    if (!dz) return true;
    // consider pinned if the right edge is at or past the last point
    const end = dz.end ?? 100;
    return end > 99.5; // percentage
  }

  function setSeriesVisibility() {
    const boxes = document.querySelectorAll('input[type=checkbox][data-series]');
    const showMap = {};
    boxes.forEach(b => showMap[b.dataset.series] = b.checked);
    const opt = chart.getOption();
    opt.series.forEach(s => {
      const key = keyFromName(s.name);
      s.data = state[key];
      s.itemStyle.opacity = showMap[key] ? 1 : 0;
      s.lineStyle.opacity = showMap[key] ? 1 : 0;
    });
    chart.setOption(opt, { lazyUpdate: true, notMerge: true });
  }

  function keyFromName(name) {
    if (name === 'Ask') return 'ask';
    if (name === 'Bid') return 'bid';
    if (name === 'Mid (ticks)') return 'mid_tick';
    if (name === 'Max (labels)') return 'max_lbl';
    if (name === 'Mid (labels)') return 'mid_lbl';
    if (name === 'Min (labels)') return 'min_lbl';
    return '';
  }

  function applyInitialOption() {
    chart.setOption(baseOption(), { notMerge: true, lazyUpdate: true });
  }

  async function fetchJSON(url) {
    const r = await fetch(url);
    if (!r.ok) throw new Error(await r.text());
    return r.json();
  }

  function appendTicks(rows) {
    if (!rows || !rows.length) return;
    const atRight = pinnedRight();
    for (const r of rows) {
      state.x.push(r.id);
      state.ts.push(r.ts || null);
      state.mid_tick.push(r.mid ?? null);
      state.ask.push(r.ask ?? null);
      state.bid.push(r.bid ?? null);
      // placeholders to align arrays
      state.max_lbl.push(null);
      state.mid_lbl.push(null);
      state.min_lbl.push(null);
      state.lastId = r.id;
    }
    chart.setOption({
      xAxis: { data: state.x },
      series: [
        { name: 'Ask', data: state.ask },
        { name: 'Mid (ticks)', data: state.mid_tick },
        { name: 'Bid', data: state.bid },
        { name: 'Max (labels)', data: state.max_lbl },
        { name: 'Mid (labels)', data: state.mid_lbl },
        { name: 'Min (labels)', data: state.min_lbl },
      ]
    }, { lazyUpdate: true });
    if (atRight) stickToRight();
  }

  function stickToRight() {
    const len = state.x.length;
    if (len < 2) return;
    chart.dispatchAction({ type: 'dataZoom', start: 100, end: 100 });
  }

  function overlayLabels(rows, targetKey) {
    if (!rows || !rows.length) return;
    const map = new Map(state.x.map((id, i) => [id, i]));
    const arr = state[targetKey];
    for (const r of rows) {
      const i = map.get(r.id);
      if (i != null) arr[i] = r.value;
    }
    chart.setOption({
      series: [{ name:'Max (labels)', data: state.max_lbl },
               { name:'Mid (labels)', data: state.mid_lbl },
               { name:'Min (labels)', data: state.min_lbl }]
    }, { lazyUpdate: true });
  }

  async function loadInitial() {
    setStatus('loading last 10k…');
    const rows = await fetchJSON('/api/ticks/latestN?limit=10000');
    appendTicks(rows);
    setStatus(`loaded ${rows.length} ticks`);
    // Try to fetch labels aligned with the current window
    if (rows.length) {
      const startId = rows[0].id;
      await Promise.all([
        fetchJSON(`/api/labels/max/range?start_id=${startId}&limit=${rows.length}`).then(r=>overlayLabels(r,'max_lbl')).catch(()=>{}),
        fetchJSON(`/api/labels/mid/range?start_id=${startId}&limit=${rows.length}`).then(r=>overlayLabels(r,'mid_lbl')).catch(()=>{}),
        fetchJSON(`/api/labels/min/range?start_id=${startId}&limit=${rows.length}`).then(r=>overlayLabels(r,'min_lbl')).catch(()=>{}),
      ]);
    }
  }

  async function loadNew() {
    if (state.fetching) return;
    state.fetching = true;
    setStatus('fetching…');
    try {
      const rows = await fetchJSON(`/api/ticks/after?since_id=${state.lastId || 0}&limit=5000`);
      appendTicks(rows);
      setStatus(rows.length ? `+${rows.length}` : 'no new ticks');
    } catch (e) {
      setStatus('error');
      console.error(e);
    } finally {
      state.fetching = false;
    }
  }

  function setStatus(text) { document.getElementById('status').textContent = text; }

  // UI wiring
  document.querySelectorAll('input[type=checkbox][data-series]')
    .forEach(cb => cb.addEventListener('change', setSeriesVisibility));

  document.getElementById('btnLoadNew').addEventListener('click', loadNew);

  const autoBox = document.getElementById('autoFetch');
  let autoTimer = null;
  autoBox.addEventListener('change', () => {
    state.auto = autoBox.checked;
    if (state.auto) {
      if (autoTimer) clearInterval(autoTimer);
      autoTimer = setInterval(loadNew, 1000);
    } else {
      if (autoTimer) clearInterval(autoTimer);
      autoTimer = null;
    }
  });

  // boot
  applyInitialOption();
  loadInitial();
})();
