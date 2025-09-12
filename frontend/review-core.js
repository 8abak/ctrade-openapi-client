// Review page with chunked range loading and append-only behavior
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
    x: [], ts: [],
    ask: [], bid: [], mid_tick: [],
    max_lbl: [], mid_lbl: [], min_lbl: [],
    rightmostId: 0,
  };

  function intAxisFormatter(v) { return Math.round(v); }

  function mkLine(name, key, color) {
    return {
      name, type: 'line', showSymbol: false, smooth: false,
      data: state[key], itemStyle: { color }, lineStyle: { color, width: 1.2 },
      connectNulls: false,
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
          const idx = params[0].dataIndex;
          const id = state.x[idx];
          const ts = state.ts[idx] || '';
          const dt = ts ? new Date(ts) : null;
          const d = dt ? `${dt.toLocaleDateString()} ${dt.toLocaleTimeString()}` : '(no time)';
          const lines = [`<div><b>ID</b>: ${id}</div><div><b>Time</b>: ${d}</div><hr>`];
          for (const p of params) if (p.value != null) lines.push(
            `<div><span style="display:inline-block;width:8px;height:8px;background:${p.color};margin-right:6px;border-radius:2px"></span>${p.seriesName}: ${p.value}</div>`
          );
          return lines.join('');
        }
      },
      xAxis: {
        type: 'category', data: state.x,
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

  chart.setOption(baseOption(), { notMerge: true, lazyUpdate: true });

  function pinnedRight() {
    const dz = chart.getOption().dataZoom?.[0];
    if (!dz) return false;
    const end = dz.end ?? 100;
    return end > 99.5;
  }

  function setSeriesVisibility() {
    const boxes = document.querySelectorAll('input[type=checkbox][data-series]');
    const showMap = {};
    boxes.forEach(b => showMap[b.dataset.series] = b.checked);
    const opt = chart.getOption();
    opt.series.forEach(s => {
      const key = keyFromName(s.name);
      s.data = state[key];
      const visible = showMap[key];
      s.itemStyle.opacity = visible ? 1 : 0;
      s.lineStyle.opacity = visible ? 1 : 0;
    });
    chart.setOption(opt, { notMerge: true, lazyUpdate: true });
  }
  document.querySelectorAll('input[type=checkbox][data-series]')
    .forEach(cb => cb.addEventListener('change', setSeriesVisibility));

  function keyFromName(name) {
    if (name === 'Ask') return 'ask';
    if (name === 'Bid') return 'bid';
    if (name === 'Mid (ticks)') return 'mid_tick';
    if (name === 'Max (labels)') return 'max_lbl';
    if (name === 'Mid (labels)') return 'mid_lbl';
    if (name === 'Min (labels)') return 'min_lbl';
    return '';
  }

  async function fetchJSON(url) {
    const r = await fetch(url);
    if (!r.ok) throw new Error(await r.text());
    return r.json();
  }

  function setStatus(s) { document.getElementById('status').textContent = s; }

  function appendTicks(rows) {
    if (!rows || !rows.length) return;
    const atRight = pinnedRight();
    for (const r of rows) {
      state.x.push(r.id);
      state.ts.push(r.ts || null);
      state.mid_tick.push(r.mid ?? null);
      state.ask.push(r.ask ?? null);
      state.bid.push(r.bid ?? null);
      state.max_lbl.push(null);
      state.mid_lbl.push(null);
      state.min_lbl.push(null);
      state.rightmostId = r.id;
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

  function overlayLabels(rows, key) {
    if (!rows || !rows.length) return;
    const map = new Map(state.x.map((id, i) => [id, i]));
    const arr = state[key];
    for (const r of rows) {
      const i = map.get(r.id);
      if (i != null) arr[i] = r.value;
    }
    chart.setOption({
      series: [
        { name: 'Max (labels)', data: state.max_lbl },
        { name: 'Mid (labels)', data: state.mid_lbl },
        { name: 'Min (labels)', data: state.min_lbl },
      ]
    }, { lazyUpdate: true });
  }

  function stickToRight() {
    chart.dispatchAction({ type: 'dataZoom', start: 100, end: 100 });
  }

  async function loadChunk(startId, size) {
    setStatus('loading…');
    // `/api/ticks` is an alias to /ticks/range in your backend
    const endId = startId + size - 1;
    const rows = await fetchJSON(`/api/ticks?from_id=${startId}&to_id=${endId}`);
    appendTicks(rows);
    if (rows.length) {
      // align labels to the window
      await Promise.all([
        fetchJSON(`/api/labels/max/range?start_id=${startId}&limit=${rows.length}`).then(r=>overlayLabels(r,'max_lbl')).catch(()=>{}),
        fetchJSON(`/api/labels/mid/range?start_id=${startId}&limit=${rows.length}`).then(r=>overlayLabels(r,'mid_lbl')).catch(()=>{}),
        fetchJSON(`/api/labels/min/range?start_id=${startId}&limit=${rows.length}`).then(r=>overlayLabels(r,'min_lbl')).catch(()=>{}),
      ]);
    }
    setStatus(`loaded ${rows.length}`);
  }

  // UI actions
  document.getElementById('btnLoad').addEventListener('click', async () => {
    const startId = parseInt(document.getElementById('startId').value || '1', 10);
    const size = parseInt(document.getElementById('chunkSize').value || '10000', 10);
    // reset
    Object.assign(state, {
      x: [], ts: [], ask: [], bid: [], mid_tick: [],
      max_lbl: [], mid_lbl: [], min_lbl: [],
      rightmostId: 0,
    });
    chart.clear();
    chart.setOption(baseOption(), { notMerge: true });
    await loadChunk(startId, size);
  });

  document.getElementById('btnMore').addEventListener('click', async () => {
    if (!state.rightmostId) return;
    const size = parseInt(document.getElementById('chunkSize').value || '10000', 10);
    await loadChunk(state.rightmostId + 1, size);
  });

  // ready: keep initial empty chart; user chooses StartID/Chunk to begin
})();
