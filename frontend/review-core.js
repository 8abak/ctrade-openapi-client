const Review = (() => {
  let chart;

  const state = {
    segmId: null,
    showMid: true,
    showKal: true,
    showSegLines: true,
  };

  async function fetchJSON(url) {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`Fetch failed: ${url}`);
    return res.json();
  }

  async function loadSegms() {
    const segms = await fetchJSON('/api/segms');
    const sel = document.getElementById('segmSelect');
    sel.innerHTML = '';

    segms.forEach(s => {
      const opt = document.createElement('option');
      opt.value = s.id;
      opt.textContent = `${s.start_ts.slice(0,10)} (#${s.id})`;
      sel.appendChild(opt);
    });

    if (segms.length) {
      state.segmId = segms[0].id;
      sel.value = state.segmId;
    }
  }

  async function loadAndRender() {
    if (!state.segmId) return;

    const [ticks, seglines] = await Promise.all([
      fetchJSON(`/api/ticks?segm_id=${state.segmId}`),
      fetchJSON(`/api/seglines?segm_id=${state.segmId}`),
    ]);

    renderChart(ticks, seglines);
    renderTable(seglines);
  }

  function renderChart(ticks, seglines) {
    const series = [];

    if (state.showMid) {
      series.push({
        name: 'Mid',
        type: 'line',
        data: ticks.map(t => [t.timestamp, t.mid]),
        showSymbol: false,
        lineStyle: { width: 1 },
      });
    }

    if (state.showKal) {
      series.push({
        name: 'Kal',
        type: 'line',
        data: ticks.map(t => [t.timestamp, t.kal]),
        showSymbol: false,
        lineStyle: { width: 1 },
      });
    }

    if (state.showSegLines) {
      seglines
        .filter(l => l.is_active)
        .forEach(l => {
          series.push({
            name: `segline-${l.id}`,
            type: 'line',
            data: [
              [l.start_ts, l.start_price],
              [l.end_ts, l.end_price],
            ],
            showSymbol: false,
            lineStyle: { width: 2 },
            z: 10,
          });
        });
    }

    chart.setOption({
      animation: false,
      tooltip: { trigger: 'axis' },
      xAxis: { type: 'time' },
      yAxis: { scale: true },
      series,
    }, true);
  }

  function renderTable(seglines) {
    const tbody = document.querySelector('#linesTable tbody');
    tbody.innerHTML = '';

    seglines
      .filter(l => l.is_active)
      .sort((a, b) => Math.abs(b.max_abs_dist) - Math.abs(a.max_abs_dist))
      .forEach(l => {
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td>${l.id}</td>
          <td>${l.depth}</td>
          <td>${l.iteration}</td>
          <td>${l.start_ts.slice(11,19)}</td>
          <td>${l.end_ts.slice(11,19)}</td>
          <td>${l.tick_count}</td>
          <td>${Number(l.slope).toFixed(5)}</td>
          <td>${Number(l.max_abs_dist).toFixed(3)}</td>
        `;
        tbody.appendChild(tr);
      });
  }

  async function breakOnce() {
    await fetch(`/api/break_line?segm_id=${state.segmId}`, { method: 'POST' });
    await loadAndRender();
  }

  function initUI() {
    document.getElementById('segmSelect').onchange = e => {
      state.segmId = Number(e.target.value);
      loadAndRender();
    };

    document.getElementById('toggleMid').onclick = e => {
      state.showMid = !state.showMid;
      e.target.classList.toggle('active', state.showMid);
      loadAndRender();
    };

    document.getElementById('toggleKal').onclick = e => {
      state.showKal = !state.showKal;
      e.target.classList.toggle('active', state.showKal);
      loadAndRender();
    };

    document.getElementById('toggleSegLines').onclick = e => {
      state.showSegLines = !state.showSegLines;
      e.target.classList.toggle('active', state.showSegLines);
      loadAndRender();
    };

    document.getElementById('breakBtn').onclick = breakOnce;
  }

  async function init() {
    chart = echarts.init(document.getElementById('chart'));
    await loadSegms();
    initUI();
    await loadAndRender();
  }

  return { init };
})();

window.addEventListener('DOMContentLoaded', Review.init);
