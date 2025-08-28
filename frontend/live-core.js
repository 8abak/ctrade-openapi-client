(() => {
  const chart = echarts.init(document.getElementById('chart'));
  const btnToggle = document.getElementById('toggleBtn');
  const selWin = document.getElementById('winSel');
  const inpJump = document.getElementById('jumpId');
  const btnJump = document.getElementById('jumpBtn');
  const state = document.getElementById('state');

  let running = true;
  let es = null;
  const xs = [];      // Date objects
  const ys = [];      // mid
  const ids = [];     // tick ids
  const preds = [];   // {id, at_id, hit, ts, y}

  function render() {
    const win = Number(selWin.value);
    const n = xs.length;
    const start = Math.max(0, n - win);
    const xSlice = xs.slice(start);
    const ySlice = ys.slice(start);
    const predHit = preds.filter(p => p.hit === true).map(p => [p.ts, p.y]);
    const predMiss = preds.filter(p => p.hit === false).map(p => [p.ts, p.y]);

    chart.setOption({
      animation: false,
      tooltip: { trigger: 'axis' },
      xAxis: { type: 'time' },
      yAxis: { type: 'value', scale: true },
      series: [
        { type: 'line', name: 'Mid', showSymbol: false, data: xSlice.map((t, i) => [t, ySlice[i]]) },
        { type: 'scatter', name: 'Pred ✓', data: predHit, symbol: 'circle', symbolSize: 8 },
        { type: 'scatter', name: 'Pred ✗', data: predMiss, symbol: 'diamond', symbolSize: 8 },
      ],
      legend: { top: 10 },
      grid: { left: 10, right: 10, top: 40, bottom: 10, containLabel: true },
    });
  }

  function startStream() {
    if (es) es.close();
    es = new EventSource('/api/live');
    es.addEventListener('tick', (ev) => {
      const d = JSON.parse(ev.data);
      const dt = new Date(d.ts);
      ids.push(d.id);
      xs.push(dt);
      ys.push(Number(d.mid));
      render();
    });
    es.addEventListener('pred', (ev) => {
      const p = JSON.parse(ev.data);
      // place marker at entry
      const i = ids.indexOf(p.at_id);
      if (i >= 0) {
        preds.push({ id: p.id, at_id: p.at_id, hit: p.hit, ts: xs[i], y: ys[i] });
        render();
      }
    });
    es.onerror = () => { /* keep alive; server will retry */ }
    state.textContent = 'live';
  }

  function stopStream() {
    if (es) { es.close(); es = null; }
    state.textContent = 'paused';
  }

  btnToggle.addEventListener('click', () => {
    running = !running;
    btnToggle.textContent = running ? '⏸ Pause' : '▶ Resume';
    if (running) startStream(); else stopStream();
  });

  btnJump.addEventListener('click', async () => {
    const id = Number(inpJump.value);
    if (!id) return;
    // Load a window around the id for quick context
    const from = Math.max(1, id - 2000), to = id + 2000;
    const rows = await fetch(`/api/ticks?from_id=${from}&to_id=${to}`).then(r => r.json());
    xs.length = 0; ys.length = 0; ids.length = 0;
    for (const r of rows) {
      ids.push(r.id);
      xs.push(new Date(r.ts));
      ys.push(Number(r.mid));
    }
    render();
  });

  selWin.addEventListener('change', render);

  startStream();
})();
