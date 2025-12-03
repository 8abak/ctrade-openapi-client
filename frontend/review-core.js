// PATH: frontend/review-core.js
// Review window viewer for tick-level Kalman study with:
//   - bid / ask / mid
//   - kal / kal_fast / kal_slow
//   - Run/Stop auto-scroll over historical data
//
// Backend endpoint:
//   GET /api/review/window?from_id=...&window=...

(function () {
  /* global echarts */

  const chartEl = document.getElementById('chart');
  const chart = echarts.init(chartEl);

  // --- Controls ---
  const fromIdInput = document.getElementById('fromId');
  const windowInput = document.getElementById('windowSize');
  const goBtn       = document.getElementById('btnGo');
  const prevBtn     = document.getElementById('btnPrev');
  const nextBtn     = document.getElementById('btnNext');
  const playBtn     = document.getElementById('btnPlay');
  const statusEl    = document.getElementById('status');

  const chkBid      = document.getElementById('showBid');
  const chkAsk      = document.getElementById('showAsk');
  const chkMid      = document.getElementById('showMid');
  const chkKal      = document.getElementById('showKal');
  const chkKalFast  = document.getElementById('showKalFast');
  const chkKalSlow  = document.getElementById('showKalSlow');

  // --- Data holders ---
  let ticks = [];

  // --- State ---
  let currentFromId = null;
  let currentWindow = 2000;
  let loading       = false;

  let autoPlay      = false;
  let playTimer     = null;

  // ---------- Helpers ----------

  function setStatus(text) {
    if (statusEl) statusEl.textContent = text || '';
  }

  function setLoading(isLoading) {
    loading = isLoading;
    if (goBtn)   goBtn.disabled   = isLoading;
    if (prevBtn) prevBtn.disabled = isLoading;
    if (nextBtn) nextBtn.disabled = isLoading;
    if (playBtn) playBtn.disabled = isLoading && !autoPlay;
    if (isLoading) setStatus('Loading...');
  }

  function safeInt(val, fallback) {
    const n = Number(val);
    if (!Number.isFinite(n) || n <= 0) return fallback;
    return Math.floor(n);
  }

  async function fetchWindow(fromId, windowSize) {
    const url = `/api/review/window?from_id=${encodeURIComponent(fromId)}&window=${encodeURIComponent(windowSize)}`;
    const resp = await fetch(url);
    if (!resp.ok) {
      const txt = await resp.text();
      throw new Error(`HTTP ${resp.status}: ${txt}`);
    }
    return resp.json();
  }

  // ---------- Chart drawing ----------

  function rebuildChart() {
    const showBid     = chkBid     ? chkBid.checked     : true;
    const showAsk     = chkAsk     ? chkAsk.checked     : true;
    const showMid     = chkMid     ? chkMid.checked     : true;
    const showKal     = chkKal     ? chkKal.checked     : true;
    const showKalFast = chkKalFast ? chkKalFast.checked : true;
    const showKalSlow = chkKalSlow ? chkKalSlow.checked : true;

    if (!ticks.length) {
      chart.setOption({
        backgroundColor: '#0d1117',
        animation: false,
        grid: { left: 60, right: 20, top: 40, bottom: 60 },
        xAxis: {
          type: 'time',
          axisLine: { lineStyle: { color: '#8b949e' } },
          axisLabel: { color: '#8b949e' },
          splitLine: { lineStyle: { color: '#30363d' } },
        },
        yAxis: {
          type: 'value',
          scale: true,
          minInterval: 1,
          axisLine: { lineStyle: { color: '#8b949e' } },
          axisLabel: { color: '#8b949e' },
          splitLine: { lineStyle: { color: '#30363d' } },
        },
        dataZoom: [
          { type: 'inside', throttle: 50 },
          {
            type: 'slider',
            height: 18,
            bottom: 30,
            handleSize: 8,
            borderColor: '#30363d',
            backgroundColor: '#161b22',
            fillerColor: 'rgba(88, 166, 255, 0.2)',
          },
        ],
        series: [],
      }, true);
      return;
    }

    const bidSeries = ticks.map(t => [t.ts, Number(t.bid)]);
    const askSeries = ticks.map(t => [t.ts, Number(t.ask)]);
    const midSeries = ticks.map(t => [t.ts, Number(t.mid)]);
    const kalSeries = ticks.map(t =>
      t.kal != null ? [t.ts, Number(t.kal)] : [t.ts, NaN]
    );
    const kalFastSeries = ticks.map(t =>
      t.kal_fast != null ? [t.ts, Number(t.kal_fast)] : [t.ts, NaN]
    );
    const kalSlowSeries = ticks.map(t =>
      t.kal_slow != null ? [t.ts, Number(t.kal_slow)] : [t.ts, NaN]
    );

    const series = [];
    function pushLine(name, data, z, color) {
      series.push({
        name,
        type: 'line',
        showSymbol: false,
        data,
        lineStyle: {
          width: 1,
          color: color || undefined,
        },
        z,
      });
    }

    let z = 1;
    if (showBid)     pushLine('Bid',      bidSeries,     z++, '#58a6ff');
    if (showAsk)     pushLine('Ask',      askSeries,     z++, '#f85149');
    if (showMid)     pushLine('Mid',      midSeries,     z++, '#c9d1d9');
    if (showKal)     pushLine('Kalman',   kalSeries,     z++, '#2ea043');
    if (showKalFast) pushLine('Kal Fast', kalFastSeries, z++, '#d29922');
    if (showKalSlow) pushLine('Kal Slow', kalSlowSeries, z++, '#a371f7');

    const option = {
      backgroundColor: '#0d1117',
      animation: false,
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'cross' },
        valueFormatter: value => (value != null ? value.toFixed(3) : ''),
        formatter: function (params) {
          if (!params || !params.length) return '';

          const axis = params[0];
          const idx  = axis.dataIndex;
          const tick = ticks[idx];

          const byName = {};
          for (const p of params) byName[p.seriesName] = p;

          const lines = [];
          if (tick && tick.ts) {
            lines.push(echarts.format.formatTime('yyyy-MM-dd hh:mm:ss', tick.ts));
          }

          if (tick && tick.id != null) {
            lines.push(`ID: ${tick.id}`);
          }

          function addVal(name, label) {
            const p = byName[name];
            if (!p || !p.data || p.data[1] == null || !Number.isFinite(p.data[1])) return;
            lines.push(`${label}: ${p.data[1].toFixed(3)}`);
          }

          addVal('Bid',      'Bid');
          addVal('Ask',      'Ask');
          addVal('Mid',      'Mid');
          addVal('Kalman',   'Kal');
          addVal('Kal Fast', 'Kal Fast');
          addVal('Kal Slow', 'Kal Slow');

          return lines.join('<br/>');
        },
      },
      legend: {
        show: true,
        top: 4,
        textStyle: { color: '#c9d1d9', fontSize: 11 },
        selected: {
          'Bid':      showBid,
          'Ask':      showAsk,
          'Mid':      showMid,
          'Kalman':   showKal,
          'Kal Fast': showKalFast,
          'Kal Slow': showKalSlow,
        },
      },
      grid: {
        left: 60,
        right: 20,
        top: 35,
        bottom: 60,
      },
      xAxis: {
        type: 'time',
        boundaryGap: false,
        axisLine: { lineStyle: { color: '#8b949e' } },
        axisLabel: {
          color: '#8b949e',
          formatter: value => echarts.format.formatTime('hh:mm:ss', value),
        },
        splitLine: { lineStyle: { color: '#30363d' } },
      },
      yAxis: {
        type: 'value',
        scale: true,
        minInterval: 1,
        axisLine: { lineStyle: { color: '#8b949e' } },
        axisLabel: { color: '#8b949e' },
        splitLine: { lineStyle: { color: '#30363d' } },
      },
      dataZoom: [
        {
          type: 'inside',
          throttle: 50,
          zoomOnMouseWheel: true,
          moveOnMouseWheel: true,
          moveOnMouseMove: true,
        },
        {
          type: 'slider',
          height: 18,
          bottom: 30,
          borderColor: '#30363d',
          backgroundColor: '#161b22',
          fillerColor: 'rgba(88, 166, 255, 0.2)',
          handleIcon:
            'path://M8,0 L12,0 C12.552,0 13,0.448 13,1 L13,15 C13,15.552 12.552,16 12,16 L8,16 C7.448,16 7,15.552 7,15 L7,1 C7,0.448 7.448,0 8,0 Z',
          handleSize: 10,
          handleStyle: { borderWidth: 1 },
        },
      ],
      series,
    };

    chart.setOption(option, true);
  }

  // ---------- Loading windows ----------

  async function loadWindow(fromId, windowSize) {
    currentFromId = fromId;
    currentWindow = windowSize;

    if (fromIdInput) fromIdInput.value = String(fromId);
    if (windowInput) windowInput.value = String(windowSize);

    setLoading(true);
    try {
      const data = await fetchWindow(fromId, windowSize);

      // we expect backend /api/review/window to return
      // ticks with: id, ts, bid, ask, mid, kal, kal_fast, kal_slow
      ticks = (data.ticks || []).map(t => ({
        ...t,
        id: Number(t.id),
        bid: t.bid != null ? Number(t.bid) : t.bid,
        ask: t.ask != null ? Number(t.ask) : t.ask,
        mid: t.mid != null ? Number(t.mid) : t.mid,
        kal: t.kal != null ? Number(t.kal) : t.kal,
        kal_fast: t.kal_fast != null ? Number(t.kal_fast) : t.kal_fast,
        kal_slow: t.kal_slow != null ? Number(t.kal_slow) : t.kal_slow,
      }));

      if (!ticks.length) {
        setStatus(`No ticks for window from id ${fromId} (window ${windowSize})`);
        rebuildChart();
        if (autoPlay) {
          autoPlay = false;
          if (playBtn) playBtn.textContent = 'Run';
          stopPlayTimer();
        }
        return;
      }

      const firstId = ticks[0].id;
      const lastId  = ticks[ticks.length - 1].id;

      setStatus(`Ticks ${firstId}–${lastId} (${ticks.length})`);

      rebuildChart();
    } catch (err) {
      console.error(err);
      setStatus(`Error: ${err.message || err}`);
      ticks = [];
      rebuildChart();
    } finally {
      setLoading(false);
    }
  }

  // ---------- Button handlers ----------

  function handleGo() {
    const fromId = safeInt(fromIdInput && fromIdInput.value, null);
    const windowSize = safeInt(windowInput && windowInput.value, currentWindow);

    if (fromId == null) {
      setStatus('Please enter a valid starting tick id.');
      return;
    }
    loadWindow(fromId, windowSize);
  }

  function handlePrev() {
    if (currentFromId == null) {
      setStatus('No current window; use Go first.');
      return;
    }
    const windowSize = safeInt(windowInput && windowInput.value, currentWindow);
    const newFrom = Math.max(1, currentFromId - windowSize);
    loadWindow(newFrom, windowSize);
  }

  function handleNext() {
    if (currentFromId == null) {
      setStatus('No current window; use Go first.');
      return;
    }
    const windowSize = safeInt(windowInput && windowInput.value, currentWindow);
    const newFrom = currentFromId + windowSize;
    loadWindow(newFrom, windowSize);
  }

  function stopPlayTimer() {
    if (playTimer) {
      clearTimeout(playTimer);
      playTimer = null;
    }
  }

  // auto-play: slide one tick at a time, keeping same window size
  function scheduleNextStep() {
    if (!autoPlay) return;

    if (loading) {
      playTimer = setTimeout(scheduleNextStep, 500);
      return;
    }

    if (currentFromId == null) {
      setStatus('No current window; use Go first.');
      autoPlay = false;
      if (playBtn) playBtn.textContent = 'Run';
      return;
    }

    const windowSize = safeInt(windowInput && windowInput.value, currentWindow);
    const nextFrom = currentFromId + 1;
    loadWindow(nextFrom, windowSize);

    playTimer = setTimeout(scheduleNextStep, 1500);
  }

  function togglePlay() {
    autoPlay = !autoPlay;
    if (!playBtn) return;

    if (autoPlay) {
      playBtn.textContent = 'Stop';
      scheduleNextStep();
    } else {
      playBtn.textContent = 'Run';
      stopPlayTimer();
    }
  }

  // ---------- Event wiring ----------

  if (goBtn)   goBtn.addEventListener('click', () => { if (!loading) handleGo(); });
  if (prevBtn) prevBtn.addEventListener('click', () => { if (!loading) handlePrev(); });
  if (nextBtn) nextBtn.addEventListener('click', () => { if (!loading) handleNext(); });
  if (playBtn) playBtn.addEventListener('click', togglePlay);

  if (fromIdInput) {
    fromIdInput.addEventListener('keydown', e => {
      if (e.key === 'Enter' && !loading) handleGo();
    });
  }
  if (windowInput) {
    windowInput.addEventListener('keydown', e => {
      if (e.key === 'Enter' && !loading) handleGo();
    });
  }

  function hookToggle(el) {
    if (!el) return;
    el.addEventListener('change', rebuildChart);
  }

  hookToggle(chkBid);
  hookToggle(chkAsk);
  hookToggle(chkMid);
  hookToggle(chkKal);
  hookToggle(chkKalFast);
  hookToggle(chkKalSlow);
  

  window.addEventListener('resize', () => {
    chart.resize();
  });

  // Initial empty chart
  rebuildChart();
})();
