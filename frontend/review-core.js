// PATH: frontend/review-core.js
// Review window viewer with:
//   - zones, Kalman, segments
//   - snowball predictions (latest SGD run) drawn on price
//   - Play/Stop auto-scroll over historical data
//
// Backend endpoints used:
//   GET  /api/review/window?from_id=...&window=...
//   POST /api/sql   { sql: "..." }   -> rows for predictions

(function () {
  /* global echarts */

  const chartEl = document.getElementById('chart');
  const chart = echarts.init(chartEl);

  // --- Controls ---
  const fromIdInput   = document.getElementById('fromId');
  const windowInput   = document.getElementById('windowSize');
  const goBtn         = document.getElementById('btnGo');
  const prevBtn       = document.getElementById('btnPrev');
  const nextBtn       = document.getElementById('btnNext');
  const playBtn       = document.getElementById('btnPlay');
  const statusEl      = document.getElementById('status');

  const chkKal        = document.getElementById('showKal');
  const chkZones      = document.getElementById('showZones');
  const chkSegs       = document.getElementById('showSegs');
  const chkPred       = document.getElementById('showPreds');

  // --- Data holders ---
  let ticks        = [];
  let segs         = [];
  let zones        = [];
  let predictions  = []; // from kalseg_prediction (latest SGD run)

  // --- State ---
  let currentFromId = null;
  let currentWindow = 5000;
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
    if (playBtn) playBtn.disabled = isLoading && !autoPlay; // allow stop
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

  async function fetchPredictionsForRange(startId, endId) {
    // Latest SGD run on kalseg_prediction, restricted to this id range
    const sql = `
      WITH latest_sgd AS (
        SELECT run_id
        FROM kalseg_prediction
        WHERE run_id LIKE 'sgd-%'
        ORDER BY run_id DESC
        LIMIT 1
      )
      SELECT
        seg_id,
        start_id,
        pred_label,
        proba_down,
        proba_none,
        proba_up,
        run_id
      FROM kalseg_prediction p
      JOIN latest_sgd l USING (run_id)
      WHERE start_id BETWEEN ${startId} AND ${endId}
      ORDER BY start_id;
    `;

    const resp = await fetch('/api/sql', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql }),
    });

    if (!resp.ok) {
      const txt = await resp.text();
      throw new Error(`SQL HTTP ${resp.status}: ${txt}`);
    }

    const data = await resp.json();
    return data && Array.isArray(data.rows) ? data.rows : [];
  }

  function buildZoneBands(ticksArr, zonesArr) {
    if (!ticksArr.length || !zonesArr.length) return [];

    const bands = [];

    for (const z of zonesArr) {
      const startId = Number(z.start_id);
      const endId   = Number(z.end_id);

      let min = Number.POSITIVE_INFINITY;
      let max = Number.NEGATIVE_INFINITY;
      let tsStart = null;
      let tsEnd   = null;

      for (const t of ticksArr) {
        const id = Number(t.id);
        if (id < startId || id > endId) continue;

        const price = Number(t.mid);
        if (!Number.isFinite(price)) continue;

        if (price < min) min = price;
        if (price > max) max = price;
        if (!tsStart || id === startId) tsStart = t.ts;
        tsEnd = t.ts;
      }

      if (!Number.isFinite(min) || !Number.isFinite(max) || tsStart == null || tsEnd == null) {
        continue;
      }

      const dir = (z.direction || '').toString().toLowerCase();
      let color = 'rgba(56, 139, 253, 0.18)'; // blue-ish default
      if (dir === 'up' || dir === '1' || dir === 'u') {
        color = 'rgba(46, 160, 67, 0.18)';     // green-ish
      } else if (dir === 'dn' || dir === '-1' || dir === 'down' || dir === 'd') {
        color = 'rgba(248, 81, 73, 0.18)';     // red-ish
      }

      bands.push({
        name: z.zone_type || '',
        itemStyle: { color },
        coord: [tsStart, tsEnd, min, max],
      });
    }

    return bands;
  }

  function buildSegmentPoints(ticksArr, segsArr) {
    if (!ticksArr.length || !segsArr.length) return [];

    const byId = new Map();
    for (const t of ticksArr) byId.set(Number(t.id), t);

    const points = [];

    for (const s of segsArr) {
      const startId = Number(s.start_id);

      let startTick = byId.get(startId);
      if (!startTick) {
        for (const t of ticksArr) {
          if (Number(t.id) >= startId) {
            startTick = t;
            break;
          }
        }
      }
      if (!startTick) continue;

      const dirRaw = (s.direction || '').toString().toLowerCase();
      const isUp   = (dirRaw === 'up' || dirRaw === '1' || dirRaw === 'u');

      const price  = Number(
        startTick.kal != null ? startTick.kal : startTick.mid
      );
      if (!Number.isFinite(price)) continue;

      points.push({
        value: [startTick.ts, price],
        direction: dirRaw,
        symbolRotate: isUp ? 0 : 180,
      });
    }

    return points;
  }

  function buildPredictionPoints(ticksArr, predsArr) {
    if (!ticksArr.length || !predsArr.length) return [];

    const byId = new Map();
    for (const t of ticksArr) byId.set(Number(t.id), t);

    const points = [];

    for (const p of predsArr) {
      const startId = Number(p.start_id);
      let startTick = byId.get(startId);
      if (!startTick) {
        for (const t of ticksArr) {
          if (Number(t.id) >= startId) {
            startTick = t;
            break;
          }
        }
      }
      if (!startTick) continue;

      const price = Number(
        startTick.kal != null ? startTick.kal : startTick.mid
      );
      if (!Number.isFinite(price)) continue;

      const probaDown = Number(p.proba_down ?? 0);
      const probaNone = Number(p.proba_none ?? 0);
      const probaUp   = Number(p.proba_up ?? 0);
      const maxProba  = Math.max(probaDown, probaNone, probaUp);

      points.push({
        value: [startTick.ts, price],
        segId: Number(p.seg_id),
        startId,
        predLabel: Number(p.pred_label),
        probaDown,
        probaNone,
        probaUp,
        maxProba,
        runId: p.run_id,
      });
    }

    return points;
  }

  // ---------- Chart drawing ----------

  function rebuildChart() {
    const showKal   = chkKal   ? chkKal.checked   : true;
    const showZones = chkZones ? chkZones.checked : true;
    const showSegs  = chkSegs  ? chkSegs.checked  : true;
    const showPreds = chkPred  ? chkPred.checked  : true;

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
          minInterval: 1,      // only full-dollar grid lines
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

    const midSeries = ticks.map(t => [t.ts, Number(t.mid)]);
    const kalSeries = ticks.map(t =>
      t.kal != null ? [t.ts, Number(t.kal)] : [t.ts, Number(t.mid)]
    );

    const zoneBands     = showZones ? buildZoneBands(ticks, zones) : [];
    const segPoints     = showSegs  ? buildSegmentPoints(ticks, segs) : [];
    const predictionPts = showPreds ? buildPredictionPoints(ticks, predictions) : [];

    const series = [];

    // Zones
    if (showZones && zoneBands.length) {
      series.push({
        name: 'Zones',
        type: 'custom',
        renderItem: function (params, api) {
          const band = zoneBands[params.dataIndex];
          const xStart = api.coord([band.coord[0], band.coord[2]])[0];
          const xEnd   = api.coord([band.coord[1], band.coord[3]])[0];
          const yTop   = api.coord([band.coord[0], band.coord[3]])[1];
          const yBot   = api.coord([band.coord[0], band.coord[2]])[1];
          const width  = xEnd - xStart;
          const height = yBot - yTop;

          return {
            type: 'rect',
            shape: echarts.graphic.clipRectByRect(
              {
                x: width >= 0 ? xStart : xEnd,
                y: height >= 0 ? yTop : yBot,
                width: Math.abs(width),
                height: Math.abs(height),
              },
              {
                x: params.coordSys.x,
                y: params.coordSys.y,
                width: params.coordSys.width,
                height: params.coordSys.height,
              }
            ),
            style: api.style({ fill: band.itemStyle.color }),
          };
        },
        encode: { x: 0, y: 1 },
        data: zoneBands,
        z: 0,
        silent: true,
      });
    }

    // Mid line
    series.push({
      name: 'Mid',
      type: 'line',
      showSymbol: false,
      data: midSeries,
      lineStyle: { width: 1 },
      z: 1,
    });

    // Kalman
    if (showKal) {
      series.push({
        name: 'Kalman',
        type: 'line',
        showSymbol: false,
        data: kalSeries,
        lineStyle: { width: 1 },
        z: 2,
      });
    }

    // Segment arrows (Kalman direction changes)
    if (showSegs && segPoints.length) {
      series.push({
        name: 'Segments',
        type: 'scatter',
        symbol: 'triangle',
        symbolSize: 12,
        data: segPoints.map(p => ({
          value: p.value,
          direction: p.direction,
          symbolRotate: p.symbolRotate,
        })),
        encode: { x: 0, y: 1 },
        itemStyle: {
          color: function (param) {
            const dir = (param.data.direction || '').toString().toLowerCase();
            if (dir === 'up' || dir === '1' || dir === 'u')  return '#2ea043';
            if (dir === 'dn' || dir === '-1' || dir === 'down' || dir === 'd') return '#f85149';
            return '#8b949e';
          },
        },
        z: 3,
      });
    }

    // Prediction markers (snowball guesses)
    if (showPreds && predictionPts.length) {
      series.push({
        name: 'Predictions',
        type: 'scatter',
        symbol: 'triangle',
        // bigger marker for higher certainty (max probability)
        symbolSize: function (value, params) {
          const d = params.data;
          const p = Math.max(0, Math.min(1, d.maxProba || 0));
          // from 8px at 0.5 up to 18px at 1.0
          const base = 8;
          const extra = p > 0.5 ? (p - 0.5) * 20 : 0;
          return base + extra;
        },
        data: predictionPts.map(p => ({
          value: p.value,
          segId: p.segId,
          startId: p.startId,
          predLabel: p.predLabel,
          probaDown: p.probaDown,
          probaNone: p.probaNone,
          probaUp: p.probaUp,
          maxProba: p.maxProba,
          runId: p.runId,
          // rotate for up/down like arrows
          symbolRotate: p.predLabel < 0 ? 180 : 0,
        })),
        encode: { x: 0, y: 1 },
        itemStyle: {
          color: function (params) {
            const d = params.data;
            if (d.predLabel > 0) return '#2ea043'; // buy
            if (d.predLabel < 0) return '#f85149'; // sell
            return '#58a6ff';                      // "no strong move"
          },
        },
        z: 4,
      });
    }

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
          const ts = axis.axisValueLabel;
          const midPoint   = params.find(p => p.seriesName === 'Mid');
          const kalPoint   = params.find(p => p.seriesName === 'Kalman');
          const predPoint  = params.find(p => p.seriesName === 'Predictions');
          const tickDatum  = midPoint && ticks[midPoint.dataIndex];

          const lines = [];
          lines.push(ts);

          if (tickDatum) {
            lines.push(
              `ID: ${tickDatum.id}`,
            );
          }

          if (midPoint) {
            lines.push(`Mid: ${midPoint.data[1].toFixed(3)}`);
          }
          if (kalPoint) {
            lines.push(`Kalman: ${kalPoint.data[1].toFixed(3)}`);
          }

          if (predPoint && predPoint.data) {
            const d = predPoint.data;
            const label =
              d.predLabel > 0 ? 'UP'
              : d.predLabel < 0 ? 'DOWN'
              : 'NONE';
            lines.push(
              '',
              `Prediction (${label})`,
              `p_down: ${(d.probaDown * 100).toFixed(1)}%`,
              `p_none: ${(d.probaNone * 100).toFixed(1)}%`,
              `p_up:   ${(d.probaUp * 100).toFixed(1)}%`
            );
          }

          return lines.join('<br/>');
        },
      },
      legend: {
        show: true,
        top: 4,
        textStyle: { color: '#c9d1d9', fontSize: 11 },
        selected: {
          'Kalman': showKal,
          'Zones': showZones,
          'Segments': showSegs,
          'Predictions': showPreds,
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
        minInterval: 1,         // *** only whole-dollar horizontal grid lines
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

  // ---------- Loading windows + predictions ----------

  async function loadWindow(fromId, windowSize) {
    currentFromId = fromId;
    currentWindow = windowSize;

    if (fromIdInput) fromIdInput.value = String(fromId);
    if (windowInput) windowInput.value = String(windowSize);

    setLoading(true);
    try {
      const data = await fetchWindow(fromId, windowSize);

      ticks = (data.ticks || []).map(t => ({
        ...t,
        id: Number(t.id),
      }));
      segs  = data.segs  || [];
      zones = data.zones || [];
      predictions = [];

      if (!ticks.length) {
        setStatus(`No ticks for window from id ${fromId} (window ${windowSize})`);
        rebuildChart();
        return;
      }

      const firstId = ticks[0].id;
      const lastId  = ticks[ticks.length - 1].id;

      // Fetch predictions within same id range (best effort)
      try {
        predictions = await fetchPredictionsForRange(firstId, lastId);
      } catch (predErr) {
        console.error('Prediction fetch failed:', predErr);
        predictions = [];
      }

      setStatus(
        `Ticks ${firstId}–${lastId} (${ticks.length}), ` +
        `${segs.length} segs, ${zones.length} zones, ` +
        `${predictions.length} preds`
      );

      rebuildChart();
    } catch (err) {
      console.error(err);
      setStatus(`Error: ${err.message || err}`);
      ticks = [];
      segs = [];
      zones = [];
      predictions = [];
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

  function scheduleNextStep() {
    if (!autoPlay) return;

    if (loading) {
      // Wait a bit and try again
      playTimer = setTimeout(scheduleNextStep, 500);
      return;
    }

    handleNext();
    // Give some time for the user to see each window
    playTimer = setTimeout(scheduleNextStep, 1500);
  }

  function togglePlay() {
    autoPlay = !autoPlay;
    if (!playBtn) return;

    if (autoPlay) {
      playBtn.textContent = 'Stop';
      scheduleNextStep();
    } else {
      playBtn.textContent = 'Play';
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

  if (chkKal)   chkKal.addEventListener('change', rebuildChart);
  if (chkZones) chkZones.addEventListener('change', rebuildChart);
  if (chkSegs)  chkSegs.addEventListener('change', rebuildChart);
  if (chkPred)  chkPred.addEventListener('change', rebuildChart);

  window.addEventListener('resize', () => {
    chart.resize();
  });

  // Initial empty chart
  rebuildChart();
})();
