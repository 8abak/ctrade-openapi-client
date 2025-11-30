// PATH: frontend/review-core.js
// Review window viewer with:
//   - bid/ask/mid + kal/kal_fast/kal_slow (spread + residuals optional)
//   - zones, segments
//   - snowball predictions (latest SGD run) drawn on price
//   - Run/Stop auto-scroll over historical data (tick-by-tick)
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

  // price / kalman toggles
  const chkBid        = document.getElementById('showBid');
  const chkAsk        = document.getElementById('showAsk');
  const chkMid        = document.getElementById('showMid');
  const chkKal        = document.getElementById('showKal');
  const chkKalFast    = document.getElementById('showKalFast');
  const chkKalSlow    = document.getElementById('showKalSlow');
  const chkSpread     = document.getElementById('showSpread');
  const chkFastResid  = document.getElementById('showFastResid');
  const chkSlowResid  = document.getElementById('showSlowResid');

  // overlays
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

  // Optional labels for personalities
  const personalityLabels = {
    '-1': 'MIXED_LONG',
    '0':  'UP_CLEAN_FAST',
    '1':  'DOWN_CLEAN_FAST',
    '2':  'SMALL_NOISY_CHOP',
  };

  // ---------- Helpers ----------

  function setStatus(text) {
    if (statusEl) statusEl.textContent = text || '';
  }

  function setLoading(isLoading) {
    loading = isLoading;
    if (goBtn)   goBtn.disabled   = isLoading;
    if (prevBtn) prevBtn.disabled = isLoading;
    if (nextBtn) nextBtn.disabled = isLoading;
    // while running we still want the button enabled so user can Stop
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

  // --- predictions: latest SGD run, restricted by tick-id range ---
  async function fetchPredictionsForRange(startId, endId) {
    const sql = `
      SELECT
        seg_id,
        start_id,
        pred_label,
        proba_down,
        proba_none,
        proba_up,
        run_id
      FROM kalseg_prediction
      WHERE run_id = (
        SELECT run_id
        FROM kalseg_prediction
        WHERE run_id LIKE 'sgd-%'
        ORDER BY id DESC
        LIMIT 1
      )
      AND start_id BETWEEN ${startId} AND ${endId}
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

    if (!data) return [];
    if (Array.isArray(data.rows))   return data.rows;
    if (Array.isArray(data.data))   return data.data;
    if (Array.isArray(data.result)) return data.result;
    if (Array.isArray(data))        return data;
    return [];
  }

  // ---------- Zone bands with personality-based colors ----------
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

      // --- Personality + zone-type based colour selection ---
      const clusterRaw = z.personality_cluster;
      const clusterId  = (clusterRaw === undefined || clusterRaw === null)
        ? null
        : Number(clusterRaw);

      const zoneTypeRaw   = (z.zone_type || z.zone_type_code || '').toString();
      const zoneTypeUpper = zoneTypeRaw.toUpperCase();

      let color;
      let labelName = '';

      if (clusterId !== null && personalityLabels[String(clusterId)]) {
        labelName = personalityLabels[String(clusterId)];
      } else if (zoneTypeUpper) {
        labelName = zoneTypeUpper;
      }

      if (zoneTypeUpper.startsWith('CHOP')) {
        color = 'rgba(255, 255, 255, 0.10)';
      } else if (clusterId === 0) {
        color = 'rgba(46, 160, 67, 0.18)';
      } else if (clusterId === 1) {
        color = 'rgba(248, 81, 73, 0.18)';
      } else if (clusterId === 2) {
        color = 'rgba(255, 255, 255, 0.10)';
      } else if (clusterId === -1) {
        color = 'rgba(99, 110, 139, 0.16)';
      } else {
        const dir = (z.direction || '').toString().toLowerCase();
        if (dir === 'up' || dir === '1' || dir === 'u') {
          color = 'rgba(46, 160, 67, 0.18)';
        } else if (dir === 'dn' || dir === '-1' || dir === 'down' || dir === 'd') {
          color = 'rgba(248, 81, 73, 0.18)';
        } else {
          color = 'rgba(56, 139, 253, 0.18)';
        }
      }

      bands.push({
        name: labelName,
        personality_cluster: clusterId,
        zone_type: zoneTypeRaw,
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
    const showBid       = chkBid      ? chkBid.checked      : true;
    const showAsk       = chkAsk      ? chkAsk.checked      : true;
    const showMid       = chkMid      ? chkMid.checked      : true;
    const showKal       = chkKal      ? chkKal.checked      : true;
    const showKalFast   = chkKalFast  ? chkKalFast.checked  : true;
    const showKalSlow   = chkKalSlow  ? chkKalSlow.checked  : true;
    const showSpread    = chkSpread   ? chkSpread.checked   : false;
    const showFastResid = chkFastResid? chkFastResid.checked: false;
    const showSlowResid = chkSlowResid? chkSlowResid.checked: false;

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

    // Build series data arrays
    const bidSeries       = ticks.map(t => [t.ts, Number(t.bid)]);
    const askSeries       = ticks.map(t => [t.ts, Number(t.ask)]);
    const midSeries       = ticks.map(t => [t.ts, Number(t.mid)]);
    const kalSeries       = ticks.map(t =>
      t.kal != null ? [t.ts, Number(t.kal)] : [t.ts, NaN]
    );
    const kalFastSeries   = ticks.map(t =>
      t.kal_fast != null ? [t.ts, Number(t.kal_fast)] : [t.ts, NaN]
    );
    const kalSlowSeries   = ticks.map(t =>
      t.kal_slow != null ? [t.ts, Number(t.kal_slow)] : [t.ts, NaN]
    );
    const spreadSeries    = ticks.map(t =>
      t.spread != null ? [t.ts, Number(t.spread)] : [t.ts, NaN]
    );
    const fastResidSeries = ticks.map(t =>
      t.kal_fast_resid != null ? [t.ts, Number(t.kal_fast_resid)] : [t.ts, NaN]
    );
    const slowResidSeries = ticks.map(t =>
      t.kal_slow_resid != null ? [t.ts, Number(t.kal_slow_resid)] : [t.ts, NaN]
    );

    const zoneBands     = showZones ? buildZoneBands(ticks, zones) : [];
    const segPoints     = showSegs  ? buildSegmentPoints(ticks, segs) : [];
    const predictionPts = showPreds ? buildPredictionPoints(ticks, predictions) : [];

    const series = [];

    // Zones as background rectangles
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

    // Price + kalman lines
    function pushLine(name, data, z) {
      series.push({
        name,
        type: 'line',
        showSymbol: false,
        data,
        lineStyle: { width: 1 },
        z,
      });
    }

    let zLevel = 1;
    if (showBid)       pushLine('Bid',       bidSeries,       zLevel++);
    if (showAsk)       pushLine('Ask',       askSeries,       zLevel++);
    if (showMid)       pushLine('Mid',       midSeries,       zLevel++);
    if (showKal)       pushLine('Kalman',    kalSeries,       zLevel++);
    if (showKalFast)   pushLine('Kal Fast',  kalFastSeries,   zLevel++);
    if (showKalSlow)   pushLine('Kal Slow',  kalSlowSeries,   zLevel++);
    if (showSpread)    pushLine('Spread',    spreadSeries,    zLevel++);
    if (showFastResid) pushLine('Fast Resid',fastResidSeries, zLevel++);
    if (showSlowResid) pushLine('Slow Resid',slowResidSeries, zLevel++);

    // Segment markers
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
        z: zLevel++,
      });
    }

    // Prediction markers
    if (showPreds && predictionPts.length) {
      series.push({
        name: 'Predictions',
        type: 'scatter',
        symbol: 'triangle',
        symbolSize: function (value, params) {
          const d = params.data;
          const p = Math.max(0, Math.min(1, d.maxProba || 0));
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
          symbolRotate: p.predLabel < 0 ? 180 : 0,
        })),
        encode: { x: 0, y: 1 },
        itemStyle: {
          color: function (params) {
            const d = params.data;
            if (d.predLabel > 0) return '#2ea043'; // buy
            if (d.predLabel < 0) return '#f85149'; // sell
            return '#58a6ff';                      // none / flat
          },
        },
        z: zLevel++,
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
          const tsLabel = axis.axisValueLabel;
          const dataIndex = axis.dataIndex;
          const tickDatum = ticks[dataIndex];

          const byName = {};
          for (const p of params) {
            byName[p.seriesName] = p;
          }

          const lines = [];
          lines.push(tsLabel);

          if (tickDatum) {
            lines.push(`ID: ${tickDatum.id}`);
          }

          function addVal(seriesName, label) {
            const p = byName[seriesName];
            if (!p || !p.data || p.data[1] == null || !Number.isFinite(p.data[1])) return;
            lines.push(`${label}: ${p.data[1].toFixed(3)}`);
          }

          addVal('Bid',        'Bid');
          addVal('Ask',        'Ask');
          addVal('Mid',        'Mid');
          addVal('Kalman',     'Kal');
          addVal('Kal Fast',   'Kal Fast');
          addVal('Kal Slow',   'Kal Slow');
          addVal('Spread',     'Spread');
          addVal('Fast Resid', 'Fast Resid');
          addVal('Slow Resid', 'Slow Resid');

          const predPoint = byName['Predictions'];
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
          'Bid':        showBid,
          'Ask':        showAsk,
          'Mid':        showMid,
          'Kalman':     showKal,
          'Kal Fast':   showKalFast,
          'Kal Slow':   showKalSlow,
          'Spread':     showSpread,
          'Fast Resid': showFastResid,
          'Slow Resid': showSlowResid,
          'Zones':      showZones,
          'Segments':   showSegs,
          'Predictions':showPreds,
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
        bid: t.bid != null ? Number(t.bid) : t.bid,
        ask: t.ask != null ? Number(t.ask) : t.ask,
        mid: t.mid != null ? Number(t.mid) : t.mid,
        kal: t.kal != null ? Number(t.kal) : t.kal,
        kal_fast: t.kal_fast != null ? Number(t.kal_fast) : t.kal_fast,
        kal_slow: t.kal_slow != null ? Number(t.kal_slow) : t.kal_slow,
        spread: t.spread != null ? Number(t.spread) : t.spread,
        kal_fast_resid: t.kal_fast_resid != null ? Number(t.kal_fast_resid) : t.kal_fast_resid,
        kal_slow_resid: t.kal_slow_resid != null ? Number(t.kal_slow_resid) : t.kal_slow_resid,
      }));
      segs  = data.segs  || [];
      zones = data.zones || [];
      predictions = [];

      if (!ticks.length) {
        setStatus(`No ticks for window from id ${fromId} (window ${windowSize})`);
        rebuildChart();

        // If we were auto-playing and hit the end, stop cleanly.
        if (autoPlay) {
          autoPlay = false;
          if (playBtn) playBtn.textContent = 'Run';
          stopPlayTimer();
        }
        return;
      }

      const firstId = ticks[0].id;
      const lastId  = ticks[ticks.length - 1].id;

      try {
        predictions = await fetchPredictionsForRange(firstId, lastId);
      } catch (predErr) {
        console.error('Prediction fetch failed:', predErr);
        predictions = [];
      }

      const runId = predictions.length ? predictions[0].run_id : null;

      setStatus(
        `Ticks ${firstId}–${lastId} (${ticks.length}), ` +
        `${segs.length} segs, ${zones.length} zones, ` +
        `${predictions.length} preds${runId ? ' [' + runId + ']' : ''}`
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

  // auto-play: slide one tick at a time, keeping same window size
  function scheduleNextStep() {
    if (!autoPlay) return;

    if (loading) {
      // Wait a bit and try again
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
    const nextFrom = currentFromId + 1; // one tick shift
    loadWindow(nextFrom, windowSize);

    // Wait a little so user can see motion
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

  // Toggle handlers – redraw when any checkbox changes
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
  hookToggle(chkSpread);
  hookToggle(chkFastResid);
  hookToggle(chkSlowResid);
  hookToggle(chkZones);
  hookToggle(chkSegs);
  hookToggle(chkPred);

  window.addEventListener('resize', () => {
    chart.resize();
  });

  // Initial empty chart
  rebuildChart();
})();
