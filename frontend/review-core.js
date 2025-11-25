// PATH: frontend/review-core.js
// Review window viewer (historical only, no live stream).
// Uses /api/review/window?from_id=...&window=...
//
// Response:
//   {
//     ticks: [{ id, ts, mid, kal, bid, ask, spread }, ...],
//     segs:  [{ id, start_id, end_id, direction }, ...],
//     zones: [{ id, start_id, end_id, direction, zone_type }, ...]
//   }

(function () {
  /* global echarts */

  const chartEl = document.getElementById('chart');
  const chart = echarts.init(chartEl);

  const fromIdInput = document.getElementById('fromId');
  const windowInput = document.getElementById('windowSize');
  const goBtn      = document.getElementById('btnGo');
  const prevBtn    = document.getElementById('btnPrev');
  const nextBtn    = document.getElementById('btnNext');
  const statusEl   = document.getElementById('status');

  const chkKal   = document.getElementById('showKal');
  const chkZones = document.getElementById('showZones');
  const chkSegs  = document.getElementById('showSegs');

  let ticks = [];
  let segs  = [];
  let zones = [];

  let currentFromId = null;
  let currentWindow = 5000;
  let loading       = false;

  function setStatus(text) {
    if (statusEl) statusEl.textContent = text || '';
  }

  function setLoading(isLoading) {
    loading = isLoading;
    if (goBtn)   goBtn.disabled = isLoading;
    if (prevBtn) prevBtn.disabled = isLoading;
    if (nextBtn) nextBtn.disabled = isLoading;
    if (isLoading) {
      setStatus('Loading...');
    }
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
      let color = 'rgba(56, 139, 253, 0.18)'; // default blue band
      if (dir === 'up' || dir === '1' || dir === 'u') {
        color = 'rgba(46, 160, 67, 0.18)'; // green-ish
      } else if (dir === 'dn' || dir === '-1' || dir === 'down' || dir === 'd') {
        color = 'rgba(248, 81, 73, 0.18)'; // red-ish
      }

      bands.push({
        name: z.zone_type || '',
        itemStyle: { color },
        coord: [tsStart, tsEnd, min, max],
      });
    }

    return bands;
  }

  // Segment arrows on Kalman line at segment START
  function buildSegmentPoints(ticksArr, segsArr) {
    if (!ticksArr.length || !segsArr.length) return [];

    const byId = new Map();
    for (const t of ticksArr) {
      byId.set(Number(t.id), t);
    }

    const points = [];

    for (const s of segsArr) {
      const startId = Number(s.start_id);

      // Try exact id, else first tick with id >= startId
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

      const price = Number(
        startTick.kal != null ? startTick.kal : startTick.mid
      );
      if (!Number.isFinite(price)) continue;

      points.push({
        value: [startTick.ts, price],
        direction: dirRaw,
        symbolRotate: isUp ? 0 : 180, // ▲ for up, ▼ for down
      });
    }

    return points;
  }

  function rebuildChart() {
    const showKal   = chkKal   ? chkKal.checked   : true;
    const showZones = chkZones ? chkZones.checked : true;
    const showSegs  = chkSegs  ? chkSegs.checked  : true;

    if (!ticks.length) {
      chart.setOption({
        backgroundColor: '#0d1117',
        animation: false,
        grid: {
          left: 60,
          right: 20,
          top: 40,
          bottom: 60,
        },
        xAxis: {
          type: 'time',
          axisLine: { lineStyle: { color: '#8b949e' } },
          axisLabel: { color: '#8b949e' },
          splitLine: { lineStyle: { color: '#30363d' } },
        },
        yAxis: {
          type: 'value',
          scale: true,
          axisLine: { lineStyle: { color: '#8b949e' } },
          axisLabel: { color: '#8b949e' },
          splitLine: { lineStyle: { color: '#30363d' } },
        },
        dataZoom: [
          {
            type: 'inside',
            throttle: 50,
          },
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

    const zoneBands = showZones ? buildZoneBands(ticks, zones) : [];
    const segPoints = showSegs ? buildSegmentPoints(ticks, segs) : [];

    const series = [];

    // Zones as rectangles
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
            style: api.style({
              fill: band.itemStyle.color,
            }),
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
      lineStyle: {
        width: 1,
      },
      z: 1,
    });

    // Kalman line
    if (showKal) {
      series.push({
        name: 'Kalman',
        type: 'line',
        showSymbol: false,
        data: kalSeries,
        lineStyle: {
          width: 1,
        },
        z: 2,
      });
    }

    // Segment arrows ON kalman line
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
            if (dir === 'up' || dir === '1' || dir === 'u') {
              return '#2ea043'; // green
            }
            if (dir === 'dn' || dir === '-1' || dir === 'down' || dir === 'd') {
              return '#f85149'; // red
            }
            return '#8b949e';   // grey fallback
          },
        },
        z: 3,
      });
    }

    const option = {
      backgroundColor: '#0d1117',
      animation: false,
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'cross' },
        // Custom formatter so we can show tick id + clean info
        formatter: function (params) {
          if (!params || !params.length) return '';

          // Prefer Mid series for anchor, fall back to first
          let p = params.find(x => x.seriesName === 'Mid') || params[0];
          const ts = p.value[0];

          // Find original tick by timestamp
          const tick = ticks.find(t => t.ts === ts);
          const dt = new Date(ts);

          const date = dt.toLocaleDateString();
          const time = dt.toLocaleTimeString();

          const fmt = v => (v == null ? '' : (+v).toFixed(3));

          const lines = [];
          if (tick && typeof tick.id !== 'undefined') {
            lines.push(`id: ${tick.id}`);
          }
          lines.push(`${date} ${time}`);

          // Show mid / kalman if available
          if (tick && tick.mid != null) {
            lines.push(`Mid\t${fmt(tick.mid)}`);
          }
          if (tick && tick.kal != null) {
            lines.push(`Kalman\t${fmt(tick.kal)}`);
          }

          return lines.join('<br/>');
        },
      },
      legend: {
        show: true,
        top: 4,
        textStyle: { color: '#c9d1d9', fontSize: 11 },
        selected: {
          Kalman: showKal,
          Segments: showSegs,
          Zones: showZones,
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
        axisLine: { lineStyle: { color: '#8b949e' } },
        axisLabel: {
          color: '#8b949e',
          // show clean whole numbers like 3361, 3362, ...
          formatter: value => value != null ? value.toFixed(0) : '',
        },
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
          handleStyle: {
            borderWidth: 1,
          },
        },
      ],
      series,
    };

    chart.setOption(option, true);
  }

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

      if (!ticks.length) {
        setStatus(`No ticks for window from id ${fromId} (window ${windowSize})`);
      } else {
        const firstId = ticks[0].id;
        const lastId  = ticks[ticks.length - 1].id;
        setStatus(
          `Ticks ${firstId}–${lastId} (${ticks.length}), ` +
          `${segs.length} segs, ${zones.length} zones`
        );
      }
      rebuildChart();
    } catch (err) {
      console.error(err);
      setStatus(`Error: ${err.message || err}`);
    } finally {
      setLoading(false);
    }
  }

  function handleGo() {
    const fromId = safeInt(fromIdInput && fromIdInput.value, null);
    const windowSize = safeInt(windowInput && windowInput.value, 5000);

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

  // --------- Wiring events ---------

  if (goBtn) {
    goBtn.addEventListener('click', () => {
      if (!loading) handleGo();
    });
  }
  if (prevBtn) {
    prevBtn.addEventListener('click', () => {
      if (!loading) handlePrev();
    });
  }
  if (nextBtn) {
    nextBtn.addEventListener('click', () => {
      if (!loading) handleNext();
    });
  }

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

  window.addEventListener('resize', () => {
    chart.resize();
  });

  // Initial empty chart
  rebuildChart();
})();
