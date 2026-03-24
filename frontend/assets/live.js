(function () {
  const DEFAULTS = {
    mode: "live",
    run: "run",
    id: "",
    window: 2000,
    series: ["mid"],
  };

  const SERIES_CONFIG = {
    ask: { label: "Ask", field: "ask", color: "#ffb35c", width: 1.35 },
    bid: { label: "Bid", field: "bid", color: "#7ef0c7", width: 1.35 },
    mid: { label: "Mid", field: "mid", color: "#6dd8ff", width: 2.0 },
  };

  const state = {
    rows: [],
    currentMode: DEFAULTS.mode,
    currentRun: DEFAULTS.run,
    visibleSpanMs: null,
    visibleWindow: null,
    activeSeries: { ask: false, bid: false, mid: true },
    source: null,
    chart: null,
  };

  const elements = {
    modeToggle: document.getElementById("modeToggle"),
    runToggle: document.getElementById("runToggle"),
    tickId: document.getElementById("tickId"),
    windowSize: document.getElementById("windowSize"),
    applyButton: document.getElementById("applyButton"),
    statusLine: document.getElementById("statusLine"),
    liveMeta: document.getElementById("liveMeta"),
    chartHost: document.getElementById("liveChart"),
    chartPanel: document.getElementById("chartPanel"),
    seriesSelector: document.getElementById("seriesSelector"),
    fullscreenButton: document.getElementById("fullscreenButton"),
  };

  function parseSeries(rawValue) {
    if (!rawValue) {
      return DEFAULTS.series.slice();
    }
    const selected = rawValue.split(",").map((item) => item.trim()).filter((item) => SERIES_CONFIG[item]);
    return selected.length ? Array.from(new Set(selected)) : DEFAULTS.series.slice();
  }

  function parseQuery() {
    const params = new URLSearchParams(window.location.search);
    const mode = params.get("mode") === "review" ? "review" : DEFAULTS.mode;
    const run = params.get("run") === "stop" ? "stop" : DEFAULTS.run;
    const id = params.get("id") || DEFAULTS.id;
    const windowSize = Number.parseInt(params.get("window"), 10);
    return {
      mode,
      run,
      id,
      window: Number.isFinite(windowSize) && windowSize > 0 ? windowSize : DEFAULTS.window,
      series: parseSeries(params.get("series")),
    };
  }

  function getActiveSeriesKeys() {
    return Object.keys(SERIES_CONFIG).filter((key) => state.activeSeries[key]);
  }

  function getPrimarySeriesKey() {
    if (state.activeSeries.mid) {
      return "mid";
    }
    return getActiveSeriesKeys()[0] || "mid";
  }

  function setSegment(container, value) {
    container.querySelectorAll("button").forEach((button) => {
      button.classList.toggle("active", button.dataset.value === value);
    });
  }

  function syncSeriesButtons() {
    elements.seriesSelector.querySelectorAll("button").forEach((button) => {
      button.classList.toggle("active", Boolean(state.activeSeries[button.dataset.series]));
    });
  }

  function syncControls(config) {
    state.currentMode = config.mode;
    state.currentRun = config.run;
    state.activeSeries = { ask: false, bid: false, mid: false };
    config.series.forEach((seriesKey) => {
      state.activeSeries[seriesKey] = true;
    });
    elements.tickId.value = config.id || "";
    elements.windowSize.value = String(config.window);
    setSegment(elements.modeToggle, config.mode);
    setSegment(elements.runToggle, config.run);
    syncSeriesButtons();
  }

  function currentConfig() {
    return {
      mode: state.currentMode,
      run: state.currentRun,
      id: elements.tickId.value.trim(),
      window: Math.max(1, Math.min(10000, Number.parseInt(elements.windowSize.value, 10) || DEFAULTS.window)),
      series: getActiveSeriesKeys(),
    };
  }

  function writeQuery(config) {
    const params = new URLSearchParams();
    params.set("mode", config.mode);
    params.set("run", config.run);
    params.set("window", String(config.window));
    params.set("series", config.series.join(","));
    if (config.id) {
      params.set("id", config.id);
    }
    history.replaceState(null, "", "/live?" + params.toString());
  }

  function status(text, isError) {
    elements.statusLine.textContent = text;
    elements.statusLine.classList.toggle("error", Boolean(isError));
  }

  function ensureChart() {
    if (!state.chart) {
      state.chart = echarts.init(elements.chartHost, null, { renderer: "canvas" });
      state.chart.on("datazoom", () => {
        updateVisibleZoomFromChart();
        applyVisibleYAxis();
      });
      window.addEventListener("resize", () => state.chart.resize());
      document.addEventListener("fullscreenchange", handleFullscreenChange);
    }
    return state.chart;
  }

  function handleFullscreenChange() {
    const isFullscreen = document.fullscreenElement === elements.chartPanel;
    elements.fullscreenButton.textContent = isFullscreen ? "Exit Fullscreen" : "Fullscreen";
    if (state.chart) {
      window.setTimeout(() => state.chart.resize(), 60);
    }
  }

  function readZoomWindowFromChart() {
    if (!state.chart || !state.rows.length) {
      return null;
    }
    const option = state.chart.getOption();
    const dataZoom = option.dataZoom && option.dataZoom[0];
    if (!dataZoom) {
      return null;
    }

    if (typeof dataZoom.startValue === "number" && typeof dataZoom.endValue === "number") {
      return clampZoomWindow({
        startMs: dataZoom.startValue,
        endMs: dataZoom.endValue,
      });
    }

    const firstTs = state.rows[0].timestampMs;
    const lastTs = state.rows[state.rows.length - 1].timestampMs;
    return clampZoomWindow({
      startMs: firstTs + (lastTs - firstTs) * ((dataZoom.start || 0) / 100),
      endMs: firstTs + (lastTs - firstTs) * ((dataZoom.end || 100) / 100),
    });
  }

  function clampZoomWindow(windowRange) {
    if (!state.rows.length) {
      return windowRange;
    }
    const firstTs = state.rows[0].timestampMs;
    const lastTs = state.rows[state.rows.length - 1].timestampMs;
    const startMs = Math.max(firstTs, Math.min(windowRange.startMs, lastTs));
    const endMs = Math.max(startMs, Math.min(windowRange.endMs, lastTs));
    return { startMs, endMs };
  }

  function updateVisibleZoomFromChart() {
    const zoomWindow = readZoomWindowFromChart();
    if (!zoomWindow) {
      return;
    }
    state.visibleWindow = zoomWindow;
    state.visibleSpanMs = Math.max(1000, zoomWindow.endMs - zoomWindow.startMs);
  }

  function visibleRows(windowRange) {
    if (!state.rows.length) {
      return [];
    }
    return state.rows.filter((row) => row.timestampMs >= windowRange.startMs && row.timestampMs <= windowRange.endMs);
  }

  function visibleYExtent(windowRange) {
    const selected = getActiveSeriesKeys();
    const rows = visibleRows(windowRange);
    const searchRows = rows.length ? rows : state.rows.slice(-1);
    let minPrice = Number.POSITIVE_INFINITY;
    let maxPrice = Number.NEGATIVE_INFINITY;

    searchRows.forEach((row) => {
      selected.forEach((seriesKey) => {
        const value = row[SERIES_CONFIG[seriesKey].field];
        if (typeof value === "number") {
          minPrice = Math.min(minPrice, value);
          maxPrice = Math.max(maxPrice, value);
        }
      });
    });

    if (!Number.isFinite(minPrice) || !Number.isFinite(maxPrice)) {
      minPrice = 0;
      maxPrice = 2;
    }

    let axisMin = Math.floor(minPrice) - 1;
    let axisMax = Math.ceil(maxPrice) + 1;
    if (axisMax <= axisMin) {
      axisMax = axisMin + 2;
    }
    return { min: axisMin, max: axisMax };
  }

  function gapAreas(rows) {
    const gaps = [];
    for (let index = 1; index < rows.length; index += 1) {
      const previous = rows[index - 1];
      const current = rows[index];
      if ((current.timestampMs - previous.timestampMs) >= 60000) {
        gaps.push([{ xAxis: previous.timestampMs }, { xAxis: current.timestampMs }]);
      }
    }
    return gaps;
  }

  function buildSeries(rows, selected) {
    const gaps = gapAreas(rows);
    const showFilledMid = selected.length === 1 && selected[0] === "mid";

    return selected.map((seriesKey) => {
      const config = SERIES_CONFIG[seriesKey];
      return {
        name: config.label,
        type: "line",
        showSymbol: false,
        smooth: false,
        data: rows.map((row) => [row.timestampMs, row[config.field]]),
        lineStyle: { width: config.width, color: config.color },
        areaStyle: showFilledMid && seriesKey === "mid" ? {
          color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
            { offset: 0, color: "rgba(109, 216, 255, 0.20)" },
            { offset: 1, color: "rgba(109, 216, 255, 0.02)" },
          ]),
        } : undefined,
        markArea: seriesKey === selected[0] && gaps.length ? {
          silent: true,
          itemStyle: { color: "rgba(255, 200, 87, 0.08)" },
          data: gaps,
        } : undefined,
      };
    });
  }

  function buildTooltipFormatter() {
    return function formatter(params) {
      if (!params.length) {
        return "";
      }
      const date = new Date(params[0].value[0]);
      const lines = [date.toLocaleString("en-AU", { hour12: false })];
      params.forEach((item) => {
        lines.push(item.seriesName + ": " + Number(item.value[1]).toFixed(2));
      });
      return lines.join("<br>");
    };
  }

  function buildMetaText() {
    if (!state.rows.length) {
      elements.liveMeta.textContent = "No rows returned.";
      return;
    }
    const primarySeries = getPrimarySeriesKey();
    const lastRow = state.rows[state.rows.length - 1];
    const price = lastRow[SERIES_CONFIG[primarySeries].field];
    elements.liveMeta.textContent = [
      "Rows " + state.rows.length,
      "Last id " + lastRow.id,
      "Price " + Number(price).toFixed(2),
      new Date(lastRow.timestampMs).toLocaleString("en-AU", { hour12: false }),
    ].join(" | ");
  }

  function determineTargetZoom(options) {
    const rows = state.rows;
    const firstTs = rows.length ? rows[0].timestampMs : Date.now() - 60000;
    const lastTs = rows.length ? rows[rows.length - 1].timestampMs : Date.now();
    const defaultSpan = lastTs > firstTs ? lastTs - firstTs : 60000;

    if (options && options.preserveCurrentZoom) {
      return clampZoomWindow(readZoomWindowFromChart() || state.visibleWindow || {
        startMs: Math.max(firstTs, lastTs - (state.visibleSpanMs || defaultSpan)),
        endMs: lastTs,
      });
    }

    if (!state.visibleSpanMs || (options && options.resetWindow)) {
      state.visibleSpanMs = Math.max(1000, defaultSpan);
    }

    return clampZoomWindow({
      startMs: Math.max(firstTs, lastTs - state.visibleSpanMs),
      endMs: lastTs,
    });
  }

  function buildYAxis(windowRange) {
    const extent = visibleYExtent(windowRange);
    return {
      type: "value",
      scale: false,
      min: extent.min,
      max: extent.max,
      minInterval: 1,
      splitNumber: 4,
      axisLabel: {
        color: "#9eadc5",
        formatter(value) {
          return String(Math.round(value));
        },
      },
      axisLine: { lineStyle: { color: "rgba(147, 181, 255, 0.24)" } },
      splitLine: { lineStyle: { color: "rgba(147, 181, 255, 0.06)" } },
    };
  }

  function applyVisibleYAxis() {
    if (!state.chart || !state.rows.length) {
      return;
    }
    const zoomWindow = readZoomWindowFromChart() || state.visibleWindow;
    if (!zoomWindow) {
      return;
    }
    state.chart.setOption({ yAxis: buildYAxis(zoomWindow) }, false);
  }

  function renderChart(options) {
    const chart = ensureChart();
    const selected = getActiveSeriesKeys();
    const targetZoom = determineTargetZoom(options || {});
    const firstTs = state.rows.length ? state.rows[0].timestampMs : Date.now() - 60000;
    const lastTs = state.rows.length ? state.rows[state.rows.length - 1].timestampMs : Date.now();

    state.visibleWindow = targetZoom;
    state.visibleSpanMs = Math.max(1000, targetZoom.endMs - targetZoom.startMs);

    chart.setOption({
      animation: false,
      backgroundColor: "transparent",
      grid: { left: 54, right: 18, top: 16, bottom: 84 },
      tooltip: {
        trigger: "axis",
        backgroundColor: "rgba(6, 11, 20, 0.96)",
        borderColor: "rgba(109, 216, 255, 0.24)",
        textStyle: { color: "#f3f6fb" },
        axisPointer: {
          type: "cross",
          lineStyle: { color: "rgba(109, 216, 255, 0.28)" },
        },
        formatter: buildTooltipFormatter(),
      },
      xAxis: {
        type: "time",
        min: firstTs,
        max: lastTs,
        axisLine: { lineStyle: { color: "rgba(147, 181, 255, 0.26)" } },
        axisLabel: {
          color: "#9eadc5",
          formatter(value) {
            return new Date(value).toLocaleTimeString("en-AU", {
              hour12: false,
              hour: "2-digit",
              minute: "2-digit",
              second: "2-digit",
            });
          },
        },
        splitLine: { lineStyle: { color: "rgba(147, 181, 255, 0.08)" } },
      },
      yAxis: buildYAxis(targetZoom),
      dataZoom: [
        {
          type: "inside",
          filterMode: "none",
          startValue: targetZoom.startMs,
          endValue: targetZoom.endMs,
        },
        {
          type: "slider",
          height: 42,
          bottom: 18,
          filterMode: "none",
          startValue: targetZoom.startMs,
          endValue: targetZoom.endMs,
          borderColor: "rgba(147, 181, 255, 0.16)",
          backgroundColor: "rgba(8, 13, 23, 0.94)",
          fillerColor: "rgba(109, 216, 255, 0.16)",
          dataBackground: {
            lineStyle: { color: "rgba(109, 216, 255, 0.48)" },
            areaStyle: { color: "rgba(109, 216, 255, 0.1)" },
          },
        },
      ],
      series: buildSeries(state.rows, selected),
    }, true);

    buildMetaText();
  }

  function closeStream() {
    if (state.source) {
      state.source.close();
      state.source = null;
    }
  }

  function connectStream(lastId, windowSize) {
    closeStream();
    if (state.currentRun !== "run") {
      return;
    }

    const params = new URLSearchParams({
      afterId: String(lastId || 0),
      limit: String(Math.max(50, Math.min(500, windowSize))),
    });
    const source = new EventSource("/api/live/stream?" + params.toString());
    state.source = source;

    source.onmessage = (event) => {
      const payload = JSON.parse(event.data);
      if (!payload.rows || !payload.rows.length) {
        return;
      }

      const seen = new Set(state.rows.map((row) => row.id));
      payload.rows.forEach((row) => {
        if (!seen.has(row.id)) {
          state.rows.push(row);
        }
      });

      const maxBuffer = Math.max(windowSize * 5, 5000);
      if (state.rows.length > maxBuffer) {
        state.rows = state.rows.slice(state.rows.length - maxBuffer);
      }

      renderChart({ preserveCurrentZoom: false });
      status("Streaming " + payload.rowCount + " new row(s).", false);
    };

    source.onerror = () => {
      status("Stream interrupted. Reconnecting...", true);
    };
  }

  async function loadData(resetWindow) {
    closeStream();
    const config = currentConfig();
    writeQuery(config);
    const params = new URLSearchParams({
      mode: config.mode,
      window: String(config.window),
    });
    if (config.mode === "review" && config.id) {
      params.set("id", config.id);
    }

    status("Loading chart...", false);
    try {
      const response = await fetch("/api/live/bootstrap?" + params.toString());
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload.detail || "Bootstrap request failed.");
      }

      state.rows = payload.rows || [];
      renderChart({ resetWindow: Boolean(resetWindow) });
      status("Loaded " + payload.rowCount + " row(s).", false);
      if (config.run === "run") {
        connectStream(payload.lastId || 0, config.window);
      }
    } catch (error) {
      status(error.message || "Live bootstrap failed.", true);
    }
  }

  function bindSegment(container, handler) {
    container.querySelectorAll("button").forEach((button) => {
      button.addEventListener("click", () => handler(button.dataset.value));
    });
  }

  function toggleSeries(seriesKey) {
    const activeKeys = getActiveSeriesKeys();
    if (state.activeSeries[seriesKey] && activeKeys.length === 1) {
      return;
    }
    state.activeSeries[seriesKey] = !state.activeSeries[seriesKey];
    syncSeriesButtons();
    writeQuery(currentConfig());
    renderChart({ preserveCurrentZoom: true });
  }

  async function toggleFullscreen() {
    if (document.fullscreenElement === elements.chartPanel) {
      await document.exitFullscreen();
      return;
    }
    if (elements.chartPanel.requestFullscreen) {
      await elements.chartPanel.requestFullscreen();
    }
  }

  bindSegment(elements.modeToggle, (value) => {
    state.currentMode = value;
    setSegment(elements.modeToggle, value);
  });

  bindSegment(elements.runToggle, (value) => {
    state.currentRun = value;
    setSegment(elements.runToggle, value);
    if (value === "stop") {
      closeStream();
      status("Streaming stopped.", false);
    }
  });

  elements.seriesSelector.querySelectorAll("button").forEach((button) => {
    button.addEventListener("click", () => toggleSeries(button.dataset.series));
  });

  elements.fullscreenButton.addEventListener("click", toggleFullscreen);
  elements.applyButton.addEventListener("click", () => loadData(true));

  const initial = parseQuery();
  syncControls(initial);
  loadData(true);
}());
