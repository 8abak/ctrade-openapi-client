(function () {
  const DEFAULTS = {
    mode: "live",
    run: "run",
    display: "ticks",
    series: "mid",
    id: "",
    reviewStart: "",
    reviewSpeed: 1,
    window: 2000,
  };

  const DISPLAY_CONFIG = {
    ticks: { label: "Ticks", maxWindow: 10000 },
    "ticks-zig": { label: "Ticks + Zig", maxWindow: 10000 },
    zig: { label: "Zig Only", maxWindow: 100000 },
  };

  const SERIES_CONFIG = {
    mid: { label: "Mid", color: "#6dd8ff" },
    ask: { label: "Ask", color: "#ffb35c" },
    bid: { label: "Bid", color: "#7ef0c7" },
  };

  const REVIEW_SPEEDS = [0.5, 1, 2, 3, 5];
  const ZOOM_COMPONENT_IDS = ["zoom-inside", "zoom-slider"];
  const MIN_CHART_WIDTH = 180;
  const MIN_CHART_HEIGHT = 180;

  const state = {
    chart: null,
    rows: [],
    zigRows: [],
    source: null,
    reviewTimer: 0,
    reviewEndId: null,
    loadToken: 0,
    lastMetrics: null,
    streamConnected: false,
    hasMoreLeft: false,
    viewport: null,
    lastDatasetBounds: null,
    applyingViewport: false,
    layoutResizeFrame: 0,
    layoutResizeTimeout: 0,
    resizeObserver: null,
    resizeBound: false,
    rangeFirstId: null,
    rangeLastId: null,
    rangeFirstTimestampMs: null,
    rangeLastTimestampMs: null,
    ui: {
      sidebarCollapsed: true,
      settingsCollapsed: true,
    },
  };

  const elements = {
    liveWorkspace: document.getElementById("liveWorkspace"),
    liveSidebar: document.getElementById("liveSidebar"),
    sidebarToggle: document.getElementById("sidebarToggle"),
    sidebarBackdrop: document.getElementById("sidebarBackdrop"),
    settingsToggle: document.getElementById("settingsToggle"),
    settingsSectionBody: document.getElementById("settingsSectionBody"),
    settingsToggleState: document.getElementById("settingsToggleState"),
    modeToggle: document.getElementById("modeToggle"),
    runToggle: document.getElementById("runToggle"),
    displayToggle: document.getElementById("displayToggle"),
    seriesToggle: document.getElementById("seriesToggle"),
    tickId: document.getElementById("tickId"),
    reviewStart: document.getElementById("reviewStart"),
    reviewSpeedToggle: document.getElementById("reviewSpeedToggle"),
    windowSize: document.getElementById("windowSize"),
    applyButton: document.getElementById("applyButton"),
    loadMoreLeftButton: document.getElementById("loadMoreLeftButton"),
    statusLine: document.getElementById("statusLine"),
    liveMeta: document.getElementById("liveMeta"),
    livePerf: document.getElementById("livePerf"),
    chartPanel: document.getElementById("chartPanel"),
    chartHost: document.getElementById("liveChart"),
  };

  function parseQuery() {
    const params = new URLSearchParams(window.location.search);
    const reviewSpeed = Number.parseFloat(params.get("speed") || String(DEFAULTS.reviewSpeed));
    const display = params.get("display");
    return {
      mode: params.get("mode") === "review" ? "review" : DEFAULTS.mode,
      run: params.get("run") === "stop" ? "stop" : DEFAULTS.run,
      display: Object.prototype.hasOwnProperty.call(DISPLAY_CONFIG, display) ? display : DEFAULTS.display,
      series: Object.prototype.hasOwnProperty.call(SERIES_CONFIG, params.get("series")) ? params.get("series") : DEFAULTS.series,
      id: params.get("id") || DEFAULTS.id,
      reviewStart: params.get("reviewStart") || DEFAULTS.reviewStart,
      reviewSpeed: REVIEW_SPEEDS.includes(reviewSpeed) ? reviewSpeed : DEFAULTS.reviewSpeed,
      window: sanitizeWindowValue(params.get("window"), Object.prototype.hasOwnProperty.call(DISPLAY_CONFIG, display) ? display : DEFAULTS.display),
    };
  }

  function sanitizeWindowValue(rawValue, displayMode) {
    const config = DISPLAY_CONFIG[displayMode] || DISPLAY_CONFIG[DEFAULTS.display];
    return Math.max(1, Math.min(config.maxWindow, Number.parseInt(rawValue || String(DEFAULTS.window), 10) || DEFAULTS.window));
  }

  function writeQuery() {
    const config = currentConfig();
    const params = new URLSearchParams();
    params.set("mode", config.mode);
    params.set("run", config.run);
    params.set("display", config.display);
    params.set("series", config.series);
    params.set("window", String(config.window));
    if (config.id) {
      params.set("id", config.id);
    }
    if (config.reviewStart) {
      params.set("reviewStart", config.reviewStart);
    }
    params.set("speed", String(config.reviewSpeed));
    window.history.replaceState({}, "", window.location.pathname + "?" + params.toString());
  }

  function bindSegment(container, handler) {
    container.querySelectorAll("button").forEach((button) => {
      button.addEventListener("click", () => handler(button.dataset.value));
    });
  }

  function setSegment(container, value) {
    container.querySelectorAll("button").forEach((button) => {
      button.classList.toggle("active", button.dataset.value === String(value));
    });
  }

  function currentConfig() {
    const display = elements.displayToggle.querySelector("button.active")?.dataset.value || DEFAULTS.display;
    return {
      mode: elements.modeToggle.querySelector("button.active")?.dataset.value || DEFAULTS.mode,
      run: elements.runToggle.querySelector("button.active")?.dataset.value || DEFAULTS.run,
      display,
      series: elements.seriesToggle.querySelector("button.active")?.dataset.value || DEFAULTS.series,
      id: (elements.tickId.value || "").trim(),
      reviewStart: (elements.reviewStart.value || "").trim(),
      reviewSpeed: Number.parseFloat(elements.reviewSpeedToggle.querySelector("button.active")?.dataset.value || String(DEFAULTS.reviewSpeed)),
      window: sanitizeWindowValue(elements.windowSize.value, display),
    };
  }

  function displayUsesTicks(displayMode) {
    return displayMode === "ticks" || displayMode === "ticks-zig";
  }

  function displayUsesZig(displayMode) {
    return displayMode === "ticks-zig" || displayMode === "zig";
  }

  function updateWindowConstraints() {
    const display = currentConfig().display;
    elements.windowSize.max = String(DISPLAY_CONFIG[display].maxWindow);
    elements.windowSize.value = String(sanitizeWindowValue(elements.windowSize.value, display));
  }

  function updateSeriesAvailability() {
    elements.seriesToggle.closest(".live-control-field").hidden = !displayUsesTicks(currentConfig().display);
  }

  function applyInitialConfig(config) {
    setSegment(elements.modeToggle, config.mode);
    setSegment(elements.runToggle, config.run);
    setSegment(elements.displayToggle, config.display);
    setSegment(elements.seriesToggle, config.series);
    setSegment(elements.reviewSpeedToggle, config.reviewSpeed);
    elements.tickId.value = config.id;
    elements.reviewStart.value = config.reviewStart;
    elements.windowSize.value = String(config.window);
    setSidebarCollapsed(true);
    setSettingsCollapsed(true);
    updateWindowConstraints();
    updateReviewFields();
    updateSeriesAvailability();
    renderMeta();
    renderPerf();
    writeQuery();
  }

  function setSidebarCollapsed(collapsed) {
    state.ui.sidebarCollapsed = Boolean(collapsed);
    elements.liveWorkspace.classList.toggle("is-sidebar-collapsed", state.ui.sidebarCollapsed);
    elements.sidebarToggle.setAttribute("aria-expanded", String(!state.ui.sidebarCollapsed));
    elements.sidebarToggle.setAttribute("aria-label", state.ui.sidebarCollapsed ? "Open live controls" : "Close live controls");
    elements.sidebarBackdrop.tabIndex = state.ui.sidebarCollapsed ? -1 : 0;
    queueChartResize();
  }

  function setSettingsCollapsed(collapsed) {
    state.ui.settingsCollapsed = Boolean(collapsed);
    elements.settingsSectionBody.classList.toggle("is-collapsed", state.ui.settingsCollapsed);
    elements.settingsToggle.setAttribute("aria-expanded", String(!state.ui.settingsCollapsed));
    elements.settingsToggleState.textContent = state.ui.settingsCollapsed ? "collapsed" : "open";
    queueChartResize();
  }

  function chartHostRect() {
    const rect = elements.chartHost.getBoundingClientRect();
    return {
      width: Math.round(rect.width),
      height: Math.round(rect.height),
    };
  }

  function chartHostHasSize() {
    const rect = chartHostRect();
    return rect.width >= MIN_CHART_WIDTH && rect.height >= MIN_CHART_HEIGHT;
  }

  function flushChartResize() {
    if (state.chart && chartHostHasSize()) {
      state.chart.resize();
    }
  }

  function queueChartResize() {
    if (state.layoutResizeFrame) {
      window.cancelAnimationFrame(state.layoutResizeFrame);
      state.layoutResizeFrame = 0;
    }
    if (state.layoutResizeTimeout) {
      window.clearTimeout(state.layoutResizeTimeout);
      state.layoutResizeTimeout = 0;
    }

    state.layoutResizeFrame = window.requestAnimationFrame(() => {
      state.layoutResizeFrame = 0;
      flushChartResize();
      window.requestAnimationFrame(() => {
        flushChartResize();
      });
    });

    state.layoutResizeTimeout = window.setTimeout(() => {
      state.layoutResizeTimeout = 0;
      flushChartResize();
    }, 220);
  }

  function bindResizeLifecycle() {
    if (!state.resizeObserver && typeof ResizeObserver === "function") {
      state.resizeObserver = new ResizeObserver(() => {
        queueChartResize();
      });
      [elements.liveWorkspace, elements.chartPanel, elements.chartHost].forEach((element) => {
        if (element) {
          state.resizeObserver.observe(element);
        }
      });
    }

    if (!state.resizeBound) {
      state.resizeBound = true;
      window.addEventListener("resize", () => {
        queueChartResize();
      });
      [elements.liveWorkspace, elements.liveSidebar, elements.chartPanel].forEach((element) => {
        if (!element) {
          return;
        }
        element.addEventListener("transitionend", () => {
          queueChartResize();
        });
      });
      if (document.fonts && typeof document.fonts.ready?.then === "function") {
        document.fonts.ready.then(() => {
          queueChartResize();
        }).catch(() => {
          queueChartResize();
        });
      }
    }
  }

  function updateReviewFields() {
    const reviewMode = currentConfig().mode === "review";
    elements.tickId.disabled = !reviewMode;
    elements.reviewStart.disabled = !reviewMode;
    elements.reviewSpeedToggle.querySelectorAll("button").forEach((button) => {
      button.disabled = !reviewMode;
    });
  }

  function status(message, isError) {
    elements.statusLine.textContent = message;
    elements.statusLine.classList.toggle("error", Boolean(isError));
  }

  function renderMeta() {
    if (state.rangeLastId == null) {
      elements.liveMeta.textContent = "No chart range loaded.";
      return;
    }
    const config = currentConfig();
    const level0Count = state.zigRows.filter((row) => (row.level ?? 0) >= 0).length;
    const level1Count = state.zigRows.filter((row) => (row.level ?? 0) >= 1).length;
    const level2Count = state.zigRows.filter((row) => (row.level ?? 0) >= 2).length;
    const level3Count = state.zigRows.filter((row) => (row.level ?? 0) >= 3).length;
    const candidateCount = state.zigRows.filter((row) => row.state === "candidate").length;
    const parts = [
      config.mode.toUpperCase(),
      DISPLAY_CONFIG[config.display].label,
      "left " + state.rangeFirstId,
      "right " + state.rangeLastId,
      "zig L0 " + level0Count,
      "L1 " + level1Count,
      "L2 " + level2Count,
      "L3 " + level3Count,
      "cand " + candidateCount,
      state.hasMoreLeft ? "more-left yes" : "more-left no",
    ];
    if (displayUsesTicks(config.display)) {
      parts.splice(2, 0, "ticks " + state.rows.length + "/" + config.window);
      parts.push("series " + config.series);
    } else {
      parts.splice(2, 0, "tick-window " + config.window);
    }
    elements.liveMeta.textContent = parts.join(" | ");
  }

  function renderPerf() {
    const metrics = state.lastMetrics || {};
    const parts = ["Stream " + (state.streamConnected ? "up" : "down")];
    if (metrics.dbLatestId != null) {
      parts.push("DB " + metrics.dbLatestId);
    }
    if (metrics.fetchLatencyMs != null) {
      parts.push("Fetch " + Math.round(metrics.fetchLatencyMs * 100) / 100 + "ms");
    }
    if (metrics.serializeLatencyMs != null) {
      parts.push("Serialize " + Math.round(metrics.serializeLatencyMs * 100) / 100 + "ms");
    }
    if (metrics.serverSentAtMs != null) {
      parts.push("Wire " + (Date.now() - metrics.serverSentAtMs) + "ms");
    }
    elements.livePerf.textContent = parts.join(" | ");
  }

  function ensureChart() {
    bindResizeLifecycle();
    if (!chartHostHasSize()) {
      return null;
    }

    if (!state.chart) {
      state.chart = echarts.init(elements.chartHost, null, { renderer: "canvas" });
      state.chart.setOption({
        animation: false,
        grid: { left: 54, right: 16, top: 14, bottom: 54 },
        tooltip: {
          trigger: "axis",
          axisPointer: { type: "cross" },
          valueFormatter: function (value) {
            return typeof value === "number" ? value.toFixed(2) : value;
          },
        },
        xAxis: { type: "time", axisLabel: { color: "#9eadc5" } },
        yAxis: { type: "value", scale: true, axisLabel: { color: "#9eadc5" } },
        dataZoom: [
          { id: "zoom-inside", type: "inside", filterMode: "none", zoomLock: false },
          {
            id: "zoom-slider",
            type: "slider",
            filterMode: "none",
            height: 20,
            bottom: 10,
            borderColor: "rgba(147, 181, 255, 0.12)",
            backgroundColor: "rgba(8, 13, 22, 0.92)",
            fillerColor: "rgba(109, 216, 255, 0.12)",
            handleStyle: { color: "#6dd8ff", borderColor: "#6dd8ff" },
          },
        ],
        series: [],
      }, { notMerge: true, lazyUpdate: true });

      state.chart.on("dataZoom", () => {
        if (state.applyingViewport) {
          return;
        }
        state.viewport = normalizeViewport(captureViewportFromChart(), getDatasetBounds());
      });
    }

    return state.chart;
  }

  function captureViewportFromChart() {
    if (!state.chart) {
      return null;
    }
    const option = state.chart.getOption();
    if (!option || !option.dataZoom || !option.dataZoom.length) {
      return null;
    }
    const zoom = option.dataZoom[0] || {};
    if (zoom.startValue == null || zoom.endValue == null) {
      return null;
    }
    return { startValue: Number(zoom.startValue), endValue: Number(zoom.endValue) };
  }

  function getDatasetBounds() {
    if (state.rows.length) {
      return { startValue: state.rows[0].timestampMs, endValue: state.rows[state.rows.length - 1].timestampMs };
    }
    if (state.zigRows.length) {
      return { startValue: state.zigRows[0].timestampMs, endValue: state.zigRows[state.zigRows.length - 1].timestampMs };
    }
    if (state.rangeFirstTimestampMs != null && state.rangeLastTimestampMs != null) {
      return { startValue: state.rangeFirstTimestampMs, endValue: state.rangeLastTimestampMs };
    }
    return null;
  }

  function fullViewport(bounds) {
    if (!bounds) {
      return null;
    }
    return { startValue: bounds.startValue, endValue: bounds.endValue, span: Math.max(0, bounds.endValue - bounds.startValue) };
  }

  function normalizeViewport(viewport, bounds) {
    if (!bounds) {
      return null;
    }
    if (!viewport) {
      return fullViewport(bounds);
    }
    let startValue = Number(viewport.startValue);
    let endValue = Number(viewport.endValue);
    if (!Number.isFinite(startValue) || !Number.isFinite(endValue)) {
      return fullViewport(bounds);
    }
    if (endValue < startValue) {
      const swap = startValue;
      startValue = endValue;
      endValue = swap;
    }
    const minValue = bounds.startValue;
    const maxValue = bounds.endValue;
    const fullSpan = Math.max(0, maxValue - minValue);
    let span = Math.max(0, endValue - startValue);
    if (span >= fullSpan) {
      return fullViewport(bounds);
    }
    if (startValue < minValue) {
      endValue += minValue - startValue;
      startValue = minValue;
    }
    if (endValue > maxValue) {
      startValue -= endValue - maxValue;
      endValue = maxValue;
    }
    startValue = Math.max(minValue, startValue);
    endValue = Math.min(maxValue, endValue);
    if (endValue < startValue) {
      endValue = startValue;
    }
    span = Math.max(0, endValue - startValue);
    if (span > fullSpan) {
      return fullViewport(bounds);
    }
    return { startValue, endValue, span };
  }

  function shiftViewportForward(viewport, previousBounds, nextBounds, shouldAdvance) {
    if (!shouldAdvance || !viewport || !previousBounds || !nextBounds) {
      return viewport;
    }
    const delta = nextBounds.endValue - previousBounds.endValue;
    if (!Number.isFinite(delta) || delta <= 0) {
      return viewport;
    }
    return {
      startValue: viewport.startValue + delta,
      endValue: viewport.endValue + delta,
      span: viewport.span,
    };
  }

  function buildDataZoomState(viewport) {
    return ZOOM_COMPONENT_IDS.map((id) => {
      const next = { id, filterMode: "none" };
      if (viewport) {
        next.startValue = viewport.startValue;
        next.endValue = viewport.endValue;
      }
      return next;
    });
  }

  function rowsToSeriesData(rows, valueKey) {
    return rows.map((row) => [row.timestampMs, row[valueKey]]);
  }

  function zigRowsAtLevel(level) {
    return state.zigRows.filter((row) => (row.level ?? 0) >= level);
  }

  function zigSeriesRows(level, stateName) {
    const rows = zigRowsAtLevel(level);
    if (!rows.length) {
      return [];
    }
    if (stateName === "final") {
      return rows.filter((row) => row.state !== "candidate");
    }
    const candidateRows = rows.filter((row) => row.state === "candidate");
    if (!candidateRows.length) {
      return [];
    }
    const lastCandidate = candidateRows[candidateRows.length - 1];
    const candidateIndex = rows.findIndex((row) => row.pivotId === lastCandidate.pivotId);
    if (candidateIndex > 0) {
      return [rows[candidateIndex - 1], lastCandidate];
    }
    return [lastCandidate];
  }

  function zigToSeriesData(level, stateName) {
    return zigSeriesRows(level, stateName).map((row) => [row.timestampMs, row.price]);
  }

  function buildChartSeries(config) {
    const series = [];
    if (displayUsesTicks(config.display)) {
      series.push({
        id: "raw-price",
        name: SERIES_CONFIG[config.series].label,
        type: "line",
        showSymbol: false,
        hoverAnimation: false,
        animation: false,
        connectNulls: false,
        data: rowsToSeriesData(state.rows, config.series),
        lineStyle: { color: SERIES_CONFIG[config.series].color, width: 1.45 },
      });
    }
    if (displayUsesZig(config.display) && state.zigRows.length) {
      [
        { level: 0, id: "fast-zig-l0", name: "Fast Zig L0", color: "#ffc857", border: "#f7e7b3", width: 1.6, symbolSize: 4 },
        { level: 1, id: "fast-zig-l1", name: "Fast Zig L1", color: "#ff8c42", border: "#ffd9b8", width: 2.0, symbolSize: 5 },
        { level: 2, id: "fast-zig-l2", name: "Fast Zig L2", color: "#ff4d6d", border: "#ffd3dc", width: 2.5, symbolSize: 6 },
        { level: 3, id: "fast-zig-l3", name: "Fast Zig L3", color: "#f8fafc", border: "#ffd166", width: 3.0, symbolSize: 8 },
      ].forEach((entry) => {
        [
          {
            stateName: "final",
            suffix: "final",
            lineType: "solid",
            opacity: 1,
            symbol: "circle",
            fillColor: entry.color,
            borderColor: entry.border,
          },
          {
            stateName: "candidate",
            suffix: "candidate",
            lineType: "dashed",
            opacity: 0.92,
            symbol: "emptyCircle",
            fillColor: "#0f172a",
            borderColor: entry.color,
          },
        ].forEach((variant) => {
          const data = zigToSeriesData(entry.level, variant.stateName);
          if (!data.length) {
            return;
          }
          series.push({
            id: entry.id + "-" + variant.suffix,
            name: entry.name + (variant.stateName === "candidate" ? " Candidate" : ""),
            type: "line",
            showSymbol: true,
            symbol: variant.symbol,
            symbolSize: entry.symbolSize,
            hoverAnimation: false,
            animation: false,
            connectNulls: false,
            z: 5 + entry.level,
            data: data,
            lineStyle: { color: entry.color, width: entry.width, type: variant.lineType, opacity: variant.opacity },
            itemStyle: {
              color: variant.fillColor,
              borderColor: variant.borderColor,
              borderWidth: 1,
              opacity: variant.opacity,
            },
          });
        });
      });
    }
    return series;
  }

  function renderChart(options) {
    const settings = options || {};
    const chart = ensureChart();
    if (!chart) {
      queueChartResize();
      return;
    }
    const datasetBounds = getDatasetBounds();
    const previousBounds = state.lastDatasetBounds;
    let nextViewport;
    if (!datasetBounds) {
      nextViewport = null;
    } else if (settings.resetView || !state.viewport) {
      nextViewport = fullViewport(datasetBounds);
    } else {
      nextViewport = normalizeViewport(
        shiftViewportForward(state.viewport, previousBounds, datasetBounds, Boolean(settings.shiftWithRun)),
        datasetBounds,
      );
    }

    state.applyingViewport = true;
    chart.setOption({
      series: buildChartSeries(currentConfig()),
      dataZoom: buildDataZoomState(nextViewport),
    }, { replaceMerge: ["series"], lazyUpdate: true });
    state.lastDatasetBounds = datasetBounds;

    window.requestAnimationFrame(() => {
      state.applyingViewport = false;
      state.viewport = normalizeViewport(captureViewportFromChart() || nextViewport, datasetBounds);
    });
  }

  function sortZigRows(rows) {
    return rows.slice().sort((left, right) => {
      if (left.pivotId !== right.pivotId) {
        return left.pivotId - right.pivotId;
      }
      return left.versionId - right.versionId;
    });
  }

  function trimRowsToWindow(anchor) {
    const windowSize = currentConfig().window;
    if (state.rows.length <= windowSize) {
      return;
    }
    if (anchor === "left") {
      state.rows = state.rows.slice(0, windowSize);
      return;
    }
    state.rows = state.rows.slice(state.rows.length - windowSize);
  }

  function trimZigRowsToCurrentRange() {
    if (!state.zigRows.length || state.rangeFirstId == null) {
      return;
    }
    const retained = state.zigRows.filter((row) => row.sourceTickId >= state.rangeFirstId);
    let leftNeighbor = null;
    for (let index = 0; index < state.zigRows.length; index += 1) {
      const row = state.zigRows[index];
      if (row.sourceTickId < state.rangeFirstId) {
        leftNeighbor = row;
      } else {
        break;
      }
    }
    state.zigRows = leftNeighbor ? [leftNeighbor].concat(retained) : retained;
  }

  function syncRangeFromRows() {
    if (!state.rows.length) {
      return;
    }
    state.rangeFirstId = state.rows[0].id;
    state.rangeLastId = state.rows[state.rows.length - 1].id;
    state.rangeFirstTimestampMs = state.rows[0].timestampMs;
    state.rangeLastTimestampMs = state.rows[state.rows.length - 1].timestampMs;
  }

  function applyRangePayload(payload) {
    if (payload.firstId != null) {
      state.rangeFirstId = payload.firstId;
    }
    if (payload.lastId != null) {
      state.rangeLastId = payload.lastId;
    }
    if (payload.firstTimestampMs != null) {
      state.rangeFirstTimestampMs = payload.firstTimestampMs;
    }
    if (payload.lastTimestampMs != null) {
      state.rangeLastTimestampMs = payload.lastTimestampMs;
    }
    if (state.rows.length) {
      syncRangeFromRows();
    }
  }

  function replaceRows(rows) {
    state.rows = Array.isArray(rows) ? rows.slice() : [];
    trimRowsToWindow("right");
    syncRangeFromRows();
  }

  function replaceZigRows(rows) {
    state.zigRows = sortZigRows(Array.isArray(rows) ? rows : []);
    if (displayUsesTicks(currentConfig().display)) {
      trimZigRowsToCurrentRange();
    }
  }

  function dedupeAppend(rows) {
    if (!rows.length) {
      return 0;
    }
    const lastId = state.rows.length ? state.rows[state.rows.length - 1].id : 0;
    let appended = 0;
    rows.forEach((row) => {
      if (row.id > lastId + appended) {
        state.rows.push(row);
        appended += 1;
      }
    });
    if (appended) {
      trimRowsToWindow("right");
      syncRangeFromRows();
    }
    return appended;
  }

  function dedupePrepend(rows) {
    if (!rows.length) {
      return 0;
    }
    const firstId = state.rows.length ? state.rows[0].id : Number.MAX_SAFE_INTEGER;
    const older = rows.filter((row) => row.id < firstId);
    if (!older.length) {
      return 0;
    }
    state.rows = older.concat(state.rows);
    trimRowsToWindow("left");
    syncRangeFromRows();
    return older.length;
  }

  function mergeZigChanges(rows) {
    if (!rows.length) {
      return 0;
    }
    const byPivotId = new Map();
    state.zigRows.forEach((row) => {
      byPivotId.set(row.pivotId, row);
    });
    rows.forEach((row) => {
      byPivotId.set(row.pivotId, row);
    });
    state.zigRows = sortZigRows(Array.from(byPivotId.values()));
    if (displayUsesTicks(currentConfig().display)) {
      trimZigRowsToCurrentRange();
    }
    return rows.length;
  }

  function clearActivity() {
    if (state.source) {
      state.source.close();
      state.source = null;
    }
    if (state.reviewTimer) {
      window.clearTimeout(state.reviewTimer);
      state.reviewTimer = 0;
    }
    state.streamConnected = false;
    renderPerf();
  }

  async function fetchJson(url, options) {
    const response = await fetch(url, options);
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(payload.detail || "Request failed.");
    }
    return payload;
  }

  async function resolveReviewStartId(config) {
    if (config.reviewStart) {
      const payload = await fetchJson("/api/live/review-start?" + new URLSearchParams({
        timestamp: config.reviewStart,
        timezoneName: "Australia/Sydney",
      }).toString());
      elements.tickId.value = String(payload.resolvedId);
      return payload.resolvedId;
    }
    if (config.id) {
      return Number.parseInt(config.id, 10);
    }
    throw new Error("Review mode requires a start id or Sydney review start time.");
  }

  function bootstrapUrl(config, startId) {
    const params = new URLSearchParams({
      mode: config.mode,
      window: String(config.window),
      display: config.display,
    });
    if (config.mode === "review" && startId != null) {
      params.set("id", String(startId));
    }
    return "/api/live/bootstrap?" + params.toString();
  }

  function nextUrl(config, afterId, endId, limit) {
    const params = new URLSearchParams({
      afterId: String(afterId),
      limit: String(limit),
      display: config.display,
    });
    if (endId != null) {
      params.set("endId", String(endId));
    }
    return "/api/live/next?" + params.toString();
  }

  function previousUrl(config) {
    return "/api/live/previous?" + new URLSearchParams({
      beforeId: String(state.rangeFirstId || 1),
      currentLastId: String(state.rangeLastId || state.rangeFirstId || 1),
      limit: String(historyBatchSize()),
      display: config.display,
    }).toString();
  }

  async function loadBootstrap(resetView) {
    const config = currentConfig();
    const startId = config.mode === "review" ? await resolveReviewStartId(config) : null;
    const payload = await fetchJson(bootstrapUrl(config, startId));
    replaceRows(payload.rows || []);
    replaceZigRows(payload.zigRows || []);
    applyRangePayload(payload);
    state.reviewEndId = payload.reviewEndId || null;
    state.hasMoreLeft = Boolean(payload.hasMoreLeft);
    state.lastMetrics = payload.metrics || null;
    state.lastDatasetBounds = null;
    state.viewport = null;
    renderMeta();
    renderPerf();
    renderChart({ resetView: Boolean(resetView) });

    const message = displayUsesTicks(config.display)
      ? "Loaded " + state.rows.length + " tick(s) and " + state.zigRows.length + " zig point(s)."
      : "Loaded " + state.zigRows.length + " zig point(s) over tick range " + state.rangeFirstId + "-" + state.rangeLastId + ".";
    status(message, false);

    if (config.run === "run") {
      if (config.mode === "live") {
        connectStream(state.rangeLastId || 0);
      } else {
        scheduleReviewStep();
      }
    }
  }

  function connectStream(afterId) {
    clearActivity();
    const config = currentConfig();
    const source = new EventSource("/api/live/stream?" + new URLSearchParams({
      afterId: String(afterId || 0),
      limit: "250",
      display: config.display,
    }).toString());
    state.source = source;

    source.onopen = function () {
      state.streamConnected = true;
      renderPerf();
      status("Live stream connected.", false);
    };

    source.onmessage = function (event) {
      const payload = JSON.parse(event.data);
      state.lastMetrics = payload;
      const tickAppended = displayUsesTicks(config.display) ? dedupeAppend(payload.rows || []) : 0;
      const zigChanged = displayUsesZig(config.display) ? mergeZigChanges(payload.zigChanges || []) : 0;
      if (!displayUsesTicks(config.display) && payload.lastId != null) {
        state.rangeLastId = payload.lastId;
      }
      renderMeta();
      renderPerf();
      if (tickAppended || zigChanged) {
        renderChart({ shiftWithRun: currentConfig().run === "run" });
      }
    };

    source.addEventListener("heartbeat", function (event) {
      state.lastMetrics = JSON.parse(event.data);
      renderPerf();
    });

    source.onerror = function () {
      state.streamConnected = false;
      renderPerf();
      status("Live stream disconnected. Click Load or Run to reconnect.", true);
      clearActivity();
    };
  }

  async function reviewStep() {
    const config = currentConfig();
    if (config.mode !== "review" || config.run !== "run") {
      return;
    }
    if (state.rangeLastId == null || !state.reviewEndId) {
      status("Review is waiting for a loaded range.", true);
      return;
    }
    if (state.rangeLastId >= state.reviewEndId) {
      status("Review reached the current end snapshot.", false);
      return;
    }

    const limit = Math.max(25, Math.min(500, Math.round(100 * config.reviewSpeed)));
    const payload = await fetchJson(nextUrl(config, state.rangeLastId, state.reviewEndId, limit));
    state.lastMetrics = payload.metrics || null;
    const tickAppended = displayUsesTicks(config.display) ? dedupeAppend(payload.rows || []) : 0;
    const zigChanged = displayUsesZig(config.display) ? mergeZigChanges(payload.zigChanges || []) : 0;
    if (!displayUsesTicks(config.display) && payload.lastId != null) {
      state.rangeLastId = payload.lastId;
    }
    renderMeta();
    renderPerf();
    if (tickAppended || zigChanged) {
      renderChart({ shiftWithRun: true });
    }
    status(payload.endReached ? "Review reached the current end snapshot." : "Review running.", false);
    if (!payload.endReached && currentConfig().run === "run") {
      scheduleReviewStep();
    }
  }

  function scheduleReviewStep() {
    if (state.reviewTimer) {
      window.clearTimeout(state.reviewTimer);
      state.reviewTimer = 0;
    }
    const delay = Math.max(80, Math.round(450 / currentConfig().reviewSpeed));
    state.reviewTimer = window.setTimeout(() => {
      state.reviewTimer = 0;
      reviewStep().catch((error) => {
        status(error.message || "Review fetch failed.", true);
      });
    }, delay);
  }

  function historyBatchSize() {
    const windowSize = currentConfig().window;
    return Math.max(1, Math.min(windowSize, Math.round(windowSize / 2)));
  }

  async function resumeRunIfNeeded() {
    const config = currentConfig();
    if (config.run !== "run" || state.rangeLastId == null) {
      return;
    }
    if (config.mode === "live") {
      connectStream(state.rangeLastId);
      return;
    }
    scheduleReviewStep();
  }

  async function loadMoreLeft() {
    if (state.rangeFirstId == null) {
      status("Load the chart first.", true);
      return;
    }

    clearActivity();
    const config = currentConfig();
    const payload = await fetchJson(previousUrl(config));
    state.lastMetrics = payload.metrics || null;
    const prepended = displayUsesTicks(config.display) ? dedupePrepend(payload.rows || []) : 0;
    if (!displayUsesTicks(config.display)) {
      state.rangeFirstId = payload.firstId;
      state.rangeLastId = payload.lastId;
    } else if (payload.firstId != null) {
      state.rangeFirstId = payload.firstId;
    }
    if (displayUsesZig(config.display)) {
      replaceZigRows(payload.zigRows || []);
    }
    state.hasMoreLeft = Boolean(payload.hasMoreLeft);
    renderMeta();
    renderPerf();
    if (prepended || displayUsesZig(config.display)) {
      renderChart({ shiftWithRun: false });
      status(
        displayUsesTicks(config.display)
          ? prepended + " older tick(s) merged into the current window."
          : "Older zig history merged into the current range.",
        false
      );
    } else {
      status("No older data was available.", false);
    }
    await resumeRunIfNeeded();
  }

  async function loadAll(resetView) {
    const token = state.loadToken + 1;
    state.loadToken = token;
    clearActivity();
    writeQuery();
    try {
      await loadBootstrap(resetView);
    } catch (error) {
      if (token === state.loadToken) {
        status(error.message || "Load failed.", true);
      }
    }
  }

  bindSegment(elements.modeToggle, function (value) {
    setSegment(elements.modeToggle, value);
    updateReviewFields();
    writeQuery();
    status("Mode updated. Click Load to refresh data.", false);
  });

  bindSegment(elements.runToggle, function (value) {
    setSegment(elements.runToggle, value);
    writeQuery();
    clearActivity();
    if (value === "run" && state.rangeLastId != null) {
      resumeRunIfNeeded();
      return;
    }
    status("Run state updated.", false);
  });

  bindSegment(elements.displayToggle, function (value) {
    setSegment(elements.displayToggle, value);
    updateWindowConstraints();
    updateSeriesAvailability();
    writeQuery();
    renderMeta();
    renderChart({ shiftWithRun: false });
    status("Display mode updated. Click Load to refresh the range.", false);
  });

  bindSegment(elements.seriesToggle, function (value) {
    setSegment(elements.seriesToggle, value);
    writeQuery();
    renderMeta();
    renderChart({ shiftWithRun: false });
  });

  bindSegment(elements.reviewSpeedToggle, function (value) {
    setSegment(elements.reviewSpeedToggle, value);
    writeQuery();
    if (currentConfig().mode === "review" && currentConfig().run === "run") {
      clearActivity();
      scheduleReviewStep();
    }
  });

  [elements.tickId, elements.reviewStart, elements.windowSize].forEach((control) => {
    control.addEventListener("change", function () {
      if (control === elements.windowSize) {
        updateWindowConstraints();
      }
      writeQuery();
    });
  });

  elements.sidebarToggle.addEventListener("click", function () {
    setSidebarCollapsed(!state.ui.sidebarCollapsed);
  });

  elements.sidebarBackdrop.addEventListener("click", function () {
    setSidebarCollapsed(true);
  });

  elements.settingsToggle.addEventListener("click", function () {
    setSettingsCollapsed(!state.ui.settingsCollapsed);
  });

  elements.applyButton.addEventListener("click", function () {
    loadAll(true);
  });

  elements.loadMoreLeftButton.addEventListener("click", function () {
    loadMoreLeft().catch((error) => {
      status(error.message || "Load More Left failed.", true);
    });
  });

  window.addEventListener("keydown", function (event) {
    if (event.key === "Escape" && !state.ui.sidebarCollapsed) {
      setSidebarCollapsed(true);
    }
  });

  const initialConfig = parseQuery();
  applyInitialConfig(initialConfig);
  loadAll(true);
}());
