(function () {
  const DEFAULTS = {
    mode: "live",
    run: "run",
    series: "mid",
    id: "",
    reviewStart: "",
    reviewSpeed: 1,
    window: 2000,
  };

  const SERIES_CONFIG = {
    mid: { label: "Mid", color: "#6dd8ff" },
    ask: { label: "Ask", color: "#ffb35c" },
    bid: { label: "Bid", color: "#7ef0c7" },
  };

  const REVIEW_SPEEDS = [0.5, 1, 2, 3, 5];
  const MAX_WINDOW = 10000;
  const ZOOM_COMPONENT_IDS = ["zoom-inside", "zoom-slider"];

  const state = {
    chart: null,
    rows: [],
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
    optionalSeries: new Map(),
    ui: {
      sidebarCollapsed: true,
      settingsCollapsed: true,
    },
  };

  const elements = {
    liveWorkspace: document.getElementById("liveWorkspace"),
    sidebarToggle: document.getElementById("sidebarToggle"),
    sidebarBackdrop: document.getElementById("sidebarBackdrop"),
    settingsToggle: document.getElementById("settingsToggle"),
    settingsSectionBody: document.getElementById("settingsSectionBody"),
    settingsToggleState: document.getElementById("settingsToggleState"),
    modeToggle: document.getElementById("modeToggle"),
    runToggle: document.getElementById("runToggle"),
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
    chartHost: document.getElementById("liveChart"),
  };

  function parseQuery() {
    const params = new URLSearchParams(window.location.search);
    const reviewSpeed = Number.parseFloat(params.get("speed") || String(DEFAULTS.reviewSpeed));
    return {
      mode: params.get("mode") === "review" ? "review" : DEFAULTS.mode,
      run: params.get("run") === "stop" ? "stop" : DEFAULTS.run,
      series: Object.prototype.hasOwnProperty.call(SERIES_CONFIG, params.get("series")) ? params.get("series") : DEFAULTS.series,
      id: params.get("id") || DEFAULTS.id,
      reviewStart: params.get("reviewStart") || DEFAULTS.reviewStart,
      reviewSpeed: REVIEW_SPEEDS.includes(reviewSpeed) ? reviewSpeed : DEFAULTS.reviewSpeed,
      window: sanitizeWindowValue(params.get("window")),
    };
  }

  function sanitizeWindowValue(rawValue) {
    return Math.max(1, Math.min(MAX_WINDOW, Number.parseInt(rawValue || String(DEFAULTS.window), 10) || DEFAULTS.window));
  }

  function writeQuery() {
    const config = currentConfig();
    const params = new URLSearchParams();
    params.set("mode", config.mode);
    params.set("run", config.run);
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
    return {
      mode: elements.modeToggle.querySelector("button.active")?.dataset.value || DEFAULTS.mode,
      run: elements.runToggle.querySelector("button.active")?.dataset.value || DEFAULTS.run,
      series: elements.seriesToggle.querySelector("button.active")?.dataset.value || DEFAULTS.series,
      id: (elements.tickId.value || "").trim(),
      reviewStart: (elements.reviewStart.value || "").trim(),
      reviewSpeed: Number.parseFloat(elements.reviewSpeedToggle.querySelector("button.active")?.dataset.value || String(DEFAULTS.reviewSpeed)),
      window: sanitizeWindowValue(elements.windowSize.value),
    };
  }

  function applyInitialConfig(config) {
    setSegment(elements.modeToggle, config.mode);
    setSegment(elements.runToggle, config.run);
    setSegment(elements.seriesToggle, config.series);
    setSegment(elements.reviewSpeedToggle, config.reviewSpeed);
    elements.tickId.value = config.id;
    elements.reviewStart.value = config.reviewStart;
    elements.windowSize.value = String(config.window);
    setSidebarCollapsed(true);
    setSettingsCollapsed(true);
    updateReviewFields();
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
  }

  function queueChartResize() {
    if (!state.chart) {
      return;
    }
    state.chart.resize();
    window.setTimeout(() => {
      if (state.chart) {
        state.chart.resize();
      }
    }, 220);
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
    if (!state.rows.length) {
      elements.liveMeta.textContent = "No ticks loaded.";
      return;
    }
    const config = currentConfig();
    const first = state.rows[0];
    const last = state.rows[state.rows.length - 1];
    elements.liveMeta.textContent = [
      config.mode.toUpperCase(),
      "loaded " + state.rows.length + "/" + config.window,
      "left " + first.id,
      "right " + last.id,
      "series " + config.series,
      state.hasMoreLeft ? "more-left yes" : "more-left no",
    ].join(" | ");
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
        xAxis: {
          type: "time",
          axisLabel: { color: "#9eadc5" },
        },
        yAxis: {
          type: "value",
          scale: true,
          axisLabel: { color: "#9eadc5" },
        },
        dataZoom: [
          {
            id: "zoom-inside",
            type: "inside",
            filterMode: "none",
            zoomLock: false,
          },
          {
            id: "zoom-slider",
            type: "slider",
            filterMode: "none",
            height: 20,
            bottom: 10,
            borderColor: "rgba(147, 181, 255, 0.12)",
            backgroundColor: "rgba(8, 13, 22, 0.92)",
            fillerColor: "rgba(109, 216, 255, 0.12)",
            handleStyle: {
              color: "#6dd8ff",
              borderColor: "#6dd8ff",
            },
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

      window.addEventListener("resize", () => {
        if (state.chart) {
          state.chart.resize();
        }
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
    return {
      startValue: Number(zoom.startValue),
      endValue: Number(zoom.endValue),
    };
  }

  function getDatasetBounds() {
    if (!state.rows.length) {
      return null;
    }
    return {
      startValue: state.rows[0].timestampMs,
      endValue: state.rows[state.rows.length - 1].timestampMs,
    };
  }

  function fullViewport(bounds) {
    if (!bounds) {
      return null;
    }
    return {
      startValue: bounds.startValue,
      endValue: bounds.endValue,
      span: Math.max(0, bounds.endValue - bounds.startValue),
    };
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

    return {
      startValue,
      endValue,
      span,
    };
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
      const next = {
        id,
        filterMode: "none",
      };
      if (viewport) {
        next.startValue = viewport.startValue;
        next.endValue = viewport.endValue;
      }
      return next;
    });
  }

  function rowsToSeriesData(rows, valueKey) {
    const data = new Array(rows.length);
    for (let index = 0; index < rows.length; index += 1) {
      const row = rows[index];
      data[index] = [row.timestampMs, row[valueKey]];
    }
    return data;
  }

  function normalizeOptionalSeriesDefinition(definition) {
    if (!definition || typeof definition !== "object") {
      return null;
    }
    const id = String(definition.id || "").trim();
    if (!id || !Array.isArray(definition.data)) {
      return null;
    }
    return {
      id,
      label: String(definition.label || id),
      type: definition.type === "line" ? "line" : "line",
      color: typeof definition.color === "string" && definition.color ? definition.color : "#ffc857",
      lineWidth: Number.isFinite(definition.lineWidth) ? Math.max(1, definition.lineWidth) : 1.2,
      data: definition.data,
    };
  }

  function syncOptionalSeries(payload, reset) {
    if (reset) {
      state.optionalSeries.clear();
    }
    if (!payload || !Array.isArray(payload.indicatorSeries)) {
      return;
    }

    const nextSeries = new Map();
    payload.indicatorSeries.forEach((definition) => {
      const normalized = normalizeOptionalSeriesDefinition(definition);
      if (normalized) {
        nextSeries.set(normalized.id, normalized);
      }
    });
    state.optionalSeries = nextSeries;
  }

  function buildChartSeries(config) {
    const series = [
      {
        id: "raw-price",
        name: SERIES_CONFIG[config.series].label,
        type: "line",
        showSymbol: false,
        hoverAnimation: false,
        animation: false,
        connectNulls: false,
        data: rowsToSeriesData(state.rows, config.series),
        lineStyle: {
          color: SERIES_CONFIG[config.series].color,
          width: 1.45,
        },
      },
    ];

    // Future DB-backed indicators can be appended through payload.indicatorSeries
    // without changing the raw tick series path or the loaded raw tick dataset flow.
    if (!state.optionalSeries.size) {
      return series;
    }

    state.optionalSeries.forEach((definition) => {
      series.push({
        id: "indicator:" + definition.id,
        name: definition.label,
        type: definition.type,
        showSymbol: false,
        hoverAnimation: false,
        animation: false,
        connectNulls: true,
        data: definition.data,
        lineStyle: {
          color: definition.color,
          width: definition.lineWidth,
        },
      });
    });

    return series;
  }

  function renderChart(options) {
    const settings = options || {};
    const chart = ensureChart();
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

  function replaceRows(rows) {
    state.rows = Array.isArray(rows) ? rows.slice() : [];
    trimRowsToWindow("right");
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
    return older.length;
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

  async function loadBootstrap(resetView) {
    const config = currentConfig();
    const params = new URLSearchParams({
      mode: config.mode,
      window: String(config.window),
    });
    if (config.mode === "review") {
      const startId = await resolveReviewStartId(config);
      params.set("id", String(startId));
    }

    const payload = await fetchJson("/api/live/bootstrap?" + params.toString());
    replaceRows(payload.rows || []);
    syncOptionalSeries(payload, true);
    state.reviewEndId = payload.reviewEndId || null;
    state.hasMoreLeft = Boolean(payload.hasMoreLeft);
    state.lastMetrics = payload.metrics || null;
    state.lastDatasetBounds = null;
    state.viewport = null;
    renderMeta();
    renderPerf();
    renderChart({ resetView: Boolean(resetView) });
    status("Loaded " + state.rows.length + " raw tick(s).", false);

    if (config.run === "run") {
      if (config.mode === "live") {
        connectStream(payload.lastId || 0);
      } else {
        scheduleReviewStep();
      }
    }
  }

  function connectStream(afterId) {
    clearActivity();
    const source = new EventSource("/api/live/stream?" + new URLSearchParams({
      afterId: String(afterId || 0),
      limit: "250",
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
      syncOptionalSeries(payload, false);
      const appended = dedupeAppend(payload.rows || []);
      renderMeta();
      renderPerf();
      if (appended) {
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
    if (!state.rows.length || !state.reviewEndId) {
      status("Review is waiting for rows.", true);
      return;
    }

    const lastId = state.rows[state.rows.length - 1].id;
    if (lastId >= state.reviewEndId) {
      status("Review reached the current end snapshot.", false);
      return;
    }

    const limit = Math.max(25, Math.min(500, Math.round(100 * config.reviewSpeed)));
    const payload = await fetchJson("/api/live/next?" + new URLSearchParams({
      afterId: String(lastId),
      endId: String(state.reviewEndId),
      limit: String(limit),
    }).toString());
    state.lastMetrics = payload.metrics || null;
    syncOptionalSeries(payload, false);
    dedupeAppend(payload.rows || []);
    renderMeta();
    renderPerf();
    renderChart({ shiftWithRun: true });
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
    if (config.run !== "run" || !state.rows.length) {
      return;
    }
    if (config.mode === "live") {
      connectStream(state.rows[state.rows.length - 1].id);
      return;
    }
    scheduleReviewStep();
  }

  async function loadMoreLeft() {
    if (!state.rows.length) {
      status("Load the chart first.", true);
      return;
    }

    clearActivity();
    const payload = await fetchJson("/api/live/previous?" + new URLSearchParams({
      beforeId: String(state.rows[0].id),
      limit: String(historyBatchSize()),
    }).toString());
    state.lastMetrics = payload.metrics || null;
    const prepended = dedupePrepend(payload.rows || []);
    state.hasMoreLeft = Boolean(payload.hasMoreLeft);
    renderMeta();
    renderPerf();
    if (prepended) {
      renderChart({ shiftWithRun: false });
      status(prepended + " older tick(s) merged into the current window.", false);
    } else {
      status("No older ticks were available.", false);
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
    status("Mode updated. Click Load to refresh ticks.", false);
  });

  bindSegment(elements.runToggle, function (value) {
    setSegment(elements.runToggle, value);
    writeQuery();
    clearActivity();
    if (value === "run" && state.rows.length) {
      resumeRunIfNeeded();
      return;
    }
    status("Run state updated.", false);
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
        elements.windowSize.value = String(sanitizeWindowValue(elements.windowSize.value));
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
