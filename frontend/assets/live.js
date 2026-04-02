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
  };

  const elements = {
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
      reviewSpeed: [0.5, 1, 2, 3, 5].includes(reviewSpeed) ? reviewSpeed : DEFAULTS.reviewSpeed,
      window: Math.max(1, Math.min(10000, Number.parseInt(params.get("window") || String(DEFAULTS.window), 10) || DEFAULTS.window)),
    };
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
    const nextUrl = window.location.pathname + "?" + params.toString();
    window.history.replaceState({}, "", nextUrl);
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
      window: Math.max(1, Math.min(10000, Number.parseInt(elements.windowSize.value || String(DEFAULTS.window), 10) || DEFAULTS.window)),
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
    updateReviewFields();
    writeQuery();
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
    const first = state.rows[0];
    const last = state.rows[state.rows.length - 1];
    elements.liveMeta.textContent = [
      currentConfig().mode.toUpperCase(),
      "rows " + state.rows.length,
      "left " + first.id,
      "right " + last.id,
      "series " + currentConfig().series,
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
      window.addEventListener("resize", () => {
        if (state.chart) {
          state.chart.resize();
        }
      });
    }
    return state.chart;
  }

  function captureZoom() {
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
      startValue: zoom.startValue,
      endValue: zoom.endValue,
    };
  }

  function restoreZoom(zoom) {
    if (!state.chart || !zoom) {
      return;
    }
    state.chart.dispatchAction({
      type: "dataZoom",
      dataZoomIndex: 0,
      startValue: zoom.startValue,
      endValue: zoom.endValue,
    });
    state.chart.dispatchAction({
      type: "dataZoom",
      dataZoomIndex: 1,
      startValue: zoom.startValue,
      endValue: zoom.endValue,
    });
  }

  function chartData(seriesKey) {
    return state.rows.map((row) => [row.timestampMs, row[seriesKey]]);
  }

  function renderChart(options) {
    const settings = options || {};
    const chart = ensureChart();
    const config = currentConfig();
    const zoom = settings.preserveZoom ? captureZoom() : null;
    chart.setOption({
      animation: false,
      grid: { left: 58, right: 22, top: 18, bottom: 72 },
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
        { type: "inside", filterMode: "none" },
        { type: "slider", filterMode: "none", height: 28, bottom: 18 },
      ],
      series: [
        {
          name: SERIES_CONFIG[config.series].label,
          type: "line",
          showSymbol: false,
          hoverAnimation: false,
          animation: false,
          data: chartData(config.series),
          lineStyle: {
            color: SERIES_CONFIG[config.series].color,
            width: 1.6,
          },
        },
      ],
    }, { notMerge: true, lazyUpdate: true });
    restoreZoom(zoom);
  }

  function dedupeAppend(rows) {
    if (!rows.length) {
      return;
    }
    const lastId = state.rows.length ? state.rows[state.rows.length - 1].id : 0;
    rows.forEach((row) => {
      if (row.id > lastId) {
        state.rows.push(row);
      }
    });
  }

  function dedupePrepend(rows) {
    if (!rows.length) {
      return;
    }
    const firstId = state.rows.length ? state.rows[0].id : Number.MAX_SAFE_INTEGER;
    const older = rows.filter((row) => row.id < firstId);
    if (!older.length) {
      return;
    }
    state.rows = older.concat(state.rows);
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

  async function loadBootstrap(resetZoom) {
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
    state.rows = payload.rows || [];
    state.reviewEndId = payload.reviewEndId || null;
    state.hasMoreLeft = Boolean(payload.hasMoreLeft);
    state.lastMetrics = payload.metrics || null;
    renderMeta();
    renderPerf();
    renderChart({ preserveZoom: !resetZoom });
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
      dedupeAppend(payload.rows || []);
      renderMeta();
      renderPerf();
      renderChart();
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
    dedupeAppend(payload.rows || []);
    renderMeta();
    renderPerf();
    renderChart();
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

  async function loadMoreLeft() {
    if (!state.rows.length) {
      status("Load the chart first.", true);
      return;
    }
    const zoom = captureZoom();
    const payload = await fetchJson("/api/live/previous?" + new URLSearchParams({
      beforeId: String(state.rows[0].id),
      limit: String(currentConfig().window),
    }).toString());
    state.lastMetrics = payload.metrics || null;
    dedupePrepend(payload.rows || []);
    state.hasMoreLeft = Boolean(payload.hasMoreLeft);
    renderMeta();
    renderPerf();
    renderChart({ preserveZoom: Boolean(zoom) });
    status((payload.rowCount || 0) + " older tick(s) prepended.", false);
  }

  async function loadAll(resetZoom) {
    const token = state.loadToken + 1;
    state.loadToken = token;
    clearActivity();
    writeQuery();
    try {
      await loadBootstrap(resetZoom);
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
      if (currentConfig().mode === "live") {
        connectStream(state.rows[state.rows.length - 1].id);
      } else {
        scheduleReviewStep();
      }
      return;
    }
    status("Run state updated.", false);
  });

  bindSegment(elements.seriesToggle, function (value) {
    setSegment(elements.seriesToggle, value);
    writeQuery();
    renderMeta();
    renderChart({ preserveZoom: true });
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
    control.addEventListener("change", writeQuery);
  });

  elements.applyButton.addEventListener("click", function () {
    loadAll(true);
  });
  elements.loadMoreLeftButton.addEventListener("click", function () {
    loadMoreLeft().catch((error) => {
      status(error.message || "Load More Left failed.", true);
    });
  });

  const initialConfig = parseQuery();
  applyInitialConfig(initialConfig);
  loadAll(true);
}());
