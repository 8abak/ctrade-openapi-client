(function () {
  if (window.__datavisBackboneInitialized) {
    return;
  }
  window.__datavisBackboneInitialized = true;

  const DEFAULTS = {
    mode: "live",
    run: "run",
    showTicks: false,
    id: "",
    reviewStart: "",
    reviewSpeed: 1,
    window: 1200,
  };
  const REVIEW_SPEEDS = [0.5, 1, 2, 3, 5];
  const MAX_WINDOW = 6000;

  const state = {
    chart: null,
    pivots: [],
    moves: [],
    rows: [],
    source: null,
    reviewTimer: 0,
    reviewEndId: null,
    dayId: null,
    lastId: 0,
    stateRow: null,
    loadToken: 0,
    lastMetrics: null,
    streamConnected: false,
    zoom: null,
    applyingZoom: false,
    resizeObserver: null,
    ui: { sidebarCollapsed: true },
  };

  const elements = {
    workspace: document.getElementById("backboneWorkspace"),
    sidebar: document.getElementById("backboneSidebar"),
    sidebarToggle: document.getElementById("sidebarToggle"),
    sidebarBackdrop: document.getElementById("sidebarBackdrop"),
    modeToggle: document.getElementById("modeToggle"),
    runToggle: document.getElementById("runToggle"),
    showTicks: document.getElementById("showTicks"),
    tickId: document.getElementById("tickId"),
    reviewStart: document.getElementById("reviewStart"),
    reviewSpeedToggle: document.getElementById("reviewSpeedToggle"),
    windowSize: document.getElementById("windowSize"),
    applyButton: document.getElementById("applyButton"),
    statusLine: document.getElementById("statusLine"),
    backboneMeta: document.getElementById("backboneMeta"),
    backbonePerf: document.getElementById("backbonePerf"),
    daySummary: document.getElementById("daySummary"),
    stateSummary: document.getElementById("stateSummary"),
    countsSummary: document.getElementById("countsSummary"),
    thresholdSummary: document.getElementById("thresholdSummary"),
    chartHost: document.getElementById("backboneChart"),
  };

  function sanitizeWindowValue(rawValue) {
    return Math.max(1, Math.min(MAX_WINDOW, Number.parseInt(rawValue || String(DEFAULTS.window), 10) || DEFAULTS.window));
  }

  function parseQuery() {
    const params = new URLSearchParams(window.location.search);
    const speed = Number.parseFloat(params.get("speed") || String(DEFAULTS.reviewSpeed));
    return {
      mode: params.get("mode") === "review" ? "review" : DEFAULTS.mode,
      run: params.get("run") === "stop" ? "stop" : DEFAULTS.run,
      showTicks: params.get("showTicks") === "1",
      id: params.get("id") || DEFAULTS.id,
      reviewStart: params.get("reviewStart") || DEFAULTS.reviewStart,
      reviewSpeed: REVIEW_SPEEDS.includes(speed) ? speed : DEFAULTS.reviewSpeed,
      window: sanitizeWindowValue(params.get("window")),
    };
  }

  function currentConfig() {
    return {
      mode: elements.modeToggle.querySelector("button.active")?.dataset.value || DEFAULTS.mode,
      run: elements.runToggle.querySelector("button.active")?.dataset.value || DEFAULTS.run,
      showTicks: elements.showTicks.checked,
      id: (elements.tickId.value || "").trim(),
      reviewStart: (elements.reviewStart.value || "").trim(),
      reviewSpeed: Number.parseFloat(elements.reviewSpeedToggle.querySelector("button.active")?.dataset.value || String(DEFAULTS.reviewSpeed)),
      window: sanitizeWindowValue(elements.windowSize.value),
    };
  }

  function setSegment(container, value) {
    container.querySelectorAll("button").forEach(function (button) {
      button.classList.toggle("active", button.dataset.value === String(value));
    });
  }

  function bindSegment(container, handler) {
    container.querySelectorAll("button").forEach(function (button) {
      button.addEventListener("click", function () {
        handler(button.dataset.value);
      });
    });
  }

  function writeQuery() {
    const config = currentConfig();
    const params = new URLSearchParams({
      mode: config.mode,
      run: config.run,
      showTicks: config.showTicks ? "1" : "0",
      window: String(config.window),
      speed: String(config.reviewSpeed),
    });
    if (config.id) {
      params.set("id", config.id);
    }
    if (config.reviewStart) {
      params.set("reviewStart", config.reviewStart);
    }
    window.history.replaceState({}, "", window.location.pathname + "?" + params.toString());
  }

  function setSidebarCollapsed(collapsed) {
    state.ui.sidebarCollapsed = Boolean(collapsed);
    elements.workspace.classList.toggle("is-sidebar-collapsed", state.ui.sidebarCollapsed);
    elements.sidebarToggle.setAttribute("aria-expanded", String(!state.ui.sidebarCollapsed));
    if (state.chart) {
      requestAnimationFrame(function () { state.chart.resize(); });
    }
  }

  function updateReviewFields() {
    const reviewMode = currentConfig().mode === "review";
    elements.tickId.disabled = !reviewMode;
    elements.reviewStart.disabled = !reviewMode;
    elements.windowSize.disabled = !reviewMode;
    elements.reviewSpeedToggle.querySelectorAll("button").forEach(function (button) {
      button.disabled = !reviewMode;
    });
  }

  function status(message, isError) {
    elements.statusLine.textContent = message;
    elements.statusLine.classList.toggle("error", Boolean(isError));
  }

  function renderMeta() {
    if (!state.pivots.length && !state.moves.length) {
      elements.backboneMeta.textContent = "No backbone range loaded.";
      return;
    }
    const firstPivot = state.pivots[0];
    const lastPivot = state.pivots[state.pivots.length - 1];
    elements.backboneMeta.textContent = [
      currentConfig().mode.toUpperCase(),
      "pivots " + state.pivots.length,
      "moves " + state.moves.length,
      "left " + (firstPivot?.tickId ?? "-"),
      "right " + (state.lastId || lastPivot?.tickId || "-"),
      state.rows.length ? ("ticks " + state.rows.length) : "ticks off",
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
      parts.push("Wire " + Math.max(0, Date.now() - metrics.serverSentAtMs) + "ms");
    }
    elements.backbonePerf.textContent = parts.join(" | ");
  }

  function renderInfo() {
    const backboneState = state.stateRow || {};
    elements.daySummary.textContent = state.dayId
      ? "Broker day " + (backboneState.brokerday || "-") + " | dayId " + state.dayId
      : "Broker day unavailable.";
    elements.stateSummary.textContent = backboneState.lastProcessedTickId
      ? "Last processed tick " + backboneState.lastProcessedTickId + " | direction " + (backboneState.direction || "None")
      : "No backbone state yet.";
    elements.countsSummary.textContent = state.pivots.length + " pivots | " + state.moves.length + " moves";
    elements.thresholdSummary.textContent = backboneState.currentThreshold != null
      ? "Threshold " + Number(backboneState.currentThreshold).toFixed(4)
      : "Threshold -";
  }

  function escapeHtml(value) {
    return String(value)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll("\"", "&quot;");
  }

  function formatTimestamp(value) {
    const numeric = Number(value);
    return Number.isFinite(numeric) ? new Date(numeric).toLocaleString() : "-";
  }

  function formatPrice(value) {
    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric.toFixed(2) : "-";
  }

  function tooltipFormatter(param) {
    const point = param?.data?.pivot;
    if (point) {
      return [
        "<div class=\"chart-tip\">",
        "<div class=\"chart-tip-section\">",
        "<div class=\"chart-tip-title\">", escapeHtml(point.pivotType || "Pivot"), "</div>",
        "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Tick</span><span class=\"chart-tip-value\">", escapeHtml(String(point.tickId || "-")), "</span></div>",
        "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Time</span><span class=\"chart-tip-value\">", escapeHtml(formatTimestamp(point.tickTimeMs)), "</span></div>",
        "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Price</span><span class=\"chart-tip-value\">", escapeHtml(formatPrice(point.price)), "</span></div>",
        "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Threshold</span><span class=\"chart-tip-value\">", escapeHtml(point.threshold != null ? Number(point.threshold).toFixed(4) : "-"), "</span></div>",
        "</div></div>",
      ].join("");
    }

    const move = param?.data?.move;
    if (move) {
      return [
        "<div class=\"chart-tip\">",
        "<div class=\"chart-tip-section\">",
        "<div class=\"chart-tip-title\">", escapeHtml(move.direction || "Move"), "</div>",
        "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Start</span><span class=\"chart-tip-value\">", escapeHtml(String(move.startTickId || "-")), "</span></div>",
        "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">End</span><span class=\"chart-tip-value\">", escapeHtml(String(move.endTickId || "-")), "</span></div>",
        "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Delta</span><span class=\"chart-tip-value\">", escapeHtml(formatPrice(move.priceDelta)), "</span></div>",
        "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Ticks</span><span class=\"chart-tip-value\">", escapeHtml(String(move.tickCount || 0)), "</span></div>",
        "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Duration</span><span class=\"chart-tip-value\">", escapeHtml(String(move.durationMs || 0) + " ms"), "</span></div>",
        "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Threshold</span><span class=\"chart-tip-value\">", escapeHtml(move.thresholdAtConfirm != null ? Number(move.thresholdAtConfirm).toFixed(4) : "-"), "</span></div>",
        "</div></div>",
      ].join("");
    }

    const tick = param?.data?.tick;
    if (tick) {
      return [
        "<div class=\"chart-tip\">",
        "<div class=\"chart-tip-section\">",
        "<div class=\"chart-tip-title\">Raw Tick</div>",
        "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Tick</span><span class=\"chart-tip-value\">", escapeHtml(String(tick.id || "-")), "</span></div>",
        "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Time</span><span class=\"chart-tip-value\">", escapeHtml(formatTimestamp(tick.timestampMs)), "</span></div>",
        "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Mid</span><span class=\"chart-tip-value\">", escapeHtml(formatPrice(tick.mid)), "</span></div>",
        "</div></div>",
      ].join("");
    }

    return "";
  }

  function ensureChart() {
    const rect = elements.chartHost.getBoundingClientRect();
    if (rect.width < 180 || rect.height < 180) {
      return null;
    }
    if (!state.chart) {
      state.chart = echarts.init(elements.chartHost, null, { renderer: "canvas" });
      state.chart.setOption({
        animation: false,
        grid: { left: 60, right: 18, top: 16, bottom: 58 },
        tooltip: {
          trigger: "item",
          confine: true,
          formatter: tooltipFormatter,
          backgroundColor: "transparent",
          borderWidth: 0,
          padding: 0,
          extraCssText: "box-shadow:none;",
        },
        xAxis: {
          type: "time",
          scale: true,
          boundaryGap: false,
          axisLabel: { color: "#9eadc5", hideOverlap: true },
          axisPointer: { label: { backgroundColor: "#0d1420" } },
        },
        yAxis: {
          type: "value",
          scale: true,
          axisLabel: { color: "#9eadc5" },
          splitLine: { lineStyle: { color: "rgba(147,181,255,0.10)" } },
        },
        dataZoom: [
          { id: "zoom-inside", type: "inside", filterMode: "none", rangeMode: ["value", "value"] },
          {
            id: "zoom-slider",
            type: "slider",
            filterMode: "none",
            rangeMode: ["value", "value"],
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
      state.chart.on("dataZoom", function () {
        if (state.applyingZoom) {
          return;
        }
        const option = state.chart.getOption();
        const zoom = option?.dataZoom?.[0] || null;
        state.zoom = zoom ? { start: zoom.start, end: zoom.end, startValue: zoom.startValue, endValue: zoom.endValue } : null;
      });
      if (typeof ResizeObserver === "function") {
        state.resizeObserver = new ResizeObserver(function () { state.chart.resize(); });
        state.resizeObserver.observe(elements.chartHost);
      }
      window.addEventListener("resize", function () {
        state.chart.resize();
      });
    }
    return state.chart;
  }

  function renderChart(options) {
    const chart = ensureChart();
    if (!chart) {
      return;
    }
    const resetView = Boolean(options?.resetView);
    const series = [];

    if (currentConfig().showTicks && state.rows.length) {
      series.push({
        name: "ticks",
        type: "line",
        data: state.rows.map(function (row) {
          return { value: [Number(row.timestampMs), Number(row.mid)], tick: row };
        }),
        showSymbol: false,
        lineStyle: { width: 1, color: "rgba(109, 216, 255, 0.18)" },
        emphasis: { lineStyle: { width: 1.2 } },
        z: 1,
      });
    }

    if (state.pivots.length) {
      series.push({
        name: "backbone",
        type: "line",
        data: state.pivots.map(function (pivot) {
          return { value: [Number(pivot.tickTimeMs), Number(pivot.price)], pivot: pivot };
        }),
        showSymbol: false,
        lineStyle: { width: 2.6, color: "#ffb35c" },
        itemStyle: { color: "#ffb35c" },
        z: 5,
      });
    }

    const pivotSeries = [
      { type: "Start", color: "#6dd8ff", symbol: "diamond", size: 9 },
      { type: "High", color: "#ff6b88", symbol: "triangle", size: 10 },
      { type: "Low", color: "#7ef0c7", symbol: "triangle", size: 10, rotate: 180 },
    ];
    pivotSeries.forEach(function (definition) {
      const filtered = state.pivots.filter(function (pivot) { return pivot.pivotType === definition.type; });
      if (!filtered.length) {
        return;
      }
      series.push({
        name: definition.type.toLowerCase(),
        type: "scatter",
        symbol: definition.symbol,
        symbolSize: definition.size,
        data: filtered.map(function (pivot) {
          return { value: [Number(pivot.tickTimeMs), Number(pivot.price)], pivot: pivot };
        }),
        itemStyle: { color: definition.color },
        z: 7,
      });
    });

    if (state.moves.length) {
      series.push({
        name: "moves",
        type: "scatter",
        symbol: "circle",
        symbolSize: 7,
        data: state.moves.map(function (move) {
          const midpointTime = Number(move.startTimeMs) + ((Number(move.endTimeMs) - Number(move.startTimeMs)) / 2);
          const midpointPrice = Number(move.startPrice) + ((Number(move.endPrice) - Number(move.startPrice)) / 2);
          return { value: [midpointTime, midpointPrice], move: move };
        }),
        itemStyle: { color: "rgba(255,255,255,0.22)" },
        z: 6,
      });
    }

    chart.setOption({ series: series }, { notMerge: true, lazyUpdate: true });
    if (resetView) {
      state.zoom = null;
    }
    if (state.zoom) {
      state.applyingZoom = true;
      chart.dispatchAction({
        type: "dataZoom",
        dataZoomIndex: 0,
        start: state.zoom.start,
        end: state.zoom.end,
        startValue: state.zoom.startValue,
        endValue: state.zoom.endValue,
      });
      state.applyingZoom = false;
    }
  }

  function resetStateFromPayload(payload) {
    state.dayId = payload.dayId || null;
    state.reviewEndId = payload.reviewEndId || null;
    state.pivots = Array.isArray(payload.pivots) ? payload.pivots.slice() : [];
    state.moves = Array.isArray(payload.moves) ? payload.moves.slice() : [];
    state.rows = Array.isArray(payload.rows) ? payload.rows.slice() : [];
    state.stateRow = Object.assign({ brokerday: payload.brokerday || null }, payload.state || {});
    state.lastId = Number(payload.lastId || 0);
    state.lastMetrics = payload.metrics || null;
  }

  function mergeItems(currentItems, updates, key) {
    const next = new Map();
    currentItems.forEach(function (item) { next.set(String(item[key]), item); });
    updates.forEach(function (item) { next.set(String(item[key]), item); });
    return Array.from(next.values());
  }

  function appendTicks(updates) {
    if (!updates.length) {
      return;
    }
    const next = new Map();
    state.rows.forEach(function (row) { next.set(String(row.id), row); });
    updates.forEach(function (row) { next.set(String(row.id), row); });
    state.rows = Array.from(next.values()).sort(function (left, right) { return Number(left.id) - Number(right.id); });
  }

  function applyDeltaPayload(payload) {
    if (payload.dayChanged) {
      loadAll(true).catch(function (error) {
        status(error.message || "Day reload failed.", true);
      });
      return;
    }
    state.dayId = payload.dayId || state.dayId;
    state.lastId = Math.max(Number(state.lastId || 0), Number(payload.lastId || 0));
    state.stateRow = payload.state ? Object.assign({}, state.stateRow || {}, payload.state) : state.stateRow;
    state.lastMetrics = payload.metrics || state.lastMetrics;
    state.pivots = mergeItems(state.pivots, payload.pivotUpdates || [], "id")
      .sort(function (left, right) { return Number(left.tickId) - Number(right.tickId); });
    state.moves = mergeItems(state.moves, payload.moveUpdates || [], "id")
      .sort(function (left, right) { return Number(left.endTickId) - Number(right.endTickId); });
    appendTicks(payload.rows || []);
    renderMeta();
    renderPerf();
    renderInfo();
    renderChart({ resetView: false });
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

  function fetchJson(url) {
    return fetch(url).then(async function (response) {
      const payload = await response.json().catch(function () { return {}; });
      if (!response.ok) {
        throw new Error(payload?.detail || payload?.message || "Request failed.");
      }
      return payload;
    });
  }

  async function resolveReviewStartId(config) {
    if (config.reviewStart) {
      const payload = await fetchJson("/api/backbone/review-start?" + new URLSearchParams({
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
    const startId = config.mode === "review" ? await resolveReviewStartId(config) : null;
    const params = new URLSearchParams({
      mode: config.mode,
      window: String(config.window),
      showTicks: config.showTicks ? "1" : "0",
    });
    if (startId != null) {
      params.set("id", String(startId));
    }
    const payload = await fetchJson("/api/backbone/bootstrap?" + params.toString());
    resetStateFromPayload(payload);
    renderMeta();
    renderPerf();
    renderInfo();
    renderChart({ resetView: Boolean(resetView) });
    status("Loaded " + state.pivots.length + " pivot(s).", false);
    if (config.run === "run") {
      resumeRunIfNeeded();
    }
  }

  function connectStream(afterId) {
    clearActivity();
    const config = currentConfig();
    const source = new EventSource("/api/backbone/stream?" + new URLSearchParams({
      afterId: String(afterId || 0),
      limit: "250",
      showTicks: config.showTicks ? "1" : "0",
    }).toString());
    state.source = source;
    source.onopen = function () {
      state.streamConnected = true;
      renderPerf();
      status("Live stream connected.", false);
    };
    source.onmessage = function (event) {
      applyDeltaPayload(JSON.parse(event.data));
    };
    source.addEventListener("heartbeat", function (event) {
      state.lastMetrics = JSON.parse(event.data);
      renderPerf();
    });
    source.onerror = function () {
      clearActivity();
      status("Live stream disconnected. Click Load or Run to reconnect.", true);
    };
  }

  async function reviewStep() {
    const config = currentConfig();
    if (config.mode !== "review" || config.run !== "run") {
      return;
    }
    if (!state.dayId || !state.reviewEndId || state.lastId >= state.reviewEndId) {
      status("Review reached the current end snapshot.", false);
      return;
    }
    const payload = await fetchJson("/api/backbone/next?" + new URLSearchParams({
      afterId: String(state.lastId || 0),
      limit: String(Math.max(10, Math.min(250, Math.round(50 * config.reviewSpeed)))),
      dayId: String(state.dayId),
      endId: String(state.reviewEndId),
      showTicks: config.showTicks ? "1" : "0",
    }).toString());
    applyDeltaPayload(payload);
    status(payload.endReached ? "Review reached the current end snapshot." : "Review running.", false);
    if (!payload.endReached && currentConfig().run === "run") {
      scheduleReviewStep();
    }
  }

  function scheduleReviewStep() {
    if (state.reviewTimer) {
      window.clearTimeout(state.reviewTimer);
    }
    const delay = Math.max(80, Math.round(450 / currentConfig().reviewSpeed));
    state.reviewTimer = window.setTimeout(function () {
      state.reviewTimer = 0;
      reviewStep().catch(function (error) {
        status(error.message || "Review fetch failed.", true);
      });
    }, delay);
  }

  function resumeRunIfNeeded() {
    const config = currentConfig();
    if (config.run !== "run") {
      return;
    }
    if (config.mode === "live") {
      connectStream(state.lastId || 0);
      return;
    }
    scheduleReviewStep();
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

  function applyInitialConfig(config) {
    setSegment(elements.modeToggle, config.mode);
    setSegment(elements.runToggle, config.run);
    setSegment(elements.reviewSpeedToggle, config.reviewSpeed);
    elements.showTicks.checked = Boolean(config.showTicks);
    elements.tickId.value = config.id;
    elements.reviewStart.value = config.reviewStart;
    elements.windowSize.value = String(config.window);
    setSidebarCollapsed(true);
    updateReviewFields();
    renderMeta();
    renderPerf();
    renderInfo();
    writeQuery();
  }

  bindSegment(elements.modeToggle, function (value) {
    setSegment(elements.modeToggle, value);
    updateReviewFields();
    writeQuery();
    status("Mode updated. Click Load to refresh data.", false);
  });
  bindSegment(elements.runToggle, function (value) {
    setSegment(elements.runToggle, value);
    clearActivity();
    writeQuery();
    if (value === "run" && state.lastId != null) {
      resumeRunIfNeeded();
      return;
    }
    status("Run state updated.", false);
  });
  bindSegment(elements.reviewSpeedToggle, function (value) {
    setSegment(elements.reviewSpeedToggle, value);
    writeQuery();
  });

  [elements.showTicks, elements.tickId, elements.reviewStart, elements.windowSize].forEach(function (control) {
    control.addEventListener("change", function () {
      if (control === elements.windowSize) {
        elements.windowSize.value = String(sanitizeWindowValue(elements.windowSize.value));
      }
      writeQuery();
      if (control === elements.showTicks) {
        loadAll(false).catch(function (error) {
          status(error.message || "Display refresh failed.", true);
        });
      }
    });
  });

  elements.sidebarToggle.addEventListener("click", function () { setSidebarCollapsed(!state.ui.sidebarCollapsed); });
  elements.sidebarBackdrop.addEventListener("click", function () { setSidebarCollapsed(true); });
  elements.applyButton.addEventListener("click", function () { loadAll(true); });

  applyInitialConfig(parseQuery());
  loadAll(true);
}());
