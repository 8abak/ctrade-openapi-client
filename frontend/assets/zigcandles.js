(function () {
  const DEFAULTS = {
    mode: "live",
    run: "run",
    display: "candles",
    level: 0,
    series: "mid",
    id: "",
    reviewStart: "",
    reviewSpeed: 1,
    window: 2000,
    provisional: true,
    table: true,
  };

  const SERIES_CONFIG = {
    mid: { label: "Mid" },
    ask: { label: "Ask" },
    bid: { label: "Bid" },
  };

  const REVIEW_SPEEDS = [0.5, 1, 2, 3, 5];
  const MIN_CHART_WIDTH = 180;
  const MIN_CHART_HEIGHT = 180;

  const state = {
    chart: null,
    bars: [],
    source: null,
    reviewTimer: 0,
    reviewEndId: null,
    reviewStartId: null,
    loadToken: 0,
    lastMetrics: null,
    streamConnected: false,
    hasMoreLeft: false,
    rangeFirstId: null,
    rangeLastId: null,
    rangeFirstTimestampMs: null,
    rangeLastTimestampMs: null,
    selectedBarId: null,
    zoom: { start: 0, end: 100 },
    autoscaleFrame: 0,
    layoutResizeFrame: 0,
    layoutResizeTimeout: 0,
    resizeObserver: null,
    resizeBound: false,
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
    levelToggle: document.getElementById("levelToggle"),
    seriesToggle: document.getElementById("seriesToggle"),
    tickId: document.getElementById("tickId"),
    reviewStart: document.getElementById("reviewStart"),
    reviewSpeedToggle: document.getElementById("reviewSpeedToggle"),
    windowSize: document.getElementById("windowSize"),
    showProvisional: document.getElementById("showProvisional"),
    showTable: document.getElementById("showTable"),
    applyButton: document.getElementById("applyButton"),
    loadMoreLeftButton: document.getElementById("loadMoreLeftButton"),
    statusLine: document.getElementById("statusLine"),
    liveMeta: document.getElementById("liveMeta"),
    livePerf: document.getElementById("livePerf"),
    chartHost: document.getElementById("zigCandlesChart"),
    tablePanel: document.getElementById("tablePanel"),
    tableMeta: document.getElementById("tableMeta"),
    barsTableBody: document.getElementById("barsTableBody"),
  };

  function parseQuery() {
    const params = new URLSearchParams(window.location.search);
    const reviewSpeed = Number.parseFloat(params.get("speed") || String(DEFAULTS.reviewSpeed));
    const level = Number.parseInt(params.get("level") || String(DEFAULTS.level), 10);
    return {
      mode: params.get("mode") === "review" ? "review" : DEFAULTS.mode,
      run: params.get("run") === "stop" ? "stop" : DEFAULTS.run,
      display: DEFAULTS.display,
      level: Number.isFinite(level) ? Math.max(0, Math.min(3, level)) : DEFAULTS.level,
      series: Object.prototype.hasOwnProperty.call(SERIES_CONFIG, params.get("series")) ? params.get("series") : DEFAULTS.series,
      id: params.get("id") || DEFAULTS.id,
      reviewStart: params.get("reviewStart") || DEFAULTS.reviewStart,
      reviewSpeed: REVIEW_SPEEDS.includes(reviewSpeed) ? reviewSpeed : DEFAULTS.reviewSpeed,
      window: sanitizeWindowValue(params.get("window")),
      provisional: params.get("provisional") !== "0",
      table: params.get("table") !== "0",
    };
  }

  function sanitizeWindowValue(rawValue) {
    return Math.max(1, Math.min(10000, Number.parseInt(rawValue || String(DEFAULTS.window), 10) || DEFAULTS.window));
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
      display: DEFAULTS.display,
      level: Number.parseInt(elements.levelToggle.querySelector("button.active")?.dataset.value || String(DEFAULTS.level), 10),
      series: elements.seriesToggle.querySelector("button.active")?.dataset.value || DEFAULTS.series,
      id: (elements.tickId.value || "").trim(),
      reviewStart: (elements.reviewStart.value || "").trim(),
      reviewSpeed: Number.parseFloat(elements.reviewSpeedToggle.querySelector("button.active")?.dataset.value || String(DEFAULTS.reviewSpeed)),
      window: sanitizeWindowValue(elements.windowSize.value),
      provisional: Boolean(elements.showProvisional.checked),
      table: Boolean(elements.showTable.checked),
    };
  }

  function writeQuery() {
    const config = currentConfig();
    const params = new URLSearchParams();
    params.set("mode", config.mode);
    params.set("run", config.run);
    params.set("display", config.display);
    params.set("level", String(config.level));
    params.set("series", config.series);
    params.set("window", String(config.window));
    params.set("speed", String(config.reviewSpeed));
    params.set("provisional", config.provisional ? "1" : "0");
    params.set("table", config.table ? "1" : "0");
    if (config.id) {
      params.set("id", config.id);
    }
    if (config.reviewStart) {
      params.set("reviewStart", config.reviewStart);
    }
    window.history.replaceState({}, "", window.location.pathname + "?" + params.toString());
  }

  function status(message, isError) {
    elements.statusLine.textContent = message;
    elements.statusLine.classList.toggle("error", Boolean(isError));
  }

  function setSidebarCollapsed(collapsed) {
    state.ui.sidebarCollapsed = Boolean(collapsed);
    elements.liveWorkspace.classList.toggle("is-sidebar-collapsed", state.ui.sidebarCollapsed);
    elements.sidebarToggle.setAttribute("aria-expanded", String(!state.ui.sidebarCollapsed));
    elements.sidebarToggle.setAttribute("aria-label", state.ui.sidebarCollapsed ? "Open zig candle controls" : "Close zig candle controls");
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

  function updateReviewFields() {
    const reviewMode = currentConfig().mode === "review";
    elements.tickId.disabled = !reviewMode;
    elements.reviewStart.disabled = !reviewMode;
    elements.reviewSpeedToggle.querySelectorAll("button").forEach((button) => {
      button.disabled = !reviewMode;
    });
  }

  function applyInitialConfig(config) {
    setSegment(elements.modeToggle, config.mode);
    setSegment(elements.runToggle, config.run);
    setSegment(elements.levelToggle, config.level);
    setSegment(elements.seriesToggle, config.series);
    setSegment(elements.reviewSpeedToggle, config.reviewSpeed);
    setSegment(elements.displayToggle, DEFAULTS.display);
    elements.tickId.value = config.id;
    elements.reviewStart.value = config.reviewStart;
    elements.windowSize.value = String(config.window);
    elements.showProvisional.checked = Boolean(config.provisional);
    elements.showTable.checked = Boolean(config.table);
    elements.tablePanel.hidden = !config.table;
    setSidebarCollapsed(true);
    setSettingsCollapsed(true);
    updateReviewFields();
    renderMeta();
    renderPerf();
    writeQuery();
  }

  function chartHostRect() {
    const rect = elements.chartHost.getBoundingClientRect();
    return { width: Math.round(rect.width), height: Math.round(rect.height) };
  }

  function chartHostHasSize() {
    const rect = chartHostRect();
    return rect.width >= MIN_CHART_WIDTH && rect.height >= MIN_CHART_HEIGHT;
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
      if (state.chart && chartHostHasSize()) {
        state.chart.resize();
        queueVisibleYAxisUpdate();
      }
    });
    state.layoutResizeTimeout = window.setTimeout(() => {
      state.layoutResizeTimeout = 0;
      if (state.chart && chartHostHasSize()) {
        state.chart.resize();
        queueVisibleYAxisUpdate();
      }
    }, 220);
  }

  function bindResizeLifecycle() {
    if (!state.resizeObserver && typeof ResizeObserver === "function") {
      state.resizeObserver = new ResizeObserver(() => {
        queueChartResize();
      });
      [elements.liveWorkspace, elements.liveSidebar, elements.chartHost, elements.tablePanel].forEach((element) => {
        if (element) {
          state.resizeObserver.observe(element);
        }
      });
    }
    if (!state.resizeBound) {
      state.resizeBound = true;
      window.addEventListener("resize", queueChartResize);
      [elements.liveWorkspace, elements.liveSidebar, elements.tablePanel].forEach((element) => {
        if (!element) {
          return;
        }
        element.addEventListener("transitionend", queueChartResize);
      });
      if (document.fonts && typeof document.fonts.ready?.then === "function") {
        document.fonts.ready.then(queueChartResize).catch(queueChartResize);
      }
    }
  }

  function renderMeta() {
    if (state.rangeLastId == null) {
      elements.liveMeta.textContent = "No candle window loaded.";
      return;
    }
    const config = currentConfig();
    const finalCount = state.bars.filter((bar) => bar.isFinal).length;
    const provisionalCount = state.bars.filter((bar) => !bar.isFinal).length;
    elements.liveMeta.textContent = [
      config.mode.toUpperCase(),
      "Candles",
      "L" + config.level,
      config.series,
      "bars " + state.bars.length,
      "final " + finalCount,
      "active " + provisionalCount,
      "left " + state.rangeFirstId,
      "right " + state.rangeLastId,
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

  function formatNumber(value) {
    return typeof value === "number" ? value.toFixed(2) : "";
  }

  function formatSignedNumber(value) {
    if (typeof value !== "number") {
      return "";
    }
    return (value > 0 ? "+" : "") + value.toFixed(2);
  }

  function formatTableTimestamp(timestamp) {
    const date = new Date(timestamp);
    return date.toLocaleString("en-AU", {
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
  }

  function escapeHtml(value) {
    return String(value)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll("\"", "&quot;");
  }

  function renderTable() {
    if (!state.bars.length) {
      elements.tableMeta.textContent = "No bars loaded.";
      elements.barsTableBody.innerHTML = "";
      return;
    }
    elements.tableMeta.textContent = state.bars.length + " bar(s) | most recent first";
    const rows = state.bars.slice().reverse().map((bar) => {
      const tr = document.createElement("tr");
      tr.dataset.barId = bar.id;
      tr.classList.toggle("is-selected", bar.id === state.selectedBarId);
      tr.classList.toggle("is-provisional", !bar.isFinal);
      tr.innerHTML = [
        "<td>" + escapeHtml(bar.barState) + "</td>",
        "<td>" + escapeHtml(formatTableTimestamp(bar.endTimestamp)) + "</td>",
        "<td>" + escapeHtml(bar.direction) + "</td>",
        "<td>" + formatNumber(bar.open) + "</td>",
        "<td>" + formatNumber(bar.high) + "</td>",
        "<td>" + formatNumber(bar.low) + "</td>",
        "<td>" + formatNumber(bar.close) + "</td>",
        "<td>" + escapeHtml(String(bar.tickCount)) + "</td>",
        "<td>" + formatSignedNumber(bar.priceRange) + "</td>",
        "<td>" + formatSignedNumber(bar.netMove) + "</td>",
        "<td>" + escapeHtml(bar.durationLabel) + "</td>",
        "<td>" + escapeHtml(String(bar.startPivotId) + " -> " + (bar.endPivotId == null ? "active" : String(bar.endPivotId))) + "</td>",
        "<td>" + escapeHtml(String(bar.startTickId) + "-" + String(bar.endTickId)) + "</td>",
      ].join("");
      tr.addEventListener("click", () => {
        focusBar(bar.id);
      });
      return tr;
    });
    elements.barsTableBody.replaceChildren(...rows);
  }

  function candleAxisLabel(bar) {
    const date = new Date(bar.endTimestampMs || bar.endTimestamp);
    const options = state.bars.length > 48
      ? { hour: "2-digit", minute: "2-digit" }
      : { hour: "2-digit", minute: "2-digit", second: "2-digit" };
    return date.toLocaleTimeString("en-AU", options);
  }

  function candleItemStyle(bar, isSelected) {
    const up = bar.close >= bar.open;
    const palette = up
      ? { color: "#7ef0c7", border: "#bcffe8" }
      : { color: "#ff8c42", border: "#ffd9b8" };
    if (!bar.isFinal) {
      return {
        color: "rgba(109, 216, 255, 0.18)",
        color0: "rgba(109, 216, 255, 0.18)",
        borderColor: isSelected ? "#f3f6fb" : "#6dd8ff",
        borderColor0: isSelected ? "#f3f6fb" : "#6dd8ff",
        borderWidth: isSelected ? 2.2 : 1.4,
      };
    }
    return {
      color: palette.color,
      color0: palette.color,
      borderColor: isSelected ? "#f3f6fb" : palette.border,
      borderColor0: isSelected ? "#f3f6fb" : palette.border,
      borderWidth: isSelected ? 2.2 : 1.1,
    };
  }

  function buildChartData() {
    return state.bars.map((bar) => ({
      value: [bar.open, bar.close, bar.low, bar.high],
      itemStyle: candleItemStyle(bar, bar.id === state.selectedBarId),
      bar: bar,
    }));
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
        grid: { left: 54, right: 18, top: 14, bottom: 54 },
        tooltip: {
          trigger: "axis",
          axisPointer: { type: "cross" },
          formatter: function (params) {
            const point = Array.isArray(params) ? params[0] : params;
            const bar = point?.data?.bar;
            if (!bar) {
              return "";
            }
            return [
              "<div class=\"zigcandles-tip\">",
              "<strong>" + escapeHtml(bar.symbol + " L" + bar.level + " " + bar.barState.toUpperCase()) + "</strong><br>",
              escapeHtml(bar.direction + " | " + bar.series) + "<br>",
              "O " + formatNumber(bar.open) + " | H " + formatNumber(bar.high) + " | L " + formatNumber(bar.low) + " | C " + formatNumber(bar.close) + "<br>",
              "ticks " + escapeHtml(String(bar.tickCount)) + " | range " + formatSignedNumber(bar.priceRange) + " | move " + formatSignedNumber(bar.netMove) + "<br>",
              "dur " + escapeHtml(bar.durationLabel) + " | ids " + escapeHtml(String(bar.startTickId) + "-" + String(bar.endTickId)) + "<br>",
              "pivots " + escapeHtml(String(bar.startPivotId) + " -> " + (bar.endPivotId == null ? "active" : String(bar.endPivotId))),
              "</div>",
            ].join("");
          },
        },
        xAxis: {
          type: "category",
          axisLabel: { color: "#9eadc5" },
          axisLine: { lineStyle: { color: "rgba(147, 181, 255, 0.18)" } },
        },
        yAxis: {
          type: "value",
          scale: true,
          axisLabel: { color: "#9eadc5" },
          splitLine: { lineStyle: { color: "rgba(147, 181, 255, 0.08)" } },
        },
        dataZoom: [
          { id: "zoom-inside", type: "inside", filterMode: "none" },
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
        series: [{
          type: "candlestick",
          name: "Zig candles",
          data: [],
        }],
      }, { notMerge: true, lazyUpdate: true });

      state.chart.on("dataZoom", function () {
        const option = state.chart.getOption();
        const zoom = option?.dataZoom?.[0] || {};
        state.zoom = {
          start: Number.isFinite(Number(zoom.start)) ? Number(zoom.start) : 0,
          end: Number.isFinite(Number(zoom.end)) ? Number(zoom.end) : 100,
        };
        queueVisibleYAxisUpdate();
      });
    }
    return state.chart;
  }

  function visibleBarSlice() {
    if (!state.bars.length) {
      return [];
    }
    const startPercent = Math.max(0, Math.min(100, state.zoom.start));
    const endPercent = Math.max(0, Math.min(100, state.zoom.end));
    const startIndex = Math.max(0, Math.floor((startPercent / 100) * state.bars.length));
    const endIndex = Math.min(state.bars.length, Math.ceil((endPercent / 100) * state.bars.length));
    return state.bars.slice(startIndex, Math.max(startIndex + 1, endIndex));
  }

  function queueVisibleYAxisUpdate() {
    if (state.autoscaleFrame) {
      window.cancelAnimationFrame(state.autoscaleFrame);
      state.autoscaleFrame = 0;
    }
    state.autoscaleFrame = window.requestAnimationFrame(() => {
      state.autoscaleFrame = 0;
      if (!state.chart || !state.bars.length) {
        return;
      }
      const visible = visibleBarSlice();
      let minValue = Number.POSITIVE_INFINITY;
      let maxValue = Number.NEGATIVE_INFINITY;
      visible.forEach((bar) => {
        minValue = Math.min(minValue, bar.low);
        maxValue = Math.max(maxValue, bar.high);
      });
      if (!Number.isFinite(minValue) || !Number.isFinite(maxValue)) {
        return;
      }
      const span = Math.max(0.01, maxValue - minValue);
      const padding = Math.max(0.05, span * 0.04);
      state.chart.setOption({
        yAxis: {
          min: Number((minValue - padding).toFixed(6)),
          max: Number((maxValue + padding).toFixed(6)),
        },
      }, { lazyUpdate: true });
    });
  }

  function renderChart(resetView) {
    const chart = ensureChart();
    if (!chart) {
      queueChartResize();
      return;
    }
    if (!state.bars.length) {
      chart.setOption({
        xAxis: { data: [] },
        series: [{ data: [] }],
      }, { replaceMerge: ["series"], lazyUpdate: true });
      return;
    }
    if (resetView) {
      state.zoom = { start: 0, end: 100 };
    }
    chart.setOption({
      xAxis: { data: state.bars.map(candleAxisLabel) },
      dataZoom: [
        { id: "zoom-inside", start: state.zoom.start, end: state.zoom.end },
        { id: "zoom-slider", start: state.zoom.start, end: state.zoom.end },
      ],
      series: [{
        type: "candlestick",
        name: "Zig candles",
        data: buildChartData(),
      }],
    }, { replaceMerge: ["series"], lazyUpdate: true });
    queueVisibleYAxisUpdate();
  }

  function replaceBars(rows) {
    state.bars = Array.isArray(rows) ? rows.slice() : [];
    if (!state.bars.some((bar) => bar.id === state.selectedBarId)) {
      state.selectedBarId = state.bars.length ? state.bars[state.bars.length - 1].id : null;
    }
  }

  function applyRangePayload(payload) {
    state.rangeFirstId = payload.firstId ?? state.rangeFirstId;
    state.rangeLastId = payload.lastId ?? state.rangeLastId;
    state.rangeFirstTimestampMs = payload.firstTimestampMs ?? state.rangeFirstTimestampMs;
    state.rangeLastTimestampMs = payload.lastTimestampMs ?? state.rangeLastTimestampMs;
  }

  function syncPayload(payload) {
    replaceBars(payload.bars || []);
    applyRangePayload(payload);
    state.reviewEndId = payload.reviewEndId || state.reviewEndId || null;
    state.hasMoreLeft = Boolean(payload.hasMoreLeft);
    state.lastMetrics = payload.metrics || null;
    renderMeta();
    renderPerf();
    renderTable();
  }

  function focusBar(barId) {
    const index = state.bars.findIndex((bar) => bar.id === barId);
    if (index < 0) {
      return;
    }
    state.selectedBarId = barId;
    renderTable();
    renderChart(false);
    if (state.chart) {
      const startValue = Math.max(0, index - 8);
      const endValue = Math.min(state.bars.length - 1, index + 8);
      state.chart.dispatchAction({ type: "dataZoom", startValue: startValue, endValue: endValue });
      state.chart.dispatchAction({ type: "showTip", seriesIndex: 0, dataIndex: index });
    }
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
      const payload = await fetchJson("/api/zigcandles/review-start?" + new URLSearchParams({
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
      level: String(config.level),
      series: config.series,
      provisional: config.provisional ? "true" : "false",
    });
    if (config.mode === "review" && startId != null) {
      params.set("id", String(startId));
    }
    return "/api/zigcandles/bootstrap?" + params.toString();
  }

  function nextUrl(config, afterId, endId) {
    const params = new URLSearchParams({
      afterId: String(afterId),
      limit: String(Math.max(25, Math.min(500, Math.round(100 * config.reviewSpeed)))),
      window: String(config.window),
      level: String(config.level),
      series: config.series,
      provisional: config.provisional ? "true" : "false",
    });
    if (endId != null) {
      params.set("endId", String(endId));
    }
    if (config.mode === "review" && state.reviewStartId != null) {
      params.set("reviewStartId", String(state.reviewStartId));
    }
    return "/api/zigcandles/next?" + params.toString();
  }

  function previousUrl(config) {
    return "/api/zigcandles/previous?" + new URLSearchParams({
      beforeId: String(state.rangeFirstId || 1),
      currentLastId: String(state.rangeLastId || 1),
      limit: String(historyBatchSize()),
      window: String(config.window),
      level: String(config.level),
      series: config.series,
      provisional: config.provisional ? "true" : "false",
    }).toString();
  }

  async function loadBootstrap(resetView) {
    const config = currentConfig();
    state.reviewStartId = config.mode === "review" ? await resolveReviewStartId(config) : null;
    const payload = await fetchJson(bootstrapUrl(config, state.reviewStartId));
    syncPayload(payload);
    renderChart(Boolean(resetView));
    status("Loaded " + state.bars.length + " zig candle(s).", false);
    if (config.run === "run") {
      if (config.mode === "live") {
        connectStream(state.rangeLastId || 0);
      } else {
        scheduleReviewStep();
      }
    }
    if (state.selectedBarId) {
      focusBar(state.selectedBarId);
    }
  }

  function connectStream(afterId) {
    clearActivity();
    const config = currentConfig();
    const source = new EventSource("/api/zigcandles/stream?" + new URLSearchParams({
      afterId: String(afterId || 0),
      limit: "250",
      window: String(config.window),
      level: String(config.level),
      series: config.series,
      provisional: config.provisional ? "true" : "false",
    }).toString());
    state.source = source;

    source.onopen = function () {
      state.streamConnected = true;
      renderPerf();
      status("Live stream connected.", false);
    };

    source.onmessage = function (event) {
      const payload = JSON.parse(event.data);
      syncPayload(payload);
      renderChart(false);
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
    const payload = await fetchJson(nextUrl(config, state.rangeLastId, state.reviewEndId));
    syncPayload(payload);
    renderChart(false);
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
    if (state.rangeFirstId == null || state.rangeLastId == null) {
      status("Load the chart first.", true);
      return;
    }
    clearActivity();
    const payload = await fetchJson(previousUrl(currentConfig()));
    syncPayload(payload);
    renderChart(false);
    status(state.bars.length ? "Older zig candles shifted into view." : "No older data was available.", false);
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

  bindSegment(elements.levelToggle, function (value) {
    setSegment(elements.levelToggle, value);
    writeQuery();
    status("Level updated. Click Load to refresh data.", false);
  });

  bindSegment(elements.seriesToggle, function (value) {
    setSegment(elements.seriesToggle, value);
    writeQuery();
    status("Series updated. Click Load to refresh data.", false);
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

  [elements.showProvisional, elements.showTable].forEach((control) => {
    control.addEventListener("change", function () {
      elements.tablePanel.hidden = !elements.showTable.checked;
      writeQuery();
      queueChartResize();
      status("Settings updated. Click Load to refresh derived bars.", false);
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

  applyInitialConfig(parseQuery());
  loadAll(true);
})();
