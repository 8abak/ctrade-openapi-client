(function () {
  if (window.__datavisSeparationInitialized) {
    return;
  }
  window.__datavisSeparationInitialized = true;

  const DEFAULTS = {
    mode: "live",
    run: "run",
    showMicro: true,
    showMedian: true,
    showMacro: true,
    showTicks: false,
    includeOpen: true,
    id: "",
    reviewStart: "",
    reviewSpeed: 1,
    window: 160,
  };
  const REVIEW_SPEEDS = [0.5, 1, 2, 3, 5];
  const MAX_WINDOW = 4000;
  const TRADE_POLL_INTERVAL_MS = 15000;
  const SMART_POLL_INTERVAL_MS = 2000;
  const LEVEL_ORDER = { macro: 0, median: 1, micro: 2 };
  const LEVEL_STYLE = {
    micro: { z: 8, alpha: 0.22, lineWidth: 1.1, emphasis: 0.92 },
    median: { z: 5, alpha: 0.28, lineWidth: 1.5, emphasis: 0.96 },
    macro: { z: 2, alpha: 0.20, lineWidth: 1.9, emphasis: 0.98 },
  };
  const SHAPE_LABELS = { spike: "Spike", drift: "Drift", oval: "Oval", balance: "Balance", transition: "Transition" };
  const DIRECTION_COLORS = {
    up: { fill: "255,179,92", stroke: "#ffb35c", center: "#ffd29a" },
    down: { fill: "126,240,199", stroke: "#7ef0c7", center: "#9ff7d5" },
    flat: { fill: "109,216,255", stroke: "#6dd8ff", center: "#b5ebff" },
  };

  const state = {
    chart: null,
    segments: [],
    rows: [],
    source: null,
    reviewTimer: 0,
    reviewEndId: null,
    loadToken: 0,
    lastMetrics: null,
    streamConnected: false,
    hasMoreLeft: false,
    spanFirstId: null,
    spanLastId: null,
    spanStartMs: null,
    spanEndMs: null,
    rightEdgeAnchored: true,
    zoom: null,
    applyingZoom: false,
    resizeObserver: null,
    ui: { sidebarCollapsed: true },
    trade: {
      authConfigured: true,
      authenticated: false,
      username: null,
      loginBusy: false,
      actionBusy: false,
      brokerConfigured: false,
      brokerStatus: null,
      positions: [],
      refreshPromise: null,
      pollTimer: 0,
      smart: {
        payload: null,
        refreshPromise: null,
        pollTimer: 0,
      },
    },
  };

  const elements = {
    workspace: document.getElementById("separationWorkspace"),
    sidebar: document.getElementById("separationSidebar"),
    sidebarToggle: document.getElementById("sidebarToggle"),
    sidebarBackdrop: document.getElementById("sidebarBackdrop"),
    modeToggle: document.getElementById("modeToggle"),
    runToggle: document.getElementById("runToggle"),
    showMicro: document.getElementById("showMicro"),
    showMedian: document.getElementById("showMedian"),
    showMacro: document.getElementById("showMacro"),
    showAllButton: document.getElementById("showAllButton"),
    showTicks: document.getElementById("showTicks"),
    includeOpen: document.getElementById("includeOpen"),
    tickId: document.getElementById("tickId"),
    reviewStart: document.getElementById("reviewStart"),
    reviewSpeedToggle: document.getElementById("reviewSpeedToggle"),
    windowSize: document.getElementById("windowSize"),
    applyButton: document.getElementById("applyButton"),
    loadMoreLeftButton: document.getElementById("loadMoreLeftButton"),
    statusLine: document.getElementById("statusLine"),
    separationMeta: document.getElementById("separationMeta"),
    separationPerf: document.getElementById("separationPerf"),
    chartHost: document.getElementById("separationChart"),
    tradeStatusLine: document.getElementById("tradeStatusLine"),
    tradeLoginForm: document.getElementById("tradeLoginForm"),
    tradeUsername: document.getElementById("tradeUsername"),
    tradePassword: document.getElementById("tradePassword"),
    tradeLoginButton: document.getElementById("tradeLoginButton"),
    tradeLogoutButton: document.getElementById("tradeLogoutButton"),
    tradeSessionSummary: document.getElementById("tradeSessionSummary"),
    tradeBrokerSummary: document.getElementById("tradeBrokerSummary"),
    loginStatePill: document.getElementById("loginStatePill"),
    buttonStatePill: document.getElementById("buttonStatePill"),
    buttonsSummaryLine: document.getElementById("buttonsSummaryLine"),
    separationSmartBuyButton: document.getElementById("separationSmartBuyButton"),
    separationSmartSellButton: document.getElementById("separationSmartSellButton"),
    separationSmartCloseButton: document.getElementById("separationSmartCloseButton"),
    separationSmartStatus: document.getElementById("separationSmartStatus"),
    separationTradeHint: document.getElementById("separationTradeHint"),
    manualCloseButton: document.getElementById("manualCloseButton"),
  };

  function sanitizeWindowValue(rawValue) {
    return Math.max(1, Math.min(MAX_WINDOW, Number.parseInt(rawValue || String(DEFAULTS.window), 10) || DEFAULTS.window));
  }

  function currentLevels() {
    const levels = [];
    if (elements.showMicro.checked) { levels.push("micro"); }
    if (elements.showMedian.checked) { levels.push("median"); }
    if (elements.showMacro.checked) { levels.push("macro"); }
    return levels.length ? levels : ["micro", "median", "macro"];
  }

  function parseQuery() {
    const params = new URLSearchParams(window.location.search);
    const speed = Number.parseFloat(params.get("speed") || String(DEFAULTS.reviewSpeed));
    const levels = (params.get("levels") || "micro,median,macro").split(",");
    return {
      mode: params.get("mode") === "review" ? "review" : DEFAULTS.mode,
      run: params.get("run") === "stop" ? "stop" : DEFAULTS.run,
      showMicro: levels.includes("micro"),
      showMedian: levels.includes("median"),
      showMacro: levels.includes("macro"),
      showTicks: params.get("showTicks") === "1",
      includeOpen: params.get("includeOpen") !== "0",
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
      levels: currentLevels(),
      showTicks: elements.showTicks.checked,
      includeOpen: elements.includeOpen.checked,
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
      levels: config.levels.join(","),
      showTicks: config.showTicks ? "1" : "0",
      includeOpen: config.includeOpen ? "1" : "0",
      window: String(config.window),
      speed: String(config.reviewSpeed),
    });
    if (config.id) { params.set("id", config.id); }
    if (config.reviewStart) { params.set("reviewStart", config.reviewStart); }
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
    elements.reviewSpeedToggle.querySelectorAll("button").forEach(function (button) {
      button.disabled = !reviewMode;
    });
  }

  function status(message, isError) {
    elements.statusLine.textContent = message;
    elements.statusLine.classList.toggle("error", Boolean(isError));
  }

  function renderMeta() {
    if (state.spanLastId == null) {
      elements.separationMeta.textContent = "No separation range loaded.";
      return;
    }
    const counts = { micro: 0, median: 0, macro: 0 };
    let openCount = 0;
    state.segments.forEach(function (segment) {
      counts[String(segment.level)] += 1;
      if (segment.status === "open") {
        openCount += 1;
      }
    });
    elements.separationMeta.textContent = [
      currentConfig().mode.toUpperCase(),
      "packets " + state.segments.length + "/" + currentConfig().window,
      "left " + state.spanFirstId,
      "right " + state.spanLastId,
      "micro " + counts.micro,
      "median " + counts.median,
      "macro " + counts.macro,
      "open " + openCount,
      state.hasMoreLeft ? "more-left yes" : "more-left no",
    ].join(" | ");
  }

  function renderPerf() {
    const metrics = state.lastMetrics || {};
    const parts = ["Stream " + (state.streamConnected ? "up" : "down")];
    if (metrics.dbLatestId != null) { parts.push("DB " + metrics.dbLatestId); }
    if (metrics.fetchLatencyMs != null) { parts.push("Fetch " + Math.round(metrics.fetchLatencyMs * 100) / 100 + "ms"); }
    if (metrics.serializeLatencyMs != null) { parts.push("Serialize " + Math.round(metrics.serializeLatencyMs * 100) / 100 + "ms"); }
    if (metrics.serverSentAtMs != null) { parts.push("Wire " + Math.max(0, Date.now() - metrics.serverSentAtMs) + "ms"); }
    elements.separationPerf.textContent = parts.join(" | ");
  }

  function escapeHtml(value) {
    return String(value)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll("\"", "&quot;");
  }

  function formatTimestamp(value) {
    const timestamp = Number(value);
    return Number.isFinite(timestamp) ? new Date(timestamp).toLocaleString() : "-";
  }

  function formatPrice(value) {
    const number = Number(value);
    return Number.isFinite(number) ? number.toFixed(2) : "-";
  }

  function tooltipHtml(param) {
    const segment = param?.data?.segment;
    if (!segment) {
      return "";
    }
    return [
      "<div class=\"chart-tip\">",
      "<div class=\"chart-tip-section\">",
      "<div class=\"chart-tip-title\">", escapeHtml(String(segment.level || "").toUpperCase()), " / ", escapeHtml(SHAPE_LABELS[segment.shapetype] || segment.shapetype || "-"), "</div>",
      "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Direction</span><span class=\"chart-tip-value\">", escapeHtml(segment.direction || "-"), "</span></div>",
      "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Status</span><span class=\"chart-tip-value\">", escapeHtml(segment.status || "-"), "</span></div>",
      "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Start</span><span class=\"chart-tip-value\">", escapeHtml(formatTimestamp(segment.starttimeMs)), "</span></div>",
      "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">End</span><span class=\"chart-tip-value\">", escapeHtml(formatTimestamp(segment.endtimeMs)), "</span></div>",
      "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Start price</span><span class=\"chart-tip-value\">", escapeHtml(formatPrice(segment.startprice)), "</span></div>",
      "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">End price</span><span class=\"chart-tip-value\">", escapeHtml(formatPrice(segment.endprice)), "</span></div>",
      "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">High / Low</span><span class=\"chart-tip-value\">", escapeHtml(formatPrice(segment.highprice)), " / ", escapeHtml(formatPrice(segment.lowprice)), "</span></div>",
      "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Net move</span><span class=\"chart-tip-value\">", escapeHtml(formatPrice(segment.netmove)), "</span></div>",
      "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Range</span><span class=\"chart-tip-value\">", escapeHtml(formatPrice(segment.rangeprice)), "</span></div>",
      "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Efficiency</span><span class=\"chart-tip-value\">", escapeHtml(Number(segment.efficiency || 0).toFixed(3)), "</span></div>",
      "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Thickness</span><span class=\"chart-tip-value\">", escapeHtml(Number(segment.thickness || 0).toFixed(3)), "</span></div>",
      "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Count</span><span class=\"chart-tip-value\">", escapeHtml(String(segment.tickcount || 0)), "</span></div>",
      "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Unit</span><span class=\"chart-tip-value\">", escapeHtml(formatPrice(segment.unitprice)), "</span></div>",
      "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Source</span><span class=\"chart-tip-value\">", escapeHtml(segment.sourcemode || "-"), "</span></div>",
      "</div>",
      "</div>",
    ].join("");
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
          formatter: tooltipHtml,
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
        state.rightEdgeAnchored = !zoom || Number(zoom.end) >= 99.5;
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

  function levelIndex(level) {
    return LEVEL_ORDER[String(level)] ?? 2;
  }

  function shapeIndex(shape) {
    return ["transition", "balance", "drift", "spike", "oval"].indexOf(String(shape || "transition"));
  }

  function segmentSeriesData(level) {
    return state.segments
      .filter(function (segment) { return segment.level === level; })
      .map(function (segment) {
        return {
          value: [
            Number(segment.starttimeMs),
            Number(segment.endtimeMs),
            Number(segment.lowprice),
            Number(segment.highprice),
            Number(segment.startprice),
            Number(segment.endprice),
            segment.direction === "up" ? 1 : (segment.direction === "down" ? -1 : 0),
            segment.status === "open" ? 1 : 0,
            levelIndex(segment.level),
            shapeIndex(segment.shapetype),
          ],
          segment: segment,
        };
      });
  }

  function rowsSeriesData() {
    return state.rows.map(function (row) {
      return [Number(row.timestampMs), Number(row.mid)];
    });
  }

  function segmentRender(params, api) {
    const startMs = Number(api.value(0));
    const endMs = Number(api.value(1));
    const low = Number(api.value(2));
    const high = Number(api.value(3));
    const startPrice = Number(api.value(4));
    const endPrice = Number(api.value(5));
    const directionValue = Number(api.value(6));
    const openValue = Number(api.value(7));
    const levelValue = Number(api.value(8));
    if (![startMs, endMs, low, high, startPrice, endPrice].every(Number.isFinite)) {
      return null;
    }
    const level = levelValue === 0 ? "macro" : (levelValue === 1 ? "median" : "micro");
    const palette = directionValue > 0 ? DIRECTION_COLORS.up : (directionValue < 0 ? DIRECTION_COLORS.down : DIRECTION_COLORS.flat);
    const levelStyle = LEVEL_STYLE[level];
    const leftBottom = api.coord([startMs, low]);
    const rightTop = api.coord([endMs, high]);
    const startCenter = api.coord([startMs, startPrice]);
    const endCenter = api.coord([endMs, endPrice]);
    const rect = echarts.graphic.clipRectByRect({
      x: Math.min(leftBottom[0], rightTop[0]),
      y: Math.min(leftBottom[1], rightTop[1]),
      width: Math.max(3, Math.abs(rightTop[0] - leftBottom[0])),
      height: Math.max(3, Math.abs(rightTop[1] - leftBottom[1])),
      r: Math.max(3, Math.min(12, Math.abs(rightTop[0] - leftBottom[0]) * 0.18)),
    }, params.coordSys);
    if (!rect) {
      return null;
    }
    const open = openValue > 0;
    const alpha = open ? Math.min(0.98, levelStyle.alpha + 0.16) : levelStyle.alpha;
    return {
      type: "group",
      children: [
        {
          type: "rect",
          shape: { x: rect.x, y: rect.y, width: rect.width, height: rect.height, r: rect.r || 0 },
          style: {
            fill: "rgba(" + palette.fill + "," + (alpha * 0.45) + ")",
            stroke: palette.stroke,
            lineWidth: open ? levelStyle.lineWidth + 0.6 : levelStyle.lineWidth,
            opacity: levelStyle.emphasis,
            lineDash: open ? [6, 4] : [],
          },
        },
        {
          type: "line",
          shape: { x1: startCenter[0], y1: startCenter[1], x2: endCenter[0], y2: endCenter[1] },
          style: {
            stroke: palette.center,
            lineWidth: open ? levelStyle.lineWidth + 0.4 : levelStyle.lineWidth,
            opacity: open ? 1 : 0.86,
          },
          silent: true,
        },
      ],
    };
  }

  function buildSeries() {
    const series = [];
    if (currentConfig().showTicks && state.rows.length) {
      series.push({
        id: "ticks",
        type: "line",
        data: rowsSeriesData(),
        showSymbol: false,
        animation: false,
        lineStyle: { color: "rgba(147, 164, 189, 0.38)", width: 1.0 },
        z: 1,
      });
    }
    ["macro", "median", "micro"].forEach(function (level) {
      if (!currentConfig().levels.includes(level)) {
        return;
      }
      series.push({
        id: "segments-" + level,
        name: level,
        type: "custom",
        renderItem: segmentRender,
        data: segmentSeriesData(level),
        animation: false,
        z: LEVEL_STYLE[level].z,
      });
    });
    return series;
  }

  function yBounds() {
    const values = [];
    state.segments.forEach(function (segment) {
      values.push(Number(segment.highprice), Number(segment.lowprice));
    });
    state.rows.forEach(function (row) {
      values.push(Number(row.mid));
    });
    const finite = values.filter(Number.isFinite);
    if (!finite.length) {
      return {};
    }
    const low = Math.min.apply(null, finite);
    const high = Math.max.apply(null, finite);
    const span = Math.max(0, high - low);
    const padding = span > 0 ? Math.max(span * 0.06, 0.02) : 0.05;
    return { min: low - padding, max: high + padding };
  }

  function renderChart(options) {
    const chart = ensureChart();
    if (!chart) {
      requestAnimationFrame(function () { renderChart(options); });
      return;
    }
    const zoom = {};
    if (options?.resetView || state.rightEdgeAnchored) {
      zoom.start = 0;
      zoom.end = 100;
    } else if (state.zoom) {
      zoom.start = state.zoom.start;
      zoom.end = state.zoom.end;
      zoom.startValue = state.zoom.startValue;
      zoom.endValue = state.zoom.endValue;
    }
    state.applyingZoom = true;
    chart.setOption({
      xAxis: {
        min: state.spanStartMs != null ? Number(state.spanStartMs) : null,
        max: state.spanEndMs != null ? Number(state.spanEndMs) : null,
      },
      yAxis: yBounds(),
      series: buildSeries(),
      dataZoom: [
        { id: "zoom-inside", ...zoom },
        { id: "zoom-slider", ...zoom },
      ],
    }, { replaceMerge: ["series"], lazyUpdate: true });
    requestAnimationFrame(function () {
      state.applyingZoom = false;
    });
  }

  function compareSegments(left, right) {
    return Number(left.starttickid) - Number(right.starttickid)
      || Number(left.endtickid) - Number(right.endtickid)
      || levelIndex(left.level) - levelIndex(right.level)
      || Number(left.id) - Number(right.id);
  }

  function syncSpan() {
    const ids = [];
    const times = [];
    state.segments.forEach(function (segment) {
      ids.push(Number(segment.starttickid), Number(segment.endtickid));
      times.push(Number(segment.starttimeMs), Number(segment.endtimeMs));
    });
    state.rows.forEach(function (row) {
      ids.push(Number(row.id));
      times.push(Number(row.timestampMs));
    });
    const finiteIds = ids.filter(Number.isFinite);
    const finiteTimes = times.filter(Number.isFinite);
    state.spanFirstId = finiteIds.length ? Math.min.apply(null, finiteIds) : null;
    state.spanLastId = finiteIds.length ? Math.max.apply(null, finiteIds) : null;
    state.spanStartMs = finiteTimes.length ? Math.min.apply(null, finiteTimes) : null;
    state.spanEndMs = finiteTimes.length ? Math.max.apply(null, finiteTimes) : null;
  }

  function replaceSegments(items) {
    state.segments = (items || []).slice().sort(compareSegments);
    syncSpan();
  }

  function replaceRows(items) {
    state.rows = Array.isArray(items) ? items.slice() : [];
    syncSpan();
  }

  function mergeSegments(items) {
    const byId = new Map();
    state.segments.forEach(function (segment) {
      byId.set(String(segment.id), segment);
    });
    (items || []).forEach(function (segment) {
      byId.set(String(segment.id), segment);
    });
    state.segments = Array.from(byId.values()).sort(compareSegments);
    syncSpan();
  }

  function appendRows(items) {
    const byId = new Map(state.rows.map(function (row) { return [String(row.id), row]; }));
    (items || []).forEach(function (row) {
      byId.set(String(row.id), row);
    });
    state.rows = Array.from(byId.values()).sort(function (left, right) {
      return Number(left.id) - Number(right.id);
    });
    syncSpan();
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
    const payload = await response.json().catch(function () { return {}; });
    if (!response.ok) {
      throw new Error(payload.detail || "Request failed.");
    }
    return payload;
  }

  async function resolveReviewStartId(config) {
    if (config.reviewStart) {
      const payload = await fetchJson("/api/separation/review-start?" + new URLSearchParams({
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
      levels: config.levels.join(","),
      showAll: "0",
      includeOpen: config.includeOpen ? "1" : "0",
      showTicks: config.showTicks ? "1" : "0",
    });
    if (config.mode === "review" && startId != null) {
      params.set("id", String(startId));
    }
    return "/api/separation/bootstrap?" + params.toString();
  }

  async function loadBootstrap(resetView) {
    const config = currentConfig();
    const startId = config.mode === "review" ? await resolveReviewStartId(config) : null;
    const payload = await fetchJson(bootstrapUrl(config, startId));
    replaceSegments(payload.segments || []);
    replaceRows(payload.rows || []);
    state.reviewEndId = payload.reviewEndId || null;
    state.hasMoreLeft = Boolean(payload.hasMoreLeft);
    state.lastMetrics = payload.metrics || null;
    if (resetView) {
      state.zoom = null;
      state.rightEdgeAnchored = true;
    }
    renderMeta();
    renderPerf();
    renderChart({ resetView: Boolean(resetView) });
    status("Loaded " + state.segments.length + " packet(s).", false);
    if (config.run === "run") {
      if (config.mode === "live") {
        connectStream(state.spanLastId || 0);
      } else {
        connectReviewStream(state.spanLastId || 0, state.reviewEndId || 0);
      }
    }
  }

  async function loadMoreLeft() {
    if (state.spanFirstId == null) {
      status("Load the chart first.", true);
      return;
    }
    clearActivity();
    const config = currentConfig();
    const payload = await fetchJson("/api/separation/previous?" + new URLSearchParams({
      beforeId: String(state.spanFirstId),
      limit: String(config.window),
      levels: config.levels.join(","),
      showAll: "0",
      includeOpen: config.includeOpen ? "1" : "0",
      showTicks: config.showTicks ? "1" : "0",
    }).toString());
    const merged = (payload.segments || []).concat(state.segments);
    replaceSegments(merged);
    if (config.showTicks) {
      appendRows(payload.rows || []);
    }
    state.hasMoreLeft = Boolean(payload.hasMoreLeft);
    state.lastMetrics = payload.metrics || null;
    renderMeta();
    renderPerf();
    renderChart({ shiftWithRun: false });
    if (currentConfig().run === "run") {
      resumeRunIfNeeded();
    }
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

  function connectStream(afterId) {
    clearActivity();
    const config = currentConfig();
    const source = new EventSource("/api/separation/stream?" + new URLSearchParams({
      afterId: String(afterId || 0),
      limit: "250",
      levels: config.levels.join(","),
      showAll: "0",
      includeOpen: config.includeOpen ? "1" : "0",
      showTicks: config.showTicks ? "1" : "0",
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
      mergeSegments(payload.segmentUpdates || []);
      appendRows(payload.rows || []);
      renderMeta();
      renderPerf();
      renderChart({ shiftWithRun: currentConfig().run === "run" });
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

  function connectReviewStream(afterId, endId) {
    clearActivity();
    const config = currentConfig();
    if (!endId || afterId >= endId) {
      status("Review reached the current end snapshot.", false);
      return;
    }
    const source = new EventSource("/api/separation/review-stream?" + new URLSearchParams({
      afterId: String(afterId || 0),
      endId: String(endId),
      speed: String(config.reviewSpeed),
      levels: config.levels.join(","),
      showAll: "0",
      includeOpen: config.includeOpen ? "1" : "0",
      showTicks: config.showTicks ? "1" : "0",
    }).toString());
    state.source = source;
    source.onopen = function () {
      state.streamConnected = true;
      renderPerf();
      status("Review replay connected.", false);
    };
    source.onmessage = function (event) {
      const payload = JSON.parse(event.data);
      state.lastMetrics = payload;
      mergeSegments(payload.segmentUpdates || []);
      appendRows(payload.rows || []);
      renderMeta();
      renderPerf();
      renderChart({ shiftWithRun: true });
      if (payload.endReached) {
        clearActivity();
        status("Review reached the current end snapshot.", false);
      }
    };
    source.onerror = function () {
      clearActivity();
      status("Review replay disconnected. Click Load or Run to reconnect.", true);
    };
  }

  function resumeRunIfNeeded() {
    const config = currentConfig();
    if (config.run !== "run" || state.spanLastId == null) {
      return;
    }
    if (config.mode === "live") {
      connectStream(state.spanLastId);
    } else {
      connectReviewStream(state.spanLastId, state.reviewEndId);
    }
  }

  function brokerStatusFromPayload(payload) {
    return payload?.broker || payload?.payload?.broker || payload || { configured: false, ready: false, reason: "Broker state unavailable." };
  }

  function smartPayload() {
    return state.trade.smart.payload || {
      context: { enabled: false, reason: "Smart scalping unavailable." },
      state: { armed: { buy: false, sell: false, close: false }, statusText: "Idle", openPositionCount: 0 },
    };
  }

  function currentSmartArmed(key) {
    return Boolean(smartPayload()?.state?.armed?.[key]);
  }

  function smartContextReady() {
    const config = currentConfig();
    return config.mode === "live" && config.run === "run";
  }

  function tradeStatus(message, isError) {
    elements.tradeStatusLine.textContent = message;
    elements.tradeStatusLine.classList.toggle("error", Boolean(isError));
  }

  function setTradeBusy(busy) {
    state.trade.actionBusy = Boolean(busy);
    [
      elements.tradeUsername,
      elements.tradePassword,
      elements.tradeLoginButton,
      elements.tradeLogoutButton,
      elements.separationSmartBuyButton,
      elements.separationSmartSellButton,
      elements.separationSmartCloseButton,
      elements.manualCloseButton,
    ].forEach(function (element) {
      if (element) {
        element.disabled = Boolean(busy);
      }
    });
    renderTradePanel();
  }

  function tradeFetchJson(url, options) {
    const request = Object.assign({ headers: { "Content-Type": "application/json" } }, options || {});
    return fetch(url, request).then(async function (response) {
      const payload = await response.json().catch(function () { return {}; });
      if (!response.ok) {
        const detail = payload?.detail?.message || payload?.detail || payload?.message || "Trade request failed.";
        const error = new Error(detail);
        error.payload = payload;
        throw error;
      }
      return payload;
    });
  }

  function applyTradeSessionPayload(payload) {
    state.trade.authConfigured = payload?.authConfigured !== false;
    state.trade.authenticated = Boolean(payload?.authenticated) && state.trade.authConfigured;
    state.trade.username = state.trade.authenticated ? (payload?.username || null) : null;
    state.trade.brokerStatus = brokerStatusFromPayload(payload);
    state.trade.brokerConfigured = Boolean(state.trade.brokerStatus?.configured);
    if (!state.trade.authenticated) {
      state.trade.positions = [];
      state.trade.smart.payload = null;
      stopTradePolling();
      stopSmartPolling();
    }
    renderTradePanel();
  }

  function tradeHintText() {
    if (!state.trade.authConfigured) {
      return "Trade login is not configured on the server.";
    }
    if (!state.trade.authenticated) {
      return "Login required.";
    }
    if (!smartContextReady()) {
      return "Smart controls require Live + Run.";
    }
    if (!state.trade.brokerConfigured || !state.trade.brokerStatus?.ready) {
      return state.trade.brokerStatus?.reason || "Broker state unavailable.";
    }
    if (currentSmartArmed("close") && state.trade.positions.length !== 1) {
      return "Smart Close is armed and waiting for a single open position.";
    }
    return state.trade.positions.length ? (state.trade.positions.length + " open position(s).") : "No open position. Smart Close can stay armed.";
  }

  function renderTradePanel() {
    const smart = smartPayload();
    const armedCount = ["buy", "sell", "close"].filter(currentSmartArmed).length;
    if (elements.loginStatePill) {
      elements.loginStatePill.textContent = state.trade.authenticated ? "Ready" : (state.trade.authConfigured ? "Locked" : "Unavailable");
    }
    if (elements.tradeSessionSummary) {
      elements.tradeSessionSummary.textContent = state.trade.authenticated ? ((state.trade.username || "trade user") + " | " + (state.trade.brokerStatus?.symbol || "Broker")) : "Trade login required.";
    }
    if (elements.tradeBrokerSummary) {
      elements.tradeBrokerSummary.textContent = state.trade.brokerStatus?.reason || (state.trade.brokerConfigured ? "Broker ready." : "Broker state unavailable.");
    }
    if (elements.buttonStatePill) {
      elements.buttonStatePill.textContent = armedCount ? "ON" : "OFF";
    }
    if (elements.buttonsSummaryLine) {
      elements.buttonsSummaryLine.textContent = state.trade.authenticated ? (smart.state?.statusText || tradeHintText()) : tradeHintText();
    }
    [
      [elements.separationSmartBuyButton, "buy"],
      [elements.separationSmartSellButton, "sell"],
      [elements.separationSmartCloseButton, "close"],
    ].forEach(function (entry) {
      const button = entry[0];
      const key = entry[1];
      if (!button) {
        return;
      }
      button.classList.toggle("is-armed", currentSmartArmed(key));
      button.textContent = "Smart " + key.charAt(0).toUpperCase() + key.slice(1) + " " + (currentSmartArmed(key) ? "ON" : "OFF");
    });
    if (elements.separationSmartStatus) {
      elements.separationSmartStatus.textContent = smart.state?.statusText || "Smart scalping unavailable.";
    }
    if (elements.separationTradeHint) {
      elements.separationTradeHint.textContent = tradeHintText();
    }
    if (elements.manualCloseButton) {
      elements.manualCloseButton.disabled = state.trade.actionBusy || !state.trade.authenticated || state.trade.positions.length !== 1;
    }
  }

  async function refreshTradeData(options) {
    if (!state.trade.authenticated) {
      return;
    }
    if (state.trade.refreshPromise) {
      return state.trade.refreshPromise;
    }
    state.trade.refreshPromise = (async function () {
      const payload = await tradeFetchJson("/api/trade/open");
      state.trade.brokerStatus = brokerStatusFromPayload(payload);
      state.trade.brokerConfigured = Boolean(state.trade.brokerStatus?.configured);
      state.trade.positions = Array.isArray(payload.positions) ? payload.positions : [];
      state.trade.smart.payload = payload.smart || state.trade.smart.payload;
      renderTradePanel();
      if (!options?.silent) {
        tradeStatus("Trade state updated.", false);
      }
      if (smartContextReady()) {
        scheduleSmartPolling();
      } else {
        stopSmartPolling();
      }
      scheduleTradePolling();
    })().finally(function () {
      state.trade.refreshPromise = null;
    });
    return state.trade.refreshPromise;
  }

  async function refreshSmartState() {
    if (!state.trade.authenticated) {
      return;
    }
    if (state.trade.smart.refreshPromise) {
      return state.trade.smart.refreshPromise;
    }
    state.trade.smart.refreshPromise = tradeFetchJson("/api/trade/smart").then(function (payload) {
      state.trade.smart.payload = payload;
      renderTradePanel();
      if (smartContextReady()) {
        scheduleSmartPolling();
      }
    }).finally(function () {
      state.trade.smart.refreshPromise = null;
    });
    return state.trade.smart.refreshPromise;
  }

  async function syncSmartContext() {
    if (!state.trade.authenticated) {
      return;
    }
    const payload = await tradeFetchJson("/api/trade/smart/context", {
      method: "POST",
      body: JSON.stringify({ page: "separation", mode: currentConfig().mode, run: currentConfig().run }),
    });
    state.trade.smart.payload = payload;
    renderTradePanel();
    scheduleSmartPolling();
  }

  async function toggleSmartEntry(side) {
    if (!state.trade.authenticated || state.trade.actionBusy) {
      return;
    }
    await syncSmartContext().catch(function () {});
    const payload = await tradeFetchJson("/api/trade/smart/entry", {
      method: "POST",
      body: JSON.stringify({ side: side, armed: !currentSmartArmed(side) }),
    });
    state.trade.smart.payload = payload;
    renderTradePanel();
    tradeStatus("Smart " + side.toUpperCase() + " updated.", false);
  }

  async function toggleSmartClose() {
    if (!state.trade.authenticated || state.trade.actionBusy) {
      return;
    }
    await syncSmartContext().catch(function () {});
    const payload = await tradeFetchJson("/api/trade/smart/close", {
      method: "POST",
      body: JSON.stringify({ armed: !currentSmartArmed("close") }),
    });
    state.trade.smart.payload = payload;
    renderTradePanel();
    tradeStatus("Smart Close updated.", false);
  }

  async function requestTradeLogin() {
    if (state.trade.loginBusy || state.trade.actionBusy) {
      return;
    }
    state.trade.loginBusy = true;
    renderTradePanel();
    try {
      const payload = await tradeFetchJson("/api/trade/login", {
        method: "POST",
        body: JSON.stringify({ username: elements.tradeUsername.value, password: elements.tradePassword.value }),
      });
      elements.tradePassword.value = "";
      applyTradeSessionPayload({ authenticated: true, username: payload.username, authConfigured: true, broker: state.trade.brokerStatus });
      await refreshTradeData({ silent: true }).catch(function () {});
      await syncSmartContext().catch(function () {});
      tradeStatus("Trade login successful.", false);
    } catch (error) {
      tradeStatus(error.message || "Trade login failed.", true);
    } finally {
      state.trade.loginBusy = false;
      renderTradePanel();
    }
  }

  async function requestTradeLogout() {
    if (state.trade.actionBusy) {
      return;
    }
    setTradeBusy(true);
    try {
      await tradeFetchJson("/api/trade/logout", { method: "POST" });
    } catch (error) {
      void error;
    }
    applyTradeSessionPayload({ authenticated: false, username: null, authConfigured: state.trade.authConfigured, broker: state.trade.brokerStatus });
    tradeStatus("Trade session logged out.", false);
    setTradeBusy(false);
  }

  async function requestManualClose() {
    if (!state.trade.authenticated || state.trade.positions.length !== 1 || state.trade.actionBusy) {
      return;
    }
    const position = state.trade.positions[0];
    setTradeBusy(true);
    try {
      const payload = await tradeFetchJson("/api/trade/position/close", {
        method: "POST",
        body: JSON.stringify({ positionId: Number(position.positionId), volume: Number(position.volume) }),
      });
      state.trade.smart.payload = payload.smart || state.trade.smart.payload;
      tradeStatus("Position close submitted.", false);
      await refreshTradeData({ silent: true });
    } catch (error) {
      tradeStatus(error.message || "Manual close failed.", true);
    } finally {
      setTradeBusy(false);
    }
  }

  function stopTradePolling() {
    if (state.trade.pollTimer) {
      window.clearTimeout(state.trade.pollTimer);
      state.trade.pollTimer = 0;
    }
  }

  function scheduleTradePolling() {
    stopTradePolling();
    if (!state.trade.authenticated) {
      return;
    }
    state.trade.pollTimer = window.setTimeout(function () {
      refreshTradeData({ silent: true }).catch(function () {});
    }, TRADE_POLL_INTERVAL_MS);
  }

  function stopSmartPolling() {
    if (state.trade.smart.pollTimer) {
      window.clearTimeout(state.trade.smart.pollTimer);
      state.trade.smart.pollTimer = 0;
    }
  }

  function scheduleSmartPolling() {
    stopSmartPolling();
    if (!state.trade.authenticated || !smartContextReady()) {
      return;
    }
    state.trade.smart.pollTimer = window.setTimeout(function () {
      refreshSmartState().catch(function () {});
    }, SMART_POLL_INTERVAL_MS);
  }

  async function loadTradeSession() {
    try {
      const payload = await tradeFetchJson("/api/trade/me");
      applyTradeSessionPayload(payload);
      if (payload.authenticated) {
        await refreshTradeData({ silent: true }).catch(function () {});
        await syncSmartContext().catch(function () {});
      }
      tradeStatus(payload.authenticated ? "Trade session active." : "Trade login required.", false);
    } catch (error) {
      applyTradeSessionPayload({ authenticated: false, username: null, authConfigured: true, broker: state.trade.brokerStatus });
      tradeStatus(error.message || "Trade session check failed.", true);
    }
  }

  function applyInitialConfig(config) {
    setSegment(elements.modeToggle, config.mode);
    setSegment(elements.runToggle, config.run);
    setSegment(elements.reviewSpeedToggle, config.reviewSpeed);
    elements.showMicro.checked = Boolean(config.showMicro);
    elements.showMedian.checked = Boolean(config.showMedian);
    elements.showMacro.checked = Boolean(config.showMacro);
    elements.showTicks.checked = Boolean(config.showTicks);
    elements.includeOpen.checked = Boolean(config.includeOpen);
    elements.tickId.value = config.id;
    elements.reviewStart.value = config.reviewStart;
    elements.windowSize.value = String(config.window);
    setSidebarCollapsed(true);
    updateReviewFields();
    renderMeta();
    renderPerf();
    renderTradePanel();
    writeQuery();
  }

  bindSegment(elements.modeToggle, function (value) {
    setSegment(elements.modeToggle, value);
    updateReviewFields();
    writeQuery();
    syncSmartContext().catch(function () {});
    status("Mode updated. Click Load to refresh data.", false);
  });
  bindSegment(elements.runToggle, function (value) {
    setSegment(elements.runToggle, value);
    clearActivity();
    writeQuery();
    syncSmartContext().catch(function () {});
    if (value === "run" && state.spanLastId != null) {
      resumeRunIfNeeded();
    }
  });
  bindSegment(elements.reviewSpeedToggle, function (value) {
    setSegment(elements.reviewSpeedToggle, value);
    writeQuery();
    if (currentConfig().mode === "review" && currentConfig().run === "run") {
      resumeRunIfNeeded();
    }
  });

  [elements.showMicro, elements.showMedian, elements.showMacro, elements.showTicks, elements.includeOpen].forEach(function (control) {
    control.addEventListener("change", function () {
      if (!currentLevels().length) {
        elements.showMicro.checked = true;
      }
      writeQuery();
      loadAll(false);
    });
  });
  elements.showAllButton.addEventListener("click", function () {
    elements.showMicro.checked = true;
    elements.showMedian.checked = true;
    elements.showMacro.checked = true;
    writeQuery();
    loadAll(false);
  });
  [elements.tickId, elements.reviewStart, elements.windowSize].forEach(function (control) {
    control.addEventListener("change", function () {
      if (control === elements.windowSize) {
        elements.windowSize.value = String(sanitizeWindowValue(elements.windowSize.value));
      }
      writeQuery();
    });
  });
  elements.sidebarToggle.addEventListener("click", function () { setSidebarCollapsed(!state.ui.sidebarCollapsed); });
  elements.sidebarBackdrop.addEventListener("click", function () { setSidebarCollapsed(true); });
  elements.applyButton.addEventListener("click", function () { loadAll(true); });
  elements.loadMoreLeftButton.addEventListener("click", function () {
    loadMoreLeft().catch(function (error) { status(error.message || "Load More Left failed.", true); });
  });

  elements.tradeLoginForm.addEventListener("submit", function (event) {
    event.preventDefault();
    requestTradeLogin();
  });
  elements.tradeLogoutButton.addEventListener("click", function () { requestTradeLogout(); });
  elements.separationSmartBuyButton.addEventListener("click", function () { toggleSmartEntry("buy").catch(function (error) { tradeStatus(error.message || "Smart buy failed.", true); }); });
  elements.separationSmartSellButton.addEventListener("click", function () { toggleSmartEntry("sell").catch(function (error) { tradeStatus(error.message || "Smart sell failed.", true); }); });
  elements.separationSmartCloseButton.addEventListener("click", function () { toggleSmartClose().catch(function (error) { tradeStatus(error.message || "Smart close failed.", true); }); });
  elements.manualCloseButton.addEventListener("click", function () { requestManualClose(); });

  applyInitialConfig(parseQuery());
  loadTradeSession();
  loadAll(true);
}());
