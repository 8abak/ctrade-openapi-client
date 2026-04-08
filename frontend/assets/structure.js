(function () {
  const DEFAULTS = {
    mode: "live",
    run: "run",
    showEvents: false,
    showStructure: true,
    showRanges: true,
    id: "",
    reviewStart: "",
    reviewSpeed: 1,
    window: 50,
  };
  const INITIAL_LIVE_ITEM_WINDOW = 1;
  const BACKFILL_CHUNK_ITEMS = 4;
  const MAX_WINDOW = 200000;
  const REVIEW_SPEEDS = [0.5, 1, 2, 3, 5];
  const EVENT_COLORS = {
    highexpand: "#ffb35c",
    lowexpand: "#7ef0c7",
    pullback: "#93a4bd",
    reversalstart: "#ff5f7a",
    rangetop: "#ffc857",
    rangebottom: "#6dd8ff",
    rangebreakup: "#f6ad55",
    rangebreakdown: "#5eead4",
  };
  const STRUCTURE_STYLES = {
    up: { fill: "rgba(255,179,92,0.34)", stroke: "#ffb35c", boundary: "rgba(255,179,92,0.92)" },
    down: { fill: "rgba(126,240,199,0.30)", stroke: "#7ef0c7", boundary: "rgba(126,240,199,0.90)" },
    range: { fill: "rgba(109,216,255,0.16)", stroke: "#6dd8ff", boundary: "rgba(109,216,255,0.78)" },
  };

  const state = {
    chart: null,
    structureBars: [],
    rangeBoxes: [],
    structureEvents: [],
    source: null,
    reviewTimer: 0,
    reviewEndId: null,
    loadToken: 0,
    lastMetrics: null,
    streamConnected: false,
    hasMoreLeft: false,
    loadedWindow: 0,
    targetWindow: DEFAULTS.window,
    spanFirstId: null,
    spanLastId: null,
    spanStartMs: null,
    spanEndMs: null,
    rightEdgeAnchored: true,
    zoom: null,
    applyingZoom: false,
    resizeObserver: null,
    backfillTimer: 0,
    backfillToken: 0,
    ui: { sidebarCollapsed: true },
  };

  const elements = {
    workspace: document.getElementById("structureWorkspace"),
    sidebar: document.getElementById("structureSidebar"),
    sidebarToggle: document.getElementById("sidebarToggle"),
    sidebarBackdrop: document.getElementById("sidebarBackdrop"),
    modeToggle: document.getElementById("modeToggle"),
    runToggle: document.getElementById("runToggle"),
    showEvents: document.getElementById("showEvents"),
    showStructure: document.getElementById("showStructure"),
    showRanges: document.getElementById("showRanges"),
    tickId: document.getElementById("tickId"),
    reviewStart: document.getElementById("reviewStart"),
    reviewSpeedToggle: document.getElementById("reviewSpeedToggle"),
    windowSize: document.getElementById("windowSize"),
    applyButton: document.getElementById("applyButton"),
    loadMoreLeftButton: document.getElementById("loadMoreLeftButton"),
    statusLine: document.getElementById("statusLine"),
    structureMeta: document.getElementById("structureMeta"),
    structurePerf: document.getElementById("structurePerf"),
    chartHost: document.getElementById("structureChart"),
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
      showEvents: params.has("showEvents") ? params.get("showEvents") !== "0" : DEFAULTS.showEvents,
      showStructure: params.has("showStructure") ? params.get("showStructure") !== "0" : DEFAULTS.showStructure,
      showRanges: params.has("showRanges") ? params.get("showRanges") !== "0" : DEFAULTS.showRanges,
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
      showEvents: elements.showEvents.checked,
      showStructure: elements.showStructure.checked,
      showRanges: elements.showRanges.checked,
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
      showEvents: config.showEvents ? "1" : "0",
      showStructure: config.showStructure ? "1" : "0",
      showRanges: config.showRanges ? "1" : "0",
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

  function visibilityParams(config) {
    return {
      showEvents: config.showEvents ? "1" : "0",
      showStructure: config.showStructure ? "1" : "0",
      showRanges: config.showRanges ? "1" : "0",
    };
  }

  function setSidebarCollapsed(collapsed) {
    state.ui.sidebarCollapsed = Boolean(collapsed);
    elements.workspace.classList.toggle("is-sidebar-collapsed", state.ui.sidebarCollapsed);
    elements.sidebarToggle.setAttribute("aria-expanded", String(!state.ui.sidebarCollapsed));
    elements.sidebarToggle.setAttribute("aria-label", state.ui.sidebarCollapsed ? "Open structure map controls" : "Close structure map controls");
    elements.sidebarBackdrop.tabIndex = state.ui.sidebarCollapsed ? -1 : 0;
    if (state.chart) {
      requestAnimationFrame(function () {
        state.chart.resize();
      });
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

  function formatNumber(value, digits) {
    const number = Number(value);
    return Number.isFinite(number) ? number.toFixed(digits) : "-";
  }

  function formatDuration(value) {
    const ms = Number(value);
    if (!Number.isFinite(ms) || ms <= 0) {
      return "0m";
    }
    const totalSeconds = Math.round(ms / 1000);
    const days = Math.floor(totalSeconds / 86400);
    const hours = Math.floor((totalSeconds % 86400) / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    if (days > 0) {
      return days + "d " + String(hours).padStart(2, "0") + "h";
    }
    if (hours > 0) {
      return hours + "h " + String(minutes).padStart(2, "0") + "m";
    }
    return Math.max(1, minutes) + "m";
  }

  function formatTimestamp(value) {
    const timestamp = Number(value);
    if (!Number.isFinite(timestamp)) {
      return "-";
    }
    return new Date(timestamp).toLocaleString();
  }

  function renderMeta() {
    if (state.spanLastId == null) {
      elements.structureMeta.textContent = "No structure range loaded.";
      return;
    }
    const activeBars = state.structureBars.filter(function (bar) { return bar.status === "active"; }).length;
    const activeRanges = state.rangeBoxes.filter(function (box) { return box.status === "active"; }).length;
    const durationMs = state.spanStartMs != null && state.spanEndMs != null ? Math.max(0, state.spanEndMs - state.spanStartMs) : 0;
    const tickSpan = state.spanFirstId != null && state.spanLastId != null ? Math.max(0, Number(state.spanLastId) - Number(state.spanFirstId) + 1) : 0;
    const itemCount = state.structureBars.length + state.rangeBoxes.length;
    elements.structureMeta.textContent = [
      currentConfig().mode.toUpperCase(),
      "items " + itemCount + "/" + state.targetWindow,
      "tick-span " + tickSpan,
      "time-span " + formatDuration(durationMs),
      "bars " + state.structureBars.length + " active " + activeBars,
      "ranges " + state.rangeBoxes.length + " active " + activeRanges,
      "events " + state.structureEvents.length,
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
      parts.push("Wire " + Math.max(0, Date.now() - metrics.serverSentAtMs) + "ms");
    }
    elements.structurePerf.textContent = parts.join(" | ");
  }

  function escapeHtml(value) {
    return String(value)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll("\"", "&quot;");
  }

  function tooltipRow(label, value, tone) {
    if (value == null || value === "") {
      return "";
    }
    const toneClass = tone ? " is-" + tone : "";
    return "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">" + escapeHtml(label) + "</span><span class=\"chart-tip-value" + toneClass + "\">" + escapeHtml(value) + "</span></div>";
  }

  function tooltipSection(title, rows, note) {
    const content = rows.filter(Boolean).join("");
    if (!content && !note) {
      return "";
    }
    return "<div class=\"chart-tip-section\"><div class=\"chart-tip-title\">" + escapeHtml(title) + "</div>" + content + (note ? "<div class=\"chart-tip-note\">" + escapeHtml(note) + "</div>" : "") + "</div>";
  }

  function formatSignedPrice(value) {
    const number = Number(value);
    if (!Number.isFinite(number)) {
      return "-";
    }
    const fixed = number.toFixed(2);
    return number > 0 ? "+" + fixed : fixed;
  }

  function priceTone(value) {
    const number = Number(value);
    if (!Number.isFinite(number) || number === 0) {
      return "";
    }
    return number > 0 ? "positive" : "negative";
  }

  function structureDirection(bar) {
    if (bar?.type === "up") {
      return "up";
    }
    if (bar?.type === "down") {
      return "down";
    }
    return "sideways";
  }

  function tooltipHtml(param) {
    const sections = [];
    if (param?.seriesId === "structure-bars" && param.data?.bar) {
      const bar = param.data.bar;
      const move = Number(bar.close) - Number(bar.open);
      const span = Number(bar.high) - Number(bar.low);
      sections.push(tooltipSection("Structure", [
        tooltipRow("Type", "structure / " + String(bar.type)),
        tooltipRow("Item id", bar.id),
        tooltipRow("Status", bar.status),
        tooltipRow("Direction", structureDirection(bar)),
        tooltipRow("Start id", bar.startTickId),
        tooltipRow("End id", bar.endTickId),
        tooltipRow("Start time", formatTimestamp(bar.startTimestampMs)),
        tooltipRow("End time", formatTimestamp(bar.endTimestampMs)),
        tooltipRow("Duration", formatDuration(Number(bar.endTimestampMs) - Number(bar.startTimestampMs))),
        tooltipRow("From price", formatNumber(bar.open, 2)),
        tooltipRow("To price", formatNumber(bar.close, 2)),
        tooltipRow("High", formatNumber(bar.high, 2)),
        tooltipRow("Low", formatNumber(bar.low, 2)),
        tooltipRow("Price span", formatSignedPrice(span), priceTone(span)),
        tooltipRow("Move", formatSignedPrice(move), priceTone(move)),
      ]));
    } else if (param?.seriesId === "range-boxes" && param.data?.box) {
      const box = param.data.box;
      const span = Number(box.top) - Number(box.bottom);
      sections.push(tooltipSection("Range", [
        tooltipRow("Type", "range"),
        tooltipRow("Item id", box.id),
        tooltipRow("Status", box.status),
        tooltipRow("Direction", box.breakDirection ? "break " + String(box.breakDirection) : "sideways"),
        tooltipRow("Start id", box.startTickId),
        tooltipRow("End id", box.endTickId),
        tooltipRow("Start time", formatTimestamp(box.startTimestampMs)),
        tooltipRow("End time", formatTimestamp(box.endTimestampMs)),
        tooltipRow("Duration", formatDuration(Number(box.endTimestampMs) - Number(box.startTimestampMs))),
        tooltipRow("High", formatNumber(box.top, 2)),
        tooltipRow("Low", formatNumber(box.bottom, 2)),
        tooltipRow("Price span", formatSignedPrice(span), priceTone(span)),
      ], box.breakDirection ? "Break direction: " + String(box.breakDirection) : ""));
    } else if (param?.seriesId === "structure-events" && param.data?.event) {
      const event = param.data.event;
      sections.push(tooltipSection("Event", [
        tooltipRow("Type", event.type),
        tooltipRow("Time", formatTimestamp(event.timestampMs)),
        tooltipRow("Price", formatNumber(event.price, 2)),
        tooltipRow("From", event.fromState),
        tooltipRow("To", event.toState),
      ]));
    }
    return sections.length ? "<div class=\"chart-tip\">" + sections.join("") + "</div>" : "";
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
        state.resizeObserver = new ResizeObserver(function () {
          state.chart.resize();
        });
        state.resizeObserver.observe(elements.chartHost);
      }
      window.addEventListener("resize", function () {
        state.chart.resize();
      });
    }
    return state.chart;
  }

  function barsToSeriesData() {
    const typeMap = { down: -1, range: 0, up: 1 };
    return state.structureBars.map(function (bar) {
      return {
        value: [
          Number(bar.startTimestampMs),
          Number(bar.endTimestampMs),
          Number(bar.open),
          Number(bar.high),
          Number(bar.low),
          Number(bar.close),
          typeMap[bar.type] || 0,
          bar.status === "active" ? 1 : 0,
        ],
        bar: bar,
      };
    });
  }

  function rangeBoxesToSeriesData() {
    return state.rangeBoxes.map(function (box) {
      return {
        value: [
          Number(box.startTimestampMs),
          Number(box.endTimestampMs),
          Number(box.bottom),
          Number(box.top),
          box.status === "active" ? 1 : 0,
        ],
        box: box,
      };
    });
  }

  function eventsToSeriesData() {
    return state.structureEvents.map(function (event) {
      return {
        value: [Number(event.timestampMs), Number(event.price)],
        event: event,
        itemStyle: { color: EVENT_COLORS[event.type] || "#f8fafc" },
      };
    });
  }

  function buildVerticalLine(x, y1, y2, stroke, lineWidth, opacity, lineDash) {
    return {
      type: "line",
      shape: { x1: x, y1: y1, x2: x, y2: y2 },
      style: { stroke: stroke, lineWidth: lineWidth, opacity: opacity, lineDash: lineDash || [] },
      silent: true,
    };
  }

  function structureRender(params, api) {
    const startMs = Number(api.value(0));
    const endMs = Number(api.value(1));
    const open = Number(api.value(2));
    const high = Number(api.value(3));
    const low = Number(api.value(4));
    const close = Number(api.value(5));
    const typeValue = Number(api.value(6));
    const active = Number(api.value(7)) > 0;
    if (![startMs, endMs, open, high, low, close].every(Number.isFinite)) {
      return null;
    }

    const startBody = api.coord([startMs, open]);
    const endBody = api.coord([endMs, close]);
    const startHigh = api.coord([startMs, high]);
    const startLow = api.coord([startMs, low]);
    const endHigh = api.coord([endMs, high]);
    const endLow = api.coord([endMs, low]);
    const style = typeValue > 0 ? STRUCTURE_STYLES.up : (typeValue < 0 ? STRUCTURE_STYLES.down : STRUCTURE_STYLES.range);
    const left = Math.min(startBody[0], endBody[0]);
    const right = Math.max(startBody[0], endBody[0]);
    const width = Math.max(3, right - left);
    const top = Math.min(startBody[1], endBody[1]);
    const bodyHeight = Math.max(2, Math.abs(endBody[1] - startBody[1]));
    const bodyRect = echarts.graphic.clipRectByRect({
      x: left,
      y: top,
      width: width,
      height: bodyHeight,
    }, params.coordSys);
    if (!bodyRect) {
      return null;
    }

    return {
      type: "group",
      children: [
        buildVerticalLine(startBody[0], startHigh[1], startLow[1], style.stroke, active ? 1.6 : 1.0, active ? 1 : 0.7),
        {
          type: "rect",
          shape: bodyRect,
          style: {
            fill: style.fill,
            stroke: style.stroke,
            lineWidth: active ? 1.4 : 1.0,
            opacity: active ? 1 : 0.72,
          },
        },
        buildVerticalLine(
          left,
          Math.min(startHigh[1], startLow[1]),
          Math.max(startHigh[1], startLow[1]),
          style.boundary,
          active ? 1.2 : 1.0,
          active ? 0.52 : 0.26
        ),
        buildVerticalLine(
          right,
          Math.min(endHigh[1], endLow[1]),
          Math.max(endHigh[1], endLow[1]),
          style.boundary,
          active ? 1.5 : 1.0,
          active ? 0.9 : 0.34,
          active ? [5, 4] : []
        ),
      ],
    };
  }

  function rangeBoxRender(params, api) {
    const startMs = Number(api.value(0));
    const endMs = Number(api.value(1));
    const bottom = Number(api.value(2));
    const top = Number(api.value(3));
    const active = Number(api.value(4)) > 0;
    if (![startMs, endMs, bottom, top].every(Number.isFinite)) {
      return null;
    }

    const startBottom = api.coord([startMs, bottom]);
    const endTop = api.coord([endMs, top]);
    const rect = echarts.graphic.clipRectByRect({
      x: Math.min(startBottom[0], endTop[0]),
      y: Math.min(startBottom[1], endTop[1]),
      width: Math.max(2, Math.abs(endTop[0] - startBottom[0])),
      height: Math.max(2, Math.abs(endTop[1] - startBottom[1])),
    }, params.coordSys);
    if (!rect) {
      return null;
    }

    const stroke = active ? "rgba(176,238,255,0.84)" : "rgba(147,164,189,0.42)";
    const fill = active ? "rgba(109,216,255,0.12)" : "rgba(147,164,189,0.06)";
    const left = rect.x;
    const right = rect.x + rect.width;
    const topY = rect.y;
    const bottomY = rect.y + rect.height;
    return {
      type: "group",
      children: [
        {
          type: "rect",
          shape: rect,
          style: { fill: fill, stroke: stroke, lineWidth: active ? 1.4 : 1.0 },
        },
        buildVerticalLine(left, topY, bottomY, stroke, 1.0, active ? 0.5 : 0.22),
        buildVerticalLine(right, topY, bottomY, stroke, active ? 1.3 : 1.0, active ? 0.75 : 0.26, active ? [5, 4] : []),
      ],
    };
  }

  function buildSeries(config) {
    const series = [];
    if (config.showRanges) {
      series.push({
        id: "range-boxes",
        name: "Range boxes",
        type: "custom",
        renderItem: rangeBoxRender,
        data: rangeBoxesToSeriesData(),
        animation: false,
        z: 2,
      });
    }
    if (config.showStructure) {
      series.push({
        id: "structure-bars",
        name: "Structure",
        type: "custom",
        renderItem: structureRender,
        data: barsToSeriesData(),
        animation: false,
        z: 5,
      });
    }
    if (config.showEvents) {
      series.push({
        id: "structure-events",
        name: "Events",
        type: "scatter",
        data: eventsToSeriesData(),
        symbolSize: 7,
        animation: false,
        z: 8,
      });
    }
    return series;
  }

  function yBounds() {
    const config = currentConfig();
    const values = [];
    if (config.showStructure) {
      state.structureBars.forEach(function (bar) {
        values.push(Number(bar.high));
        values.push(Number(bar.low));
      });
    }
    if (config.showRanges) {
      state.rangeBoxes.forEach(function (box) {
        values.push(Number(box.top));
        values.push(Number(box.bottom));
      });
    }
    if (config.showEvents) {
      state.structureEvents.forEach(function (event) {
        values.push(Number(event.price));
      });
    }
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
      requestAnimationFrame(function () {
        renderChart(options);
      });
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
      series: buildSeries(currentConfig()),
      dataZoom: [
        { id: "zoom-inside", ...zoom },
        { id: "zoom-slider", ...zoom },
      ],
    }, { replaceMerge: ["series"], lazyUpdate: true });
    requestAnimationFrame(function () {
      state.applyingZoom = false;
    });
  }

  function syncSpanFromStructure() {
    const ids = [];
    const timestamps = [];
    state.structureBars.forEach(function (bar) {
      ids.push(Number(bar.startTickId), Number(bar.endTickId));
      timestamps.push(Number(bar.startTimestampMs), Number(bar.endTimestampMs));
    });
    state.rangeBoxes.forEach(function (box) {
      ids.push(Number(box.startTickId), Number(box.endTickId));
      timestamps.push(Number(box.startTimestampMs), Number(box.endTimestampMs));
    });
    const finiteIds = ids.filter(Number.isFinite);
    const finiteTimes = timestamps.filter(Number.isFinite);
    if (finiteIds.length) {
      state.spanFirstId = Math.min.apply(null, finiteIds);
      state.spanLastId = Math.max.apply(null, finiteIds);
    }
    if (finiteTimes.length) {
      state.spanStartMs = Math.min.apply(null, finiteTimes);
      state.spanEndMs = Math.max.apply(null, finiteTimes);
    }
  }

  function structureItemCount() {
    return state.structureBars.length + state.rangeBoxes.length;
  }

  function trimToTargetWindow() {
    const target = Math.max(1, Number(state.targetWindow) || DEFAULTS.window);
    const entries = [];
    state.structureBars.forEach(function (bar) {
      entries.push({
        kind: "structure",
        id: Number(bar.id),
        startTickId: Number(bar.startTickId),
        endTickId: Number(bar.endTickId),
      });
    });
    state.rangeBoxes.forEach(function (box) {
      entries.push({
        kind: "range",
        id: Number(box.id),
        startTickId: Number(box.startTickId),
        endTickId: Number(box.endTickId),
      });
    });
    if (entries.length <= target) {
      state.loadedWindow = entries.length;
      syncSpanFromStructure();
      return;
    }
    entries.sort(function (left, right) {
      return Number(left.endTickId) - Number(right.endTickId)
        || Number(left.startTickId) - Number(right.startTickId)
        || (left.kind === right.kind ? 0 : (left.kind === "structure" ? -1 : 1))
        || Number(left.id) - Number(right.id);
    });
    const kept = entries.slice(entries.length - target);
    const structureIds = new Set(kept.filter(function (entry) { return entry.kind === "structure"; }).map(function (entry) { return entry.id; }));
    const rangeIds = new Set(kept.filter(function (entry) { return entry.kind === "range"; }).map(function (entry) { return entry.id; }));
    const firstId = Math.min.apply(null, kept.map(function (entry) { return entry.startTickId; }));
    const lastId = Math.max.apply(null, kept.map(function (entry) { return entry.endTickId; }));
    state.structureBars = state.structureBars.filter(function (bar) {
      return structureIds.has(Number(bar.id));
    });
    state.rangeBoxes = state.rangeBoxes.filter(function (box) {
      return rangeIds.has(Number(box.id));
    });
    state.structureEvents = state.structureEvents.filter(function (event) {
      const tickId = Number(event.tickId);
      return tickId >= firstId && tickId <= lastId;
    });
    state.loadedWindow = kept.length;
    syncSpanFromStructure();
  }

  function replaceStructure(payload) {
    state.structureBars = Array.isArray(payload.structureBars) ? payload.structureBars.slice().sort(function (left, right) {
      return Number(left.id) - Number(right.id);
    }) : [];
    state.rangeBoxes = Array.isArray(payload.rangeBoxes) ? payload.rangeBoxes.slice().sort(function (left, right) {
      return Number(left.id) - Number(right.id);
    }) : [];
    state.structureEvents = Array.isArray(payload.structureEvents) ? payload.structureEvents.slice().sort(function (left, right) {
      return Number(left.tickId) - Number(right.tickId) || Number(left.id) - Number(right.id);
    }) : [];
    trimToTargetWindow();
  }

  function applySpanPayload(payload) {
    if (payload.firstId != null) {
      state.spanFirstId = payload.firstId;
    }
    if (payload.lastId != null) {
      state.spanLastId = payload.lastId;
    }
    if (payload.firstTimestampMs != null) {
      state.spanStartMs = payload.firstTimestampMs;
    }
    if (payload.lastTimestampMs != null) {
      state.spanEndMs = payload.lastTimestampMs;
    }
  }

  function mergeById(items, updates) {
    const byId = new Map();
    items.forEach(function (item) {
      if (item && item.id != null) {
        byId.set(item.id, item);
      }
    });
    (updates || []).forEach(function (item) {
      if (item && item.id != null) {
        byId.set(item.id, item);
      }
    });
    return Array.from(byId.values()).sort(function (left, right) {
      return Number(left.id) - Number(right.id);
    });
  }

  function mergeOlderOnly(items, olderItems) {
    const byId = new Map();
    items.forEach(function (item) {
      if (item && item.id != null) {
        byId.set(item.id, item);
      }
    });
    (olderItems || []).forEach(function (item) {
      if (item && item.id != null && !byId.has(item.id)) {
        byId.set(item.id, item);
      }
    });
    return Array.from(byId.values()).sort(function (left, right) {
      return Number(left.id) - Number(right.id);
    });
  }

  function mergeEvents(items, updates) {
    const byKey = new Map();
    items.concat(updates || []).forEach(function (event) {
      if (event) {
        byKey.set(String(event.id) + ":" + String(event.tickId), event);
      }
    });
    return Array.from(byKey.values()).sort(function (left, right) {
      return Number(left.tickId) - Number(right.tickId) || Number(left.id) - Number(right.id);
    });
  }

  function applyStreamPayload(payload) {
    state.structureBars = mergeById(state.structureBars, payload.structureBarUpdates || []);
    state.rangeBoxes = mergeById(state.rangeBoxes, payload.rangeBoxUpdates || []);
    state.structureEvents = mergeEvents(state.structureEvents, payload.structureEvents || []);
    trimToTargetWindow();
    if (payload.lastId != null) {
      state.spanLastId = payload.lastId;
    }
    return (payload.structureBarUpdates || []).length || (payload.rangeBoxUpdates || []).length || (payload.structureEvents || []).length;
  }

  function applyBackfillPayload(payload) {
    state.structureBars = mergeOlderOnly(state.structureBars, payload.structureBars || []);
    state.rangeBoxes = mergeOlderOnly(state.rangeBoxes, payload.rangeBoxes || []);
    state.structureEvents = mergeEvents(payload.structureEvents || [], state.structureEvents);
    state.hasMoreLeft = Boolean(payload.hasMoreLeft);
    trimToTargetWindow();
    return structureItemCount();
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
    if (state.backfillTimer) {
      window.clearTimeout(state.backfillTimer);
      state.backfillTimer = 0;
    }
    state.backfillToken += 1;
    state.streamConnected = false;
    renderPerf();
  }

  async function fetchJson(url, options) {
    const response = await fetch(url, options);
    const payload = await response.json().catch(function () {
      return {};
    });
    if (!response.ok) {
      throw new Error(payload.detail || "Request failed.");
    }
    return payload;
  }

  async function resolveReviewStartId(config) {
    if (config.reviewStart) {
      const payload = await fetchJson("/api/structure/review-start?" + new URLSearchParams({
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

  function bootstrapUrl(config, startId, windowOverride) {
    const params = new URLSearchParams({
      mode: config.mode,
      window: String(windowOverride || config.window),
      ...visibilityParams(config),
    });
    if (config.mode === "review" && startId != null) {
      params.set("id", String(startId));
    }
    return "/api/structure/bootstrap?" + params.toString();
  }

  function nextUrl(config, afterId, endId, limit) {
    const params = new URLSearchParams({
      afterId: String(afterId),
      limit: String(limit),
      window: String(config.window),
      ...visibilityParams(config),
    });
    if (endId != null) {
      params.set("endId", String(endId));
    }
    return "/api/structure/next?" + params.toString();
  }

  function previousUrl(config, targetWindow) {
    return "/api/structure/previous?" + new URLSearchParams({
      beforeId: String(state.spanFirstId || 1),
      currentLastId: String(state.spanLastId || state.spanFirstId || 1),
      window: String(targetWindow),
      ...visibilityParams(config),
    }).toString();
  }

  function scheduleBackfill() {
    if (state.backfillTimer) {
      window.clearTimeout(state.backfillTimer);
    }
    const token = state.backfillToken;
    state.backfillTimer = window.setTimeout(function () {
      state.backfillTimer = 0;
      backfillStep(token).catch(function (error) {
        if (token === state.backfillToken) {
          status(error.message || "Structure backfill failed.", true);
        }
      });
    }, 60);
  }

  async function backfillStep(token) {
    if (token !== state.backfillToken) {
      return;
    }
    const config = currentConfig();
    if (config.mode !== "live" || state.loadedWindow >= state.targetWindow || !state.hasMoreLeft || state.spanLastId == null) {
      return;
    }
    const nextTarget = Math.min(state.targetWindow, Math.max(state.loadedWindow + BACKFILL_CHUNK_ITEMS, INITIAL_LIVE_ITEM_WINDOW + BACKFILL_CHUNK_ITEMS));
    const payload = await fetchJson(previousUrl(config, nextTarget));
    if (token !== state.backfillToken) {
      return;
    }
    state.lastMetrics = payload.metrics || state.lastMetrics;
    applyBackfillPayload(payload);
    renderMeta();
    renderPerf();
    renderChart({ shiftWithRun: false });
    if (state.loadedWindow < state.targetWindow && state.hasMoreLeft) {
      scheduleBackfill();
    }
  }

  async function loadBootstrap(resetView) {
    const config = currentConfig();
    const startId = config.mode === "review" ? await resolveReviewStartId(config) : null;
    state.targetWindow = config.window;
    const initialWindow = config.mode === "live" ? Math.min(config.window, INITIAL_LIVE_ITEM_WINDOW) : config.window;
    const payload = await fetchJson(bootstrapUrl(config, startId, initialWindow));
    replaceStructure(payload);
    applySpanPayload(payload);
    state.reviewEndId = payload.reviewEndId || null;
    state.hasMoreLeft = Boolean(payload.hasMoreLeft);
    state.lastMetrics = payload.metrics || null;
    state.loadedWindow = structureItemCount();
    if (resetView) {
      state.zoom = null;
      state.rightEdgeAnchored = true;
    }
    renderMeta();
    renderPerf();
    renderChart({ resetView: Boolean(resetView) });
    status("Loaded " + (state.structureBars.length + state.rangeBoxes.length) + " structure item(s).", false);
    if (config.mode === "live" && state.loadedWindow < state.targetWindow && state.hasMoreLeft) {
      scheduleBackfill();
    }
    if (config.run === "run") {
      if (config.mode === "live") {
        connectStream(state.spanLastId || 0);
      } else {
        scheduleReviewStep();
      }
    }
  }

  function connectStream(afterId) {
    clearActivity();
    const config = currentConfig();
    const source = new EventSource("/api/structure/stream?" + new URLSearchParams({
      afterId: String(afterId || 0),
      limit: "250",
      window: String(config.window),
      ...visibilityParams(config),
    }).toString());
    state.source = source;
    source.onopen = function () {
      state.streamConnected = true;
      renderPerf();
      status("Structure stream connected.", false);
    };
    source.onmessage = function (event) {
      const payload = JSON.parse(event.data);
      state.lastMetrics = payload;
      const changed = applyStreamPayload(payload);
      renderMeta();
      renderPerf();
      if (changed) {
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
      status("Structure stream disconnected. Click Load or Run to reconnect.", true);
      clearActivity();
    };
  }

  async function reviewStep() {
    const config = currentConfig();
    if (config.mode !== "review" || config.run !== "run") {
      return;
    }
    if (state.spanLastId == null || !state.reviewEndId) {
      status("Review is waiting for loaded structure items.", true);
      return;
    }
    if (state.spanLastId >= state.reviewEndId) {
      status("Review reached the current end snapshot.", false);
      return;
    }
    const limit = Math.max(25, Math.min(500, Math.round(100 * config.reviewSpeed)));
    const payload = await fetchJson(nextUrl(config, state.spanLastId, state.reviewEndId, limit));
    state.lastMetrics = payload.metrics || null;
    replaceStructure(payload);
    applySpanPayload(payload);
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
    }
    const delay = Math.max(80, Math.round(450 / currentConfig().reviewSpeed));
    state.reviewTimer = window.setTimeout(function () {
      state.reviewTimer = 0;
      reviewStep().catch(function (error) {
        status(error.message || "Review fetch failed.", true);
      });
    }, delay);
  }

  async function resumeRunIfNeeded() {
    const config = currentConfig();
    if (config.run !== "run" || state.spanLastId == null) {
      return;
    }
    if (config.mode === "live") {
      connectStream(state.spanLastId);
    } else {
      scheduleReviewStep();
    }
  }

  async function loadMoreLeft() {
    if (state.spanFirstId == null) {
      status("Load the structure map first.", true);
      return;
    }
    const config = currentConfig();
    const targetWindow = Math.min(MAX_WINDOW, Math.max(state.targetWindow, structureItemCount()) + config.window);
    if (targetWindow <= state.targetWindow) {
      status("Loaded history is already at the structure map cap.", false);
      return;
    }
    state.targetWindow = targetWindow;
    renderMeta();
    if (config.mode === "live") {
      scheduleBackfill();
      status("Expanding structure history toward about " + state.targetWindow + " item(s).", false);
      return;
    }
    clearActivity();
    const payload = await fetchJson(previousUrl(config, targetWindow));
    state.lastMetrics = payload.metrics || null;
    replaceStructure(payload);
    applySpanPayload(payload);
    state.loadedWindow = structureItemCount();
    state.hasMoreLeft = Boolean(payload.hasMoreLeft);
    renderMeta();
    renderPerf();
    renderChart({ shiftWithRun: false });
    status("Extended structure history to about " + state.loadedWindow + " structure item(s).", false);
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

  function applyInitialConfig(config) {
    setSegment(elements.modeToggle, config.mode);
    setSegment(elements.runToggle, config.run);
    setSegment(elements.reviewSpeedToggle, config.reviewSpeed);
    elements.showEvents.checked = Boolean(config.showEvents);
    elements.showStructure.checked = Boolean(config.showStructure);
    elements.showRanges.checked = Boolean(config.showRanges);
    elements.tickId.value = config.id;
    elements.reviewStart.value = config.reviewStart;
    elements.windowSize.value = String(config.window);
    state.targetWindow = config.window;
    setSidebarCollapsed(true);
    updateReviewFields();
    renderMeta();
    renderPerf();
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
    writeQuery();
    clearActivity();
    if (value === "run" && state.spanLastId != null) {
      resumeRunIfNeeded();
      return;
    }
    status("Run state updated.", false);
  });

  [elements.showEvents, elements.showStructure, elements.showRanges].forEach(function (control) {
    control.addEventListener("change", function () {
      writeQuery();
      loadAll(false).catch(function (error) {
        status(error.message || "Display refresh failed.", true);
      });
      status("Display layers updated.", false);
    });
  });

  bindSegment(elements.reviewSpeedToggle, function (value) {
    setSegment(elements.reviewSpeedToggle, value);
    writeQuery();
    if (currentConfig().mode === "review" && currentConfig().run === "run") {
      clearActivity();
      scheduleReviewStep();
    }
  });

  [elements.tickId, elements.reviewStart, elements.windowSize].forEach(function (control) {
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
  elements.applyButton.addEventListener("click", function () {
    loadAll(true);
  });
  elements.loadMoreLeftButton.addEventListener("click", function () {
    loadMoreLeft().catch(function (error) {
      status(error.message || "Load More Left failed.", true);
    });
  });
  window.addEventListener("keydown", function (event) {
    if (event.key === "Escape" && !state.ui.sidebarCollapsed) {
      setSidebarCollapsed(true);
    }
  });

  applyInitialConfig(parseQuery());
  loadAll(true);
}());
