(function () {
  const DEFAULTS = {
    mode: "live",
    run: "run",
    showTicks: true,
    showEvents: false,
    showStructure: false,
    showRanges: false,
    id: "",
    reviewStart: "",
    reviewSpeed: 1,
    window: 2000,
  };
  const MAX_WINDOW = 10000;
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
  const BAR_COLORS = {
    up: { fill: "rgba(255,179,92,0.40)", stroke: "#ffb35c" },
    down: { fill: "rgba(126,240,199,0.36)", stroke: "#7ef0c7" },
    range: { fill: "rgba(109,216,255,0.20)", stroke: "#6dd8ff" },
  };

  const state = {
    chart: null,
    rows: [],
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
    loadedWindow: DEFAULTS.window,
    rangeFirstId: null,
    rangeLastId: null,
    rightEdgeAnchored: true,
    zoom: null,
    applyingZoom: false,
    overlayFrame: 0,
    resizeObserver: null,
    ui: { sidebarCollapsed: true },
  };

  const elements = {
    liveWorkspace: document.getElementById("liveWorkspace"),
    liveSidebar: document.getElementById("liveSidebar"),
    sidebarToggle: document.getElementById("sidebarToggle"),
    sidebarBackdrop: document.getElementById("sidebarBackdrop"),
    modeToggle: document.getElementById("modeToggle"),
    runToggle: document.getElementById("runToggle"),
    showTicks: document.getElementById("showTicks"),
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
    liveMeta: document.getElementById("liveMeta"),
    livePerf: document.getElementById("livePerf"),
    chartPanel: document.getElementById("chartPanel"),
    chartHost: document.getElementById("liveChart"),
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
      showTicks: params.has("showTicks") ? params.get("showTicks") !== "0" : DEFAULTS.showTicks,
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
      showTicks: elements.showTicks.checked,
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
    container.querySelectorAll("button").forEach((button) => {
      button.classList.toggle("active", button.dataset.value === String(value));
    });
  }

  function bindSegment(container, handler) {
    container.querySelectorAll("button").forEach((button) => {
      button.addEventListener("click", () => handler(button.dataset.value));
    });
  }

  function writeQuery() {
    const config = currentConfig();
    const params = new URLSearchParams({
      mode: config.mode,
      run: config.run,
      showTicks: config.showTicks ? "1" : "0",
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
      showTicks: config.showTicks ? "1" : "0",
      showEvents: config.showEvents ? "1" : "0",
      showStructure: config.showStructure ? "1" : "0",
      showRanges: config.showRanges ? "1" : "0",
    };
  }

  function setSidebarCollapsed(collapsed) {
    state.ui.sidebarCollapsed = Boolean(collapsed);
    elements.liveWorkspace.classList.toggle("is-sidebar-collapsed", state.ui.sidebarCollapsed);
    elements.sidebarToggle.setAttribute("aria-expanded", String(!state.ui.sidebarCollapsed));
    elements.sidebarToggle.setAttribute("aria-label", state.ui.sidebarCollapsed ? "Open live controls" : "Close live controls");
    elements.sidebarBackdrop.tabIndex = state.ui.sidebarCollapsed ? -1 : 0;
    if (state.chart) {
      requestAnimationFrame(() => {
        state.chart.resize();
        queueOverlayRender();
      });
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
    const activeBars = state.structureBars.filter((bar) => bar.status === "active").length;
    const activeRanges = state.rangeBoxes.filter((box) => box.status === "active").length;
    elements.liveMeta.textContent = [
      currentConfig().mode.toUpperCase(),
      "ticks " + state.rows.length + "/" + currentConfig().window,
      "left " + state.rangeFirstId,
      "right " + state.rangeLastId,
      state.hasMoreLeft ? "more-left yes" : "more-left no",
      "bars " + state.structureBars.length + " active " + activeBars,
      "ranges " + state.rangeBoxes.length + " active " + activeRanges,
      "events " + state.structureEvents.length,
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
    elements.livePerf.textContent = parts.join(" | ");
  }

  function escapeHtml(value) {
    return String(value)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll("\"", "&quot;");
  }

  function formatPrice(value) {
    const number = Number(value);
    return Number.isFinite(number) ? number.toFixed(2) : "-";
  }

  function rowAtTickId(tickId) {
    const rounded = Math.round(Number(tickId));
    if (!Number.isFinite(rounded)) {
      return null;
    }
    return state.rows.find((row) => Number(row.id) === rounded) || null;
  }

  function eventsAtTickId(tickId) {
    const rounded = Math.round(Number(tickId));
    if (!Number.isFinite(rounded)) {
      return [];
    }
    return state.structureEvents.filter((event) => Number(event.tickId) === rounded);
  }

  function boxesAtTickId(tickId) {
    const rounded = Math.round(Number(tickId));
    if (!Number.isFinite(rounded)) {
      return [];
    }
    return state.rangeBoxes.filter((box) => rounded >= Number(box.startTickId) && rounded <= Number(box.endTickId));
  }

  function tooltipHtml(params) {
    const entries = Array.isArray(params) ? params : [params];
    const point = entries[0];
    const tickId = Number(point?.axisValue ?? point?.value?.[0]);
    const row = rowAtTickId(tickId);
    const lines = [];
    if (row) {
      lines.push("tick " + row.id + " | " + row.timestamp);
      lines.push("bid " + formatPrice(row.bid) + " | ask " + formatPrice(row.ask) + " | mid " + formatPrice(row.mid));
    } else if (Number.isFinite(tickId)) {
      lines.push("tick " + Math.round(tickId));
    }
    eventsAtTickId(tickId).forEach((event) => lines.push(String(event.type) + " " + formatPrice(event.price)));
    boxesAtTickId(tickId).forEach((box) => lines.push("range " + formatPrice(box.bottom) + " - " + formatPrice(box.top) + " " + box.status));
    return lines.length ? "<div class=\"chart-tip\">" + lines.map(escapeHtml).join("<br>") + "</div>" : "";
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
        grid: { left: 54, right: 16, top: 14, bottom: 54 },
        tooltip: { trigger: "axis", axisPointer: { type: "cross" }, formatter: tooltipHtml },
        xAxis: { type: "value", scale: true, boundaryGap: ["1%", "1%"], axisLabel: { color: "#9eadc5" } },
        yAxis: { type: "value", scale: true, axisLabel: { color: "#9eadc5" } },
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
      state.chart.on("dataZoom", () => {
        if (state.applyingZoom) {
          return;
        }
        const option = state.chart.getOption();
        const zoom = option?.dataZoom?.[0] || null;
        state.zoom = zoom ? { start: zoom.start, end: zoom.end, startValue: zoom.startValue, endValue: zoom.endValue } : null;
        state.rightEdgeAnchored = !zoom || Number(zoom.end) >= 99.5;
        queueOverlayRender();
      });
      if (typeof ResizeObserver === "function") {
        state.resizeObserver = new ResizeObserver(() => {
          state.chart.resize();
          queueOverlayRender();
        });
        state.resizeObserver.observe(elements.chartHost);
      }
      window.addEventListener("resize", () => {
        state.chart.resize();
        queueOverlayRender();
      });
    }
    return state.chart;
  }

  function rowsToSeriesData() {
    return state.rows.map((row) => [Number(row.id), Number(row.mid)]);
  }

  function barsToSeriesData() {
    const typeMap = { down: -1, range: 0, up: 1 };
    return state.structureBars.map((bar) => [
      Number(bar.startTickId),
      Number(bar.endTickId),
      Number(bar.open),
      Number(bar.high),
      Number(bar.low),
      Number(bar.close),
      typeMap[bar.type] || 0,
      bar.status === "active" ? 1 : 0,
    ]);
  }

  function eventsToSeriesData() {
    return state.structureEvents.map((event) => ({
      value: [Number(event.tickId), Number(event.price), event.type],
      itemStyle: { color: EVENT_COLORS[event.type] || "#f8fafc" },
    }));
  }

  function structureCandleRender(params, api) {
    const startTick = api.value(0);
    const endTick = api.value(1);
    const open = api.value(2);
    const high = api.value(3);
    const low = api.value(4);
    const close = api.value(5);
    const typeValue = Number(api.value(6));
    const statusValue = Number(api.value(7));
    const start = api.coord([startTick, open]);
    const end = api.coord([endTick, close]);
    const highPoint = api.coord([startTick, high]);
    const lowPoint = api.coord([startTick, low]);
    const style = typeValue > 0 ? BAR_COLORS.up : (typeValue < 0 ? BAR_COLORS.down : BAR_COLORS.range);
    const width = Math.max(4, Math.abs(end[0] - start[0]));
    const left = Math.min(start[0], end[0]);
    const center = left + width / 2;
    const top = Math.min(start[1], end[1]);
    const bodyHeight = Math.max(3, Math.abs(end[1] - start[1]));
    const active = statusValue > 0;
    return {
      type: "group",
      children: [
        {
          type: "line",
          shape: { x1: center, y1: highPoint[1], x2: center, y2: lowPoint[1] },
          style: { stroke: style.stroke, lineWidth: active ? 1.7 : 1.0, opacity: active ? 1 : 0.66 },
        },
        {
          type: "rect",
          shape: { x: left, y: top, width, height: bodyHeight, r: 2 },
          style: { fill: style.fill, stroke: style.stroke, lineWidth: active ? 1.4 : 1.0, opacity: active ? 1 : 0.66 },
        },
      ],
    };
  }

  function buildSeries(config) {
    const series = [];
    if (config.showTicks) {
      series.push({
        id: "raw-mid",
        name: "Raw mid",
        type: "line",
        showSymbol: false,
        hoverAnimation: false,
        animation: false,
        data: rowsToSeriesData(),
        lineStyle: { color: "#6dd8ff", width: 1.35 },
        z: 5,
      });
    }
    if (config.showStructure) {
      series.push({
        id: "structure-candles",
        name: "Structure candles",
        type: "custom",
        renderItem: structureCandleRender,
        data: barsToSeriesData(),
        animation: false,
        encode: { x: [0, 1], y: [2, 3, 4, 5] },
        z: 4,
      });
    }
    if (config.showEvents) {
      series.push({
        id: "structure-events",
        name: "Meaningful ticks",
        type: "scatter",
        data: eventsToSeriesData(),
        symbolSize: 7,
        animation: false,
        z: 9,
      });
    }
    return series;
  }

  function yBounds() {
    const config = currentConfig();
    const values = [];
    state.rows.forEach((row) => values.push(Number(row.mid)));
    if (config.showStructure) {
      state.structureBars.forEach((bar) => {
        values.push(Number(bar.high));
        values.push(Number(bar.low));
      });
    }
    if (config.showRanges) {
      state.rangeBoxes.forEach((box) => {
        values.push(Number(box.top));
        values.push(Number(box.bottom));
      });
    }
    if (config.showEvents) {
      state.structureEvents.forEach((event) => values.push(Number(event.price)));
    }
    const finite = values.filter(Number.isFinite);
    if (!finite.length) {
      return {};
    }
    const low = Math.min(...finite);
    const high = Math.max(...finite);
    const span = Math.max(0, high - low);
    const padding = span > 0 ? Math.max(span * 0.06, 0.02) : 0.05;
    return { min: low - padding, max: high + padding };
  }

  function renderChart(options) {
    const chart = ensureChart();
    if (!chart) {
      requestAnimationFrame(() => renderChart(options));
      return;
    }
    const config = currentConfig();
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
      series: buildSeries(config),
      yAxis: yBounds(),
      dataZoom: [
        { id: "zoom-inside", ...zoom },
        { id: "zoom-slider", ...zoom },
      ],
    }, { replaceMerge: ["series"], lazyUpdate: true });
    requestAnimationFrame(() => {
      state.applyingZoom = false;
      queueOverlayRender();
    });
  }

  function rangeBoxStyle(box) {
    if (box.status === "closed") {
      return { fill: "rgba(147,164,189,0.07)", stroke: "rgba(147,164,189,0.40)", lineWidth: 1 };
    }
    return { fill: "rgba(109,216,255,0.10)", stroke: "rgba(176,238,255,0.70)", lineWidth: 1.3 };
  }

  function buildRangeBoxGraphics() {
    const chart = state.chart;
    if (!chart || !currentConfig().showRanges || !state.rangeBoxes.length) {
      return [];
    }
    const grid = chart.getModel()?.getComponent("grid", 0);
    const rect = grid?.coordinateSystem?.getRect?.();
    if (!rect) {
      return [];
    }
    const children = [];
    state.rangeBoxes.forEach((box, index) => {
      const leftPoint = chart.convertToPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [Number(box.startTickId), Number(box.bottom)]);
      const rightPoint = chart.convertToPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [Number(box.endTickId), Number(box.bottom)]);
      const topPoint = chart.convertToPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [Number(box.startTickId), Number(box.top)]);
      const bottomPoint = chart.convertToPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [Number(box.startTickId), Number(box.bottom)]);
      if (!Array.isArray(leftPoint) || !Array.isArray(rightPoint) || !Array.isArray(topPoint) || !Array.isArray(bottomPoint)) {
        return;
      }
      let left = Number(leftPoint[0]);
      let right = Number(rightPoint[0]);
      let top = Math.min(Number(topPoint[1]), Number(bottomPoint[1]));
      let bottom = Math.max(Number(topPoint[1]), Number(bottomPoint[1]));
      if (!Number.isFinite(left) || !Number.isFinite(right) || !Number.isFinite(top) || !Number.isFinite(bottom)) {
        return;
      }
      if (right < rect.x || left > rect.x + rect.width || bottom < rect.y || top > rect.y + rect.height) {
        return;
      }
      left = Math.max(rect.x, left);
      right = Math.min(rect.x + rect.width, right);
      top = Math.max(rect.y, top);
      bottom = Math.min(rect.y + rect.height, bottom);
      const style = rangeBoxStyle(box);
      children.push({
        id: "range-box-" + String(box.id || index),
        type: "rect",
        silent: true,
        z: 2,
        shape: { x: left, y: top, width: Math.max(2, right - left), height: Math.max(2, bottom - top), r: 2 },
        style: { fill: style.fill, stroke: style.stroke, lineWidth: style.lineWidth },
      });
    });
    return children;
  }

  function renderOverlay() {
    if (!state.chart) {
      return;
    }
    state.chart.setOption({
      graphic: [{
        id: "range-box-overlay",
        type: "group",
        silent: true,
        z: 2,
        children: buildRangeBoxGraphics(),
      }],
    }, { replaceMerge: ["graphic"], lazyUpdate: true });
  }

  function queueOverlayRender() {
    if (state.overlayFrame) {
      window.cancelAnimationFrame(state.overlayFrame);
    }
    state.overlayFrame = window.requestAnimationFrame(() => {
      state.overlayFrame = 0;
      renderOverlay();
    });
  }

  function syncRangeFromRows() {
    if (!state.rows.length) {
      return;
    }
    state.rangeFirstId = state.rows[0].id;
    state.rangeLastId = state.rows[state.rows.length - 1].id;
  }

  function replaceRows(rows) {
    state.rows = Array.isArray(rows) ? rows.slice() : [];
    syncRangeFromRows();
  }

  function replaceStructure(payload) {
    state.structureBars = Array.isArray(payload.structureBars) ? payload.structureBars.slice() : [];
    state.rangeBoxes = Array.isArray(payload.rangeBoxes) ? payload.rangeBoxes.slice() : [];
    state.structureEvents = Array.isArray(payload.structureEvents) ? payload.structureEvents.slice() : [];
  }

  function mergeById(items, updates) {
    const byId = new Map();
    items.forEach((item) => {
      if (item && item.id != null) {
        byId.set(item.id, item);
      }
    });
    (updates || []).forEach((item) => {
      if (item && item.id != null) {
        byId.set(item.id, item);
      }
    });
    return Array.from(byId.values()).sort((left, right) => Number(left.id) - Number(right.id));
  }

  function trimStructureToRows() {
    if (!state.rows.length) {
      return;
    }
    const first = Number(state.rows[0].id);
    const last = Number(state.rows[state.rows.length - 1].id);
    state.structureBars = state.structureBars.filter((bar) => Number(bar.endTickId) >= first && Number(bar.startTickId) <= last);
    state.rangeBoxes = state.rangeBoxes.filter((box) => Number(box.endTickId) >= first && Number(box.startTickId) <= last);
    state.structureEvents = state.structureEvents.filter((event) => Number(event.tickId) >= first && Number(event.tickId) <= last);
  }

  function dedupeAppend(rows) {
    if (!Array.isArray(rows) || !rows.length) {
      return 0;
    }
    const existing = new Set(state.rows.map((row) => Number(row.id)));
    let appended = 0;
    rows.forEach((row) => {
      if (!existing.has(Number(row.id))) {
        state.rows.push(row);
        existing.add(Number(row.id));
        appended += 1;
      }
    });
    if (appended) {
      state.rows.sort((left, right) => Number(left.id) - Number(right.id));
      if (state.rows.length > currentConfig().window) {
        state.rows = state.rows.slice(state.rows.length - currentConfig().window);
      }
      syncRangeFromRows();
      trimStructureToRows();
    }
    return appended;
  }

  function dedupePrepend(rows, targetWindow) {
    if (!Array.isArray(rows) || !rows.length) {
      return 0;
    }
    const existing = new Set(state.rows.map((row) => Number(row.id)));
    const older = rows.filter((row) => !existing.has(Number(row.id)));
    if (!older.length) {
      return 0;
    }
    state.rows = older.concat(state.rows).sort((left, right) => Number(left.id) - Number(right.id));
    if (state.rows.length > targetWindow) {
      state.rows = state.rows.slice(0, targetWindow);
    }
    syncRangeFromRows();
    return older.length;
  }

  function applyRangePayload(payload) {
    if (payload.firstId != null) {
      state.rangeFirstId = payload.firstId;
    }
    if (payload.lastId != null) {
      state.rangeLastId = payload.lastId;
    }
    if (state.rows.length) {
      syncRangeFromRows();
    }
  }

  function applyStreamPayload(payload) {
    if (payload.lastId != null) {
      state.rangeLastId = payload.lastId;
    }
    const appended = dedupeAppend(payload.rows || []);
    state.structureBars = mergeById(state.structureBars, payload.structureBarUpdates || []);
    state.rangeBoxes = mergeById(state.rangeBoxes, payload.rangeBoxUpdates || []);
    if (Array.isArray(payload.structureEvents) && payload.structureEvents.length) {
      const byKey = new Map();
      state.structureEvents.concat(payload.structureEvents).forEach((event) => {
        byKey.set(String(event.id) + ":" + String(event.tickId), event);
      });
      state.structureEvents = Array.from(byKey.values()).sort((left, right) => Number(left.tickId) - Number(right.tickId) || Number(left.id) - Number(right.id));
    }
    trimStructureToRows();
    return appended || (payload.structureBarUpdates || []).length || (payload.rangeBoxUpdates || []).length || (payload.structureEvents || []).length;
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
      ...visibilityParams(config),
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
      window: String(config.window),
      ...visibilityParams(config),
    });
    if (endId != null) {
      params.set("endId", String(endId));
    }
    return "/api/live/next?" + params.toString();
  }

  function previousUrl(config, limit) {
    return "/api/live/previous?" + new URLSearchParams({
      beforeId: String(state.rangeFirstId || 1),
      currentLastId: String(state.rangeLastId || state.rangeFirstId || 1),
      limit: String(limit),
      ...visibilityParams(config),
    }).toString();
  }

  async function loadBootstrap(resetView) {
    const config = currentConfig();
    const startId = config.mode === "review" ? await resolveReviewStartId(config) : null;
    const payload = await fetchJson(bootstrapUrl(config, startId));
    state.loadedWindow = Number(payload.window) || config.window;
    replaceRows(payload.rows || []);
    replaceStructure(payload);
    applyRangePayload(payload);
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
    status("Loaded " + state.rows.length + " tick(s).", false);
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
      window: String(config.window),
      ...visibilityParams(config),
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
    const appended = dedupeAppend(payload.rows || []);
    replaceStructure(payload);
    applyRangePayload(payload);
    renderMeta();
    renderPerf();
    if (appended || payload.structureBars?.length || payload.rangeBoxes?.length || payload.structureEvents?.length) {
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
    }
    const delay = Math.max(80, Math.round(450 / currentConfig().reviewSpeed));
    state.reviewTimer = window.setTimeout(() => {
      state.reviewTimer = 0;
      reviewStep().catch((error) => status(error.message || "Review fetch failed.", true));
    }, delay);
  }

  async function resumeRunIfNeeded() {
    const config = currentConfig();
    if (config.run !== "run" || state.rangeLastId == null) {
      return;
    }
    if (config.mode === "live") {
      connectStream(state.rangeLastId);
    } else {
      scheduleReviewStep();
    }
  }

  async function loadMoreLeft() {
    if (state.rangeFirstId == null) {
      status("Load the chart first.", true);
      return;
    }
    clearActivity();
    const config = currentConfig();
    const previousFirstId = state.rangeFirstId;
    const targetWindow = Math.min(MAX_WINDOW, (Number(state.loadedWindow) || config.window) + config.window);
    const limit = Math.max(0, Math.min(config.window, targetWindow - state.rows.length));
    if (!limit) {
      status("Loaded history is already at the chart cap.", false);
      await resumeRunIfNeeded();
      return;
    }
    const payload = await fetchJson(previousUrl(config, limit));
    state.lastMetrics = payload.metrics || null;
    const prepended = dedupePrepend(payload.rows || [], targetWindow);
    replaceStructure(payload);
    applyRangePayload(payload);
    state.loadedWindow = prepended ? targetWindow : state.loadedWindow;
    state.hasMoreLeft = Boolean(payload.hasMoreLeft);
    renderMeta();
    renderPerf();
    if (prepended || (payload.firstId != null && payload.firstId < previousFirstId)) {
      renderChart({ shiftWithRun: false });
      status(prepended + " older tick(s) were added off-screen to the left.", false);
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

  function applyInitialConfig(config) {
    setSegment(elements.modeToggle, config.mode);
    setSegment(elements.runToggle, config.run);
    setSegment(elements.reviewSpeedToggle, config.reviewSpeed);
    elements.showTicks.checked = Boolean(config.showTicks);
    elements.showEvents.checked = Boolean(config.showEvents);
    elements.showStructure.checked = Boolean(config.showStructure);
    elements.showRanges.checked = Boolean(config.showRanges);
    elements.tickId.value = config.id;
    elements.reviewStart.value = config.reviewStart;
    elements.windowSize.value = String(config.window);
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
    if (value === "run" && state.rangeLastId != null) {
      resumeRunIfNeeded();
      return;
    }
    status("Run state updated.", false);
  });

  [elements.showTicks, elements.showEvents, elements.showStructure, elements.showRanges].forEach((control) => {
    control.addEventListener("change", function () {
      writeQuery();
      loadAll(false).catch((error) => status(error.message || "Display refresh failed.", true));
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
  elements.applyButton.addEventListener("click", function () {
    loadAll(true);
  });
  elements.loadMoreLeftButton.addEventListener("click", function () {
    loadMoreLeft().catch((error) => status(error.message || "Load More Left failed.", true));
  });
  window.addEventListener("keydown", function (event) {
    if (event.key === "Escape" && !state.ui.sidebarCollapsed) {
      setSidebarCollapsed(true);
    }
  });

  applyInitialConfig(parseQuery());
  loadAll(true);
}());
