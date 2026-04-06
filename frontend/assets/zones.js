(function () {
  const DEFAULTS = {
    mode: "live",
    run: "run",
    display: "zones",
    level: 0,
    series: "mid",
    id: "",
    reviewStart: "",
    reviewSpeed: 1,
    window: 2000,
    provisional: true,
    table: true,
    showAreas: false,
    areaHigherOnly: false,
  };

  const DISPLAY_CONFIG = {
    zones: { label: "Zones", showsCandles: false },
    "zone-candles": { label: "Zone Candles", showsCandles: true },
    "zones-zone-candles": { label: "Zones + Zone Candles", showsCandles: true },
  };

  const SERIES_CONFIG = {
    mid: { label: "Mid" },
    ask: { label: "Ask" },
    bid: { label: "Bid" },
  };

  const REVIEW_SPEEDS = [0.5, 1, 2, 3, 5];
  const ZOOM_COMPONENT_IDS = ["zoom-inside", "zoom-slider"];
  const MAX_CANDLE_WINDOW = 10000;
  const MIN_CHART_WIDTH = 180;
  const MIN_CHART_HEIGHT = 180;

  const state = {
    chart: null,
    bars: [],
    zones: [],
    areaRows: [],
    zoneState: null,
    loadedWindow: DEFAULTS.window,
    renderedSeries: [],
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
    selectedZoneId: null,
    zoom: { start: 0, end: 100 },
    applyingZoom: false,
    autoscaleFrame: 0,
    layoutResizeFrame: 0,
    layoutResizeTimeout: 0,
    zoneOverlayFrame: 0,
    resizeObserver: null,
    resizeBound: false,
    ui: {
      sidebarCollapsed: true,
      settingsCollapsed: true,
      tableCollapsed: true,
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
    showAreas: document.getElementById("showAreas"),
    areaStateActive: document.getElementById("areaStateActive"),
    areaStateUsed: document.getElementById("areaStateUsed"),
    areaStateClosed: document.getElementById("areaStateClosed"),
    areaSideTop: document.getElementById("areaSideTop"),
    areaSideBottom: document.getElementById("areaSideBottom"),
    areaHigherOnly: document.getElementById("areaHigherOnly"),
    applyButton: document.getElementById("applyButton"),
    loadMoreLeftButton: document.getElementById("loadMoreLeftButton"),
    statusLine: document.getElementById("statusLine"),
    liveMeta: document.getElementById("liveMeta"),
    zoneStateMeta: document.getElementById("zoneStateMeta"),
    livePerf: document.getElementById("livePerf"),
    chartHost: document.getElementById("zonesChart"),
    chartStage: document.getElementById("chartStage"),
    tablePanel: document.getElementById("tablePanel"),
    tableToggle: document.getElementById("tableToggle"),
    tableMeta: document.getElementById("tableMeta"),
    zonesTableBody: document.getElementById("zonesTableBody"),
  };

  function parseQuery() {
    const params = new URLSearchParams(window.location.search);
    const reviewSpeed = Number.parseFloat(params.get("speed") || String(DEFAULTS.reviewSpeed));
    const display = params.get("display");
    const level = Number.parseInt(params.get("level") || String(DEFAULTS.level), 10);
    const areaStates = parseFilterList(params.get("areaStates"), ["active"]);
    const areaSides = parseFilterList(params.get("areaSides"), ["top", "bottom"]);
    return {
      mode: params.get("mode") === "review" ? "review" : DEFAULTS.mode,
      run: params.get("run") === "stop" ? "stop" : DEFAULTS.run,
      display: Object.prototype.hasOwnProperty.call(DISPLAY_CONFIG, display) ? display : DEFAULTS.display,
      level: Number.isFinite(level) ? Math.max(0, Math.min(3, level)) : DEFAULTS.level,
      series: Object.prototype.hasOwnProperty.call(SERIES_CONFIG, params.get("series")) ? params.get("series") : DEFAULTS.series,
      id: params.get("id") || DEFAULTS.id,
      reviewStart: params.get("reviewStart") || DEFAULTS.reviewStart,
      reviewSpeed: REVIEW_SPEEDS.includes(reviewSpeed) ? reviewSpeed : DEFAULTS.reviewSpeed,
      window: sanitizeWindowValue(params.get("window")),
      provisional: params.get("provisional") !== "0",
      table: params.get("table") !== "0",
      showAreas: params.get("showAreas") === "1",
      areaStates: areaStates,
      areaSides: areaSides,
      areaHigherOnly: params.get("areaHigherOnly") === "1",
    };
  }

  function parseFilterList(rawValue, defaultValues) {
    if (!rawValue) {
      return defaultValues.slice();
    }
    const values = rawValue
      .split(",")
      .map((item) => item.trim().toLowerCase())
      .filter(Boolean);
    return values.length ? Array.from(new Set(values)) : defaultValues.slice();
  }

  function sanitizeWindowValue(rawValue) {
    return Math.max(1, Math.min(MAX_CANDLE_WINDOW, Number.parseInt(rawValue || String(DEFAULTS.window), 10) || DEFAULTS.window));
  }

  function clampLoadedWindow(value) {
    return Math.max(1, Math.min(MAX_CANDLE_WINDOW, Number(value) || DEFAULTS.window));
  }

  function currentLoadedWindow() {
    return clampLoadedWindow(state.loadedWindow || currentConfig().window);
  }

  function displayShowsCandles(display) {
    return Boolean((DISPLAY_CONFIG[display] || DISPLAY_CONFIG[DEFAULTS.display]).showsCandles);
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
      display: elements.displayToggle.querySelector("button.active")?.dataset.value || DEFAULTS.display,
      level: Number.parseInt(elements.levelToggle.querySelector("button.active")?.dataset.value || String(DEFAULTS.level), 10),
      series: elements.seriesToggle.querySelector("button.active")?.dataset.value || DEFAULTS.series,
      id: (elements.tickId.value || "").trim(),
      reviewStart: (elements.reviewStart.value || "").trim(),
      reviewSpeed: Number.parseFloat(elements.reviewSpeedToggle.querySelector("button.active")?.dataset.value || String(DEFAULTS.reviewSpeed)),
      window: sanitizeWindowValue(elements.windowSize.value),
      provisional: Boolean(elements.showProvisional.checked),
      table: Boolean(elements.showTable.checked),
      showAreas: Boolean(elements.showAreas.checked),
      areaStates: selectedAreaStates(),
      areaSides: selectedAreaSides(),
      areaHigherOnly: Boolean(elements.areaHigherOnly.checked),
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
    params.set("showAreas", config.showAreas ? "1" : "0");
    params.set("areaStates", config.areaStates.join(","));
    params.set("areaSides", config.areaSides.join(","));
    params.set("areaHigherOnly", config.areaHigherOnly ? "1" : "0");
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
    elements.sidebarToggle.setAttribute("aria-label", state.ui.sidebarCollapsed ? "Open zone controls" : "Close zone controls");
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

  function setTableCollapsed(collapsed) {
    state.ui.tableCollapsed = Boolean(collapsed);
    elements.chartStage.classList.toggle("is-table-collapsed", state.ui.tableCollapsed);
    elements.tableToggle.setAttribute("aria-expanded", String(!state.ui.tableCollapsed));
    elements.tableToggle.textContent = state.ui.tableCollapsed ? "Expand Strip" : "Collapse Strip";
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
    setSegment(elements.displayToggle, config.display);
    setSegment(elements.levelToggle, config.level);
    setSegment(elements.seriesToggle, config.series);
    setSegment(elements.reviewSpeedToggle, config.reviewSpeed);
    elements.tickId.value = config.id;
    elements.reviewStart.value = config.reviewStart;
    elements.windowSize.value = String(config.window);
    elements.showProvisional.checked = Boolean(config.provisional);
    elements.showTable.checked = Boolean(config.table);
    elements.showAreas.checked = Boolean(config.showAreas);
    elements.areaStateActive.checked = config.areaStates.includes("active");
    elements.areaStateUsed.checked = config.areaStates.includes("used");
    elements.areaStateClosed.checked = config.areaStates.includes("closed");
    elements.areaSideTop.checked = config.areaSides.includes("top");
    elements.areaSideBottom.checked = config.areaSides.includes("bottom");
    elements.areaHigherOnly.checked = Boolean(config.areaHigherOnly);
    elements.tablePanel.hidden = !config.table;
    setSidebarCollapsed(true);
    setSettingsCollapsed(true);
    setTableCollapsed(true);
    updateReviewFields();
    renderMeta();
    renderZoneStateMeta();
    renderPerf();
    writeQuery();
  }

  function selectedAreaStates() {
    const values = [];
    if (elements.areaStateActive.checked) {
      values.push("active");
    }
    if (elements.areaStateUsed.checked) {
      values.push("used");
    }
    if (elements.areaStateClosed.checked) {
      values.push("closed");
    }
    if (values.length) {
      return values;
    }
    elements.areaStateActive.checked = true;
    return ["active"];
  }

  function selectedAreaSides() {
    const values = [];
    if (elements.areaSideTop.checked) {
      values.push("top");
    }
    if (elements.areaSideBottom.checked) {
      values.push("bottom");
    }
    if (values.length) {
      return values;
    }
    elements.areaSideTop.checked = true;
    elements.areaSideBottom.checked = true;
    return ["top", "bottom"];
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
        queueZoneOverlayRender();
      }
    });
    state.layoutResizeTimeout = window.setTimeout(() => {
      state.layoutResizeTimeout = 0;
      if (state.chart && chartHostHasSize()) {
        state.chart.resize();
        queueVisibleYAxisUpdate();
        queueZoneOverlayRender();
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
      elements.liveMeta.textContent = "No persisted zone window loaded.";
      return;
    }
    const config = currentConfig();
    const activeZones = state.zones.filter((zone) => zone.status === "active").length;
    const provisionalZones = state.zones.filter((zone) => zone.status === "provisional").length;
    const closedZones = state.zones.filter((zone) => zone.status === "closed").length;
    const activeAreas = state.areaRows.filter((area) => area.state === "active").length;
    const usedAreas = state.areaRows.filter((area) => area.state === "used").length;
    const closedAreas = state.areaRows.filter((area) => area.state === "closed").length;
    elements.liveMeta.textContent = [
      config.mode.toUpperCase(),
      DISPLAY_CONFIG[config.display].label,
      "L" + config.level,
      config.series,
      "zones " + state.zones.length + "/" + currentLoadedWindow(),
      "z-active " + activeZones,
      "z-prov " + provisionalZones,
      "z-closed " + closedZones,
      config.showAreas ? "areas " + state.areaRows.length : null,
      config.showAreas ? "a-active " + activeAreas : null,
      config.showAreas ? "a-used " + usedAreas : null,
      config.showAreas ? "a-closed " + closedAreas : null,
      "candles " + state.bars.length,
      "left " + state.rangeFirstId,
      "cursor " + state.rangeLastId,
      state.hasMoreLeft ? "more-left yes" : "more-left no",
    ].filter(Boolean).join(" | ");
  }

  function renderZoneStateMeta() {
    const zoneState = state.zoneState;
    if (!zoneState) {
      elements.zoneStateMeta.textContent = "Zone state unavailable for this level.";
      return;
    }
    elements.zoneStateMeta.textContent = [
      "state L" + zoneState.level,
      "activeZoneId " + (zoneState.activeZoneId ?? "-"),
      "lastTick " + (zoneState.lastProcessedTickId ?? "-"),
      "lastPivot " + (zoneState.lastProcessedPivotId ?? "-"),
      zoneState.updatedAt ? "updated " + zoneState.updatedAt : "updated -",
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

  function formatPrice(value) {
    if (typeof value !== "number") {
      return "";
    }
    return value.toFixed(6).replace(/\.?0+$/, "");
  }

  function formatTableTimestamp(timestamp) {
    if (!timestamp) {
      return "";
    }
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

  function zoneRowValue(zone, key) {
    return zone[key] == null ? "" : zone[key];
  }

  function renderTable() {
    if (!state.zones.length) {
      elements.tableMeta.textContent = "No zones loaded.";
      elements.zonesTableBody.innerHTML = "";
      return;
    }
    elements.tableMeta.textContent = state.zones.length + " zone(s) | most recent first";
    const rows = state.zones.slice().reverse().map((zone) => {
      const tr = document.createElement("tr");
      tr.dataset.zoneId = String(zone.id);
      tr.classList.toggle("is-selected", zone.id === state.selectedZoneId);
      tr.classList.toggle("is-active", zone.status === "active");
      tr.classList.toggle("is-closed", zone.status === "closed");
      tr.innerHTML = [
        "<td>" + escapeHtml(zoneRowValue(zone, "symbol")) + "</td>",
        "<td>" + escapeHtml(String(zoneRowValue(zone, "selectedLevel"))) + "</td>",
        "<td>" + escapeHtml(zoneRowValue(zone, "status")) + "</td>",
        "<td>" + escapeHtml(zoneRowValue(zone, "patternType")) + "</td>",
        "<td>" + escapeHtml(formatTableTimestamp(zone.startTimestamp)) + "</td>",
        "<td>" + escapeHtml(formatTableTimestamp(zone.endTimestamp)) + "</td>",
        "<td>" + escapeHtml(String(zoneRowValue(zone, "startTickId"))) + "</td>",
        "<td>" + escapeHtml(String(zoneRowValue(zone, "endTickId"))) + "</td>",
        "<td>" + escapeHtml(String(zoneRowValue(zone, "anchorStartPivotId"))) + "</td>",
        "<td>" + escapeHtml(String(zoneRowValue(zone, "anchorMiddlePivotId"))) + "</td>",
        "<td>" + escapeHtml(String(zoneRowValue(zone, "anchorEndPivotId"))) + "</td>",
        "<td>" + escapeHtml(formatPrice(zone.initialZoneLow)) + "</td>",
        "<td>" + escapeHtml(formatPrice(zone.initialZoneHigh)) + "</td>",
        "<td>" + escapeHtml(formatPrice(zone.zoneLow)) + "</td>",
        "<td>" + escapeHtml(formatPrice(zone.zoneHigh)) + "</td>",
        "<td>" + escapeHtml(formatPrice(zone.zoneHeight)) + "</td>",
        "<td>" + escapeHtml(formatPrice(zone.sameSideDistance)) + "</td>",
        "<td>" + escapeHtml(String(zoneRowValue(zone, "tickCountInside"))) + "</td>",
        "<td>" + escapeHtml(String(zoneRowValue(zone, "durationInsideMs"))) + "</td>",
        "<td>" + escapeHtml(zoneRowValue(zone, "breakoutDirection")) + "</td>",
        "<td>" + escapeHtml(String(zoneRowValue(zone, "breakoutTickId"))) + "</td>",
      ].join("");
      tr.addEventListener("click", () => {
        focusZone(zone.id);
      });
      return tr;
    });
    elements.zonesTableBody.replaceChildren(...rows);
  }

  function candleAxisLabel(bar) {
    const timestampValue = bar?.labelTimestampMs || bar?.endTimestampMs || bar?.endTimestamp;
    if (!timestampValue) {
      return "";
    }
    const date = new Date(timestampValue);
    const options = state.bars.length > 72
      ? { hour: "2-digit", minute: "2-digit" }
      : { hour: "2-digit", minute: "2-digit", second: "2-digit" };
    return date.toLocaleTimeString("en-AU", options);
  }

  function displayShowsZones(display) {
    return display === "zones" || display === "zones-zone-candles";
  }

  function candleItemStyle(bar) {
    const isSelected = bar?.zoneId === state.selectedZoneId;
    if (!displayShowsCandles(currentConfig().display)) {
      return {
        color: "rgba(0, 0, 0, 0)",
        color0: "rgba(0, 0, 0, 0)",
        borderColor: "rgba(0, 0, 0, 0)",
        borderColor0: "rgba(0, 0, 0, 0)",
        borderWidth: 0,
      };
    }
    const up = Number(bar?.close) >= Number(bar?.open);
    const palette = up
      ? { color: "#7ef0c7", border: "#bcffe8" }
      : { color: "#ff8c42", border: "#ffd9b8" };
    if (!bar?.isFinal) {
      return {
        color: "rgba(109, 216, 255, 0.18)",
        color0: "rgba(109, 216, 255, 0.18)",
        borderColor: isSelected ? "#f3f6fb" : "#6dd8ff",
        borderColor0: isSelected ? "#f3f6fb" : "#6dd8ff",
        borderWidth: isSelected ? 2.2 : 1.35,
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
      itemStyle: candleItemStyle(bar),
      bar: bar,
    }));
  }

  function findBarIndexByZoneId(zoneId) {
    return state.bars.findIndex((bar) => bar.zoneId === zoneId);
  }

  function zoneSpanFor(zone) {
    if (!zone) {
      return null;
    }
    const index = findBarIndexByZoneId(zone.id);
    if (index < 0) {
      return null;
    }
    return { startIndex: index, endIndex: index };
  }

  function zoneForBarIndex(index) {
    if (!Number.isInteger(index) || index < 0 || index >= state.bars.length) {
      return null;
    }
    const zoneId = state.bars[index]?.zoneId;
    return state.zones.find((zone) => zone.id === zoneId) || null;
  }

  function areaSpanFor(area) {
    if (!area || !state.bars.length) {
      return null;
    }
    const rightTickId = area.rightTickId ?? area.closeTickId ?? area.birthTickId;
    const startIndex = state.bars.findIndex((bar) => Number(bar.endTickId) >= Number(area.birthTickId));
    if (startIndex < 0) {
      return null;
    }
    let endIndex = startIndex;
    for (let index = startIndex; index < state.bars.length; index += 1) {
      if (Number(state.bars[index].startTickId) <= Number(rightTickId)) {
        endIndex = index;
        continue;
      }
      break;
    }
    return {
      startIndex: startIndex,
      endIndex: Math.max(startIndex, endIndex),
    };
  }

  function areasForBarIndex(index) {
    if (!Number.isInteger(index)) {
      return [];
    }
    return state.areaRows.filter((area) => {
      const span = areaSpanFor(area);
      return span && index >= span.startIndex && index <= span.endIndex;
    });
  }

  function zoneStyle(zone) {
    if (zone.id === state.selectedZoneId) {
      return {
        fill: "rgba(176, 238, 255, 0.18)",
        stroke: "rgba(243, 246, 251, 0.96)",
        lineWidth: 2,
      };
    }
    if (zone.status === "active") {
      return {
        fill: "rgba(109, 216, 255, 0.14)",
        stroke: "rgba(176, 238, 255, 0.82)",
        lineWidth: 1.35,
      };
    }
    if (zone.status === "provisional") {
      return {
        fill: "rgba(109, 216, 255, 0.08)",
        stroke: "rgba(170, 230, 255, 0.7)",
        lineWidth: 1.15,
      };
    }
    return {
      fill: "rgba(255, 200, 87, 0.06)",
      stroke: "rgba(255, 214, 138, 0.34)",
      lineWidth: 1,
    };
  }

  function areaStyle(area) {
    const isTop = area.side === "top";
    const base = isTop
      ? { fill: "rgba(255, 140, 102, 0.12)", stroke: "rgba(255, 184, 163, 0.82)" }
      : { fill: "rgba(90, 208, 186, 0.12)", stroke: "rgba(181, 248, 232, 0.82)" };
    if (area.state === "used") {
      return {
        fill: base.fill.replace("0.12", "0.06"),
        stroke: base.stroke.replace("0.82", "0.5"),
        lineWidth: area.isLevel2Extreme ? 2.1 : (area.isLevel1Extreme ? 1.75 : 1.2),
        lineDash: [6, 4],
      };
    }
    if (area.state === "closed") {
      return {
        fill: base.fill.replace("0.12", "0.035"),
        stroke: base.stroke.replace("0.82", "0.28"),
        lineWidth: area.isLevel2Extreme ? 1.9 : (area.isLevel1Extreme ? 1.5 : 1.0),
        lineDash: [4, 4],
      };
    }
    return {
      fill: base.fill.replace("0.12", area.isLevel2Extreme ? "0.18" : "0.14"),
      stroke: base.stroke,
      lineWidth: area.isLevel2Extreme ? 2.3 : (area.isLevel1Extreme ? 1.85 : 1.35),
      lineDash: [],
    };
  }

  function zoneCenterStepPx(index) {
    if (!state.chart || !state.bars.length) {
      return 12;
    }
    const current = state.chart.convertToPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [index, 0]);
    if (!Array.isArray(current)) {
      return 12;
    }
    let neighbor = null;
    if (index + 1 < state.bars.length) {
      neighbor = state.chart.convertToPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [index + 1, 0]);
    } else if (index > 0) {
      neighbor = state.chart.convertToPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [index - 1, 0]);
    }
    if (!Array.isArray(neighbor)) {
      return 12;
    }
    const step = Math.abs(Number(neighbor[0]) - Number(current[0]));
    return Number.isFinite(step) && step > 0 ? step : 12;
  }

  function buildZoneOverlayGraphics() {
    const chart = state.chart;
    if (!chart || !displayShowsZones(currentConfig().display) || !state.zones.length || !state.bars.length) {
      return [];
    }
    const grid = chart.getModel()?.getComponent("grid", 0);
    const rect = grid?.coordinateSystem?.getRect?.();
    if (!rect) {
      return [];
    }

    const elements = [];
    state.zones.forEach((zone, index) => {
      const span = zoneSpanFor(zone);
      if (!span) {
        return;
      }
      const barIndex = span.startIndex;
      const leftPoint = chart.convertToPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [barIndex, zone.zoneLow]);
      const topPoint = chart.convertToPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [barIndex, zone.zoneHigh]);
      const bottomPoint = chart.convertToPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [barIndex, zone.zoneLow]);
      if (!Array.isArray(leftPoint) || !Array.isArray(topPoint) || !Array.isArray(bottomPoint)) {
        return;
      }

      const step = zoneCenterStepPx(barIndex);
      let left = Number(leftPoint[0]) - step / 2;
      let right = Number(leftPoint[0]) + step / 2;
      let top = Math.min(Number(topPoint[1]), Number(bottomPoint[1]));
      let bottom = Math.max(Number(topPoint[1]), Number(bottomPoint[1]));
      if (!Number.isFinite(left) || !Number.isFinite(right) || !Number.isFinite(top) || !Number.isFinite(bottom)) {
        return;
      }

      left = Math.max(rect.x, left);
      right = Math.min(rect.x + rect.width, right);
      top = Math.max(rect.y, top);
      bottom = Math.min(rect.y + rect.height, bottom);
      const style = zoneStyle(zone);

      elements.push({
        id: "zone-rect-" + (zone.id || index),
        type: "rect",
        silent: true,
        z: 1,
        shape: {
          x: left,
          y: top,
          width: Math.max(1, right - left),
          height: Math.max(2, bottom - top),
          r: 2,
        },
        style: {
          fill: style.fill,
          stroke: style.stroke,
          lineWidth: style.lineWidth,
        },
      });
    });
    return elements;
  }

  function buildAreaOverlayGraphics() {
    const chart = state.chart;
    if (!chart || !currentConfig().showAreas || !state.areaRows.length || !state.bars.length) {
      return [];
    }
    const grid = chart.getModel()?.getComponent("grid", 0);
    const rect = grid?.coordinateSystem?.getRect?.();
    if (!rect) {
      return [];
    }

    const elements = [];
    state.areaRows.forEach((area, index) => {
      const span = areaSpanFor(area);
      if (!span) {
        return;
      }
      const leftPoint = chart.convertToPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [span.startIndex, area.displayLow]);
      const rightPoint = chart.convertToPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [span.endIndex, area.displayLow]);
      const topPoint = chart.convertToPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [span.startIndex, area.displayHigh]);
      const bottomPoint = chart.convertToPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [span.startIndex, area.displayLow]);
      if (!Array.isArray(leftPoint) || !Array.isArray(rightPoint) || !Array.isArray(topPoint) || !Array.isArray(bottomPoint)) {
        return;
      }

      const stepStart = zoneCenterStepPx(span.startIndex);
      const stepEnd = zoneCenterStepPx(span.endIndex);
      let left = Number(leftPoint[0]) - stepStart / 2;
      let right = Number(rightPoint[0]) + stepEnd / 2;
      let top = Math.min(Number(topPoint[1]), Number(bottomPoint[1]));
      let bottom = Math.max(Number(topPoint[1]), Number(bottomPoint[1]));
      if (!Number.isFinite(left) || !Number.isFinite(right) || !Number.isFinite(top) || !Number.isFinite(bottom)) {
        return;
      }

      left = Math.max(rect.x, left);
      right = Math.min(rect.x + rect.width, right);
      top = Math.max(rect.y, top);
      bottom = Math.min(rect.y + rect.height, bottom);

      const style = areaStyle(area);
      elements.push({
        id: "area-rect-" + (area.id || index),
        type: "rect",
        silent: true,
        z: 0,
        shape: {
          x: left,
          y: top,
          width: Math.max(1, right - left),
          height: Math.max(2, bottom - top),
          r: 2,
        },
        style: {
          fill: style.fill,
          stroke: style.stroke,
          lineWidth: style.lineWidth,
          lineDash: style.lineDash,
        },
      });
    });
    return elements;
  }

  function renderZoneOverlay() {
    if (!state.chart) {
      return;
    }
    state.chart.setOption({
      graphic: [{
        id: "zone-overlay",
        type: "group",
        silent: true,
        z: 1,
        children: buildAreaOverlayGraphics().concat(buildZoneOverlayGraphics()),
      }],
    }, { replaceMerge: ["graphic"], lazyUpdate: true });
  }

  function queueZoneOverlayRender() {
    if (state.zoneOverlayFrame) {
      window.cancelAnimationFrame(state.zoneOverlayFrame);
    }
    state.zoneOverlayFrame = window.requestAnimationFrame(() => {
      state.zoneOverlayFrame = 0;
      renderZoneOverlay();
    });
  }

  function zoneTooltipHtml(zone, bar) {
    if (!zone) {
      return "";
    }
    const effectiveEndTime = zone.endTimestamp || zone.rightTimestamp || "";
    const effectiveEndTickId = zone.endTickId ?? zone.rightTickId ?? "";
    return [
      "<div class=\"zones-tip-zone\">",
      "<strong>" + escapeHtml(zone.symbol + " L" + zone.selectedLevel + " " + String(zone.status || "").toUpperCase()) + "</strong><br>",
      "pattern " + escapeHtml(zone.patternType || "") + " | series " + escapeHtml(bar?.series || currentConfig().series) + "<br>",
      "starttime " + escapeHtml(zone.startTimestamp || "") + "<br>",
      "endtime " + escapeHtml(effectiveEndTime) + "<br>",
      "starttickid " + escapeHtml(String(zone.startTickId ?? "")) + " | endtickid " + escapeHtml(String(effectiveEndTickId)) + "<br>",
      "startpivotid " + escapeHtml(String(zone.anchorStartPivotId ?? "")) + " | middlepivotid " + escapeHtml(String(zone.anchorMiddlePivotId ?? "")) + " | endpivotid " + escapeHtml(String(zone.anchorEndPivotId ?? "")) + "<br>",
      "O " + escapeHtml(formatPrice(bar?.open)) + " | H " + escapeHtml(formatPrice(bar?.high ?? zone.zoneHigh)) + " | L " + escapeHtml(formatPrice(bar?.low ?? zone.zoneLow)) + " | C " + escapeHtml(formatPrice(bar?.close)) + "<br>",
      "initialzonelow " + escapeHtml(formatPrice(zone.initialZoneLow)) + " | initialzonehigh " + escapeHtml(formatPrice(zone.initialZoneHigh)) + "<br>",
      "zonelow " + escapeHtml(formatPrice(zone.zoneLow)) + " | zonehigh " + escapeHtml(formatPrice(zone.zoneHigh)) + " | zoneheight " + escapeHtml(formatPrice(zone.zoneHeight)) + "<br>",
      "samesidedistance " + escapeHtml(formatPrice(zone.sameSideDistance)) + " | tickcountinside " + escapeHtml(String(zone.tickCountInside ?? "")) + " | durationms " + escapeHtml(String(zone.durationInsideMs ?? "")) + "<br>",
      "breakdirection " + escapeHtml(zone.breakoutDirection || "-") + " | breaktickid " + escapeHtml(String(zone.breakoutTickId ?? "")),
      "</div>",
    ].join("");
  }

  function areaTooltipHtml(area) {
    return [
      "<div class=\"zones-tip-zone\">",
      "<strong>" + escapeHtml(String(area.side || "").toUpperCase() + " " + String(area.state || "").toUpperCase()) + "</strong><br>",
      "birthtime " + escapeHtml(String(area.birthTime || "")) + " | sourcepivotid " + escapeHtml(String(area.sourcePivotId ?? "")) + "<br>",
      "originallow " + escapeHtml(formatPrice(area.originalLow)) + " | originalhigh " + escapeHtml(formatPrice(area.originalHigh)) + "<br>",
      "currentlow " + escapeHtml(formatPrice(area.currentLow)) + " | currenthigh " + escapeHtml(formatPrice(area.currentHigh)) + "<br>",
      "isl1extreme " + escapeHtml(area.isLevel1Extreme ? "yes" : "no") + " | isl2extreme " + escapeHtml(area.isLevel2Extreme ? "yes" : "no") + "<br>",
      "priorityscore " + escapeHtml(String(area.priorityScore ?? "")) + " | touchcount " + escapeHtml(String(area.touchCount ?? 0)) + "<br>",
      "firsttouchtime " + escapeHtml(String(area.firstTouchTime || "-")) + " | firstbreaktime " + escapeHtml(String(area.firstBreakTime || "-")) + "<br>",
      "closereason " + escapeHtml(String(area.closeReason || "-")),
      "</div>",
    ].join("");
  }

  function zoomIndicesFromState(zoomState, barCount) {
    if (!barCount) {
      return null;
    }
    const directStart = Number(zoomState?.startValue);
    const directEnd = Number(zoomState?.endValue);
    if (Number.isFinite(directStart) && Number.isFinite(directEnd)) {
      const startIndex = Math.max(0, Math.min(barCount - 1, Math.floor(Math.min(directStart, directEnd))));
      const endIndex = Math.max(startIndex, Math.min(barCount - 1, Math.ceil(Math.max(directStart, directEnd))));
      return { startIndex, endIndex };
    }
    const startPercent = Math.max(0, Math.min(100, Number(zoomState?.start)));
    const endPercent = Math.max(0, Math.min(100, Number(zoomState?.end)));
    const startIndex = Math.max(0, Math.min(barCount - 1, Math.floor((startPercent / 100) * barCount)));
    const endIndex = Math.max(startIndex, Math.min(barCount - 1, Math.ceil((endPercent / 100) * barCount) - 1));
    return { startIndex, endIndex };
  }

  function zoomStateFromIndexRange(startIndex, endIndex, barCount) {
    if (!barCount) {
      return { start: 0, end: 100 };
    }
    const clampedStart = Math.max(0, Math.min(barCount - 1, Math.floor(startIndex)));
    const clampedEnd = Math.max(clampedStart, Math.min(barCount - 1, Math.ceil(endIndex)));
    return {
      start: (clampedStart / barCount) * 100,
      end: ((clampedEnd + 1) / barCount) * 100,
      startValue: clampedStart,
      endValue: clampedEnd,
    };
  }

  function captureVisibleBarWindow() {
    if (!state.bars.length) {
      return null;
    }
    const option = state.chart ? state.chart.getOption() : null;
    const zoomState = option?.dataZoom?.[0] || state.zoom;
    const indices = zoomIndicesFromState(zoomState, state.bars.length);
    if (!indices) {
      return null;
    }
    const width = Math.max(0, indices.endIndex - indices.startIndex);
    const rightGap = Math.max(0, state.bars.length - 1 - indices.endIndex);
    return {
      startBarId: state.bars[indices.startIndex]?.id ?? null,
      endBarId: state.bars[indices.endIndex]?.id ?? null,
      width: width,
      anchoredRight: rightGap <= Math.max(1, Math.min(4, Math.ceil((width + 1) * 0.08))),
    };
  }

  function restoreVisibleBarWindow(windowState) {
    if (!windowState || !state.bars.length) {
      return false;
    }
    const width = Math.max(0, Number(windowState.width) || 0);
    const startIndex = state.bars.findIndex((bar) => bar.id === windowState.startBarId);
    const endIndex = state.bars.findIndex((bar) => bar.id === windowState.endBarId);
    if (startIndex >= 0 && endIndex >= 0) {
      state.zoom = zoomStateFromIndexRange(startIndex, endIndex, state.bars.length);
      return true;
    }
    if (windowState.anchoredRight) {
      const nextEnd = state.bars.length - 1;
      const nextStart = Math.max(0, nextEnd - width);
      state.zoom = zoomStateFromIndexRange(nextStart, nextEnd, state.bars.length);
      return true;
    }
    if (endIndex >= 0) {
      const nextStart = Math.max(0, endIndex - width);
      state.zoom = zoomStateFromIndexRange(nextStart, endIndex, state.bars.length);
      return true;
    }
    if (startIndex >= 0) {
      const nextEnd = Math.min(state.bars.length - 1, startIndex + width);
      state.zoom = zoomStateFromIndexRange(startIndex, nextEnd, state.bars.length);
      return true;
    }
    return false;
  }

  function indexIsVisible(index) {
    const range = visibleIndexRange();
    if (!range) {
      return false;
    }
    return index >= range.startIndex && index <= range.endIndex;
  }

  function softFocusIndex(index) {
    if (!state.bars.length || !Number.isInteger(index)) {
      return false;
    }
    const range = visibleIndexRange();
    if (!range || indexIsVisible(index)) {
      return false;
    }
    const width = Math.max(0, range.endIndex - range.startIndex);
    let nextStart = index < range.startIndex ? index : index - width;
    nextStart = Math.max(0, Math.min(state.bars.length - 1, nextStart));
    const nextEnd = Math.max(nextStart, Math.min(state.bars.length - 1, nextStart + width));
    state.zoom = zoomStateFromIndexRange(nextStart, nextEnd, state.bars.length);
    return true;
  }

  function showTipForIndex(index) {
    if (!state.chart || !Number.isInteger(index) || !indexIsVisible(index)) {
      return;
    }
    window.requestAnimationFrame(() => {
      if (!state.chart) {
        return;
      }
      state.chart.dispatchAction({ type: "showTip", seriesIndex: 0, dataIndex: index });
    });
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
            const point = Array.isArray(params)
              ? params.find((entry) => entry?.seriesId === "zones-candles-main") || params[0]
              : params;
            const bar = point?.data?.bar;
            const zone = zoneForBarIndex(Number(point?.dataIndex));
            const areaHtml = currentConfig().showAreas
              ? areasForBarIndex(Number(point?.dataIndex)).map(areaTooltipHtml).join("")
              : "";
            if (!zone && !bar && !areaHtml) {
              return "";
            }
            return [
              "<div class=\"zones-tip\">",
              zoneTooltipHtml(zone, bar),
              areaHtml,
              "</div>",
            ].join("");
          },
        },
        xAxis: {
          type: "category",
          boundaryGap: true,
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
        series: [{
          id: "zones-candles-main",
          type: "candlestick",
          name: "Zone candles",
          data: [],
        }],
      }, { notMerge: true, lazyUpdate: true });

      state.chart.on("dataZoom", function () {
        if (state.applyingZoom) {
          return;
        }
        const option = state.chart.getOption();
        const zoom = option?.dataZoom?.[0] || state.zoom;
        const indices = zoomIndicesFromState(zoom, state.bars.length);
        state.zoom = indices
          ? zoomStateFromIndexRange(indices.startIndex, indices.endIndex, state.bars.length)
          : { start: 0, end: 100 };
        queueVisibleYAxisUpdate();
        queueZoneOverlayRender();
      });

      state.chart.on("click", function (params) {
        if (params?.seriesId !== "zones-candles-main") {
          return;
        }
        const bar = params?.data?.bar || state.bars[Number(params?.dataIndex)];
        if (bar?.zoneId != null) {
          focusZone(bar.zoneId);
        }
      });
    }
    return state.chart;
  }

  function visibleIndexRange() {
    if (!state.bars.length) {
      return null;
    }
    return zoomIndicesFromState(state.zoom, state.bars.length);
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
      const range = visibleIndexRange();
      const startIndex = range ? range.startIndex : 0;
      const endIndex = range ? range.endIndex : state.bars.length - 1;
      let minValue = Number.POSITIVE_INFINITY;
      let maxValue = Number.NEGATIVE_INFINITY;

      state.bars.slice(startIndex, Math.max(startIndex + 1, endIndex + 1)).forEach((bar) => {
        minValue = Math.min(minValue, bar.low);
        maxValue = Math.max(maxValue, bar.high);
      });

      state.zones.forEach((zone) => {
        const span = zoneSpanFor(zone);
        if (!span || span.endIndex < startIndex || span.startIndex > endIndex) {
          return;
        }
        minValue = Math.min(minValue, zone.zoneLow);
        maxValue = Math.max(maxValue, zone.zoneHigh);
      });
      if (currentConfig().showAreas) {
        state.areaRows.forEach((area) => {
          const span = areaSpanFor(area);
          if (!span || span.endIndex < startIndex || span.startIndex > endIndex) {
            return;
          }
          minValue = Math.min(minValue, Number(area.displayLow));
          maxValue = Math.max(maxValue, Number(area.displayHigh));
        });
      }

      if (!Number.isFinite(minValue) || !Number.isFinite(maxValue)) {
        return;
      }
      const span = Math.max(0.01, maxValue - minValue);
      const padding = Math.max(0.05, span * 0.055);
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
        series: [{ id: "zones-candles-main", data: [] }],
      }, { replaceMerge: ["series"], lazyUpdate: true });
      queueZoneOverlayRender();
      return;
    }
    if (resetView) {
      state.zoom = { start: 0, end: 100 };
    }
    state.applyingZoom = true;
    chart.setOption({
      xAxis: { data: state.bars.map(candleAxisLabel) },
      dataZoom: ZOOM_COMPONENT_IDS.map((id) => ({
        id: id,
        start: state.zoom.start,
        end: state.zoom.end,
        startValue: Number.isFinite(Number(state.zoom.startValue)) ? Number(state.zoom.startValue) : undefined,
        endValue: Number.isFinite(Number(state.zoom.endValue)) ? Number(state.zoom.endValue) : undefined,
        rangeMode: ["value", "value"],
      })),
      series: [{
        id: "zones-candles-main",
        type: "candlestick",
        name: "Zone candles",
        data: buildChartData(),
        z: 4,
      }],
    }, { replaceMerge: ["series"], lazyUpdate: true });
    state.renderedSeries = chart.getOption()?.series || [];
    window.requestAnimationFrame(() => {
      const option = chart.getOption();
      const zoom = option?.dataZoom?.[0] || state.zoom;
      const indices = zoomIndicesFromState(zoom, state.bars.length);
      state.zoom = indices
        ? zoomStateFromIndexRange(indices.startIndex, indices.endIndex, state.bars.length)
        : state.zoom;
      state.applyingZoom = false;
      state.renderedSeries = chart.getOption()?.series || [];
      queueVisibleYAxisUpdate();
      queueZoneOverlayRender();
    });
  }

  function replaceBars(rows) {
    state.bars = Array.isArray(rows) ? rows.slice() : [];
  }

  function replaceZones(rows) {
    const nextRows = Array.isArray(rows) ? rows : [];
    const byId = new Map();
    nextRows.forEach((row) => {
      if (!row || row.id == null) {
        return;
      }
      byId.set(row.id, row);
    });
    state.zones = Array.from(byId.values()).sort((left, right) => {
      const leftStart = Number(left?.startTickId || 0);
      const rightStart = Number(right?.startTickId || 0);
      if (leftStart !== rightStart) {
        return leftStart - rightStart;
      }
      return Number(left?.id || 0) - Number(right?.id || 0);
    });
    if (!state.zones.some((zone) => zone.id === state.selectedZoneId)) {
      const activeZone = state.zones.find((zone) => zone.status === "active");
      state.selectedZoneId = activeZone ? activeZone.id : (state.zones[state.zones.length - 1]?.id ?? null);
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
    replaceZones(payload.zones || []);
    replaceAreaRows(payload.areaRows || []);
    applyRangePayload(payload);
    state.reviewEndId = payload.reviewEndId || state.reviewEndId || null;
    state.hasMoreLeft = Boolean(payload.hasMoreLeft);
    state.lastMetrics = payload.metrics || null;
    renderMeta();
    renderPerf();
    renderTable();
  }

  function replaceAreaRows(rows) {
    state.areaRows = (Array.isArray(rows) ? rows.slice() : []).sort((left, right) => {
      const leftBirth = Number(left?.birthTickId || 0);
      const rightBirth = Number(right?.birthTickId || 0);
      if (leftBirth !== rightBirth) {
        return leftBirth - rightBirth;
      }
      const priorityDiff = Number(right?.priorityScore || 0) - Number(left?.priorityScore || 0);
      if (priorityDiff !== 0) {
        return priorityDiff;
      }
      return Number(left?.id || 0) - Number(right?.id || 0);
    });
  }

  function focusZone(zoneId) {
    const zone = state.zones.find((item) => item.id === zoneId);
    if (!zone) {
      return;
    }
    state.selectedZoneId = zoneId;
    renderTable();
    const index = findBarIndexByZoneId(zone.id);
    if (index < 0) {
      renderChart(false);
      return;
    }
    renderChart(false);
    showTipForIndex(index);
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

  async function refreshZoneState(level) {
    const payload = await fetchJson("/api/zones/state?" + new URLSearchParams({
      level: String(level),
    }).toString());
    state.zoneState = payload.state || null;
    renderZoneStateMeta();
  }

  async function resolveReviewStartId(config) {
    if (config.reviewStart) {
      const payload = await fetchJson("/api/zones/review-start?" + new URLSearchParams({
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
      showAreas: config.showAreas ? "1" : "0",
      areaStates: config.areaStates.join(","),
      areaSides: config.areaSides.join(","),
      areaHigherOnly: config.areaHigherOnly ? "1" : "0",
    });
    if (config.mode === "review" && startId != null) {
      params.set("id", String(startId));
    }
    return "/api/zones/bootstrap?" + params.toString();
  }

  function nextUrl(config, afterId, endId) {
    const params = new URLSearchParams({
      afterId: String(afterId),
      limit: String(Math.max(25, Math.min(500, Math.round(100 * config.reviewSpeed)))),
      window: String(currentLoadedWindow()),
      level: String(config.level),
      series: config.series,
      provisional: config.provisional ? "true" : "false",
      showAreas: config.showAreas ? "1" : "0",
      areaStates: config.areaStates.join(","),
      areaSides: config.areaSides.join(","),
      areaHigherOnly: config.areaHigherOnly ? "1" : "0",
    });
    if (endId != null) {
      params.set("endId", String(endId));
    }
    if (config.mode === "review" && state.reviewStartId != null) {
      params.set("reviewStartId", String(state.reviewStartId));
    }
    return "/api/zones/next?" + params.toString();
  }

  function previousUrl(config, limit) {
    return "/api/zones/previous?" + new URLSearchParams({
      currentLastId: String(state.rangeLastId || 1),
      limit: String(limit),
      window: String(currentLoadedWindow()),
      level: String(config.level),
      series: config.series,
      provisional: config.provisional ? "true" : "false",
      showAreas: config.showAreas ? "1" : "0",
      areaStates: config.areaStates.join(","),
      areaSides: config.areaSides.join(","),
      areaHigherOnly: config.areaHigherOnly ? "1" : "0",
    }).toString();
  }

  async function loadBootstrap(resetView) {
    const config = currentConfig();
    state.reviewStartId = config.mode === "review" ? await resolveReviewStartId(config) : null;
    const payload = await fetchJson(bootstrapUrl(config, state.reviewStartId));
    const preservedViewport = resetView ? null : captureVisibleBarWindow();
    state.loadedWindow = clampLoadedWindow(payload.window || config.window);
    syncPayload(payload);
    await refreshZoneState(config.level);
    restoreVisibleBarWindow(preservedViewport);
    renderChart(Boolean(resetView));
    status("Loaded " + state.zones.length + " persisted zone episode(s).", false);
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
    const source = new EventSource("/api/zones/stream?" + new URLSearchParams({
      afterId: String(afterId || 0),
      limit: "250",
      window: String(currentLoadedWindow()),
      level: String(config.level),
      series: config.series,
      provisional: config.provisional ? "true" : "false",
      showAreas: config.showAreas ? "1" : "0",
      areaStates: config.areaStates.join(","),
      areaSides: config.areaSides.join(","),
      areaHigherOnly: config.areaHigherOnly ? "1" : "0",
    }).toString());
    state.source = source;

    source.onopen = function () {
      state.streamConnected = true;
      renderPerf();
      status("Live stream connected.", false);
    };

    source.onmessage = function (event) {
      const payload = JSON.parse(event.data);
      const preservedViewport = captureVisibleBarWindow();
      syncPayload(payload);
      restoreVisibleBarWindow(preservedViewport);
      renderChart(false);
      refreshZoneState(currentConfig().level).catch(() => {});
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
    const preservedViewport = captureVisibleBarWindow();
    syncPayload(payload);
    await refreshZoneState(config.level);
    restoreVisibleBarWindow(preservedViewport);
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
    const remaining = Math.max(0, MAX_CANDLE_WINDOW - currentLoadedWindow());
    if (remaining <= 0) {
      return 0;
    }
    return Math.max(1, Math.min(currentConfig().window, remaining));
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
    const config = currentConfig();
    const previousFirstId = state.rangeFirstId;
    const previousLoadedWindow = currentLoadedWindow();
    const batchSize = historyBatchSize();
    if (!batchSize) {
      status("Loaded history is already at the current chart cap.", false);
      await resumeRunIfNeeded();
      return;
    }
    const preservedViewport = captureVisibleBarWindow();
    const payload = await fetchJson(previousUrl(config, batchSize));
    const didExpandLeft = payload.firstId != null && previousFirstId != null && payload.firstId < previousFirstId;
    state.loadedWindow = didExpandLeft ? clampLoadedWindow(previousLoadedWindow + batchSize) : previousLoadedWindow;
    syncPayload(payload);
    await refreshZoneState(config.level);
    restoreVisibleBarWindow(preservedViewport);
    renderChart(false);
    status(state.zones.length && didExpandLeft ? "Older zone episodes were added off-screen to the left." : "No older zones were available.", false);
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
    writeQuery();
    renderMeta();
    renderChart(false);
    queueChartResize();
    status("Display updated.", false);
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
      if (elements.showTable.checked) {
        setTableCollapsed(true);
      }
      writeQuery();
      renderMeta();
      renderChart(false);
      queueChartResize();
      status("Settings updated. Click Load to refresh persisted zones.", false);
    });
  });

  [
    elements.showAreas,
    elements.areaStateActive,
    elements.areaStateUsed,
    elements.areaStateClosed,
    elements.areaSideTop,
    elements.areaSideBottom,
    elements.areaHigherOnly,
  ].forEach((control) => {
    control.addEventListener("change", function () {
      currentConfig();
      writeQuery();
      loadAll(false).catch((error) => {
        status(error.message || "Area refresh failed.", true);
      });
      status("Unused area filters updated.", false);
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

  elements.tableToggle.addEventListener("click", function () {
    setTableCollapsed(!state.ui.tableCollapsed);
  });

  applyInitialConfig(parseQuery());
  loadAll(true);
}());
