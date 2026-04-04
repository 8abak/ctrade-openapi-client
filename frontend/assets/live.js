(function () {
  const DEFAULTS = {
    mode: "live",
    run: "run",
    showTicks: true,
    showZigs: true,
    showZones: false,
    series: "mid",
    id: "",
    reviewStart: "",
    reviewSpeed: 1,
    window: 2000,
  };

  const DISPLAY_CONFIG = {
    ticks: { label: "Ticks", maxWindow: 10000 },
    "ticks-zig": { label: "Ticks + Zigs", maxWindow: 10000 },
    "ticks-zones": { label: "Ticks + Zones", maxWindow: 10000 },
    "ticks-zig-zones": { label: "Ticks + Zigs + Zones", maxWindow: 10000 },
    zig: { label: "Zigs Only", maxWindow: 100000 },
    "zig-zones": { label: "Zigs + Zones", maxWindow: 100000 },
    zones: { label: "Zones Only", maxWindow: 100000 },
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
    zoneRows: [],
    loadedWindow: DEFAULTS.window,
    renderedSeries: [],
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
    autoscaleFrame: 0,
    zoneOverlayFrame: 0,
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
    showTicks: document.getElementById("showTicks"),
    showZigs: document.getElementById("showZigs"),
    showZones: document.getElementById("showZones"),
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
    const legacyDisplay = params.get("display") || "ticks-zig";
    return {
      mode: params.get("mode") === "review" ? "review" : DEFAULTS.mode,
      run: params.get("run") === "stop" ? "stop" : DEFAULTS.run,
      showTicks: params.has("showTicks") ? params.get("showTicks") !== "0" : (legacyDisplay === "ticks" || legacyDisplay === "ticks-zig"),
      showZigs: params.has("showZigs") ? params.get("showZigs") !== "0" : (legacyDisplay === "ticks-zig" || legacyDisplay === "zig"),
      showZones: params.get("showZones") === "1",
      series: Object.prototype.hasOwnProperty.call(SERIES_CONFIG, params.get("series")) ? params.get("series") : DEFAULTS.series,
      id: params.get("id") || DEFAULTS.id,
      reviewStart: params.get("reviewStart") || DEFAULTS.reviewStart,
      reviewSpeed: REVIEW_SPEEDS.includes(reviewSpeed) ? reviewSpeed : DEFAULTS.reviewSpeed,
      window: sanitizeWindowValue(params.get("window"), displayKeyFromLayers(
        params.has("showTicks") ? params.get("showTicks") !== "0" : (legacyDisplay === "ticks" || legacyDisplay === "ticks-zig"),
        params.has("showZigs") ? params.get("showZigs") !== "0" : (legacyDisplay === "ticks-zig" || legacyDisplay === "zig"),
        params.get("showZones") === "1",
      )),
    };
  }

  function displayKeyFromLayers(showTicks, showZigs, showZones) {
    const enabled = {
      ticks: Boolean(showTicks),
      zigs: Boolean(showZigs),
      zones: Boolean(showZones),
    };
    if (!enabled.ticks && !enabled.zigs && !enabled.zones) {
      enabled.ticks = true;
    }
    if (enabled.ticks && enabled.zigs && enabled.zones) {
      return "ticks-zig-zones";
    }
    if (enabled.ticks && enabled.zigs) {
      return "ticks-zig";
    }
    if (enabled.ticks && enabled.zones) {
      return "ticks-zones";
    }
    if (enabled.zigs && enabled.zones) {
      return "zig-zones";
    }
    if (enabled.ticks) {
      return "ticks";
    }
    if (enabled.zigs) {
      return "zig";
    }
    return "zones";
  }

  function sanitizeWindowValue(rawValue, displayKey) {
    const config = DISPLAY_CONFIG[displayKey] || DISPLAY_CONFIG.ticks;
    return Math.max(1, Math.min(config.maxWindow, Number.parseInt(rawValue || String(DEFAULTS.window), 10) || DEFAULTS.window));
  }

  function writeQuery() {
    const config = currentConfig();
    const params = new URLSearchParams();
    params.set("mode", config.mode);
    params.set("run", config.run);
    params.set("showTicks", config.showTicks ? "1" : "0");
    params.set("showZigs", config.showZigs ? "1" : "0");
    params.set("showZones", config.showZones ? "1" : "0");
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
    let showTicks = Boolean(elements.showTicks.checked);
    let showZigs = Boolean(elements.showZigs.checked);
    let showZones = Boolean(elements.showZones.checked);
    if (!showTicks && !showZigs && !showZones) {
      showTicks = true;
      elements.showTicks.checked = true;
    }
    const display = displayKeyFromLayers(showTicks, showZigs, showZones);
    return {
      mode: elements.modeToggle.querySelector("button.active")?.dataset.value || DEFAULTS.mode,
      run: elements.runToggle.querySelector("button.active")?.dataset.value || DEFAULTS.run,
      display,
      showTicks,
      showZigs,
      showZones,
      series: elements.seriesToggle.querySelector("button.active")?.dataset.value || DEFAULTS.series,
      id: (elements.tickId.value || "").trim(),
      reviewStart: (elements.reviewStart.value || "").trim(),
      reviewSpeed: Number.parseFloat(elements.reviewSpeedToggle.querySelector("button.active")?.dataset.value || String(DEFAULTS.reviewSpeed)),
      window: sanitizeWindowValue(elements.windowSize.value, display),
    };
  }

  function displayUsesTicks(displayMode) {
    return displayMode === "ticks" || displayMode === "ticks-zig" || displayMode === "ticks-zones" || displayMode === "ticks-zig-zones";
  }

  function displayUsesZig(displayMode) {
    return displayMode === "ticks-zig" || displayMode === "ticks-zig-zones" || displayMode === "zig" || displayMode === "zig-zones";
  }

  function displayUsesZones(displayMode) {
    return displayMode === "ticks-zones" || displayMode === "ticks-zig-zones" || displayMode === "zig-zones" || displayMode === "zones";
  }

  function updateWindowConstraints() {
    const displayKey = currentConfig().display;
    elements.windowSize.max = String(DISPLAY_CONFIG[displayKey].maxWindow);
    elements.windowSize.value = String(sanitizeWindowValue(elements.windowSize.value, displayKey));
  }

  function maxLoadedWindow(displayMode) {
    return (DISPLAY_CONFIG[displayMode] || DISPLAY_CONFIG.ticks).maxWindow;
  }

  function currentLoadedWindow(config) {
    const effectiveConfig = config || currentConfig();
    return Math.max(1, Math.min(maxLoadedWindow(effectiveConfig.display), Number(state.loadedWindow) || effectiveConfig.window));
  }

  function updateSeriesAvailability() {
    elements.seriesToggle.closest(".live-control-field").hidden = !currentConfig().showTicks;
  }

  function applyInitialConfig(config) {
    setSegment(elements.modeToggle, config.mode);
    setSegment(elements.runToggle, config.run);
    elements.showTicks.checked = Boolean(config.showTicks);
    elements.showZigs.checked = Boolean(config.showZigs);
    elements.showZones.checked = Boolean(config.showZones);
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
      queueVisibleYAxisUpdate(state.viewport || captureViewportFromChart(getDatasetBounds()) || getDatasetBounds());
      queueZoneOverlayRender();
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
    const activeZones = state.zoneRows.filter((row) => row.status === "active").length;
    const closedZones = state.zoneRows.filter((row) => row.status === "closed").length;
    const parts = [
      config.mode.toUpperCase(),
      DISPLAY_CONFIG[config.display].label,
      "ticks " + state.rows.length + "/" + config.window,
      "left " + state.rangeFirstId,
      "right " + state.rangeLastId,
      state.hasMoreLeft ? "more-left yes" : "more-left no",
    ];
    if (config.showZigs) {
      parts.push("zig L0 " + level0Count);
      parts.push("L1 " + level1Count);
      parts.push("L2 " + level2Count);
      parts.push("L3 " + level3Count);
      parts.push("cand " + candidateCount);
    }
    if (config.showZones) {
      parts.push("zones " + state.zoneRows.length);
      parts.push("z-active " + activeZones);
      parts.push("z-closed " + closedZones);
    }
    if (config.showTicks) {
      parts.push("series " + config.series);
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
          formatter: function (params) {
            const entries = Array.isArray(params) ? params : [params];
            const point = entries[0];
            const timestampMs = Number(point?.axisValue ?? point?.value?.[0]);
            const lines = entries
              .filter((entry) => typeof entry?.value?.[1] === "number")
              .map((entry) => {
                return "<div>" + String(entry.marker || "") + String(entry.seriesName || "") + ": " + Number(entry.value[1]).toFixed(2) + "</div>";
              });
            const zoneHtml = Number.isFinite(timestampMs) && displayUsesZones(currentConfig().display)
              ? zoneRowsAtTimestamp(timestampMs).map(zoneTooltipHtml).join("")
              : "";
            if (!lines.length && !zoneHtml) {
              return "";
            }
            return [
              "<div class=\"zigcandles-tip\">",
              lines.join(""),
              zoneHtml,
              "</div>",
            ].join("");
          },
        },
        xAxis: {
          type: "time",
          boundaryGap: [0.01, 0.02],
          axisLabel: { color: "#9eadc5" },
        },
        yAxis: { type: "value", scale: true, axisLabel: { color: "#9eadc5" } },
        dataZoom: [
          { id: "zoom-inside", type: "inside", filterMode: "none", zoomLock: false, rangeMode: ["value", "value"] },
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

      state.chart.on("dataZoom", (event) => {
        if (state.applyingViewport) {
          return;
        }
        const bounds = getDatasetBounds();
        state.viewport = viewportFromZoomState(resolveZoomSource(event), bounds)
          || captureViewportFromChart(bounds)
          || normalizeViewport(state.viewport, bounds);
        queueVisibleYAxisUpdate(state.viewport);
        queueZoneOverlayRender();
      });
    }

    return state.chart;
  }

  function resolveZoomSource(source) {
    if (!source) {
      return null;
    }
    if (Array.isArray(source.batch) && source.batch.length) {
      return source.batch[0];
    }
    return source;
  }

  function viewportFromZoomState(source, bounds) {
    if (!bounds || !source) {
      return null;
    }
    const zoomState = resolveZoomSource(source);
    if (!zoomState) {
      return null;
    }
    const fullSpan = Math.max(0, bounds.endValue - bounds.startValue);
    const startPercent = Number(zoomState.start);
    const endPercent = Number(zoomState.end);
    let startValue;
    let endValue;

    if (Number.isFinite(startPercent) && Number.isFinite(endPercent)) {
      startValue = bounds.startValue + (Math.max(0, Math.min(100, startPercent)) / 100) * fullSpan;
      endValue = bounds.startValue + (Math.max(0, Math.min(100, endPercent)) / 100) * fullSpan;
    } else {
      startValue = Number(zoomState.startValue);
      endValue = Number(zoomState.endValue);
    }

    if (!Number.isFinite(startValue) || !Number.isFinite(endValue)) {
      return null;
    }
    return normalizeViewport({ startValue, endValue }, bounds);
  }

  function captureViewportFromChart(bounds) {
    if (!state.chart) {
      return null;
    }
    const option = state.chart.getOption();
    if (!option || !option.dataZoom || !option.dataZoom.length) {
      return null;
    }
    return viewportFromZoomState(option.dataZoom[0] || {}, bounds);
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
    if (state.zoneRows.length) {
      return {
        startValue: state.zoneRows[0].startTimestampMs,
        endValue: Math.max(...state.zoneRows.map((row) => Number(row.rightTimestampMs ?? row.endTimestampMs ?? row.startTimestampMs))),
      };
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

  function viewportNearRightEdge(viewport, bounds) {
    if (!viewport || !bounds) {
      return false;
    }
    const gap = Number(bounds.endValue) - Number(viewport.endValue);
    const span = Math.max(1, Number(viewport.span) || (Number(viewport.endValue) - Number(viewport.startValue)));
    if (!Number.isFinite(gap) || !Number.isFinite(span)) {
      return false;
    }
    return gap <= Math.max(span * 0.04, 1000);
  }

  function shiftViewportForward(viewport, previousBounds, nextBounds, shouldAdvance) {
    if (!shouldAdvance || !viewport || !previousBounds || !nextBounds || !viewportNearRightEdge(viewport, previousBounds)) {
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

  function buildDataZoomState(viewport, bounds) {
    const normalized = normalizeViewport(viewport, bounds);
    const fullSpan = bounds ? Math.max(0, bounds.endValue - bounds.startValue) : 0;
    const startPercent = !normalized || fullSpan <= 0
      ? 0
      : ((normalized.startValue - bounds.startValue) / fullSpan) * 100;
    const endPercent = !normalized || fullSpan <= 0
      ? 100
      : ((normalized.endValue - bounds.startValue) / fullSpan) * 100;
    return ZOOM_COMPONENT_IDS.map((id) => {
      const next = { id, filterMode: "none" };
      next.start = Math.max(0, Math.min(100, startPercent));
      next.end = Math.max(0, Math.min(100, endPercent));
      next.rangeMode = ["value", "value"];
      if (normalized) {
        next.startValue = normalized.startValue;
        next.endValue = normalized.endValue;
      }
      return next;
    });
  }

  function pointValueAt(item, index) {
    if (Array.isArray(item)) {
      return Number(item[index]);
    }
    if (item && Array.isArray(item.value)) {
      return Number(item.value[index]);
    }
    if (item && typeof item === "object") {
      return Number(index === 0 ? item.x : item.y);
    }
    return Number.NaN;
  }

  function lowerBoundSeriesData(data, targetX) {
    let low = 0;
    let high = data.length;
    while (low < high) {
      const mid = Math.floor((low + high) / 2);
      if (pointValueAt(data[mid], 0) < targetX) {
        low = mid + 1;
      } else {
        high = mid;
      }
    }
    return low;
  }

  function upperBoundSeriesData(data, targetX) {
    let low = 0;
    let high = data.length;
    while (low < high) {
      const mid = Math.floor((low + high) / 2);
      if (pointValueAt(data[mid], 0) <= targetX) {
        low = mid + 1;
      } else {
        high = mid;
      }
    }
    return low;
  }

  function seriesVisibleSliceBounds(data, viewport) {
    if (!data.length || !viewport) {
      return { startIndex: 0, endIndex: data.length };
    }
    const startX = Number(viewport.startValue);
    const endX = Number(viewport.endValue);
    if (!Number.isFinite(startX) || !Number.isFinite(endX)) {
      return { startIndex: 0, endIndex: data.length };
    }
    const firstVisible = lowerBoundSeriesData(data, startX);
    const afterVisible = upperBoundSeriesData(data, endX);
    return {
      startIndex: firstVisible,
      endIndex: afterVisible,
    };
  }

  function yAxisPadding(minValue, maxValue) {
    const span = Math.max(0, maxValue - minValue);
    if (span > 0) {
      return Math.max(span * 0.04, Math.abs(span) * 0.01, 0.01);
    }
    return Math.max(Math.abs(maxValue || minValue || 0) * 0.0025, 0.05);
  }

  function visibleYBounds(viewport, seriesList) {
    let minValue = Number.POSITIVE_INFINITY;
    let maxValue = Number.NEGATIVE_INFINITY;

    (seriesList || []).forEach((series) => {
      if (!series || series.includeInYAutoscale === false || !Array.isArray(series.data) || !series.data.length) {
        return;
      }
      const slice = seriesVisibleSliceBounds(series.data, viewport);
      for (let index = slice.startIndex; index < slice.endIndex; index += 1) {
        const xValue = pointValueAt(series.data[index], 0);
        const yValue = pointValueAt(series.data[index], 1);
        if (!Number.isFinite(xValue) || !Number.isFinite(yValue)) {
          continue;
        }
        minValue = Math.min(minValue, yValue);
        maxValue = Math.max(maxValue, yValue);
      }
    });

    if (displayUsesZones(currentConfig().display) && state.zoneRows.length && viewport) {
      const startX = Number(viewport.startValue);
      const endX = Number(viewport.endValue);
      state.zoneRows.forEach((zone) => {
        const zoneStart = Number(zone.startTimestampMs);
        const zoneEnd = Number(zone.rightTimestampMs ?? zone.endTimestampMs ?? zone.startTimestampMs);
        if (!Number.isFinite(zoneStart) || !Number.isFinite(zoneEnd) || zoneEnd < startX || zoneStart > endX) {
          return;
        }
        minValue = Math.min(minValue, Number(zone.zoneLow));
        maxValue = Math.max(maxValue, Number(zone.zoneHigh));
      });
    }

    if (!Number.isFinite(minValue) || !Number.isFinite(maxValue)) {
      return null;
    }
    const padding = yAxisPadding(minValue, maxValue);
    return {
      min: Number((minValue - padding).toFixed(6)),
      max: Number((maxValue + padding).toFixed(6)),
    };
  }

  function applyVisibleYAxis(viewport) {
    if (!state.chart) {
      return;
    }
    const bounds = visibleYBounds(viewport, state.renderedSeries);
    if (!bounds) {
      return;
    }
    state.chart.setOption({
      yAxis: {
        min: bounds.min,
        max: bounds.max,
      },
    }, { lazyUpdate: true });
  }

  function queueVisibleYAxisUpdate(viewport) {
    if (state.autoscaleFrame) {
      window.cancelAnimationFrame(state.autoscaleFrame);
      state.autoscaleFrame = 0;
    }
    state.autoscaleFrame = window.requestAnimationFrame(() => {
      state.autoscaleFrame = 0;
      const bounds = getDatasetBounds();
      applyVisibleYAxis(viewport || state.viewport || captureViewportFromChart(bounds) || bounds);
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

  function zoneRowsAtTimestamp(timestampMs) {
    const seen = new Set();
    return state.zoneRows.filter((zone) => {
      if (!zone || seen.has(zone.id)) {
        return false;
      }
      const start = Number(zone.startTimestampMs);
      const end = Number(zone.rightTimestampMs ?? zone.endTimestampMs ?? zone.startTimestampMs);
      if (!Number.isFinite(start) || !Number.isFinite(end) || timestampMs < start || timestampMs > end) {
        return false;
      }
      seen.add(zone.id);
      return true;
    });
  }

  function zoneStyle(zone) {
    const palette = [
      { fill: "rgba(109, 216, 255, 0.13)", stroke: "rgba(176, 238, 255, 0.72)" },
      { fill: "rgba(255, 200, 87, 0.12)", stroke: "rgba(255, 214, 138, 0.7)" },
      { fill: "rgba(255, 140, 66, 0.11)", stroke: "rgba(255, 185, 145, 0.68)" },
      { fill: "rgba(248, 250, 252, 0.08)", stroke: "rgba(229, 236, 246, 0.62)" },
    ][Math.max(0, Math.min(3, Number(zone.selectedLevel) || 0))];
    if (zone.status === "closed") {
      return {
        fill: palette.fill.replace("0.13", "0.06").replace("0.12", "0.06").replace("0.11", "0.05").replace("0.08", "0.05"),
        stroke: palette.stroke.replace("0.72", "0.38").replace("0.7", "0.38").replace("0.68", "0.36").replace("0.62", "0.34"),
        lineWidth: 1.0,
      };
    }
    return {
      fill: palette.fill,
      stroke: palette.stroke,
      lineWidth: zone.status === "active" ? 1.45 : 1.15,
    };
  }

  function zoneTooltipHtml(zone) {
    return [
      "<div class=\"zones-tip-zone\">",
      "<strong>" + String(zone.symbol) + " L" + String(zone.selectedLevel) + " " + String(zone.status || "").toUpperCase() + "</strong><br>",
      "pattern " + String(zone.patternType || "") + " | start " + String(zone.startTickId ?? "") + " | end " + String(zone.endTickId ?? "") + "<br>",
      "current " + Number(zone.zoneLow).toFixed(2) + " - " + Number(zone.zoneHigh).toFixed(2) + " | h " + Number(zone.zoneHeight).toFixed(2) + "<br>",
      "inside " + String(zone.tickCountInside ?? "") + " | duration " + String(zone.durationInsideLabel || zone.durationInsideMs || ""),
      "</div>",
    ].join("");
  }

  function buildZoneOverlayGraphics() {
    const chart = state.chart;
    if (!chart || !displayUsesZones(currentConfig().display) || !state.zoneRows.length) {
      return [];
    }
    const grid = chart.getModel()?.getComponent("grid", 0);
    const rect = grid?.coordinateSystem?.getRect?.();
    if (!rect) {
      return [];
    }
    const children = [];
    state.zoneRows.forEach((zone, index) => {
      const startTs = Number(zone.startTimestampMs);
      const endTs = Number(zone.rightTimestampMs ?? zone.endTimestampMs ?? zone.startTimestampMs);
      const leftPoint = chart.convertToPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [startTs, zone.zoneLow]);
      const rightPoint = chart.convertToPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [endTs, zone.zoneLow]);
      const topPoint = chart.convertToPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [startTs, zone.zoneHigh]);
      const bottomPoint = chart.convertToPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [startTs, zone.zoneLow]);
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
      const style = zoneStyle(zone);
      children.push({
        id: "live-zone-" + String(zone.id || index),
        type: "rect",
        silent: true,
        z: 2,
        shape: {
          x: left,
          y: top,
          width: Math.max(2, right - left),
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
    return children;
  }

  function renderZoneOverlay() {
    if (!state.chart) {
      return;
    }
    state.chart.setOption({
      graphic: [{
        id: "live-zone-overlay",
        type: "group",
        silent: true,
        z: 2,
        children: buildZoneOverlayGraphics(),
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

  function buildChartSeries(config) {
    const series = [];
    if (displayUsesTicks(config.display)) {
      series.push({
        id: "raw-price",
        name: SERIES_CONFIG[config.series].label,
        type: "line",
        includeInYAutoscale: true,
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
            includeInYAutoscale: true,
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
    const nextSeries = buildChartSeries(currentConfig());
    const nextYAxisBounds = visibleYBounds(nextViewport, nextSeries);
    state.renderedSeries = nextSeries;

    state.applyingViewport = true;
    chart.setOption({
      series: nextSeries,
      yAxis: nextYAxisBounds ? {
        min: nextYAxisBounds.min,
        max: nextYAxisBounds.max,
      } : {},
      dataZoom: buildDataZoomState(nextViewport, datasetBounds),
    }, { replaceMerge: ["series"], lazyUpdate: true });
    state.lastDatasetBounds = datasetBounds;

    window.requestAnimationFrame(() => {
      state.applyingViewport = false;
      state.viewport = captureViewportFromChart(datasetBounds) || nextViewport;
      queueVisibleYAxisUpdate(state.viewport);
      queueZoneOverlayRender();
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
    const windowSize = currentLoadedWindow();
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

  function replaceZoneRows(rows) {
    const byId = new Map();
    (Array.isArray(rows) ? rows : []).forEach((row) => {
      if (!row || row.id == null) {
        return;
      }
      byId.set(row.id, row);
    });
    state.zoneRows = Array.from(byId.values()).sort((left, right) => {
      const leftStart = Number(left?.startTimestampMs || 0);
      const rightStart = Number(right?.startTimestampMs || 0);
      if (leftStart !== rightStart) {
        return leftStart - rightStart;
      }
      return Number(left?.id || 0) - Number(right?.id || 0);
    });
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
      showTicks: config.showTicks ? "1" : "0",
      showZigs: config.showZigs ? "1" : "0",
      showZones: config.showZones ? "1" : "0",
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
      window: String(currentLoadedWindow(config)),
      display: config.display,
      showTicks: config.showTicks ? "1" : "0",
      showZigs: config.showZigs ? "1" : "0",
      showZones: config.showZones ? "1" : "0",
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
      display: config.display,
      showTicks: config.showTicks ? "1" : "0",
      showZigs: config.showZigs ? "1" : "0",
      showZones: config.showZones ? "1" : "0",
    }).toString();
  }

  async function loadBootstrap(resetView) {
    const config = currentConfig();
    const startId = config.mode === "review" ? await resolveReviewStartId(config) : null;
    const preservedViewport = resetView ? null : (captureViewportFromChart(getDatasetBounds()) || state.viewport);
    const payload = await fetchJson(bootstrapUrl(config, startId));
    state.loadedWindow = Number(payload.window) || config.window;
    replaceRows(payload.rows || []);
    replaceZigRows(payload.zigRows || []);
    replaceZoneRows(payload.zoneRows || []);
    applyRangePayload(payload);
    state.reviewEndId = payload.reviewEndId || null;
    state.hasMoreLeft = Boolean(payload.hasMoreLeft);
    state.lastMetrics = payload.metrics || null;
    state.lastDatasetBounds = null;
    state.viewport = preservedViewport;
    renderMeta();
    renderPerf();
    renderChart({ resetView: Boolean(resetView) });

    const message = displayUsesTicks(config.display)
      ? "Loaded " + state.rows.length + " tick(s)."
      : "Loaded tick range " + state.rangeFirstId + "-" + state.rangeLastId + ".";
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
      window: String(currentLoadedWindow(config)),
      display: config.display,
      showTicks: config.showTicks ? "1" : "0",
      showZigs: config.showZigs ? "1" : "0",
      showZones: config.showZones ? "1" : "0",
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
      if (displayUsesZones(config.display)) {
        replaceZoneRows(payload.zoneRows || []);
      }
      if (!displayUsesTicks(config.display) && payload.lastId != null) {
        state.rangeLastId = payload.lastId;
      }
      renderMeta();
      renderPerf();
      if (tickAppended || zigChanged || displayUsesZones(config.display)) {
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
    if (displayUsesZones(config.display)) {
      replaceZoneRows(payload.zoneRows || []);
    }
    if (!displayUsesTicks(config.display) && payload.lastId != null) {
      state.rangeLastId = payload.lastId;
    }
    renderMeta();
    renderPerf();
    if (tickAppended || zigChanged || displayUsesZones(config.display)) {
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
    const config = currentConfig();
    const remaining = Math.max(0, maxLoadedWindow(config.display) - currentLoadedWindow(config));
    if (remaining <= 0) {
      return 0;
    }
    return Math.max(1, Math.min(config.window, remaining));
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
    const previousFirstId = state.rangeFirstId;
    const previousLoadedWindow = currentLoadedWindow(config);
    const batchSize = historyBatchSize();
    if (!batchSize) {
      status("Loaded history is already at the current chart cap.", false);
      await resumeRunIfNeeded();
      return;
    }
    const preservedViewport = captureViewportFromChart(getDatasetBounds()) || state.viewport;
    const payload = await fetchJson(previousUrl(config, batchSize));
    state.lastMetrics = payload.metrics || null;
    const prepended = displayUsesTicks(config.display) ? dedupePrepend(payload.rows || []) : 0;
    const didExpandLeft = payload.firstId != null && previousFirstId != null && payload.firstId < previousFirstId;
    state.loadedWindow = prepended || didExpandLeft
      ? previousLoadedWindow + batchSize
      : previousLoadedWindow;
    if (!displayUsesTicks(config.display)) {
      state.rangeFirstId = payload.firstId;
      state.rangeLastId = payload.lastId;
    } else if (payload.firstId != null) {
      state.rangeFirstId = payload.firstId;
    }
    if (displayUsesZig(config.display)) {
      replaceZigRows(payload.zigRows || []);
    }
    if (displayUsesZones(config.display)) {
      replaceZoneRows(payload.zoneRows || []);
    }
    state.hasMoreLeft = Boolean(payload.hasMoreLeft);
    state.viewport = preservedViewport;
    renderMeta();
    renderPerf();
    if (prepended || didExpandLeft) {
      renderChart({ shiftWithRun: false });
      status(
        displayUsesTicks(config.display)
          ? prepended + " older tick(s) were added off-screen to the left."
          : "Older zig history was added off-screen to the left.",
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

  [elements.showTicks, elements.showZigs, elements.showZones].forEach((control) => {
    control.addEventListener("change", function () {
      currentConfig();
      updateWindowConstraints();
      updateSeriesAvailability();
      writeQuery();
      loadAll(false).catch((error) => {
        status(error.message || "Display refresh failed.", true);
      });
      status("Display layers updated.", false);
    });
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
