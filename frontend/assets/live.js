(function () {
  const DEFAULTS = {
    mode: "live",
    run: "run",
    id: "",
    reviewStart: "",
    reviewSpeed: 1,
    sidebarCollapsed: false,
    controlSectionCollapsed: false,
    zigSectionCollapsed: false,
    ottSectionCollapsed: false,
    envelopeSectionCollapsed: false,
    window: 2000,
    series: ["mid"],
    zigMicro: false,
    zigMed: true,
    zigMaxi: true,
    zigMacro: true,
    zigViewMode: "normal",
    ottEnabled: true,
    ottSupport: true,
    ottMarkers: true,
    ottTrades: true,
    ottHighlight: true,
    ottSource: "mid",
    ottSignalMode: "support",
    ottMaType: "VAR",
    ottLength: 2,
    ottPercent: 1.4,
    ottRangePreset: "lastweek",
    envelopeEnabled: true,
    envelopeSource: "mid",
    envelopeLength: 500,
    envelopeBandwidth: 8,
    envelopeMult: 3,
  };
  const SYDNEY_TIMEZONE = "Australia/Sydney";
  const REVIEW_SPEEDS = [0.5, 1, 2, 3, 5];
  const REVIEW_PREFETCH_FLOOR = 250;
  const REVIEW_PREFETCH_CEILING = 1000;
  const REVIEW_PREFETCH_THRESHOLD = 120;
  const REVIEW_PREFETCH_RATIO = 0.65;
  const REVIEW_PREFETCH_MIN_PLAYBACK_MS = 12000;
  const REVIEW_ZIG_RETRY_DELAY_MS = 250;
  const REVIEW_OTT_RETRY_DELAY_MS = 250;
  const REVIEW_ENVELOPE_RETRY_DELAY_MS = 250;
  const BACKTEST_OVERLAY_TIMEOUT_MS = 8000;
  const BACKTEST_RUN_TIMEOUT_MS = 45000;
  const STRUCTURE_VIEW_TARGET_POINTS = 360;
  const CHART_RESIZE_RETRY_LIMIT = 6;
  const CHART_RESIZE_RETRY_DELAY_MS = 40;
  const SIGNAL_MARKER_MIN_OFFSET = 0.18;
  const SIGNAL_MARKER_MAX_OFFSET = 1.25;

  const SERIES_CONFIG = {
    ask: { label: "Ask", field: "ask", color: "#ffb35c", width: 1.35 },
    bid: { label: "Bid", field: "bid", color: "#7ef0c7", width: 1.35 },
    mid: { label: "Mid", field: "mid", color: "#6dd8ff", width: 2.0 },
  };

  const ZIG_LEVEL_CONFIG = {
    micro: { label: "Micro Zig", color: "#8fe6ff", width: 1.05, opacity: 0.4 },
    med: { label: "Medium Zig", color: "#ffd166", width: 1.6, opacity: 0.72 },
    maxi: { label: "Maxi Zig", color: "#ff8c69", width: 2.2, opacity: 0.84 },
    macro: { label: "Macro Zig", color: "#f8fafc", width: 2.9, opacity: 0.96 },
  };

  const state = {
    rows: [],
    currentMode: DEFAULTS.mode,
    currentRun: DEFAULTS.run,
    visibleSpanMs: null,
    visibleWindow: null,
    activeSeries: { ask: false, bid: false, mid: true },
    source: null,
    chart: null,
    chartListenersBound: false,
    chartResizeFrame: 0,
    chartResizeTailFrame: 0,
    chartResizeTimer: 0,
    chartResizeObserver: null,
    chartRenderFrame: 0,
    pendingRenderOptions: null,
    zigRows: {
      micro: new Map(),
      med: new Map(),
      maxi: new Map(),
      macro: new Map(),
    },
    zigStatusPayload: null,
    zigStatePayload: null,
    ottRows: new Map(),
    ottTrades: [],
    ottRun: null,
    ottLastId: 0,
    ottStatusPayload: null,
    ottOverlayPayload: null,
    envelopeRows: new Map(),
    envelopeLastId: 0,
    envelopeStatusPayload: null,
    backtest: {
      overlayRequestToken: 0,
      overlayController: null,
      runController: null,
    },
    loadStatus: {
      chart: { text: "Chart: waiting.", severity: "info" },
      zig: { text: "Zig: waiting.", severity: "info" },
      ott: { text: "OTT: waiting.", severity: "info" },
      envelope: { text: "Envelope: waiting.", severity: "info" },
      backtest: { text: "Backtest: idle.", severity: "info" },
    },
    review: {
      bufferRows: [],
      visibleCount: 0,
      lastBufferedId: 0,
      playbackSpeed: DEFAULTS.reviewSpeed,
      exhausted: false,
      fetchPromise: null,
      zigFetchPromise: null,
      ottFetchPromise: null,
      envelopeFetchPromise: null,
      rafId: 0,
      anchorVisibleCount: 0,
      anchorTimestampMs: 0,
      anchorPerfMs: 0,
      reachedEndAnnounced: false,
      resolvedStartId: null,
      resolvedStartTimestamp: null,
      sessionEndId: null,
      sessionEndTimestamp: null,
      fetchingTicks: false,
      fetchingZig: false,
      fetchingOtt: false,
      fetchingEnvelope: false,
      trueEndReached: false,
      requestedVisibleCount: 0,
      waitingFor: null,
      zigRequestedEndId: 0,
      lastZigRequestAt: 0,
      ottRequestedEndId: 0,
      lastOttRequestAt: 0,
      envelopeRequestedEndId: 0,
      lastEnvelopeRequestAt: 0,
    },
    ui: {
      sidebarCollapsed: DEFAULTS.sidebarCollapsed,
      sections: {
        control: DEFAULTS.controlSectionCollapsed,
        zig: DEFAULTS.zigSectionCollapsed,
        ott: DEFAULTS.ottSectionCollapsed,
        envelope: DEFAULTS.envelopeSectionCollapsed,
      },
    },
  };

  const elements = {
    liveWorkspace: document.getElementById("liveWorkspace"),
    liveSidebar: document.getElementById("liveSidebar"),
    sidebarToggle: document.getElementById("sidebarToggle"),
    modeToggle: document.getElementById("modeToggle"),
    runToggle: document.getElementById("runToggle"),
    tickId: document.getElementById("tickId"),
    reviewStart: document.getElementById("reviewStart"),
    reviewYesterdayButton: document.getElementById("reviewYesterdayButton"),
    reviewSpeedToggle: document.getElementById("reviewSpeedToggle"),
    windowSize: document.getElementById("windowSize"),
    applyButton: document.getElementById("applyButton"),
    statusLine: document.getElementById("statusLine"),
    liveMeta: document.getElementById("liveMeta"),
    chartHost: document.getElementById("liveChart"),
    chartPanel: document.getElementById("chartPanel"),
    seriesSelector: document.getElementById("seriesSelector"),
    zigMicroToggle: document.getElementById("zigMicroToggle"),
    zigMedToggle: document.getElementById("zigMedToggle"),
    zigMaxiToggle: document.getElementById("zigMaxiToggle"),
    zigMacroToggle: document.getElementById("zigMacroToggle"),
    zigViewToggle: document.getElementById("zigViewToggle"),
    ottToggle: document.getElementById("ottToggle"),
    ottSupportToggle: document.getElementById("ottSupportToggle"),
    ottMarkersToggle: document.getElementById("ottMarkersToggle"),
    ottTradesToggle: document.getElementById("ottTradesToggle"),
    ottHighlightToggle: document.getElementById("ottHighlightToggle"),
    ottSource: document.getElementById("ottSource"),
    ottSignalMode: document.getElementById("ottSignalMode"),
    ottMaType: document.getElementById("ottMaType"),
    ottLength: document.getElementById("ottLength"),
    ottPercent: document.getElementById("ottPercent"),
    ottRangePreset: document.getElementById("ottRangePreset"),
    runOttBacktestButton: document.getElementById("runOttBacktestButton"),
    envelopeToggle: document.getElementById("envelopeToggle"),
    envelopeSource: document.getElementById("envelopeSource"),
    envelopeLength: document.getElementById("envelopeLength"),
    envelopeBandwidth: document.getElementById("envelopeBandwidth"),
    envelopeMult: document.getElementById("envelopeMult"),
    controlSectionBody: document.getElementById("controlSectionBody"),
    zigSectionBody: document.getElementById("zigSectionBody"),
    ottSectionBody: document.getElementById("ottSectionBody"),
    envelopeSectionBody: document.getElementById("envelopeSectionBody"),
    controlSectionToggle: document.getElementById("controlSectionToggle"),
    zigSectionToggle: document.getElementById("zigSectionToggle"),
    ottSectionToggle: document.getElementById("ottSectionToggle"),
    envelopeSectionToggle: document.getElementById("envelopeSectionToggle"),
  };

  const SIDEBAR_SECTIONS = {
    control: { body: elements.controlSectionBody, toggle: elements.controlSectionToggle },
    zig: { body: elements.zigSectionBody, toggle: elements.zigSectionToggle },
    envelope: { body: elements.envelopeSectionBody, toggle: elements.envelopeSectionToggle },
    ott: { body: elements.ottSectionBody, toggle: elements.ottSectionToggle },
  };

  function currentChartHost() {
    return elements.chartHost && elements.chartHost.isConnected ? elements.chartHost : null;
  }

  function currentChartDom() {
    return state.chart && typeof state.chart.getDom === "function" ? state.chart.getDom() : null;
  }

  function chartDomMatchesHost(host) {
    const dom = currentChartDom();
    return Boolean(host && dom && dom === host && dom.isConnected);
  }

  function chartHostHasSize(host) {
    return Boolean(host && host.isConnected && host.offsetWidth && host.offsetHeight);
  }

  function mergeRenderOptions(base, extra) {
    const merged = {
      preserveCurrentZoom: Boolean(base && base.preserveCurrentZoom) || Boolean(extra && extra.preserveCurrentZoom),
      resetWindow: Boolean(base && base.resetWindow) || Boolean(extra && extra.resetWindow),
    };
    if (merged.resetWindow) {
      merged.preserveCurrentZoom = false;
    }
    return merged;
  }

  function scheduleChartRender(options) {
    state.pendingRenderOptions = mergeRenderOptions(state.pendingRenderOptions, options);
    if (state.chartRenderFrame) {
      return;
    }
    state.chartRenderFrame = requestAnimationFrame(() => {
      state.chartRenderFrame = 0;
      const pendingOptions = state.pendingRenderOptions;
      state.pendingRenderOptions = null;
      renderChart(pendingOptions, { fromScheduler: true });
    });
  }

  function isOffsetWidthError(error) {
    return String(error && error.message || "").includes("offsetWidth");
  }

  function disposeChart() {
    if (state.chartResizeFrame) {
      cancelAnimationFrame(state.chartResizeFrame);
      state.chartResizeFrame = 0;
    }
    if (state.chartResizeTailFrame) {
      cancelAnimationFrame(state.chartResizeTailFrame);
      state.chartResizeTailFrame = 0;
    }
    if (state.chartResizeTimer) {
      window.clearTimeout(state.chartResizeTimer);
      state.chartResizeTimer = 0;
    }
    if (state.chartRenderFrame) {
      cancelAnimationFrame(state.chartRenderFrame);
      state.chartRenderFrame = 0;
    }
    state.pendingRenderOptions = null;
    if (!state.chart) {
      return;
    }
    try {
      state.chart.dispose();
    } catch (error) {
      // Ignore disposal failures while recovering a stale chart instance.
    }
    state.chart = null;
  }

  function parseSeries(rawValue) {
    if (!rawValue) {
      return DEFAULTS.series.slice();
    }
    const selected = rawValue.split(",").map((item) => item.trim()).filter((item) => SERIES_CONFIG[item]);
    return selected.length ? Array.from(new Set(selected)) : DEFAULTS.series.slice();
  }

  function parseBoolean(value, fallback) {
    if (value === null) {
      return fallback;
    }
    return value !== "0" && value !== "false" && value !== "off";
  }

  function parseCollapsed(value, fallback) {
    if (value === null) {
      return fallback;
    }
    return value === "1" || value === "true" || value === "collapsed";
  }

  function sanitizeReviewSpeed(rawValue) {
    const value = Number.parseFloat(rawValue);
    return REVIEW_SPEEDS.includes(value) ? value : DEFAULTS.reviewSpeed;
  }

  function parseQuery() {
    const params = new URLSearchParams(window.location.search);
    const mode = params.get("mode") === "review" ? "review" : DEFAULTS.mode;
    const run = params.get("run") === "stop" ? "stop" : DEFAULTS.run;
    const id = params.get("id") || DEFAULTS.id;
    const reviewStart = params.get("reviewStart") || DEFAULTS.reviewStart;
    const windowSize = Number.parseInt(params.get("window"), 10);
    const ottLength = Number.parseInt(params.get("ottLength"), 10);
    const ottPercent = Number.parseFloat(params.get("ottPercent"));
    const envelopeLength = Number.parseInt(params.get("envelopeLength"), 10);
    const envelopeBandwidth = Number.parseFloat(params.get("envelopeBandwidth"));
    const envelopeMult = Number.parseFloat(params.get("envelopeMult"));
    const controlSectionParam = params.has("controlSection") ? params.get("controlSection") : params.get("primaryBar");
    const zigSectionParam = params.get("zigSection");
    const ottSectionParam = params.has("ottSection") ? params.get("ottSection") : params.get("ottBar");
    const envelopeSectionParam = params.has("envelopeSection") ? params.get("envelopeSection") : params.get("envelopeBar");
    return {
      mode,
      run,
      id,
      reviewStart,
      reviewSpeed: sanitizeReviewSpeed(params.get("reviewSpeed")),
      sidebarCollapsed: parseCollapsed(params.get("sidebar"), DEFAULTS.sidebarCollapsed),
      controlSectionCollapsed: parseCollapsed(controlSectionParam, DEFAULTS.controlSectionCollapsed),
      zigSectionCollapsed: parseCollapsed(zigSectionParam, DEFAULTS.zigSectionCollapsed),
      ottSectionCollapsed: parseCollapsed(ottSectionParam, DEFAULTS.ottSectionCollapsed),
      envelopeSectionCollapsed: parseCollapsed(envelopeSectionParam, DEFAULTS.envelopeSectionCollapsed),
      window: Number.isFinite(windowSize) && windowSize > 0 ? windowSize : DEFAULTS.window,
      series: parseSeries(params.get("series")),
      zigMicro: parseBoolean(params.get("zigMicro"), DEFAULTS.zigMicro),
      zigMed: parseBoolean(params.get("zigMed"), DEFAULTS.zigMed),
      zigMaxi: parseBoolean(params.get("zigMaxi"), DEFAULTS.zigMaxi),
      zigMacro: parseBoolean(params.get("zigMacro"), DEFAULTS.zigMacro),
      zigViewMode: ["normal", "structure", "zigonly"].includes(params.get("zigView")) ? params.get("zigView") : DEFAULTS.zigViewMode,
      ottEnabled: parseBoolean(params.get("ott"), DEFAULTS.ottEnabled),
      ottSupport: parseBoolean(params.get("ottSupport"), DEFAULTS.ottSupport),
      ottMarkers: parseBoolean(params.get("ottMarkers"), DEFAULTS.ottMarkers),
      ottTrades: parseBoolean(params.get("ottTrades"), DEFAULTS.ottTrades),
      ottHighlight: parseBoolean(params.get("ottHighlight"), DEFAULTS.ottHighlight),
      ottSource: ["ask", "bid", "mid"].includes(params.get("ottSource")) ? params.get("ottSource") : DEFAULTS.ottSource,
      ottSignalMode: ["support", "price", "color"].includes(params.get("ottSignalMode")) ? params.get("ottSignalMode") : DEFAULTS.ottSignalMode,
      ottMaType: ["SMA", "EMA", "WMA", "TMA", "VAR", "WWMA", "ZLEMA", "TSF"].includes(params.get("ottMaType")) ? params.get("ottMaType") : DEFAULTS.ottMaType,
      ottLength: Number.isFinite(ottLength) && ottLength > 0 ? ottLength : DEFAULTS.ottLength,
      ottPercent: Number.isFinite(ottPercent) && ottPercent >= 0 ? ottPercent : DEFAULTS.ottPercent,
      ottRangePreset: params.get("ottRangePreset") === "lastweek" ? "lastweek" : DEFAULTS.ottRangePreset,
      envelopeEnabled: parseBoolean(params.get("envelope"), DEFAULTS.envelopeEnabled),
      envelopeSource: ["ask", "bid", "mid"].includes(params.get("envelopeSource")) ? params.get("envelopeSource") : DEFAULTS.envelopeSource,
      envelopeLength: Number.isFinite(envelopeLength) && envelopeLength > 0 ? envelopeLength : DEFAULTS.envelopeLength,
      envelopeBandwidth: Number.isFinite(envelopeBandwidth) && envelopeBandwidth > 0 ? envelopeBandwidth : DEFAULTS.envelopeBandwidth,
      envelopeMult: Number.isFinite(envelopeMult) && envelopeMult >= 0 ? envelopeMult : DEFAULTS.envelopeMult,
    };
  }

  function getActiveSeriesKeys() {
    return Object.keys(SERIES_CONFIG).filter((key) => state.activeSeries[key]);
  }

  function getPrimarySeriesKey() {
    if (state.activeSeries.mid) {
      return "mid";
    }
    return getActiveSeriesKeys()[0] || "mid";
  }

  function setSegment(container, value) {
    container.querySelectorAll("button").forEach((button) => {
      button.classList.toggle("active", button.dataset.value === value);
    });
  }

  function syncSeriesButtons() {
    elements.seriesSelector.querySelectorAll("button").forEach((button) => {
      button.classList.toggle("active", Boolean(state.activeSeries[button.dataset.series]));
    });
  }

  function syncReviewSpeedButtons() {
    elements.reviewSpeedToggle.querySelectorAll("button").forEach((button) => {
      button.classList.toggle("active", Number.parseFloat(button.dataset.value) === state.review.playbackSpeed);
    });
  }

  function setSectionCollapsed(target, collapsed) {
    if (!target) {
      return;
    }
    target.classList.toggle("is-collapsed", collapsed);
  }

  function syncSidebarState() {
    const sidebarCollapsed = Boolean(state.ui.sidebarCollapsed);
    if (elements.liveWorkspace) {
      elements.liveWorkspace.classList.toggle("is-sidebar-collapsed", sidebarCollapsed);
    }
    if (elements.liveSidebar) {
      elements.liveSidebar.setAttribute("aria-hidden", String(sidebarCollapsed));
    }
    if (elements.sidebarToggle) {
      elements.sidebarToggle.setAttribute("aria-expanded", String(!sidebarCollapsed));
    }

    Object.keys(SIDEBAR_SECTIONS).forEach((sectionKey) => {
      const section = SIDEBAR_SECTIONS[sectionKey];
      const collapsed = Boolean(state.ui.sections[sectionKey]);
      setSectionCollapsed(section.body, collapsed);
      if (!section.toggle) {
        return;
      }
      section.toggle.setAttribute("aria-expanded", String(!collapsed));
      const stateLabel = section.toggle.querySelector(".live-section-state");
      if (stateLabel) {
        stateLabel.textContent = collapsed ? "Expand" : "Collapse";
      }
    });
  }

  function updateReviewControlState() {
    const inReview = state.currentMode === "review";
    elements.reviewStart.disabled = !inReview;
    elements.reviewYesterdayButton.disabled = !inReview;
    elements.reviewSpeedToggle.querySelectorAll("button").forEach((button) => {
      button.disabled = !inReview;
    });
  }

  function syncControls(config) {
    state.currentMode = config.mode;
    state.currentRun = config.run;
    state.review.playbackSpeed = config.reviewSpeed;
    state.ui.sidebarCollapsed = Boolean(config.sidebarCollapsed);
    state.ui.sections.control = Boolean(config.controlSectionCollapsed);
    state.ui.sections.zig = Boolean(config.zigSectionCollapsed);
    state.ui.sections.ott = Boolean(config.ottSectionCollapsed);
    state.ui.sections.envelope = Boolean(config.envelopeSectionCollapsed);
    state.activeSeries = { ask: false, bid: false, mid: false };
    config.series.forEach((seriesKey) => {
      state.activeSeries[seriesKey] = true;
    });
    elements.tickId.value = config.id || "";
    elements.reviewStart.value = config.reviewStart || "";
    elements.windowSize.value = String(config.window);
    elements.zigMicroToggle.checked = Boolean(config.zigMicro);
    elements.zigMedToggle.checked = Boolean(config.zigMed);
    elements.zigMaxiToggle.checked = Boolean(config.zigMaxi);
    elements.zigMacroToggle.checked = Boolean(config.zigMacro);
    setSegment(elements.zigViewToggle, config.zigViewMode);
    elements.ottSupportToggle.checked = Boolean(config.ottSupport);
    elements.ottMarkersToggle.checked = Boolean(config.ottMarkers);
    elements.ottTradesToggle.checked = Boolean(config.ottTrades);
    elements.ottHighlightToggle.checked = Boolean(config.ottHighlight);
    elements.ottSource.value = config.ottSource;
    elements.ottSignalMode.value = config.ottSignalMode;
    elements.ottMaType.value = config.ottMaType;
    elements.ottLength.value = String(config.ottLength);
    elements.ottPercent.value = String(config.ottPercent);
    elements.ottRangePreset.value = config.ottRangePreset;
    elements.envelopeSource.value = config.envelopeSource;
    elements.envelopeLength.value = String(config.envelopeLength);
    elements.envelopeBandwidth.value = String(config.envelopeBandwidth);
    elements.envelopeMult.value = String(config.envelopeMult);
    setSegment(elements.modeToggle, config.mode);
    setSegment(elements.runToggle, config.run);
    setSegment(elements.ottToggle, config.ottEnabled ? "on" : "off");
    setSegment(elements.envelopeToggle, config.envelopeEnabled ? "on" : "off");
    syncSeriesButtons();
    syncReviewSpeedButtons();
    syncSidebarState();
    updateReviewControlState();
  }

  function currentConfig() {
    return {
      mode: state.currentMode,
      run: state.currentRun,
      id: elements.tickId.value.trim(),
      reviewStart: elements.reviewStart.value.trim(),
      reviewSpeed: state.review.playbackSpeed,
      sidebarCollapsed: state.ui.sidebarCollapsed,
      controlSectionCollapsed: state.ui.sections.control,
      zigSectionCollapsed: state.ui.sections.zig,
      ottSectionCollapsed: state.ui.sections.ott,
      envelopeSectionCollapsed: state.ui.sections.envelope,
      window: Math.max(1, Math.min(10000, Number.parseInt(elements.windowSize.value, 10) || DEFAULTS.window)),
      series: getActiveSeriesKeys(),
      zigMicro: elements.zigMicroToggle.checked,
      zigMed: elements.zigMedToggle.checked,
      zigMaxi: elements.zigMaxiToggle.checked,
      zigMacro: elements.zigMacroToggle.checked,
      zigViewMode: elements.zigViewToggle.querySelector("button.active")?.dataset.value || DEFAULTS.zigViewMode,
      ottEnabled: elements.ottToggle.querySelector("button.active")?.dataset.value !== "off",
      ottSupport: elements.ottSupportToggle.checked,
      ottMarkers: elements.ottMarkersToggle.checked,
      ottTrades: elements.ottTradesToggle.checked,
      ottHighlight: elements.ottHighlightToggle.checked,
      ottSource: elements.ottSource.value,
      ottSignalMode: elements.ottSignalMode.value,
      ottMaType: elements.ottMaType.value,
      ottLength: Math.max(1, Number.parseInt(elements.ottLength.value, 10) || DEFAULTS.ottLength),
      ottPercent: Math.max(0, Number.parseFloat(elements.ottPercent.value) || DEFAULTS.ottPercent),
      ottRangePreset: elements.ottRangePreset.value || DEFAULTS.ottRangePreset,
      envelopeEnabled: elements.envelopeToggle.querySelector("button.active")?.dataset.value !== "off",
      envelopeSource: elements.envelopeSource.value,
      envelopeLength: Math.max(1, Number.parseInt(elements.envelopeLength.value, 10) || DEFAULTS.envelopeLength),
      envelopeBandwidth: Math.max(0.1, Number.parseFloat(elements.envelopeBandwidth.value) || DEFAULTS.envelopeBandwidth),
      envelopeMult: Math.max(0, Number.parseFloat(elements.envelopeMult.value) || DEFAULTS.envelopeMult),
    };
  }

  function writeQuery(config) {
    const params = new URLSearchParams();
    params.set("mode", config.mode);
    params.set("run", config.run);
    params.set("window", String(config.window));
    params.set("reviewSpeed", String(config.reviewSpeed));
    params.set("sidebar", config.sidebarCollapsed ? "1" : "0");
    params.set("controlSection", config.controlSectionCollapsed ? "1" : "0");
    params.set("zigSection", config.zigSectionCollapsed ? "1" : "0");
    params.set("ottSection", config.ottSectionCollapsed ? "1" : "0");
    params.set("envelopeSection", config.envelopeSectionCollapsed ? "1" : "0");
    params.set("series", config.series.join(","));
    params.set("zigMicro", config.zigMicro ? "1" : "0");
    params.set("zigMed", config.zigMed ? "1" : "0");
    params.set("zigMaxi", config.zigMaxi ? "1" : "0");
    params.set("zigMacro", config.zigMacro ? "1" : "0");
    params.set("zigView", config.zigViewMode);
    params.set("ott", config.ottEnabled ? "1" : "0");
    params.set("ottSupport", config.ottSupport ? "1" : "0");
    params.set("ottMarkers", config.ottMarkers ? "1" : "0");
    params.set("ottTrades", config.ottTrades ? "1" : "0");
    params.set("ottHighlight", config.ottHighlight ? "1" : "0");
    params.set("ottSource", config.ottSource);
    params.set("ottSignalMode", config.ottSignalMode);
    params.set("ottMaType", config.ottMaType);
    params.set("ottLength", String(config.ottLength));
    params.set("ottPercent", String(config.ottPercent));
    params.set("ottRangePreset", config.ottRangePreset);
    params.set("envelope", config.envelopeEnabled ? "1" : "0");
    params.set("envelopeSource", config.envelopeSource);
    params.set("envelopeLength", String(config.envelopeLength));
    params.set("envelopeBandwidth", String(config.envelopeBandwidth));
    params.set("envelopeMult", String(config.envelopeMult));
    if (config.id) {
      params.set("id", config.id);
    }
    if (config.reviewStart) {
      params.set("reviewStart", config.reviewStart);
    }
    history.replaceState(null, "", "/live?" + params.toString());
  }

  function status(text, isError) {
    elements.statusLine.textContent = text;
    elements.statusLine.classList.toggle("error", Boolean(isError));
  }

  function setLoadStatus(key, text, severity, options) {
    state.loadStatus[key] = {
      text,
      severity: severity || "info",
    };
    if (!options || !options.silent) {
      renderLoadStatus();
    }
  }

  function renderLoadStatus() {
    const parts = ["chart", "zig", "ott", "envelope", "backtest"]
      .map((key) => state.loadStatus[key])
      .filter((entry) => entry && entry.text);
    elements.statusLine.textContent = parts.map((entry) => entry.text).join(" | ") || "Ready.";
    elements.statusLine.classList.toggle("error", parts.some((entry) => entry.severity === "error"));
  }

  function initializeLoadStatus(config) {
    setLoadStatus("chart", "Chart: loading...", "info", { silent: true });
    setLoadStatus("zig", shouldLoadZig(config) ? "Zig: loading..." : "Zig: off.", "info", { silent: true });
    setLoadStatus("ott", shouldLoadOtt(config) ? "OTT: loading..." : "OTT: off.", "info", { silent: true });
    setLoadStatus("envelope", shouldLoadEnvelope(config) ? "Envelope: loading..." : "Envelope: off.", "info", { silent: true });
    if (config.mode === "review") {
      setLoadStatus(
        "backtest",
        config.ottTrades ? "Backtest: checking cache..." : "Backtest: off.",
        "info",
        { silent: true }
      );
    } else {
      setLoadStatus("backtest", "Backtest: n/a.", "info", { silent: true });
    }
    renderLoadStatus();
  }

  function updateZigLoadStatus(payload) {
    if (!payload) {
      setLoadStatus("zig", "Zig: off.", "info");
      return;
    }
    if (payload.status && payload.status !== "ok") {
      setLoadStatus(
        "zig",
        "Zig: " + (payload.message || payload.status + "."),
        isOverlayWarningStatus(payload.status) ? "warn" : "error"
      );
      return;
    }
    const config = currentConfig();
    const parts = enabledZigLevels(config).map((level) => {
      const levelPayload = payload.levels && payload.levels[level] ? payload.levels[level] : null;
      return ZIG_LEVEL_CONFIG[level].label.replace(" Zig", "") + " " + Number(levelPayload && levelPayload.rowCount || 0);
    });
    if (config.mode === "review" && payload.range && payload.range.endId != null) {
      setLoadStatus("zig", "Zig: " + parts.join(" | ") + " thru tick " + payload.range.endId + ".", "info");
      return;
    }
    setLoadStatus("zig", parts.length ? "Zig: " + parts.join(" | ") + "." : "Zig: off.", "info");
  }

  function updateOttLoadStatus(payload) {
    if (!payload) {
      setLoadStatus("ott", "OTT: off.", "info");
      return;
    }
    if (payload.status && payload.status !== "ok") {
      setLoadStatus(
        "ott",
        "OTT: " + (payload.message || payload.status + "."),
        isOverlayWarningStatus(payload.status) ? "warn" : "error"
      );
      return;
    }
    const config = currentConfig();
    if (config.mode === "review") {
      const coverage = reviewOttCoverage();
      const suffix = coverage.contiguousEndId != null ? " thru tick " + coverage.contiguousEndId : "";
      setLoadStatus(
        "ott",
        "OTT: synced " + coverage.contiguousAvailableCount + "/" + state.review.bufferRows.length + " buffered row(s)" + suffix + ".",
        coverage.missingCount ? "warn" : "info"
      );
      return;
    }
    const availableCount = payload.availableRowCount != null ? payload.availableRowCount : payload.rowCount;
    setLoadStatus("ott", "OTT: loaded " + Number(availableCount || 0) + " row(s).", "info");
  }

  function updateEnvelopeLoadStatus(payload) {
    if (!payload) {
      setLoadStatus("envelope", "Envelope: off.", "info");
      return;
    }
    if (payload.status && payload.status !== "ok") {
      setLoadStatus(
        "envelope",
        "Envelope: " + (payload.message || payload.status + "."),
        isOverlayWarningStatus(payload.status) ? "warn" : "error"
      );
      return;
    }
    const config = currentConfig();
    if (config.mode === "review") {
      const coverage = reviewEnvelopeCoverage();
      setLoadStatus(
        "envelope",
        "Envelope: synced " + coverage.storedCount + "/" + state.review.bufferRows.length + " stored row(s), bands " + coverage.bandAvailableCount + ".",
        coverage.storedCount < state.review.bufferRows.length ? "warn" : "info"
      );
      return;
    }
    const storedCount = payload.storedRowCount != null ? payload.storedRowCount : payload.rowCount;
    const bandCount = payload.availableRowCount != null ? payload.availableRowCount : storedCount;
    setLoadStatus("envelope", "Envelope: loaded " + Number(storedCount || 0) + " row(s), bands " + Number(bandCount || 0) + ".", "info");
  }

  function updateBacktestLoadStatus(payload) {
    if (!payload || !payload.run) {
      setLoadStatus("backtest", "Backtest: no cached backtest yet. Click Run Backtest.", "info");
      return;
    }
    if (payload.status && payload.status !== "ok" && payload.message) {
      const prefix = payload.status === "no-trades" ? "Backtest: cached run loaded. " : "Backtest: ";
      setLoadStatus(
        "backtest",
        prefix + payload.message,
        payload.status === "no-trades" ? "info" : (isOverlayWarningStatus(payload.status) ? "warn" : "error")
      );
      return;
    }
    setLoadStatus("backtest", "Backtest: cached " + Number(payload.tradeCount || 0) + " trade(s).", "info");
  }

  function isAbortError(error) {
    return error && (error.name === "AbortError" || String(error.message || "").toLowerCase().includes("aborted"));
  }

  function abortController(controller) {
    if (!controller) {
      return;
    }
    try {
      controller.abort();
    } catch (error) {
      // Ignore abort failures while swapping requests.
    }
  }

  function cancelBacktestOverlayRequest() {
    abortController(state.backtest.overlayController);
    state.backtest.overlayController = null;
    state.backtest.overlayRequestToken += 1;
  }

  function cancelBacktestRunRequest() {
    abortController(state.backtest.runController);
    state.backtest.runController = null;
  }

  function isOverlayWarningStatus(statusValue) {
    return ["empty", "partial", "ahead", "warming", "no-signals", "no-trades", "not-cached"].includes(statusValue);
  }

  function formatSignalCounts(signalCounts) {
    if (!signalCounts) {
      return null;
    }
    return "Signals " + Number(signalCounts.totalCount || 0) + " (" + Number(signalCounts.buyCount || 0) + " buy / " + Number(signalCounts.sellCount || 0) + " sell)";
  }

  function reviewOttCoverage() {
    const firstBufferedId = state.review.bufferRows.length ? state.review.bufferRows[0].id : null;
    const lastBufferedId = state.review.bufferRows.length
      ? state.review.bufferRows[state.review.bufferRows.length - 1].id
      : null;
    let availableCount = 0;
    let contiguousAvailableCount = 0;
    let firstAvailableId = null;
    let lastAvailableId = null;
    let firstMissingSeen = false;
    state.review.bufferRows.forEach((row) => {
      const ottRow = state.ottRows.get(row.id);
      if (ottRow && ottRow.available) {
        availableCount += 1;
        if (firstAvailableId == null) {
          firstAvailableId = row.id;
        }
        lastAvailableId = row.id;
        if (!firstMissingSeen) {
          contiguousAvailableCount += 1;
        }
      } else {
        firstMissingSeen = true;
      }
    });
    return {
      availableCount,
      contiguousAvailableCount,
      missingCount: Math.max(0, state.review.bufferRows.length - availableCount),
      firstBufferedId,
      lastBufferedId,
      firstAvailableId,
      lastAvailableId,
      contiguousEndId: state.review.bufferRows.length
        ? (contiguousAvailableCount
          ? state.review.bufferRows[contiguousAvailableCount - 1].id
          : Math.max(0, firstBufferedId - 1))
        : null,
    };
  }

  function reviewEnvelopeCoverage() {
    const firstBufferedId = state.review.bufferRows.length ? state.review.bufferRows[0].id : null;
    const lastBufferedId = state.review.bufferRows.length
      ? state.review.bufferRows[state.review.bufferRows.length - 1].id
      : null;
    let storedCount = 0;
    let basisAvailableCount = 0;
    let bandAvailableCount = 0;
    let lastStoredId = null;
    let lastBandId = null;

    state.review.bufferRows.forEach((row) => {
      const envelopeRow = state.envelopeRows.get(row.id);
      if (!envelopeRow) {
        return;
      }
      storedCount += 1;
      lastStoredId = row.id;
      if (envelopeRow.basisAvailable) {
        basisAvailableCount += 1;
      }
      if (envelopeRow.bandAvailable) {
        bandAvailableCount += 1;
        lastBandId = row.id;
      }
    });

    return {
      firstBufferedId,
      lastBufferedId,
      storedCount,
      basisAvailableCount,
      bandAvailableCount,
      lastStoredId,
      lastBandId,
    };
  }

  function collectOverlayMessages(config) {
    const messages = [];
    const statuses = [];
    if (shouldLoadZig(config) && state.zigStatusPayload && state.zigStatusPayload.status && state.zigStatusPayload.status !== "ok") {
      statuses.push(state.zigStatusPayload.status);
      if (state.zigStatusPayload.message) {
        messages.push(state.zigStatusPayload.message);
      }
    }
    if (shouldLoadOtt(config) && state.ottStatusPayload && state.ottStatusPayload.status && state.ottStatusPayload.status !== "ok") {
      statuses.push(state.ottStatusPayload.status);
      if (state.ottStatusPayload.message) {
        messages.push(state.ottStatusPayload.message);
      }
    }
    if (shouldLoadEnvelope(config) && state.envelopeStatusPayload && state.envelopeStatusPayload.status && state.envelopeStatusPayload.status !== "ok") {
      statuses.push(state.envelopeStatusPayload.status);
      if (state.envelopeStatusPayload.message) {
        messages.push(state.envelopeStatusPayload.message);
      }
    }
    if (config.mode === "review" && state.ottOverlayPayload && state.ottOverlayPayload.status && state.ottOverlayPayload.status !== "ok") {
      statuses.push(state.ottOverlayPayload.status);
      if (state.ottOverlayPayload.message) {
        messages.push(state.ottOverlayPayload.message);
      }
    }
    return {
      messages,
      hasWarning: statuses.some((value) => isOverlayWarningStatus(value)),
    };
  }

  function requiresReviewOttCoverage(config) {
    return config.mode === "review" && Boolean(config.ottEnabled || config.ottSupport || config.ottMarkers);
  }

  function reviewPlaybackStateLabel() {
    if (state.currentMode !== "review") {
      return null;
    }
    if (state.review.trueEndReached) {
      return "Ended";
    }
    if (state.currentRun !== "run") {
      return "Paused";
    }
    if (state.review.waitingFor === "ott") {
      return "Waiting OTT";
    }
    if (state.review.waitingFor === "ticks") {
      return "Waiting ticks";
    }
    return "Playing";
  }

  function reviewBufferStartAfterId() {
    const firstBufferedId = state.review.bufferRows.length ? state.review.bufferRows[0].id : null;
    return Math.max(0, firstBufferedId != null ? (firstBufferedId - 1) : 0);
  }

  function reviewChunkTargetEnd(afterId, limit, endId) {
    const boundedEnd = afterId + Math.max(1, limit);
    return endId != null ? Math.min(endId, boundedEnd) : boundedEnd;
  }

  function reviewRemainingPlaybackMsToCount(targetCount) {
    const visibleRow = reviewLastVisibleRow();
    const safeCount = Math.max(0, Math.min(targetCount, state.review.bufferRows.length));
    const targetRow = safeCount ? state.review.bufferRows[safeCount - 1] : null;
    if (!visibleRow || !targetRow) {
      return null;
    }
    return Math.max(
      0,
      (targetRow.timestampMs - visibleRow.timestampMs) / Math.max(0.25, state.review.playbackSpeed || 1)
    );
  }

  function setReviewWaiting(reason, nowMs) {
    const changed = state.review.waitingFor !== reason;
    state.review.waitingFor = reason;
    setReviewPlaybackAnchor(nowMs);
    if (changed) {
      buildMetaText();
    }
  }

  function clearReviewWaiting(nowMs) {
    if (!state.review.waitingFor) {
      return;
    }
    state.review.waitingFor = null;
    setReviewPlaybackAnchor(nowMs);
    buildMetaText();
  }

  function syncReviewVisibleRange(config, options) {
    if (config.mode !== "review") {
      return false;
    }
    const maxVisibleCount = requiresReviewOttCoverage(config)
      ? reviewOttCoverage().contiguousAvailableCount
      : state.review.bufferRows.length;
    if (state.review.visibleCount <= maxVisibleCount) {
      return false;
    }
    setReviewVisibleCount(maxVisibleCount, options || { preserveCurrentZoom: true });
    return true;
  }

  function safeResizeChart(options) {
    const host = currentChartHost();
    if (!host) {
      disposeChart();
      return false;
    }
    if (!state.chart) {
      return false;
    }
    if (!chartDomMatchesHost(host)) {
      disposeChart();
      return false;
    }
    if (!chartHostHasSize(host)) {
      return false;
    }
    try {
      state.chart.resize();
      if (!options || options.applyYAxis !== false) {
        applyVisibleYAxis();
      }
      return true;
    } catch (error) {
      if (isOffsetWidthError(error)) {
        disposeChart();
        scheduleChartRender({ preserveCurrentZoom: true });
        return false;
      }
      throw error;
    }
  }

  function requestChartResize(options) {
    const requestOptions = {
      applyYAxis: !options || options.applyYAxis !== false,
      remainingAttempts: options && typeof options.remainingAttempts === "number"
        ? options.remainingAttempts
        : CHART_RESIZE_RETRY_LIMIT,
    };
    if (state.chartResizeFrame) {
      cancelAnimationFrame(state.chartResizeFrame);
      state.chartResizeFrame = 0;
    }
    if (state.chartResizeTailFrame) {
      cancelAnimationFrame(state.chartResizeTailFrame);
      state.chartResizeTailFrame = 0;
    }
    if (state.chartResizeTimer) {
      window.clearTimeout(state.chartResizeTimer);
      state.chartResizeTimer = 0;
    }
    state.chartResizeFrame = requestAnimationFrame(() => {
      state.chartResizeFrame = 0;
      state.chartResizeTailFrame = requestAnimationFrame(() => {
        state.chartResizeTailFrame = 0;
        const resized = safeResizeChart({ applyYAxis: requestOptions.applyYAxis });
        if (!resized && state.chart && currentChartHost() && requestOptions.remainingAttempts > 0) {
          state.chartResizeTimer = window.setTimeout(() => {
            state.chartResizeTimer = 0;
            requestChartResize({
              applyYAxis: requestOptions.applyYAxis,
              remainingAttempts: requestOptions.remainingAttempts - 1,
            });
          }, CHART_RESIZE_RETRY_DELAY_MS);
        }
      });
    });
  }

  function bindChartLifecycle() {
    if (state.chartListenersBound) {
      return;
    }
    window.addEventListener("resize", requestChartResize);
    if (typeof ResizeObserver === "function" && elements.chartPanel && !state.chartResizeObserver) {
      state.chartResizeObserver = new ResizeObserver(() => {
        requestChartResize();
      });
      state.chartResizeObserver.observe(elements.chartPanel);
    }
    state.chartListenersBound = true;
  }

  function ensureChart() {
    const host = currentChartHost();
    if (!host) {
      disposeChart();
      return null;
    }
    if (!chartHostHasSize(host)) {
      return null;
    }
    bindChartLifecycle();
    if (state.chart && !chartDomMatchesHost(host)) {
      disposeChart();
    }
    if (!state.chart) {
      const existingInstance = typeof echarts.getInstanceByDom === "function"
        ? echarts.getInstanceByDom(host)
        : null;
      if (existingInstance) {
        try {
          existingInstance.dispose();
        } catch (error) {
          // Ignore stale instance disposal failures while taking ownership of the host.
        }
      }
      state.chart = echarts.init(host, null, { renderer: "canvas" });
      state.chart.on("datazoom", () => {
        updateVisibleZoomFromChart();
        applyVisibleYAxis();
      });
    }
    return state.chart;
  }

  function readZoomWindowFromChart() {
    const host = currentChartHost();
    if (!state.chart || !host || !chartDomMatchesHost(host) || !state.rows.length) {
      return null;
    }
    let option = null;
    try {
      option = state.chart.getOption();
    } catch (error) {
      if (isOffsetWidthError(error)) {
        disposeChart();
        scheduleChartRender({ preserveCurrentZoom: true });
        return null;
      }
      throw error;
    }
    const dataZoom = option.dataZoom && option.dataZoom[0];
    if (!dataZoom) {
      return null;
    }
    if (typeof dataZoom.startValue === "number" && typeof dataZoom.endValue === "number") {
      return clampZoomWindow({ startMs: dataZoom.startValue, endMs: dataZoom.endValue });
    }
    const firstTs = state.rows[0].timestampMs;
    const lastTs = state.rows[state.rows.length - 1].timestampMs;
    return clampZoomWindow({
      startMs: firstTs + (lastTs - firstTs) * ((dataZoom.start || 0) / 100),
      endMs: firstTs + (lastTs - firstTs) * ((dataZoom.end || 100) / 100),
    });
  }

  function clampZoomWindow(windowRange) {
    if (!state.rows.length) {
      return windowRange;
    }
    const firstTs = state.rows[0].timestampMs;
    const lastTs = state.rows[state.rows.length - 1].timestampMs;
    const startMs = Math.max(firstTs, Math.min(windowRange.startMs, lastTs));
    const endMs = Math.max(startMs, Math.min(windowRange.endMs, lastTs));
    return { startMs, endMs };
  }

  function updateVisibleZoomFromChart() {
    const zoomWindow = readZoomWindowFromChart();
    if (!zoomWindow) {
      return;
    }
    state.visibleWindow = zoomWindow;
    state.visibleSpanMs = Math.max(1000, zoomWindow.endMs - zoomWindow.startMs);
  }

  function visibleRows(windowRange) {
    if (!state.rows.length) {
      return [];
    }
    return state.rows.filter((row) => row.timestampMs >= windowRange.startMs && row.timestampMs <= windowRange.endMs);
  }

  function structureSeriesValue(row, seriesKey) {
    const configuredSeries = SERIES_CONFIG[seriesKey];
    if (configuredSeries && typeof row[configuredSeries.field] === "number") {
      return row[configuredSeries.field];
    }
    if (typeof row.mid === "number") {
      return row.mid;
    }
    if (typeof row.price === "number") {
      return row.price;
    }
    if (typeof row.ask === "number") {
      return row.ask;
    }
    if (typeof row.bid === "number") {
      return row.bid;
    }
    return null;
  }

  function structureRows(rows, primarySeriesKey, targetPoints) {
    if (!rows.length || rows.length <= targetPoints) {
      return rows;
    }
    const bucketCount = Math.max(1, Math.floor(targetPoints / 4));
    const bucketSize = Math.max(1, Math.ceil(rows.length / bucketCount));
    const selected = new Map();
    for (let start = 0; start < rows.length; start += bucketSize) {
      const bucket = rows.slice(start, Math.min(rows.length, start + bucketSize));
      if (!bucket.length) {
        continue;
      }
      let highRow = bucket[0];
      let lowRow = bucket[0];
      bucket.forEach((row) => {
        const value = structureSeriesValue(row, primarySeriesKey);
        const highValue = structureSeriesValue(highRow, primarySeriesKey);
        const lowValue = structureSeriesValue(lowRow, primarySeriesKey);
        if (value != null && (highValue == null || value > highValue)) {
          highRow = row;
        }
        if (value != null && (lowValue == null || value < lowValue)) {
          lowRow = row;
        }
      });
      [bucket[0], lowRow, highRow, bucket[bucket.length - 1]].forEach((row) => {
        selected.set(row.id, row);
      });
    }
    return Array.from(selected.values()).sort((left, right) => left.id - right.id);
  }

  function displayedRowsForView(rows, config) {
    if (!rows.length) {
      return rows;
    }
    if (config.zigViewMode === "zigonly") {
      return [];
    }
    if (config.zigViewMode === "structure") {
      return structureRows(rows, getPrimarySeriesKey(), STRUCTURE_VIEW_TARGET_POINTS);
    }
    return rows;
  }

  function enabledZigLevels(config) {
    return Object.keys(ZIG_LEVEL_CONFIG).filter((level) => Boolean(config["zig" + level.charAt(0).toUpperCase() + level.slice(1)]));
  }

  function shouldLoadZig(config) {
    return enabledZigLevels(config).length > 0;
  }

  function shouldLoadOtt(config) {
    return config.ottEnabled || config.ottSupport || config.ottMarkers || (config.mode === "review" && config.ottTrades);
  }

  function shouldLoadEnvelope(config) {
    return config.envelopeEnabled;
  }

  function zigSegmentsForLevel(level, rows) {
    if (!rows.length) {
      return [];
    }
    const visibleLastId = rows[rows.length - 1].id;
    const firstId = rows[0].id;
    const lastId = rows[rows.length - 1].id;
    return Array.from(state.zigRows[level].values())
      .filter((segment) => (
        segment.confirmtickid <= visibleLastId
        && segment.starttickid <= lastId
        && segment.endtickid >= firstId
      ))
      .sort((left, right) => (
        (left.endtickid - right.endtickid)
        || (left.confirmtickid - right.confirmtickid)
        || (left.id - right.id)
      ));
  }

  function zigPolylinePoints(segments) {
    if (!segments.length) {
      return [];
    }
    const points = [[segments[0].startTimeMs, segments[0].startprice]];
    segments.forEach((segment) => {
      const previous = points[points.length - 1];
      if (!previous || previous[0] !== segment.startTimeMs || previous[1] !== segment.startprice) {
        points.push([segment.startTimeMs, segment.startprice]);
      }
      points.push([segment.endTimeMs, segment.endprice]);
    });
    return points;
  }

  function zigSegmentsAtTimestamp(level, timestampMs, rows) {
    return zigSegmentsForLevel(level, rows).filter((segment) => {
      const startMs = Math.min(segment.startTimeMs, segment.endTimeMs);
      const endMs = Math.max(segment.startTimeMs, segment.endTimeMs);
      return timestampMs >= startMs && timestampMs <= endMs;
    });
  }

  function signalMarkerOffset(row, ottRow) {
    const basePrice = typeof (ottRow && ottRow.price) === "number"
      ? ottRow.price
      : (typeof row.price === "number" ? row.price : row.mid);
    const spread = Math.abs(Number((ottRow && ottRow.spread != null ? ottRow.spread : row.spread) || 0));
    if (typeof basePrice !== "number") {
      return SIGNAL_MARKER_MIN_OFFSET;
    }
    return Math.min(
      SIGNAL_MARKER_MAX_OFFSET,
      Math.max(SIGNAL_MARKER_MIN_OFFSET, spread * 2.25, Math.abs(basePrice) * 0.00012)
    );
  }

  function markerPrice(row, ottRow, buyField, sellField) {
    const basePrice = typeof (ottRow && ottRow.price) === "number"
      ? ottRow.price
      : (typeof row.price === "number" ? row.price : row.mid);
    if (!ottRow) {
      return basePrice;
    }
    const offset = signalMarkerOffset(row, ottRow);
    if (ottRow[buyField] && typeof basePrice === "number") {
      return basePrice - offset;
    }
    if (ottRow[sellField] && typeof basePrice === "number") {
      return basePrice + offset;
    }
    return ottRow.ott2 || ottRow.ott || basePrice;
  }

  function visibleYExtent(windowRange) {
    const selected = getActiveSeriesKeys();
    const rows = visibleRows(windowRange);
    const searchRows = rows.length ? rows : state.rows.slice(-1);
    const config = currentConfig();
    let minPrice = Number.POSITIVE_INFINITY;
    let maxPrice = Number.NEGATIVE_INFINITY;

    searchRows.forEach((row) => {
      selected.forEach((seriesKey) => {
        const value = row[SERIES_CONFIG[seriesKey].field];
        if (typeof value === "number") {
          minPrice = Math.min(minPrice, value);
          maxPrice = Math.max(maxPrice, value);
        }
      });
      const ottRow = state.ottRows.get(row.id);
      if (ottRow) {
        if (config.ottEnabled && typeof ottRow.ott2 === "number") {
          minPrice = Math.min(minPrice, ottRow.ott2);
          maxPrice = Math.max(maxPrice, ottRow.ott2);
        }
        if (config.ottSupport && typeof ottRow.mavg === "number") {
          minPrice = Math.min(minPrice, ottRow.mavg);
          maxPrice = Math.max(maxPrice, ottRow.mavg);
        }
        if (config.ottMarkers) {
          const [buyField, sellField] = getSignalFields(config.ottSignalMode);
          const markValue = markerPrice(row, ottRow, buyField, sellField);
          if (typeof markValue === "number" && (ottRow[buyField] || ottRow[sellField])) {
            minPrice = Math.min(minPrice, markValue);
            maxPrice = Math.max(maxPrice, markValue);
          }
        }
      }
      const envelopeRow = state.envelopeRows.get(row.id);
      if (envelopeRow && config.envelopeEnabled) {
        if (typeof envelopeRow.basis === "number") {
          minPrice = Math.min(minPrice, envelopeRow.basis);
          maxPrice = Math.max(maxPrice, envelopeRow.basis);
        }
        if (typeof envelopeRow.upper === "number") {
          minPrice = Math.min(minPrice, envelopeRow.upper);
          maxPrice = Math.max(maxPrice, envelopeRow.upper);
        }
        if (typeof envelopeRow.lower === "number") {
          minPrice = Math.min(minPrice, envelopeRow.lower);
          maxPrice = Math.max(maxPrice, envelopeRow.lower);
        }
      }
    });

    if (shouldLoadZig(config) && searchRows.length) {
      enabledZigLevels(config).forEach((level) => {
        zigSegmentsForLevel(level, searchRows).forEach((segment) => {
          minPrice = Math.min(minPrice, segment.startprice, segment.endprice);
          maxPrice = Math.max(maxPrice, segment.startprice, segment.endprice);
        });
      });
    }

    if (config.mode === "review" && config.ottTrades) {
      state.ottTrades.forEach((trade) => {
        if (trade.entryTsMs >= windowRange.startMs && trade.entryTsMs <= windowRange.endMs) {
          minPrice = Math.min(minPrice, trade.entryprice);
          maxPrice = Math.max(maxPrice, trade.entryprice);
        }
        if (trade.exitTsMs >= windowRange.startMs && trade.exitTsMs <= windowRange.endMs) {
          minPrice = Math.min(minPrice, trade.exitprice);
          maxPrice = Math.max(maxPrice, trade.exitprice);
        }
      });
    }

    if (!Number.isFinite(minPrice) || !Number.isFinite(maxPrice)) {
      minPrice = 0;
      maxPrice = 2;
    }

    let axisMin = Math.floor(minPrice) - 1;
    let axisMax = Math.ceil(maxPrice) + 1;
    if (axisMax <= axisMin) {
      axisMax = axisMin + 2;
    }
    return { min: axisMin, max: axisMax };
  }

  function gapAreas(rows) {
    const gaps = [];
    for (let index = 1; index < rows.length; index += 1) {
      const previous = rows[index - 1];
      const current = rows[index];
      if ((current.timestampMs - previous.timestampMs) >= 60000) {
        gaps.push([{ xAxis: previous.timestampMs }, { xAxis: current.timestampMs }]);
      }
    }
    return gaps;
  }

  function buildPriceSeries(rows, selected) {
    if (!rows.length || !selected.length) {
      return [];
    }
    const gaps = gapAreas(rows);
    const showFilledMid = selected.length === 1 && selected[0] === "mid";
    return selected.map((seriesKey, index) => {
      const config = SERIES_CONFIG[seriesKey];
      return {
        name: config.label,
        type: "line",
        showSymbol: false,
        smooth: false,
        data: rows.map((row) => [row.timestampMs, row[config.field]]),
        lineStyle: { width: config.width, color: config.color },
        areaStyle: showFilledMid && seriesKey === "mid" ? {
          color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
            { offset: 0, color: "rgba(109, 216, 255, 0.20)" },
            { offset: 1, color: "rgba(109, 216, 255, 0.02)" },
          ]),
        } : undefined,
        markArea: index === 0 && gaps.length ? {
          silent: true,
          itemStyle: { color: "rgba(255, 200, 87, 0.08)" },
          data: gaps,
        } : undefined,
      };
    });
  }

  function getSignalFields(signalMode) {
    if (signalMode === "price") {
      return ["pricebuy", "pricesell"];
    }
    if (signalMode === "color") {
      return ["colorbuy", "colorsell"];
    }
    return ["supportbuy", "supportsell"];
  }

  function buildSignalSeries(rows, config) {
    if (!config.ottMarkers) {
      return [];
    }
    const [buyField, sellField] = getSignalFields(config.ottSignalMode);
    const buyData = [];
    const sellData = [];

    rows.forEach((row) => {
      const ottRow = state.ottRows.get(row.id);
      if (!ottRow) {
        return;
      }
      const markerValue = markerPrice(row, ottRow, buyField, sellField);
      if (ottRow[buyField]) {
        buyData.push([row.timestampMs, markerValue]);
      }
      if (ottRow[sellField]) {
        sellData.push([row.timestampMs, markerValue]);
      }
    });

    return [
      {
        name: "OTT Buy",
        type: "scatter",
        symbol: "triangle",
        symbolSize: 11,
        itemStyle: { color: "#4ade80" },
        data: buyData,
      },
      {
        name: "OTT Sell",
        type: "scatter",
        symbol: "triangle",
        symbolRotate: 180,
        symbolSize: 11,
        itemStyle: { color: "#f87171" },
        data: sellData,
      },
    ];
  }

  function buildTradeSeries(config) {
    if (config.mode !== "review" || !config.ottTrades || !state.ottTrades.length) {
      return [];
    }
    const longEntries = [];
    const shortEntries = [];
    const exits = [];

    state.ottTrades.forEach((trade) => {
      if (trade.direction === "long") {
        longEntries.push([trade.entryTsMs, trade.entryprice]);
      } else {
        shortEntries.push([trade.entryTsMs, trade.entryprice]);
      }
      exits.push([trade.exitTsMs, trade.exitprice]);
    });

    return [
      {
        name: "Long Entry",
        type: "scatter",
        symbol: "triangle",
        symbolSize: 14,
        itemStyle: { color: "#22c55e" },
        data: longEntries,
      },
      {
        name: "Short Entry",
        type: "scatter",
        symbol: "triangle",
        symbolRotate: 180,
        symbolSize: 14,
        itemStyle: { color: "#ef4444" },
        data: shortEntries,
      },
      {
        name: "Exit",
        type: "scatter",
        symbol: "circle",
        symbolSize: 10,
        itemStyle: { color: "#f3f6fb", borderColor: "#0b1220", borderWidth: 1.5 },
        data: exits,
      },
    ];
  }

  function buildOttSeries(rows, config) {
    if (!shouldLoadOtt(config) || !rows.length) {
      return [];
    }
    const overlaySeries = [];

    if (config.ottEnabled) {
      if (config.ottHighlight) {
        const neutralData = [];
        const upData = [];
        const downData = [];
        rows.forEach((row) => {
          const ottRow = state.ottRows.get(row.id);
          const value = ottRow && typeof ottRow.ott2 === "number" ? ottRow.ott2 : null;
          if (value == null) {
            neutralData.push([row.timestampMs, null]);
            upData.push([row.timestampMs, null]);
            downData.push([row.timestampMs, null]);
            return;
          }
          if (ottRow.ott3 == null) {
            neutralData.push([row.timestampMs, value]);
            upData.push([row.timestampMs, null]);
            downData.push([row.timestampMs, null]);
          } else if (ottRow.ott2 > ottRow.ott3) {
            neutralData.push([row.timestampMs, null]);
            upData.push([row.timestampMs, value]);
            downData.push([row.timestampMs, null]);
          } else {
            neutralData.push([row.timestampMs, null]);
            upData.push([row.timestampMs, null]);
            downData.push([row.timestampMs, value]);
          }
        });
        overlaySeries.push(
          {
            name: "OTT",
            type: "line",
            showSymbol: false,
            smooth: false,
            connectNulls: false,
            data: neutralData,
            lineStyle: { width: 2.2, color: "#b800d9", opacity: 0.75 },
          },
          {
            name: "OTT Up",
            type: "line",
            showSymbol: false,
            smooth: false,
            connectNulls: false,
            data: upData,
            lineStyle: { width: 2.35, color: "#22c55e" },
          },
          {
            name: "OTT Down",
            type: "line",
            showSymbol: false,
            smooth: false,
            connectNulls: false,
            data: downData,
            lineStyle: { width: 2.35, color: "#ef4444" },
          }
        );
      } else {
        overlaySeries.push({
          name: "OTT",
          type: "line",
          showSymbol: false,
          smooth: false,
          connectNulls: false,
          data: rows.map((row) => {
            const ottRow = state.ottRows.get(row.id);
            return [row.timestampMs, ottRow && ottRow.ott2 != null ? ottRow.ott2 : null];
          }),
          lineStyle: { width: 2.25, color: "#b800d9" },
        });
      }
    }

    if (config.ottSupport) {
      overlaySeries.push({
        name: "OTT Support",
        type: "line",
        showSymbol: false,
        smooth: false,
        connectNulls: false,
        data: rows.map((row) => {
          const ottRow = state.ottRows.get(row.id);
          return [row.timestampMs, ottRow && ottRow.mavg != null ? ottRow.mavg : null];
        }),
        lineStyle: { width: 1.7, color: "#0585E1", opacity: 0.92 },
      });
    }

    return overlaySeries.concat(buildSignalSeries(rows, config), buildTradeSeries(config));
  }

  function buildEnvelopeSeries(rows, config) {
    if (!shouldLoadEnvelope(config) || !rows.length) {
      return [];
    }
    return [
      {
        name: "Envelope Basis",
        type: "line",
        showSymbol: false,
        smooth: false,
        connectNulls: false,
        data: rows.map((row) => {
          const envelopeRow = state.envelopeRows.get(row.id);
          return [row.timestampMs, envelopeRow && envelopeRow.basisAvailable ? envelopeRow.basis : null];
        }),
        lineStyle: { width: 1.8, color: "#f8d36c", type: "dashed", opacity: 0.96 },
      },
      {
        name: "Envelope Upper",
        type: "line",
        showSymbol: false,
        smooth: false,
        connectNulls: false,
        data: rows.map((row) => {
          const envelopeRow = state.envelopeRows.get(row.id);
          return [row.timestampMs, envelopeRow && envelopeRow.bandAvailable ? envelopeRow.upper : null];
        }),
        lineStyle: { width: 1.45, color: "#5eead4", opacity: 0.9 },
      },
      {
        name: "Envelope Lower",
        type: "line",
        showSymbol: false,
        smooth: false,
        connectNulls: false,
        data: rows.map((row) => {
          const envelopeRow = state.envelopeRows.get(row.id);
          return [row.timestampMs, envelopeRow && envelopeRow.bandAvailable ? envelopeRow.lower : null];
        }),
        lineStyle: { width: 1.45, color: "#fda4af", opacity: 0.9 },
      },
    ];
  }

  function buildZigSeries(rows, config) {
    if (!shouldLoadZig(config)) {
      return [];
    }
    return enabledZigLevels(config)
      .map((level) => {
        const segments = zigSegmentsForLevel(level, rows);
        const points = zigPolylinePoints(segments);
        if (points.length < 2) {
          return null;
        }
        const levelConfig = ZIG_LEVEL_CONFIG[level];
        return {
          name: levelConfig.label,
          type: "line",
          showSymbol: false,
          smooth: false,
          connectNulls: false,
          data: points,
          lineStyle: {
            width: levelConfig.width,
            color: levelConfig.color,
            opacity: levelConfig.opacity,
          },
          z: 5,
        };
      })
      .filter(Boolean);
  }

  function canonicalTooltipSeriesName(seriesName) {
    if (seriesName === "OTT" || seriesName === "OTT Up" || seriesName === "OTT Down") {
      return "OTT";
    }
    if (seriesName === "OTT Buy" || seriesName === "OTT Sell" || seriesName === "Long Entry" || seriesName === "Short Entry" || seriesName === "Exit") {
      return null;
    }
    return seriesName;
  }

  function findRowsAtTimestamp(timestampMs) {
    if (!state.rows.length || typeof timestampMs !== "number") {
      return [];
    }
    let low = 0;
    let high = state.rows.length - 1;
    let matchIndex = -1;
    while (low <= high) {
      const middle = Math.floor((low + high) / 2);
      const value = state.rows[middle].timestampMs;
      if (value === timestampMs) {
        matchIndex = middle;
        break;
      }
      if (value < timestampMs) {
        low = middle + 1;
      } else {
        high = middle - 1;
      }
    }
    if (matchIndex < 0) {
      return [];
    }
    let start = matchIndex;
    let end = matchIndex;
    while (start > 0 && state.rows[start - 1].timestampMs === timestampMs) {
      start -= 1;
    }
    while ((end + 1) < state.rows.length && state.rows[end + 1].timestampMs === timestampMs) {
      end += 1;
    }
    return state.rows.slice(start, end + 1);
  }

  function formatSignalModeLabel(signalMode) {
    return signalMode ? signalMode.charAt(0).toUpperCase() + signalMode.slice(1) : "Signal";
  }

  function tooltipSignalLines(timestampMs, config) {
    const [buyField, sellField] = getSignalFields(config.ottSignalMode);
    const lines = [];
    const seen = new Set();
    findRowsAtTimestamp(timestampMs).forEach((row) => {
      const ottRow = state.ottRows.get(row.id);
      if (!ottRow) {
        return;
      }
      if (ottRow[buyField]) {
        const text = "Signal: Buy (" + formatSignalModeLabel(config.ottSignalMode) + ")";
        if (!seen.has(text)) {
          seen.add(text);
          lines.push(text);
        }
      }
      if (ottRow[sellField]) {
        const text = "Signal: Sell (" + formatSignalModeLabel(config.ottSignalMode) + ")";
        if (!seen.has(text)) {
          seen.add(text);
          lines.push(text);
        }
      }
    });
    return lines;
  }

  function tooltipTradeLines(timestampMs) {
    const lines = [];
    const seen = new Set();
    state.ottTrades.forEach((trade) => {
      if (trade.entryTsMs === timestampMs) {
        const label = trade.direction === "long" ? "Trade: Long Entry" : "Trade: Short Entry";
        const text = label + " @ " + Number(trade.entryprice).toFixed(2);
        if (!seen.has(text)) {
          seen.add(text);
          lines.push(text);
        }
      }
      if (trade.exitTsMs === timestampMs) {
        const text = "Trade: Exit @ " + Number(trade.exitprice).toFixed(2);
        if (!seen.has(text)) {
          seen.add(text);
          lines.push(text);
        }
      }
    });
    return lines;
  }

  function formatZigTime(timestampMs) {
    return new Date(timestampMs).toLocaleTimeString("en-AU", {
      hour12: false,
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
  }

  function tooltipZigLines(timestampMs, config) {
    if (!shouldLoadZig(config) || !state.rows.length) {
      return [];
    }
    const lines = [];
    enabledZigLevels(config).forEach((level) => {
      zigSegmentsAtTimestamp(level, timestampMs, state.rows).forEach((segment) => {
        const direction = segment.dir === 1 ? "up" : "down";
        lines.push(
          "Zig " + ZIG_LEVEL_CONFIG[level].label.replace(" Zig", "") + " #" + segment.id
            + " " + direction
            + " | ticks " + segment.starttickid + "->" + segment.endtickid
            + " | confirm " + segment.confirmtickid
        );
        lines.push(
          "Times " + formatZigTime(segment.startTimeMs)
            + " -> " + formatZigTime(segment.endTimeMs)
            + " | confirm " + formatZigTime(segment.confirmTimeMs)
            + " | amp " + Number(segment.amplitude).toFixed(2)
            + " | dur " + Number(segment.dursec).toFixed(1) + "s"
            + (segment.childcount ? " | child " + Number(segment.childcount) : "")
        );
      });
    });
    return lines;
  }

  function buildTooltipFormatter() {
    return function formatter(params) {
      const items = Array.isArray(params) ? params : (params ? [params] : []);
      if (!items.length) {
        return "";
      }
      const axisValue = Number(
        items[0].axisValue != null
          ? items[0].axisValue
          : (Array.isArray(items[0].value) ? items[0].value[0] : items[0].value)
      );
      const lines = [new Date(axisValue).toLocaleString("en-AU", { hour12: false })];
      const valueByLabel = new Map();
      items.forEach((item) => {
        const label = canonicalTooltipSeriesName(item.seriesName);
        const value = Array.isArray(item.value) ? item.value[1] : item.value;
        if (!label || typeof value !== "number") {
          return;
        }
        valueByLabel.set(label, Number(value).toFixed(2));
      });

      ["Mid", "Envelope Basis", "Envelope Upper", "Envelope Lower", "Macro Zig", "Maxi Zig", "Medium Zig", "Micro Zig", "OTT", "OTT Support", "Ask", "Bid"].forEach((label) => {
        if (valueByLabel.has(label)) {
          lines.push(label + ": " + valueByLabel.get(label));
          valueByLabel.delete(label);
        }
      });
      valueByLabel.forEach((value, label) => {
        lines.push(label + ": " + value);
      });

      const config = currentConfig();
      tooltipZigLines(axisValue, config).forEach((line) => lines.push(line));
      tooltipSignalLines(axisValue, config).forEach((line) => lines.push(line));
      tooltipTradeLines(axisValue).forEach((line) => lines.push(line));
      return lines.join("<br>");
    };
  }

  function buildMetaText() {
    if (!state.rows.length) {
      const config = currentConfig();
      if (config.mode === "review" && state.review.bufferRows.length) {
        const coverage = reviewOttCoverage();
        const parts = ["Replay 0/" + state.review.bufferRows.length];
        if (coverage.firstBufferedId != null && coverage.lastBufferedId != null) {
          if (state.review.sessionEndId != null) {
            parts.push("Ticks " + coverage.firstBufferedId + "-" + coverage.lastBufferedId + "/" + state.review.sessionEndId);
          } else {
            parts.push("Ticks " + coverage.firstBufferedId + "-" + coverage.lastBufferedId);
          }
        }
        parts.push(state.review.fetchingTicks ? "Tick prefetch" : "Tick ready");
        if (shouldLoadOtt(config) && coverage.firstBufferedId != null) {
          parts.push("OTT " + coverage.firstBufferedId + "-" + Number(coverage.contiguousEndId || Math.max(0, coverage.firstBufferedId - 1)));
          parts.push(state.review.fetchingOtt ? "OTT prefetch" : "OTT ready");
          parts.push("OTT sync " + coverage.contiguousAvailableCount + "/" + state.review.bufferRows.length);
        }
        parts.push(reviewPlaybackStateLabel());
        parts.push("Speed " + state.review.playbackSpeed + "x");
        elements.liveMeta.textContent = parts.filter(Boolean).join(" | ");
        return;
      }
      elements.liveMeta.textContent = "No rows returned.";
      return;
    }
    const config = currentConfig();
    const primarySeries = getPrimarySeriesKey();
    const lastRow = state.rows[state.rows.length - 1];
    const price = lastRow[SERIES_CONFIG[primarySeries].field];
    const lastOtt = state.ottRows.get(lastRow.id);
    const meta = config.mode === "review"
      ? [
        "Ptr " + lastRow.id,
        "Replay " + state.review.visibleCount + "/" + state.review.bufferRows.length,
        "Price " + Number(price).toFixed(2),
      ]
      : [
        "Rows " + state.rows.length,
        "Last id " + lastRow.id,
        "Price " + Number(price).toFixed(2),
      ];
    if (config.mode === "review") {
      const coverage = reviewOttCoverage();
      if (coverage.firstBufferedId != null && coverage.lastBufferedId != null) {
        if (state.review.sessionEndId != null) {
          meta.push("Ticks " + coverage.firstBufferedId + "-" + coverage.lastBufferedId + "/" + state.review.sessionEndId);
        } else {
          meta.push("Ticks " + coverage.firstBufferedId + "-" + coverage.lastBufferedId);
        }
      }
      meta.push(state.review.fetchingTicks ? "Tick prefetch" : "Tick ready");
      if (shouldLoadOtt(config)) {
        if (coverage.firstBufferedId != null) {
          meta.push("OTT " + coverage.firstBufferedId + "-" + Number(coverage.contiguousEndId || Math.max(0, coverage.firstBufferedId - 1)));
        }
        meta.push(state.review.fetchingOtt ? "OTT prefetch" : "OTT ready");
        meta.push("OTT sync " + coverage.contiguousAvailableCount + "/" + state.review.bufferRows.length);
      }
      meta.push(reviewPlaybackStateLabel());
      meta.push("Speed " + state.review.playbackSpeed + "x");
    }
    if (lastOtt && lastOtt.ott2 != null && config.ottEnabled) {
      meta.push("OTT " + Number(lastOtt.ott2).toFixed(2));
    }
    const lastEnvelope = state.envelopeRows.get(lastRow.id);
    if (lastEnvelope && lastEnvelope.basisAvailable && config.envelopeEnabled) {
      meta.push("Env " + Number(lastEnvelope.basis).toFixed(2));
    }
    if (config.mode === "review" && state.ottStatusPayload && state.ottStatusPayload.signalCounts) {
      meta.push(formatSignalCounts(state.ottStatusPayload.signalCounts));
    }
    if (config.mode === "review" && (state.ottOverlayPayload || state.ottRun)) {
      const tradeCount = state.ottOverlayPayload && state.ottOverlayPayload.tradeCount != null
        ? state.ottOverlayPayload.tradeCount
        : (state.ottRun && state.ottRun.tradecount != null ? state.ottRun.tradecount : 0);
      meta.push("Trades " + Number(tradeCount));
    }
    meta.push("View " + (config.zigViewMode === "zigonly" ? "ZigOnly" : config.zigViewMode.charAt(0).toUpperCase() + config.zigViewMode.slice(1)));
    meta.push(new Date(lastRow.timestampMs).toLocaleString("en-AU", { hour12: false }));
    elements.liveMeta.textContent = meta.join(" | ");
  }

  function determineTargetZoom(options) {
    const rows = state.rows;
    const firstTs = rows.length ? rows[0].timestampMs : Date.now() - 60000;
    const lastTs = rows.length ? rows[rows.length - 1].timestampMs : Date.now();
    const defaultSpan = lastTs > firstTs ? lastTs - firstTs : 60000;

    if (options && options.preserveCurrentZoom) {
      return clampZoomWindow(readZoomWindowFromChart() || state.visibleWindow || {
        startMs: Math.max(firstTs, lastTs - (state.visibleSpanMs || defaultSpan)),
        endMs: lastTs,
      });
    }

    if (!state.visibleSpanMs || (options && options.resetWindow)) {
      state.visibleSpanMs = Math.max(1000, defaultSpan);
    }

    return clampZoomWindow({
      startMs: Math.max(firstTs, lastTs - state.visibleSpanMs),
      endMs: lastTs,
    });
  }

  function buildYAxis(windowRange) {
    const extent = visibleYExtent(windowRange);
    return {
      type: "value",
      scale: false,
      min: extent.min,
      max: extent.max,
      minInterval: 1,
      splitNumber: 4,
      axisLabel: {
        color: "#9eadc5",
        formatter(value) {
          return String(Math.round(value));
        },
      },
      axisLine: { lineStyle: { color: "rgba(147, 181, 255, 0.24)" } },
      splitLine: { lineStyle: { color: "rgba(147, 181, 255, 0.06)" } },
    };
  }

  function applyVisibleYAxis() {
    const host = currentChartHost();
    if (!state.chart || !host || !chartDomMatchesHost(host) || !state.rows.length) {
      return;
    }
    const zoomWindow = readZoomWindowFromChart() || state.visibleWindow;
    if (!zoomWindow) {
      return;
    }
    try {
      state.chart.setOption({ yAxis: buildYAxis(zoomWindow) }, false);
    } catch (error) {
      if (isOffsetWidthError(error)) {
        disposeChart();
        scheduleChartRender({ preserveCurrentZoom: true });
        requestChartResize({ applyYAxis: false, remainingAttempts: CHART_RESIZE_RETRY_LIMIT });
        return;
      }
      throw error;
    }
  }

  function renderChart(options, renderOptions) {
    const host = currentChartHost();
    if (!host || !chartHostHasSize(host)) {
      scheduleChartRender(options);
      requestChartResize({ remainingAttempts: CHART_RESIZE_RETRY_LIMIT });
      buildMetaText();
      return;
    }
    const chart = ensureChart();
    if (!chart) {
      scheduleChartRender(options);
      requestChartResize({ remainingAttempts: CHART_RESIZE_RETRY_LIMIT });
      return;
    }
    const selected = getActiveSeriesKeys();
    const targetZoom = determineTargetZoom(options || {});
    const firstTs = state.rows.length ? state.rows[0].timestampMs : Date.now() - 60000;
    const lastTs = state.rows.length ? state.rows[state.rows.length - 1].timestampMs : Date.now();
    const config = currentConfig();
    const displayRows = displayedRowsForView(state.rows, config);

    state.visibleWindow = targetZoom;
    state.visibleSpanMs = Math.max(1000, targetZoom.endMs - targetZoom.startMs);

    try {
      chart.setOption({
        animation: false,
        backgroundColor: "transparent",
        grid: { left: 54, right: 18, top: 16, bottom: 84 },
        tooltip: {
          trigger: "axis",
          backgroundColor: "rgba(6, 11, 20, 0.96)",
          borderColor: "rgba(109, 216, 255, 0.24)",
          textStyle: { color: "#f3f6fb" },
          axisPointer: {
            type: "cross",
            lineStyle: { color: "rgba(109, 216, 255, 0.28)" },
          },
          formatter: buildTooltipFormatter(),
        },
        xAxis: {
          type: "time",
          min: firstTs,
          max: lastTs,
          axisLine: { lineStyle: { color: "rgba(147, 181, 255, 0.26)" } },
          axisLabel: {
            color: "#9eadc5",
            formatter(value) {
              return new Date(value).toLocaleTimeString("en-AU", {
                hour12: false,
                hour: "2-digit",
                minute: "2-digit",
                second: "2-digit",
              });
            },
          },
          splitLine: { lineStyle: { color: "rgba(147, 181, 255, 0.08)" } },
        },
        yAxis: buildYAxis(targetZoom),
        dataZoom: [
          {
            type: "inside",
            filterMode: "none",
            startValue: targetZoom.startMs,
            endValue: targetZoom.endMs,
          },
          {
            type: "slider",
            height: 42,
            bottom: 18,
            filterMode: "none",
            startValue: targetZoom.startMs,
            endValue: targetZoom.endMs,
            borderColor: "rgba(147, 181, 255, 0.16)",
            backgroundColor: "rgba(8, 13, 23, 0.94)",
            fillerColor: "rgba(109, 216, 255, 0.16)",
            dataBackground: {
              lineStyle: { color: "rgba(109, 216, 255, 0.48)" },
              areaStyle: { color: "rgba(109, 216, 255, 0.1)" },
            },
          },
        ],
        series: buildPriceSeries(displayRows, selected)
          .concat(buildEnvelopeSeries(displayRows, config))
          .concat(buildZigSeries(state.rows, config))
          .concat(buildOttSeries(displayRows, config)),
      }, true);
    } catch (error) {
      if (isOffsetWidthError(error)) {
        disposeChart();
        scheduleChartRender(options);
        requestChartResize({ remainingAttempts: CHART_RESIZE_RETRY_LIMIT });
        return;
      }
      throw error;
    }

    requestChartResize({ remainingAttempts: CHART_RESIZE_RETRY_LIMIT });
    buildMetaText();
  }

  function closeStream() {
    if (state.source) {
      state.source.close();
      state.source = null;
    }
  }

  function stopReviewPlayback(options) {
    if (state.review.rafId) {
      cancelAnimationFrame(state.review.rafId);
      state.review.rafId = 0;
    }
    if (!options || !options.silent) {
      status("Review playback paused.", false);
    }
  }

  function resetReviewState() {
    stopReviewPlayback({ silent: true });
    state.review.bufferRows = [];
    state.review.visibleCount = 0;
    state.review.requestedVisibleCount = 0;
    state.review.lastBufferedId = 0;
    state.review.exhausted = false;
    state.review.fetchPromise = null;
    state.review.zigFetchPromise = null;
    state.review.ottFetchPromise = null;
    state.review.envelopeFetchPromise = null;
    state.review.anchorVisibleCount = 0;
    state.review.anchorTimestampMs = 0;
    state.review.anchorPerfMs = 0;
    state.review.reachedEndAnnounced = false;
    state.review.resolvedStartId = null;
    state.review.resolvedStartTimestamp = null;
    state.review.sessionEndId = null;
    state.review.sessionEndTimestamp = null;
    state.review.fetchingTicks = false;
    state.review.fetchingZig = false;
    state.review.fetchingOtt = false;
    state.review.fetchingEnvelope = false;
    state.review.trueEndReached = false;
    state.review.waitingFor = null;
    state.review.zigRequestedEndId = 0;
    state.review.lastZigRequestAt = 0;
    state.review.ottRequestedEndId = 0;
    state.review.lastOttRequestAt = 0;
    state.review.envelopeRequestedEndId = 0;
    state.review.lastEnvelopeRequestAt = 0;
  }

  function setReviewVisibleCount(nextCount, options) {
    const boundedCount = Math.max(0, Math.min(nextCount, state.review.bufferRows.length));
    state.review.requestedVisibleCount = boundedCount;
    state.review.visibleCount = boundedCount;
    state.rows = state.review.bufferRows.slice(0, boundedCount);
    if (options && options.skipRender) {
      return;
    }
    renderChart({
      preserveCurrentZoom: Boolean(options && options.preserveCurrentZoom),
      resetWindow: Boolean(options && options.resetWindow),
    });
  }

  function reviewPrefetchLimit(config) {
    const speedMultiplier = 1 + (Math.max(0, state.review.playbackSpeed - 1) * 0.25);
    return Math.max(
      REVIEW_PREFETCH_FLOOR,
      Math.min(REVIEW_PREFETCH_CEILING, Math.floor(Math.max(120, Math.floor(config.window / 2)) * speedMultiplier))
    );
  }

  function reviewPrefetchThreshold(config) {
    return Math.max(REVIEW_PREFETCH_THRESHOLD, Math.floor(reviewPrefetchLimit(config) * REVIEW_PREFETCH_RATIO));
  }

  function reviewLastVisibleRow() {
    if (!state.review.visibleCount) {
      return null;
    }
    return state.review.bufferRows[state.review.visibleCount - 1] || null;
  }

  function reviewRemainingPlaybackMs() {
    const visibleRow = reviewLastVisibleRow();
    const bufferedRow = state.review.bufferRows[state.review.bufferRows.length - 1] || null;
    if (!visibleRow || !bufferedRow) {
      return null;
    }
    return Math.max(
      0,
      (bufferedRow.timestampMs - visibleRow.timestampMs) / Math.max(0.25, state.review.playbackSpeed || 1)
    );
  }

  function setReviewPlaybackAnchor(nowMs) {
    const anchorRow = reviewLastVisibleRow() || state.review.bufferRows[0] || null;
    state.review.anchorVisibleCount = Math.max(1, state.review.visibleCount || (anchorRow ? 1 : 0));
    state.review.anchorTimestampMs = anchorRow ? anchorRow.timestampMs : 0;
    state.review.anchorPerfMs = nowMs;
  }

  function reviewVisibleCountForTimestamp(targetTimestampMs) {
    let low = Math.max(0, state.review.anchorVisibleCount - 1);
    let high = state.review.bufferRows.length;
    while (low < high) {
      const middle = Math.floor((low + high) / 2);
      if (state.review.bufferRows[middle].timestampMs <= targetTimestampMs) {
        low = middle + 1;
      } else {
        high = middle;
      }
    }
    return Math.max(state.review.anchorVisibleCount, low);
  }

  function announceReviewEnd() {
    if (state.review.reachedEndAnnounced) {
      return;
    }
    state.review.reachedEndAnnounced = true;
    state.review.trueEndReached = true;
    stopReviewPlayback({ silent: true });
    state.currentRun = "stop";
    setSegment(elements.runToggle, "stop");
    writeQuery(currentConfig());
    status(
      state.review.sessionEndId != null
        ? "Reached end of review range at tick " + state.review.sessionEndId + "."
        : "Reached end of review range.",
      false
    );
    buildMetaText();
  }

  function clearZigState() {
    state.zigRows = {
      micro: new Map(),
      med: new Map(),
      maxi: new Map(),
      macro: new Map(),
    };
    state.zigStatusPayload = null;
    state.zigStatePayload = null;
    state.review.zigFetchPromise = null;
    state.review.fetchingZig = false;
    state.review.zigRequestedEndId = 0;
    state.review.lastZigRequestAt = 0;
  }

  function clearOttState() {
    cancelBacktestOverlayRequest();
    cancelBacktestRunRequest();
    state.ottRows = new Map();
    state.ottTrades = [];
    state.ottRun = null;
    state.ottLastId = 0;
    state.ottStatusPayload = null;
    state.ottOverlayPayload = null;
    state.review.ottRequestedEndId = 0;
    state.review.lastOttRequestAt = 0;
  }

  function clearEnvelopeState() {
    state.envelopeRows = new Map();
    state.envelopeLastId = 0;
    state.envelopeStatusPayload = null;
    state.review.envelopeFetchPromise = null;
    state.review.fetchingEnvelope = false;
    state.review.envelopeRequestedEndId = 0;
    state.review.lastEnvelopeRequestAt = 0;
  }

  function fetchReviewZigChunk(config, options) {
    if (config.mode !== "review" || !shouldLoadZig(config)) {
      return Promise.resolve(null);
    }
    if (state.review.zigFetchPromise) {
      return state.review.zigFetchPromise;
    }
    const requestedAfterId = options && options.afterId != null
      ? options.afterId
      : Math.max(0, state.review.zigRequestedEndId || reviewBufferStartAfterId());
    const effectiveAfterId = Math.max(0, requestedAfterId || 0);
    const targetEndId = options && options.endId != null ? options.endId : state.review.lastBufferedId;
    if (targetEndId != null && effectiveAfterId >= targetEndId) {
      return Promise.resolve(null);
    }
    const nowMs = performance.now();
    if (
      (!options || !options.force)
      && targetEndId != null
      && targetEndId <= state.review.zigRequestedEndId
      && (nowMs - state.review.lastZigRequestAt) < REVIEW_ZIG_RETRY_DELAY_MS
    ) {
      return Promise.resolve(null);
    }
    state.review.zigRequestedEndId = Math.max(state.review.zigRequestedEndId, targetEndId || 0);
    state.review.lastZigRequestAt = nowMs;
    state.review.fetchingZig = true;
    setLoadStatus("zig", "Zig: syncing review chunk...", "info");
    buildMetaText();
    state.review.zigFetchPromise = loadZigNext(effectiveAfterId, config, {
      endId: targetEndId,
    })
      .then((payload) => {
        updateZigLoadStatus(payload || state.zigStatusPayload);
        if (state.rows.length) {
          renderChart({ preserveCurrentZoom: true });
        }
        return payload;
      })
      .catch((error) => {
        setLoadStatus("zig", "Zig: " + (error.message || "matching review chunk failed."), "error");
        throw error;
      })
      .finally(() => {
        state.review.fetchingZig = false;
        state.review.zigFetchPromise = null;
        buildMetaText();
      });
    return state.review.zigFetchPromise;
  }

  function fetchReviewOttChunk(config, options) {
    if (config.mode !== "review" || !shouldLoadOtt(config)) {
      return Promise.resolve(null);
    }
    if (state.review.ottFetchPromise) {
      return state.review.ottFetchPromise;
    }
    const coverage = reviewOttCoverage();
    const requestedAfterId = options && options.afterId != null
      ? options.afterId
      : (coverage.contiguousEndId != null ? coverage.contiguousEndId : reviewBufferStartAfterId());
    const effectiveAfterId = Math.max(0, requestedAfterId || 0);
    const targetEndId = options && options.endId != null ? options.endId : state.review.lastBufferedId;
    if (targetEndId != null && effectiveAfterId >= targetEndId) {
      return Promise.resolve(null);
    }
    const nowMs = performance.now();
    if (
      (!options || !options.force)
      && targetEndId != null
      && targetEndId <= state.review.ottRequestedEndId
      && (nowMs - state.review.lastOttRequestAt) < REVIEW_OTT_RETRY_DELAY_MS
    ) {
      return Promise.resolve(null);
    }
    const limit = Math.max(
      1,
      Math.min(
        REVIEW_PREFETCH_CEILING,
        options && options.limitOverride != null
          ? Math.max(options.limitOverride, targetEndId != null ? (targetEndId - effectiveAfterId) : 0)
          : Math.max(reviewPrefetchLimit(config), targetEndId != null ? (targetEndId - effectiveAfterId) : 0)
      )
    );
    state.review.ottRequestedEndId = Math.max(state.review.ottRequestedEndId, targetEndId || 0);
    state.review.lastOttRequestAt = nowMs;
    state.review.fetchingOtt = true;
    setLoadStatus("ott", "OTT: prefetching synced review chunk...", "info");
    buildMetaText();
    state.review.ottFetchPromise = loadOttNext(effectiveAfterId, config, {
      limitOverride: limit,
      endId: targetEndId,
    })
      .then((payload) => {
        updateOttLoadStatus(payload || state.ottStatusPayload);
        if (state.rows.length) {
          renderChart({ preserveCurrentZoom: true });
        }
        return payload;
      })
      .catch((error) => {
        setLoadStatus("ott", "OTT: " + (error.message || "matching review chunk failed."), "error");
        throw error;
      })
      .finally(() => {
        state.review.fetchingOtt = false;
        state.review.ottFetchPromise = null;
        buildMetaText();
      });
    return state.review.ottFetchPromise;
  }

  function fetchReviewEnvelopeChunk(config, options) {
    if (config.mode !== "review" || !shouldLoadEnvelope(config)) {
      return Promise.resolve(null);
    }
    if (state.review.envelopeFetchPromise) {
      return state.review.envelopeFetchPromise;
    }
    const requestedAfterId = options && options.afterId != null
      ? options.afterId
      : Math.max(0, state.review.envelopeRequestedEndId || reviewBufferStartAfterId());
    const effectiveAfterId = Math.max(0, requestedAfterId || 0);
    const targetEndId = options && options.endId != null ? options.endId : state.review.lastBufferedId;
    if (targetEndId != null && effectiveAfterId >= targetEndId) {
      return Promise.resolve(null);
    }
    const nowMs = performance.now();
    if (
      (!options || !options.force)
      && targetEndId != null
      && targetEndId <= state.review.envelopeRequestedEndId
      && (nowMs - state.review.lastEnvelopeRequestAt) < REVIEW_ENVELOPE_RETRY_DELAY_MS
    ) {
      return Promise.resolve(null);
    }
    const limit = Math.max(
      1,
      Math.min(
        REVIEW_PREFETCH_CEILING,
        options && options.limitOverride != null
          ? Math.max(options.limitOverride, targetEndId != null ? (targetEndId - effectiveAfterId) : 0)
          : Math.max(reviewPrefetchLimit(config), targetEndId != null ? (targetEndId - effectiveAfterId) : 0)
      )
    );
    state.review.envelopeRequestedEndId = Math.max(state.review.envelopeRequestedEndId, targetEndId || 0);
    state.review.lastEnvelopeRequestAt = nowMs;
    state.review.fetchingEnvelope = true;
    setLoadStatus("envelope", "Envelope: syncing review chunk...", "info");
    buildMetaText();
    state.review.envelopeFetchPromise = loadEnvelopeNext(effectiveAfterId, config, {
      limitOverride: limit,
      endId: targetEndId,
    })
      .then((payload) => {
        updateEnvelopeLoadStatus(payload || state.envelopeStatusPayload);
        if (state.rows.length) {
          renderChart({ preserveCurrentZoom: true });
        }
        return payload;
      })
      .catch((error) => {
        setLoadStatus("envelope", "Envelope: " + (error.message || "matching review chunk failed."), "error");
        throw error;
      })
      .finally(() => {
        state.review.fetchingEnvelope = false;
        state.review.envelopeFetchPromise = null;
        buildMetaText();
      });
    return state.review.envelopeFetchPromise;
  }

  function applyZigPayload(payload, reset) {
    if (reset) {
      state.zigRows = {
        micro: new Map(),
        med: new Map(),
        maxi: new Map(),
        macro: new Map(),
      };
      state.review.zigRequestedEndId = 0;
    }
    Object.keys(ZIG_LEVEL_CONFIG).forEach((level) => {
      const levelPayload = payload.levels && payload.levels[level] ? payload.levels[level] : null;
      const rows = levelPayload && Array.isArray(levelPayload.rows) ? levelPayload.rows : [];
      rows.forEach((row) => {
        state.zigRows[level].set(row.id, row);
      });
    });
    state.zigStatusPayload = payload;
    state.zigStatePayload = payload.state || state.zigStatePayload;
    if (payload.range && payload.range.endId != null) {
      state.review.zigRequestedEndId = Math.max(state.review.zigRequestedEndId, payload.range.endId);
    }
    if (payload.endId != null) {
      state.review.zigRequestedEndId = Math.max(state.review.zigRequestedEndId, payload.endId);
    }
  }

  function applyOttPayload(payload, reset) {
    if (reset) {
      state.ottRows = new Map();
      state.ottLastId = 0;
      state.review.ottRequestedEndId = 0;
    }
    (payload.rows || []).forEach((row) => {
      state.ottRows.set(row.tickid, row);
    });
    state.ottLastId = payload.lastId || payload.rows?.[payload.rows.length - 1]?.tickid || state.ottLastId;
    state.ottStatusPayload = payload;
    if (payload.lastId != null) {
      state.review.ottRequestedEndId = Math.max(state.review.ottRequestedEndId, payload.lastId);
    }
  }

  function applyEnvelopePayload(payload, reset) {
    if (reset) {
      state.envelopeRows = new Map();
      state.envelopeLastId = 0;
      state.review.envelopeRequestedEndId = 0;
    }
    (payload.rows || []).forEach((row) => {
      state.envelopeRows.set(row.tickid, row);
    });
    state.envelopeLastId = payload.lastId || payload.rows?.[payload.rows.length - 1]?.tickid || state.envelopeLastId;
    state.envelopeStatusPayload = payload;
    if (payload.lastId != null) {
      state.review.envelopeRequestedEndId = Math.max(state.review.envelopeRequestedEndId, payload.lastId);
    }
  }

  async function fetchJson(url, options, requestOptions) {
    const fetchOptions = options ? { ...options } : {};
    let timeoutId = 0;
    let timedOut = false;
    let timeoutController = null;
    let abortListener = null;

    if (requestOptions && requestOptions.timeoutMs) {
      timeoutController = new AbortController();
      if (requestOptions.signal) {
        if (requestOptions.signal.aborted) {
          timeoutController.abort();
        } else {
          abortListener = () => timeoutController.abort();
          requestOptions.signal.addEventListener("abort", abortListener, { once: true });
        }
      }
      fetchOptions.signal = timeoutController.signal;
      timeoutId = window.setTimeout(() => {
        timedOut = true;
        timeoutController.abort();
      }, requestOptions.timeoutMs);
    } else if (requestOptions && requestOptions.signal) {
      fetchOptions.signal = requestOptions.signal;
    }

    try {
      const response = await fetch(url, fetchOptions);
      const bodyText = await response.text();
      let payload = null;
      if (bodyText) {
        try {
          payload = JSON.parse(bodyText);
        } catch (error) {
          payload = null;
        }
      }
      if (!response.ok) {
        const message = payload && typeof payload === "object"
          ? (payload.detail || payload.message || response.statusText || "Request failed.")
          : (bodyText || response.statusText || "Request failed.");
        throw new Error(message);
      }
      if (payload === null) {
        throw new Error(bodyText || "Expected JSON response.");
      }
      return payload;
    } catch (error) {
      if (timedOut) {
        throw new Error((requestOptions && requestOptions.timeoutMessage) || "Request timed out.");
      }
      throw error;
    } finally {
      if (timeoutId) {
        window.clearTimeout(timeoutId);
      }
      if (requestOptions && requestOptions.signal && abortListener) {
        requestOptions.signal.removeEventListener("abort", abortListener);
      }
    }
  }

  async function loadZigWindow(config, range) {
    if (!shouldLoadZig(config) || !range || range.startId == null || range.endId == null) {
      return null;
    }
    const params = new URLSearchParams({
      startId: String(range.startId),
      endId: String(range.endId),
      levels: enabledZigLevels(config).join(","),
    });
    const payload = await fetchJson("/api/zig/window?" + params.toString());
    applyZigPayload(payload, true);
    return payload;
  }

  async function loadZigNext(afterId, config, options) {
    if (!shouldLoadZig(config)) {
      return null;
    }
    const requestOptions = options || {};
    const params = new URLSearchParams({
      afterId: String(afterId),
      levels: enabledZigLevels(config).join(","),
    });
    if (requestOptions.endId != null) {
      params.set("endId", String(requestOptions.endId));
    }
    const payload = await fetchJson("/api/zig/next?" + params.toString());
    applyZigPayload(payload, false);
    return payload;
  }

  async function loadOttBootstrap(config) {
    const params = new URLSearchParams({
      mode: config.mode,
      window: String(config.window),
      source: config.ottSource,
      signalmode: config.ottSignalMode,
      matype: config.ottMaType,
      length: String(config.ottLength),
      percent: String(config.ottPercent),
    });
    if (config.mode === "review" && config.id) {
      params.set("id", config.id);
      if (state.review.sessionEndId != null) {
        params.set("endId", String(state.review.sessionEndId));
      }
    }
    const payload = await fetchJson("/api/ott/bootstrap?" + params.toString());
    applyOttPayload(payload, true);
    return payload;
  }

  async function loadEnvelopeBootstrap(config) {
    const params = new URLSearchParams({
      mode: config.mode,
      window: String(config.window),
      source: config.envelopeSource,
      length: String(config.envelopeLength),
      bandwidth: String(config.envelopeBandwidth),
      mult: String(config.envelopeMult),
    });
    if (config.mode === "review" && config.id) {
      params.set("id", config.id);
      if (state.review.sessionEndId != null) {
        params.set("endId", String(state.review.sessionEndId));
      }
    }
    const payload = await fetchJson("/api/envelope/bootstrap?" + params.toString());
    applyEnvelopePayload(payload, true);
    return payload;
  }

  async function loadOttNext(afterId, config, options) {
    if (!shouldLoadOtt(config)) {
      return null;
    }
    const requestOptions = typeof options === "number"
      ? { limitOverride: options }
      : (options || {});
    const params = new URLSearchParams({
      afterId: String(afterId),
      limit: String(requestOptions.limitOverride || Math.max(50, Math.min(500, config.window))),
      source: config.ottSource,
      signalmode: config.ottSignalMode,
      matype: config.ottMaType,
      length: String(config.ottLength),
      percent: String(config.ottPercent),
    });
    if (requestOptions.endId != null) {
      params.set("endId", String(requestOptions.endId));
    }
    const payload = await fetchJson("/api/ott/next?" + params.toString());
    applyOttPayload(payload, false);
    return payload;
  }

  async function loadEnvelopeNext(afterId, config, options) {
    if (!shouldLoadEnvelope(config)) {
      return null;
    }
    const requestOptions = typeof options === "number"
      ? { limitOverride: options }
      : (options || {});
    const params = new URLSearchParams({
      afterId: String(afterId),
      limit: String(requestOptions.limitOverride || Math.max(50, Math.min(500, config.window))),
      source: config.envelopeSource,
      length: String(config.envelopeLength),
      bandwidth: String(config.envelopeBandwidth),
      mult: String(config.envelopeMult),
    });
    if (requestOptions.endId != null) {
      params.set("endId", String(requestOptions.endId));
    }
    const payload = await fetchJson("/api/envelope/next?" + params.toString());
    applyEnvelopePayload(payload, false);
    return payload;
  }

  async function runBacktest(config, force) {
    if (config.mode !== "review" || !config.ottTrades) {
      state.ottRun = null;
      state.ottTrades = [];
      state.ottOverlayPayload = null;
      return null;
    }
    cancelBacktestRunRequest();
    const controller = new AbortController();
    state.backtest.runController = controller;
    try {
      const runPayload = await fetchJson("/api/ott/backtest/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          source: config.ottSource,
          matype: config.ottMaType,
          length: config.ottLength,
          percent: config.ottPercent,
          signalmode: config.ottSignalMode,
          rangepreset: config.ottRangePreset,
          force: Boolean(force),
        }),
      }, {
        signal: controller.signal,
        timeoutMs: BACKTEST_RUN_TIMEOUT_MS,
        timeoutMessage: "Backtest is taking too long. The chart stays usable; try loading the overlay again shortly.",
      });
      state.ottRun = runPayload.run;
      return runPayload;
    } finally {
      if (state.backtest.runController === controller) {
        state.backtest.runController = null;
      }
    }
  }

  async function loadBacktestOverlay(config, rangeRows, options) {
    const rows = rangeRows || state.rows;
    if (config.mode !== "review" || !config.ottTrades || !rows.length) {
      state.ottTrades = [];
      state.ottRun = null;
      state.ottOverlayPayload = null;
      setLoadStatus("backtest", config.mode === "review" ? "Backtest: off." : "Backtest: n/a.", "info");
      return null;
    }
    cancelBacktestOverlayRequest();
    const controller = new AbortController();
    const requestToken = state.backtest.overlayRequestToken;
    state.backtest.overlayController = controller;
    if (!options || !options.silentStatus) {
      setLoadStatus("backtest", "Backtest: checking cache...", "info");
    }
    const params = new URLSearchParams({
      source: config.ottSource,
      matype: config.ottMaType,
      length: String(config.ottLength),
      percent: String(config.ottPercent),
      signalmode: config.ottSignalMode,
      rangePreset: config.ottRangePreset,
      startId: String(rows[0].id),
      endId: String(rows[rows.length - 1].id),
    });
    try {
      const overlayPayload = await fetchJson("/api/ott/backtest/overlay?" + params.toString(), null, {
        signal: controller.signal,
        timeoutMs: BACKTEST_OVERLAY_TIMEOUT_MS,
        timeoutMessage: "Backtest cache lookup timed out. The chart stays usable.",
      });
      if (requestToken !== state.backtest.overlayRequestToken) {
        return null;
      }
      if (state.backtest.overlayController === controller) {
        state.backtest.overlayController = null;
      }
      state.ottRun = overlayPayload.run;
      state.ottTrades = (overlayPayload.trades || []).map((trade) => ({
        ...trade,
        entryTsMs: trade.entryTsMs,
        exitTsMs: trade.exitTsMs,
      }));
      state.ottOverlayPayload = overlayPayload;
      updateBacktestLoadStatus(overlayPayload);
      return overlayPayload;
    } catch (error) {
      if (state.backtest.overlayController === controller) {
        state.backtest.overlayController = null;
      }
      if (requestToken !== state.backtest.overlayRequestToken || isAbortError(error)) {
        return null;
      }
      state.ottTrades = [];
      state.ottRun = null;
      state.ottOverlayPayload = null;
      setLoadStatus("backtest", "Backtest: " + (error.message || "overlay unavailable."), "warn");
      throw error;
    }
  }

  async function resolveReviewStart(config) {
    if (config.mode !== "review") {
      return { ...config };
    }
    if (config.reviewStart) {
      const params = new URLSearchParams({
        timestamp: config.reviewStart,
        timezoneName: SYDNEY_TIMEZONE,
      });
      const payload = await fetchJson("/api/live/review-start?" + params.toString());
      elements.tickId.value = String(payload.resolvedId);
      state.review.resolvedStartId = payload.resolvedId;
      state.review.resolvedStartTimestamp = payload.resolvedTimestamp;
      return {
        ...config,
        id: String(payload.resolvedId),
      };
    }
    if (!config.id) {
      throw new Error("Review mode requires a start id or Sydney review start time.");
    }
    state.review.resolvedStartId = Number.parseInt(config.id, 10) || null;
    state.review.resolvedStartTimestamp = null;
    return { ...config };
  }

  async function fetchReviewNextChunk(config) {
    if (state.review.fetchPromise) {
      return state.review.fetchPromise;
    }
    if (state.review.exhausted) {
      return Promise.resolve(null);
    }
    const afterId = state.review.lastBufferedId;
    const endId = state.review.sessionEndId;
    const nextChunkLimit = reviewPrefetchLimit(config);
    const targetEndId = reviewChunkTargetEnd(afterId, nextChunkLimit, endId);
    if (endId != null && afterId >= endId) {
      state.review.exhausted = true;
      buildMetaText();
      return Promise.resolve(null);
    }
    const params = new URLSearchParams({
      afterId: String(afterId),
      limit: String(Math.max(1, targetEndId - afterId)),
    });
    if (targetEndId != null) {
      params.set("endId", String(targetEndId));
    }
    state.review.fetchingTicks = true;
    setLoadStatus("chart", "Chart: prefetching next review chunk...", "info");
    buildMetaText();
    if (shouldLoadOtt(config)) {
      fetchReviewOttChunk(config, {
        endId: targetEndId,
        limitOverride: nextChunkLimit,
      }).catch(() => null);
    }
    state.review.fetchPromise = fetchJson("/api/live/next?" + params.toString())
      .then((payload) => {
        const newRows = payload.rows || [];
        if (!newRows.length) {
          state.review.exhausted = Boolean(payload.endReached || (endId != null && afterId >= endId));
          setLoadStatus(
            "chart",
            state.review.exhausted ? "Chart: review range fully buffered." : "Chart: no additional review rows returned.",
            state.review.exhausted ? "info" : "warn"
          );
          if (state.review.visibleCount >= state.review.bufferRows.length) {
            announceReviewEnd();
          }
          if (shouldLoadOtt(config) && reviewOttCoverage().contiguousAvailableCount < state.review.bufferRows.length) {
            fetchReviewOttChunk(config, { endId: state.review.lastBufferedId }).catch(() => null);
          }
          return payload;
        }

        const seen = new Set(state.review.bufferRows.map((row) => row.id));
        newRows.forEach((row) => {
          if (!seen.has(row.id)) {
            state.review.bufferRows.push(row);
          }
        });
        state.review.lastBufferedId = state.review.bufferRows.length
          ? state.review.bufferRows[state.review.bufferRows.length - 1].id
          : afterId;
        state.review.exhausted = Boolean(payload.endReached || (endId != null && state.review.lastBufferedId >= endId));
        setLoadStatus(
          "chart",
          state.review.exhausted
            ? "Chart: review range fully buffered."
            : "Chart: buffered " + state.review.bufferRows.length + " review row(s).",
          "info"
        );

        if (shouldLoadOtt(config) && reviewOttCoverage().contiguousAvailableCount < state.review.bufferRows.length) {
          fetchReviewOttChunk(config, { endId: state.review.lastBufferedId }).catch(() => null);
        }

        if (shouldLoadEnvelope(config) && reviewEnvelopeCoverage().storedCount < state.review.bufferRows.length) {
          fetchReviewEnvelopeChunk(config, { endId: state.review.lastBufferedId }).catch(() => null);
        }

        if (config.ottTrades) {
          loadBacktestOverlay(config, state.review.bufferRows, { silentStatus: true })
            .then((overlayPayload) => {
              if (overlayPayload) {
                renderChart({ preserveCurrentZoom: true });
              }
            })
            .catch((error) => {
              if (!isAbortError(error)) {
                renderLoadStatus();
              }
            });
        }
        maybePrefetchReview(config);
        buildMetaText();
        return payload;
      })
      .catch((error) => {
        setLoadStatus("chart", "Chart: " + (error.message || "review chunk fetch failed."), "error");
        throw error;
      })
      .finally(() => {
        state.review.fetchingTicks = false;
        state.review.fetchPromise = null;
        buildMetaText();
      });
    return state.review.fetchPromise;
  }

  function maybePrefetchReview(config) {
    const remaining = state.review.bufferRows.length - state.review.visibleCount;
    const remainingPlaybackMs = reviewRemainingPlaybackMs();
    const ottCoverage = reviewOttCoverage();
    const remainingOtt = ottCoverage.contiguousAvailableCount - state.review.visibleCount;
    const remainingOttPlaybackMs = reviewRemainingPlaybackMsToCount(ottCoverage.contiguousAvailableCount);
    const envelopeCoverage = reviewEnvelopeCoverage();
    if (
      !state.review.exhausted &&
      (
        remaining <= reviewPrefetchThreshold(config)
        || (remainingPlaybackMs != null && remainingPlaybackMs <= REVIEW_PREFETCH_MIN_PLAYBACK_MS)
      )
    ) {
      fetchReviewNextChunk(config).catch((error) => {
        status(error.message || "Review fetch failed.", true);
      });
    }
    if (
      shouldLoadZig(config)
      && state.review.zigRequestedEndId < state.review.lastBufferedId
      && (
        remaining <= reviewPrefetchThreshold(config)
        || (remainingPlaybackMs != null && remainingPlaybackMs <= REVIEW_PREFETCH_MIN_PLAYBACK_MS)
      )
    ) {
      fetchReviewZigChunk(config, {
        endId: state.review.lastBufferedId,
      }).catch((error) => {
        status(error.message || "Zig sync failed.", true);
      });
    }
    if (
      requiresReviewOttCoverage(config)
      && ottCoverage.contiguousAvailableCount < state.review.bufferRows.length
      && (
        remainingOtt <= reviewPrefetchThreshold(config)
        || (remainingOttPlaybackMs != null && remainingOttPlaybackMs <= REVIEW_PREFETCH_MIN_PLAYBACK_MS)
      )
    ) {
      fetchReviewOttChunk(config, {
        endId: state.review.lastBufferedId,
        limitOverride: reviewPrefetchLimit(config),
      }).catch((error) => {
        status(error.message || "OTT sync failed.", true);
      });
    }
    if (
      shouldLoadEnvelope(config)
      && envelopeCoverage.storedCount < state.review.bufferRows.length
      && (
        remaining <= reviewPrefetchThreshold(config)
        || (remainingPlaybackMs != null && remainingPlaybackMs <= REVIEW_PREFETCH_MIN_PLAYBACK_MS)
      )
    ) {
      fetchReviewEnvelopeChunk(config, {
        endId: state.review.lastBufferedId,
        limitOverride: reviewPrefetchLimit(config),
      }).catch((error) => {
        status(error.message || "Envelope sync failed.", true);
      });
    }
  }

  function reviewFrame(nowMs) {
    if (state.currentMode !== "review" || state.currentRun !== "run") {
      state.review.rafId = 0;
      return;
    }
    if (!state.review.bufferRows.length) {
      announceReviewEnd();
      return;
    }

    const config = currentConfig();
    const targetTimestampMs = state.review.anchorTimestampMs + ((nowMs - state.review.anchorPerfMs) * state.review.playbackSpeed);
    const requestedVisibleCount = reviewVisibleCountForTimestamp(targetTimestampMs);
    const ottCoverage = reviewOttCoverage();
    const maxVisibleCount = requiresReviewOttCoverage(config)
      ? ottCoverage.contiguousAvailableCount
      : state.review.bufferRows.length;
    const nextVisibleCount = Math.min(requestedVisibleCount, maxVisibleCount);
    if (nextVisibleCount !== state.review.visibleCount) {
      setReviewVisibleCount(nextVisibleCount, { preserveCurrentZoom: false });
      maybePrefetchReview(config);
    }

    const lastBufferedRow = state.review.bufferRows[state.review.bufferRows.length - 1] || null;
    const waitingForOtt = requiresReviewOttCoverage(config) && requestedVisibleCount > maxVisibleCount;
    const waitingForTicks = Boolean(
      !waitingForOtt
      && !state.review.exhausted
      && lastBufferedRow
      && targetTimestampMs >= lastBufferedRow.timestampMs
      && state.review.visibleCount >= state.review.bufferRows.length
    );

    if (waitingForOtt) {
      setReviewWaiting("ott", nowMs);
      maybePrefetchReview(config);
    } else if (waitingForTicks) {
      setReviewWaiting("ticks", nowMs);
      maybePrefetchReview(config);
    } else {
      clearReviewWaiting(nowMs);
    }

    if (state.review.visibleCount >= state.review.bufferRows.length && !state.review.exhausted) {
      maybePrefetchReview(config);
    }

    if (requiresReviewOttCoverage(config) && state.review.visibleCount >= ottCoverage.contiguousAvailableCount) {
      maybePrefetchReview(config);
    }

    if (state.review.visibleCount >= state.review.bufferRows.length && state.review.exhausted) {
      announceReviewEnd();
      return;
    }

    state.review.rafId = requestAnimationFrame(reviewFrame);
  }

  function startReviewPlayback() {
    if (state.currentMode !== "review") {
      return;
    }
    if (!state.review.bufferRows.length) {
      status("No review ticks are available for playback.", true);
      return;
    }
    if (!state.review.visibleCount) {
      const config = currentConfig();
      const initialVisibleCount = requiresReviewOttCoverage(config)
        ? Math.min(reviewOttCoverage().contiguousAvailableCount, 2)
        : Math.min(state.review.bufferRows.length, 2);
      setReviewVisibleCount(initialVisibleCount, { resetWindow: true });
    }
    stopReviewPlayback({ silent: true });
    setReviewPlaybackAnchor(performance.now());
    maybePrefetchReview(currentConfig());
    state.review.rafId = requestAnimationFrame(reviewFrame);
    status("Review playback running at " + state.review.playbackSpeed + "x.", false);
  }

  function connectStream(lastId, windowSize) {
    closeStream();
    if (state.currentRun !== "run" || state.currentMode !== "live") {
      return;
    }
    const params = new URLSearchParams({
      afterId: String(lastId || 0),
      limit: String(Math.max(50, Math.min(500, windowSize))),
    });
    const source = new EventSource("/api/live/stream?" + params.toString());
    state.source = source;

    source.onmessage = (event) => {
      const payload = JSON.parse(event.data);
      if (!payload.rows || !payload.rows.length) {
        return;
      }

      const config = currentConfig();
      const seen = new Set(state.rows.map((row) => row.id));
      const previousLastId = state.rows.length ? state.rows[state.rows.length - 1].id : 0;
      payload.rows.forEach((row) => {
        if (!seen.has(row.id)) {
          state.rows.push(row);
        }
      });

      const maxBuffer = Math.max(windowSize * 5, 5000);
      if (state.rows.length > maxBuffer) {
        state.rows = state.rows.slice(state.rows.length - maxBuffer);
      }

      const overlayRequests = [];
      if (shouldLoadZig(config)) {
        overlayRequests.push(
          loadZigNext(previousLastId, config, { endId: payload.lastId }).catch((error) => {
            status(error.message || "Zig incremental update failed.", true);
            return null;
          })
        );
      }
      if (shouldLoadOtt(config)) {
        overlayRequests.push(
          loadOttNext(previousLastId, config).catch((error) => {
            status(error.message || "OTT incremental update failed.", true);
            return null;
          })
        );
      }
      if (shouldLoadEnvelope(config)) {
        overlayRequests.push(
          loadEnvelopeNext(previousLastId, config).catch((error) => {
            status(error.message || "Envelope incremental update failed.", true);
            return null;
          })
        );
      }

      Promise.all(overlayRequests).then(() => {
        const overlayState = collectOverlayMessages(config);
        if (overlayState.messages.length) {
          status("Streaming " + payload.rowCount + " new row(s). " + overlayState.messages.join(" "), overlayState.hasWarning);
        } else {
          status("Streaming " + payload.rowCount + " new row(s).", false);
        }
      }).finally(() => {
        renderChart({ preserveCurrentZoom: false });
      });
    };

    source.onerror = () => {
      status("Stream interrupted. Reconnecting...", true);
    };
  }

  async function loadData(resetWindow) {
    closeStream();
    stopReviewPlayback({ silent: true });
    resetReviewState();
    clearZigState();
    clearOttState();
    clearEnvelopeState();

    try {
      const config = await resolveReviewStart(currentConfig());
      writeQuery(config);
      initializeLoadStatus(config);
      const params = new URLSearchParams({
        mode: config.mode,
        window: String(config.window),
      });
      if (config.mode === "review" && config.id) {
        params.set("id", config.id);
      }
      const livePayload = await fetchJson("/api/live/bootstrap?" + params.toString());
      const loadedRows = livePayload.rows || [];
      if (config.mode === "review") {
        state.review.bufferRows = loadedRows.slice();
        state.review.visibleCount = Math.min(loadedRows.length, loadedRows.length > 1 ? 2 : loadedRows.length);
        state.review.requestedVisibleCount = state.review.visibleCount;
        state.review.lastBufferedId = loadedRows.length ? loadedRows[loadedRows.length - 1].id : 0;
        state.review.sessionEndId = livePayload.reviewEndId != null ? livePayload.reviewEndId : state.review.lastBufferedId;
        state.review.sessionEndTimestamp = livePayload.reviewEndTimestamp || null;
        state.review.exhausted = Boolean(livePayload.endReached) || (
          state.review.sessionEndId != null && state.review.lastBufferedId >= state.review.sessionEndId
        );
        state.rows = state.review.bufferRows.slice(0, state.review.visibleCount);
      } else {
        state.rows = loadedRows;
      }
      setLoadStatus(
        "chart",
        loadedRows.length ? "Chart: loaded " + Number(livePayload.rowCount || loadedRows.length) + " row(s)." : "Chart: no rows returned.",
        loadedRows.length ? "info" : "warn",
        { silent: true }
      );

      if (shouldLoadZig(config)) {
        try {
          const zigRange = loadedRows.length ? {
            startId: loadedRows[0].id,
            endId: loadedRows[loadedRows.length - 1].id,
          } : null;
          const zigPayload = await loadZigWindow(config, zigRange);
          updateZigLoadStatus(zigPayload);
        } catch (error) {
          clearZigState();
          setLoadStatus("zig", "Zig: " + (error.message || "window load failed."), "error", { silent: true });
        }
      } else {
        setLoadStatus("zig", "Zig: off.", "info", { silent: true });
      }

      if (shouldLoadOtt(config)) {
        try {
          const ottPayload = await loadOttBootstrap(config);
          updateOttLoadStatus(ottPayload);
          syncReviewVisibleRange(config, { skipRender: true });
          state.rows = state.review.bufferRows.slice(0, state.review.visibleCount);
        } catch (error) {
          clearOttState();
          setLoadStatus("ott", "OTT: " + (error.message || "bootstrap failed."), "error", { silent: true });
        }
      } else {
        setLoadStatus("ott", "OTT: off.", "info", { silent: true });
      }

      if (shouldLoadEnvelope(config)) {
        try {
          const envelopePayload = await loadEnvelopeBootstrap(config);
          updateEnvelopeLoadStatus(envelopePayload);
        } catch (error) {
          clearEnvelopeState();
          setLoadStatus("envelope", "Envelope: " + (error.message || "bootstrap failed."), "error", { silent: true });
        }
      } else {
        setLoadStatus("envelope", "Envelope: off.", "info", { silent: true });
      }

      if (config.mode !== "review") {
        setLoadStatus("backtest", "Backtest: n/a.", "info", { silent: true });
      } else if (!config.ottTrades) {
        setLoadStatus("backtest", "Backtest: off.", "info", { silent: true });
      }

      renderChart({ resetWindow: Boolean(resetWindow) });
      renderLoadStatus();

      if (config.mode === "review" && config.ottTrades) {
        loadBacktestOverlay(config, state.review.bufferRows, { silentStatus: true })
          .then((overlayPayload) => {
            if (overlayPayload) {
              renderChart({ preserveCurrentZoom: true });
            }
          })
          .catch((error) => {
            if (!isAbortError(error)) {
              renderLoadStatus();
            }
          });
      } else if (config.mode === "review") {
        renderLoadStatus();
      }
      if (config.run === "run" && config.mode === "live") {
        connectStream(livePayload.lastId || 0, config.window);
      }
      if (config.run === "run" && config.mode === "review") {
        startReviewPlayback();
      }
    } catch (error) {
      setLoadStatus("chart", "Chart: " + (error.message || "bootstrap failed."), "error", { silent: true });
      renderLoadStatus();
    }
  }

  function scheduleChartResize() {
    requestChartResize({ remainingAttempts: CHART_RESIZE_RETRY_LIMIT });
  }

  function bindSegment(container, handler) {
    container.querySelectorAll("button").forEach((button) => {
      button.addEventListener("click", () => handler(button.dataset.value));
    });
  }

  function toggleSeries(seriesKey) {
    const activeKeys = getActiveSeriesKeys();
    if (state.activeSeries[seriesKey] && activeKeys.length === 1) {
      return;
    }
    state.activeSeries[seriesKey] = !state.activeSeries[seriesKey];
    syncSeriesButtons();
    writeQuery(currentConfig());
    renderChart({ preserveCurrentZoom: true });
  }

  function setYesterdaySydneyMorning() {
    const yesterday = new Date(Date.now() - (24 * 60 * 60 * 1000));
    const parts = new Intl.DateTimeFormat("en-CA", {
      timeZone: SYDNEY_TIMEZONE,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    }).formatToParts(yesterday).reduce((accumulator, part) => {
      accumulator[part.type] = part.value;
      return accumulator;
    }, {});
    elements.reviewStart.value = parts.year + "-" + parts.month + "-" + parts.day + "T08:00:00";
  }

  function toggleSidebarSection(sectionKey) {
    if (!Object.prototype.hasOwnProperty.call(state.ui.sections, sectionKey)) {
      return;
    }
    state.ui.sections[sectionKey] = !state.ui.sections[sectionKey];
    syncSidebarState();
    writeQuery(currentConfig());
    scheduleChartResize();
  }

  function toggleSidebar() {
    state.ui.sidebarCollapsed = !state.ui.sidebarCollapsed;
    syncSidebarState();
    writeQuery(currentConfig());
    scheduleChartResize();
  }

  bindSegment(elements.modeToggle, (value) => {
    state.currentMode = value;
    setSegment(elements.modeToggle, value);
    updateReviewControlState();
    if (value !== "live") {
      closeStream();
    }
    if (value !== "review") {
      stopReviewPlayback({ silent: true });
    }
    writeQuery(currentConfig());
    status("Mode updated. Load to refresh data.", false);
  });

  bindSegment(elements.runToggle, (value) => {
    state.currentRun = value;
    setSegment(elements.runToggle, value);
    writeQuery(currentConfig());
    if (value === "stop") {
      closeStream();
      if (state.currentMode === "review") {
        stopReviewPlayback({ silent: true });
        status("Review playback stopped.", false);
      } else {
        status("Streaming stopped.", false);
      }
      return;
    }
    if (state.currentMode === "review") {
      startReviewPlayback();
      return;
    }
    if (state.rows.length) {
      connectStream(state.rows[state.rows.length - 1].id, currentConfig().window);
      status("Streaming resumed.", false);
    }
  });

  bindSegment(elements.reviewSpeedToggle, (value) => {
    state.review.playbackSpeed = sanitizeReviewSpeed(value);
    syncReviewSpeedButtons();
    writeQuery(currentConfig());
    if (state.currentMode === "review" && state.currentRun === "run" && state.review.visibleCount) {
      setReviewPlaybackAnchor(performance.now());
      status("Review playback running at " + state.review.playbackSpeed + "x.", false);
    }
  });

  [
    elements.zigMicroToggle,
    elements.zigMedToggle,
    elements.zigMaxiToggle,
    elements.zigMacroToggle,
  ].forEach((control) => {
    control.addEventListener("change", () => {
      const config = currentConfig();
      writeQuery(config);
      clearZigState();
      setLoadStatus(
        "zig",
        shouldLoadZig(config) ? "Zig: settings changed. Click Load." : "Zig: off.",
        "info"
      );
      renderChart({ preserveCurrentZoom: true });
    });
  });

  bindSegment(elements.zigViewToggle, (value) => {
    setSegment(elements.zigViewToggle, value);
    const config = currentConfig();
    writeQuery(config);
    renderChart({ preserveCurrentZoom: true });
  });

  bindSegment(elements.ottToggle, (value) => {
    setSegment(elements.ottToggle, value);
    const config = currentConfig();
    writeQuery(config);
    if (!shouldLoadOtt(config)) {
      setLoadStatus("ott", "OTT: off.", "info");
    } else if (state.ottStatusPayload) {
      updateOttLoadStatus(state.ottStatusPayload);
    }
    syncReviewVisibleRange(config, { preserveCurrentZoom: true, skipRender: true });
    renderChart({ preserveCurrentZoom: true });
  });

  bindSegment(elements.envelopeToggle, (value) => {
    setSegment(elements.envelopeToggle, value);
    const config = currentConfig();
    writeQuery(config);
    if (!shouldLoadEnvelope(config)) {
      setLoadStatus("envelope", "Envelope: off.", "info");
    } else if (state.envelopeStatusPayload) {
      updateEnvelopeLoadStatus(state.envelopeStatusPayload);
    } else {
      setLoadStatus("envelope", "Envelope: click Load to fetch.", "info");
    }
    renderChart({ preserveCurrentZoom: true });
  });

  elements.seriesSelector.querySelectorAll("button").forEach((button) => {
    button.addEventListener("click", () => toggleSeries(button.dataset.series));
  });

  [
    elements.ottSupportToggle,
    elements.ottMarkersToggle,
    elements.ottTradesToggle,
    elements.ottHighlightToggle,
    elements.ottSignalMode,
  ].forEach((control) => {
    control.addEventListener("change", () => {
      const config = currentConfig();
      writeQuery(config);
      syncReviewVisibleRange(config, { preserveCurrentZoom: true, skipRender: true });
      renderChart({ preserveCurrentZoom: true });
      if (control === elements.ottTradesToggle || control === elements.ottSignalMode) {
        loadBacktestOverlay(
          config,
          state.currentMode === "review" ? state.review.bufferRows : state.rows
        )
          .then((overlayPayload) => {
            if (overlayPayload) {
              renderChart({ preserveCurrentZoom: true });
            } else {
              renderLoadStatus();
            }
          })
          .catch((error) => {
            if (!isAbortError(error)) {
              renderLoadStatus();
            }
          });
      } else {
        if (!config.ottTrades && config.mode === "review") {
          setLoadStatus("backtest", "Backtest: off.", "info");
        }
        if (!shouldLoadOtt(config)) {
          setLoadStatus("ott", "OTT: off.", "info");
        } else if (state.ottStatusPayload) {
          updateOttLoadStatus(state.ottStatusPayload);
        } else {
          renderLoadStatus();
        }
      }
    });
  });

  [
    elements.ottSource,
    elements.ottMaType,
    elements.ottLength,
    elements.ottPercent,
    elements.ottRangePreset,
    elements.reviewStart,
    elements.tickId,
    elements.windowSize,
  ].forEach((control) => {
    control.addEventListener("change", () => {
      if (control === elements.reviewStart && elements.reviewStart.value) {
        state.currentMode = "review";
        setSegment(elements.modeToggle, "review");
        updateReviewControlState();
      }
      writeQuery(currentConfig());
    });
  });

  [
    elements.envelopeSource,
    elements.envelopeLength,
    elements.envelopeBandwidth,
    elements.envelopeMult,
  ].forEach((control) => {
    control.addEventListener("change", () => {
      const config = currentConfig();
      writeQuery(config);
      clearEnvelopeState();
      setLoadStatus(
        "envelope",
        shouldLoadEnvelope(config) ? "Envelope: settings changed. Click Load." : "Envelope: off.",
        "info"
      );
      renderChart({ preserveCurrentZoom: true });
    });
  });

  elements.reviewYesterdayButton.addEventListener("click", () => {
    state.currentMode = "review";
    setSegment(elements.modeToggle, "review");
    updateReviewControlState();
    setYesterdaySydneyMorning();
    writeQuery(currentConfig());
  });

  elements.sidebarToggle.addEventListener("click", toggleSidebar);
  elements.controlSectionToggle.addEventListener("click", () => toggleSidebarSection("control"));
  elements.zigSectionToggle.addEventListener("click", () => toggleSidebarSection("zig"));
  elements.envelopeSectionToggle.addEventListener("click", () => toggleSidebarSection("envelope"));
  elements.ottSectionToggle.addEventListener("click", () => toggleSidebarSection("ott"));

  elements.applyButton.addEventListener("click", () => loadData(true));
  elements.runOttBacktestButton.addEventListener("click", async () => {
    const config = currentConfig();
    writeQuery(config);
    if (config.mode !== "review") {
      setLoadStatus("backtest", "Backtest: switch to review mode to run it.", "info");
      return;
    }
    if (!config.ottTrades) {
      setLoadStatus("backtest", "Backtest: enable backtest trades first.", "info");
      return;
    }
    elements.runOttBacktestButton.disabled = true;
    try {
      setLoadStatus("backtest", "Backtest: running...", "info");
      const runPayload = await runBacktest(config, true);
      setLoadStatus(
        "backtest",
        runPayload && runPayload.run && runPayload.run.reused
          ? "Backtest: cached run reused. Loading overlay..."
          : "Backtest: run complete. Loading overlay...",
        "info"
      );
      const overlayPayload = await loadBacktestOverlay(
        config,
        state.currentMode === "review" ? state.review.bufferRows : state.rows,
        { silentStatus: true }
      );
      if (overlayPayload) {
        renderChart({ preserveCurrentZoom: true });
      } else {
        renderLoadStatus();
      }
    } catch (error) {
      if (!isAbortError(error)) {
        setLoadStatus("backtest", "Backtest: " + (error.message || "run failed."), "error");
      }
    } finally {
      elements.runOttBacktestButton.disabled = false;
    }
  });

  const initial = parseQuery();
  syncControls(initial);
  if (!initial.reviewStart && initial.mode === "review" && !initial.id) {
    setYesterdaySydneyMorning();
    writeQuery(currentConfig());
  }
  loadData(true);
}());
