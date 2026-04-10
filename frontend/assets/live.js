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
  const TRADE_POLL_INTERVAL_MS = 8000;
  const TRADE_HISTORY_LIMIT = 40;
  const TRADE_DEFAULT_LOT_SIZE = 0.01;
  const TRADE_REVIEW_DEFAULT_TICKS_BEFORE = 300;
  const TRADE_REVIEW_DEFAULT_TICKS_AFTER = 300;
  const TRADE_MARKER_COLORS = {
    buyEntry: "#7ef0c7",
    sellEntry: "#ff9fb2",
    buyExit: "#38d39f",
    sellExit: "#ff6b88",
    pending: "#ffc857",
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
    trade: {
      authConfigured: true,
      authError: null,
      brokerConfigured: false,
      brokerStatus: null,
      lastLoggedErrorKey: null,
      authenticated: false,
      username: null,
      loginBusy: false,
      actionBusy: false,
      historyAvailable: true,
      loading: false,
      pollTimer: 0,
      positions: [],
      pendingOrders: [],
      trades: [],
      deals: [],
      lastLoadedAtMs: null,
      volumeInfo: null,
      activeOrderSide: null,
      activePositionId: null,
      pendingProtectionEdits: {},
      selectedHistoricalTradeOverlay: null,
    },
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
    tradePanel: document.getElementById("tradePanel"),
    tradeStatusLine: document.getElementById("tradeStatusLine"),
    tradeAuthPill: document.getElementById("tradeAuthPill"),
    tradeLoginForm: document.getElementById("tradeLoginForm"),
    tradeUsername: document.getElementById("tradeUsername"),
    tradePassword: document.getElementById("tradePassword"),
    tradeLoginButton: document.getElementById("tradeLoginButton"),
    tradeControls: document.getElementById("tradeControls"),
    tradeSessionSummary: document.getElementById("tradeSessionSummary"),
    tradeBrokerSummary: document.getElementById("tradeBrokerSummary"),
    tradeLogoutButton: document.getElementById("tradeLogoutButton"),
    tradePreparedLotSize: document.getElementById("tradePreparedLotSize"),
    tradePreparedStopLoss: document.getElementById("tradePreparedStopLoss"),
    tradePreparedTakeProfit: document.getElementById("tradePreparedTakeProfit"),
    tradePreparedPresets: document.getElementById("tradePreparedPresets"),
    tradePreparedVolumeInfo: document.getElementById("tradePreparedVolumeInfo"),
    tradePreparedSummary: document.getElementById("tradePreparedSummary"),
    tradeOpenList: document.getElementById("tradeOpenList"),
    tradePendingList: document.getElementById("tradePendingList"),
    tradeReviewSection: document.getElementById("tradeReviewSection"),
    tradeReviewHint: document.getElementById("tradeReviewHint"),
    tradeReviewTicksBefore: document.getElementById("tradeReviewTicksBefore"),
    tradeReviewTicksAfter: document.getElementById("tradeReviewTicksAfter"),
    tradeReviewSummary: document.getElementById("tradeReviewSummary"),
    tradeHistoryList: document.getElementById("tradeHistoryList"),
    chartTradeEntry: document.getElementById("chartTradeEntry"),
    chartTradeBuyButton: document.getElementById("chartTradeBuyButton"),
    chartTradeSellButton: document.getElementById("chartTradeSellButton"),
    chartTradeHint: document.getElementById("chartTradeHint"),
    chartPositionEditor: document.getElementById("chartPositionEditor"),
    chartPositionTitle: document.getElementById("chartPositionTitle"),
    chartPositionCopy: document.getElementById("chartPositionCopy"),
    chartPositionStopLoss: document.getElementById("chartPositionStopLoss"),
    chartPositionTakeProfit: document.getElementById("chartPositionTakeProfit"),
    chartPositionPendingState: document.getElementById("chartPositionPendingState"),
    chartPositionConfirmButton: document.getElementById("chartPositionConfirmButton"),
    chartPositionCancelButton: document.getElementById("chartPositionCancelButton"),
  };

  function sanitizeWindowValue(rawValue) {
    return Math.max(1, Math.min(MAX_WINDOW, Number.parseInt(rawValue || String(DEFAULTS.window), 10) || DEFAULTS.window));
  }

  function clampTradeReviewTicks(rawValue, fallback) {
    return Math.max(0, Math.min(MAX_WINDOW, Number.parseInt(rawValue || String(fallback), 10) || fallback));
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

  function tradeStatus(message, isError) {
    if (!elements.tradeStatusLine) {
      return;
    }
    elements.tradeStatusLine.textContent = message;
    elements.tradeStatusLine.classList.toggle("error", Boolean(isError));
    elements.tradeStatusLine.classList.toggle("success", Boolean(!isError));
  }

  function tradePayloadDetail(payload) {
    return payload?.detail && typeof payload.detail === "object" ? payload.detail : null;
  }

  function brokerStatusFromPayload(payload) {
    const detail = tradePayloadDetail(payload);
    const broker = payload?.broker || detail?.broker || null;
    const brokerConfigured = Boolean(
      payload?.brokerConfigured
      ?? detail?.brokerConfigured
      ?? payload?.configured
      ?? detail?.configured
      ?? broker?.configured
    );
    const stateValue = typeof broker?.state === "string" && broker.state
      ? broker.state
      : (brokerConfigured ? "unavailable" : "not_configured");
    const reason = typeof broker?.reason === "string" && broker.reason ? broker.reason : null;
    return {
      configured: brokerConfigured,
      connected: Boolean(broker?.connected),
      authenticated: Boolean(broker?.authenticated),
      ready: Boolean(broker?.ready),
      state: stateValue,
      reason,
      code: typeof broker?.code === "string" && broker.code ? broker.code : null,
      symbol: typeof broker?.symbol === "string" && broker.symbol ? broker.symbol : null,
      symbolId: broker?.symbolId ?? null,
      connectionType: typeof broker?.connectionType === "string" && broker.connectionType ? broker.connectionType : null,
      lastError: typeof broker?.lastError === "string" && broker.lastError ? broker.lastError : reason,
    };
  }

  function setTradeBusy(busy) {
    const disabled = Boolean(busy);
    state.trade.actionBusy = disabled;
    [
      elements.tradeLogoutButton,
      elements.tradeLoginButton,
      elements.tradePreparedLotSize,
      elements.tradePreparedStopLoss,
      elements.tradePreparedTakeProfit,
      elements.chartTradeBuyButton,
      elements.chartTradeSellButton,
      elements.chartPositionConfirmButton,
      elements.chartPositionCancelButton,
    ].forEach((button) => {
      if (button) {
        button.disabled = disabled;
      }
    });
    if (elements.tradePreparedPresets) {
      elements.tradePreparedPresets.querySelectorAll("button").forEach((button) => {
        button.disabled = disabled;
      });
    }
    renderTradeEntryOverlay();
    renderBrokerSummary();
    renderPositionEditor();
  }

  function clearTradeRuntimeState() {
    stopTradePolling();
    state.trade.positions = [];
    state.trade.pendingOrders = [];
    state.trade.trades = [];
    state.trade.deals = [];
    state.trade.historyAvailable = true;
    state.trade.volumeInfo = null;
    state.trade.lastLoadedAtMs = null;
    state.trade.activeOrderSide = null;
    state.trade.activePositionId = null;
    state.trade.pendingProtectionEdits = {};
    state.trade.selectedHistoricalTradeOverlay = null;
  }

  function applyTradeSessionPayload(payload) {
    state.trade.authConfigured = payload?.authConfigured !== false;
    state.trade.authError = payload?.error || null;
    state.trade.brokerStatus = brokerStatusFromPayload(payload);
    state.trade.brokerConfigured = Boolean(state.trade.brokerStatus?.configured);
    state.trade.authenticated = state.trade.authConfigured && Boolean(payload?.authenticated);
    state.trade.username = state.trade.authenticated ? (payload?.username || null) : null;
    if (!state.trade.authenticated) {
      clearTradeRuntimeState();
    }
    elements.tradeLoginForm.hidden = state.trade.authenticated || !state.trade.authConfigured;
    elements.tradeControls.hidden = !state.trade.authenticated;
    elements.tradeLogoutButton.hidden = !state.trade.authenticated || !state.trade.authConfigured;
    elements.tradeUsername.disabled = !state.trade.authConfigured || state.trade.loginBusy || state.trade.actionBusy;
    elements.tradePassword.disabled = !state.trade.authConfigured || state.trade.loginBusy || state.trade.actionBusy;
    elements.tradeLoginButton.disabled = !state.trade.authConfigured || state.trade.loginBusy || state.trade.actionBusy;
    elements.tradeAuthPill.classList.toggle("ready", state.trade.authenticated);
    elements.tradeAuthPill.textContent = state.trade.authenticated
      ? ("Ready " + (state.trade.username || ""))
      : (state.trade.authConfigured ? "Locked" : "Unavailable");
    renderTradeEntryOverlay();
    renderPositionEditor();
    queueOverlayRender();
  }

  function tradeConsole(method, url, error) {
    const message = String(error?.message || "");
    const expected =
      error?.code === "TRADE_AUTH_NOT_CONFIGURED"
      || message.toLowerCase().includes("trade login required");
    if (expected || !window.console) {
      return;
    }
    const details = {
      status: error?.status ?? null,
      code: error?.code ?? null,
      message,
    };
    const key = [method, url, details.status, details.code, details.message].join("|");
    if (state.trade.lastLoggedErrorKey === key) {
      return;
    }
    state.trade.lastLoggedErrorKey = key;
    if (typeof window.console.error === "function") {
      window.console.error("[trade] " + method + " " + url + " failed", details);
    }
  }

  function tradeErrorMessage(payload) {
    const detail = tradePayloadDetail(payload);
    if (typeof payload?.message === "string" && payload.message) {
      return payload.message;
    }
    if (typeof payload?.detail === "string" && payload.detail) {
      return payload.detail;
    }
    if (typeof detail?.message === "string" && detail.message) {
      return detail.message;
    }
    if (typeof payload?.error === "string" && payload.error) {
      return payload.error;
    }
    if (typeof detail?.error === "string" && detail.error) {
      return detail.error;
    }
    return "Request failed.";
  }

  function parseOptionalPriceInput(element) {
    const raw = (element?.value || "").trim();
    if (!raw) {
      return null;
    }
    const number = Number(raw);
    if (!Number.isFinite(number) || number <= 0) {
      throw new Error("Price values must be greater than zero.");
    }
    return number;
  }

  function formatSignedPnl(value) {
    const number = Number(value);
    if (!Number.isFinite(number)) {
      return "-";
    }
    const fixed = number.toFixed(2);
    return number > 0 ? "+" + fixed : fixed;
  }

  function formatCompactNumber(value, digits) {
    const number = Number(value);
    if (!Number.isFinite(number)) {
      return "-";
    }
    return number.toFixed(digits).replace(/\.?0+$/, "");
  }

  function formatLots(value) {
    return formatCompactNumber(value, 4);
  }

  function currentTradeVolumeInfo() {
    return state.trade.volumeInfo || { defaultLotSize: TRADE_DEFAULT_LOT_SIZE };
  }

  function tradeLotSizeUnits() {
    return Number(currentTradeVolumeInfo().lotSize || 0);
  }

  function volumeToLots(volume) {
    const lotSize = tradeLotSizeUnits();
    const units = Number(volume);
    if (!Number.isFinite(units) || !Number.isFinite(lotSize) || lotSize <= 0) {
      return null;
    }
    return units / lotSize;
  }

  function tradeBrokerLimits() {
    const info = currentTradeVolumeInfo();
    const defaultLotSize = Number(info.defaultLotSize || TRADE_DEFAULT_LOT_SIZE);
    const minLotSize = Number(info.minLotSize || defaultLotSize || TRADE_DEFAULT_LOT_SIZE);
    const brokerStep = Number(info.lotStep || TRADE_DEFAULT_LOT_SIZE);
    return {
      defaultLotSize: Number.isFinite(defaultLotSize) && defaultLotSize > 0 ? defaultLotSize : TRADE_DEFAULT_LOT_SIZE,
      minLotSize: Number.isFinite(minLotSize) && minLotSize > 0 ? minLotSize : TRADE_DEFAULT_LOT_SIZE,
      lotStep: Number.isFinite(brokerStep) && brokerStep > TRADE_DEFAULT_LOT_SIZE ? brokerStep : TRADE_DEFAULT_LOT_SIZE,
    };
  }

  function lotStepAligned(lotSize, step) {
    const size = Number(lotSize);
    const stepSize = Number(step);
    if (!Number.isFinite(size) || !Number.isFinite(stepSize) || stepSize <= 0) {
      return false;
    }
    const ratio = size / stepSize;
    return Math.abs(ratio - Math.round(ratio)) < 0.000001;
  }

  function parseOptionalPreparedPriceInput(element) {
    const raw = (element?.value || "").trim();
    if (!raw) {
      return null;
    }
    const number = Number(raw);
    if (!Number.isFinite(number) || number <= 0) {
      throw new Error("Prepared SL/TP values must be greater than zero.");
    }
    return number;
  }

  function readPreparedTradeInputs() {
    const rawLotSize = (elements.tradePreparedLotSize?.value || "").trim();
    const lotSize = Number(rawLotSize);
    if (!Number.isFinite(lotSize) || lotSize <= 0) {
      throw new Error("Lot size must be greater than zero.");
    }
    return {
      lotSize,
      stopLoss: parseOptionalPreparedPriceInput(elements.tradePreparedStopLoss),
      takeProfit: parseOptionalPreparedPriceInput(elements.tradePreparedTakeProfit),
    };
  }

  function brokerUnavailableReason() {
    const broker = state.trade.brokerStatus;
    if (!state.trade.brokerConfigured || broker?.state === "not_configured") {
      return broker?.reason || "Broker integration is not configured.";
    }
    if (broker?.reason) {
      return broker.reason;
    }
    if (state.trade.loading) {
      return "Loading broker state...";
    }
    return "Broker state unavailable.";
  }

  function preparedTradeState() {
    const inputs = { lotSize: null, stopLoss: null, takeProfit: null };
    try {
      Object.assign(inputs, readPreparedTradeInputs());
    } catch (error) {
      return { ready: false, reason: error.message || "Prepared trade inputs are invalid.", ...inputs };
    }
    const limits = tradeBrokerLimits();
    if (!state.trade.authConfigured) {
      return { ready: false, reason: "Trade login is not configured on the server.", ...inputs };
    }
    if (!state.trade.authenticated) {
      return { ready: false, reason: "Login required.", ...inputs };
    }
    if (!state.trade.brokerConfigured) {
      return { ready: false, reason: brokerUnavailableReason(), ...inputs };
    }
    if (!state.trade.brokerStatus?.ready || !state.trade.volumeInfo || !state.trade.lastLoadedAtMs) {
      return { ready: false, reason: brokerUnavailableReason(), ...inputs };
    }
    if (inputs.lotSize < limits.minLotSize) {
      return { ready: false, reason: "Lot size must be at least " + formatLots(limits.minLotSize) + " lot.", ...inputs };
    }
    if (!lotStepAligned(inputs.lotSize, limits.lotStep)) {
      return { ready: false, reason: "Lot size must use " + formatLots(limits.lotStep) + " lot steps.", ...inputs };
    }
    return { ready: true, reason: "", ...inputs };
  }

  function positionLots(position) {
    const direct = Number(position?.volumeLots);
    if (Number.isFinite(direct)) {
      return direct;
    }
    return volumeToLots(position?.volume);
  }

  function formatTradeVolume(volume, lots) {
    const lotValue = Number.isFinite(Number(lots)) ? formatLots(lots) + " lot" : null;
    const unitValue = Number.isFinite(Number(volume)) ? String(volume) + " u" : null;
    return [lotValue, unitValue].filter(Boolean).join(" | ") || "-";
  }

  function sanitizeEditableProtectionValue(rawValue, fallbackValue) {
    const raw = String(rawValue || "").trim();
    if (!raw) {
      return fallbackValue == null ? null : Number(fallbackValue);
    }
    const number = Number(raw);
    if (!Number.isFinite(number) || number <= 0) {
      throw new Error("Price values must be greater than zero.");
    }
    return number;
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

  function tooltipSection(title, rows) {
    const content = rows.filter(Boolean).join("");
    if (!content) {
      return "";
    }
    return "<div class=\"chart-tip-section\"><div class=\"chart-tip-title\">" + escapeHtml(title) + "</div>" + content + "</div>";
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

  function rowTimestampMs(row) {
    if (!row) {
      return null;
    }
    const direct = Number(row.timestampMs);
    if (Number.isFinite(direct)) {
      return direct;
    }
    const fallback = Date.parse(row.timestamp);
    return Number.isFinite(fallback) ? fallback : null;
  }

  function tickIdForTimestampMs(timestampMs) {
    const target = Number(timestampMs);
    if (!Number.isFinite(target) || !state.rows.length) {
      return null;
    }
    let best = null;
    let bestDelta = Number.POSITIVE_INFINITY;
    state.rows.forEach((row) => {
      const rowMs = rowTimestampMs(row);
      if (!Number.isFinite(rowMs)) {
        return;
      }
      const delta = Math.abs(rowMs - target);
      if (delta < bestDelta) {
        bestDelta = delta;
        best = row;
      }
    });
    return best ? Number(best.id) : null;
  }

  function activePositionById(positionId) {
    const id = Number(positionId);
    return state.trade.positions.find((item) => Number(item.positionId) === id) || null;
  }

  function activeTradePosition() {
    return activePositionById(state.trade.activePositionId);
  }

  function samePriceValue(left, right) {
    if (left == null && right == null) {
      return true;
    }
    const leftNumber = Number(left);
    const rightNumber = Number(right);
    if (!Number.isFinite(leftNumber) || !Number.isFinite(rightNumber)) {
      return false;
    }
    return Math.abs(leftNumber - rightNumber) < 0.0000001;
  }

  function pendingProtectionForPosition(position) {
    const pending = state.trade.pendingProtectionEdits[String(position.positionId)] || {};
    const stopLoss = Object.prototype.hasOwnProperty.call(pending, "stopLoss") ? pending.stopLoss : (position.stopLoss != null ? Number(position.stopLoss) : null);
    const takeProfit = Object.prototype.hasOwnProperty.call(pending, "takeProfit") ? pending.takeProfit : (position.takeProfit != null ? Number(position.takeProfit) : null);
    const stopChanged = !samePriceValue(stopLoss, position.stopLoss);
    const takeChanged = !samePriceValue(takeProfit, position.takeProfit);
    return {
      stopLoss,
      takeProfit,
      stopChanged,
      takeChanged,
      hasChanges: stopChanged || takeChanged,
    };
  }

  function discardPendingProtection(positionId) {
    if (positionId == null) {
      state.trade.pendingProtectionEdits = {};
    } else {
      delete state.trade.pendingProtectionEdits[String(positionId)];
    }
    renderPositionEditor();
    queueOverlayRender();
  }

  function setActiveTradePosition(positionId) {
    const position = activePositionById(positionId);
    state.trade.activePositionId = position ? Number(position.positionId) : null;
    renderPositionEditor();
    queueOverlayRender();
  }

  function setPendingProtectionValue(positionId, key, value) {
    const position = activePositionById(positionId);
    if (!position || (key !== "stopLoss" && key !== "takeProfit")) {
      return;
    }
    const pendingKey = String(position.positionId);
    const current = { ...(state.trade.pendingProtectionEdits[pendingKey] || {}) };
    if (samePriceValue(value, position[key])) {
      delete current[key];
    } else {
      current[key] = value == null ? null : Number(value);
    }
    if (!Object.keys(current).length) {
      delete state.trade.pendingProtectionEdits[pendingKey];
    } else {
      state.trade.pendingProtectionEdits[pendingKey] = current;
    }
    state.trade.activePositionId = Number(position.positionId);
    renderPositionEditor();
    queueOverlayRender();
  }

  function syncTradeSelection() {
    const openIds = new Set(state.trade.positions.map((item) => Number(item.positionId)));
    Object.keys(state.trade.pendingProtectionEdits).forEach((positionId) => {
      if (!openIds.has(Number(positionId))) {
        delete state.trade.pendingProtectionEdits[positionId];
      }
    });
    if (!state.trade.positions.length) {
      state.trade.activePositionId = null;
      return;
    }
    if (!openIds.has(Number(state.trade.activePositionId))) {
      state.trade.activePositionId = Number(state.trade.positions[0].positionId);
    }
  }

  function syncPreparedTradeInputs() {
    const limits = tradeBrokerLimits();
    if (elements.tradePreparedLotSize) {
      elements.tradePreparedLotSize.min = String(limits.minLotSize);
      elements.tradePreparedLotSize.step = String(limits.lotStep);
      if (!(Number(elements.tradePreparedLotSize.value) > 0)) {
        elements.tradePreparedLotSize.value = formatLots(limits.defaultLotSize);
      }
    }
    if (elements.tradePreparedVolumeInfo) {
      elements.tradePreparedVolumeInfo.textContent = "Broker min " + formatLots(limits.minLotSize) + " lot | step " + formatLots(limits.lotStep) + " lot.";
    }
  }

  function currentTradeReferencePrice(position) {
    const currentTick = Number(state.rangeLastId);
    const currentRow = rowAtTickId(currentTick);
    const livePrice = currentRow ? Number(currentRow.mid) : NaN;
    if (Number.isFinite(livePrice) && livePrice > 0) {
      return livePrice;
    }
    const entryPrice = Number(position?.entryPrice);
    return Number.isFinite(entryPrice) && entryPrice > 0 ? entryPrice : null;
  }

  function protectionKeyForDrop(position, targetPrice) {
    const price = Number(targetPrice);
    const referencePrice = Number(currentTradeReferencePrice(position));
    if (!position || !Number.isFinite(price) || price <= 0 || !Number.isFinite(referencePrice) || referencePrice <= 0) {
      return null;
    }
    if (position.side === "sell") {
      return price >= referencePrice ? "stopLoss" : "takeProfit";
    }
    return price <= referencePrice ? "stopLoss" : "takeProfit";
  }

  function renderPreparedTradeSummary() {
    if (!elements.tradePreparedSummary) {
      return;
    }
    const prepared = preparedTradeState();
    const parts = [
      "Prepared " + (prepared.lotSize != null && Number.isFinite(Number(prepared.lotSize)) ? formatLots(prepared.lotSize) : formatLots(tradeBrokerLimits().defaultLotSize)) + " lot",
      "SL " + (prepared.stopLoss != null ? formatPrice(prepared.stopLoss) : "none"),
      "TP " + (prepared.takeProfit != null ? formatPrice(prepared.takeProfit) : "none"),
    ];
    elements.tradePreparedSummary.textContent = prepared.ready
      ? parts.join(" | ")
      : parts.join(" | ") + " | " + prepared.reason;
  }

  function renderBrokerSummary() {
    if (!elements.tradeBrokerSummary) {
      return;
    }
    const broker = state.trade.brokerStatus;
    if (!state.trade.authConfigured) {
      elements.tradeBrokerSummary.textContent = "Broker unavailable until trade login is configured.";
      return;
    }
    if (!state.trade.authenticated) {
      elements.tradeBrokerSummary.textContent = "Broker status will load after trade login.";
      return;
    }
    if (!broker || (!broker.ready && !broker.reason && !state.trade.lastLoadedAtMs)) {
      elements.tradeBrokerSummary.textContent = "Broker status loading.";
      return;
    }
    if (broker.ready) {
      const symbol = broker.symbol || "-";
      const mode = broker.connectionType ? broker.connectionType.toUpperCase() : "LIVE";
      elements.tradeBrokerSummary.textContent = "Broker ready | " + symbol + " | " + mode + ".";
      return;
    }
    elements.tradeBrokerSummary.textContent = "Broker unavailable | " + brokerUnavailableReason();
  }

  function renderTradeEntryOverlay() {
    if (!elements.chartTradeEntry) {
      return;
    }
    const liveMode = currentConfig().mode === "live";
    elements.chartTradeEntry.hidden = !liveMode;
    if (!liveMode) {
      return;
    }
    const prepared = preparedTradeState();
    const authConfigured = state.trade.authConfigured;
    const busy = state.trade.actionBusy;
    if (elements.chartTradeBuyButton) {
      elements.chartTradeBuyButton.hidden = !authConfigured;
      elements.chartTradeBuyButton.disabled = !authConfigured || !prepared.ready || busy;
      elements.chartTradeBuyButton.textContent = busy && state.trade.activeOrderSide === "buy" ? "Buying..." : "Buy Market";
    }
    if (elements.chartTradeSellButton) {
      elements.chartTradeSellButton.hidden = !authConfigured;
      elements.chartTradeSellButton.disabled = !authConfigured || !prepared.ready || busy;
      elements.chartTradeSellButton.textContent = busy && state.trade.activeOrderSide === "sell" ? "Selling..." : "Sell Market";
    }
    if (elements.chartTradeHint) {
      elements.chartTradeHint.textContent = busy && state.trade.activeOrderSide
        ? ("Sending " + state.trade.activeOrderSide + " | " + formatLots(prepared.lotSize) + " lot | SL " + (prepared.stopLoss != null ? formatPrice(prepared.stopLoss) : "none") + " | TP " + (prepared.takeProfit != null ? formatPrice(prepared.takeProfit) : "none"))
        : (prepared.ready
          ? ("Using " + formatLots(prepared.lotSize) + " lot | SL " + (prepared.stopLoss != null ? formatPrice(prepared.stopLoss) : "none") + " | TP " + (prepared.takeProfit != null ? formatPrice(prepared.takeProfit) : "none"))
          : prepared.reason);
    }
    renderPreparedTradeSummary();
    renderBrokerSummary();
  }

  function renderPositionEditor() {
    if (!elements.chartPositionEditor) {
      return;
    }
    syncTradeSelection();
    const position = activeTradePosition();
    const visible = currentConfig().mode === "live" && state.trade.authenticated && Boolean(position);
    elements.chartPositionEditor.hidden = !visible;
    if (!visible) {
      elements.chartPositionEditor.classList.remove("is-pending");
      return;
    }
    const draft = pendingProtectionForPosition(position);
    elements.chartPositionTitle.textContent = formatPositionSide(position.side) + " #" + String(position.positionId) + " | " + formatTradeVolume(position.volume, positionLots(position));
    elements.chartPositionCopy.textContent = "Drag on chart to apply SL/TP immediately, or type exact prices and apply once.";
    elements.chartPositionStopLoss.value = draft.stopLoss != null ? Number(draft.stopLoss).toFixed(2) : "";
    elements.chartPositionTakeProfit.value = draft.takeProfit != null ? Number(draft.takeProfit).toFixed(2) : "";
    elements.chartPositionPendingState.textContent = draft.hasChanges
      ? "Pending change: " + [
        draft.stopChanged ? "SL " + formatPrice(draft.stopLoss) : null,
        draft.takeChanged ? "TP " + formatPrice(draft.takeProfit) : null,
      ].filter(Boolean).join(" | ")
      : "No pending changes.";
    elements.chartPositionEditor.classList.toggle("is-pending", draft.hasChanges);
    elements.chartPositionConfirmButton.disabled = state.trade.actionBusy || !draft.hasChanges;
    elements.chartPositionCancelButton.disabled = state.trade.actionBusy || !draft.hasChanges;
  }

  function tradeMarkersAtTickId(tickId) {
    const rounded = Math.round(Number(tickId));
    if (!Number.isFinite(rounded)) {
      return [];
    }
    const overlay = selectedHistoricalTradeVisible();
    if (!overlay) {
      return [];
    }
    const markers = [];
    const trade = overlay.trade || {};
    if (Number(overlay.entryTickId) === rounded && trade.entryPrice != null) {
      markers.push({
        kind: "entry",
        side: trade.side,
        volume: trade.volume,
        volumeLots: trade.volumeLots,
        price: trade.entryPrice,
        timestamp: trade.entryTimestamp,
        positionId: trade.positionId,
      });
    }
    if (Number(overlay.exitTickId) === rounded && trade.exitPrice != null) {
      markers.push({
        kind: "exit",
        side: trade.side,
        volume: trade.volume,
        volumeLots: trade.volumeLots,
        price: trade.exitPrice,
        timestamp: trade.exitTimestamp,
        pnl: trade.realizedNetPnl,
        positionId: trade.positionId,
      });
    }
    return markers;
  }

  function tooltipHtml(params) {
    const entries = Array.isArray(params) ? params : [params];
    const point = entries[0];
    const tickId = Number(point?.axisValue ?? point?.value?.[0]);
    const row = rowAtTickId(tickId);
    const sections = [];
    if (row) {
      const timestamp = new Date(row.timestamp);
      sections.push(tooltipSection("Tick", [
        tooltipRow("Id", row.id),
        tooltipRow("Date", timestamp.toLocaleDateString()),
        tooltipRow("Time", timestamp.toLocaleTimeString()),
        tooltipRow("Bid", formatPrice(row.bid)),
        tooltipRow("Ask", formatPrice(row.ask)),
        tooltipRow("Mid", formatPrice(row.mid)),
      ]));
    } else if (Number.isFinite(tickId)) {
      sections.push(tooltipSection("Tick", [
        tooltipRow("Id", Math.round(tickId)),
      ]));
    }
    const events = eventsAtTickId(tickId);
    if (events.length) {
      sections.push(tooltipSection("Events", events.map((event) => tooltipRow(event.type, formatPrice(event.price)))));
    }
    const boxes = boxesAtTickId(tickId);
    if (boxes.length) {
      sections.push(tooltipSection("Ranges", boxes.map((box) => tooltipRow(
        "Range #" + String(box.id),
        formatPrice(box.bottom) + " - " + formatPrice(box.top) + " (" + String(box.status) + ")"
      ))));
    }
    const tradeMarkers = tradeMarkersAtTickId(tickId);
    if (tradeMarkers.length) {
      sections.push(tooltipSection("Selected Trade", tradeMarkers.map((marker) => {
        if (marker.kind === "entry") {
          return tooltipRow(
            "Entry " + String(marker.side || "").toUpperCase(),
            formatPrice(marker.price) + " | " + formatTradeVolume(marker.volume, marker.volumeLots) + " | " + String(marker.timestamp || "-")
          );
        }
        return tooltipRow(
          "Exit " + String(marker.side || "").toUpperCase(),
          formatPrice(marker.price) + " | PnL " + formatSignedPnl(marker.pnl) + " | " + String(marker.timestamp || "-")
        );
      })));
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
        grid: { left: 54, right: 16, top: 14, bottom: 54 },
        tooltip: {
          trigger: "axis",
          axisPointer: { type: "cross" },
          formatter: tooltipHtml,
          backgroundColor: "transparent",
          borderWidth: 0,
          padding: 0,
          extraCssText: "box-shadow:none;",
        },
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

  function selectedHistoricalTradeEntrySeriesData() {
    const overlay = selectedHistoricalTradeVisible();
    if (!overlay || overlay.trade?.entryPrice == null) {
      return [];
    }
    return [{
      value: [Number(overlay.entryTickId), Number(overlay.trade.entryPrice)],
      trade: overlay.trade,
      itemStyle: {
        color: overlay.trade.side === "buy" ? TRADE_MARKER_COLORS.buyEntry : TRADE_MARKER_COLORS.sellEntry,
      },
    }];
  }

  function selectedHistoricalTradeExitSeriesData() {
    const overlay = selectedHistoricalTradeVisible();
    if (!overlay || overlay.trade?.exitPrice == null) {
      return [];
    }
    return [{
      value: [Number(overlay.exitTickId), Number(overlay.trade.exitPrice)],
      trade: overlay.trade,
      itemStyle: {
        color: overlay.trade.side === "buy" ? TRADE_MARKER_COLORS.buyExit : TRADE_MARKER_COLORS.sellExit,
      },
    }];
  }

  function pendingToSeriesData() {
    return state.trade.pendingOrders
      .map((order) => {
        const timestampMs = order.timestampMs || Date.now();
        const tickId = tickIdForTimestampMs(timestampMs) ?? Number(state.rangeLastId);
        const price = order.limitPrice ?? order.stopPrice;
        if (!Number.isFinite(tickId) || !Number.isFinite(Number(price))) {
          return null;
        }
        return {
          value: [Number(tickId), Number(price)],
          order,
          itemStyle: { color: TRADE_MARKER_COLORS.pending },
        };
      })
      .filter(Boolean);
  }

  function openConnectorData() {
    const currentTick = Number(state.rangeLastId);
    if (!Number.isFinite(currentTick)) {
      return [];
    }
    const currentRow = rowAtTickId(currentTick);
    const currentPrice = currentRow ? Number(currentRow.mid) : null;
    return state.trade.positions
      .map((position) => {
        const entryTick = tickIdForTimestampMs(position.openTimestampMs);
        const entryPrice = Number(position.entryPrice);
        if (!Number.isFinite(entryTick) || !Number.isFinite(entryPrice) || !Number.isFinite(currentPrice)) {
          return null;
        }
        return {
          value: [Number(entryTick), Number(entryPrice), Number(currentTick), Number(currentPrice)],
          position,
        };
      })
      .filter(Boolean);
  }

  function selectedHistoricalConnectorData() {
    const overlay = selectedHistoricalTradeVisible();
    if (!overlay || overlay.trade?.entryPrice == null || overlay.trade?.exitPrice == null) {
      return [];
    }
    return [{
      value: [
        Number(overlay.entryTickId),
        Number(overlay.trade.entryPrice),
        Number(overlay.exitTickId),
        Number(overlay.trade.exitPrice),
      ],
      trade: overlay.trade,
    }];
  }

  function tradeConnectorRender(params, api) {
    const x1 = Number(api.value(0));
    const y1 = Number(api.value(1));
    const x2 = Number(api.value(2));
    const y2 = Number(api.value(3));
    const start = api.coord([x1, y1]);
    const end = api.coord([x2, y2]);
    const item = params.data?.position || params.data?.trade || {};
    const side = item.side === "buy" ? "buy" : "sell";
    const color = side === "buy" ? "rgba(126,240,199,0.88)" : "rgba(255,159,178,0.88)";
    return {
      type: "line",
      shape: { x1: start[0], y1: start[1], x2: end[0], y2: end[1] },
      style: {
        stroke: color,
        lineWidth: 1.2,
        lineDash: [5, 3],
      },
      silent: true,
    };
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
    if (isLiveTradeOverlayMode()) {
      series.push({
        id: "trade-open-connectors",
        name: "Open positions",
        type: "custom",
        renderItem: tradeConnectorRender,
        data: openConnectorData(),
        animation: false,
        encode: { x: [0, 2], y: [1, 3] },
        z: 6,
      });
      series.push({
        id: "trade-pending-markers",
        name: "Pending orders",
        type: "scatter",
        data: pendingToSeriesData(),
        symbol: "rect",
        symbolSize: 8,
        animation: false,
        z: 11,
      });
    }
    if (isSelectedHistoricalReviewMode()) {
      series.push({
        id: "selected-trade-connector",
        name: "Selected trade",
        type: "custom",
        renderItem: tradeConnectorRender,
        data: selectedHistoricalConnectorData(),
        animation: false,
        encode: { x: [0, 2], y: [1, 3] },
        z: 10,
      });
      series.push({
        id: "selected-trade-entry-marker",
        name: "Selected trade entry",
        type: "scatter",
        data: selectedHistoricalTradeEntrySeriesData(),
        symbol: "triangle",
        symbolSize: 11,
        animation: false,
        z: 12,
      });
      series.push({
        id: "selected-trade-exit-marker",
        name: "Selected trade exit",
        type: "scatter",
        data: selectedHistoricalTradeExitSeriesData(),
        symbol: "diamond",
        symbolSize: 10,
        animation: false,
        z: 13,
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
    if (isLiveTradeOverlayMode()) {
      state.trade.positions.forEach((position) => {
        const draft = pendingProtectionForPosition(position);
        values.push(Number(position.entryPrice));
        if (draft.stopLoss != null) {
          values.push(Number(draft.stopLoss));
        }
        if (draft.takeProfit != null) {
          values.push(Number(draft.takeProfit));
        }
      });
      state.trade.pendingOrders.forEach((order) => {
        if (order.limitPrice != null) {
          values.push(Number(order.limitPrice));
        }
        if (order.stopPrice != null) {
          values.push(Number(order.stopPrice));
        }
      });
    }
    if (isSelectedHistoricalReviewMode()) {
      const trade = selectedHistoricalTradeOverlay()?.trade;
      if (trade?.entryPrice != null) {
        values.push(Number(trade.entryPrice));
      }
      if (trade?.exitPrice != null) {
        values.push(Number(trade.exitPrice));
      }
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

  function buildTradeProtectionGraphics() {
    const chart = state.chart;
    if (!chart || !isLiveTradeOverlayMode()) {
      return [];
    }
    const grid = chart.getModel()?.getComponent("grid", 0);
    const rect = grid?.coordinateSystem?.getRect?.();
    if (!rect) {
      return [];
    }
    const rightId = Number(state.rangeLastId || (state.rows[state.rows.length - 1]?.id || 0));
    const currentRow = rowAtTickId(rightId);
    const currentPrice = currentRow ? Number(currentRow.mid) : null;
    const graphics = [];

    state.trade.positions.forEach((position, index) => {
      const draft = pendingProtectionForPosition(position);
      const isActive = Number(state.trade.activePositionId) === Number(position.positionId);
      const positionColor = position.side === "buy" ? "rgba(126,240,199,0.92)" : "rgba(255,159,178,0.92)";
      const positionText = position.side === "buy" ? "#cffff0" : "#ffd1da";
      if (Number.isFinite(currentPrice)) {
        const currentPoint = chart.convertToPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [rightId || 1, currentPrice]);
        if (Array.isArray(currentPoint)) {
          const currentY = Number(currentPoint[1]);
          const labelY = Math.max(rect.y + 10, Math.min(rect.y + rect.height - 10, currentY + (index * 18) - 9));
          if (Number.isFinite(currentY)) {
            graphics.push({
              id: "trade-position-label-" + String(position.positionId),
              type: "group",
              silent: true,
              z: 14,
              children: [
                {
                  type: "rect",
                  shape: { x: rect.x + rect.width - 128, y: labelY - 10, width: 124, height: 20, r: 4 },
                  style: {
                    fill: isActive ? "rgba(5,9,15,0.94)" : "rgba(5,9,15,0.82)",
                    stroke: isActive ? "rgba(255,200,87,0.72)" : positionColor,
                    lineWidth: isActive ? 1.2 : 1,
                  },
                },
                {
                  type: "text",
                  style: {
                    text: formatPositionSide(position.side) + " #" + String(position.positionId) + " " + formatLots(positionLots(position)),
                    x: rect.x + rect.width - 66,
                    y: labelY,
                    textAlign: "center",
                    textVerticalAlign: "middle",
                    fill: positionText,
                    font: "11px 'IBM Plex Mono'",
                  },
                },
              ],
            });
          }
        }
      }

      [
        { key: "stopLoss", price: draft.stopLoss, changed: draft.stopChanged },
        { key: "takeProfit", price: draft.takeProfit, changed: draft.takeChanged },
      ].forEach(({ key, price, changed }) => {
        const numericPrice = Number(price);
        const actualPrice = Number(position[key]);
        const linePrice = Number.isFinite(numericPrice) ? numericPrice : actualPrice;
        if (!Number.isFinite(linePrice) || linePrice <= 0) {
          return;
        }
        const point = chart.convertToPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [rightId || 1, linePrice]);
        if (!Array.isArray(point)) {
          return;
        }
        const baseY = Number(point[1]);
        if (!Number.isFinite(baseY) || baseY < rect.y || baseY > rect.y + rect.height) {
          return;
        }
        const isStop = key === "stopLoss";
        const color = changed
          ? "rgba(255,200,87,0.92)"
          : (isStop ? "rgba(255,107,136,0.85)" : "rgba(126,240,199,0.85)");
        const textColor = changed
          ? "#ffe9a6"
          : (isStop ? "#ffc0cd" : "#c7ffeb");
        const labelPrefix = changed ? (isStop ? "SL*" : "TP*") : (isStop ? "SL" : "TP");
        graphics.push({
          id: "trade-protection-" + String(position.positionId) + "-" + key,
          type: "group",
          x: 0,
          y: 0,
          draggable: !state.trade.actionBusy,
          z: isActive ? 18 : 16,
          cursor: state.trade.actionBusy ? "default" : "ns-resize",
          onclick: function () {
            setActiveTradePosition(position.positionId);
          },
          ondrag: function () {
            if (state.trade.actionBusy) {
              return;
            }
            const targetY = Math.max(rect.y + 2, Math.min(rect.y + rect.height - 2, baseY + Number(this.y || 0)));
            this.x = 0;
            this.y = targetY - baseY;
          },
          ondragend: function () {
            if (state.trade.actionBusy) {
              this.x = 0;
              this.y = 0;
              queueOverlayRender();
              return;
            }
            const targetY = Math.max(rect.y + 2, Math.min(rect.y + rect.height - 2, baseY + Number(this.y || 0)));
            this.x = 0;
            this.y = 0;
            const converted = chart.convertFromPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [rect.x + 12, targetY]);
            const targetPrice = Number(Array.isArray(converted) ? converted[1] : NaN);
            if (!Number.isFinite(targetPrice) || targetPrice <= 0) {
              tradeStatus("Drag rejected: invalid target price.", true);
              queueOverlayRender();
              return;
            }
            requestProtectionDrag(position.positionId, targetPrice, key);
          },
          children: [
            {
              type: "line",
              shape: { x1: rect.x + 2, y1: baseY, x2: rect.x + rect.width - 2, y2: baseY },
              style: {
                stroke: color,
                lineWidth: isActive ? 1.5 : 1.2,
                lineDash: changed ? [3, 2] : [6, 3],
                opacity: isActive ? 1 : 0.88,
              },
            },
            {
              type: "rect",
              shape: { x: rect.x + rect.width - 92, y: baseY - 10, width: 88, height: 18, r: 4 },
              style: {
                fill: "rgba(5,9,15,0.9)",
                stroke: isActive ? "rgba(255,200,87,0.72)" : color,
                lineWidth: isActive ? 1.2 : 1,
              },
            },
            {
              type: "text",
              style: {
                text: labelPrefix + " " + Number(linePrice).toFixed(2),
                x: rect.x + rect.width - 48,
                y: baseY,
                textAlign: "center",
                textVerticalAlign: "middle",
                fill: textColor,
                font: "11px 'IBM Plex Mono'",
              },
            },
          ],
        });
      });

      if (draft.stopLoss == null || draft.takeProfit == null) {
        const referencePrice = Number(currentTradeReferencePrice(position));
        const referencePoint = Number.isFinite(referencePrice)
          ? chart.convertToPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [rightId || 1, referencePrice])
          : null;
        if (Array.isArray(referencePoint)) {
          const addY = Number(referencePoint[1]);
          if (Number.isFinite(addY) && addY >= rect.y && addY <= rect.y + rect.height) {
            graphics.push({
              id: "trade-protection-add-" + String(position.positionId),
              type: "group",
              x: 0,
              y: 0,
              draggable: !state.trade.actionBusy,
              z: isActive ? 17 : 15,
              cursor: state.trade.actionBusy ? "default" : "ns-resize",
              onclick: function () {
                setActiveTradePosition(position.positionId);
              },
              ondrag: function () {
                if (state.trade.actionBusy) {
                  return;
                }
                const targetY = Math.max(rect.y + 2, Math.min(rect.y + rect.height - 2, addY + Number(this.y || 0)));
                this.x = 0;
                this.y = targetY - addY;
              },
              ondragend: function () {
                if (state.trade.actionBusy) {
                  this.x = 0;
                  this.y = 0;
                  queueOverlayRender();
                  return;
                }
                const targetY = Math.max(rect.y + 2, Math.min(rect.y + rect.height - 2, addY + Number(this.y || 0)));
                this.x = 0;
                this.y = 0;
                const converted = chart.convertFromPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [rect.x + 12, targetY]);
                const targetPrice = Number(Array.isArray(converted) ? converted[1] : NaN);
                if (!Number.isFinite(targetPrice) || targetPrice <= 0) {
                  tradeStatus("Drag rejected: invalid target price.", true);
                  queueOverlayRender();
                  return;
                }
                requestProtectionDrag(position.positionId, targetPrice, null);
              },
              children: [
                {
                  type: "line",
                  shape: { x1: rect.x + 18, y1: addY, x2: rect.x + rect.width - 104, y2: addY },
                  style: { stroke: "rgba(255,200,87,0.72)", lineWidth: 1, lineDash: [2, 4] },
                },
                {
                  type: "rect",
                  shape: { x: rect.x + rect.width - 100, y: addY - 10, width: 96, height: 18, r: 4 },
                  style: {
                    fill: "rgba(5,9,15,0.82)",
                    stroke: "rgba(255,200,87,0.72)",
                    lineWidth: 1,
                  },
                },
                {
                  type: "text",
                  style: {
                    text: "Drag to add",
                    x: rect.x + rect.width - 52,
                    y: addY,
                    textAlign: "center",
                    textVerticalAlign: "middle",
                    fill: "#ffe9a6",
                    font: "11px 'IBM Plex Mono'",
                  },
                },
              ],
            });
          }
        }
      }
    });

    state.trade.pendingOrders.forEach((order, index) => {
      const price = Number(order.limitPrice ?? order.stopPrice);
      if (!Number.isFinite(price) || price <= 0) {
        return;
      }
      const point = chart.convertToPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [rightId || 1, price]);
      if (!Array.isArray(point)) {
        return;
      }
      const y = Number(point[1]);
      if (!Number.isFinite(y) || y < rect.y || y > rect.y + rect.height) {
        return;
      }
      graphics.push({
        id: "trade-pending-line-" + String(order.orderId || index),
        type: "line",
        silent: true,
        z: 8,
        shape: { x1: rect.x + 2, y1: y, x2: rect.x + rect.width - 2, y2: y },
        style: { stroke: "rgba(255,200,87,0.62)", lineWidth: 1, lineDash: [3, 4] },
      });
    });

    return graphics;
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
      }, {
        id: "trade-overlay",
        type: "group",
        silent: false,
        z: 15,
        children: buildTradeProtectionGraphics(),
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
      const error = new Error(tradeErrorMessage(payload));
      const detail = tradePayloadDetail(payload);
      error.status = response.status;
      error.code = typeof payload?.error === "string"
        ? payload.error
        : (typeof detail?.error === "string" ? detail.error : null);
      error.payload = payload;
      throw error;
    }
    return payload;
  }

  async function tradeFetchJson(url, options) {
    const request = {
      method: options?.method || "GET",
      headers: { "Content-Type": "application/json", ...(options?.headers || {}) },
      body: options?.body,
    };
    if (!request.body) {
      delete request.body;
    }
    try {
      const payload = await fetchJson(url, request);
      state.trade.lastLoggedErrorKey = null;
      return payload;
    } catch (error) {
      tradeConsole(request.method, url, error);
      throw error;
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
    state.trade.pollTimer = window.setTimeout(() => {
      state.trade.pollTimer = 0;
      refreshTradeData({ silent: true }).catch((error) => tradeStatus(error.message || "Trade refresh failed.", true));
    }, TRADE_POLL_INTERVAL_MS);
  }

  function formatPositionSide(side) {
    return String(side || "").toUpperCase();
  }

  function formatTradeTimestamp(value) {
    if (!value) {
      return "-";
    }
    const parsedMs = Date.parse(value);
    if (!Number.isFinite(parsedMs)) {
      return String(value);
    }
    const timestamp = new Date(parsedMs);
    return timestamp.toLocaleDateString() + " " + timestamp.toLocaleTimeString();
  }

  function selectedHistoricalTradeKey(trade) {
    return [
      Number(trade?.positionId || 0),
      Number(trade?.entryTimestampMs || 0),
      Number(trade?.exitTimestampMs || 0),
    ].join(":");
  }

  function completedTradeHistoryItems() {
    return (state.trade.trades || []).filter((trade) => !trade?.isOpen && trade?.entryTimestampMs && trade?.exitTimestampMs);
  }

  function completedTradeByKey(key) {
    return completedTradeHistoryItems().find((trade) => selectedHistoricalTradeKey(trade) === String(key)) || null;
  }

  function currentTradeReviewSettings() {
    return {
      beforeTicks: clampTradeReviewTicks(elements.tradeReviewTicksBefore?.value, TRADE_REVIEW_DEFAULT_TICKS_BEFORE),
      afterTicks: clampTradeReviewTicks(elements.tradeReviewTicksAfter?.value, TRADE_REVIEW_DEFAULT_TICKS_AFTER),
    };
  }

  function isLiveTradeOverlayMode() {
    return state.trade.authenticated && currentConfig().mode === "live";
  }

  function isSelectedHistoricalReviewMode() {
    return currentConfig().mode === "review" && Boolean(state.trade.selectedHistoricalTradeOverlay);
  }

  function reviewTradeSelectionReady() {
    const config = currentConfig();
    return state.trade.authenticated && config.mode === "review" && config.run === "stop";
  }

  function selectedHistoricalTradeOverlay() {
    return state.trade.selectedHistoricalTradeOverlay;
  }

  function selectedHistoricalTradeVisible() {
    const overlay = selectedHistoricalTradeOverlay();
    if (!overlay || !isSelectedHistoricalReviewMode()) {
      return null;
    }
    const firstId = Number(state.rangeFirstId);
    const lastId = Number(state.rangeLastId);
    if (!Number.isFinite(firstId) || !Number.isFinite(lastId)) {
      return overlay;
    }
    const entryTickId = Number(overlay.entryTickId);
    const exitTickId = Number(overlay.exitTickId);
    if ((Number.isFinite(entryTickId) && entryTickId >= firstId && entryTickId <= lastId)
      || (Number.isFinite(exitTickId) && exitTickId >= firstId && exitTickId <= lastId)) {
      return overlay;
    }
    return null;
  }

  function renderTradeReviewControls() {
    if (!elements.tradeReviewSection) {
      return;
    }
    const reviewReady = reviewTradeSelectionReady();
    const config = currentConfig();
    const selected = selectedHistoricalTradeOverlay();
    const completedCount = completedTradeHistoryItems().length;
    elements.tradeReviewSection.classList.toggle("is-disabled", !reviewReady);
    [elements.tradeReviewTicksBefore, elements.tradeReviewTicksAfter].forEach((input, index) => {
      if (!input) {
        return;
      }
      const fallback = index === 0 ? TRADE_REVIEW_DEFAULT_TICKS_BEFORE : TRADE_REVIEW_DEFAULT_TICKS_AFTER;
      input.value = String(clampTradeReviewTicks(input.value, fallback));
      input.disabled = !reviewReady || state.trade.actionBusy;
    });
    if (elements.tradeReviewHint) {
      elements.tradeReviewHint.textContent = !state.trade.authenticated
        ? "Trade review becomes available after trade login."
        : (config.mode === "review" && config.run === "run" && selected
          ? "Replay is running from the selected trade window."
          : (!reviewReady
          ? "Switch the chart to Review + Stop to load one completed trade."
          : (completedCount
            ? "Click one completed trade to load a focused review window."
            : "No completed trades are available for review.")));
    }
    if (elements.tradeReviewSummary) {
      elements.tradeReviewSummary.textContent = selected
        ? [
          "Selected ",
          formatPositionSide(selected.trade?.side),
          " #",
          String(selected.trade?.positionId || "-"),
          " | Entry ",
          formatTradeTimestamp(selected.trade?.entryTimestamp),
          " | Exit ",
          formatTradeTimestamp(selected.trade?.exitTimestamp),
          " | PnL ",
          formatSignedPnl(selected.trade?.realizedNetPnl),
          " | Replay window ",
          String(selected.beforeTicks),
          " before / ",
          String(selected.afterTicks),
          " after",
        ].join("")
        : "No review trade selected.";
    }
  }

  function renderTradeLists() {
    if (elements.tradeSessionSummary) {
      elements.tradeSessionSummary.textContent = !state.trade.authConfigured
        ? "Trade login is not configured on the server."
        : (state.trade.authenticated
          ? "Session unlocked for " + (state.trade.username || "trade user") + ". Market entry is one-click from the chart."
          : "Login required for chart trading.");
    }
    renderPreparedTradeSummary();
    renderBrokerSummary();
    const openItems = state.trade.positions || [];
    if (!openItems.length) {
      elements.tradeOpenList.innerHTML = "<div class=\"sql-empty\">No open positions.</div>";
    } else {
      elements.tradeOpenList.innerHTML = openItems.map((position) => {
        const draft = pendingProtectionForPosition(position);
        return [
          "<article class=\"trade-item\" data-position-id=\"", escapeHtml(position.positionId), "\">",
          "<div class=\"trade-item-head\"><span>", escapeHtml(formatPositionSide(position.side)), " #", escapeHtml(position.positionId), "</span><span>", escapeHtml(formatTradeVolume(position.volume, positionLots(position))), "</span></div>",
          "<div class=\"trade-item-meta\">Entry ", escapeHtml(formatPrice(position.entryPrice)), " | uPnL ", escapeHtml(formatSignedPnl(position.netUnrealizedPnl)), "</div>",
          "<div class=\"trade-item-meta\">SL ", escapeHtml(formatPrice(draft.stopLoss)), draft.stopChanged ? " pending" : "", " | TP ", escapeHtml(formatPrice(draft.takeProfit)), draft.takeChanged ? " pending" : "", "</div>",
          "<div class=\"trade-item-actions\">",
          "<button class=\"ghost-button compact-button\" type=\"button\" data-action=\"select-position\" data-position-id=\"", escapeHtml(position.positionId), "\">Edit SL/TP</button>",
          "<button class=\"ghost-button compact-button\" type=\"button\" data-action=\"close-position\" data-position-id=\"", escapeHtml(position.positionId), "\" data-volume=\"", escapeHtml(position.volume || 0), "\">Close</button>",
          "<button class=\"ghost-button compact-button\" type=\"button\" data-action=\"close-half-position\" data-position-id=\"", escapeHtml(position.positionId), "\" data-volume=\"", escapeHtml(Math.max(1, Math.floor(Number(position.volume || 0) / 2))), "\">Close 1/2</button>",
          "</div>",
          "</article>",
        ].join("");
      }).join("");
    }

    const pendingItems = state.trade.pendingOrders || [];
    if (!pendingItems.length) {
      elements.tradePendingList.innerHTML = "<div class=\"sql-empty\">No pending orders.</div>";
    } else {
      elements.tradePendingList.innerHTML = pendingItems.map((order) => [
        "<article class=\"trade-item\">",
        "<div class=\"trade-item-head\"><span>", escapeHtml(String(order.orderType || "ORDER")), " #", escapeHtml(order.orderId), "</span><span>", escapeHtml(formatPositionSide(order.side)), "</span></div>",
        "<div class=\"trade-item-meta\">",
        escapeHtml(formatTradeVolume(order.volume, order.volumeLots)),
        " | Px ", escapeHtml(formatPrice(order.limitPrice != null ? order.limitPrice : order.stopPrice)),
        "</div></article>",
      ].join("")).join("");
    }

    const historyItems = completedTradeHistoryItems();
    const reviewReady = reviewTradeSelectionReady();
    const selectedKey = selectedHistoricalTradeOverlay()?.key;
    if (!historyItems.length) {
      elements.tradeHistoryList.innerHTML = "<div class=\"sql-empty\">" + (state.trade.historyAvailable ? "No recent trade history." : "Recent trade history unavailable.") + "</div>";
    } else {
      elements.tradeHistoryList.innerHTML = historyItems.map((trade) => [
        "<article class=\"trade-item", selectedHistoricalTradeKey(trade) === selectedKey ? " is-selected" : "", "\">",
        "<div class=\"trade-item-head\"><span>", escapeHtml(formatPositionSide(trade.side)), " #", escapeHtml(trade.positionId), "</span><span>", escapeHtml(formatSignedPnl(trade.realizedNetPnl)), "</span></div>",
        "<div class=\"trade-item-meta\">",
        escapeHtml(formatTradeVolume(trade.volume, trade.volumeLots)),
        " | Entry ", escapeHtml(formatPrice(trade.entryPrice)),
        trade.exitPrice != null ? " -> Exit " + escapeHtml(formatPrice(trade.exitPrice)) : " -> Exit -",
        "</div>",
        "<div class=\"trade-item-meta\">In ", escapeHtml(formatTradeTimestamp(trade.entryTimestamp)), "</div>",
        "<div class=\"trade-item-meta\">Out ", escapeHtml(formatTradeTimestamp(trade.exitTimestamp)), "</div>",
        "<div class=\"trade-item-actions trade-item-actions-single\">",
        "<button class=\"ghost-button compact-button\" type=\"button\" data-action=\"select-trade\" data-trade-key=\"", escapeHtml(selectedHistoricalTradeKey(trade)), "\"", (reviewReady && !state.trade.actionBusy) ? "" : " disabled", ">",
        selectedHistoricalTradeKey(trade) === selectedKey ? "Reload review" : "Review trade",
        "</button>",
        "</div>",
        "</article>",
      ].join("")).join("");
    }
    renderTradeReviewControls();
    renderPositionEditor();
  }

  async function refreshTradeData(options) {
    if (!state.trade.authenticated) {
      return;
    }
    const silent = Boolean(options?.silent);
    state.trade.loading = true;
    if (!silent) {
      tradeStatus("Loading trade state...", false);
    }
    try {
      const openPayload = await tradeFetchJson("/api/trade/open");
      state.trade.brokerStatus = brokerStatusFromPayload(openPayload);
      state.trade.brokerConfigured = Boolean(state.trade.brokerStatus?.configured);
      state.trade.volumeInfo = openPayload.volumeInfo || currentTradeVolumeInfo();
      state.trade.positions = Array.isArray(openPayload.positions) ? openPayload.positions : [];
      state.trade.pendingOrders = Array.isArray(openPayload.pendingOrders) ? openPayload.pendingOrders : [];
      state.trade.lastLoadedAtMs = Date.now();
      state.trade.historyAvailable = true;
      try {
        const historyPayload = await tradeFetchJson("/api/trade/history?limit=" + String(TRADE_HISTORY_LIMIT));
        state.trade.brokerStatus = brokerStatusFromPayload(historyPayload);
        state.trade.brokerConfigured = Boolean(state.trade.brokerStatus?.configured);
        state.trade.volumeInfo = openPayload.volumeInfo || historyPayload.volumeInfo || currentTradeVolumeInfo();
        state.trade.trades = Array.isArray(historyPayload.trades) ? historyPayload.trades : [];
        state.trade.deals = Array.isArray(historyPayload.deals) ? historyPayload.deals : [];
        const selected = selectedHistoricalTradeOverlay();
        if (selected) {
          const refreshedTrade = completedTradeByKey(selected.key);
          if (refreshedTrade) {
            state.trade.selectedHistoricalTradeOverlay = { ...selected, trade: refreshedTrade };
          }
        }
      } catch (error) {
        state.trade.historyAvailable = false;
        if (!silent) {
          tradeStatus("Trade state updated. Recent history unavailable.", true);
        }
      }
      syncTradeSelection();
      syncPreparedTradeInputs();
      renderTradeLists();
      renderChart({ shiftWithRun: false });
      if (!silent && state.trade.historyAvailable) {
        tradeStatus("Trade state updated.", false);
      }
      scheduleTradePolling();
    } catch (error) {
      const message = String(error?.message || "").toLowerCase();
      if (error?.code === "TRADE_AUTH_NOT_CONFIGURED" || message.includes("trade login is not configured on the server")) {
        applyTradeSessionPayload({
          authenticated: false,
          username: null,
          authConfigured: false,
          brokerConfigured: error?.payload?.brokerConfigured ?? tradePayloadDetail(error?.payload)?.brokerConfigured ?? state.trade.brokerConfigured,
          configured: error?.payload?.configured ?? tradePayloadDetail(error?.payload)?.configured ?? state.trade.brokerConfigured,
          broker: error?.payload?.broker || tradePayloadDetail(error?.payload)?.broker || state.trade.brokerStatus,
          error: error?.code || "TRADE_AUTH_NOT_CONFIGURED",
        });
        renderTradeLists();
      } else if (message.includes("trade login required")) {
        applyTradeSessionPayload({
          authenticated: false,
          username: null,
          authConfigured: true,
          brokerConfigured: state.trade.brokerConfigured,
          configured: state.trade.brokerConfigured,
          broker: state.trade.brokerStatus,
        });
        renderTradeLists();
      } else {
        state.trade.brokerStatus = brokerStatusFromPayload(error?.payload || { broker: state.trade.brokerStatus });
        state.trade.brokerConfigured = Boolean(state.trade.brokerStatus?.configured);
        state.trade.volumeInfo = null;
        state.trade.lastLoadedAtMs = null;
        state.trade.positions = [];
        state.trade.pendingOrders = [];
        state.trade.historyAvailable = false;
        renderTradeLists();
        renderChart({ shiftWithRun: false });
      }
      throw error;
    } finally {
      state.trade.loading = false;
    }
  }

  async function requestTradeLogin() {
    if (state.trade.loginBusy || state.trade.actionBusy) {
      return;
    }
    if (!state.trade.authConfigured) {
      tradeStatus("Trade login is not configured on the server.", true);
      return;
    }
    const username = (elements.tradeUsername.value || "").trim();
    const password = elements.tradePassword.value || "";
    if (!username || !password) {
      tradeStatus("Username and password are required.", true);
      return;
    }
    state.trade.loginBusy = true;
    setTradeBusy(true);
    try {
      const payload = await tradeFetchJson("/api/trade/login", {
        method: "POST",
        body: JSON.stringify({ username, password }),
      });
      elements.tradePassword.value = "";
      applyTradeSessionPayload({
        authenticated: true,
        username: payload.username || username,
        authConfigured: true,
        brokerConfigured: state.trade.brokerConfigured,
        configured: state.trade.brokerConfigured,
        broker: state.trade.brokerStatus,
      });
      tradeStatus("Trade login successful.", false);
      await refreshTradeData({ silent: true }).catch((error) => {
        tradeStatus(error.message || "Trade refresh failed.", true);
      });
    } catch (error) {
      if (error?.code === "TRADE_AUTH_NOT_CONFIGURED") {
        applyTradeSessionPayload({
          authenticated: false,
          username: null,
          authConfigured: false,
          brokerConfigured: error?.payload?.brokerConfigured ?? tradePayloadDetail(error?.payload)?.brokerConfigured ?? state.trade.brokerConfigured,
          configured: error?.payload?.configured ?? tradePayloadDetail(error?.payload)?.configured ?? state.trade.brokerConfigured,
          broker: error?.payload?.broker || tradePayloadDetail(error?.payload)?.broker || state.trade.brokerStatus,
          error: error.code,
        });
        renderTradeLists();
      }
      tradeStatus(error.message || "Trade login failed.", true);
    } finally {
      state.trade.loginBusy = false;
      setTradeBusy(false);
    }
  }

  async function requestTradeLogout() {
    if (state.trade.actionBusy) {
      return;
    }
    setTradeBusy(true);
    stopTradePolling();
    try {
      await tradeFetchJson("/api/trade/logout", { method: "POST" });
    } catch (error) {
      void error;
    }
    applyTradeSessionPayload({
      authenticated: false,
      username: null,
      authConfigured: state.trade.authConfigured,
      brokerConfigured: state.trade.brokerConfigured,
      configured: state.trade.brokerConfigured,
      broker: state.trade.brokerStatus,
    });
    renderTradeLists();
    renderChart({ shiftWithRun: false });
    tradeStatus("Trade session logged out.", false);
    setTradeBusy(false);
  }

  async function submitMarketOrder(side) {
    if (!state.trade.authenticated || state.trade.actionBusy) {
      return;
    }
    const prepared = preparedTradeState();
    if (!prepared.ready) {
      tradeStatus(prepared.reason || "Prepared trade inputs are invalid.", true);
      return;
    }
    state.trade.activeOrderSide = side === "sell" ? "sell" : "buy";
    renderTradeEntryOverlay();
    setTradeBusy(true);
    try {
      const payload = await tradeFetchJson("/api/trade/order/market", {
        method: "POST",
        body: JSON.stringify({
          side: state.trade.activeOrderSide,
          lotSize: prepared.lotSize,
          stopLoss: prepared.stopLoss,
          takeProfit: prepared.takeProfit,
        }),
      });
      state.trade.brokerStatus = brokerStatusFromPayload(payload);
      state.trade.brokerConfigured = Boolean(state.trade.brokerStatus?.configured);
      tradeStatus((state.trade.activeOrderSide === "sell" ? "Sell" : "Buy") + " market order submitted.", false);
      await refreshTradeData({ silent: true });
    } catch (error) {
      state.trade.brokerStatus = brokerStatusFromPayload(error?.payload || { broker: state.trade.brokerStatus });
      state.trade.brokerConfigured = Boolean(state.trade.brokerStatus?.configured);
      tradeStatus(error.message || "Order submit failed.", true);
    } finally {
      state.trade.activeOrderSide = null;
      setTradeBusy(false);
    }
  }

  async function submitClosePosition(positionId, volume) {
    if (!state.trade.authenticated || state.trade.actionBusy) {
      return;
    }
    const parsedVolume = Number.parseInt(String(volume || 0), 10);
    if (!Number.isFinite(parsedVolume) || parsedVolume <= 0) {
      tradeStatus("Close volume is invalid.", true);
      return;
    }
    setTradeBusy(true);
    try {
      const payload = await tradeFetchJson("/api/trade/position/close", {
        method: "POST",
        body: JSON.stringify({ positionId: Number(positionId), volume: parsedVolume }),
      });
      state.trade.brokerStatus = brokerStatusFromPayload(payload);
      state.trade.brokerConfigured = Boolean(state.trade.brokerStatus?.configured);
      tradeStatus("Position close submitted.", false);
      await refreshTradeData({ silent: true });
    } catch (error) {
      state.trade.brokerStatus = brokerStatusFromPayload(error?.payload || { broker: state.trade.brokerStatus });
      state.trade.brokerConfigured = Boolean(state.trade.brokerStatus?.configured);
      tradeStatus(error.message || "Close position failed.", true);
    } finally {
      setTradeBusy(false);
    }
  }

  async function submitAmendPosition(positionId, stopLoss, takeProfit) {
    if (!state.trade.authenticated || state.trade.actionBusy) {
      return;
    }
    const position = activePositionById(positionId);
    if (!position) {
      tradeStatus("Position no longer exists.", true);
      return;
    }
    const payload = { positionId: Number(positionId) };
    if (stopLoss != null) {
      payload.stopLoss = Number(stopLoss);
    } else if (position.stopLoss != null) {
      payload.clearStopLoss = true;
    }
    if (takeProfit != null) {
      payload.takeProfit = Number(takeProfit);
    } else if (position.takeProfit != null) {
      payload.clearTakeProfit = true;
    }
    if (payload.stopLoss == null && payload.takeProfit == null && !payload.clearStopLoss && !payload.clearTakeProfit) {
      tradeStatus("Provide SL or TP before amending.", true);
      return;
    }
    setTradeBusy(true);
    try {
      const response = await tradeFetchJson("/api/trade/position/amend-sltp", {
        method: "POST",
        body: JSON.stringify(payload),
      });
      state.trade.brokerStatus = brokerStatusFromPayload(response);
      state.trade.brokerConfigured = Boolean(state.trade.brokerStatus?.configured);
      discardPendingProtection(positionId);
      tradeStatus("Position protections updated.", false);
      await refreshTradeData({ silent: true });
    } catch (error) {
      state.trade.brokerStatus = brokerStatusFromPayload(error?.payload || { broker: state.trade.brokerStatus });
      state.trade.brokerConfigured = Boolean(state.trade.brokerStatus?.configured);
      tradeStatus(error.message || "Amend SL/TP failed.", true);
      throw error;
    } finally {
      setTradeBusy(false);
    }
  }

  function requestProtectionDrag(positionId, targetPrice, originKey) {
    const position = activePositionById(positionId);
    if (!position) {
      tradeStatus("Position no longer exists.", true);
      return;
    }
    if (state.trade.actionBusy) {
      tradeStatus("Sending...", false);
      return;
    }
    const targetKey = protectionKeyForDrop(position, targetPrice);
    if (!targetKey) {
      tradeStatus("Protection type could not be resolved.", true);
      return;
    }
    const roundedPrice = Number(Number(targetPrice).toFixed(2));
    const previousPending = { ...(state.trade.pendingProtectionEdits[String(positionId)] || {}) };
    const draft = pendingProtectionForPosition(position);
    const nextValues = {
      stopLoss: draft.stopLoss,
      takeProfit: draft.takeProfit,
    };
    if (originKey && originKey !== targetKey) {
      nextValues[originKey] = null;
    }
    nextValues[targetKey] = roundedPrice;
    setPendingProtectionValue(positionId, "stopLoss", nextValues.stopLoss);
    setPendingProtectionValue(positionId, "takeProfit", nextValues.takeProfit);
    tradeStatus((targetKey === "stopLoss" ? "SL" : "TP") + " applying...", false);
    submitAmendPosition(positionId, nextValues.stopLoss, nextValues.takeProfit).catch(() => {
      const pendingKey = String(positionId);
      if (Object.keys(previousPending).length) {
        state.trade.pendingProtectionEdits[pendingKey] = previousPending;
      } else {
        delete state.trade.pendingProtectionEdits[pendingKey];
      }
      renderPositionEditor();
      queueOverlayRender();
    });
  }

  async function loadTradeSession() {
    try {
      const payload = await tradeFetchJson("/api/trade/me");
      applyTradeSessionPayload(payload);
      renderTradeLists();
      if (payload.authConfigured === false) {
        tradeStatus(payload.message || "Trade login is not configured on the server.", true);
        return;
      }
      if (payload.authenticated) {
        tradeStatus("Trade session active.", false);
        await refreshTradeData({ silent: true }).catch((error) => {
          tradeStatus(error.message || "Trade refresh failed.", true);
        });
        return;
      }
      tradeStatus("Trade login required.", false);
    } catch (error) {
      applyTradeSessionPayload({
        authenticated: false,
        username: null,
        authConfigured: true,
        brokerConfigured: state.trade.brokerConfigured,
        configured: state.trade.brokerConfigured,
        broker: error?.payload?.broker || tradePayloadDetail(error?.payload)?.broker || state.trade.brokerStatus,
      });
      renderTradeLists();
      tradeStatus(error.message || "Trade session check failed.", true);
    }
  }

  function setupTradePanel() {
    if (!elements.tradePanel) {
      return;
    }
    applyTradeSessionPayload({
      authenticated: false,
      username: null,
      authConfigured: true,
      brokerConfigured: false,
      configured: false,
      broker: { configured: false, state: "not_configured", reason: "Broker integration is not configured." },
    });
    renderTradeLists();
    tradeStatus("Trade login required.", false);

    elements.tradeLoginForm.addEventListener("submit", function (event) {
      event.preventDefault();
      requestTradeLogin();
    });
    elements.tradeLogoutButton.addEventListener("click", function () {
      requestTradeLogout();
    });
    elements.chartTradeBuyButton.addEventListener("click", function () {
      submitMarketOrder("buy");
    });
    elements.chartTradeSellButton.addEventListener("click", function () {
      submitMarketOrder("sell");
    });
    elements.tradeOpenList.addEventListener("click", function (event) {
      const button = event.target.closest("button[data-action]");
      if (!button) {
        return;
      }
      const action = button.dataset.action;
      const positionId = Number(button.dataset.positionId);
      if (action === "select-position") {
        setActiveTradePosition(positionId);
        return;
      }
      if (action === "close-position" || action === "close-half-position") {
        submitClosePosition(positionId, Number(button.dataset.volume || 0));
        return;
      }
    });
    elements.tradeHistoryList.addEventListener("click", function (event) {
      const button = event.target.closest("button[data-action=\"select-trade\"]");
      if (!button || button.disabled) {
        return;
      }
      const trade = completedTradeByKey(button.dataset.tradeKey);
      if (!trade) {
        status("The selected trade is no longer available in recent history.", true);
        return;
      }
      loadSelectedTradeReview(trade, { resetView: true }).catch((error) => {
        status(error.message || "Trade review load failed.", true);
      });
    });
    [elements.tradeReviewTicksBefore, elements.tradeReviewTicksAfter].forEach(function (input) {
      input.addEventListener("change", function () {
        reloadSelectedTradeReviewFromSettings();
      });
    });
    [elements.chartPositionStopLoss, elements.chartPositionTakeProfit].forEach(function (input) {
      input.addEventListener("input", function () {
        const position = activeTradePosition();
        if (!position) {
          return;
        }
        try {
          const fallback = position[input === elements.chartPositionStopLoss ? "stopLoss" : "takeProfit"];
          const nextValue = sanitizeEditableProtectionValue(input.value, fallback);
          setPendingProtectionValue(position.positionId, input === elements.chartPositionStopLoss ? "stopLoss" : "takeProfit", nextValue);
        } catch (error) {
          tradeStatus(error.message || "Invalid protection value.", true);
        }
      });
    });
    elements.chartPositionCancelButton.addEventListener("click", function () {
      const position = activeTradePosition();
      if (!position) {
        return;
      }
      discardPendingProtection(position.positionId);
      tradeStatus("Pending protection changes cleared.", false);
    });
    elements.chartPositionConfirmButton.addEventListener("click", function () {
      const position = activeTradePosition();
      if (!position) {
        return;
      }
      const draft = pendingProtectionForPosition(position);
      submitAmendPosition(position.positionId, draft.stopLoss, draft.takeProfit).catch(function () {});
    });

    [elements.tradePreparedLotSize, elements.tradePreparedStopLoss, elements.tradePreparedTakeProfit].forEach(function (input) {
      input.addEventListener("input", function () {
        renderTradeEntryOverlay();
      });
    });
    elements.tradePreparedPresets.addEventListener("click", function (event) {
      const button = event.target.closest("button[data-lot-size]");
      if (!button || !elements.tradePreparedLotSize) {
        return;
      }
      elements.tradePreparedLotSize.value = String(button.dataset.lotSize || TRADE_DEFAULT_LOT_SIZE);
      renderTradeEntryOverlay();
    });
    syncPreparedTradeInputs();
    renderTradeEntryOverlay();
    renderPositionEditor();
    loadTradeSession();
  }

  async function loadSelectedTradeReview(trade, options) {
    if (!trade?.entryTimestamp || !trade?.exitTimestamp) {
      throw new Error("The selected trade does not have a complete entry and exit.");
    }
    const settings = currentTradeReviewSettings();
    const [entryPayload, exitPayload] = await Promise.all([
      fetchJson("/api/live/review-start?" + new URLSearchParams({
        timestamp: trade.entryTimestamp,
        timezoneName: "Australia/Sydney",
      }).toString()),
      fetchJson("/api/live/review-start?" + new URLSearchParams({
        timestamp: trade.exitTimestamp,
        timezoneName: "Australia/Sydney",
      }).toString()),
    ]);
    const entryTickId = Number(entryPayload?.resolvedId);
    const exitTickId = Number(exitPayload?.resolvedId);
    if (!Number.isFinite(entryTickId) || !Number.isFinite(exitTickId)) {
      throw new Error("The selected trade could not be mapped to chart ticks.");
    }
    const startId = Math.max(1, Math.min(entryTickId, exitTickId) - settings.beforeTicks);
    const visibleSpan = Math.max(1, Math.abs(exitTickId - entryTickId) + 1);
    const reviewWindow = sanitizeWindowValue(visibleSpan + settings.beforeTicks + settings.afterTicks);
    state.trade.selectedHistoricalTradeOverlay = {
      key: selectedHistoricalTradeKey(trade),
      trade,
      entryTickId,
      exitTickId,
      startId,
      window: reviewWindow,
      beforeTicks: settings.beforeTicks,
      afterTicks: settings.afterTicks,
    };
    setSegment(elements.modeToggle, "review");
    setSegment(elements.runToggle, "stop");
    updateReviewFields();
    renderTradeEntryOverlay();
    renderPositionEditor();
    elements.tickId.value = String(startId);
    elements.reviewStart.value = "";
    elements.windowSize.value = String(reviewWindow);
    renderTradeLists();
    writeQuery();
    await loadAll(options?.resetView !== false);
    status(options?.fromSettingsChange ? "Selected trade review window updated." : "Selected trade loaded for review.", false);
  }

  function reloadSelectedTradeReviewFromSettings() {
    const selected = selectedHistoricalTradeOverlay();
    if (!selected || !reviewTradeSelectionReady()) {
      renderTradeReviewControls();
      return;
    }
    loadSelectedTradeReview(selected.trade, { resetView: false, fromSettingsChange: true }).catch((error) => {
      status(error.message || "Trade review reload failed.", true);
    });
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
    renderTradeLists();
    renderTradeEntryOverlay();
    renderPositionEditor();
    writeQuery();
    renderChart({ shiftWithRun: false });
    status("Mode updated. Click Load to refresh data.", false);
  });

  bindSegment(elements.runToggle, function (value) {
    setSegment(elements.runToggle, value);
    renderTradeLists();
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
    if (event.key !== "Escape") {
      return;
    }
    if (!state.ui.sidebarCollapsed) {
      setSidebarCollapsed(true);
    }
  });

  applyInitialConfig(parseQuery());
  setupTradePanel();
  loadAll(true);
}());
