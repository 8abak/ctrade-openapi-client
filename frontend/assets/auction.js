(function () {
  const DEFAULTS = {
    mode: "live",
    run: "run",
    id: "",
    reviewStart: "",
    reviewSpeed: 1,
    window: 2000,
    focusKind: "brokerday",
    showValueArea: true,
    showPoc: true,
    showRefs: true,
    showEvents: true,
    showHistory: false,
    showHeavyOverlays: false,
  };
  const MAX_WINDOW = 10000;
  const REVIEW_SPEEDS = [0.5, 1, 2, 3, 5];
  const HISTORY_REFRESH_DELAY_MS = 750;
  const LIVE_RECONNECT_DELAY_MS = 1000;
  const MIN_VISIBLE_PRICE_RANGE = 0.6;
  const DEFAULT_PROFILE_PRICE_STEP = 0.1;
  const TRADE_POLL_INTERVAL_MS = 15000;
  const SMART_POLL_INTERVAL_MS = 2000;
  const SYDNEY_TIMEZONE = "Australia/Sydney";
  const FOCUS_LABELS = {
    brokerday: "Broker Day",
    london: "London Session",
    newyork: "New York Session",
    rolling15m: "Rolling 15m",
    rolling60m: "Rolling 60m",
    rolling240m: "Rolling 240m",
    rolling24h: "Rolling 24h",
  };
  const HISTORY_SESSION_COLORS = {
    brokerday: { line: "rgba(255, 179, 92, 0.80)", area: "rgba(255, 179, 92, 0.08)" },
    london: { line: "rgba(109, 216, 255, 0.84)", area: "rgba(109, 216, 255, 0.07)" },
    newyork: { line: "rgba(126, 240, 199, 0.84)", area: "rgba(126, 240, 199, 0.07)" },
  };
  const SYDNEY_DATE_TIME_FORMATTER = new Intl.DateTimeFormat("en-AU", {
    timeZone: SYDNEY_TIMEZONE,
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
  const SYDNEY_TIME_FORMATTER = new Intl.DateTimeFormat("en-AU", {
    timeZone: SYDNEY_TIMEZONE,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
  const SYDNEY_AXIS_TIME_FORMATTER = new Intl.DateTimeFormat("en-AU", {
    timeZone: SYDNEY_TIMEZONE,
    day: "2-digit",
    month: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });

  const state = {
    chart: null,
    profileChart: null,
    rows: [],
    source: null,
    tickSource: null,
    auctionSource: null,
    reviewTimer: 0,
    reconnectTimer: 0,
    reviewEndId: null,
    lastMetrics: null,
    lastAuctionMetrics: null,
    streamConnected: false,
    auctionStreamConnected: false,
    auction: null,
    history: {
      sessions: [],
      refreshTimer: 0,
      loading: false,
      lastRangeKey: "",
    },
    renderFrame: 0,
    tickRenderFrame: 0,
    loadToken: 0,
    fastTickStats: {
      appendedCount: 0,
      browserLatencyMs: null,
      renderLatencyMs: null,
    },
    view: {
      followLive: true,
      fitMode: "price",
      yRange: null,
      xRange: null,
      userLockedX: false,
      userLockedY: false,
      applyingZoom: false,
    },
    ui: {
      sidebarCollapsed: true,
      chartFullscreen: false,
    },
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
      loading: false,
      pollTimer: 0,
      refreshPromise: null,
      pendingRefresh: false,
      positions: [],
      volumeInfo: null,
      lastLoadedAtMs: null,
      smart: {
        payload: null,
        pollTimer: 0,
        refreshPromise: null,
        lastTradeMutationId: 0,
      },
    },
  };

  const elements = {
    workspace: document.getElementById("auctionWorkspace"),
    sidebar: document.getElementById("auctionSidebar"),
    sidebarToggle: document.getElementById("sidebarToggle"),
    sidebarBackdrop: document.getElementById("sidebarBackdrop"),
    modeToggle: document.getElementById("modeToggle"),
    runToggle: document.getElementById("runToggle"),
    focusKind: document.getElementById("focusKind"),
    showValueArea: document.getElementById("showValueArea"),
    showPoc: document.getElementById("showPoc"),
    showRefs: document.getElementById("showRefs"),
    showEvents: document.getElementById("showEvents"),
    showHistory: document.getElementById("showHistory"),
    showHeavyOverlays: document.getElementById("showHeavyOverlays"),
    tickId: document.getElementById("tickId"),
    windowSize: document.getElementById("windowSize"),
    reviewStart: document.getElementById("reviewStart"),
    reviewSpeedToggle: document.getElementById("reviewSpeedToggle"),
    applyButton: document.getElementById("applyButton"),
    statusLine: document.getElementById("statusLine"),
    auctionMeta: document.getElementById("auctionMeta"),
    auctionPerf: document.getElementById("auctionPerf"),
    statusStrip: document.getElementById("statusStrip"),
    contextSection: document.getElementById("contextSection"),
    focusLabel: document.getElementById("focusLabel"),
    contextSummaryLine: document.getElementById("contextSummaryLine"),
    focusSummary: document.getElementById("focusSummary"),
    referenceList: document.getElementById("referenceList"),
    ladderList: document.getElementById("ladderList"),
    profileLabel: document.getElementById("profileLabel"),
    followLiveButton: document.getElementById("followLiveButton"),
    fitPriceActionButton: document.getElementById("fitPriceActionButton"),
    fitAuctionRefsButton: document.getElementById("fitAuctionRefsButton"),
    resetViewButton: document.getElementById("resetViewButton"),
    chartViewLabel: document.getElementById("chartViewLabel"),
    eventsPanel: document.getElementById("eventsPanel"),
    eventCount: document.getElementById("eventCount"),
    eventSummaryLine: document.getElementById("eventSummaryLine"),
    eventRibbon: document.getElementById("eventRibbon"),
    eventListShell: document.getElementById("eventListShell"),
    loginPanel: document.getElementById("loginPanel"),
    loginStatePill: document.getElementById("loginStatePill"),
    loginSummaryLine: document.getElementById("loginSummaryLine"),
    tradeStatusLine: document.getElementById("tradeStatusLine"),
    tradeLoginForm: document.getElementById("tradeLoginForm"),
    tradeUsername: document.getElementById("tradeUsername"),
    tradePassword: document.getElementById("tradePassword"),
    tradeLoginButton: document.getElementById("tradeLoginButton"),
    tradeSessionShell: document.getElementById("tradeSessionShell"),
    tradeSessionSummary: document.getElementById("tradeSessionSummary"),
    tradeBrokerSummary: document.getElementById("tradeBrokerSummary"),
    tradeLogoutButton: document.getElementById("tradeLogoutButton"),
    buttonsPanel: document.getElementById("buttonsPanel"),
    buttonStatePill: document.getElementById("buttonStatePill"),
    buttonsSummaryLine: document.getElementById("buttonsSummaryLine"),
    auctionSmartBuyButton: document.getElementById("auctionSmartBuyButton"),
    auctionSmartSellButton: document.getElementById("auctionSmartSellButton"),
    auctionSmartCloseButton: document.getElementById("auctionSmartCloseButton"),
    auctionSmartStatus: document.getElementById("auctionSmartStatus"),
    auctionTradeHint: document.getElementById("auctionTradeHint"),
    chartPanel: document.getElementById("chartPanel"),
    chartHost: document.getElementById("auctionChart"),
    profileChartHost: document.getElementById("auctionProfileChart"),
    chartFullscreenButton: document.getElementById("chartFullscreenButton"),
  };

  function sanitizeWindowValue(rawValue) {
    return Math.max(200, Math.min(MAX_WINDOW, Number.parseInt(rawValue || String(DEFAULTS.window), 10) || DEFAULTS.window));
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

  function formatPositionSide(side) {
    return String(side || "").toLowerCase() === "sell" ? "SELL" : "BUY";
  }

  function formatSignedPrice(value) {
    const number = Number(value);
    if (!Number.isFinite(number)) {
      return "-";
    }
    const fixed = number.toFixed(2);
    return number > 0 ? "+" + fixed : fixed;
  }

  function formatPercent(value) {
    const number = Number(value);
    return Number.isFinite(number) ? Math.round(number * 100) + "%" : "-";
  }

  function toDate(value) {
    if (value == null || value === "") {
      return null;
    }
    const date = value instanceof Date ? value : new Date(value);
    return Number.isFinite(date.getTime()) ? date : null;
  }

  function formatSydneyDateTime(value) {
    const date = toDate(value);
    return date ? (SYDNEY_DATE_TIME_FORMATTER.format(date) + " Sydney") : "-";
  }

  function formatSydneyTime(value) {
    const date = toDate(value);
    return date ? (SYDNEY_TIME_FORMATTER.format(date) + " Sydney") : "-";
  }

  function formatSydneyAxisTime(value) {
    const date = toDate(value);
    return date ? SYDNEY_AXIS_TIME_FORMATTER.format(date).replace(",", "") : "";
  }

  function focusRangeText(focus) {
    if (!focus?.startTs || !focus?.endTs) {
      return "";
    }
    return formatSydneyDateTime(focus.startTs) + " -> " + formatSydneyDateTime(focus.endTs);
  }

  function toneFromDirection(value) {
    const number = Number(value);
    if (!Number.isFinite(number) || number === 0) {
      return "neutral";
    }
    return number > 0 ? "positive" : "negative";
  }

  function collectNumbers(values) {
    return (values || []).map(function (value) { return Number(value); }).filter(function (value) { return Number.isFinite(value); });
  }

  function normalizeRange(minValue, maxValue, fallbackSpan) {
    const min = Number(minValue);
    const max = Number(maxValue);
    const span = Math.max(0.5, Number(fallbackSpan) || 1);
    if (!Number.isFinite(min) && !Number.isFinite(max)) {
      return null;
    }
    if (!Number.isFinite(min)) {
      return { min: max - span, max: max };
    }
    if (!Number.isFinite(max)) {
      return { min: min, max: min + span };
    }
    if (min === max) {
      return { min: min - (span / 2), max: max + (span / 2) };
    }
    return min < max ? { min: min, max: max } : { min: max, max: min };
  }

  function clampRange(range, bounds) {
    if (!range || !bounds) {
      return range || bounds || null;
    }
    const boundSpan = Math.max(0.5, bounds.max - bounds.min);
    const targetSpan = Math.min(Math.max(0.5, range.max - range.min), boundSpan);
    let min = range.min;
    let max = range.max;
    if (!Number.isFinite(min) || !Number.isFinite(max)) {
      return { min: bounds.min, max: bounds.max };
    }
    if (targetSpan >= boundSpan) {
      return { min: bounds.min, max: bounds.max };
    }
    if (min < bounds.min) {
      max += bounds.min - min;
      min = bounds.min;
    }
    if (max > bounds.max) {
      min -= max - bounds.max;
      max = bounds.max;
    }
    if (min < bounds.min) {
      min = bounds.min;
    }
    if (max > bounds.max) {
      max = bounds.max;
    }
    return { min: min, max: max };
  }

  function rangeFromNumbers(values, padding) {
    const numbers = collectNumbers(values);
    if (!numbers.length) {
      return null;
    }
    const min = Math.min.apply(null, numbers);
    const max = Math.max.apply(null, numbers);
    const pad = Math.max(0.4, Number(padding) || 0);
    return normalizeRange(min - pad, max + pad, pad * 2);
  }

  function readDataZoomWindow(id) {
    if (!state.chart) {
      return null;
    }
    const option = state.chart.getOption();
    const zoom = (option.dataZoom || []).find(function (item) { return item.id === id; });
    if (!zoom) {
      return null;
    }
    const startValue = Number(zoom.startValue);
    const endValue = Number(zoom.endValue);
    if (!Number.isFinite(startValue) || !Number.isFinite(endValue)) {
      return null;
    }
    return normalizeRange(startValue, endValue, 1);
  }

  function currentFocusLabel() {
    return FOCUS_LABELS[elements.focusKind.value] || "Auction";
  }

  function clipRowsToBrokerdayWindow() {
    const focus = state.auction?.focusWindow || null;
    if (currentConfig().focusKind !== "brokerday" || focus?.sessionKind !== "brokerday") {
      return;
    }
    const startTsMs = Number(focus.startTsMs);
    const endTsMs = Number(focus.endTsMs);
    if (!Number.isFinite(startTsMs) || !Number.isFinite(endTsMs)) {
      return;
    }
    state.rows = state.rows.filter(function (row) {
      const rowTsMs = Number(row?.timestampMs);
      return Number.isFinite(rowTsMs) && rowTsMs >= startTsMs && rowTsMs <= endTsMs;
    });
  }

  function setAuctionSnapshot(snapshot) {
    state.auction = snapshot || null;
    clipRowsToBrokerdayWindow();
  }

  function parseQuery() {
    const params = new URLSearchParams(window.location.search);
    const speed = Number.parseFloat(params.get("speed") || String(DEFAULTS.reviewSpeed));
    const focusKind = params.get("focusKind") || DEFAULTS.focusKind;
    return {
      mode: params.get("mode") === "review" ? "review" : DEFAULTS.mode,
      run: params.get("run") === "stop" ? "stop" : DEFAULTS.run,
      id: params.get("id") || DEFAULTS.id,
      reviewStart: params.get("reviewStart") || DEFAULTS.reviewStart,
      reviewSpeed: REVIEW_SPEEDS.includes(speed) ? speed : DEFAULTS.reviewSpeed,
      window: sanitizeWindowValue(params.get("window")),
      focusKind: Object.prototype.hasOwnProperty.call(FOCUS_LABELS, focusKind) ? focusKind : DEFAULTS.focusKind,
      showValueArea: params.has("showValueArea") ? params.get("showValueArea") !== "0" : DEFAULTS.showValueArea,
      showPoc: params.has("showPoc") ? params.get("showPoc") !== "0" : DEFAULTS.showPoc,
      showRefs: params.has("showRefs") ? params.get("showRefs") !== "0" : DEFAULTS.showRefs,
      showEvents: params.has("showEvents") ? params.get("showEvents") !== "0" : DEFAULTS.showEvents,
      showHistory: params.has("showHistory") ? params.get("showHistory") !== "0" : DEFAULTS.showHistory,
      showHeavyOverlays: params.has("showHeavyOverlays") ? params.get("showHeavyOverlays") !== "0" : DEFAULTS.showHeavyOverlays,
    };
  }

  function currentConfig() {
    return {
      mode: elements.modeToggle.querySelector("button.active")?.dataset.value || DEFAULTS.mode,
      run: elements.runToggle.querySelector("button.active")?.dataset.value || DEFAULTS.run,
      id: (elements.tickId.value || "").trim(),
      reviewStart: (elements.reviewStart.value || "").trim(),
      reviewSpeed: Number.parseFloat(elements.reviewSpeedToggle.querySelector("button.active")?.dataset.value || String(DEFAULTS.reviewSpeed)),
      window: sanitizeWindowValue(elements.windowSize.value),
      focusKind: elements.focusKind.value || DEFAULTS.focusKind,
      showValueArea: elements.showValueArea.checked,
      showPoc: elements.showPoc.checked,
      showRefs: elements.showRefs.checked,
      showEvents: elements.showEvents.checked,
      showHistory: elements.showHistory.checked,
      showHeavyOverlays: elements.showHeavyOverlays.checked,
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
      window: String(config.window),
      speed: String(config.reviewSpeed),
      focusKind: config.focusKind,
      showValueArea: config.showValueArea ? "1" : "0",
      showPoc: config.showPoc ? "1" : "0",
      showRefs: config.showRefs ? "1" : "0",
      showEvents: config.showEvents ? "1" : "0",
      showHistory: config.showHistory ? "1" : "0",
      showHeavyOverlays: config.showHeavyOverlays ? "1" : "0",
    });
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

  function renderMeta() {
    const focus = state.auction?.focusWindow || null;
    if (!focus || !state.rows.length) {
      elements.auctionMeta.textContent = "No auction window loaded.";
      return;
    }
    const parts = [
      currentConfig().mode.toUpperCase(),
      focus.label || currentFocusLabel(),
      "ticks " + state.rows.length,
      "state " + (focus.stateKind || "Unknown"),
      "location " + (focus.locationKind || "Unknown"),
      "action " + (focus.preferredAction || "NoTrade"),
    ];
    if (state.history.sessions.length) {
      parts.push("history " + state.history.sessions.length);
    }
    if (focus.startTs && focus.endTs) {
      parts.push(focusRangeText(focus));
    }
    elements.auctionMeta.textContent = parts.join(" | ");
  }

  function renderPerf() {
    const tickMetrics = state.lastMetrics || {};
    const auctionMetrics = state.lastAuctionMetrics || {};
    const parts = [
      "Ticks " + (state.streamConnected ? "up" : "down"),
      "Auction " + (state.auctionStreamConnected ? "up" : "down"),
    ];
    if (tickMetrics.dbLatestId != null) {
      parts.push("DB " + tickMetrics.dbLatestId);
    }
    if (tickMetrics.fetchLatencyMs != null) {
      parts.push("Tick fetch " + Math.round(tickMetrics.fetchLatencyMs * 100) / 100 + "ms");
    }
    if (state.fastTickStats.browserLatencyMs != null) {
      parts.push("Wire " + Math.round(state.fastTickStats.browserLatencyMs) + "ms");
    }
    if (state.fastTickStats.appendedCount > 0) {
      parts.push("Append " + state.fastTickStats.appendedCount);
    }
    if (state.fastTickStats.renderLatencyMs != null) {
      parts.push("Paint " + Math.round(state.fastTickStats.renderLatencyMs) + "ms");
    }
    if (auctionMetrics.snapshotBuildLatencyMs != null) {
      parts.push("Auction build " + Math.round(auctionMetrics.snapshotBuildLatencyMs * 100) / 100 + "ms");
    }
    if (state.auction?.asOfTsMs != null) {
      parts.push("Auction age " + Math.max(0, Date.now() - state.auction.asOfTsMs) + "ms");
    }
    if (state.history.loading) {
      parts.push("History loading");
    }
    elements.auctionPerf.textContent = parts.join(" | ");
  }

  function setSidebarCollapsed(collapsed) {
    state.ui.sidebarCollapsed = Boolean(collapsed);
    elements.workspace.classList.toggle("is-sidebar-collapsed", state.ui.sidebarCollapsed);
    elements.sidebarToggle.setAttribute("aria-expanded", String(!state.ui.sidebarCollapsed));
    elements.sidebarToggle.setAttribute("aria-label", state.ui.sidebarCollapsed ? "Open auction controls" : "Close auction controls");
    elements.sidebarBackdrop.tabIndex = state.ui.sidebarCollapsed ? -1 : 0;
    syncChartLayout();
  }

  function syncChartLayout() {
    window.requestAnimationFrame(function () {
      if (state.chart) {
        state.chart.resize();
      }
      if (state.profileChart) {
        state.profileChart.resize();
      }
      if (state.rows.length) {
        renderChart();
      }
    });
  }

  function activeFullscreenElement() {
    return document.fullscreenElement
      || document.webkitFullscreenElement
      || document.msFullscreenElement
      || null;
  }

  function fullscreenSupported() {
    const panel = elements.chartPanel;
    return Boolean(
      panel
      && (
        typeof panel.requestFullscreen === "function"
        || typeof panel.webkitRequestFullscreen === "function"
        || typeof panel.msRequestFullscreen === "function"
      )
    );
  }

  function requestElementFullscreen(element) {
    if (!element) {
      return Promise.reject(new Error("Fullscreen target is unavailable."));
    }
    if (typeof element.requestFullscreen === "function") {
      return element.requestFullscreen();
    }
    if (typeof element.webkitRequestFullscreen === "function") {
      return element.webkitRequestFullscreen();
    }
    if (typeof element.msRequestFullscreen === "function") {
      return element.msRequestFullscreen();
    }
    return Promise.reject(new Error("Fullscreen is not available in this browser."));
  }

  function exitAnyFullscreen() {
    if (typeof document.exitFullscreen === "function") {
      return document.exitFullscreen();
    }
    if (typeof document.webkitExitFullscreen === "function") {
      return document.webkitExitFullscreen();
    }
    if (typeof document.msExitFullscreen === "function") {
      return document.msExitFullscreen();
    }
    return Promise.resolve();
  }

  function updateChartFullscreenUi() {
    const isFullscreen = activeFullscreenElement() === elements.chartPanel;
    state.ui.chartFullscreen = isFullscreen;
    elements.chartPanel?.classList.toggle("is-fullscreen", isFullscreen);
    if (elements.chartFullscreenButton) {
      elements.chartFullscreenButton.textContent = isFullscreen ? "Exit Fullscreen" : "Fullscreen";
      elements.chartFullscreenButton.classList.toggle("is-active", isFullscreen);
      elements.chartFullscreenButton.setAttribute("aria-pressed", String(isFullscreen));
    }
    syncChartLayout();
  }

  async function toggleChartFullscreen() {
    if (!fullscreenSupported()) {
      status("Fullscreen is not available in this browser.", true);
      return;
    }
    try {
      if (activeFullscreenElement() === elements.chartPanel) {
        await exitAnyFullscreen();
      } else {
        await requestElementFullscreen(elements.chartPanel);
      }
    } catch (error) {
      status(error.message || "Fullscreen request failed.", true);
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

  function tradePayloadDetail(payload) {
    return payload?.detail && typeof payload.detail === "object" ? payload.detail : null;
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

  async function fetchJson(url, options) {
    const response = await fetch(url, options);
    const payload = await response.json().catch(function () {
      return {};
    });
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

  async function resolveReviewStartId(config) {
    if (config.reviewStart) {
      const payload = await fetchJson("/api/auction/review-start?" + new URLSearchParams({
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

  function tradeStatus(message, isError) {
    if (!elements.tradeStatusLine) {
      return;
    }
    elements.tradeStatusLine.textContent = message;
    elements.tradeStatusLine.classList.toggle("error", Boolean(isError));
    elements.tradeStatusLine.classList.toggle("success", Boolean(!isError));
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
      reason: reason,
      code: typeof broker?.code === "string" && broker.code ? broker.code : null,
      symbol: typeof broker?.symbol === "string" && broker.symbol ? broker.symbol : null,
      symbolId: broker?.symbolId ?? null,
      lastError: typeof broker?.lastError === "string" && broker.lastError ? broker.lastError : reason,
    };
  }

  function tradeConsole(method, url, error) {
    const message = String(error?.message || "");
    const expected = error?.code === "TRADE_AUTH_NOT_CONFIGURED" || message.toLowerCase().includes("trade login required");
    if (expected || !window.console) {
      return;
    }
    const key = [method, url, error?.status ?? null, error?.code ?? null, message].join("|");
    if (state.trade.lastLoggedErrorKey === key) {
      return;
    }
    state.trade.lastLoggedErrorKey = key;
    if (typeof window.console.error === "function") {
      window.console.error("[auction-trade] " + method + " " + url + " failed", {
        status: error?.status ?? null,
        code: error?.code ?? null,
        message: message,
      });
    }
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

  function stopSmartPolling() {
    if (state.trade.smart.pollTimer) {
      window.clearTimeout(state.trade.smart.pollTimer);
      state.trade.smart.pollTimer = 0;
    }
  }

  function scheduleTradePolling() {
    stopTradePolling();
    if (!state.trade.authenticated) {
      return;
    }
    state.trade.pollTimer = window.setTimeout(function () {
      state.trade.pollTimer = 0;
      refreshTradeData({ silent: true }).catch(function (error) {
        tradeStatus(error.message || "Trade refresh failed.", true);
      });
    }, TRADE_POLL_INTERVAL_MS);
  }

  function scheduleSmartPolling() {
    stopSmartPolling();
    if (!state.trade.authenticated || !smartContextReady()) {
      return;
    }
    state.trade.smart.pollTimer = window.setTimeout(function () {
      state.trade.smart.pollTimer = 0;
      refreshSmartState({ silent: true }).catch(function (error) {
        tradeStatus(error.message || "Smart scalp refresh failed.", true);
      });
    }, SMART_POLL_INTERVAL_MS);
  }

  function clearTradeRuntimeState() {
    stopTradePolling();
    stopSmartPolling();
    state.trade.refreshPromise = null;
    state.trade.pendingRefresh = false;
    state.trade.positions = [];
    state.trade.volumeInfo = null;
    state.trade.lastLoadedAtMs = null;
    state.trade.smart.payload = null;
    state.trade.smart.lastTradeMutationId = 0;
    if (state.chart) {
      renderChart();
    }
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

  function smartPayload() {
    return state.trade.smart.payload || {
      context: { enabled: false, reason: "Smart scalping unavailable." },
      state: {
        armed: { buy: false, sell: false, close: true },
        backendState: "armed_close_waiting",
        statusText: "Smart Close armed. Waiting for a single open position.",
        cooldownRemainingMs: 0,
        currentPosition: null,
        openPositionCount: 0,
      },
      broker: state.trade.brokerStatus,
    };
  }

  function currentSmartArmed(key) {
    return Boolean(smartPayload()?.state?.armed?.[key]);
  }

  function smartContextReady() {
    return currentConfig().mode === "live" && currentConfig().run === "run";
  }

  function auctionTradeOverlayActive() {
    return currentConfig().mode === "live" && state.trade.positions.length > 0;
  }

  function tradeOverlayPrices() {
    if (!auctionTradeOverlayActive()) {
      return [];
    }
    return collectNumbers((state.trade.positions || []).map(function (position) {
      return position.entryPrice;
    }));
  }

  function currentTradeVolumeInfo() {
    return state.trade.volumeInfo || { defaultLotSize: 0.01 };
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

  function positionLots(position) {
    const direct = Number(position?.volumeLots);
    if (Number.isFinite(direct)) {
      return direct;
    }
    return volumeToLots(position?.volume);
  }

  function applySmartPayload(payload) {
    const resolved = payload?.smart || payload;
    if (!resolved || typeof resolved !== "object") {
      return;
    }
    state.trade.smart.payload = resolved;
    const mutationId = Number(resolved?.state?.lastTradeMutationId || 0);
    if (mutationId > Number(state.trade.smart.lastTradeMutationId || 0)) {
      state.trade.smart.lastTradeMutationId = mutationId;
      if (state.trade.authenticated) {
        refreshTradeData({ silent: true }).catch(function () {});
      }
    }
  }

  function smartAvailability(kind) {
    const smart = smartPayload();
    if (!state.trade.authConfigured) {
      return { available: false, reason: "Trade login is not configured on the server." };
    }
    if (!state.trade.authenticated) {
      return { available: false, reason: "Login required." };
    }
    if (!state.trade.brokerConfigured || !state.trade.brokerStatus?.ready || !state.trade.lastLoadedAtMs) {
      return { available: false, reason: brokerUnavailableReason() };
    }
    if (!smartContextReady()) {
      return { available: false, reason: "Smart controls require Live + Run." };
    }
    if (!smart.context?.enabled && smart.context?.reason) {
      return { available: false, reason: smart.context.reason };
    }
    if ((kind === "buy" || kind === "sell") && state.trade.positions.length) {
      return { available: false, reason: "Position already open." };
    }
    return { available: true, reason: "" };
  }

  function smartSummaryText() {
    const smart = smartPayload();
    const stateValue = smart.state || {};
    const armed = [];
    if (stateValue.armed?.buy) {
      armed.push("Smart Buy armed");
    }
    if (stateValue.armed?.sell) {
      armed.push("Smart Sell armed");
    }
    if (stateValue.armed?.close) {
      armed.push("Smart Close armed");
    }
    if (armed.length) {
      return armed.join(" | ");
    }
    return stateValue.statusText || smart.context?.reason || "Smart scalping idle.";
  }

  function tradeSessionSummaryText() {
    if (!state.trade.authConfigured) {
      return "Trade login is not configured on the server.";
    }
    if (!state.trade.authenticated) {
      return "Trade login required for auction controls.";
    }
    return "Session unlocked for " + (state.trade.username || "trade user") + ".";
  }

  function brokerSummaryText() {
    const broker = state.trade.brokerStatus;
    if (!state.trade.authenticated) {
      return brokerUnavailableReason();
    }
    if (!state.trade.brokerConfigured || !broker?.ready) {
      return brokerUnavailableReason();
    }
    const positions = state.trade.positions.length;
    return [
      broker.symbol || "Broker ready",
      positions + " open " + (positions === 1 ? "position" : "positions"),
      broker.connected ? "connected" : "connection unknown",
    ].join(" | ");
  }

  function tradeHintText() {
    const smart = smartPayload();
    if (!state.trade.authConfigured) {
      return "Trade login is not configured on the server.";
    }
    if (!state.trade.authenticated) {
      return "Login required.";
    }
    if (!smartContextReady()) {
      return "Smart controls require Live + Run.";
    }
    if (!state.trade.brokerConfigured || !state.trade.brokerStatus?.ready || !state.trade.lastLoadedAtMs) {
      return brokerUnavailableReason();
    }
    if (currentSmartArmed("close") && state.trade.positions.length !== 1) {
      return "Smart Close is armed and waiting for a single open position.";
    }
    return state.trade.positions.length
      ? (state.trade.positions.length + " open " + (state.trade.positions.length === 1 ? "position." : "positions."))
      : "No open position. Smart Close can stay armed.";
  }

  function setTradeBusy(busy) {
    const disabled = Boolean(busy);
    state.trade.actionBusy = disabled;
    [
      elements.tradeUsername,
      elements.tradePassword,
      elements.tradeLoginButton,
      elements.tradeLogoutButton,
      elements.auctionSmartBuyButton,
      elements.auctionSmartSellButton,
      elements.auctionSmartCloseButton,
    ].forEach(function (element) {
      if (element) {
        element.disabled = disabled;
      }
    });
    renderLoginPanel();
    renderButtonsPanel();
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
    renderLoginPanel();
    renderButtonsPanel();
  }

  function renderLoginPanel() {
    if (!elements.loginPanel) {
      return;
    }
    const authConfigured = state.trade.authConfigured;
    const authenticated = state.trade.authenticated;
    if (elements.loginStatePill) {
      elements.loginStatePill.textContent = authenticated ? "Ready" : (authConfigured ? "Locked" : "Unavailable");
    }
    if (elements.loginSummaryLine) {
      elements.loginSummaryLine.textContent = authenticated
        ? ((state.trade.username || "trade user") + " | " + brokerSummaryText())
        : tradeSessionSummaryText();
    }
    if (elements.tradeLoginForm) {
      elements.tradeLoginForm.hidden = authenticated || !authConfigured;
    }
    if (elements.tradeSessionShell) {
      elements.tradeSessionShell.hidden = !authenticated;
    }
    if (elements.tradeUsername) {
      elements.tradeUsername.disabled = !authConfigured || state.trade.loginBusy || state.trade.actionBusy;
    }
    if (elements.tradePassword) {
      elements.tradePassword.disabled = !authConfigured || state.trade.loginBusy || state.trade.actionBusy;
    }
    if (elements.tradeLoginButton) {
      elements.tradeLoginButton.disabled = !authConfigured || state.trade.loginBusy || state.trade.actionBusy;
      elements.tradeLoginButton.textContent = state.trade.loginBusy ? "Logging in..." : "Login";
    }
    if (elements.tradeSessionSummary) {
      elements.tradeSessionSummary.textContent = tradeSessionSummaryText();
    }
    if (elements.tradeBrokerSummary) {
      elements.tradeBrokerSummary.textContent = brokerSummaryText();
    }
  }

  function renderButtonsPanel() {
    if (!elements.buttonsPanel) {
      return;
    }
    const armedCount = ["buy", "sell", "close"].filter(currentSmartArmed).length;
    const buyAvailability = smartAvailability("buy");
    const sellAvailability = smartAvailability("sell");
    const closeAvailability = smartAvailability("close");
    const summaryText = state.trade.authenticated ? smartSummaryText() : tradeHintText();
    if (elements.buttonStatePill) {
      elements.buttonStatePill.textContent = armedCount ? "ON" : "OFF";
    }
    if (elements.buttonsSummaryLine) {
      elements.buttonsSummaryLine.textContent = summaryText;
    }
    [
      [elements.auctionSmartBuyButton, "buy", buyAvailability],
      [elements.auctionSmartSellButton, "sell", sellAvailability],
      [elements.auctionSmartCloseButton, "close", closeAvailability],
    ].forEach(function (entry) {
      const button = entry[0];
      const key = entry[1];
      const availability = entry[2];
      if (!button) {
        return;
      }
      button.disabled = state.trade.actionBusy || !availability.available;
      button.classList.toggle("is-armed", currentSmartArmed(key));
      button.textContent = "Smart " + key.charAt(0).toUpperCase() + key.slice(1) + " " + (currentSmartArmed(key) ? "ON" : "OFF");
    });
    if (elements.auctionSmartStatus) {
      elements.auctionSmartStatus.textContent = summaryText;
    }
    if (elements.auctionTradeHint) {
      elements.auctionTradeHint.textContent = tradeHintText();
    }
  }

  async function refreshTradeData(options) {
    if (!state.trade.authenticated) {
      return;
    }
    if (state.trade.refreshPromise) {
      state.trade.pendingRefresh = true;
      return state.trade.refreshPromise;
    }
    const silent = Boolean(options?.silent);
    state.trade.refreshPromise = (async function () {
      state.trade.loading = true;
      if (!silent) {
        tradeStatus("Loading trade state...", false);
      }
      try {
        const openPayload = await tradeFetchJson("/api/trade/open");
        state.trade.brokerStatus = brokerStatusFromPayload(openPayload);
        state.trade.brokerConfigured = Boolean(state.trade.brokerStatus?.configured);
        state.trade.volumeInfo = openPayload.volumeInfo || null;
        state.trade.positions = Array.isArray(openPayload.positions) ? openPayload.positions : [];
        state.trade.lastLoadedAtMs = Date.now();
        applySmartPayload(openPayload.smart);
        renderLoginPanel();
        renderButtonsPanel();
        if (state.chart) {
          renderChart();
        }
        if (!silent) {
          tradeStatus("Trade state updated.", false);
        }
        if (smartContextReady()) {
          scheduleSmartPolling();
        } else {
          stopSmartPolling();
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
        } else if (message.includes("trade login required")) {
          applyTradeSessionPayload({
            authenticated: false,
            username: null,
            authConfigured: true,
            brokerConfigured: state.trade.brokerConfigured,
            configured: state.trade.brokerConfigured,
            broker: state.trade.brokerStatus,
          });
        } else {
          state.trade.brokerStatus = brokerStatusFromPayload(error?.payload || { broker: state.trade.brokerStatus });
          state.trade.brokerConfigured = Boolean(state.trade.brokerStatus?.configured);
          state.trade.positions = [];
          state.trade.volumeInfo = null;
          state.trade.lastLoadedAtMs = null;
          renderLoginPanel();
          renderButtonsPanel();
          if (state.chart) {
            renderChart();
          }
        }
        throw error;
      } finally {
        state.trade.loading = false;
        state.trade.refreshPromise = null;
        if (state.trade.pendingRefresh && state.trade.authenticated) {
          state.trade.pendingRefresh = false;
          window.setTimeout(function () {
            refreshTradeData({ silent: true }).catch(function (error) {
              tradeStatus(error.message || "Trade refresh failed.", true);
            });
          }, 0);
        }
      }
    }());
    return state.trade.refreshPromise;
  }

  async function refreshSmartState(options) {
    if (!state.trade.authenticated) {
      return;
    }
    if (state.trade.smart.refreshPromise) {
      return state.trade.smart.refreshPromise;
    }
    state.trade.smart.refreshPromise = (async function () {
      try {
        const payload = await tradeFetchJson("/api/trade/smart");
        applySmartPayload(payload);
        renderButtonsPanel();
        if (!options?.silent) {
          tradeStatus("Smart scalp state updated.", false);
        }
        if (smartContextReady()) {
          scheduleSmartPolling();
        } else {
          stopSmartPolling();
        }
      } catch (error) {
        if (!options?.silent) {
          tradeStatus(error.message || "Smart scalp refresh failed.", true);
        }
        throw error;
      } finally {
        state.trade.smart.refreshPromise = null;
      }
    }());
    return state.trade.smart.refreshPromise;
  }

  async function syncSmartContext(options) {
    if (!state.trade.authenticated) {
      return;
    }
    try {
      const payload = await tradeFetchJson("/api/trade/smart/context", {
        method: "POST",
        body: JSON.stringify({
          page: "auction",
          mode: currentConfig().mode,
          run: currentConfig().run,
        }),
      });
      applySmartPayload(payload);
      renderButtonsPanel();
      if (!options?.silent) {
        tradeStatus("Smart scalp context synced.", false);
      }
      scheduleSmartPolling();
    } catch (error) {
      if (!options?.silent) {
        tradeStatus(error.message || "Smart scalp context sync failed.", true);
      }
      throw error;
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
    const username = (elements.tradeUsername?.value || "").trim();
    const password = elements.tradePassword?.value || "";
    if (!username || !password) {
      tradeStatus("Username and password are required.", true);
      return;
    }
    state.trade.loginBusy = true;
    renderLoginPanel();
    try {
      const payload = await tradeFetchJson("/api/trade/login", {
        method: "POST",
        body: JSON.stringify({ username: username, password: password }),
      });
      if (elements.tradePassword) {
        elements.tradePassword.value = "";
      }
      applyTradeSessionPayload({
        authenticated: true,
        username: payload.username || username,
        authConfigured: true,
        brokerConfigured: state.trade.brokerConfigured,
        configured: state.trade.brokerConfigured,
        broker: state.trade.brokerStatus,
      });
      tradeStatus("Trade login successful.", false);
      await refreshTradeData({ silent: true }).catch(function (error) {
        tradeStatus(error.message || "Trade refresh failed.", true);
      });
      await syncSmartContext({ silent: true }).catch(function () {});
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
      }
      tradeStatus(error.message || "Trade login failed.", true);
    } finally {
      state.trade.loginBusy = false;
      renderLoginPanel();
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
    tradeStatus("Trade session logged out.", false);
    setTradeBusy(false);
  }

  async function toggleSmartEntry(side) {
    if (!state.trade.authenticated || state.trade.actionBusy) {
      return;
    }
    await syncSmartContext({ silent: true }).catch(function () {});
    const nextArmed = !currentSmartArmed(side);
    setTradeBusy(true);
    try {
      const payload = await tradeFetchJson("/api/trade/smart/entry", {
        method: "POST",
        body: JSON.stringify({ side: side, armed: nextArmed }),
      });
      applySmartPayload(payload);
      renderButtonsPanel();
      tradeStatus(nextArmed ? ("Smart " + side.toUpperCase() + " armed.") : ("Smart " + side.toUpperCase() + " disarmed."), false);
      scheduleSmartPolling();
    } catch (error) {
      tradeStatus(error.message || "Smart entry update failed.", true);
    } finally {
      setTradeBusy(false);
    }
  }

  async function toggleSmartClose() {
    if (!state.trade.authenticated || state.trade.actionBusy) {
      return;
    }
    await syncSmartContext({ silent: true }).catch(function () {});
    const nextArmed = !currentSmartArmed("close");
    setTradeBusy(true);
    try {
      const payload = await tradeFetchJson("/api/trade/smart/close", {
        method: "POST",
        body: JSON.stringify({ armed: nextArmed }),
      });
      applySmartPayload(payload);
      renderButtonsPanel();
      tradeStatus(nextArmed ? "Smart Close armed." : "Smart Close disarmed.", false);
      scheduleSmartPolling();
    } catch (error) {
      tradeStatus(error.message || "Smart close update failed.", true);
    } finally {
      setTradeBusy(false);
    }
  }

  async function loadTradeSession() {
    try {
      const payload = await tradeFetchJson("/api/trade/me");
      applyTradeSessionPayload(payload);
      if (payload.authConfigured === false) {
        tradeStatus(payload.message || "Trade login is not configured on the server.", true);
        return;
      }
      if (payload.authenticated) {
        tradeStatus("Trade session active.", false);
        await refreshTradeData({ silent: true }).catch(function (error) {
          tradeStatus(error.message || "Trade refresh failed.", true);
        });
        await syncSmartContext({ silent: true }).catch(function () {});
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
      tradeStatus(error.message || "Trade session check failed.", true);
    }
  }

  function clearHistoryTimer() {
    if (state.history.refreshTimer) {
      window.clearTimeout(state.history.refreshTimer);
      state.history.refreshTimer = 0;
    }
  }

  function clearReconnectTimer() {
    if (state.reconnectTimer) {
      window.clearTimeout(state.reconnectTimer);
      state.reconnectTimer = 0;
    }
  }

  function closeLiveSources() {
    if (state.tickSource) {
      state.tickSource.close();
      state.tickSource = null;
    }
    if (state.auctionSource) {
      state.auctionSource.close();
      state.auctionSource = null;
    }
    state.streamConnected = false;
    state.auctionStreamConnected = false;
  }

  function clearActivity() {
    clearReconnectTimer();
    closeLiveSources();
    if (state.source) {
      state.source.close();
      state.source = null;
    }
    if (state.reviewTimer) {
      window.clearTimeout(state.reviewTimer);
      state.reviewTimer = 0;
    }
    clearHistoryTimer();
    renderPerf();
  }

  function applyPayload(payload) {
    state.rows = Array.isArray(payload.rows) ? payload.rows.slice() : [];
    state.reviewEndId = payload.reviewEndId || null;
    setAuctionSnapshot(payload.auction || null);
    state.lastMetrics = payload.metrics || null;
    state.lastAuctionMetrics = payload.metrics || null;
    state.fastTickStats.appendedCount = 0;
    state.fastTickStats.browserLatencyMs = null;
    state.fastTickStats.renderLatencyMs = null;
  }

  function trimRowsToWindow() {
    const cap = Math.max(200, Number(currentConfig().window) || DEFAULTS.window);
    if (state.rows.length > cap) {
      state.rows = state.rows.slice(state.rows.length - cap);
    }
  }

  function appendRows(rows) {
    const knownIds = new Set(state.rows.map(function (row) { return Number(row.id); }));
    let appendedCount = 0;
    (rows || []).forEach(function (row) {
      const rowId = Number(row.id || 0);
      if (!knownIds.has(rowId)) {
        state.rows.push(row);
        knownIds.add(rowId);
        appendedCount += 1;
      }
    });
    trimRowsToWindow();
    return appendedCount;
  }

  function currentRange() {
    if (!state.rows.length) {
      return null;
    }
    return {
      startTsMs: Number(state.rows[0].timestampMs),
      endTsMs: Number(state.rows[state.rows.length - 1].timestampMs),
    };
  }

  function historyRangeKey() {
    const range = currentRange();
    if (!range) {
      return "";
    }
    return [range.startTsMs, range.endTsMs, currentConfig().showRefs ? 1 : 0, currentConfig().showEvents ? 1 : 0].join(":");
  }

  async function refreshHistoryMarkers(force) {
    const config = currentConfig();
    const range = currentRange();
    if (!config.showHistory || !range) {
      state.history.sessions = [];
      state.history.lastRangeKey = "";
      renderMeta();
      renderPerf();
      renderChart();
      return;
    }
    const key = historyRangeKey();
    if (!force && key && key === state.history.lastRangeKey) {
      return;
    }
    state.history.loading = true;
    renderPerf();
    try {
      const payload = await fetchJson("/api/auction/history?" + new URLSearchParams({
        startTsMs: String(range.startTsMs),
        endTsMs: String(range.endTsMs),
        includeRefs: config.showRefs ? "1" : "0",
        includeEvents: config.showEvents ? "1" : "0",
        limitSessions: "36",
      }).toString());
      state.history.sessions = Array.isArray(payload.sessions) ? payload.sessions : [];
      state.history.lastRangeKey = key;
      renderMeta();
      renderChart();
    } catch (error) {
      state.history.sessions = [];
      state.history.lastRangeKey = "";
      status(error.message || "Auction history fetch failed.", true);
    } finally {
      state.history.loading = false;
      renderPerf();
    }
  }

  function scheduleHistoryRefresh(force) {
    clearHistoryTimer();
    if (!currentConfig().showHistory) {
      return;
    }
    state.history.refreshTimer = window.setTimeout(function () {
      state.history.refreshTimer = 0;
      refreshHistoryMarkers(Boolean(force)).catch(function (error) {
        status(error.message || "Auction history refresh failed.", true);
      });
    }, HISTORY_REFRESH_DELAY_MS);
  }

  function renderStatusStrip() {
    const focus = state.auction?.focusWindow || null;
    if (!focus) {
      elements.statusStrip.innerHTML = "";
      return;
    }
    const cards = [
      ["State", focus.stateKind || "Unknown", toneFromDirection(focus.valueDrift)],
      ["Open Type", focus.openType || "Unknown", "neutral"],
      ["Location", focus.locationKind || "Unknown", "neutral"],
      ["Acceptance", focus.acceptanceKind || "Neutral", focus.acceptanceKind === "Rejected" ? "negative" : (focus.acceptanceKind === "Accepted" ? "positive" : "neutral")],
      ["Inventory", focus.inventoryType || "Neutral", "neutral"],
      ["Preferred Action", focus.preferredAction || "NoTrade", (focus.preferredAction || "").includes("Sell") ? "negative" : ((focus.preferredAction || "").includes("Buy") ? "positive" : "neutral")],
      ["Confidence", formatPercent(focus.confidence), "neutral"],
    ];
    elements.statusStrip.innerHTML = cards.map(function (card) {
      return "<article class=\"auction-status-card is-" + card[2] + "\"><div class=\"auction-status-label\">" + escapeHtml(card[0]) + "</div><div class=\"auction-status-value\">" + escapeHtml(card[1]) + "</div></article>";
    }).join("");
  }

  function renderFocusSummary() {
    const focus = state.auction?.focusWindow || null;
    elements.focusLabel.textContent = focus?.label || currentFocusLabel();
    if (!focus) {
      elements.contextSummaryLine.textContent = "No auction context loaded.";
      if (elements.contextSection?.open) {
        elements.focusSummary.innerHTML = "<div class=\"sql-empty\">No focus summary yet.</div>";
      }
      return;
    }
    elements.contextSummaryLine.textContent = [
      focus.stateKind || "Unknown",
      focus.locationKind || "Unknown",
      focus.preferredAction || "NoTrade",
      formatPrice(focus.lowPrice) + " to " + formatPrice(focus.highPrice),
    ].join(" | ");
    if (!elements.contextSection?.open) {
      return;
    }
    elements.focusSummary.innerHTML = [
      "<div class=\"auction-context-grid\">",
      "<span>Bracket position</span><strong>" + escapeHtml(formatPercent(focus.bracketPosition)) + "</strong>",
      "<span>Value drift</span><strong>" + escapeHtml(formatSignedPrice(focus.valueDrift)) + "</strong>",
      "<span>Balance score</span><strong>" + escapeHtml(String(focus.balanceScore ?? "-")) + "</strong>",
      "<span>Trend score</span><strong>" + escapeHtml(String(focus.trendScore ?? "-")) + "</strong>",
      "<span>Transition score</span><strong>" + escapeHtml(String(focus.transitionScore ?? "-")) + "</strong>",
      "<span>POC / VAH / VAL</span><strong>" + escapeHtml([formatPrice(focus.pocPrice), formatPrice(focus.vahPrice), formatPrice(focus.valPrice)].join(" / ")) + "</strong>",
      "<span>Invalidation</span><strong>" + escapeHtml(formatPrice(focus.invalidationPrice)) + "</strong>",
      "<span>Targets</span><strong>" + escapeHtml([formatPrice(focus.targetPrice1), formatPrice(focus.targetPrice2)].join(" / ")) + "</strong>",
      "<span>Window</span><strong>" + escapeHtml(focusRangeText(focus) || "-") + "</strong>",
      "<span>Window note</span><strong>" + escapeHtml(focus.summaryText || "-") + "</strong>",
      "</div>",
    ].join("");
  }

  function renderReferences() {
    const refs = state.auction?.focusWindow?.nearestReferences || [];
    if (!elements.contextSection?.open) {
      return;
    }
    if (!refs.length) {
      elements.referenceList.innerHTML = "<div class=\"sql-empty\">No references yet.</div>";
      return;
    }
    elements.referenceList.innerHTML = refs.map(function (ref) {
      const tone = (ref.refKind || "").toLowerCase().includes("low") || (ref.refKind || "").toLowerCase().includes("val") ? "support" : "risk";
      return [
        "<article class=\"auction-ref-row is-", tone, "\">",
        "<div class=\"auction-ref-head\"><span class=\"auction-ref-kind\">", escapeHtml(ref.refKind || "Ref"), "</span><span class=\"auction-ref-price\">", escapeHtml(formatPrice(ref.price)), "</span></div>",
        "<div class=\"auction-ref-meta\">dist ", escapeHtml(formatPrice(ref.distance)), " | strength ", escapeHtml(String(ref.strength ?? "-")), "</div>",
        "</article>",
      ].join("");
    }).join("");
  }

  function renderLadder() {
    const ladder = state.auction?.ladder || [];
    if (!elements.contextSection?.open) {
      return;
    }
    if (!ladder.length) {
      elements.ladderList.innerHTML = "<div class=\"sql-empty\">No ladder rows yet.</div>";
      return;
    }
    elements.ladderList.innerHTML = ladder.map(function (row) {
      const tone = (row.preferredAction || "").includes("Sell") ? "negative" : ((row.preferredAction || "").includes("Buy") ? "positive" : "neutral");
      return [
        "<article class=\"auction-ladder-row is-", tone, "\">",
        "<div class=\"auction-ladder-head\"><span class=\"auction-ladder-label\">", escapeHtml(row.label || row.kind || "Window"), "</span><span class=\"auction-ladder-value\">", escapeHtml(row.stateKind || "Unknown"), "</span></div>",
        "<div class=\"auction-ref-meta\">", escapeHtml([row.locationKind || "-", row.preferredAction || "-", "drift " + formatSignedPrice(row.valueDrift)].join(" | ")), "</div>",
        "</article>",
      ].join("");
    }).join("");
  }

  function renderViewControls() {
    const manualView = state.view.userLockedX || state.view.userLockedY;
    elements.followLiveButton.classList.toggle("is-active", state.view.followLive && !state.view.userLockedX);
    elements.fitPriceActionButton.classList.toggle("is-active", state.view.fitMode === "price" && !state.view.userLockedY);
    elements.fitAuctionRefsButton.classList.toggle("is-active", state.view.fitMode === "refs" && !state.view.userLockedY);
    if (manualView) {
      elements.chartViewLabel.textContent = "Custom chart view preserved on live updates.";
      return;
    }
    if (state.view.fitMode === "refs") {
      elements.chartViewLabel.textContent = "Showing price action with auction references in frame.";
      return;
    }
    elements.chartViewLabel.textContent = "Showing the full " + currentFocusLabel() + " price span across the loaded data window.";
  }

  function resetChartView() {
    state.view.followLive = true;
    state.view.fitMode = "price";
    state.view.xRange = null;
    state.view.yRange = null;
    state.view.userLockedX = false;
    state.view.userLockedY = false;
  }

  function fitPriceActionView() {
    state.view.fitMode = "price";
    state.view.yRange = null;
    state.view.userLockedY = false;
    renderChart();
  }

  function fitAuctionReferenceView() {
    state.view.fitMode = "refs";
    state.view.yRange = null;
    state.view.userLockedY = false;
    renderChart();
  }

  function priceValuesFromRows(rows) {
    return (rows || []).map(function (row) { return Number(row.mid); }).filter(function (value) { return Number.isFinite(value); });
  }

  function activeRowsForVisibleWindow() {
    if (!state.rows.length || !state.view.xRange) {
      return state.rows.slice();
    }
    const visibleRows = state.rows.filter(function (row) {
      const ts = Number(row.timestampMs);
      return ts >= state.view.xRange.min && ts <= state.view.xRange.max;
    });
    return visibleRows.length ? visibleRows : state.rows.slice();
  }

  function focusReferencePrices(config) {
    const focus = state.auction?.focusWindow || null;
    const prices = [];
    if (!focus) {
      return prices;
    }
    if (config.showValueArea) {
      prices.push(focus.vahPrice, focus.valPrice);
    }
    if (config.showPoc) {
      prices.push(focus.pocPrice);
    }
    if (config.showRefs) {
      (focus.references || []).forEach(function (ref) {
        prices.push(ref.price);
      });
    }
    return collectNumbers(prices);
  }

  function historyReferencePrices(config) {
    if (!config.showHistory) {
      return [];
    }
    const prices = [];
    state.history.sessions.forEach(function (session) {
      if (config.showValueArea) {
        prices.push(session.vahPrice, session.valPrice);
      }
      if (config.showPoc) {
        prices.push(session.pocPrice);
      }
      if (config.showRefs) {
        (session.refs || []).forEach(function (ref) {
          prices.push(ref.price);
        });
      }
      prices.push(session.highPrice, session.lowPrice);
      if (config.showEvents) {
        (session.events || []).forEach(function (event) {
          prices.push(event.price1, event.price2);
        });
      }
    });
    return collectNumbers(prices);
  }

  function auctionEventPrices(config) {
    if (!config.showEvents) {
      return [];
    }
    return collectNumbers((state.auction?.events || []).flatMap(function (event) {
      return [event.price1, event.price2];
    }));
  }

  function computeNavigationYRange(config) {
    const prices = []
      .concat(priceValuesFromRows(state.rows))
      .concat(focusReferencePrices(config))
      .concat(historyReferencePrices(config))
      .concat(auctionEventPrices(config));
    const numeric = collectNumbers(prices);
    if (!numeric.length) {
      return null;
    }
    const min = Math.min.apply(null, numeric);
    const max = Math.max.apply(null, numeric);
    const margin = Math.max(DEFAULT_PROFILE_PRICE_STEP, (max - min) * 0.03);
    return normalizeRange(min - margin, max + margin, margin * 2);
  }

  function computePriceActionYRange(config, navigationRange) {
    const focus = state.auction?.focusWindow || null;
    const focusPrices = collectNumbers([focus?.lowPrice, focus?.highPrice].concat(tradeOverlayPrices()));
    const focusRange = focusPrices.length
      ? normalizeRange(
        Math.min.apply(null, focusPrices),
        Math.max.apply(null, focusPrices),
        MIN_VISIBLE_PRICE_RANGE
      )
      : null;
    if (focusRange) {
      return clampRange(focusRange, navigationRange);
    }
    const rowPrices = priceValuesFromRows(state.rows);
    if (!rowPrices.length) {
      return navigationRange;
    }
    return clampRange(
      normalizeRange(
        Math.min.apply(null, rowPrices),
        Math.max.apply(null, rowPrices),
        MIN_VISIBLE_PRICE_RANGE
      ),
      navigationRange
    );
  }

  function computeReferenceFitYRange(config, navigationRange) {
    const focus = state.auction?.focusWindow || null;
    const prices = priceValuesFromRows(activeRowsForVisibleWindow())
      .concat(focusReferencePrices(config))
      .concat(tradeOverlayPrices())
      .concat(auctionEventPrices(config));
    if (config.showHistory) {
      prices.push.apply(prices, historyReferencePrices(config));
    }
    const fitRange = rangeFromNumbers(prices, 0.3);
    return clampRange(fitRange || navigationRange, navigationRange);
  }

  function resolveXRange(xBounds) {
    if (!xBounds) {
      return null;
    }
    if (!state.view.userLockedX || !state.view.xRange) {
      return { min: xBounds.min, max: xBounds.max };
    }
    return clampRange(state.view.xRange, xBounds);
  }

  function resolveYRange(config, navigationRange) {
    if (!navigationRange) {
      return null;
    }
    if (state.view.userLockedY && state.view.yRange) {
      return clampRange(state.view.yRange, navigationRange);
    }
    return state.view.fitMode === "refs"
      ? computeReferenceFitYRange(config, navigationRange)
      : computePriceActionYRange(config, navigationRange);
  }

  function syncChartViewState(eventInfo) {
    if (!state.chart || state.view.applyingZoom) {
      return;
    }
    const ids = []
      .concat(eventInfo?.dataZoomId || [])
      .concat((eventInfo?.batch || []).map(function (item) { return item.dataZoomId; }).filter(Boolean));
    const touchedX = !ids.length || ids.some(function (id) { return String(id).indexOf("auction-x-") === 0; });
    const touchedY = !ids.length || ids.some(function (id) { return String(id).indexOf("auction-y-") === 0; });
    const xRange = readDataZoomWindow("auction-x-slider");
    const yRange = readDataZoomWindow("auction-y-slider");
    const xChanged = xRange && (
      !state.view.xRange
      || Math.abs(state.view.xRange.min - xRange.min) > 0.0001
      || Math.abs(state.view.xRange.max - xRange.max) > 0.0001
    );
    if (touchedX && xRange && xChanged) {
      state.view.xRange = xRange;
      state.view.userLockedX = true;
      state.view.followLive = false;
    }
    const yChanged = yRange && (
      !state.view.yRange
      || Math.abs(state.view.yRange.min - yRange.min) > 0.0001
      || Math.abs(state.view.yRange.max - yRange.max) > 0.0001
    );
    if (touchedY && yRange && yChanged) {
      state.view.yRange = yRange;
      state.view.userLockedY = true;
      state.view.fitMode = "manual";
    }
    renderViewControls();
  }

  function profilePriceRange(focus, profile) {
    const focusRange = normalizeRange(focus?.lowPrice, focus?.highPrice, MIN_VISIBLE_PRICE_RANGE);
    const prices = collectNumbers((profile || []).map(function (item) { return item.priceBin; }));
    const profileRange = prices.length
      ? normalizeRange(
        Math.min.apply(null, prices),
        Math.max.apply(null, prices),
        MIN_VISIBLE_PRICE_RANGE
      )
      : null;
    if (focusRange && profileRange) {
      return normalizeRange(
        Math.min(focusRange.min, profileRange.min),
        Math.max(focusRange.max, profileRange.max),
        MIN_VISIBLE_PRICE_RANGE
      );
    }
    return focusRange || profileRange || null;
  }

  function profilePriceStep(profile) {
    const prices = collectNumbers((profile || []).map(function (item) { return item.priceBin; })).sort(function (left, right) {
      return left - right;
    });
    if (prices.length < 2) {
      return DEFAULT_PROFILE_PRICE_STEP;
    }
    let step = Number.POSITIVE_INFINITY;
    for (let index = 1; index < prices.length; index += 1) {
      const diff = prices[index] - prices[index - 1];
      if (diff > 0 && diff < step) {
        step = diff;
      }
    }
    return Number.isFinite(step) ? step : DEFAULT_PROFILE_PRICE_STEP;
  }

  function nearestProfilePrice(profile, currentPrice) {
    if (!Number.isFinite(currentPrice) || !(profile || []).length) {
      return null;
    }
    let winner = null;
    let bestDistance = Number.POSITIVE_INFINITY;
    profile.forEach(function (item) {
      const price = Number(item.priceBin);
      if (!Number.isFinite(price)) {
        return;
      }
      const distance = Math.abs(price - currentPrice);
      if (distance < bestDistance) {
        bestDistance = distance;
        winner = price;
      }
    });
    return winner;
  }

  function profileMaxActivity(profile) {
    const values = collectNumbers((profile || []).map(function (item) { return item.activityScore; }));
    return values.length ? Math.max.apply(null, values) : 1;
  }

  function profileReferenceLines(focus, currentPrice) {
    const lines = [];
    if (focus?.pocPrice != null) {
      lines.push({
        name: "POC",
        yAxis: focus.pocPrice,
        lineStyle: { color: "#ffb35c", width: 1.2, type: "dashed" },
        label: { formatter: "POC", color: "#ffb35c", fontSize: 10 },
      });
    }
    if (focus?.vahPrice != null) {
      lines.push({
        name: "VAH",
        yAxis: focus.vahPrice,
        lineStyle: { color: "rgba(109,216,255,0.60)", width: 1, type: "dashed" },
        label: { show: false },
      });
    }
    if (focus?.valPrice != null) {
      lines.push({
        name: "VAL",
        yAxis: focus.valPrice,
        lineStyle: { color: "rgba(109,216,255,0.60)", width: 1, type: "dashed" },
        label: { show: false },
      });
    }
    if (Number.isFinite(currentPrice)) {
      lines.push({
        name: "Current",
        yAxis: currentPrice,
        lineStyle: { color: "#ff6b88", width: 2.2, type: "solid" },
        label: { formatter: "Current", color: "#ff9fb2", fontSize: 10 },
      });
    }
    return lines;
  }

  function renderEvents() {
    const events = state.auction?.events || [];
    elements.eventCount.textContent = String(events.length);
    if (!events.length) {
      elements.eventSummaryLine.textContent = "No auction events yet.";
      if (elements.eventsPanel?.open) {
        elements.eventRibbon.innerHTML = "<div class=\"sql-empty\">No auction events yet.</div>";
      }
      return;
    }
    const latest = events[0];
    elements.eventSummaryLine.textContent = [
      latest.eventKind || "Event",
      formatPrice(latest.price1),
      latest.windowLabel || latest.windowKind || "window",
      "strength " + String(latest.strength ?? "-"),
    ].join(" | ");
    if (!elements.eventsPanel?.open) {
      return;
    }
    elements.eventRibbon.innerHTML = events.map(function (event) {
      const tone = String(event.direction || "").toLowerCase() === "down" ? "down" : "up";
      const eventTime = formatSydneyTime(event.eventTsMs);
      return [
        "<article class=\"auction-event-row is-", tone, "\">",
        "<div class=\"auction-event-main\">",
        "<div class=\"auction-event-title\">", escapeHtml(event.eventKind || "Event"), "</div>",
        "<div class=\"auction-event-meta\">", escapeHtml((event.windowLabel || event.windowKind || "window") + " | " + eventTime), "</div>",
        "</div>",
        "<div class=\"auction-event-side\">",
        "<div class=\"auction-event-price\">", escapeHtml(formatPrice(event.price1)), event.price2 != null ? " -> " + escapeHtml(formatPrice(event.price2)) : "", "</div>",
        "<div class=\"auction-event-meta\">strength ", escapeHtml(String(event.strength ?? "-")), "</div>",
        "</div>",
        "</article>",
      ].join("");
    }).join("");
  }

  function chartTooltip(params) {
    const items = Array.isArray(params) ? params : [params];
    const point = items.find(function (item) { return item?.data?.row; }) || items[0];
    const row = point?.data?.row || null;
    const focus = state.auction?.focusWindow || null;
    const lines = [];
    if (row) {
      lines.push("<div class=\"chart-tip-title\">Tick</div>");
      lines.push("<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Id</span><span class=\"chart-tip-value\">" + escapeHtml(row.id) + "</span></div>");
      lines.push("<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Time</span><span class=\"chart-tip-value\">" + escapeHtml(formatSydneyDateTime(row.timestampMs)) + "</span></div>");
      lines.push("<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Mid</span><span class=\"chart-tip-value\">" + escapeHtml(formatPrice(row.mid)) + "</span></div>");
      lines.push("<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Spread</span><span class=\"chart-tip-value\">" + escapeHtml(formatPrice(row.spread)) + "</span></div>");
    }
    if (focus) {
      lines.push("<div class=\"chart-tip-section\"><div class=\"chart-tip-title\">Auction</div>");
      lines.push("<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">State</span><span class=\"chart-tip-value\">" + escapeHtml(focus.stateKind || "Unknown") + "</span></div>");
      lines.push("<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Location</span><span class=\"chart-tip-value\">" + escapeHtml(focus.locationKind || "Unknown") + "</span></div>");
      lines.push("<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Action</span><span class=\"chart-tip-value\">" + escapeHtml(focus.preferredAction || "NoTrade") + "</span></div></div>");
    }
    return "<div class=\"chart-tip\">" + lines.join("") + "</div>";
  }

  function ensureCharts() {
    if (!state.chart) {
      state.chart = echarts.init(elements.chartHost, null, { renderer: "canvas" });
      state.chart.setOption({
        animation: false,
        backgroundColor: "transparent",
        tooltip: { trigger: "axis", axisPointer: { type: "cross" }, formatter: chartTooltip },
        grid: { left: 56, right: 36, top: 24, bottom: 44 },
        xAxis: {
          type: "time",
          axisLabel: {
            color: "#91a1b8",
            formatter: function (value) { return formatSydneyAxisTime(value); },
          },
          axisLine: { lineStyle: { color: "rgba(147,181,255,0.16)" } },
        },
        yAxis: {
          type: "value",
          scale: true,
          axisLabel: { color: "#91a1b8" },
          splitLine: { lineStyle: { color: "rgba(147,181,255,0.08)" } },
        },
        dataZoom: [
          {
            id: "auction-x-inside",
            type: "inside",
            xAxisIndex: 0,
            filterMode: "none",
            zoomOnMouseWheel: false,
            moveOnMouseMove: true,
            moveOnMouseWheel: false,
          },
          {
            id: "auction-x-slider",
            type: "slider",
            xAxisIndex: 0,
            filterMode: "none",
            height: 20,
            bottom: 10,
          },
          {
            id: "auction-y-inside",
            type: "inside",
            yAxisIndex: 0,
            filterMode: "none",
            zoomOnMouseWheel: true,
            moveOnMouseMove: true,
            moveOnMouseWheel: false,
          },
          {
            id: "auction-y-slider",
            type: "slider",
            yAxisIndex: 0,
            filterMode: "none",
            width: 14,
            right: 10,
            top: 24,
            bottom: 44,
          },
        ],
        series: [],
      });
      state.chart.on("datazoom", function (eventInfo) {
        syncChartViewState(eventInfo);
      });
    }
    if (!state.profileChart) {
      state.profileChart = echarts.init(elements.profileChartHost, null, { renderer: "canvas" });
    }
  }

  function historyAreaData() {
    return state.history.sessions
      .filter(function (session) { return session.valPrice != null && session.vahPrice != null; })
      .map(function (session) {
        return { value: [session.startTsMs, session.endTsMs, session.valPrice, session.vahPrice], session: session };
      });
  }

  function historyLineData(config) {
    const items = [];
    state.history.sessions.forEach(function (session) {
      const sessionColors = HISTORY_SESSION_COLORS[session.sessionKind] || HISTORY_SESSION_COLORS.brokerday;
      if (config.showPoc && session.pocPrice != null) {
        items.push({
          value: [session.startTsMs, session.endTsMs, session.pocPrice],
          style: { color: sessionColors.line, dash: [6, 4], width: 1.4 },
        });
      }
      if (config.showRefs) {
        (session.refs || []).forEach(function (ref) {
          if (ref.price == null || ref.refKind === "POC") {
            return;
          }
          items.push({
            value: [session.startTsMs, session.endTsMs, ref.price],
            style: {
              color: sessionColors.line,
              dash: (ref.refKind || "").startsWith("Prev") ? [3, 5] : [2, 6],
              width: 1.0,
            },
          });
        });
      }
    });
    return items;
  }

  function historyLineRender(params, api) {
    const item = params.data || {};
    const start = api.coord([api.value(0), api.value(2)]);
    const end = api.coord([api.value(1), api.value(2)]);
    return {
      type: "line",
      shape: { x1: start[0], y1: start[1], x2: end[0], y2: end[1] },
      style: {
        stroke: item.style?.color || "rgba(255,255,255,0.45)",
        lineWidth: item.style?.width || 1,
        opacity: 0.86,
        lineDash: item.style?.dash || [],
      },
      silent: true,
    };
  }

  function historyAreaRender(params, api) {
    const leftTop = api.coord([api.value(0), api.value(3)]);
    const rightBottom = api.coord([api.value(1), api.value(2)]);
    const session = params.data?.session || {};
    const color = HISTORY_SESSION_COLORS[session.sessionKind] || HISTORY_SESSION_COLORS.brokerday;
    return {
      type: "rect",
      shape: {
        x: Math.min(leftTop[0], rightBottom[0]),
        y: Math.min(leftTop[1], rightBottom[1]),
        width: Math.max(1, Math.abs(rightBottom[0] - leftTop[0])),
        height: Math.max(1, Math.abs(rightBottom[1] - leftTop[1])),
      },
      style: {
        fill: color.area,
        stroke: color.line,
        lineWidth: 0.6,
        opacity: 0.92,
      },
      silent: true,
    };
  }

  function auctionTradeConnectorRender(params, api) {
    const start = api.coord([api.value(0), api.value(1)]);
    const end = api.coord([api.value(2), api.value(3)]);
    const position = params.data?.position || {};
    const side = String(position.side || "").toLowerCase() === "sell" ? "sell" : "buy";
    const color = side === "sell" ? "rgba(255,159,178,0.88)" : "rgba(126,240,199,0.88)";
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

  function auctionOpenConnectorData() {
    const latestRow = state.rows[state.rows.length - 1] || null;
    const currentTsMs = Number(latestRow?.timestampMs);
    const currentPrice = Number(state.auction?.focusWindow?.closePrice ?? latestRow?.mid);
    if (!auctionTradeOverlayActive() || !Number.isFinite(currentTsMs) || !Number.isFinite(currentPrice)) {
      return [];
    }
    return state.trade.positions.map(function (position) {
      const entryTsMs = Number(position.openTimestampMs);
      const entryPrice = Number(position.entryPrice);
      if (!Number.isFinite(entryTsMs) || !Number.isFinite(entryPrice) || entryPrice <= 0) {
        return null;
      }
      return {
        value: [entryTsMs, entryPrice, currentTsMs, currentPrice],
        position: position,
      };
    }).filter(Boolean);
  }

  function auctionOpenEntryMarkerData() {
    const firstTsMs = Number(state.rows[0]?.timestampMs);
    const lastTsMs = Number(state.rows[state.rows.length - 1]?.timestampMs);
    if (!auctionTradeOverlayActive() || !Number.isFinite(firstTsMs) || !Number.isFinite(lastTsMs)) {
      return [];
    }
    return state.trade.positions.map(function (position) {
      const entryPrice = Number(position.entryPrice);
      const rawEntryTsMs = Number(position.openTimestampMs);
      if (!Number.isFinite(entryPrice) || entryPrice <= 0 || !Number.isFinite(rawEntryTsMs)) {
        return null;
      }
      const entryTsMs = Math.max(firstTsMs, Math.min(lastTsMs, rawEntryTsMs));
      return {
        value: [entryTsMs, entryPrice],
        position: position,
        itemStyle: {
          color: String(position.side || "").toLowerCase() === "sell" ? "#ff9fb2" : "#7ef0c7",
        },
      };
    }).filter(Boolean);
  }

  function auctionOpenPositionEntryLines() {
    if (!auctionTradeOverlayActive()) {
      return [];
    }
    return state.trade.positions.map(function (position) {
      const entryPrice = Number(position.entryPrice);
      if (!Number.isFinite(entryPrice) || entryPrice <= 0) {
        return null;
      }
      const isSell = String(position.side || "").toLowerCase() === "sell";
      const lineColor = isSell ? "rgba(255,159,178,0.88)" : "rgba(126,240,199,0.88)";
      const labelColor = isSell ? "#ffd1da" : "#cffff0";
      const pnlText = position.netUnrealizedPnl != null ? " | " + formatSignedPnl(position.netUnrealizedPnl) : "";
      return {
        name: "Open Position",
        yAxis: entryPrice,
        lineStyle: {
          color: lineColor,
          width: 1.3,
          type: "dashed",
        },
        label: {
          show: true,
          position: "end",
          formatter: formatPositionSide(position.side) + " " + formatPrice(entryPrice) + pnlText,
          color: labelColor,
          fontSize: 10,
          padding: [2, 4],
          backgroundColor: "rgba(5,9,15,0.92)",
          borderColor: lineColor,
          borderWidth: 1,
          borderRadius: 3,
        },
      };
    }).filter(Boolean);
  }

  function buildAuctionPositionGraphics() {
    const chart = state.chart;
    if (!chart || !auctionTradeOverlayActive()) {
      return [];
    }
    const grid = chart.getModel()?.getComponent("grid", 0);
    const rect = grid?.coordinateSystem?.getRect?.();
    if (!rect) {
      return [];
    }
    const graphics = [];
    state.trade.positions.forEach(function (position, index) {
      const entryPrice = Number(position.entryPrice);
      if (!Number.isFinite(entryPrice) || entryPrice <= 0) {
        return;
      }
      const point = chart.convertToPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [Number(state.rows[state.rows.length - 1]?.timestampMs || Date.now()), entryPrice]);
      if (!Array.isArray(point)) {
        return;
      }
      const baseY = Number(point[1]);
      if (!Number.isFinite(baseY) || baseY < rect.y || baseY > rect.y + rect.height) {
        return;
      }
      const isSell = String(position.side || "").toLowerCase() === "sell";
      const lineColor = isSell ? "rgba(255,159,178,0.86)" : "rgba(126,240,199,0.86)";
      const textColor = isSell ? "#ffd1da" : "#cffff0";
      const lots = positionLots(position);
      const lotsText = Number.isFinite(lots) ? (formatLots(lots) + "L") : "";
      const labelY = Math.max(rect.y + 10, Math.min(rect.y + rect.height - 10, baseY + (index * 20) - 10));
      graphics.push({
        id: "auction-position-line-" + String(position.positionId),
        type: "group",
        silent: true,
        z: 12,
        children: [
          {
            type: "rect",
            shape: { x: rect.x + rect.width - 170, y: labelY - 10, width: 166, height: 20, r: 4 },
            style: {
              fill: "rgba(5,9,15,0.92)",
              stroke: lineColor,
              lineWidth: 1,
            },
          },
          {
            type: "text",
            style: {
              text: formatPositionSide(position.side) + " #" + String(position.positionId) + (lotsText ? (" " + lotsText) : ""),
              x: rect.x + rect.width - 87,
              y: labelY,
              textAlign: "center",
              textVerticalAlign: "middle",
              fill: textColor,
              font: "11px 'IBM Plex Mono'",
            },
          },
        ],
      });
    });
    return graphics;
  }

  function buildChartRowData() {
    return state.rows.map(function (row) {
      return { value: [row.timestampMs, row.mid], row: row };
    });
  }

  function resolveChartViewport(config, rowData) {
    const xBounds = rowData.length
      ? normalizeRange(rowData[0].value[0], rowData[rowData.length - 1].value[0], 1000)
      : null;
    const navigationYRange = computeNavigationYRange(config);
    const xRange = resolveXRange(xBounds);
    const yRange = resolveYRange(config, navigationYRange);
    state.view.xRange = xRange;
    state.view.yRange = yRange;
    return {
      xBounds: xBounds,
      navigationYRange: navigationYRange,
      xRange: xRange,
      yRange: yRange,
    };
  }

  function resolveFastTickViewport(config, rowData) {
    const viewport = resolveChartViewport(config, rowData);
    if (!viewport.navigationYRange) {
      return viewport;
    }
    if (!state.view.userLockedY && state.view.fitMode !== "refs") {
      const rowPrices = priceValuesFromRows(state.rows).concat(tradeOverlayPrices());
      const liveRange = rowPrices.length
        ? clampRange(
          normalizeRange(
            Math.min.apply(null, rowPrices),
            Math.max.apply(null, rowPrices),
            MIN_VISIBLE_PRICE_RANGE
          ),
          viewport.navigationYRange
        )
        : viewport.navigationYRange;
      viewport.yRange = liveRange;
      state.view.yRange = liveRange;
    }
    return viewport;
  }

  function queueFastTickRender() {
    if (state.tickRenderFrame) {
      return;
    }
    state.tickRenderFrame = window.requestAnimationFrame(function () {
      const startedAt = window.performance && typeof window.performance.now === "function"
        ? window.performance.now()
        : Date.now();
      state.tickRenderFrame = 0;
      if (!state.chart) {
        renderChart();
        return;
      }
      const config = currentConfig();
      const rowData = buildChartRowData();
      const viewport = resolveFastTickViewport(config, rowData);
      state.view.applyingZoom = true;
      state.chart.setOption({
        xAxis: viewport.xBounds ? { min: viewport.xBounds.min, max: viewport.xBounds.max } : {},
        yAxis: viewport.navigationYRange ? { min: viewport.navigationYRange.min, max: viewport.navigationYRange.max } : {},
        dataZoom: [
          viewport.xRange ? { id: "auction-x-inside", startValue: viewport.xRange.min, endValue: viewport.xRange.max } : { id: "auction-x-inside" },
          viewport.xRange ? { id: "auction-x-slider", startValue: viewport.xRange.min, endValue: viewport.xRange.max } : { id: "auction-x-slider" },
          viewport.yRange ? { id: "auction-y-inside", startValue: viewport.yRange.min, endValue: viewport.yRange.max } : { id: "auction-y-inside" },
          viewport.yRange ? { id: "auction-y-slider", startValue: viewport.yRange.min, endValue: viewport.yRange.max } : { id: "auction-y-slider" },
        ],
        series: [{
          id: "price-line",
          data: rowData,
        }],
      }, { lazyUpdate: true });
      renderViewControls();
      window.requestAnimationFrame(function () {
        state.view.applyingZoom = false;
        const finishedAt = window.performance && typeof window.performance.now === "function"
          ? window.performance.now()
          : Date.now();
        state.fastTickStats.renderLatencyMs = Math.max(0, finishedAt - startedAt);
        renderPerf();
      });
    });
  }

  function profileBarRender(params, api) {
    const activity = Math.max(0, Number(api.value(0) || 0));
    const price = Number(api.value(1));
    const step = Math.max(DEFAULT_PROFILE_PRICE_STEP, Number(api.value(2) || DEFAULT_PROFILE_PRICE_STEP));
    const coordSys = params.coordSys;
    const start = api.coord([0, price]);
    const end = api.coord([activity, price]);
    const topPoint = api.coord([0, price + (step / 2)]);
    const bottomPoint = api.coord([0, price - (step / 2)]);
    const rowHeight = Math.abs(Number(bottomPoint[1]) - Number(topPoint[1]));
    const barHeight = Math.max(1, Math.min(20, rowHeight > 0 ? rowHeight * 0.78 : 4));
    const xStart = Number(start[0]);
    const xEnd = Number(end[0]);
    const width = Math.max(1, Math.abs(xEnd - xStart));
    const shape = echarts.graphic.clipRectByRect({
      x: Math.min(xStart, xEnd),
      y: Number(start[1]) - (barHeight / 2),
      width: width,
      height: barHeight,
      r: Math.max(1, Math.min(4, barHeight / 2)),
    }, {
      x: coordSys.x,
      y: coordSys.y,
      width: coordSys.width,
      height: coordSys.height,
    });
    if (!shape) {
      return null;
    }
    return {
      type: "rect",
      transition: ["shape"],
      shape: shape,
      style: api.style(),
      silent: true,
    };
  }

  function renderChart() {
    ensureCharts();
    const config = currentConfig();
    const focus = state.auction?.focusWindow || null;
    const rowData = buildChartRowData();
    const currentEventData = config.showEvents ? (state.auction?.events || []).filter(function (event) {
      return event.price1 != null;
    }).map(function (event) {
      return { value: [event.eventTsMs, event.price1], event: event };
    }) : [];
    const historicalEventData = (config.showHistory && config.showEvents)
      ? state.history.sessions.flatMap(function (session) {
        return (session.events || []).filter(function (event) { return event.price1 != null; }).map(function (event) {
          return { value: [event.eventTsMs, event.price1], event: event };
        });
      })
      : [];
    const viewport = resolveChartViewport(config, rowData);

    const lineStyleByRef = {
      POC: { color: "#ffb35c", type: "solid" },
      VAH: { color: "#6dd8ff", type: "solid" },
      VAL: { color: "#6dd8ff", type: "solid" },
      PrevPOC: { color: "rgba(255,179,92,0.65)", type: "dashed" },
      PrevVAH: { color: "rgba(109,216,255,0.65)", type: "dashed" },
      PrevVAL: { color: "rgba(109,216,255,0.65)", type: "dashed" },
      BracketHigh: { color: "rgba(255,255,255,0.22)", type: "dotted" },
      BracketLow: { color: "rgba(255,255,255,0.22)", type: "dotted" },
    };
    const markLines = [];
    if (config.showPoc && focus?.pocPrice != null) {
      markLines.push({
        name: "POC",
        yAxis: focus.pocPrice,
        lineStyle: lineStyleByRef.POC,
        label: { formatter: "POC", color: lineStyleByRef.POC.color, fontSize: 10 },
      });
    }
    if (config.showRefs) {
      (focus?.references || []).forEach(function (ref) {
        if (!lineStyleByRef[ref.refKind] || ref.refKind === "POC") {
          return;
        }
        markLines.push({
          name: ref.refKind,
          yAxis: ref.price,
          lineStyle: lineStyleByRef[ref.refKind],
          label: { formatter: ref.refKind, color: lineStyleByRef[ref.refKind].color, fontSize: 10 },
        });
      });
    }
    if (focus?.sessionKind === "brokerday" && focus?.startTsMs != null) {
      markLines.push({
        name: "Broker Day Start",
        xAxis: focus.startTsMs,
        lineStyle: { color: "rgba(255,179,92,0.42)", width: 1, type: "dashed" },
        label: { formatter: "08:00 Sydney", color: "rgba(255,179,92,0.86)", fontSize: 10 },
      });
    }
    if (auctionTradeOverlayActive()) {
      markLines.push.apply(markLines, auctionOpenPositionEntryLines());
    }

    const markAreas = [];
    if (config.showValueArea && focus?.valPrice != null && focus?.vahPrice != null) {
      markAreas.push([{ yAxis: focus.valPrice, itemStyle: { color: "rgba(109,216,255,0.10)" } }, { yAxis: focus.vahPrice }]);
    }
    if (config.showHeavyOverlays && focus?.ibHigh != null && focus?.ibLow != null && focus?.startTsMs != null) {
      const ibEndMs = focus.startTsMs + ((focus.kind === "session" ? 60 : 15) * 60 * 1000);
      markAreas.push([
        { xAxis: focus.startTsMs, yAxis: focus.ibLow, itemStyle: { color: "rgba(255,179,92,0.10)" } },
        { xAxis: ibEndMs, yAxis: focus.ibHigh },
      ]);
    }

    const series = [];
    if (config.showHistory && config.showHeavyOverlays && state.history.sessions.length && config.showValueArea) {
      series.push({ id: "history-value-area", type: "custom", renderItem: historyAreaRender, silent: true, data: historyAreaData(), z: 1 });
    }
    if (config.showHistory && state.history.sessions.length && (config.showPoc || config.showRefs)) {
      series.push({ id: "history-ref-lines", type: "custom", renderItem: historyLineRender, silent: true, data: historyLineData(config), z: 2 });
    }
    series.push({
      id: "price-line",
      type: "line",
      name: "mid",
      showSymbol: false,
      smooth: false,
      animation: false,
      hoverAnimation: false,
      lineStyle: { width: 1.6, color: "#e8eef8" },
      areaStyle: { color: "rgba(109,216,255,0.05)" },
      data: rowData,
      markLine: { symbol: ["none", "none"], data: markLines, silent: true },
      markArea: { data: markAreas, silent: true },
      z: 3,
    });
    if (auctionTradeOverlayActive()) {
      series.push({
        id: "auction-open-position-connectors",
        name: "Open positions",
        type: "custom",
        renderItem: auctionTradeConnectorRender,
        silent: true,
        animation: false,
        encode: { x: [0, 2], y: [1, 3] },
        data: auctionOpenConnectorData(),
        z: 5,
      });
      series.push({
        id: "auction-open-position-entry",
        name: "Open position entry",
        type: "scatter",
        symbol: "triangle",
        symbolSize: 12,
        silent: true,
        animation: false,
        data: auctionOpenEntryMarkerData(),
        z: 6,
      });
    }
    if (config.showEvents) {
      series.push({
        id: "auction-events",
        type: "scatter",
        symbolSize: 11,
        animation: false,
        data: currentEventData,
        itemStyle: {
          color: function (params) {
            return String(params.data?.event?.direction || "").toLowerCase() === "down" ? "#ff6b88" : "#7ef0c7";
          },
        },
        z: 4,
      });
    }
    if (config.showHistory && config.showEvents && historicalEventData.length) {
      series.push({
        id: "auction-history-events",
        type: "scatter",
        symbolSize: 8,
        animation: false,
        data: historicalEventData,
        itemStyle: {
          color: function (params) {
            return String(params.data?.event?.direction || "").toLowerCase() === "down" ? "rgba(255,107,136,0.68)" : "rgba(126,240,199,0.72)";
          },
        },
        z: 2,
      });
    }

    state.view.applyingZoom = true;
    state.chart.setOption({
      xAxis: viewport.xBounds ? { min: viewport.xBounds.min, max: viewport.xBounds.max } : {},
      yAxis: viewport.navigationYRange ? { min: viewport.navigationYRange.min, max: viewport.navigationYRange.max } : {},
      dataZoom: [
        viewport.xRange ? { id: "auction-x-inside", startValue: viewport.xRange.min, endValue: viewport.xRange.max } : { id: "auction-x-inside" },
        viewport.xRange ? { id: "auction-x-slider", startValue: viewport.xRange.min, endValue: viewport.xRange.max } : { id: "auction-x-slider" },
        viewport.yRange ? { id: "auction-y-inside", startValue: viewport.yRange.min, endValue: viewport.yRange.max } : { id: "auction-y-inside" },
        viewport.yRange ? { id: "auction-y-slider", startValue: viewport.yRange.min, endValue: viewport.yRange.max } : { id: "auction-y-slider" },
      ],
      series: series,
    }, { replaceMerge: ["series"], lazyUpdate: true });
    window.requestAnimationFrame(function () {
      state.view.applyingZoom = false;
      state.chart.setOption({
        graphic: [{
          id: "auction-trade-overlay",
          type: "group",
          silent: true,
          z: 12,
          children: buildAuctionPositionGraphics(),
        }],
      }, { replaceMerge: ["graphic"], lazyUpdate: true });
    });
    renderViewControls();

    const profile = focus?.profile || [];
    const currentPrice = Number(focus?.closePrice ?? state.rows[state.rows.length - 1]?.mid);
    const profileYRange = profilePriceRange(focus, profile);
    const activityMax = Math.max(1, profileMaxActivity(profile));
    const priceStep = profilePriceStep(profile);
    const currentProfilePrice = nearestProfilePrice(profile, currentPrice);
    const currentBandHalf = Math.max(DEFAULT_PROFILE_PRICE_STEP, priceStep) / 2;
    elements.profileLabel.textContent = focus?.label || currentFocusLabel();
    state.profileChart.setOption({
      animation: false,
      backgroundColor: "transparent",
      grid: { left: 58, right: 18, top: 18, bottom: 38, containLabel: true },
      xAxis: {
        type: "value",
        min: 0,
        max: Math.max(1, activityMax * 1.12),
        name: "Time / Activity",
        nameLocation: "middle",
        nameGap: 24,
        axisLabel: { color: "#91a1b8" },
        axisLine: { lineStyle: { color: "rgba(147,181,255,0.16)" } },
        splitLine: { lineStyle: { color: "rgba(147,181,255,0.08)" } },
      },
      yAxis: {
        type: "value",
        min: profileYRange?.min,
        max: profileYRange?.max,
        name: "Price",
        nameLocation: "middle",
        nameGap: 42,
        axisLabel: {
          color: "#91a1b8",
          hideOverlap: false,
          formatter: function (value) { return Number(value).toFixed(2); },
        },
        axisLine: { lineStyle: { color: "rgba(147,181,255,0.16)" } },
        splitLine: { lineStyle: { color: "rgba(147,181,255,0.08)" } },
      },
      tooltip: {
        trigger: "item",
        formatter: function (param) {
          const item = param.data?.raw || {};
          return "<div class=\"chart-tip\"><div class=\"chart-tip-title\">Market Profile</div>"
            + "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Price</span><span class=\"chart-tip-value\">" + escapeHtml(formatPrice(item.priceBin)) + "</span></div>"
            + "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Activity</span><span class=\"chart-tip-value\">" + escapeHtml(String(item.activityScore || "-")) + "</span></div>"
            + "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Time</span><span class=\"chart-tip-value\">" + escapeHtml(String(item.timeMs || 0)) + "ms</span></div>"
            + "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Ticks</span><span class=\"chart-tip-value\">" + escapeHtml(String(item.tickCount || 0)) + "</span></div>"
            + "</div>";
        },
      },
      series: [{
        type: "custom",
        renderItem: profileBarRender,
        animation: false,
        encode: { x: 0, y: 1 },
        clip: true,
        data: profile.map(function (item) {
          const priceBin = Number(item.priceBin);
          const isCurrentBand = Number.isFinite(currentProfilePrice) && Math.abs(priceBin - currentProfilePrice) <= (priceStep / 2);
          return {
            value: [item.activityScore, item.priceBin, priceStep],
            raw: item,
            itemStyle: {
              color: isCurrentBand
                ? "#ff8ea8"
                : (item.isPoc ? "#ffb35c" : (item.inValue ? "rgba(109,216,255,0.82)" : "rgba(145,161,184,0.32)")),
              borderColor: isCurrentBand ? "#ffb5c5" : "rgba(8,14,23,0.62)",
              borderWidth: 0.7,
            },
          };
        }),
      }, {
        type: "line",
        silent: true,
        animation: false,
        showSymbol: false,
        lineStyle: { opacity: 0 },
        data: profile.map(function (item) {
          return [0, item.priceBin];
        }),
        markArea: Number.isFinite(currentPrice) ? {
          silent: true,
          itemStyle: { color: "rgba(255,107,136,0.14)" },
          data: [[
            { yAxis: currentPrice - currentBandHalf },
            { yAxis: currentPrice + currentBandHalf },
          ]],
        } : { data: [] },
        markLine: {
          symbol: ["none", "none"],
          silent: true,
          data: profileReferenceLines(focus, currentPrice),
        },
        tooltip: { show: false },
      }],
    }, { lazyUpdate: true });
  }

  function renderAll() {
    renderMeta();
    renderPerf();
    renderStatusStrip();
    renderFocusSummary();
    renderReferences();
    renderLadder();
    renderEvents();
    renderLoginPanel();
    renderButtonsPanel();
    renderChart();
  }

  function queueAuctionRender() {
    if (state.renderFrame) {
      return;
    }
    state.renderFrame = window.requestAnimationFrame(function () {
      state.renderFrame = 0;
      renderAll();
    });
  }

  async function loadBootstrap() {
    const config = currentConfig();
    const startId = config.mode === "review" ? await resolveReviewStartId(config) : null;
    const params = new URLSearchParams({ mode: config.mode, window: String(config.window), focusKind: config.focusKind });
    if (startId != null) {
      params.set("id", String(startId));
    }
    const payload = await fetchJson("/api/auction/bootstrap?" + params.toString());
    applyPayload(payload);
    trimRowsToWindow();
    renderAll();
    await refreshHistoryMarkers(true);
    status("Loaded auction view.", false);
    if (config.run === "run") {
      if (config.mode === "live") {
        connectLiveStreams(payload.lastId || 0);
      } else {
        connectReviewStream(payload.lastId || 0, payload.reviewEndId || 0);
      }
    }
  }

  function scheduleLiveReconnect() {
    if (state.reconnectTimer || currentConfig().mode !== "live" || currentConfig().run !== "run") {
      return;
    }
    state.reconnectTimer = window.setTimeout(function () {
      state.reconnectTimer = 0;
      if (currentConfig().mode !== "live" || currentConfig().run !== "run") {
        return;
      }
      connectLiveStreams(state.rows[state.rows.length - 1]?.id || 0);
    }, LIVE_RECONNECT_DELAY_MS);
  }

  function handleLiveStreamDisconnect(message) {
    closeLiveSources();
    renderPerf();
    status(message || "Auction live feeds disconnected. Reconnecting...", true);
    scheduleLiveReconnect();
  }

  function connectLiveStreams(afterId) {
    closeLiveSources();
    clearReconnectTimer();
    const config = currentConfig();
    if (config.mode !== "live" || config.run !== "run") {
      renderPerf();
      return;
    }

    const tickSource = new EventSource("/api/auction/tick-stream?" + new URLSearchParams({
      afterId: String(afterId || 0),
      limit: "64",
    }).toString());
    state.tickSource = tickSource;
    tickSource.onopen = function () {
      state.streamConnected = true;
      renderPerf();
      status("Auction tick feed connected.", false);
    };
    tickSource.onmessage = function (event) {
      const payload = JSON.parse(event.data);
      const appendedCount = appendRows(payload.rows || []);
      state.lastMetrics = payload;
      state.fastTickStats.appendedCount = appendedCount;
      state.fastTickStats.browserLatencyMs = payload.serverSentAtMs != null ? Math.max(0, Date.now() - Number(payload.serverSentAtMs)) : null;
      if (appendedCount > 0) {
        queueFastTickRender();
      } else {
        renderPerf();
      }
    };
    tickSource.addEventListener("heartbeat", function (event) {
      const payload = JSON.parse(event.data);
      state.lastMetrics = payload;
      state.fastTickStats.appendedCount = 0;
      renderPerf();
    });

    const auctionSource = new EventSource("/api/auction/stream?" + new URLSearchParams({
      focusKind: config.focusKind,
    }).toString());
    state.auctionSource = auctionSource;
    auctionSource.onopen = function () {
      state.auctionStreamConnected = true;
      renderPerf();
    };
    auctionSource.onmessage = function (event) {
      const payload = JSON.parse(event.data);
      if (payload.auction) {
        setAuctionSnapshot(payload.auction);
      }
      state.lastAuctionMetrics = payload;
      queueAuctionRender();
      scheduleHistoryRefresh(false);
    };
    auctionSource.addEventListener("heartbeat", function (event) {
      const payload = JSON.parse(event.data);
      state.lastAuctionMetrics = payload;
      renderPerf();
    });

    tickSource.onerror = function () {
      handleLiveStreamDisconnect("Auction tick feed disconnected. Reconnecting...");
    };
    auctionSource.onerror = function () {
      handleLiveStreamDisconnect("Auction snapshot feed disconnected. Reconnecting...");
    };
  }

  function connectReviewStream(afterId, endId) {
    clearActivity();
    const config = currentConfig();
    if (!endId || afterId >= endId) {
      status("Review reached the current end snapshot.", false);
      return;
    }
    const source = new EventSource("/api/auction/review-stream?" + new URLSearchParams({
      afterId: String(afterId || 0),
      endId: String(endId),
      speed: String(config.reviewSpeed),
      focusKind: config.focusKind,
    }).toString());
    state.source = source;
    source.onopen = function () {
      state.streamConnected = true;
      renderPerf();
      status("Auction review replay connected.", false);
    };
    source.onmessage = function (event) {
      const payload = JSON.parse(event.data);
      appendRows(payload.rows || []);
      if (payload.auction) {
        setAuctionSnapshot(payload.auction);
      }
      state.lastMetrics = payload;
      queueAuctionRender();
      scheduleHistoryRefresh(false);
      if (payload.endReached) {
        clearActivity();
        status("Review reached the current end snapshot.", false);
      }
    };
    source.onerror = function () {
      state.streamConnected = false;
      renderPerf();
      clearActivity();
      status("Auction review replay disconnected. Click Load or Run to reconnect.", true);
    };
  }

  async function loadAll() {
    const token = state.loadToken + 1;
    state.loadToken = token;
    clearActivity();
    resetChartView();
    renderViewControls();
    writeQuery();
    try {
      await loadBootstrap();
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
    elements.focusKind.value = config.focusKind;
    elements.showValueArea.checked = Boolean(config.showValueArea);
    elements.showPoc.checked = Boolean(config.showPoc);
    elements.showRefs.checked = Boolean(config.showRefs);
    elements.showEvents.checked = Boolean(config.showEvents);
    elements.showHistory.checked = Boolean(config.showHistory);
    elements.showHeavyOverlays.checked = Boolean(config.showHeavyOverlays);
    elements.tickId.value = config.id;
    elements.windowSize.value = String(config.window);
    elements.reviewStart.value = config.reviewStart;
    setSidebarCollapsed(true);
    resetChartView();
    updateReviewFields();
    renderMeta();
    renderPerf();
    renderViewControls();
    if (elements.chartFullscreenButton && !fullscreenSupported()) {
      elements.chartFullscreenButton.disabled = true;
      elements.chartFullscreenButton.textContent = "Fullscreen N/A";
    }
    writeQuery();
  }

  function setupAccordionPanels() {
    [
      elements.contextSection,
      elements.eventsPanel,
      elements.loginPanel,
      elements.buttonsPanel,
    ].forEach(function (panel) {
      if (!panel) {
        return;
      }
      panel.addEventListener("toggle", function () {
        if (panel === elements.contextSection) {
          renderFocusSummary();
          renderReferences();
          renderLadder();
        } else if (panel === elements.eventsPanel) {
          renderEvents();
        } else if (panel === elements.loginPanel) {
          renderLoginPanel();
        } else if (panel === elements.buttonsPanel) {
          renderButtonsPanel();
        }
        syncChartLayout();
      });
    });
  }

  function setupTradePanel() {
    if (!elements.tradeLoginForm) {
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
    tradeStatus("Trade login required.", false);
    renderLoginPanel();
    renderButtonsPanel();

    elements.tradeLoginForm.addEventListener("submit", function (event) {
      event.preventDefault();
      requestTradeLogin();
    });
    elements.tradeLogoutButton?.addEventListener("click", function () {
      requestTradeLogout();
    });
    elements.auctionSmartBuyButton?.addEventListener("click", function () {
      toggleSmartEntry("buy");
    });
    elements.auctionSmartSellButton?.addEventListener("click", function () {
      toggleSmartEntry("sell");
    });
    elements.auctionSmartCloseButton?.addEventListener("click", function () {
      toggleSmartClose();
    });
    window.addEventListener("beforeunload", function () {
      stopTradePolling();
      stopSmartPolling();
    });
    loadTradeSession();
  }

  bindSegment(elements.modeToggle, function (value) {
    setSegment(elements.modeToggle, value);
    updateReviewFields();
    writeQuery();
    renderButtonsPanel();
    syncSmartContext({ silent: true }).catch(function () {});
    status("Mode updated. Click Load to refresh data.", false);
  });
  bindSegment(elements.runToggle, function (value) {
    setSegment(elements.runToggle, value);
    writeQuery();
    clearActivity();
    renderButtonsPanel();
    syncSmartContext({ silent: true }).catch(function () {});
    if (value === "run" && state.rows.length) {
      if (currentConfig().mode === "live") {
        connectLiveStreams(state.rows[state.rows.length - 1].id);
      } else {
        connectReviewStream(state.rows[state.rows.length - 1].id, state.reviewEndId);
      }
      return;
    }
    status("Run state updated.", false);
  });
  bindSegment(elements.reviewSpeedToggle, function (value) {
    setSegment(elements.reviewSpeedToggle, value);
    writeQuery();
    if (currentConfig().mode === "review" && currentConfig().run === "run" && state.rows.length) {
      connectReviewStream(state.rows[state.rows.length - 1].id, state.reviewEndId);
    }
  });

  [elements.focusKind, elements.tickId, elements.windowSize, elements.reviewStart].forEach(function (control) {
    control.addEventListener("change", function () {
      if (control === elements.windowSize) {
        elements.windowSize.value = String(sanitizeWindowValue(elements.windowSize.value));
      }
      writeQuery();
    });
  });
  [elements.showValueArea, elements.showPoc, elements.showRefs, elements.showEvents, elements.showHistory, elements.showHeavyOverlays].forEach(function (control) {
    control.addEventListener("change", function () {
      writeQuery();
      if (control === elements.showHistory || control === elements.showRefs || control === elements.showEvents) {
        scheduleHistoryRefresh(true);
      }
      renderMeta();
      renderEvents();
      renderChart();
    });
  });

  elements.sidebarToggle.addEventListener("click", function () { setSidebarCollapsed(!state.ui.sidebarCollapsed); });
  elements.sidebarBackdrop.addEventListener("click", function () { setSidebarCollapsed(true); });
  elements.applyButton.addEventListener("click", function () { loadAll(); });
  elements.followLiveButton.addEventListener("click", function () {
    if (state.view.followLive && !state.view.userLockedX) {
      state.view.followLive = false;
      state.view.userLockedX = true;
      state.view.xRange = readDataZoomWindow("auction-x-slider") || state.view.xRange;
      renderViewControls();
      return;
    }
    state.view.followLive = true;
    state.view.userLockedX = false;
    if (state.view.followLive) {
      state.view.xRange = null;
      renderChart();
    }
  });
  elements.fitPriceActionButton.addEventListener("click", function () { fitPriceActionView(); });
  elements.fitAuctionRefsButton.addEventListener("click", function () { fitAuctionReferenceView(); });
  elements.resetViewButton.addEventListener("click", function () {
    resetChartView();
    renderChart();
  });
  elements.chartFullscreenButton?.addEventListener("click", function () {
    toggleChartFullscreen();
  });
  window.addEventListener("resize", function () {
    syncChartLayout();
  });
  document.addEventListener("fullscreenchange", function () {
    updateChartFullscreenUi();
  });
  document.addEventListener("webkitfullscreenchange", function () {
    updateChartFullscreenUi();
  });
  window.addEventListener("keydown", function (event) {
    if (event.key === "Escape" && !state.ui.sidebarCollapsed) {
      setSidebarCollapsed(true);
    }
  });

  applyInitialConfig(parseQuery());
  updateChartFullscreenUi();
  setupAccordionPanels();
  setupTradePanel();
  loadAll();
}());
