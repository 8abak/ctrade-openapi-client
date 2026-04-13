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
  const HISTORY_REFRESH_DELAY_MS = 160;
  const MIN_VISIBLE_PRICE_RANGE = 0.6;
  const DEFAULT_PROFILE_PRICE_STEP = 0.1;
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

  const state = {
    chart: null,
    profileChart: null,
    rows: [],
    source: null,
    reviewTimer: 0,
    reviewEndId: null,
    lastMetrics: null,
    streamConnected: false,
    auction: null,
    history: {
      sessions: [],
      refreshTimer: 0,
      loading: false,
      lastRangeKey: "",
    },
    loadToken: 0,
    view: {
      followLive: true,
      fitMode: "price",
      yRange: null,
      xRange: null,
      userLockedX: false,
      userLockedY: false,
      applyingZoom: false,
    },
    ui: { sidebarCollapsed: true },
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
    chartHost: document.getElementById("auctionChart"),
    profileChartHost: document.getElementById("auctionProfileChart"),
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
      currentFocusLabel(),
      "ticks " + state.rows.length,
      "state " + (focus.stateKind || "Unknown"),
      "location " + (focus.locationKind || "Unknown"),
      "action " + (focus.preferredAction || "NoTrade"),
    ];
    if (state.history.sessions.length) {
      parts.push("history " + state.history.sessions.length);
    }
    if (focus.startTs) {
      parts.push("from " + new Date(focus.startTs).toLocaleString());
    }
    elements.auctionMeta.textContent = parts.join(" | ");
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
    if (state.auction?.asOfTsMs != null) {
      parts.push("Auction " + Math.max(0, Date.now() - state.auction.asOfTsMs) + "ms");
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
    if (state.chart) {
      requestAnimationFrame(function () {
        state.chart.resize();
        if (state.profileChart) {
          state.profileChart.resize();
        }
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

  function clearHistoryTimer() {
    if (state.history.refreshTimer) {
      window.clearTimeout(state.history.refreshTimer);
      state.history.refreshTimer = 0;
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
    clearHistoryTimer();
    state.streamConnected = false;
    renderPerf();
  }

  function applyPayload(payload) {
    state.rows = Array.isArray(payload.rows) ? payload.rows.slice() : [];
    state.reviewEndId = payload.reviewEndId || null;
    state.auction = payload.auction || null;
    state.lastMetrics = payload.metrics || null;
  }

  function trimRowsToWindow() {
    const cap = Math.max(200, Number(currentConfig().window) || DEFAULTS.window);
    if (state.rows.length > cap) {
      state.rows = state.rows.slice(state.rows.length - cap);
    }
  }

  function appendRows(rows) {
    const knownIds = new Set(state.rows.map(function (row) { return Number(row.id); }));
    (rows || []).forEach(function (row) {
      const rowId = Number(row.id || 0);
      if (!knownIds.has(rowId)) {
        state.rows.push(row);
        knownIds.add(rowId);
      }
    });
    trimRowsToWindow();
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
    elements.focusLabel.textContent = currentFocusLabel();
    if (!focus) {
      elements.contextSummaryLine.textContent = "No auction context loaded.";
      elements.focusSummary.innerHTML = "<div class=\"sql-empty\">No focus summary yet.</div>";
      return;
    }
    elements.contextSummaryLine.textContent = [
      focus.stateKind || "Unknown",
      focus.locationKind || "Unknown",
      focus.preferredAction || "NoTrade",
      formatPrice(focus.lowPrice) + " to " + formatPrice(focus.highPrice),
    ].join(" | ");
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
      "<span>Window note</span><strong>" + escapeHtml(focus.summaryText || "-") + "</strong>",
      "</div>",
    ].join("");
  }

  function renderReferences() {
    const refs = state.auction?.focusWindow?.nearestReferences || [];
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
    const focusRange = normalizeRange(focus?.lowPrice, focus?.highPrice, MIN_VISIBLE_PRICE_RANGE);
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
    if (focusRange) {
      return focusRange;
    }
    const prices = collectNumbers((profile || []).map(function (item) { return item.priceBin; }));
    if (!prices.length) {
      return null;
    }
    return normalizeRange(
      Math.min.apply(null, prices),
      Math.max.apply(null, prices),
      MIN_VISIBLE_PRICE_RANGE
    );
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
      elements.eventRibbon.innerHTML = "<div class=\"sql-empty\">No auction events yet.</div>";
      return;
    }
    const latest = events[0];
    elements.eventSummaryLine.textContent = [
      latest.eventKind || "Event",
      formatPrice(latest.price1),
      latest.windowLabel || latest.windowKind || "window",
      "strength " + String(latest.strength ?? "-"),
    ].join(" | ");
    elements.eventRibbon.innerHTML = events.map(function (event) {
      const tone = String(event.direction || "").toLowerCase() === "down" ? "down" : "up";
      const eventTime = event.eventTsMs ? new Date(event.eventTsMs).toLocaleTimeString() : "-";
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
      lines.push("<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Time</span><span class=\"chart-tip-value\">" + escapeHtml(new Date(row.timestampMs).toLocaleString()) + "</span></div>");
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
          axisLabel: { color: "#91a1b8" },
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

  function renderChart() {
    ensureCharts();
    const config = currentConfig();
    const focus = state.auction?.focusWindow || null;
    const rowData = state.rows.map(function (row) {
      return { value: [row.timestampMs, row.mid], row: row };
    });
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
    const xBounds = rowData.length
      ? normalizeRange(rowData[0].value[0], rowData[rowData.length - 1].value[0], 1000)
      : null;
    const navigationYRange = computeNavigationYRange(config);
    const xRange = resolveXRange(xBounds);
    const yRange = resolveYRange(config, navigationYRange);
    state.view.xRange = xRange;
    state.view.yRange = yRange;

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
      lineStyle: { width: 1.6, color: "#e8eef8" },
      areaStyle: { color: "rgba(109,216,255,0.05)" },
      data: rowData,
      markLine: { symbol: ["none", "none"], data: markLines, silent: true },
      markArea: { data: markAreas, silent: true },
      z: 3,
    });
    if (config.showEvents) {
      series.push({
        id: "auction-events",
        type: "scatter",
        symbolSize: 11,
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
      xAxis: xBounds ? { min: xBounds.min, max: xBounds.max } : {},
      yAxis: navigationYRange ? { min: navigationYRange.min, max: navigationYRange.max } : {},
      dataZoom: [
        xRange ? { id: "auction-x-inside", startValue: xRange.min, endValue: xRange.max } : { id: "auction-x-inside" },
        xRange ? { id: "auction-x-slider", startValue: xRange.min, endValue: xRange.max } : { id: "auction-x-slider" },
        yRange ? { id: "auction-y-inside", startValue: yRange.min, endValue: yRange.max } : { id: "auction-y-inside" },
        yRange ? { id: "auction-y-slider", startValue: yRange.min, endValue: yRange.max } : { id: "auction-y-slider" },
      ],
      series: series,
    });
    state.view.applyingZoom = false;
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
      grid: { left: 56, right: 18, top: 18, bottom: 32, containLabel: false },
      xAxis: {
        type: "value",
        min: 0,
        max: activityMax,
        name: "Activity",
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
        axisLabel: { color: "#91a1b8", formatter: function (value) { return Number(value).toFixed(2); } },
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
        type: "bar",
        barMaxWidth: 12,
        data: profile.map(function (item) {
          const priceBin = Number(item.priceBin);
          const isCurrentBand = Number.isFinite(currentProfilePrice) && Math.abs(priceBin - currentProfilePrice) <= (priceStep / 2);
          return {
            value: [item.activityScore, item.priceBin],
            raw: item,
            itemStyle: {
              color: isCurrentBand
                ? "#ff8ea8"
                : (item.isPoc ? "#ffb35c" : (item.inValue ? "rgba(109,216,255,0.78)" : "rgba(145,161,184,0.28)")),
            },
          };
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
      }],
    });
  }

  function renderAll() {
    renderMeta();
    renderPerf();
    renderStatusStrip();
    renderFocusSummary();
    renderReferences();
    renderLadder();
    renderEvents();
    renderChart();
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
        connectStream(payload.lastId || 0);
      } else {
        connectReviewStream(payload.lastId || 0, payload.reviewEndId || 0);
      }
    }
  }

  function connectStream(afterId) {
    clearActivity();
    const config = currentConfig();
    const source = new EventSource("/api/auction/stream?" + new URLSearchParams({
      afterId: String(afterId || 0),
      focusKind: config.focusKind,
      limit: "250",
    }).toString());
    state.source = source;
    source.onopen = function () {
      state.streamConnected = true;
      renderPerf();
      status("Auction stream connected.", false);
    };
    source.onmessage = function (event) {
      const payload = JSON.parse(event.data);
      appendRows(payload.rows || []);
      state.auction = payload.auction || state.auction;
      state.lastMetrics = payload;
      renderAll();
      scheduleHistoryRefresh(false);
    };
    source.addEventListener("heartbeat", function (event) {
      const payload = JSON.parse(event.data);
      state.auction = payload.auction || state.auction;
      state.lastMetrics = payload;
      renderMeta();
      renderPerf();
    });
    source.onerror = function () {
      state.streamConnected = false;
      renderPerf();
      status("Auction stream disconnected. Click Load or Run to reconnect.", true);
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
      state.auction = payload.auction || state.auction;
      state.lastMetrics = payload;
      renderAll();
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
    if (value === "run" && state.rows.length) {
      if (currentConfig().mode === "live") {
        connectStream(state.rows[state.rows.length - 1].id);
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
  window.addEventListener("resize", function () {
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
  window.addEventListener("keydown", function (event) {
    if (event.key === "Escape" && !state.ui.sidebarCollapsed) {
      setSidebarCollapsed(true);
    }
  });

  applyInitialConfig(parseQuery());
  loadAll();
}());
