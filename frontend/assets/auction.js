(function () {
  const DEFAULTS = {
    mode: "live",
    run: "run",
    id: "",
    reviewStart: "",
    reviewSpeed: 1,
    window: 2000,
    focusKind: "brokerday",
  };
  const MAX_WINDOW = 10000;
  const REVIEW_SPEEDS = [0.5, 1, 2, 3, 5];
  const FOCUS_LABELS = {
    brokerday: "Broker Day",
    london: "London Session",
    newyork: "New York Session",
    rolling15m: "Rolling 15m",
    rolling60m: "Rolling 60m",
    rolling240m: "Rolling 240m",
    rolling24h: "Rolling 24h",
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
    loadToken: 0,
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
    focusSummary: document.getElementById("focusSummary"),
    referenceList: document.getElementById("referenceList"),
    ladderList: document.getElementById("ladderList"),
    profileLabel: document.getElementById("profileLabel"),
    eventCount: document.getElementById("eventCount"),
    eventRibbon: document.getElementById("eventRibbon"),
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
    elements.auctionMeta.textContent = [
      currentConfig().mode.toUpperCase(),
      currentFocusLabel(),
      "ticks " + state.rows.length,
      "state " + (focus.stateKind || "Unknown"),
      "location " + (focus.locationKind || "Unknown"),
      "action " + (focus.preferredAction || "NoTrade"),
      focus.startTs ? "from " + new Date(focus.startTs).toLocaleString() : null,
    ].filter(Boolean).join(" | ");
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

  function applyPayload(payload) {
    state.rows = Array.isArray(payload.rows) ? payload.rows.slice() : [];
    state.reviewEndId = payload.reviewEndId || null;
    state.auction = payload.auction || null;
    state.lastMetrics = payload.metrics || null;
  }

  function appendRows(rows) {
    const existingLastId = state.rows.length ? Number(state.rows[state.rows.length - 1].id || 0) : 0;
    (rows || []).forEach(function (row) {
      if (Number(row.id || 0) > existingLastId) {
        state.rows.push(row);
      }
    });
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
      elements.focusSummary.innerHTML = "<div class=\"sql-empty\">No focus summary yet.</div>";
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

  function renderEvents() {
    const events = state.auction?.events || [];
    elements.eventCount.textContent = String(events.length);
    if (!events.length) {
      elements.eventRibbon.innerHTML = "<div class=\"sql-empty\">No auction events yet.</div>";
      return;
    }
    elements.eventRibbon.innerHTML = events.map(function (event) {
      const tone = String(event.direction || "").toLowerCase() === "down" ? "down" : "up";
      return [
        "<article class=\"auction-event-chip is-", tone, "\">",
        "<div class=\"auction-event-title\">", escapeHtml(event.eventKind || "Event"), "</div>",
        "<div class=\"auction-event-price\">", escapeHtml(formatPrice(event.price1)), event.price2 != null ? " -> " + escapeHtml(formatPrice(event.price2)) : "", "</div>",
        "<div class=\"auction-event-meta\">", escapeHtml([(event.windowLabel || event.windowKind || "window"), "strength " + String(event.strength ?? "-")].join(" | ")), "</div>",
        "</article>",
      ].join("");
    }).join("");
  }

  function chartTooltip(params) {
    if (!params || !params.length) {
      return "";
    }
    const point = params[0];
    const row = point.data && point.data.row ? point.data.row : null;
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
        grid: { left: 56, right: 18, top: 24, bottom: 44 },
        xAxis: { type: "time", axisLabel: { color: "#91a1b8" }, axisLine: { lineStyle: { color: "rgba(147,181,255,0.16)" } } },
        yAxis: { type: "value", scale: true, axisLabel: { color: "#91a1b8" }, splitLine: { lineStyle: { color: "rgba(147,181,255,0.08)" } } },
        dataZoom: [{ type: "inside", zoomOnMouseWheel: true, moveOnMouseWheel: true }, { type: "slider", height: 20, bottom: 10 }],
        series: [],
      });
    }
    if (!state.profileChart) {
      state.profileChart = echarts.init(elements.profileChartHost, null, { renderer: "canvas" });
    }
  }

  function renderChart() {
    ensureCharts();
    const focus = state.auction?.focusWindow || null;
    const rowData = state.rows.map(function (row) {
      return { value: [row.timestampMs, row.mid], row: row };
    });
    const eventData = (focus?.events || []).map(function (event) {
      return { value: [event.eventTsMs, event.price1], event: event };
    });
    const priceValues = rowData.map(function (item) { return Number(item.value[1]); });
    if (focus?.highPrice != null) {
      priceValues.push(Number(focus.highPrice), Number(focus.lowPrice), Number(focus.vahPrice), Number(focus.valPrice), Number(focus.pocPrice));
    }
    const minPrice = priceValues.length ? Math.min.apply(null, priceValues) - 0.4 : null;
    const maxPrice = priceValues.length ? Math.max.apply(null, priceValues) + 0.4 : null;
    const markLines = [];
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
    (focus?.references || []).forEach(function (ref) {
      if (!lineStyleByRef[ref.refKind]) {
        return;
      }
      markLines.push({
        name: ref.refKind,
        yAxis: ref.price,
        lineStyle: lineStyleByRef[ref.refKind],
        label: { formatter: ref.refKind, color: lineStyleByRef[ref.refKind].color, fontSize: 10 },
      });
    });
    const markAreas = [];
    if (focus?.valPrice != null && focus?.vahPrice != null) {
      markAreas.push([{ yAxis: focus.valPrice, itemStyle: { color: "rgba(109,216,255,0.10)" } }, { yAxis: focus.vahPrice }]);
    }
    if (focus?.ibHigh != null && focus?.ibLow != null && focus?.startTsMs != null) {
      const ibEndMs = focus.startTsMs + ((focus.kind === "session" ? 60 : 15) * 60 * 1000);
      markAreas.push([
        { xAxis: focus.startTsMs, yAxis: focus.ibLow, itemStyle: { color: "rgba(255,179,92,0.10)" } },
        { xAxis: ibEndMs, yAxis: focus.ibHigh },
      ]);
    }
    if (focus?.acceptanceKind === "Accepted" && focus?.closePrice != null && focus?.vahPrice != null && focus.closePrice > focus.vahPrice) {
      markAreas.push([{ yAxis: focus.vahPrice, itemStyle: { color: "rgba(126,240,199,0.12)" } }, { yAxis: focus.highPrice }]);
    }
    if (focus?.acceptanceKind === "Rejected" && focus?.closePrice != null && focus?.valPrice != null && focus.closePrice < focus.valPrice) {
      markAreas.push([{ yAxis: focus.lowPrice, itemStyle: { color: "rgba(255,107,136,0.10)" } }, { yAxis: focus.valPrice }]);
    }
    state.chart.setOption({
      yAxis: { min: minPrice, max: maxPrice },
      series: [
        {
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
        },
        {
          id: "auction-events",
          type: "scatter",
          symbolSize: 11,
          data: eventData,
          itemStyle: {
            color: function (params) {
              return String(params.data?.event?.direction || "").toLowerCase() === "down" ? "#ff6b88" : "#7ef0c7";
            },
          },
        },
      ],
    });

    const profile = focus?.profile || [];
    elements.profileLabel.textContent = focus?.label || "Activity";
    state.profileChart.setOption({
      animation: false,
      backgroundColor: "transparent",
      grid: { left: 8, right: 18, top: 10, bottom: 18, containLabel: true },
      xAxis: { type: "value", axisLabel: { color: "#91a1b8" }, splitLine: { show: false } },
      yAxis: {
        type: "value",
        min: minPrice,
        max: maxPrice,
        axisLabel: { color: "#91a1b8", formatter: function (value) { return Number(value).toFixed(2); } },
        splitLine: { lineStyle: { color: "rgba(147,181,255,0.08)" } },
      },
      tooltip: {
        trigger: "item",
        formatter: function (param) {
          const item = param.data?.raw || {};
          return "<div class=\"chart-tip\"><div class=\"chart-tip-title\">Profile</div>"
            + "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Price</span><span class=\"chart-tip-value\">" + escapeHtml(formatPrice(item.priceBin)) + "</span></div>"
            + "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Activity</span><span class=\"chart-tip-value\">" + escapeHtml(String(item.activityScore || "-")) + "</span></div>"
            + "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Ticks</span><span class=\"chart-tip-value\">" + escapeHtml(String(item.tickCount || 0)) + "</span></div>"
            + "</div>";
        },
      },
      series: [{
        type: "bar",
        barWidth: 6,
        data: profile.map(function (item) {
          return {
            value: [item.activityScore, item.priceBin],
            raw: item,
            itemStyle: {
              color: item.isPoc ? "#ffb35c" : (item.inValue ? "rgba(109,216,255,0.78)" : "rgba(145,161,184,0.28)"),
            },
          };
        }),
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
    const params = new URLSearchParams({
      mode: config.mode,
      window: String(config.window),
      focusKind: config.focusKind,
    });
    if (startId != null) {
      params.set("id", String(startId));
    }
    const payload = await fetchJson("/api/auction/bootstrap?" + params.toString());
    applyPayload(payload);
    renderAll();
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
    elements.tickId.value = config.id;
    elements.windowSize.value = String(config.window);
    elements.reviewStart.value = config.reviewStart;
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

  elements.sidebarToggle.addEventListener("click", function () {
    setSidebarCollapsed(!state.ui.sidebarCollapsed);
  });
  elements.sidebarBackdrop.addEventListener("click", function () {
    setSidebarCollapsed(true);
  });
  elements.applyButton.addEventListener("click", function () {
    loadAll();
  });
  window.addEventListener("resize", function () {
    if (state.chart) {
      state.chart.resize();
    }
    if (state.profileChart) {
      state.profileChart.resize();
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
