(function () {
  if (window.__datavisBackboneInitialized) {
    return;
  }
  window.__datavisBackboneInitialized = true;

  const DEFAULTS = {
    view: "candles",
    layer: "backbone",
    candles: 35,
    ticks: 2000,
    showTicks: true,
    showBands: false,
    sizing: false,
    id: "",
  };
  const MAX_CANDLES = 400;
  const MAX_TICKS = 10000;
  const LIVE_REFRESH_MS = 2500;
  const TRADE_REFRESH_MS = 5000;
  const BANDS_PERIOD = 20;
  const BANDS_STD_MULTIPLIER = 2.0;
  const charting = window.DatavisCharting;
  const Y_AXIS_STYLE = {
    axisLabelColor: "#9eadc5",
    splitLineColor: "rgba(147,181,255,0.10)",
    targetTickCount: 6,
  };

  const state = {
    chart: null,
    payload: null,
    lastMetrics: null,
    loadToken: 0,
    zoom: null,
    viewport: charting.createViewportModel({ rightEdgeToleranceItems: 1 }),
    applyingZoom: false,
    resizeObserver: null,
    pollTimer: 0,
    inputs: {
      candles: DEFAULTS.candles,
      ticks: DEFAULTS.ticks,
    },
    ui: { sidebarCollapsed: true },
    trade: {
      authenticated: false,
      authConfigured: true,
      username: null,
      positions: [],
      smart: null,
      broker: null,
      busy: false,
      loginBusy: false,
      lastRefreshAtMs: 0,
      refreshPromise: null,
    },
  };

  const elements = {
    workspace: document.getElementById("backboneWorkspace"),
    sidebar: document.getElementById("backboneSidebar"),
    sidebarToggle: document.getElementById("sidebarToggle"),
    sidebarBackdrop: document.getElementById("sidebarBackdrop"),
    viewToggle: document.getElementById("viewToggle"),
    layerToggle: document.getElementById("layerToggle"),
    countLabel: document.getElementById("countLabel"),
    countInput: document.getElementById("countInput"),
    anchorId: document.getElementById("anchorId"),
    showBands: document.getElementById("showBands"),
    showTicks: document.getElementById("showTicks"),
    sizingToggle: document.getElementById("sizingToggle"),
    applyButton: document.getElementById("applyButton"),
    statusLine: document.getElementById("statusLine"),
    backboneMeta: document.getElementById("backboneMeta"),
    backbonePerf: document.getElementById("backbonePerf"),
    daySummary: document.getElementById("daySummary"),
    stateSummary: document.getElementById("stateSummary"),
    countsSummary: document.getElementById("countsSummary"),
    thresholdSummary: document.getElementById("thresholdSummary"),
    positionSummary: document.getElementById("positionSummary"),
    loginStatePill: document.getElementById("loginStatePill"),
    tradeStatusLine: document.getElementById("tradeStatusLine"),
    tradeLoginForm: document.getElementById("tradeLoginForm"),
    tradeUsername: document.getElementById("tradeUsername"),
    tradePassword: document.getElementById("tradePassword"),
    tradeLoginButton: document.getElementById("tradeLoginButton"),
    tradeLogoutButton: document.getElementById("tradeLogoutButton"),
    tradeSessionSummary: document.getElementById("tradeSessionSummary"),
    tradeBrokerSummary: document.getElementById("tradeBrokerSummary"),
    smartClosePill: document.getElementById("smartClosePill"),
    chartHost: document.getElementById("backboneChart"),
    buyButton: document.getElementById("backboneBuyButton"),
    sellButton: document.getElementById("backboneSellButton"),
    smartStatus: document.getElementById("backboneSmartStatus"),
    tradeHint: document.getElementById("backboneTradeHint"),
  };

  function clampCount(view, rawValue) {
    const fallback = view === "detailed" ? DEFAULTS.ticks : DEFAULTS.candles;
    const maximum = view === "detailed" ? MAX_TICKS : MAX_CANDLES;
    return Math.max(1, Math.min(maximum, Number.parseInt(rawValue || String(fallback), 10) || fallback));
  }

  function parseQuery() {
    const params = new URLSearchParams(window.location.search);
    const view = params.get("view") === "detailed" ? "detailed" : DEFAULTS.view;
    const layer = params.get("layer") === "bigbones" ? "bigbones" : DEFAULTS.layer;
    return {
      view: view,
      layer: layer,
      candles: clampCount("candles", params.get("candles")),
      ticks: clampCount("detailed", params.get("ticks")),
      showTicks: params.has("showTicks") ? params.get("showTicks") !== "0" : DEFAULTS.showTicks,
      showBands: params.get("showBands") === "1",
      sizing: params.get("sizing") === "1",
      id: params.get("id") || "",
    };
  }

  function currentConfig() {
    const view = elements.viewToggle.querySelector("button.active")?.dataset.value || DEFAULTS.view;
    return {
      view: view,
      layer: elements.layerToggle.querySelector("button.active")?.dataset.value || DEFAULTS.layer,
      candles: clampCount("candles", state.inputs.candles),
      ticks: clampCount("detailed", state.inputs.ticks),
      showTicks: Boolean(elements.showTicks.checked),
      showBands: Boolean(elements.showBands.checked),
      sizing: Boolean(elements.sizingToggle.checked),
      id: (elements.anchorId.value || "").trim(),
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
      view: config.view,
      layer: config.layer,
      candles: String(config.candles),
      ticks: String(config.ticks),
      showTicks: config.showTicks ? "1" : "0",
      showBands: config.showBands ? "1" : "0",
      sizing: config.sizing ? "1" : "0",
    });
    if (config.id) {
      params.set("id", config.id);
    }
    window.history.replaceState({}, "", window.location.pathname + "?" + params.toString());
  }

  function setSidebarCollapsed(collapsed) {
    state.ui.sidebarCollapsed = Boolean(collapsed);
    elements.workspace.classList.toggle("is-sidebar-collapsed", state.ui.sidebarCollapsed);
    elements.sidebarToggle.setAttribute("aria-expanded", String(!state.ui.sidebarCollapsed));
    if (state.chart) {
      requestAnimationFrame(function () {
        state.chart.resize();
      });
    }
  }

  function syncControlStates() {
    const config = currentConfig();
    elements.countLabel.textContent = config.view === "detailed" ? "ticks" : "candles";
    elements.countInput.value = String(config.view === "detailed" ? config.ticks : config.candles);
    elements.showBands.disabled = config.view !== "candles";
    elements.showTicks.disabled = config.view !== "detailed";
    elements.layerToggle.querySelectorAll("button").forEach(function (button) {
      button.disabled = config.view !== "candles";
    });
  }

  function isReviewMode() {
    return Boolean((currentConfig().id || "").trim());
  }

  function clearPolling() {
    if (state.pollTimer) {
      window.clearTimeout(state.pollTimer);
      state.pollTimer = 0;
    }
  }

  function schedulePolling() {
    clearPolling();
    if (isReviewMode()) {
      return;
    }
    state.pollTimer = window.setTimeout(function () {
      state.pollTimer = 0;
      loadData(false, { silentStatus: true }).catch(function (error) {
        status(error.message || "Backbone refresh failed.", true);
      });
    }, LIVE_REFRESH_MS);
  }

  function status(message, isError) {
    elements.statusLine.textContent = message;
    elements.statusLine.classList.toggle("error", Boolean(isError));
  }

  function tradeStatus(message, isError) {
    if (!elements.tradeStatusLine) {
      return;
    }
    elements.tradeStatusLine.textContent = message;
    elements.tradeStatusLine.classList.toggle("error", Boolean(isError));
  }

  function escapeHtml(value) {
    return String(value)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll("\"", "&quot;");
  }

  function formatTimestamp(value) {
    const numeric = Number(value);
    return Number.isFinite(numeric) ? new Date(numeric).toLocaleString() : "-";
  }

  function formatPrice(value) {
    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric.toFixed(2) : "-";
  }

  function formatSigned(value) {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      return "-";
    }
    return (numeric > 0 ? "+" : "") + numeric.toFixed(2);
  }

  function renderMeta() {
    const payload = state.payload || {};
    if (!payload.dayId) {
      elements.backboneMeta.textContent = "No backbone day loaded.";
      return;
    }
    const countText = payload.view === "detailed"
      ? ("ticks " + Number(payload.rowCount || 0))
      : ("candles " + Number(payload.candleCount || 0));
    elements.backboneMeta.textContent = [
      String(payload.layerLabel || "Backbone").toUpperCase(),
      payload.mode === "review" ? "review" : "broker day",
      countText,
      "left " + (payload.firstId ?? "-"),
      "right " + (payload.lastId ?? "-"),
    ].join(" | ");
  }

  function renderPerf() {
    const metrics = state.lastMetrics || {};
    const parts = ["Snapshot"];
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
    elements.backbonePerf.textContent = parts.join(" | ");
  }

  function summarizePosition(position) {
    if (!position) {
      return "No open position.";
    }
    const side = String(position.side || "position").toUpperCase();
    const entry = position.entryPrice != null ? formatPrice(position.entryPrice) : "-";
    const pnl = position.netUnrealizedPnl != null ? formatSigned(position.netUnrealizedPnl) : "-";
    return side + " #" + String(position.positionId || "-") + " | Entry " + entry + " | PnL " + pnl;
  }

  function renderInfo() {
    const payload = state.payload || {};
    const row = payload.state || {};
    elements.daySummary.textContent = payload.dayId
      ? ((payload.layerLabel || "Backbone") + " | broker day " + (payload.brokerday || "-") + " | dayId " + payload.dayId + (payload.mode === "review" ? " | review anchor " + (currentConfig().id || "-") : ""))
      : "Broker day unavailable.";
    elements.stateSummary.textContent = row.lastProcessedTickId
      ? ("Last processed point " + row.lastProcessedTickId + " | direction " + (row.direction || "None"))
      : ("No " + String(payload.layerLabel || "Backbone").toLowerCase() + " state yet.");
    elements.countsSummary.textContent = (payload.layerLabel || "Backbone") + " | " + Number(payload.pivotTotal || 0) + " pivots | " + Number(payload.moveTotal || 0) + " moves";
    elements.thresholdSummary.textContent = row.currentThreshold != null
      ? ("Threshold " + Number(row.currentThreshold).toFixed(4))
      : "Threshold -";
    elements.positionSummary.textContent = summarizePosition(state.trade.positions[0] || null);
  }

  function computeBands(candles) {
    if (!Array.isArray(candles) || candles.length < BANDS_PERIOD) {
      return { middle: [], upper: [], lower: [] };
    }
    const middle = [];
    const upper = [];
    const lower = [];
    for (let index = BANDS_PERIOD - 1; index < candles.length; index += 1) {
      const slice = candles.slice(index - BANDS_PERIOD + 1, index + 1);
      const closes = slice.map(function (item) { return Number(item.close); });
      const mean = closes.reduce(function (sum, value) { return sum + value; }, 0) / closes.length;
      const variance = closes.reduce(function (sum, value) {
        const delta = value - mean;
        return sum + (delta * delta);
      }, 0) / closes.length;
      const deviation = Math.sqrt(variance) * BANDS_STD_MULTIPLIER;
      const x = Number(candles[index].endTimeMs);
      middle.push({ value: [x, mean] });
      upper.push({ value: [x, mean + deviation] });
      lower.push({ value: [x, mean - deviation] });
    }
    return { middle: middle, upper: upper, lower: lower };
  }

  function tooltipFormatter(param) {
    const candle = param?.data?.candle;
    if (candle) {
      const layerLabel = String(state.payload?.layerLabel || "Backbone");
      return [
        "<div class=\"chart-tip\">",
        "<div class=\"chart-tip-section\">",
        "<div class=\"chart-tip-title\">", escapeHtml(layerLabel), " Candle</div>",
        "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Move</span><span class=\"chart-tip-value\">", escapeHtml(String(candle.moveId)), "</span></div>",
        "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Start</span><span class=\"chart-tip-value\">", escapeHtml(formatTimestamp(candle.startTimeMs)), "</span></div>",
        "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">End</span><span class=\"chart-tip-value\">", escapeHtml(formatTimestamp(candle.endTimeMs)), "</span></div>",
        "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Open</span><span class=\"chart-tip-value\">", escapeHtml(formatPrice(candle.open)), "</span></div>",
        "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">High</span><span class=\"chart-tip-value\">", escapeHtml(formatPrice(candle.high)), "</span></div>",
        "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Low</span><span class=\"chart-tip-value\">", escapeHtml(formatPrice(candle.low)), "</span></div>",
        "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Close</span><span class=\"chart-tip-value\">", escapeHtml(formatPrice(candle.close)), "</span></div>",
        "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Ticks</span><span class=\"chart-tip-value\">", escapeHtml(String(candle.tickCount || 0)), "</span></div>",
        "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Direction</span><span class=\"chart-tip-value\">", escapeHtml(String(candle.direction || "-")), "</span></div>",
        "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Delta</span><span class=\"chart-tip-value\">", escapeHtml(formatSigned(candle.priceDelta)), "</span></div>",
        "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Threshold</span><span class=\"chart-tip-value\">", escapeHtml(candle.thresholdAtConfirm != null ? Number(candle.thresholdAtConfirm).toFixed(4) : "-"), "</span></div>",
        "</div></div>",
      ].join("");
    }

    const pivot = param?.data?.pivot;
    if (pivot) {
      return [
        "<div class=\"chart-tip\">",
        "<div class=\"chart-tip-section\">",
        "<div class=\"chart-tip-title\">", escapeHtml(String(pivot.pivotType || "Pivot")), "</div>",
        "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Tick</span><span class=\"chart-tip-value\">", escapeHtml(String(pivot.tickId || "-")), "</span></div>",
        "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Time</span><span class=\"chart-tip-value\">", escapeHtml(formatTimestamp(pivot.tickTimeMs)), "</span></div>",
        "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Price</span><span class=\"chart-tip-value\">", escapeHtml(formatPrice(pivot.price)), "</span></div>",
        "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Threshold</span><span class=\"chart-tip-value\">", escapeHtml(pivot.threshold != null ? Number(pivot.threshold).toFixed(4) : "-"), "</span></div>",
        "</div></div>",
      ].join("");
    }

    const tick = param?.data?.tick;
    if (tick) {
      return [
        "<div class=\"chart-tip\">",
        "<div class=\"chart-tip-section\">",
        "<div class=\"chart-tip-title\">Raw Tick</div>",
        "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Tick</span><span class=\"chart-tip-value\">", escapeHtml(String(tick.id || "-")), "</span></div>",
        "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Time</span><span class=\"chart-tip-value\">", escapeHtml(formatTimestamp(tick.timestampMs)), "</span></div>",
        "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Mid</span><span class=\"chart-tip-value\">", escapeHtml(formatPrice(tick.mid)), "</span></div>",
        "</div></div>",
      ].join("");
    }

    const liveLeg = param?.data?.liveLeg;
    if (liveLeg) {
      return [
        "<div class=\"chart-tip\">",
        "<div class=\"chart-tip-section\">",
        "<div class=\"chart-tip-title\">Developing Leg</div>",
        "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Start</span><span class=\"chart-tip-value\">", escapeHtml(String(liveLeg.startTickId || "-")), "</span></div>",
        "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Latest</span><span class=\"chart-tip-value\">", escapeHtml(String(liveLeg.endTickId || "-")), "</span></div>",
        "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Direction</span><span class=\"chart-tip-value\">", escapeHtml(String(liveLeg.direction || "-")), "</span></div>",
        "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Threshold</span><span class=\"chart-tip-value\">", escapeHtml(liveLeg.threshold != null ? Number(liveLeg.threshold).toFixed(4) : "-"), "</span></div>",
        "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Latest Price</span><span class=\"chart-tip-value\">", escapeHtml(formatPrice(liveLeg.endPrice)), "</span></div>",
        "</div></div>",
      ].join("");
    }

    return "";
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
        grid: { left: 58, right: 18, top: 16, bottom: 58 },
        tooltip: {
          trigger: "item",
          confine: true,
          formatter: tooltipFormatter,
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
        state.zoom = zoom ? {
          start: zoom.start,
          end: zoom.end,
          startValue: zoom.startValue,
          endValue: zoom.endValue,
        } : null;
        const viewportState = state.viewport.captureZoom(zoom, primaryXValues(state.payload));
        state.chart.setOption({
          yAxis: visibleYBounds(state.payload, { visibleRange: viewportRange(viewportState) }),
        }, { lazyUpdate: true });
      });
      if (typeof ResizeObserver === "function") {
        state.resizeObserver = new ResizeObserver(function () {
          if (state.chart) {
            state.chart.resize();
          }
        });
        state.resizeObserver.observe(elements.chartHost);
      }
      window.addEventListener("resize", function () {
        if (state.chart) {
          state.chart.resize();
        }
      });
    }
    return state.chart;
  }

  function renderCandleSeries(payload) {
    const series = [];
    const candles = Array.isArray(payload.candles) ? payload.candles : [];
    if (!candles.length) {
      return series;
    }
    series.push({
      name: "backbone-candles",
      type: "candlestick",
      data: candles.map(function (candle) {
        return {
          value: [Number(candle.endTimeMs), Number(candle.open), Number(candle.close), Number(candle.low), Number(candle.high)],
          candle: candle,
        };
      }),
      itemStyle: {
        color: "#7ef0c7",
        color0: "#ff6b88",
        borderColor: "#7ef0c7",
        borderColor0: "#ff6b88",
      },
      emphasis: {
        itemStyle: {
          borderWidth: 1.2,
        },
      },
      z: 4,
    });
    if (currentConfig().showBands) {
      const bands = computeBands(candles);
      [
        { name: "bb-middle", data: bands.middle, color: "rgba(109, 216, 255, 0.72)" },
        { name: "bb-upper", data: bands.upper, color: "rgba(255, 200, 87, 0.76)" },
        { name: "bb-lower", data: bands.lower, color: "rgba(126, 240, 199, 0.68)" },
      ].forEach(function (band) {
        if (!band.data.length) {
          return;
        }
        series.push({
          name: band.name,
          type: "line",
          data: band.data,
          showSymbol: false,
          smooth: false,
          lineStyle: { width: 1.4, color: band.color },
          z: 5,
        });
      });
    }
    return series;
  }

  function renderDetailedSeries(payload) {
    const series = [];
    const rows = Array.isArray(payload.rows) ? payload.rows : [];
    const pivots = Array.isArray(payload.pivots) ? payload.pivots : [];
    if (currentConfig().showTicks && rows.length) {
      series.push({
        name: "ticks",
        type: "line",
        data: rows.map(function (row) {
          return { value: [Number(row.timestampMs), Number(row.mid)], tick: row };
        }),
        showSymbol: false,
        lineStyle: { width: 1, color: "rgba(109, 216, 255, 0.22)" },
        z: 1,
      });
    }
    if (pivots.length) {
      series.push({
        name: "backbone",
        type: "line",
        data: pivots.map(function (pivot) {
          return { value: [Number(pivot.tickTimeMs), Number(pivot.price)], pivot: pivot };
        }),
        showSymbol: false,
        lineStyle: { width: 2.6, color: "#ffb35c" },
        itemStyle: { color: "#ffb35c" },
        z: 4,
      });
      [
        { type: "Start", color: "#6dd8ff", symbol: "diamond", size: 8 },
        { type: "High", color: "#ff6b88", symbol: "triangle", size: 10 },
        { type: "Low", color: "#7ef0c7", symbol: "triangle", size: 10, rotate: 180 },
      ].forEach(function (definition) {
        const filtered = pivots.filter(function (pivot) {
          return pivot.pivotType === definition.type;
        });
        if (!filtered.length) {
          return;
        }
        series.push({
          name: definition.type.toLowerCase(),
          type: "scatter",
          symbol: definition.symbol,
          symbolRotate: definition.rotate || 0,
          symbolSize: definition.size,
          data: filtered.map(function (pivot) {
            return { value: [Number(pivot.tickTimeMs), Number(pivot.price)], pivot: pivot };
          }),
          itemStyle: { color: definition.color },
          z: 6,
        });
      });
    }
    if (payload.liveLeg?.startTimeMs && payload.liveLeg?.endTimeMs) {
      const liveLegPoints = [
        {
          value: [Number(payload.liveLeg.startTimeMs), Number(payload.liveLeg.startPrice)],
          liveLeg: payload.liveLeg,
        },
      ];
      if (
        payload.liveLeg.candidateTimeMs
        && payload.liveLeg.candidatePrice != null
        && Number(payload.liveLeg.candidateTickId || 0) !== Number(payload.liveLeg.startTickId || 0)
        && Number(payload.liveLeg.candidateTickId || 0) !== Number(payload.liveLeg.endTickId || 0)
      ) {
        liveLegPoints.push({
          value: [Number(payload.liveLeg.candidateTimeMs), Number(payload.liveLeg.candidatePrice)],
          liveLeg: payload.liveLeg,
        });
      }
      liveLegPoints.push({
        value: [Number(payload.liveLeg.endTimeMs), Number(payload.liveLeg.endPrice)],
        liveLeg: payload.liveLeg,
      });
      series.push({
        name: "developing-leg",
        type: "line",
        data: liveLegPoints,
        showSymbol: false,
        lineStyle: { width: 2, type: "dashed", color: "rgba(255, 255, 255, 0.78)" },
        z: 5,
      });
    }
    return series;
  }

  function primaryXValues(payload) {
    if (payload?.view === "detailed") {
      return (Array.isArray(payload?.rows) ? payload.rows : [])
        .map(function (row) { return Number(row.timestampMs); })
        .filter(Number.isFinite);
    }
    return (Array.isArray(payload?.candles) ? payload.candles : [])
      .map(function (candle) { return Number(candle.endTimeMs); })
      .filter(Number.isFinite);
  }

  function viewportRange(viewportState) {
    return viewportState
      ? { min: viewportState.startValue, max: viewportState.endValue }
      : null;
  }

  function pushYAxisItem(items, item) {
    if (item) {
      items.push(item);
    }
  }

  function buildYAxisItems(payload) {
    const config = currentConfig();
    const coreItems = [];
    const overlayItems = [];
    if (payload?.view === "candles") {
      const candles = Array.isArray(payload.candles) ? payload.candles : [];
      candles.forEach(function (candle) {
        pushYAxisItem(coreItems, charting.rangeItem(candle.endTimeMs, candle.endTimeMs, candle.low, candle.high));
      });
      if (config.showBands) {
        const bands = computeBands(candles);
        [bands.middle, bands.upper, bands.lower].forEach(function (bandPoints) {
          bandPoints.forEach(function (point) {
            pushYAxisItem(overlayItems, charting.pointItem(point[0], point[1]));
          });
        });
      }
    } else {
      const rows = Array.isArray(payload?.rows) ? payload.rows : [];
      if (config.showTicks) {
        rows.forEach(function (row) {
          pushYAxisItem(coreItems, charting.pointItem(row.timestampMs, row.mid));
        });
      }
      const pivots = Array.isArray(payload?.pivots) ? payload.pivots : [];
      pivots.forEach(function (pivot) {
        pushYAxisItem(coreItems, charting.pointItem(pivot.tickTimeMs, pivot.price));
      });
      if (payload?.liveLeg) {
        [
          [payload.liveLeg.startTimeMs, payload.liveLeg.startPrice],
          [payload.liveLeg.candidateTimeMs, payload.liveLeg.candidatePrice],
          [payload.liveLeg.endTimeMs, payload.liveLeg.endPrice],
        ].forEach(function (point) {
          pushYAxisItem(coreItems, charting.pointItem(point[0], point[1]));
        });
      }
    }
    return { coreItems: coreItems, overlayItems: overlayItems };
  }

  function visibleYBounds(payload, options) {
    const sources = buildYAxisItems(payload);
    return charting.buildVisibleIntegerYAxis({
      visibleRange: options?.visibleRange || viewportRange(state.viewport.currentWindow()),
      coreItems: sources.coreItems,
      overlayItems: sources.overlayItems,
      includeOverlays: currentConfig().sizing,
      ...Y_AXIS_STYLE,
    });
  }

  function applyVisibleYAxis() {
    if (!state.chart || !state.payload) {
      return;
    }
    state.chart.setOption({
      yAxis: visibleYBounds(state.payload),
    }, { lazyUpdate: true });
  }

  function renderChart(options) {
    const chart = ensureChart();
    if (!chart) {
      requestAnimationFrame(function () {
        renderChart(options);
      });
      return;
    }
    const payload = state.payload || {};
    const series = payload.view === "detailed"
      ? renderDetailedSeries(payload)
      : renderCandleSeries(payload);
    if (options?.resetView) {
      state.zoom = null;
      state.viewport.reset();
    }
    const viewportState = state.viewport.projectWindow(primaryXValues(payload), { reset: Boolean(options?.resetView) });
    const zoom = viewportState
      ? { startValue: viewportState.startValue, endValue: viewportState.endValue }
      : {};
    state.applyingZoom = true;
    chart.setOption({
      series: series,
      yAxis: visibleYBounds(payload, { visibleRange: viewportRange(viewportState) }),
      dataZoom: [
        { id: "zoom-inside", type: "inside", startValue: zoom.startValue, endValue: zoom.endValue },
        { id: "zoom-slider", type: "slider", startValue: zoom.startValue, endValue: zoom.endValue },
      ],
    }, { replaceMerge: ["series"], lazyUpdate: true });
    requestAnimationFrame(function () {
      state.applyingZoom = false;
    });
  }

  function fetchJson(url, options) {
    return fetch(url, options).then(async function (response) {
      const payload = await response.json().catch(function () { return {}; });
      if (!response.ok) {
        const detail = payload?.detail;
        const message = typeof detail === "string"
          ? detail
          : (detail?.message || payload?.message || "Request failed.");
        throw new Error(message);
      }
      return payload;
    });
  }

  function applyTradeSessionPayload(payload) {
    state.trade.authConfigured = payload?.authConfigured !== false;
    state.trade.authenticated = state.trade.authConfigured && Boolean(payload?.authenticated);
    state.trade.username = state.trade.authenticated ? (payload?.username || null) : null;
    state.trade.broker = payload?.broker || state.trade.broker || null;
    if (!state.trade.authenticated) {
      state.trade.positions = [];
      state.trade.smart = null;
    }
  }

  function currentSmartArmed(side) {
    const smart = state.trade.smart || {};
    if (side === "buy") {
      return Boolean(smart.smartBuyArmed);
    }
    if (side === "sell") {
      return Boolean(smart.smartSellArmed);
    }
    return Boolean(smart.smartCloseEnabled ?? smart.smartCloseArmed);
  }

  function tradeBrokerReady() {
    return Boolean(state.trade.broker?.ready);
  }

  function smartContextAllowed() {
    return !isReviewMode();
  }

  function renderTradeState() {
    const smart = state.trade.smart || {};
    const busy = state.trade.busy;
    const loginBusy = state.trade.loginBusy;
    const smartCloseArmed = currentSmartArmed("close");
    if (elements.loginStatePill) {
      elements.loginStatePill.textContent = state.trade.authenticated ? "Ready" : (state.trade.authConfigured ? "Locked" : "Unavailable");
      elements.loginStatePill.classList.toggle("ready", state.trade.authenticated);
    }
    if (elements.tradeSessionSummary) {
      elements.tradeSessionSummary.textContent = state.trade.authenticated
        ? ((state.trade.username || "trade user") + " | " + (state.trade.broker?.symbol || "Broker"))
        : "Trade login required.";
    }
    if (elements.tradeBrokerSummary) {
      elements.tradeBrokerSummary.textContent = state.trade.broker?.reason
        || (state.trade.broker?.ready ? "Broker ready." : "Broker state unavailable.");
    }
    if (elements.tradeUsername) {
      elements.tradeUsername.disabled = !state.trade.authConfigured || loginBusy || busy || state.trade.authenticated;
    }
    if (elements.tradePassword) {
      elements.tradePassword.disabled = !state.trade.authConfigured || loginBusy || busy || state.trade.authenticated;
    }
    if (elements.tradeLoginButton) {
      elements.tradeLoginButton.disabled = !state.trade.authConfigured || loginBusy || busy || state.trade.authenticated;
      elements.tradeLoginButton.textContent = loginBusy ? "Working..." : "Login";
    }
    if (elements.tradeLogoutButton) {
      elements.tradeLogoutButton.disabled = !state.trade.authConfigured || loginBusy || busy || !state.trade.authenticated;
    }
    elements.smartClosePill.textContent = smartCloseArmed ? "Smart Close ON" : "Smart Close OFF";
    elements.smartClosePill.classList.toggle("ready", smartCloseArmed);
    if (!state.trade.authConfigured) {
      elements.smartStatus.textContent = "Trade login is not configured on the server.";
      elements.tradeHint.textContent = "Trading unavailable.";
    } else if (!state.trade.authenticated) {
      elements.smartStatus.textContent = "Smart Close state unavailable until trade login.";
      elements.tradeHint.textContent = "Login here to arm Buy or Sell.";
    } else {
      const backendState = String(smart.backendState || smart.state?.backendState || "idle").replaceAll("_", " ");
      elements.smartStatus.textContent = [
        smart.smartCloseServerSide ? "Server-side" : "Client-side",
        smartCloseArmed ? "Smart Close ON" : "Smart Close OFF",
        backendState,
      ].join(" | ");
      if (!smartContextAllowed()) {
        elements.tradeHint.textContent = "Review anchor loaded. Smart entry stays disabled.";
      } else if (!tradeBrokerReady()) {
        elements.tradeHint.textContent = String(state.trade.broker?.reason || "Broker unavailable.");
      } else {
        elements.tradeHint.textContent = smart.statusText || smart.state?.statusText || "Smart entry ready.";
      }
    }

    const entryReady = state.trade.authConfigured && state.trade.authenticated && smartContextAllowed() && tradeBrokerReady() && !busy && !loginBusy;
    elements.buyButton.disabled = !entryReady;
    elements.sellButton.disabled = !entryReady;
    elements.buyButton.textContent = busy ? "Working..." : (currentSmartArmed("buy") ? "Buy Armed" : "Buy");
    elements.sellButton.textContent = busy ? "Working..." : (currentSmartArmed("sell") ? "Sell Armed" : "Sell");
    renderInfo();
  }

  async function refreshTradeState(options) {
    if (state.trade.refreshPromise) {
      return state.trade.refreshPromise;
    }
    if (options?.respectInterval && (Date.now() - Number(state.trade.lastRefreshAtMs || 0)) < TRADE_REFRESH_MS) {
      return null;
    }
    state.trade.refreshPromise = (async function () {
      try {
        const auth = await fetchJson("/api/trade/me");
        applyTradeSessionPayload(auth);
        if (!state.trade.authenticated) {
          state.trade.broker = auth.broker || null;
          return;
        }
        const openPayload = await fetchJson("/api/trade/open");
        state.trade.positions = Array.isArray(openPayload.positions) ? openPayload.positions : [];
        state.trade.smart = openPayload.smart || null;
        state.trade.broker = openPayload.broker || null;
        state.trade.lastRefreshAtMs = Date.now();
      } catch (error) {
        if (!options?.silent) {
          status(error.message || "Trade state refresh failed.", true);
        }
      } finally {
        renderTradeState();
        state.trade.refreshPromise = null;
      }
    })();
    return state.trade.refreshPromise;
  }

  async function requestTradeLogin() {
    if (state.trade.loginBusy || state.trade.busy) {
      return;
    }
    state.trade.loginBusy = true;
    renderTradeState();
    try {
      const payload = await fetchJson("/api/trade/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          username: elements.tradeUsername.value,
          password: elements.tradePassword.value,
        }),
      });
      elements.tradePassword.value = "";
      applyTradeSessionPayload({
        authenticated: true,
        username: payload.username,
        authConfigured: true,
        broker: state.trade.broker,
      });
      await refreshTradeState({ silent: true });
      await syncSmartContext().catch(function () {});
      tradeStatus("Trade login successful.", false);
    } catch (error) {
      tradeStatus(error.message || "Trade login failed.", true);
    } finally {
      state.trade.loginBusy = false;
      renderTradeState();
    }
  }

  async function requestTradeLogout() {
    if (state.trade.loginBusy || state.trade.busy) {
      return;
    }
    state.trade.busy = true;
    renderTradeState();
    try {
      await fetchJson("/api/trade/logout", { method: "POST" });
    } catch (error) {
      void error;
    } finally {
      applyTradeSessionPayload({
        authenticated: false,
        username: null,
        authConfigured: state.trade.authConfigured,
        broker: state.trade.broker,
      });
      state.trade.busy = false;
      renderTradeState();
      tradeStatus("Trade session logged out.", false);
    }
  }

  async function loadTradeSession() {
    try {
      const payload = await fetchJson("/api/trade/me");
      applyTradeSessionPayload(payload);
      renderTradeState();
      if (payload.authenticated) {
        await refreshTradeState({ silent: true });
        await syncSmartContext().catch(function () {});
      }
      tradeStatus(payload.authenticated ? "Trade session active." : "Trade login required.", false);
    } catch (error) {
      applyTradeSessionPayload({ authenticated: false, username: null, authConfigured: true, broker: state.trade.broker });
      renderTradeState();
      tradeStatus(error.message || "Trade session check failed.", true);
    }
  }

  async function syncSmartContext() {
    if (!state.trade.authenticated || !smartContextAllowed()) {
      return;
    }
    const payload = await fetchJson("/api/trade/smart/context", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ page: "backbone", mode: "live", run: "run" }),
    });
    state.trade.smart = payload;
    renderTradeState();
  }

  async function toggleSmartEntry(side) {
    if (state.trade.busy) {
      return;
    }
    if (!state.trade.authenticated) {
      status("Trade login required.", true);
      return;
    }
    if (!smartContextAllowed()) {
      status("Smart entry is disabled while a review anchor is loaded.", true);
      return;
    }
    state.trade.busy = true;
    renderTradeState();
    try {
      await syncSmartContext();
      const payload = await fetchJson("/api/trade/smart/entry", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ side: side, armed: !currentSmartArmed(side) }),
      });
      state.trade.smart = payload;
      status(payload?.statusText || payload?.state?.statusText || "Smart entry updated.", false);
      await refreshTradeState({ silent: true });
    } catch (error) {
      status(error.message || "Smart entry update failed.", true);
    } finally {
      state.trade.busy = false;
      renderTradeState();
    }
  }

  async function loadData(resetView, options) {
    const token = state.loadToken + 1;
    state.loadToken = token;
    clearPolling();
    writeQuery();
    const config = currentConfig();
    const params = new URLSearchParams();
    if (config.id) {
      params.set("id", config.id);
    }
    let endpoint = "/api/backbone/candles";
    if (config.view === "detailed") {
      endpoint = "/api/backbone/detail";
      params.set("ticks", String(config.ticks));
    } else {
      params.set("candles", String(config.candles));
      params.set("layer", String(config.layer));
    }
    try {
      const payload = await fetchJson(endpoint + "?" + params.toString());
      if (token !== state.loadToken) {
        return;
      }
      state.payload = payload;
      state.lastMetrics = payload.metrics || null;
      renderMeta();
      renderPerf();
      renderInfo();
      renderChart({ resetView: Boolean(resetView) });
      renderTradeState();
      if (!options?.silentStatus) {
        status(
          payload.view === "detailed"
            ? ("Loaded " + Number(payload.rowCount || 0) + " tick(s).")
            : ("Loaded " + Number(payload.candleCount || 0) + " " + String(payload.layerLabel || "Backbone") + " candle(s)."),
          false,
        );
      }
      await refreshTradeState({ silent: true, respectInterval: true });
      schedulePolling();
    } catch (error) {
      if (token === state.loadToken) {
        status(error.message || "Backbone load failed.", true);
      }
    }
  }

  function applyInitialConfig(config) {
    state.inputs.candles = clampCount("candles", config.candles);
    state.inputs.ticks = clampCount("detailed", config.ticks);
    setSegment(elements.viewToggle, config.view);
    setSegment(elements.layerToggle, config.layer);
    elements.showTicks.checked = Boolean(config.showTicks);
    elements.showBands.checked = Boolean(config.showBands);
    elements.sizingToggle.checked = Boolean(config.sizing);
    elements.anchorId.value = config.id;
    setSidebarCollapsed(true);
    syncControlStates();
    renderMeta();
    renderPerf();
    renderTradeState();
    writeQuery();
  }

  bindSegment(elements.viewToggle, function (value) {
    setSegment(elements.viewToggle, value);
    syncControlStates();
    writeQuery();
    status("View updated. Click Load to refresh data.", false);
  });

  bindSegment(elements.layerToggle, function (value) {
    setSegment(elements.layerToggle, value);
    syncControlStates();
    writeQuery();
    status("Candle layer updated. Click Load to refresh data.", false);
  });

  [elements.countInput, elements.anchorId].forEach(function (control) {
    control.addEventListener("change", function () {
      if (control === elements.countInput) {
        const view = currentConfig().view;
        if (view === "detailed") {
          state.inputs.ticks = clampCount("detailed", elements.countInput.value);
        } else {
          state.inputs.candles = clampCount("candles", elements.countInput.value);
        }
      }
      syncControlStates();
      writeQuery();
    });
  });

  elements.showBands.addEventListener("change", function () {
    writeQuery();
    renderChart({ resetView: false });
  });

  elements.showTicks.addEventListener("change", function () {
    writeQuery();
    renderChart({ resetView: false });
  });

  elements.sizingToggle.addEventListener("change", function () {
    writeQuery();
    renderChart({ resetView: false });
    status("Sizing updated.", false);
  });

  elements.sidebarToggle.addEventListener("click", function () {
    setSidebarCollapsed(!state.ui.sidebarCollapsed);
  });
  elements.sidebarBackdrop.addEventListener("click", function () {
    setSidebarCollapsed(true);
  });
  elements.applyButton.addEventListener("click", function () {
    loadData(true);
  });
  elements.tradeLoginForm.addEventListener("submit", function (event) {
    event.preventDefault();
    requestTradeLogin();
  });
  elements.tradeLogoutButton.addEventListener("click", function () {
    requestTradeLogout();
  });
  elements.buyButton.addEventListener("click", function () {
    toggleSmartEntry("buy");
  });
  elements.sellButton.addEventListener("click", function () {
    toggleSmartEntry("sell");
  });

  applyInitialConfig(parseQuery());
  loadTradeSession();
  loadData(true);
}());
