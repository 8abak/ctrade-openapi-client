(function () {
  const DEFAULTS = {
    mode: "live",
    run: "run",
    id: "",
    window: 2000,
    fast: 240,
    slow: 1200,
    fastOrder: 2,
    slowOrder: 3,
    series: "mid",
    view: "time",
    overlays: {
      fastPoly: true,
      slowPoly: true,
    },
  };

  const SERIES_CONFIG = {
    ask: { label: "Ask", field: "ask", color: "#ffb35c", accent: "rgba(255, 179, 92, 0.16)" },
    bid: { label: "Bid", field: "bid", color: "#7ef0c7", accent: "rgba(126, 240, 199, 0.16)" },
    mid: { label: "Mid", field: "mid", color: "#6dd8ff", accent: "rgba(109, 216, 255, 0.16)" },
  };

  const LINEAR_STYLE = {
    fast: { label: "Fast linear", color: "#ff8f5a", width: 1.7, type: [6, 4] },
    slow: { label: "Slow linear", color: "#8bf0cf", width: 1.9, type: [9, 4] },
  };

  const POLY_STYLE = {
    fast: { label: "Fast poly", color: "#ff5f45", width: 2.6 },
    slow: { label: "Slow poly", color: "#78f3bf", width: 2.8 },
  };

  const state = {
    chart: null,
    payload: null,
    tuningPayload: null,
    currentMode: DEFAULTS.mode,
    currentRun: DEFAULTS.run,
    currentSeries: DEFAULTS.series,
    currentView: DEFAULTS.view,
    overlayState: { ...DEFAULTS.overlays },
    pollTimer: null,
    pollInFlight: false,
    tuningInFlight: false,
  };

  const elements = {
    modeToggle: document.getElementById("modeToggle"),
    runToggle: document.getElementById("runToggle"),
    tickId: document.getElementById("tickId"),
    windowSize: document.getElementById("windowSize"),
    fastWindowSize: document.getElementById("fastWindowSize"),
    slowWindowSize: document.getElementById("slowWindowSize"),
    fastOrderSize: document.getElementById("fastOrderSize"),
    slowOrderSize: document.getElementById("slowOrderSize"),
    seriesSelector: document.getElementById("seriesSelector"),
    polyOverlaySelector: document.getElementById("polyOverlaySelector"),
    viewSelector: document.getElementById("viewSelector"),
    applyButton: document.getElementById("applyButton"),
    tuneButton: document.getElementById("tuneButton"),
    useTunedButton: document.getElementById("useTunedButton"),
    regressionMeta: document.getElementById("regressionMeta"),
    regressionStatus: document.getElementById("regressionStatus"),
    regressionNarrative: document.getElementById("regressionNarrative"),
    chartHost: document.getElementById("regressionChart"),
    metricFastSlope: document.getElementById("metricFastSlope"),
    metricFastDetail: document.getElementById("metricFastDetail"),
    metricSlowSlope: document.getElementById("metricSlowSlope"),
    metricSlowDetail: document.getElementById("metricSlowDetail"),
    metricAlignment: document.getElementById("metricAlignment"),
    metricAlignmentDetail: document.getElementById("metricAlignmentDetail"),
    metricFastFit: document.getElementById("metricFastFit"),
    metricFastFitDetail: document.getElementById("metricFastFitDetail"),
    metricSlowFit: document.getElementById("metricSlowFit"),
    metricSlowFitDetail: document.getElementById("metricSlowFitDetail"),
    metricBreakPressure: document.getElementById("metricBreakPressure"),
    metricBreakPressureDetail: document.getElementById("metricBreakPressureDetail"),
    metricSplit: document.getElementById("metricSplit"),
    metricSplitDetail: document.getElementById("metricSplitDetail"),
    metricFastPoly: document.getElementById("metricFastPoly"),
    metricFastPolyDetail: document.getElementById("metricFastPolyDetail"),
    metricSlowPoly: document.getElementById("metricSlowPoly"),
    metricSlowPolyDetail: document.getElementById("metricSlowPolyDetail"),
    metricPolyStructure: document.getElementById("metricPolyStructure"),
    metricPolyStructureDetail: document.getElementById("metricPolyStructureDetail"),
    metricMoveQuality: document.getElementById("metricMoveQuality"),
    metricMoveQualityDetail: document.getElementById("metricMoveQualityDetail"),
    metricAdaptive: document.getElementById("metricAdaptive"),
    metricAdaptiveDetail: document.getElementById("metricAdaptiveDetail"),
  };

  function parseBoolParam(value, fallback) {
    if (value == null) {
      return fallback;
    }
    return !(value === "0" || value === "false" || value === "off");
  }

  function parseQuery() {
    const params = new URLSearchParams(window.location.search);
    const mode = params.get("mode") === "review" ? "review" : DEFAULTS.mode;
    const run = params.get("run") === "stop" ? "stop" : DEFAULTS.run;
    const windowSize = Number.parseInt(params.get("window"), 10);
    const fast = Number.parseInt(params.get("fast"), 10);
    const slow = Number.parseInt(params.get("slow"), 10);
    const fastOrder = Number.parseInt(params.get("fastOrder"), 10);
    const slowOrder = Number.parseInt(params.get("slowOrder"), 10);
    const series = SERIES_CONFIG[params.get("series")] ? params.get("series") : DEFAULTS.series;
    const view = params.get("view") === "scatter" ? "scatter" : DEFAULTS.view;

    return {
      mode,
      run,
      id: params.get("id") || DEFAULTS.id,
      window: Number.isFinite(windowSize) && windowSize > 0 ? windowSize : DEFAULTS.window,
      fast: Number.isFinite(fast) && fast >= 20 ? fast : DEFAULTS.fast,
      slow: Number.isFinite(slow) && slow >= 20 ? slow : DEFAULTS.slow,
      fastOrder: Number.isFinite(fastOrder) && fastOrder >= 1 ? fastOrder : DEFAULTS.fastOrder,
      slowOrder: Number.isFinite(slowOrder) && slowOrder >= 1 ? slowOrder : DEFAULTS.slowOrder,
      series,
      view,
      overlays: {
        fastPoly: parseBoolParam(params.get("fastPoly"), DEFAULTS.overlays.fastPoly),
        slowPoly: parseBoolParam(params.get("slowPoly"), DEFAULTS.overlays.slowPoly),
      },
    };
  }

  function clampInt(rawValue, minimum, maximum, fallback) {
    const parsed = Number.parseInt(rawValue, 10);
    if (!Number.isFinite(parsed)) {
      return fallback;
    }
    return Math.max(minimum, Math.min(maximum, parsed));
  }

  function setSegment(container, value, attributeName) {
    container.querySelectorAll("button").forEach((button) => {
      button.classList.toggle("active", button.dataset[attributeName] === value || button.dataset.value === value);
    });
  }

  function syncOverlayButtons() {
    elements.polyOverlaySelector.querySelectorAll("button").forEach((button) => {
      button.classList.toggle("active", Boolean(state.overlayState[button.dataset.overlay]));
    });
  }

  function syncControls(config) {
    state.currentMode = config.mode;
    state.currentRun = config.run;
    state.currentSeries = config.series;
    state.currentView = config.view;
    state.overlayState = { ...config.overlays };
    elements.tickId.value = config.id || "";
    elements.windowSize.value = String(config.window);
    elements.fastWindowSize.value = String(config.fast);
    elements.slowWindowSize.value = String(config.slow);
    elements.fastOrderSize.value = String(config.fastOrder);
    elements.slowOrderSize.value = String(config.slowOrder);
    setSegment(elements.modeToggle, config.mode, "value");
    setSegment(elements.runToggle, config.run, "value");
    setSegment(elements.seriesSelector, config.series, "series");
    setSegment(elements.viewSelector, config.view, "view");
    syncOverlayButtons();
  }

  function currentConfig() {
    const windowSize = clampInt(elements.windowSize.value, 1, 10000, DEFAULTS.window);
    const fastWindow = clampInt(elements.fastWindowSize.value, 20, 10000, DEFAULTS.fast);
    const slowWindow = clampInt(elements.slowWindowSize.value, 20, 10000, DEFAULTS.slow);
    return {
      mode: state.currentMode,
      run: state.currentRun,
      id: elements.tickId.value.trim(),
      window: windowSize,
      fast: Math.min(fastWindow, windowSize),
      slow: Math.min(Math.max(slowWindow, fastWindow), windowSize),
      fastOrder: clampInt(elements.fastOrderSize.value, 1, 5, DEFAULTS.fastOrder),
      slowOrder: clampInt(elements.slowOrderSize.value, 1, 5, DEFAULTS.slowOrder),
      series: state.currentSeries,
      view: state.currentView,
      overlays: { ...state.overlayState },
    };
  }

  function writeQuery(config) {
    const params = new URLSearchParams();
    params.set("mode", config.mode);
    params.set("run", config.run);
    params.set("window", String(config.window));
    params.set("fast", String(config.fast));
    params.set("slow", String(config.slow));
    params.set("fastOrder", String(config.fastOrder));
    params.set("slowOrder", String(config.slowOrder));
    params.set("series", config.series);
    params.set("view", config.view);
    params.set("fastPoly", config.overlays.fastPoly ? "1" : "0");
    params.set("slowPoly", config.overlays.slowPoly ? "1" : "0");
    if (config.id) {
      params.set("id", config.id);
    }
    history.replaceState(null, "", "/regression?" + params.toString());
  }

  function status(message, isError) {
    elements.regressionStatus.textContent = message;
    elements.regressionStatus.classList.toggle("error", Boolean(isError));
  }

  function ensureChart() {
    if (!state.chart) {
      state.chart = echarts.init(elements.chartHost, null, { renderer: "canvas" });
      window.addEventListener("resize", () => state.chart.resize());
    }
    return state.chart;
  }

  function stopPolling() {
    if (state.pollTimer) {
      window.clearTimeout(state.pollTimer);
      state.pollTimer = null;
    }
  }

  function schedulePolling() {
    stopPolling();
    if (state.currentRun !== "run" || !state.payload || !state.payload.lastId) {
      return;
    }
    state.pollTimer = window.setTimeout(pollNext, 1000);
  }

  function selectSeriesValue(row, series) {
    return Number(row[SERIES_CONFIG[series].field]);
  }

  function formatPrice(value) {
    if (value == null || !Number.isFinite(Number(value))) {
      return "--";
    }
    return Number(value).toFixed(2);
  }

  function formatSigned(value, digits) {
    const amount = Number(value || 0);
    const fixed = amount.toFixed(digits == null ? 4 : digits);
    return amount > 0 ? "+" + fixed : fixed;
  }

  function formatPercent(value) {
    return Number(value * 100).toFixed(1) + "%";
  }

  function formatTime(timestampMs) {
    return new Date(timestampMs).toLocaleString("en-AU", { hour12: false });
  }

  function formatMetricValue(value, digits) {
    if (value == null || !Number.isFinite(Number(value))) {
      return "--";
    }
    return Number(value).toFixed(digits == null ? 3 : digits);
  }

  async function pollNext() {
    if (state.pollInFlight || !state.payload) {
      schedulePolling();
      return;
    }

    const config = currentConfig();
    state.pollInFlight = true;
    try {
      const params = new URLSearchParams({
        mode: config.mode,
        afterId: String(state.payload.lastId || 0),
        window: String(config.window),
        fast: String(config.fast),
        slow: String(config.slow),
        fastOrder: String(config.fastOrder),
        slowOrder: String(config.slowOrder),
        series: config.series,
        limit: String(Math.min(160, Math.max(25, Math.floor(config.fast / 2)))),
        persist: "true",
      });
      const response = await fetch("/api/regression/next?" + params.toString());
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload.detail || "Regression update failed.");
      }

      if (payload.advanced) {
        state.payload = payload;
        renderAll();
        status("Advanced by " + payload.newRowCount + " row(s).", false);
      } else {
        status(config.mode === "live" ? "Watching for new ticks..." : "Review window is caught up.", false);
      }
    } catch (error) {
      status(error.message || "Regression polling failed.", true);
    } finally {
      state.pollInFlight = false;
      schedulePolling();
    }
  }

  async function loadData() {
    stopPolling();
    const config = currentConfig();
    if (config.mode === "review" && !config.id) {
      status("Review mode requires a starting id.", true);
      return;
    }

    writeQuery(config);
    status("Loading regression view...", false);

    try {
      const params = new URLSearchParams({
        mode: config.mode,
        window: String(config.window),
        fast: String(config.fast),
        slow: String(config.slow),
        fastOrder: String(config.fastOrder),
        slowOrder: String(config.slowOrder),
        series: config.series,
        persist: "true",
      });
      if (config.mode === "review") {
        params.set("id", config.id);
      }

      const response = await fetch("/api/regression/bootstrap?" + params.toString());
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload.detail || "Regression bootstrap failed.");
      }

      state.payload = payload;
      renderAll();
      status("Loaded " + payload.rowCount + " row(s).", false);
      if (state.currentRun === "run") {
        schedulePolling();
      }
    } catch (error) {
      status(error.message || "Regression bootstrap failed.", true);
    }
  }

  async function tuneRecent() {
    if (state.tuningInFlight) {
      return;
    }

    state.tuningInFlight = true;
    elements.tuneButton.disabled = true;
    status("Evaluating recent parameter grid...", false);

    try {
      const config = currentConfig();
      const params = new URLSearchParams({
        series: config.series,
        lookbackHours: "72",
        maxRows: "12000",
        targetMove: "0.8",
        adverseMove: "0.6",
        horizonTicks: "120",
        minSignals: "5",
      });
      const response = await fetch("/api/regression/polynomial/tune?" + params.toString());
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload.detail || "Adaptive tuning failed.");
      }
      state.tuningPayload = payload;
      updateAdaptiveMetrics();
      updateNarrative(state.payload);
      elements.useTunedButton.disabled = !(payload.stableConfig || payload.bestConfig);
      status(payload.summary || "Adaptive tuning finished.", false);
    } catch (error) {
      elements.useTunedButton.disabled = !(state.tuningPayload && (state.tuningPayload.stableConfig || state.tuningPayload.bestConfig));
      status(error.message || "Adaptive tuning failed.", true);
    } finally {
      state.tuningInFlight = false;
      elements.tuneButton.disabled = false;
    }
  }

  function buildMeta(payload) {
    const rows = payload.rows || [];
    if (!rows.length) {
      elements.regressionMeta.textContent = "No rows returned.";
      return;
    }
    const lastRow = rows[rows.length - 1];
    const price = selectSeriesValue(lastRow, payload.series);
    const persistence = payload.persistence || {};
    const persistenceState = persistence.requested ? (persistence.stored ? "persisted" : "volatile") : "not stored";
    const signalState = persistence.signalStored ? "signal " + persistence.signalId : "no signal";
    elements.regressionMeta.textContent = [
      "Rows " + rows.length,
      "Ids " + rows[0].id + "->" + rows[rows.length - 1].id,
      SERIES_CONFIG[payload.series].label + " " + formatPrice(price),
      "poly " + payload.fastPolyOrder + "/" + payload.slowPolyOrder,
      formatTime(lastRow.timestampMs),
      persistenceState + " | " + signalState,
    ].join(" | ");
  }

  function updateLinearMetrics(payload) {
    const fast = payload.regressions.fast;
    const slow = payload.regressions.slow;
    const relationship = payload.relationship;
    const breakPressure = payload.breakPressure;

    elements.metricFastSlope.textContent = formatSigned(fast.slope, 5);
    elements.metricFastDetail.textContent = fast.tickCount + " ticks | " + formatSigned(fast.priceChange, 2) + " move | eff " + formatPercent(fast.efficiency || 0);

    elements.metricSlowSlope.textContent = formatSigned(slow.slope, 5);
    elements.metricSlowDetail.textContent = slow.tickCount + " ticks | " + formatSigned(slow.priceChange, 2) + " move | eff " + formatPercent(slow.efficiency || 0);

    elements.metricAlignment.textContent = relationship.alignmentState;
    elements.metricAlignmentDetail.textContent = [
      "distance " + formatSigned(relationship.currentFastSlowDistance, 3),
      "angle " + formatSigned(relationship.angleDifferenceDeg, 1) + " deg",
      relationship.fastAccelerating ? "fast pressing" : "pace matched",
    ].join(" | ");

    elements.metricFastFit.textContent = fast.r2 == null ? "--" : "R2 " + formatMetricValue(fast.r2, 3);
    elements.metricFastFitDetail.textContent = "MAE " + formatMetricValue(fast.mae, 4) + " | sigma " + formatMetricValue(fast.residualStd, 4);

    elements.metricSlowFit.textContent = slow.r2 == null ? "--" : "R2 " + formatMetricValue(slow.r2, 3);
    elements.metricSlowFitDetail.textContent = "MAE " + formatMetricValue(slow.mae, 4) + " | sigma " + formatMetricValue(slow.residualStd, 4);

    elements.metricBreakPressure.textContent = formatMetricValue(breakPressure.breakPressureScore, 1);
    elements.metricBreakPressureDetail.textContent = [
      breakPressure.pressureState,
      "imbalance " + formatSigned(breakPressure.recentResidualSignImbalance || 0, 2),
      "confidence " + breakPressure.confidenceState,
    ].join(" | ");

    elements.metricSplit.textContent = breakPressure.bestCandidateSplitTickId || "none";
    elements.metricSplitDetail.textContent = [
      "improvement " + formatPercent(breakPressure.bestTwoLineImprovementPct || 0),
      "probe " + (breakPressure.splitProbeWindowTicks || 0) + " ticks",
    ].join(" | ");
  }

  function updatePolynomialMetrics(payload) {
    const fast = payload.polynomials.fast;
    const slow = payload.polynomials.slow;
    const structure = payload.polyRelationship;
    const quality = payload.moveQuality;

    elements.metricFastPoly.textContent = formatSigned(fast.slope, 4);
    elements.metricFastPolyDetail.textContent = [
      "order " + fast.order,
      "fit " + formatPrice(fast.currentFittedValue),
      "curve " + formatSigned(fast.curvature, 5),
      "dist " + formatSigned(fast.normalizedDistance, 2),
    ].join(" | ");

    elements.metricSlowPoly.textContent = formatSigned(slow.slope, 4);
    elements.metricSlowPolyDetail.textContent = [
      "order " + slow.order,
      "fit " + formatPrice(slow.currentFittedValue),
      "curve " + formatSigned(slow.curvature, 5),
      "dist " + formatSigned(slow.normalizedDistance, 2),
    ].join(" | ");

    elements.metricPolyStructure.textContent = structure.slopeAgreement;
    elements.metricPolyStructureDetail.textContent = [
      structure.curvatureAgreement,
      "fit " + formatSigned(structure.fittedSpread, 3),
      "residuals " + structure.residualRegime,
    ].join(" | ");

    elements.metricMoveQuality.textContent = formatMetricValue(quality.score, 1);
    elements.metricMoveQualityDetail.textContent = [
      quality.state,
      quality.direction,
      quality.candidate ? "candidate" : "watch",
    ].join(" | ");
  }

  function updateAdaptiveMetrics() {
    if (!state.tuningPayload || !state.tuningPayload.bestConfig) {
      elements.metricAdaptive.textContent = "--";
      elements.metricAdaptiveDetail.textContent = state.tuningInFlight
        ? "Evaluating recent configs..."
        : "Run tuning to rank recent configs.";
      elements.useTunedButton.disabled = true;
      return;
    }

    const best = state.tuningPayload.bestConfig;
    elements.metricAdaptive.textContent = formatMetricValue(best.rankScore, 1);
    elements.metricAdaptiveDetail.textContent = [
      "fast " + best.fastWindowTicks + "/" + best.fastPolyOrder,
      "slow " + best.slowWindowTicks + "/" + best.slowPolyOrder,
      best.signalCount + " signals @ " + formatPercent(best.successRate),
    ].join(" | ");
    elements.useTunedButton.disabled = false;
  }

  function applyTunedConfig() {
    if (!state.tuningPayload) {
      return;
    }
    const best = state.tuningPayload.stableConfig || state.tuningPayload.bestConfig;
    if (!best) {
      return;
    }
    elements.fastWindowSize.value = String(best.fastWindowTicks);
    elements.slowWindowSize.value = String(best.slowWindowTicks);
    elements.fastOrderSize.value = String(best.fastPolyOrder);
    elements.slowOrderSize.value = String(best.slowPolyOrder);
    const currentWindow = clampInt(elements.windowSize.value, 1, 10000, DEFAULTS.window);
    elements.windowSize.value = String(Math.max(currentWindow, best.slowWindowTicks));
    loadData();
  }

  function updateNarrative(payload) {
    if (!payload || !payload.rows || !payload.rows.length) {
      elements.regressionNarrative.textContent = "Waiting for the first regression snapshot.";
      return;
    }

    const quality = payload.moveQuality;
    const structure = payload.polyRelationship;
    const tuningSummary = state.tuningPayload && state.tuningPayload.summary ? " " + state.tuningPayload.summary : "";

    elements.regressionNarrative.textContent = [
      quality.summary,
      "Fast/slow poly spread " + formatSigned(structure.fittedSpread, 3) + " with slope spread " + formatSigned(structure.slopeSpread, 5) + ".",
      "Break pressure is " + formatMetricValue(payload.breakPressure.breakPressureScore, 1) + " (" + payload.breakPressure.pressureState + ").",
      tuningSummary,
    ].join(" ");
  }

  function lineData(rows, values, useTimeAxis) {
    const axisValues = useTimeAxis ? rows.map((row) => row.timestampMs) : rows.map((_, index) => index);
    return axisValues.map((axisValue, index) => [axisValue, values[index] == null ? null : values[index]]);
  }

  function buildTooltipFormatter(payload) {
    return function formatter(params) {
      if (!params.length || !payload.rows || !payload.rows.length) {
        return "";
      }
      const primary = params.find((item) => typeof item.dataIndex === "number") || params[0];
      const index = primary.dataIndex;
      const row = payload.rows[index];
      if (!row) {
        return "";
      }

      const price = selectSeriesValue(row, payload.series);
      const fastLinear = payload.regressions.fast.fittedValues[index];
      const slowLinear = payload.regressions.slow.fittedValues[index];
      const fastPoly = payload.polynomials.fast.fittedValues[index];
      const slowPoly = payload.polynomials.slow.fittedValues[index];
      const lines = [
        "Tick " + row.id,
        formatTime(row.timestampMs),
        SERIES_CONFIG[payload.series].label + ": " + formatPrice(price),
        "Fast linear: " + formatPrice(fastLinear) + " | resid " + formatSigned(price - fastLinear, 4),
        "Slow linear: " + formatPrice(slowLinear) + " | resid " + formatSigned(price - slowLinear, 4),
      ];

      if (fastPoly != null) {
        lines.push("Fast poly: " + formatPrice(fastPoly) + " | dist " + formatSigned(price - fastPoly, 4));
      }
      if (slowPoly != null) {
        lines.push("Slow poly: " + formatPrice(slowPoly) + " | dist " + formatSigned(price - slowPoly, 4));
      }
      lines.push("Move quality: " + formatMetricValue(payload.moveQuality.score, 1) + " (" + payload.moveQuality.state + ")");
      lines.push("Poly structure: " + payload.polyRelationship.slopeAgreement + " | " + payload.polyRelationship.curvatureAgreement);
      return lines.join("<br>");
    };
  }

  function buildCommonChartOptions(payload, minValue, maxValue) {
    return {
      animation: false,
      backgroundColor: "transparent",
      grid: { left: 56, right: 24, top: 18, bottom: 80 },
      legend: {
        top: 8,
        right: 14,
        textStyle: { color: "#dfe8f5" },
      },
      tooltip: {
        trigger: "axis",
        backgroundColor: "rgba(6, 11, 20, 0.96)",
        borderColor: "rgba(109, 216, 255, 0.24)",
        textStyle: { color: "#f3f6fb" },
        axisPointer: { type: "cross", lineStyle: { color: "rgba(109, 216, 255, 0.28)" } },
        formatter: buildTooltipFormatter(payload),
      },
      yAxis: {
        type: "value",
        scale: true,
        min: minValue,
        max: maxValue,
        axisLabel: { color: "#9eadc5", formatter: (value) => Number(value).toFixed(2) },
        axisLine: { lineStyle: { color: "rgba(147, 181, 255, 0.24)" } },
        splitLine: { lineStyle: { color: "rgba(147, 181, 255, 0.08)" } },
      },
      dataZoom: [
        { type: "inside", filterMode: "none" },
        {
          type: "slider",
          height: 42,
          bottom: 16,
          filterMode: "none",
          borderColor: "rgba(147, 181, 255, 0.16)",
          backgroundColor: "rgba(8, 13, 23, 0.94)",
          fillerColor: "rgba(109, 216, 255, 0.16)",
          dataBackground: {
            lineStyle: { color: "rgba(109, 216, 255, 0.42)" },
            areaStyle: { color: "rgba(109, 216, 255, 0.08)" },
          },
        },
      ],
    };
  }

  function buildSeries(payload, useTimeAxis) {
    const rows = payload.rows;
    const seriesConfig = SERIES_CONFIG[payload.series];
    const xValues = useTimeAxis ? rows.map((row) => row.timestampMs) : rows.map((_, index) => index);
    const priceData = xValues.map((axisValue, index) => [axisValue, selectSeriesValue(rows[index], payload.series)]);
    const splitIndex = rows.findIndex((row) => row.id === payload.breakPressure.bestCandidateSplitTickId);
    const markLine = splitIndex >= 0 ? {
      silent: true,
      symbol: ["none", "none"],
      label: { formatter: "split " + payload.breakPressure.bestCandidateSplitTickId, color: "#ffc857" },
      lineStyle: { color: "#ffc857", type: "dashed", width: 1.3 },
      data: [{ xAxis: useTimeAxis ? rows[splitIndex].timestampMs : splitIndex }],
    } : undefined;

    const series = [
      useTimeAxis ? {
        name: seriesConfig.label,
        type: "line",
        showSymbol: false,
        lineStyle: { width: 2.0, color: seriesConfig.color },
        areaStyle: {
          color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
            { offset: 0, color: seriesConfig.accent },
            { offset: 1, color: "rgba(109, 216, 255, 0.02)" },
          ]),
        },
        data: priceData,
      } : {
        name: seriesConfig.label + " ticks",
        type: "scatter",
        symbolSize: 7,
        itemStyle: { color: seriesConfig.color, opacity: 0.7 },
        data: priceData,
      },
      {
        name: LINEAR_STYLE.fast.label,
        type: "line",
        showSymbol: false,
        lineStyle: { width: LINEAR_STYLE.fast.width, color: LINEAR_STYLE.fast.color, type: LINEAR_STYLE.fast.type },
        data: lineData(rows, payload.regressions.fast.fittedValues || [], useTimeAxis),
      },
      {
        name: LINEAR_STYLE.slow.label,
        type: "line",
        showSymbol: false,
        lineStyle: { width: LINEAR_STYLE.slow.width, color: LINEAR_STYLE.slow.color, type: LINEAR_STYLE.slow.type },
        markLine,
        data: lineData(rows, payload.regressions.slow.fittedValues || [], useTimeAxis),
      },
    ];

    if (state.overlayState.fastPoly) {
      series.push({
        name: POLY_STYLE.fast.label,
        type: "line",
        showSymbol: false,
        connectNulls: false,
        lineStyle: { width: POLY_STYLE.fast.width, color: POLY_STYLE.fast.color },
        data: lineData(rows, payload.polynomials.fast.fittedValues || [], useTimeAxis),
      });
    }

    if (state.overlayState.slowPoly) {
      series.push({
        name: POLY_STYLE.slow.label,
        type: "line",
        showSymbol: false,
        connectNulls: false,
        lineStyle: { width: POLY_STYLE.slow.width, color: POLY_STYLE.slow.color },
        data: lineData(rows, payload.polynomials.slow.fittedValues || [], useTimeAxis),
      });
    }

    return series;
  }

  function renderChart(payload) {
    const chart = ensureChart();
    if (!payload || !payload.rows || !payload.rows.length) {
      chart.clear();
      return;
    }

    const rows = payload.rows;
    const values = [];
    rows.forEach((row, index) => {
      values.push(selectSeriesValue(row, payload.series));
      [payload.regressions.fast.fittedValues[index], payload.regressions.slow.fittedValues[index], payload.polynomials.fast.fittedValues[index], payload.polynomials.slow.fittedValues[index]]
        .forEach((value) => {
          if (value != null && Number.isFinite(Number(value))) {
            values.push(Number(value));
          }
        });
    });

    let minValue = Math.min.apply(null, values);
    let maxValue = Math.max.apply(null, values);
    if (!Number.isFinite(minValue) || !Number.isFinite(maxValue)) {
      minValue = 0;
      maxValue = 1;
    }
    const padding = Math.max(0.08, (maxValue - minValue) * 0.12);

    const useTimeAxis = state.currentView !== "scatter";
    const options = buildCommonChartOptions(payload, minValue - padding, maxValue + padding);
    options.xAxis = useTimeAxis ? {
      type: "time",
      axisLine: { lineStyle: { color: "rgba(147, 181, 255, 0.24)" } },
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
    } : {
      type: "value",
      name: "Tick index",
      nameTextStyle: { color: "#9eadc5" },
      axisLabel: { color: "#9eadc5" },
      axisLine: { lineStyle: { color: "rgba(147, 181, 255, 0.24)" } },
      splitLine: { lineStyle: { color: "rgba(147, 181, 255, 0.08)" } },
    };
    options.series = buildSeries(payload, useTimeAxis);
    chart.setOption(options, true);
  }

  function renderAll() {
    if (!state.payload || !state.payload.rows || !state.payload.rows.length) {
      buildMeta({ rows: [] });
      updateAdaptiveMetrics();
      renderChart(null);
      return;
    }
    buildMeta(state.payload);
    updateLinearMetrics(state.payload);
    updatePolynomialMetrics(state.payload);
    updateAdaptiveMetrics();
    updateNarrative(state.payload);
    renderChart(state.payload);
  }

  function bindSegment(container, key, handler) {
    container.querySelectorAll("button").forEach((button) => {
      button.addEventListener("click", () => handler(button.dataset[key] || button.dataset.value));
    });
  }

  bindSegment(elements.modeToggle, "value", (value) => {
    state.currentMode = value;
    setSegment(elements.modeToggle, value, "value");
    writeQuery(currentConfig());
  });

  bindSegment(elements.runToggle, "value", (value) => {
    state.currentRun = value;
    setSegment(elements.runToggle, value, "value");
    writeQuery(currentConfig());
    if (value === "stop") {
      stopPolling();
      status("Updates paused.", false);
    } else if (state.payload) {
      schedulePolling();
    }
  });

  bindSegment(elements.seriesSelector, "series", (value) => {
    state.currentSeries = value;
    setSegment(elements.seriesSelector, value, "series");
    writeQuery(currentConfig());
    if (state.payload) {
      loadData();
    }
  });

  bindSegment(elements.viewSelector, "view", (value) => {
    state.currentView = value;
    setSegment(elements.viewSelector, value, "view");
    writeQuery(currentConfig());
    renderChart(state.payload);
  });

  elements.polyOverlaySelector.querySelectorAll("button").forEach((button) => {
    button.addEventListener("click", () => {
      const overlay = button.dataset.overlay;
      state.overlayState[overlay] = !state.overlayState[overlay];
      syncOverlayButtons();
      writeQuery(currentConfig());
      renderChart(state.payload);
    });
  });

  elements.applyButton.addEventListener("click", loadData);
  elements.tuneButton.addEventListener("click", tuneRecent);
  elements.useTunedButton.addEventListener("click", applyTunedConfig);

  const initial = parseQuery();
  syncControls(initial);
  elements.useTunedButton.disabled = true;
  loadData();
}());
