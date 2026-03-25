(function () {
  const DEFAULTS = {
    mode: "live",
    run: "run",
    id: "",
    window: 2000,
    fast: 240,
    slow: 1200,
    series: "mid",
    view: "time",
  };

  const SERIES_CONFIG = {
    ask: { label: "Ask", field: "ask", color: "#ffb35c", accent: "rgba(255, 179, 92, 0.16)" },
    bid: { label: "Bid", field: "bid", color: "#7ef0c7", accent: "rgba(126, 240, 199, 0.16)" },
    mid: { label: "Mid", field: "mid", color: "#6dd8ff", accent: "rgba(109, 216, 255, 0.16)" },
  };

  const REGRESSION_STYLE = {
    fast: { label: "Fast regression", color: "#ff8f5a", width: 2.2 },
    slow: { label: "Slow regression", color: "#8bf0cf", width: 2.6 },
  };

  const state = {
    chart: null,
    payload: null,
    currentMode: DEFAULTS.mode,
    currentRun: DEFAULTS.run,
    currentSeries: DEFAULTS.series,
    currentView: DEFAULTS.view,
    pollTimer: null,
    pollInFlight: false,
  };

  const elements = {
    modeToggle: document.getElementById("modeToggle"),
    runToggle: document.getElementById("runToggle"),
    tickId: document.getElementById("tickId"),
    windowSize: document.getElementById("windowSize"),
    fastWindowSize: document.getElementById("fastWindowSize"),
    slowWindowSize: document.getElementById("slowWindowSize"),
    seriesSelector: document.getElementById("seriesSelector"),
    viewSelector: document.getElementById("viewSelector"),
    applyButton: document.getElementById("applyButton"),
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
  };

  function parseQuery() {
    const params = new URLSearchParams(window.location.search);
    const mode = params.get("mode") === "review" ? "review" : DEFAULTS.mode;
    const run = params.get("run") === "stop" ? "stop" : DEFAULTS.run;
    const windowSize = Number.parseInt(params.get("window"), 10);
    const fast = Number.parseInt(params.get("fast"), 10);
    const slow = Number.parseInt(params.get("slow"), 10);
    const series = SERIES_CONFIG[params.get("series")] ? params.get("series") : DEFAULTS.series;
    const view = params.get("view") === "scatter" ? "scatter" : DEFAULTS.view;
    return {
      mode,
      run,
      id: params.get("id") || DEFAULTS.id,
      window: Number.isFinite(windowSize) && windowSize > 0 ? windowSize : DEFAULTS.window,
      fast: Number.isFinite(fast) && fast >= 20 ? fast : DEFAULTS.fast,
      slow: Number.isFinite(slow) && slow >= 20 ? slow : DEFAULTS.slow,
      series,
      view,
    };
  }

  function setSegment(container, value, attributeName) {
    container.querySelectorAll("button").forEach((button) => {
      button.classList.toggle("active", button.dataset[attributeName] === value || button.dataset.value === value);
    });
  }

  function syncControls(config) {
    state.currentMode = config.mode;
    state.currentRun = config.run;
    state.currentSeries = config.series;
    state.currentView = config.view;
    elements.tickId.value = config.id || "";
    elements.windowSize.value = String(config.window);
    elements.fastWindowSize.value = String(config.fast);
    elements.slowWindowSize.value = String(config.slow);
    setSegment(elements.modeToggle, config.mode, "value");
    setSegment(elements.runToggle, config.run, "value");
    setSegment(elements.seriesSelector, config.series, "series");
    setSegment(elements.viewSelector, config.view, "view");
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
      series: state.currentSeries,
      view: state.currentView,
    };
  }

  function clampInt(rawValue, minimum, maximum, fallback) {
    const parsed = Number.parseInt(rawValue, 10);
    if (!Number.isFinite(parsed)) {
      return fallback;
    }
    return Math.max(minimum, Math.min(maximum, parsed));
  }

  function writeQuery(config) {
    const params = new URLSearchParams();
    params.set("mode", config.mode);
    params.set("run", config.run);
    params.set("window", String(config.window));
    params.set("fast", String(config.fast));
    params.set("slow", String(config.slow));
    params.set("series", config.series);
    params.set("view", config.view);
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

  async function pollNext() {
    if (state.pollInFlight || !state.payload) {
      schedulePolling();
      return;
    }

    const config = currentConfig();
    if (config.mode === "review" && !state.payload.lastId) {
      return;
    }

    state.pollInFlight = true;
    try {
      const params = new URLSearchParams({
        mode: config.mode,
        afterId: String(state.payload.lastId || 0),
        window: String(config.window),
        fast: String(config.fast),
        slow: String(config.slow),
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
    elements.regressionMeta.textContent = [
      "Rows " + rows.length,
      "Ids " + rows[0].id + "→" + rows[rows.length - 1].id,
      SERIES_CONFIG[payload.series].label + " " + formatPrice(price),
      formatTime(lastRow.timestampMs),
      persistenceState,
    ].join(" | ");
  }

  function selectSeriesValue(row, series) {
    return Number(row[SERIES_CONFIG[series].field]);
  }

  function formatPrice(value) {
    return Number(value).toFixed(2);
  }

  function formatSigned(value, digits) {
    const amount = Number(value);
    const fixed = amount.toFixed(digits == null ? 4 : digits);
    return amount > 0 ? "+" + fixed : fixed;
  }

  function formatPercent(value) {
    return Number(value * 100).toFixed(1) + "%";
  }

  function formatTime(timestampMs) {
    return new Date(timestampMs).toLocaleString("en-AU", { hour12: false });
  }

  function updateMetrics(payload) {
    if (!payload || !payload.rows || !payload.rows.length) {
      [
        elements.metricFastSlope,
        elements.metricSlowSlope,
        elements.metricAlignment,
        elements.metricFastFit,
        elements.metricSlowFit,
        elements.metricBreakPressure,
        elements.metricSplit,
      ].forEach((node) => { node.textContent = "--"; });
      return;
    }

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
      "angle " + formatSigned(relationship.angleDifferenceDeg, 1) + "°",
      relationship.fastAccelerating ? "fast pressing" : "pace matched",
    ].join(" | ");

    elements.metricFastFit.textContent = fast.r2 == null ? "--" : "R² " + Number(fast.r2).toFixed(3);
    elements.metricFastFitDetail.textContent = "MAE " + Number(fast.mae || 0).toFixed(4) + " | σ " + Number(fast.residualStd || 0).toFixed(4);

    elements.metricSlowFit.textContent = slow.r2 == null ? "--" : "R² " + Number(slow.r2).toFixed(3);
    elements.metricSlowFitDetail.textContent = "MAE " + Number(slow.mae || 0).toFixed(4) + " | σ " + Number(slow.residualStd || 0).toFixed(4);

    elements.metricBreakPressure.textContent = Number(breakPressure.breakPressureScore || 0).toFixed(1);
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

  function updateNarrative(payload) {
    if (!payload || !payload.rows || !payload.rows.length) {
      elements.regressionNarrative.textContent = "Waiting for the first regression snapshot.";
      return;
    }

    const relationship = payload.relationship;
    const breakPressure = payload.breakPressure;
    const fast = payload.regressions.fast;
    const slow = payload.regressions.slow;

    const directionText = relationship.alignmentState === "aligned"
      ? "Fast and slow are pointing the same way."
      : relationship.alignmentState === "opposed"
        ? "Local drift is fighting the broader fit."
        : "One of the fits is flattening while the other still carries direction.";

    const pressureText = breakPressure.bestCandidateSplitTickId
      ? "The split probe is watching tick " + breakPressure.bestCandidateSplitTickId + " with " + formatPercent(breakPressure.bestTwoLineImprovementPct || 0) + " improvement."
      : "The split probe does not yet beat the single-line fit in a meaningful way.";

    elements.regressionNarrative.textContent = [
      directionText,
      "Fast slope " + formatSigned(fast.slope, 5) + " versus slow " + formatSigned(slow.slope, 5) + ".",
      "Break pressure is " + Number(breakPressure.breakPressureScore || 0).toFixed(1) + " (" + breakPressure.pressureState + ").",
      pressureText,
    ].join(" ");
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
      const fastFit = payload.regressions.fast.fittedValues[index];
      const slowFit = payload.regressions.slow.fittedValues[index];
      const lines = [
        "Tick " + row.id,
        formatTime(row.timestampMs),
        SERIES_CONFIG[payload.series].label + ": " + formatPrice(price),
        "Fast fit: " + formatPrice(fastFit) + " | resid " + formatSigned(price - fastFit, 4),
        "Slow fit: " + formatPrice(slowFit) + " | resid " + formatSigned(price - slowFit, 4),
        "Fast-Slow: " + formatSigned(fastFit - slowFit, 4),
        "Break: " + Number(payload.breakPressure.breakPressureScore || 0).toFixed(1) + " (" + payload.breakPressure.pressureState + ")",
      ];
      if (payload.breakPressure.bestCandidateSplitTickId) {
        lines.push("Split candidate: " + payload.breakPressure.bestCandidateSplitTickId);
      }
      return lines.join("<br>");
    };
  }

  function renderChart(payload) {
    const chart = ensureChart();
    if (!payload || !payload.rows || !payload.rows.length) {
      chart.clear();
      return;
    }

    const rows = payload.rows;
    const seriesKey = payload.series;
    const seriesConfig = SERIES_CONFIG[seriesKey];
    const prices = rows.map((row) => selectSeriesValue(row, seriesKey));
    const fastFit = payload.regressions.fast.fittedValues || [];
    const slowFit = payload.regressions.slow.fittedValues || [];
    const splitIndex = rows.findIndex((row) => row.id === payload.breakPressure.bestCandidateSplitTickId);
    const xValues = rows.map((_, index) => index);

    let minValue = Number.POSITIVE_INFINITY;
    let maxValue = Number.NEGATIVE_INFINITY;
    prices.concat(fastFit, slowFit).forEach((value) => {
      if (typeof value === "number" && Number.isFinite(value)) {
        minValue = Math.min(minValue, value);
        maxValue = Math.max(maxValue, value);
      }
    });
    if (!Number.isFinite(minValue) || !Number.isFinite(maxValue)) {
      minValue = 0;
      maxValue = 1;
    }
    const padding = Math.max(0.08, (maxValue - minValue) * 0.12);

    const common = {
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
        min: minValue - padding,
        max: maxValue + padding,
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

    if (state.currentView === "scatter") {
      chart.setOption({
        ...common,
        xAxis: {
          type: "value",
          name: "Tick index",
          nameTextStyle: { color: "#9eadc5" },
          axisLabel: { color: "#9eadc5" },
          axisLine: { lineStyle: { color: "rgba(147, 181, 255, 0.24)" } },
          splitLine: { lineStyle: { color: "rgba(147, 181, 255, 0.08)" } },
        },
        series: [
          {
            name: seriesConfig.label + " ticks",
            type: "scatter",
            symbolSize: 7,
            itemStyle: { color: seriesConfig.color, opacity: 0.68 },
            data: xValues.map((index) => [index, prices[index]]),
          },
          {
            name: REGRESSION_STYLE.fast.label,
            type: "line",
            showSymbol: false,
            lineStyle: { width: REGRESSION_STYLE.fast.width, color: REGRESSION_STYLE.fast.color },
            data: xValues.map((index) => [index, fastFit[index]]),
          },
          {
            name: REGRESSION_STYLE.slow.label,
            type: "line",
            showSymbol: false,
            lineStyle: { width: REGRESSION_STYLE.slow.width, color: REGRESSION_STYLE.slow.color },
            markLine: splitIndex >= 0 ? {
              silent: true,
              symbol: ["none", "none"],
              label: { formatter: "split " + payload.breakPressure.bestCandidateSplitTickId, color: "#ffc857" },
              lineStyle: { color: "#ffc857", type: "dashed", width: 1.3 },
              data: [{ xAxis: splitIndex }],
            } : undefined,
            data: xValues.map((index) => [index, slowFit[index]]),
          },
        ],
      }, true);
      return;
    }

    chart.setOption({
      ...common,
      xAxis: {
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
      },
      series: [
        {
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
          data: rows.map((row) => [row.timestampMs, selectSeriesValue(row, seriesKey)]),
        },
        {
          name: REGRESSION_STYLE.fast.label,
          type: "line",
          showSymbol: false,
          lineStyle: { width: REGRESSION_STYLE.fast.width, color: REGRESSION_STYLE.fast.color },
          data: rows.map((row, index) => [row.timestampMs, fastFit[index]]),
        },
        {
          name: REGRESSION_STYLE.slow.label,
          type: "line",
          showSymbol: false,
          lineStyle: { width: REGRESSION_STYLE.slow.width, color: REGRESSION_STYLE.slow.color },
          markLine: splitIndex >= 0 ? {
            silent: true,
            symbol: ["none", "none"],
            label: { formatter: "split " + payload.breakPressure.bestCandidateSplitTickId, color: "#ffc857" },
            lineStyle: { color: "#ffc857", type: "dashed", width: 1.3 },
            data: [{ xAxis: rows[splitIndex].timestampMs }],
          } : undefined,
          data: rows.map((row, index) => [row.timestampMs, slowFit[index]]),
        },
      ],
    }, true);
  }

  function renderAll() {
    buildMeta(state.payload);
    updateMetrics(state.payload);
    updateNarrative(state.payload);
    renderChart(state.payload);
  }

  function bindSegment(container, key, handler) {
    container.querySelectorAll("button").forEach((button) => {
      button.addEventListener("click", () => {
        handler(button.dataset[key] || button.dataset.value);
      });
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

  elements.applyButton.addEventListener("click", loadData);

  const initial = parseQuery();
  syncControls(initial);
  loadData();
}());
