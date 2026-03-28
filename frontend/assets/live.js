(function () {
  const DEFAULTS = {
    mode: "live",
    run: "run",
    id: "",
    reviewStart: "",
    reviewSpeed: 1,
    primaryBarCollapsed: false,
    ottBarCollapsed: true,
    window: 2000,
    series: ["mid"],
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
  };
  const SYDNEY_TIMEZONE = "Australia/Sydney";
  const REVIEW_SPEEDS = [0.5, 1, 2, 3, 5];
  const REVIEW_PREFETCH_FLOOR = 250;
  const REVIEW_PREFETCH_CEILING = 1000;
  const REVIEW_PREFETCH_THRESHOLD = 80;

  const SERIES_CONFIG = {
    ask: { label: "Ask", field: "ask", color: "#ffb35c", width: 1.35 },
    bid: { label: "Bid", field: "bid", color: "#7ef0c7", width: 1.35 },
    mid: { label: "Mid", field: "mid", color: "#6dd8ff", width: 2.0 },
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
    ottRows: new Map(),
    ottTrades: [],
    ottRun: null,
    ottLastId: 0,
    ottStatusPayload: null,
    ottOverlayPayload: null,
    lastBacktestKey: null,
    review: {
      bufferRows: [],
      visibleCount: 0,
      lastBufferedId: 0,
      playbackSpeed: DEFAULTS.reviewSpeed,
      exhausted: false,
      fetchPromise: null,
      rafId: 0,
      anchorVisibleCount: 0,
      anchorTimestampMs: 0,
      anchorPerfMs: 0,
      reachedEndAnnounced: false,
      resolvedStartId: null,
      resolvedStartTimestamp: null,
    },
    ui: {
      primaryBarCollapsed: DEFAULTS.primaryBarCollapsed,
      ottBarCollapsed: DEFAULTS.ottBarCollapsed,
    },
  };

  const elements = {
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
    fullscreenButton: document.getElementById("fullscreenButton"),
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
    primaryControls: document.getElementById("primaryControls"),
    ottControls: document.getElementById("ottControls"),
    primaryBarToggle: document.getElementById("primaryBarToggle"),
    ottBarToggle: document.getElementById("ottBarToggle"),
  };

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
    return {
      mode,
      run,
      id,
      reviewStart,
      reviewSpeed: sanitizeReviewSpeed(params.get("reviewSpeed")),
      primaryBarCollapsed: parseCollapsed(params.get("primaryBar"), DEFAULTS.primaryBarCollapsed),
      ottBarCollapsed: parseCollapsed(params.get("ottBar"), DEFAULTS.ottBarCollapsed),
      window: Number.isFinite(windowSize) && windowSize > 0 ? windowSize : DEFAULTS.window,
      series: parseSeries(params.get("series")),
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

  function setToolbarCollapsed(target, collapsed) {
    target.classList.toggle("is-collapsed", collapsed);
  }

  function syncToolbarState() {
    setToolbarCollapsed(elements.primaryControls, state.ui.primaryBarCollapsed);
    setToolbarCollapsed(elements.ottControls, state.ui.ottBarCollapsed);
    elements.primaryBarToggle.textContent = state.ui.primaryBarCollapsed ? "Expand" : "Collapse";
    elements.primaryBarToggle.setAttribute("aria-expanded", String(!state.ui.primaryBarCollapsed));
    elements.ottBarToggle.textContent = state.ui.ottBarCollapsed ? "Expand" : "Collapse";
    elements.ottBarToggle.setAttribute("aria-expanded", String(!state.ui.ottBarCollapsed));
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
    state.ui.primaryBarCollapsed = Boolean(config.primaryBarCollapsed);
    state.ui.ottBarCollapsed = Boolean(config.ottBarCollapsed);
    state.activeSeries = { ask: false, bid: false, mid: false };
    config.series.forEach((seriesKey) => {
      state.activeSeries[seriesKey] = true;
    });
    elements.tickId.value = config.id || "";
    elements.reviewStart.value = config.reviewStart || "";
    elements.windowSize.value = String(config.window);
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
    setSegment(elements.modeToggle, config.mode);
    setSegment(elements.runToggle, config.run);
    setSegment(elements.ottToggle, config.ottEnabled ? "on" : "off");
    syncSeriesButtons();
    syncReviewSpeedButtons();
    syncToolbarState();
    updateReviewControlState();
  }

  function currentConfig() {
    return {
      mode: state.currentMode,
      run: state.currentRun,
      id: elements.tickId.value.trim(),
      reviewStart: elements.reviewStart.value.trim(),
      reviewSpeed: state.review.playbackSpeed,
      primaryBarCollapsed: state.ui.primaryBarCollapsed,
      ottBarCollapsed: state.ui.ottBarCollapsed,
      window: Math.max(1, Math.min(10000, Number.parseInt(elements.windowSize.value, 10) || DEFAULTS.window)),
      series: getActiveSeriesKeys(),
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
    };
  }

  function writeQuery(config) {
    const params = new URLSearchParams();
    params.set("mode", config.mode);
    params.set("run", config.run);
    params.set("window", String(config.window));
    params.set("reviewSpeed", String(config.reviewSpeed));
    params.set("primaryBar", config.primaryBarCollapsed ? "1" : "0");
    params.set("ottBar", config.ottBarCollapsed ? "1" : "0");
    params.set("series", config.series.join(","));
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

  function isOttWarningStatus(statusValue) {
    return ["empty", "partial", "ahead"].includes(statusValue);
  }

  function formatSignalCounts(signalCounts) {
    if (!signalCounts) {
      return null;
    }
    return "Signals " + Number(signalCounts.totalCount || 0) + " (" + Number(signalCounts.buyCount || 0) + " buy / " + Number(signalCounts.sellCount || 0) + " sell)";
  }

  function collectOttMessages(config) {
    const messages = [];
    const statuses = [];
    if (shouldLoadOtt(config) && state.ottStatusPayload && state.ottStatusPayload.status && state.ottStatusPayload.status !== "ok") {
      statuses.push(state.ottStatusPayload.status);
      if (state.ottStatusPayload.message) {
        messages.push(state.ottStatusPayload.message);
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
      hasWarning: statuses.some((value) => isOttWarningStatus(value)),
    };
  }

  function ensureChart() {
    if (!state.chart) {
      state.chart = echarts.init(elements.chartHost, null, { renderer: "canvas" });
      state.chart.on("datazoom", () => {
        updateVisibleZoomFromChart();
        applyVisibleYAxis();
      });
      window.addEventListener("resize", () => state.chart.resize());
      document.addEventListener("fullscreenchange", handleFullscreenChange);
    }
    return state.chart;
  }

  function handleFullscreenChange() {
    const isFullscreen = document.fullscreenElement === elements.chartPanel;
    elements.fullscreenButton.textContent = isFullscreen ? "Exit Fullscreen" : "Fullscreen";
    if (state.chart) {
      window.setTimeout(() => state.chart.resize(), 60);
    }
  }

  function readZoomWindowFromChart() {
    if (!state.chart || !state.rows.length) {
      return null;
    }
    const option = state.chart.getOption();
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

  function shouldLoadOtt(config) {
    return config.ottEnabled || config.ottSupport || config.ottMarkers || (config.mode === "review" && config.ottTrades);
  }

  function markerPrice(row, ottRow, buyField, sellField) {
    if (!ottRow) {
      return row.price;
    }
    if (ottRow[buyField] && ottRow.ott != null) {
      return ottRow.ott * 0.995;
    }
    if (ottRow[sellField] && ottRow.ott != null) {
      return ottRow.ott * 1.005;
    }
    return ottRow.ott || ottRow.ott2 || ottRow.price || row.price;
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
    });

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
        symbolSize: 13,
        itemStyle: { color: "#4ade80" },
        data: buyData,
      },
      {
        name: "OTT Sell",
        type: "scatter",
        symbol: "triangle",
        symbolRotate: 180,
        symbolSize: 13,
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
    if (!shouldLoadOtt(config)) {
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

  function buildTooltipFormatter() {
    return function formatter(params) {
      if (!params.length) {
        return "";
      }
      const date = new Date(params[0].value[0]);
      const lines = [date.toLocaleString("en-AU", { hour12: false })];
      params.forEach((item) => {
        const value = Array.isArray(item.value) ? item.value[1] : item.value;
        if (typeof value === "number") {
          lines.push(item.seriesName + ": " + Number(value).toFixed(2));
        }
      });
      return lines.join("<br>");
    };
  }

  function buildMetaText() {
    if (!state.rows.length) {
      elements.liveMeta.textContent = "No rows returned.";
      return;
    }
    const config = currentConfig();
    const primarySeries = getPrimarySeriesKey();
    const lastRow = state.rows[state.rows.length - 1];
    const price = lastRow[SERIES_CONFIG[primarySeries].field];
    const lastOtt = state.ottRows.get(lastRow.id);
    const meta = [
      "Rows " + state.rows.length,
      "Last id " + lastRow.id,
      "Price " + Number(price).toFixed(2),
    ];
    if (config.mode === "review") {
      meta.unshift("Replay " + state.review.visibleCount + "/" + state.review.bufferRows.length);
      meta.push("Speed " + state.review.playbackSpeed + "x");
    }
    if (lastOtt && lastOtt.ott2 != null && config.ottEnabled) {
      meta.push("OTT " + Number(lastOtt.ott2).toFixed(2));
    }
    if (state.ottStatusPayload && state.ottStatusPayload.latestStoredTickId != null) {
      meta.push("OTT thru " + Number(state.ottStatusPayload.latestStoredTickId));
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
    if (!state.chart || !state.rows.length) {
      return;
    }
    const zoomWindow = readZoomWindowFromChart() || state.visibleWindow;
    if (!zoomWindow) {
      return;
    }
    state.chart.setOption({ yAxis: buildYAxis(zoomWindow) }, false);
  }

  function renderChart(options) {
    const chart = ensureChart();
    const selected = getActiveSeriesKeys();
    const targetZoom = determineTargetZoom(options || {});
    const firstTs = state.rows.length ? state.rows[0].timestampMs : Date.now() - 60000;
    const lastTs = state.rows.length ? state.rows[state.rows.length - 1].timestampMs : Date.now();
    const config = currentConfig();

    state.visibleWindow = targetZoom;
    state.visibleSpanMs = Math.max(1000, targetZoom.endMs - targetZoom.startMs);

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
      series: buildPriceSeries(state.rows, selected).concat(buildOttSeries(state.rows, config)),
    }, true);

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
    state.review.lastBufferedId = 0;
    state.review.exhausted = false;
    state.review.fetchPromise = null;
    state.review.anchorVisibleCount = 0;
    state.review.anchorTimestampMs = 0;
    state.review.anchorPerfMs = 0;
    state.review.reachedEndAnnounced = false;
    state.review.resolvedStartId = null;
    state.review.resolvedStartTimestamp = null;
  }

  function setReviewVisibleCount(nextCount, options) {
    const boundedCount = Math.max(0, Math.min(nextCount, state.review.bufferRows.length));
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
    return Math.max(
      REVIEW_PREFETCH_FLOOR,
      Math.min(REVIEW_PREFETCH_CEILING, Math.max(120, Math.floor(config.window / 2)))
    );
  }

  function reviewLastVisibleRow() {
    if (!state.review.visibleCount) {
      return null;
    }
    return state.review.bufferRows[state.review.visibleCount - 1] || null;
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
    stopReviewPlayback({ silent: true });
    state.currentRun = "stop";
    setSegment(elements.runToggle, "stop");
    writeQuery(currentConfig());
    status("Reached end of review range.", false);
  }

  function clearOttState() {
    state.ottRows = new Map();
    state.ottTrades = [];
    state.ottRun = null;
    state.ottLastId = 0;
    state.ottStatusPayload = null;
    state.ottOverlayPayload = null;
  }

  function applyOttPayload(payload, reset) {
    if (reset) {
      state.ottRows = new Map();
      state.ottLastId = 0;
    }
    (payload.rows || []).forEach((row) => {
      state.ottRows.set(row.tickid, row);
    });
    state.ottLastId = payload.lastId || payload.rows?.[payload.rows.length - 1]?.tickid || state.ottLastId;
    state.ottStatusPayload = payload;
  }

  async function fetchJson(url, options) {
    const response = await fetch(url, options);
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
    }
    const payload = await fetchJson("/api/ott/bootstrap?" + params.toString());
    applyOttPayload(payload, true);
    return payload;
  }

  async function loadOttNext(afterId, config, limitOverride) {
    if (!shouldLoadOtt(config)) {
      return null;
    }
    const params = new URLSearchParams({
      afterId: String(afterId),
      limit: String(limitOverride || Math.max(50, Math.min(500, config.window))),
      source: config.ottSource,
      signalmode: config.ottSignalMode,
      matype: config.ottMaType,
      length: String(config.ottLength),
      percent: String(config.ottPercent),
    });
    const payload = await fetchJson("/api/ott/next?" + params.toString());
    applyOttPayload(payload, false);
    return payload;
  }

  async function runBacktestIfNeeded(config, force) {
    if (config.mode !== "review" || !config.ottTrades) {
      state.ottRun = null;
      state.ottTrades = [];
      state.ottOverlayPayload = null;
      return null;
    }
    const backtestKey = [
      config.ottSource,
      config.ottMaType,
      config.ottLength,
      config.ottPercent,
      config.ottSignalMode,
      config.ottRangePreset,
      force ? "force" : "reuse",
    ].join(":");
    if (!force && state.lastBacktestKey === backtestKey && state.ottRun) {
      return state.ottRun;
    }

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
    });
    state.lastBacktestKey = backtestKey;
    state.ottRun = runPayload.run;
    return runPayload.run;
  }

  async function loadBacktestOverlay(config, rangeRows) {
    const rows = rangeRows || state.rows;
    if (config.mode !== "review" || !config.ottTrades || !rows.length) {
      state.ottTrades = [];
      state.ottRun = null;
      state.ottOverlayPayload = null;
      return null;
    }
    await runBacktestIfNeeded(config, false);
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
    const overlayPayload = await fetchJson("/api/ott/backtest/overlay?" + params.toString());
    state.ottRun = overlayPayload.run;
    state.ottTrades = (overlayPayload.trades || []).map((trade) => ({
      ...trade,
      entryTsMs: trade.entryTsMs,
      exitTsMs: trade.exitTsMs,
    }));
    state.ottOverlayPayload = overlayPayload;
    return overlayPayload;
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
    if (state.review.fetchPromise || state.review.exhausted) {
      return state.review.fetchPromise;
    }
    const afterId = state.review.lastBufferedId;
    const params = new URLSearchParams({
      afterId: String(afterId),
      limit: String(reviewPrefetchLimit(config)),
    });
    state.review.fetchPromise = fetchJson("/api/live/next?" + params.toString())
      .then(async (payload) => {
        const newRows = payload.rows || [];
        if (!newRows.length) {
          state.review.exhausted = true;
          if (state.review.visibleCount >= state.review.bufferRows.length) {
            announceReviewEnd();
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

        try {
          if (shouldLoadOtt(config)) {
            await loadOttNext(afterId, config, newRows.length);
          }
          if (config.ottTrades) {
            await loadBacktestOverlay(config, state.review.bufferRows);
          }
        } catch (error) {
          status(error.message || "Review overlay update failed.", true);
        }
        return payload;
      })
      .finally(() => {
        state.review.fetchPromise = null;
      });
    return state.review.fetchPromise;
  }

  function maybePrefetchReview(config) {
    const remaining = state.review.bufferRows.length - state.review.visibleCount;
    if (remaining <= REVIEW_PREFETCH_THRESHOLD && !state.review.exhausted) {
      fetchReviewNextChunk(config).catch((error) => {
        status(error.message || "Review fetch failed.", true);
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

    const targetTimestampMs = state.review.anchorTimestampMs + ((nowMs - state.review.anchorPerfMs) * state.review.playbackSpeed);
    const nextVisibleCount = reviewVisibleCountForTimestamp(targetTimestampMs);
    if (nextVisibleCount !== state.review.visibleCount) {
      setReviewVisibleCount(nextVisibleCount, { preserveCurrentZoom: false });
      maybePrefetchReview(currentConfig());
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
      setReviewVisibleCount(Math.min(state.review.bufferRows.length, 2), { resetWindow: true });
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

      const renderAfterOtt = shouldLoadOtt(config)
        ? loadOttNext(previousLastId, config).catch((error) => {
          status(error.message || "OTT incremental update failed.", true);
          return null;
        })
        : Promise.resolve(null);

      renderAfterOtt.then(() => {
        const ottState = collectOttMessages(config);
        if (ottState.messages.length) {
          status("Streaming " + payload.rowCount + " new row(s). " + ottState.messages.join(" "), ottState.hasWarning);
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
    clearOttState();

    status("Loading chart...", false);
    try {
      const config = await resolveReviewStart(currentConfig());
      writeQuery(config);
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
        state.review.lastBufferedId = loadedRows.length ? loadedRows[loadedRows.length - 1].id : 0;
        state.rows = state.review.bufferRows.slice(0, state.review.visibleCount);
      } else {
        state.rows = loadedRows;
      }
      let ottError = null;

      if (shouldLoadOtt(config)) {
        try {
          await loadOttBootstrap(config);
        } catch (error) {
          ottError = error;
          clearOttState();
        }
      }

      if (config.mode === "review" && config.ottTrades && shouldLoadOtt(config)) {
        try {
          await loadBacktestOverlay(config, config.mode === "review" ? state.review.bufferRows : state.rows);
        } catch (error) {
          ottError = error;
          state.ottTrades = [];
          state.ottRun = null;
          state.ottOverlayPayload = null;
        }
      }

      renderChart({ resetWindow: Boolean(resetWindow) });
      const ottCount = shouldLoadOtt(config) ? state.ottRows.size : 0;
      const reviewPrefix = config.mode === "review"
        ? "Loaded review from " + (state.review.resolvedStartTimestamp || config.reviewStart || ("id " + config.id)) + ". "
        : "";
      if (ottError) {
        status(reviewPrefix + "Loaded " + livePayload.rowCount + " row(s). OTT unavailable: " + ottError.message, true);
      } else {
        const ottState = collectOttMessages(config);
        if (ottState.messages.length) {
          status(reviewPrefix + "Loaded " + livePayload.rowCount + " row(s). " + ottState.messages.join(" "), ottState.hasWarning);
        } else {
          status(reviewPrefix + "Loaded " + livePayload.rowCount + " row(s)" + (ottCount ? " with " + ottCount + " OTT row(s)." : "."), false);
        }
      }
      if (config.run === "run" && config.mode === "live") {
        connectStream(livePayload.lastId || 0, config.window);
      }
      if (config.run === "run" && config.mode === "review") {
        startReviewPlayback();
      }
    } catch (error) {
      status(error.message || "Live bootstrap failed.", true);
    }
  }

  function scheduleChartResize() {
    if (!state.chart) {
      return;
    }
    window.setTimeout(() => {
      state.chart.resize();
      applyVisibleYAxis();
    }, 90);
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

  function toggleToolbarRow(key) {
    state.ui[key] = !state.ui[key];
    syncToolbarState();
    writeQuery(currentConfig());
    scheduleChartResize();
  }

  async function toggleFullscreen() {
    if (document.fullscreenElement === elements.chartPanel) {
      await document.exitFullscreen();
      return;
    }
    if (elements.chartPanel.requestFullscreen) {
      await elements.chartPanel.requestFullscreen();
    }
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

  bindSegment(elements.ottToggle, (value) => {
    setSegment(elements.ottToggle, value);
    writeQuery(currentConfig());
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
    control.addEventListener("change", async () => {
      writeQuery(currentConfig());
      if (control === elements.ottTradesToggle || control === elements.ottSignalMode) {
        try {
          const overlayPayload = await loadBacktestOverlay(
            currentConfig(),
            state.currentMode === "review" ? state.review.bufferRows : state.rows
          );
          if (overlayPayload && overlayPayload.message) {
            status(overlayPayload.message, isOttWarningStatus(overlayPayload.status));
          }
        } catch (error) {
          status(error.message || "Failed to refresh OTT backtest overlay.", true);
        }
      }
      renderChart({ preserveCurrentZoom: true });
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

  elements.reviewYesterdayButton.addEventListener("click", () => {
    state.currentMode = "review";
    setSegment(elements.modeToggle, "review");
    updateReviewControlState();
    setYesterdaySydneyMorning();
    writeQuery(currentConfig());
  });

  elements.primaryBarToggle.addEventListener("click", () => toggleToolbarRow("primaryBarCollapsed"));
  elements.ottBarToggle.addEventListener("click", () => toggleToolbarRow("ottBarCollapsed"));

  elements.fullscreenButton.addEventListener("click", toggleFullscreen);
  elements.applyButton.addEventListener("click", () => loadData(true));
  elements.runOttBacktestButton.addEventListener("click", async () => {
    const config = currentConfig();
    writeQuery(config);
    try {
      status("Running OTT backtest...", false);
      await runBacktestIfNeeded(config, true);
      const overlayPayload = await loadBacktestOverlay(
        config,
        state.currentMode === "review" ? state.review.bufferRows : state.rows
      );
      renderChart({ preserveCurrentZoom: true });
      if (overlayPayload && overlayPayload.message) {
        status(overlayPayload.message, isOttWarningStatus(overlayPayload.status));
      } else {
        status("OTT backtest refreshed.", false);
      }
    } catch (error) {
      status(error.message || "OTT backtest failed.", true);
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
