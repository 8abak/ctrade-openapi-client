(function () {
  const DEFAULT_POINT_TARGET = 2000;
  const MIN_POINT_TARGET = 200;
  const MAX_POINT_TARGET = 2400;
  const ZOOM_OUT_FACTOR = 1.85;
  const ZOOM_IN_FACTOR = 0.55;
  const MIN_SPAN_MS = 60 * 1000;
  const MAX_SPAN_MS = 365 * 24 * 60 * 60 * 1000;
  const charting = window.DatavisCharting;
  const Y_AXIS_STYLE = {
    axisLabelColor: "#91a1b8",
    splitLineColor: "rgba(147,181,255,0.08)",
    targetTickCount: 6,
  };

  const state = {
    chart: null,
    points: [],
    rangeStartTsMs: null,
    rangeEndTsMs: null,
    sourceRowCount: 0,
    loading: false,
    acd: null,
    showAcd: false,
    ui: { sidebarCollapsed: true },
  };

  const elements = {
    workspace: document.getElementById("bigPictureWorkspace"),
    sidebarToggle: document.getElementById("sidebarToggle"),
    sidebarBackdrop: document.getElementById("sidebarBackdrop"),
    pointTarget: document.getElementById("pointTarget"),
    showAcd: document.getElementById("showAcd"),
    latestButton: document.getElementById("latestButton"),
    zoomInButton: document.getElementById("zoomInButton"),
    zoomOutButton: document.getElementById("zoomOutButton"),
    statusLine: document.getElementById("statusLine"),
    meta: document.getElementById("bigPictureMeta"),
    perf: document.getElementById("bigPicturePerf"),
    chartHost: document.getElementById("bigPictureChart"),
  };

  function sanitizePointTarget(rawValue) {
    return Math.max(MIN_POINT_TARGET, Math.min(MAX_POINT_TARGET, Number.parseInt(rawValue || String(DEFAULT_POINT_TARGET), 10) || DEFAULT_POINT_TARGET));
  }

  function status(message, isError) {
    elements.statusLine.textContent = message;
    elements.statusLine.classList.toggle("error", Boolean(isError));
  }

  function setSidebarCollapsed(collapsed) {
    state.ui.sidebarCollapsed = Boolean(collapsed);
    elements.workspace.classList.toggle("is-sidebar-collapsed", state.ui.sidebarCollapsed);
    elements.sidebarToggle.setAttribute("aria-expanded", String(!state.ui.sidebarCollapsed));
    elements.sidebarToggle.setAttribute("aria-label", state.ui.sidebarCollapsed ? "Open big picture controls" : "Close big picture controls");
    elements.sidebarBackdrop.tabIndex = state.ui.sidebarCollapsed ? -1 : 0;
    if (state.chart) {
      requestAnimationFrame(function () { state.chart.resize(); });
    }
  }

  async function fetchJson(url) {
    const response = await fetch(url);
    const payload = await response.json().catch(function () { return {}; });
    if (!response.ok) {
      throw new Error(payload.detail || "Request failed.");
    }
    return payload;
  }

  function formatDuration(ms) {
    const totalSeconds = Math.max(0, Math.round(Number(ms || 0) / 1000));
    const days = Math.floor(totalSeconds / 86400);
    const hours = Math.floor((totalSeconds % 86400) / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    if (days > 0) {
      return days + "d " + String(hours).padStart(2, "0") + "h";
    }
    if (hours > 0) {
      return hours + "h " + String(minutes).padStart(2, "0") + "m";
    }
    return Math.max(1, minutes) + "m";
  }

  function formatPrice(value) {
    const number = Number(value);
    return Number.isFinite(number) ? number.toFixed(2) : "-";
  }

  function acdSpecs() {
    if (!state.showAcd || !state.acd?.available) {
      return [];
    }
    return [
      ["C+", state.acd.levels?.cUp, "#ffc857"],
      ["A+", state.acd.levels?.aUp, "#6dd8ff"],
      ["OR+", state.acd.levels?.openingHigh, "#93a4bd"],
      ["OR−", state.acd.levels?.openingLow, "#93a4bd"],
      ["A−", state.acd.levels?.aDown, "#6dd8ff"],
      ["C−", state.acd.levels?.cDown, "#ffc857"],
    ].filter(function (spec) { return Number.isFinite(Number(spec[1])); });
  }

  function acdSeries() {
    const specs = acdSpecs();
    if (!specs.length) {
      return null;
    }
    return {
      id: "bigpicture-acd-levels",
      name: "Daily ACD",
      type: "line",
      data: state.points.length ? [
        [Number(state.rangeStartTsMs), Number(state.acd.currentMid)],
        [Number(state.rangeEndTsMs), Number(state.acd.currentMid)],
      ] : [],
      showSymbol: false,
      silent: true,
      animation: false,
      lineStyle: { opacity: 0 },
      markLine: {
        silent: true,
        symbol: ["none", "none"],
        precision: 2,
        label: {
          show: true,
          position: "insideEndTop",
          color: "#e7f5ff",
          backgroundColor: "rgba(5,9,15,0.88)",
          padding: [2, 4],
          formatter: function (params) {
            return String(params.name || "ACD") + " " + formatPrice(params.value);
          },
        },
        data: specs.map(function (spec) {
          return {
            name: spec[0],
            yAxis: Number(spec[1]),
            lineStyle: {
              color: spec[2],
              width: spec[0].startsWith("C") ? 1.5 : 1.2,
              type: spec[0].startsWith("OR") ? "dotted" : "dashed",
              opacity: 0.92,
            },
          };
        }),
      },
      z: 8,
    };
  }

  function renderMeta() {
    if (state.rangeStartTsMs == null || state.rangeEndTsMs == null) {
      elements.meta.textContent = "No big picture range loaded.";
      return;
    }
    const spanMs = Math.max(0, Number(state.rangeEndTsMs) - Number(state.rangeStartTsMs));
    elements.meta.textContent = [
      "points " + state.points.length,
      "source " + state.sourceRowCount,
      "span " + formatDuration(spanMs),
      "from " + new Date(state.rangeStartTsMs).toLocaleString(),
      "to " + new Date(state.rangeEndTsMs).toLocaleString(),
    ].join(" | ");
  }

  function renderPerf(payload) {
    const metrics = payload?.metrics || {};
    const parts = [state.loading ? "Loading" : "Ready"];
    if (metrics.fetchLatencyMs != null) {
      parts.push("Fetch " + Math.round(metrics.fetchLatencyMs * 100) / 100 + "ms");
    }
    if (metrics.serializeLatencyMs != null) {
      parts.push("Serialize " + Math.round(metrics.serializeLatencyMs * 100) / 100 + "ms");
    }
    elements.perf.textContent = parts.join(" | ");
  }

  function ensureChart() {
    if (!state.chart) {
      state.chart = echarts.init(elements.chartHost, null, { renderer: "canvas" });
      state.chart.setOption({
        animation: false,
        backgroundColor: "transparent",
        tooltip: {
          trigger: "axis",
          axisPointer: { type: "cross" },
          formatter: function (params) {
            const point = Array.isArray(params) ? params[0] : params;
            const row = point?.data?.row || {};
            return "<div class=\"chart-tip\">"
              + "<div class=\"chart-tip-title\">Big Picture Tick</div>"
              + "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Id</span><span class=\"chart-tip-value\">" + String(row.id || "-") + "</span></div>"
              + "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Time</span><span class=\"chart-tip-value\">" + (row.timestampMs ? new Date(row.timestampMs).toLocaleString() : "-") + "</span></div>"
              + "<div class=\"chart-tip-row\"><span class=\"chart-tip-label\">Mid</span><span class=\"chart-tip-value\">" + (row.mid != null ? Number(row.mid).toFixed(2) : "-") + "</span></div>"
              + "</div>";
          },
        },
        grid: { left: 56, right: 18, top: 24, bottom: 44 },
        xAxis: { type: "time", axisLabel: { color: "#91a1b8" }, axisLine: { lineStyle: { color: "rgba(147,181,255,0.16)" } } },
        yAxis: { type: "value", scale: true, axisLabel: { color: "#91a1b8" }, splitLine: { lineStyle: { color: "rgba(147,181,255,0.08)" } } },
        series: [],
      });
      window.addEventListener("resize", function () {
        if (state.chart) {
          state.chart.resize();
        }
      });
    }
  }

  function renderChart() {
    ensureChart();
    const data = state.points.map(function (row) {
      return { value: [row.timestampMs, row.mid], row: row };
    });
    const visibleRange = state.rangeStartTsMs != null && state.rangeEndTsMs != null
      ? { min: Number(state.rangeStartTsMs), max: Number(state.rangeEndTsMs) }
      : null;
    const coreItems = state.points
      .map(function (row) {
        return charting.pointItem(row.timestampMs, row.mid);
      })
      .filter(Boolean);
    let yAxis = charting.buildVisibleIntegerYAxis({
        visibleRange: visibleRange,
        coreItems: coreItems,
        overlayItems: [],
        includeOverlays: false,
        ...Y_AXIS_STYLE,
      });
    const specs = acdSpecs();
    if (specs.length) {
      const prices = specs.map(function (spec) { return Number(spec[1]); });
      const low = Math.min(Number.isFinite(Number(yAxis.min)) ? Number(yAxis.min) : Math.min(...prices), ...prices);
      const high = Math.max(Number.isFinite(Number(yAxis.max)) ? Number(yAxis.max) : Math.max(...prices), ...prices);
      yAxis = charting.buildIntegerYAxis(low, high, Y_AXIS_STYLE);
    }
    const series = [{
        id: "bigpicture-line",
        type: "line",
        showSymbol: false,
        smooth: false,
        lineStyle: { width: 1.4, color: "#e8eef8" },
        areaStyle: { color: "rgba(109,216,255,0.05)" },
        data: data,
      }];
    const acd = acdSeries();
    if (acd) {
      series.push(acd);
    }
    state.chart.setOption({
      yAxis: yAxis,
      series: series,
    }, {
      replaceMerge: ["series"],
    });
  }

  async function loadAcd() {
    try {
      state.acd = await fetchJson("/api/live/acd");
    } catch (error) {
      state.acd = { available: false, reason: error.message || "ACD unavailable." };
      if (state.showAcd) {
        status(state.acd.reason, true);
      }
    }
    if (state.points.length) {
      renderChart();
    }
  }

  function applyPayload(payload) {
    state.points = Array.isArray(payload.points) ? payload.points : [];
    state.rangeStartTsMs = payload.actualStartTsMs ?? null;
    state.rangeEndTsMs = payload.actualEndTsMs ?? null;
    state.sourceRowCount = Number(payload.sourceRowCount || state.points.length || 0);
    renderMeta();
    renderPerf(payload);
    renderChart();
  }

  function currentSpanMs() {
    if (state.rangeStartTsMs == null || state.rangeEndTsMs == null) {
      return null;
    }
    return Math.max(MIN_SPAN_MS, Number(state.rangeEndTsMs) - Number(state.rangeStartTsMs));
  }

  function buildScaledRange(factor) {
    const spanMs = currentSpanMs();
    if (spanMs == null) {
      return null;
    }
    const center = Number(state.rangeStartTsMs) + (spanMs / 2);
    const nextSpan = Math.max(MIN_SPAN_MS, Math.min(MAX_SPAN_MS, Math.round(spanMs * factor)));
    return {
      startTsMs: Math.max(1, Math.round(center - (nextSpan / 2))),
      endTsMs: Math.max(2, Math.round(center + (nextSpan / 2))),
    };
  }

  async function loadLatest() {
    state.loading = true;
    renderPerf(null);
    try {
      const payload = await fetchJson("/api/bigpicture/bootstrap?" + new URLSearchParams({
        points: String(sanitizePointTarget(elements.pointTarget.value)),
      }).toString());
      applyPayload(payload);
      status("Loaded latest big picture window.", false);
    } catch (error) {
      status(error.message || "Big picture load failed.", true);
    } finally {
      state.loading = false;
      renderPerf(null);
    }
  }

  async function loadRange(range) {
    if (!range || state.loading) {
      return;
    }
    state.loading = true;
    renderPerf(null);
    try {
      const payload = await fetchJson("/api/bigpicture/window?" + new URLSearchParams({
        startTsMs: String(range.startTsMs),
        endTsMs: String(range.endTsMs),
        points: String(sanitizePointTarget(elements.pointTarget.value)),
      }).toString());
      applyPayload(payload);
      status("Loaded wider historical range.", false);
    } catch (error) {
      status(error.message || "Big picture range load failed.", true);
    } finally {
      state.loading = false;
      renderPerf(null);
    }
  }

  function handleWheel(event) {
    if (!state.points.length) {
      return;
    }
    event.preventDefault();
    if (state.loading) {
      return;
    }
    const range = buildScaledRange(event.deltaY > 0 ? ZOOM_OUT_FACTOR : ZOOM_IN_FACTOR);
    loadRange(range);
  }

  elements.pointTarget.addEventListener("change", function () {
    elements.pointTarget.value = String(sanitizePointTarget(elements.pointTarget.value));
  });
  elements.showAcd.addEventListener("change", function () {
    state.showAcd = Boolean(elements.showAcd.checked);
    try {
      window.localStorage.setItem("datavis.bigpicture.showAcd", state.showAcd ? "1" : "0");
    } catch (error) {
      void error;
    }
    renderChart();
    status(state.showAcd ? "Daily ACD enabled." : "Daily ACD hidden.", false);
  });
  elements.latestButton.addEventListener("click", function () { loadLatest(); });
  elements.zoomInButton.addEventListener("click", function () { loadRange(buildScaledRange(ZOOM_IN_FACTOR)); });
  elements.zoomOutButton.addEventListener("click", function () { loadRange(buildScaledRange(ZOOM_OUT_FACTOR)); });
  elements.sidebarToggle.addEventListener("click", function () { setSidebarCollapsed(!state.ui.sidebarCollapsed); });
  elements.sidebarBackdrop.addEventListener("click", function () { setSidebarCollapsed(true); });
  elements.chartHost.addEventListener("wheel", handleWheel, { passive: false });
  window.addEventListener("keydown", function (event) {
    if (event.key === "Escape" && !state.ui.sidebarCollapsed) {
      setSidebarCollapsed(true);
    }
  });

  setSidebarCollapsed(true);
  try {
    state.showAcd = window.localStorage.getItem("datavis.bigpicture.showAcd") === "1";
  } catch (error) {
    void error;
  }
  elements.showAcd.checked = state.showAcd;
  loadAcd();
  loadLatest();
}());
