// PATH: frontend/chart-core.js
// ECharts core for datavis.au – live + review window

window.ChartCore = (function () {
  let chart = null;

  const state = {
    mode: "review", // "review" | "live"
    ticks: [],
    pivHilo: [],
    pivSwings: [],
    hhll: [],
    zonesHhll: [],
    lastTickId: null,

    liveTimer: null,
    liveLimit: 2000, // NEW: default to last 2000 ticks for live

    windowChangeHandler: null, // callback(info)
    hasInit: false, // tracks whether we've set the base option (for zoom preservation)

    // NEW: group-level visibility flags (used when building series)
    visibility: {
      bid: true,
      ask: true,
      mid: true,
      kal: true,
      hipiv: true,
      lopiv: true,
      swings: true,
      zones: true,
    },
  };

  // ---------- Helpers ----------

  function ensureChart(domId) {
    if (chart) return chart;

    const dom = document.getElementById(domId);
    if (!dom) {
      console.error("ChartCore: container not found:", domId);
      return null;
    }

    chart = echarts.init(dom, null, { useDirtyRect: true });
    state.hasInit = false;

    window.addEventListener("resize", () => chart && chart.resize());

    return chart;
  }

  function formatDateTime(ts) {
    if (!ts || typeof ts !== "string") return { date: "", time: "" };

    const tParts = ts.split("T");
    if (tParts.length < 2) return { date: ts, time: "" };

    const date = tParts[0];
    let rest = tParts[1];

    const plusIdx = rest.indexOf("+");
    const minusIdx = rest.indexOf("-");
    let cut = rest.length;

    if (plusIdx > -1) cut = plusIdx;
    if (minusIdx > 0 && minusIdx < cut) cut = minusIdx;

    rest = rest.slice(0, cut);

    const dotIdx = rest.indexOf(".");
    if (dotIdx > -1) rest = rest.slice(0, dotIdx);

    return { date, time: rest };
  }

  function buildBaseOption() {
    return {
      backgroundColor: "#050912",
      animation: true,
      animationDuration: 200,
      tooltip: {
        trigger: "axis",
        axisPointer: {
          type: "cross",
          label: {
            backgroundColor: "#333",
          },
        },
        formatter: null,
      },
      grid: {
        left: 60,
        right: 20,
        top: 20,
        bottom: 60,
      },
      xAxis: {
        type: "category",
        boundaryGap: false,
        axisLine: { lineStyle: { color: "#555" } },
        axisLabel: {
          color: "#999",
          formatter: function (value) {
            const dt = formatDateTime(value);
            return dt.time || value;
          },
        },
        splitLine: {
          show: true,
          lineStyle: { color: "#111" },
        },
      },
      yAxis: {
        type: "value",
        axisLine: { lineStyle: { color: "#555" } },
        axisLabel: {
          color: "#999",
          formatter: function (val) {
            return "$" + Math.round(val).toString();
          },
        },
        splitLine: {
          show: true,
          lineStyle: { color: "#111" },
        },
      },
      dataZoom: [
        {
          // horizontal zoom (mouse wheel, drag)
          type: "inside",
          xAxisIndex: 0,
          filterMode: "none",
        },
        {
          type: "slider",
          xAxisIndex: 0,
          height: 20,
          bottom: 30,
        },
        {
          // vertical zoom (kept; user can still adjust if desired)
          type: "inside",
          yAxisIndex: 0,
          filterMode: "none",
        },
      ],
      series: [],
    };
  }

  // NEW: determine index range of visible data from dataZoom
  function computeVisibleIndexRange(xVals) {
    const n = xVals.length;
    if (!n) return { from: 0, to: -1 };

    let from = 0;
    let to = n - 1;

    if (!chart || !state.hasInit) {
      return { from, to };
    }

    try {
      const opt = chart.getOption();
      if (!opt || !opt.dataZoom || !opt.dataZoom.length) {
        return { from, to };
      }

      const dz = opt.dataZoom[0]; // primary x-axis zoom (inside/slider share same window)

      if (dz.startValue != null || dz.endValue != null) {
        if (dz.startValue != null) from = dz.startValue;
        if (dz.endValue != null) to = dz.endValue;
      } else if (dz.start != null || dz.end != null) {
        const s = dz.start != null ? dz.start : 0; // percent
        const e = dz.end != null ? dz.end : 100;
        const span = n - 1;
        from = Math.round((s / 100) * span);
        to = Math.round((e / 100) * span);
      }
    } catch (err) {
      console.warn("ChartCore: computeVisibleIndexRange error", err);
    }

    from = Math.max(0, Math.min(from, n - 1));
    to = Math.max(from, Math.min(to, n - 1));

    return { from, to };
  }

  // NEW: y-bounds based ONLY on visible ticks
  function computeYBounds(fromIndex, toIndex) {
    if (state.ticks.length === 0 || fromIndex > toIndex) {
      return null;
    }

    const values = [];
    for (let i = fromIndex; i <= toIndex && i < state.ticks.length; i++) {
      const t = state.ticks[i];
      if (!t) continue;
      if (t.mid != null) values.push(Number(t.mid));
      if (t.kal != null) values.push(Number(t.kal));
      if (t.bid != null) values.push(Number(t.bid));
      if (t.ask != null) values.push(Number(t.ask));
    }

    if (!values.length) return null;

    let min = Math.min(...values);
    let max = Math.max(...values);
    if (!isFinite(min) || !isFinite(max)) return null;

    // Integer bounds as required
    min = Math.floor(min);
    max = Math.ceil(max);

    if (min === max) {
      min -= 1;
      max += 1;
    }

    return { min, max };
  }

  function computeYAxisStep(min, max) {
    const range = Math.max(1, max - min);

    // simple "nice" integer step selection
    if (range <= 10) return 1;
    if (range <= 20) return 2;
    if (range <= 50) return 5;
    if (range <= 100) return 10;
    if (range <= 200) return 20;
    if (range <= 500) return 50;
    if (range <= 1000) return 100;

    const pow = Math.floor(Math.log10(range)) - 1;
    return Math.pow(10, pow);
  }

  // NEW: build a yAxis patch for current (or provided) xVals
  function buildYAxisPatch(xVals) {
    if (!xVals || !xVals.length) return null;

    const { from, to } = computeVisibleIndexRange(xVals);
    const bounds = computeYBounds(from, to);
    if (!bounds) return null;

    const step = computeYAxisStep(bounds.min, bounds.max);

    return {
      min: bounds.min,
      max: bounds.max,
      minInterval: 1,
      interval: step,
      axisLabel: {
        color: "#999",
        formatter: function (val) {
          return "$" + Math.round(val).toString();
        },
      },
      splitLine: {
        show: true,
        lineStyle: { color: "#111" },
      },
    };
  }

  function buildSeries() {
    const xVals = state.ticks.map((t) => t.ts);
    const vis = state.visibility;

    // include payload with id so tooltip can show it
    const midData = state.ticks.map((t) =>
      t.mid != null ? [t.ts, Number(t.mid), { id: t.id }] : [t.ts, null, { id: t.id }]
    );
    const kalData = state.ticks.map((t) =>
      t.kal != null ? [t.ts, Number(t.kal), { id: t.id }] : [t.ts, null, { id: t.id }]
    );
    const bidData = state.ticks.map((t) =>
      t.bid != null ? [t.ts, Number(t.bid), { id: t.id }] : [t.ts, null, { id: t.id }]
    );
    const askData = state.ticks.map((t) =>
      t.ask != null ? [t.ts, Number(t.ask), { id: t.id }] : [t.ts, null, { id: t.id }]
    );

    // tickId -> index map for overlays
    const idxByTickId = new Map();
    state.ticks.forEach((t, idx) => {
      if (t.id != null) idxByTickId.set(Number(t.id), idx);
    });

    // piv_hilo
    const pivHi = [];
    const pivLo = [];
    state.pivHilo.forEach((p) => {
      const i = idxByTickId.get(Number(p.tick_id));
      if (i == null) return;
      const y = p.mid != null ? Number(p.mid) : null;
      if (y == null) return;
      const point = [xVals[i], y, p];
      if (Number(p.ptype) > 0) {
        pivHi.push(point);
      } else {
        pivLo.push(point);
      }
    });

    // piv_swings
    const swings = [];
    state.pivSwings.forEach((p) => {
      const i = idxByTickId.get(Number(p.tick_id));
      if (i == null) return;
      const y = p.mid != null ? Number(p.mid) : null;
      if (y == null) return;
      swings.push([xVals[i], y, p]);
    });

    // hhll
    const hhllMap = {};
    state.hhll.forEach((p) => {
      const i = idxByTickId.get(Number(p.tick_id));
      if (i == null) return;
      const y = p.mid != null ? Number(p.mid) : null;
      if (y == null) return;
      const key = p.class_text || "HHLL";
      if (!hhllMap[key]) hhllMap[key] = [];
      hhllMap[key].push([xVals[i], y, p]);
    });

    // zones_hhll
    const zoneAreas = [];
    state.zonesHhll.forEach((z) => {
      const start = z.start_time || z.start_ts || null;
      const end = z.end_time || z.end_ts || null;
      if (!start || !end) return;

      const top = z.top_price != null ? Number(z.top_price) : null;
      const bot = z.bot_price != null ? Number(z.bot_price) : null;
      if (top == null || bot == null) return;

      const yTop = Math.max(top, bot);
      const yBot = Math.min(top, bot);

      zoneAreas.push([
        { xAxis: start, yAxis: yBot, value: z },
        { xAxis: end, yAxis: yTop },
      ]);
    });

    const series = [];

    // main price lines (PRICE group)
    if (vis.bid && bidData.some((d) => d[1] != null)) {
      series.push({
        id: "bid",
        name: "Bid",
        type: "line",
        showSymbol: false,
        data: bidData,
        lineStyle: { width: 1.0 },
        smooth: true,
        show: true,
      });
    }

    if (vis.ask && askData.some((d) => d[1] != null)) {
      series.push({
        id: "ask",
        name: "Ask",
        type: "line",
        showSymbol: false,
        data: askData,
        lineStyle: { width: 1.0 },
        smooth: true,
        show: true,
      });
    }

    if (vis.mid) {
      series.push({
        id: "mid",
        name: "Mid",
        type: "line",
        showSymbol: false,
        data: midData,
        lineStyle: { width: 1.2 },
        smooth: true,
        show: true,
      });
    }

    if (vis.kal) {
      series.push({
        id: "kal",
        name: "Kal",
        type: "line",
        showSymbol: false,
        data: kalData,
        lineStyle: { width: 1.2 },
        smooth: true,
        show: true,
      });
    }

    // Hi / Lo pivots
    if (vis.hipiv) {
      series.push({
        id: "piv_hi",
        name: "Hi Piv",
        type: "scatter",
        symbolSize: 6,
        data: pivHi,
        emphasis: { focus: "series" },
        show: true,
      });
    }
    if (vis.lopiv) {
      series.push({
        id: "piv_lo",
        name: "Lo Piv",
        type: "scatter",
        symbolSize: 6,
        data: pivLo,
        emphasis: { focus: "series" },
        show: true,
      });
    }

    // Swings & HHLL – one overlay group
    if (vis.swings) {
      series.push({
        id: "swings",
        name: "Swings",
        type: "scatter",
        symbolSize: 7,
        data: swings,
        emphasis: { focus: "series" },
        show: true,
      });

      Object.keys(hhllMap).forEach((cls) => {
        series.push({
          id: "hhll_" + cls,
          name: "HHLL " + cls,
          type: "scatter",
          symbolSize: 7,
          data: hhllMap[cls],
          emphasis: { focus: "series" },
          show: true,
        });
      });
    }

    // Zones
    if (vis.zones) {
      series.push({
        id: "zones_hhll",
        name: "Zones",
        type: "line",
        data: [],
        showSymbol: false,
        markArea: {
          silent: true,
          itemStyle: { opacity: 0.08 },
          data: zoneAreas,
        },
        show: true,
      });
    }

    return { xVals, series };
  }

  function buildTooltipFormatter(xVals, ticks) {
    // ts -> detailed info
    const infoByTs = new Map();
    ticks.forEach((t) => {
      infoByTs.set(t.ts, {
        id: t.id,
        bid: t.bid != null ? Number(t.bid) : null,
        ask: t.ask != null ? Number(t.ask) : null,
        mid: t.mid != null ? Number(t.mid) : null,
        kal: t.kal != null ? Number(t.kal) : null,
      });
    });

    return function (params) {
      if (!params || !params.length) return "";

      const first = params[0];
      const ts =
        first.axisValue ||
        (Array.isArray(first.data) ? first.data[0] : null);

      const dt = formatDateTime(ts);
      const info = infoByTs.get(ts) || {};
      const idText = info.id != null ? info.id : "";

      let html = "";

      html += `Id: ${idText}<br/>`;
      if (dt.date) html += `${dt.date}<br/>`;
      if (dt.time) html += `${dt.time}<br/>`;
      html += `* * *<br/>`;

      const extras = [];

      params.forEach((p) => {
        const seriesId = p.seriesId || p.seriesName;
        const data = p.data;
        const yVal = Array.isArray(data) ? data[1] : data;
        const yText = yVal == null ? "" : Number(yVal).toFixed(2);

        if (
          seriesId === "mid" ||
          seriesId === "kal" ||
          seriesId === "bid" ||
          seriesId === "ask"
        ) {
          html += `${p.marker} ${p.seriesName}: ${yText}<br/>`;
        } else if (seriesId === "piv_hi" || seriesId === "piv_lo") {
          const payload = Array.isArray(data) ? data[2] : null;
          if (payload) {
            extras.push(
              `${seriesId === "piv_hi" ? "High" : "Low"} piv – mid:${yText} ptype:${payload.ptype} winL:${payload.win_left} winR:${payload.win_right}`
            );
          }
        } else if (seriesId === "swings") {
          const payload = Array.isArray(data) ? data[2] : null;
          if (payload) {
            extras.push(
              `Swing – mid:${yText} ptype:${payload.ptype} swing:${payload.swing_index}`
            );
          }
        } else if (String(seriesId).startsWith("hhll_")) {
          const payload = Array.isArray(data) ? data[2] : null;
          if (payload) {
            extras.push(
              `HHLL – mid:${yText} class:${payload.class_text} ptype:${payload.ptype}`
            );
          }
        }
      });

      if (extras.length) {
        html += `* * *<br/>`;
        extras.forEach((e) => {
          html += `${e}<br/>`;
        });
      }

      return html;
    };
  }

  function notifyWindowChange() {
    if (!state.windowChangeHandler) return;

    const n = state.ticks.length;
    if (!n) {
      state.windowChangeHandler({
        count: 0,
        firstId: null,
        lastId: null,
      });
      return;
    }

    const firstId = state.ticks[0].id;
    const lastId = state.ticks[n - 1].id;

    state.windowChangeHandler({
      count: n,
      firstId,
      lastId,
    });
  }

  function render() {
    if (!chart) return;

    const { series, xVals } = buildSeries();
    const yAxisPatch = buildYAxisPatch(xVals);
    const tooltip = {
      formatter: buildTooltipFormatter(xVals, state.ticks),
    };

    if (!state.hasInit) {
      const option = buildBaseOption();
      option.xAxis.data = xVals;

      if (yAxisPatch) {
        option.yAxis.min = yAxisPatch.min;
        option.yAxis.max = yAxisPatch.max;
        option.yAxis.minInterval = yAxisPatch.minInterval;
        option.yAxis.interval = yAxisPatch.interval;
        option.yAxis.axisLabel = yAxisPatch.axisLabel;
        option.yAxis.splitLine = yAxisPatch.splitLine;
      }

      option.series = series;
      option.tooltip = Object.assign(option.tooltip || {}, tooltip);

      chart.setOption(option, true);
      state.hasInit = true;
    } else {
      chart.setOption(
        {
          xAxis: { data: xVals },
          yAxis: yAxisPatch || {},
          series,
          tooltip,
        },
        false // preserve dataZoom / view window
      );
    }

    notifyWindowChange();
  }

  function renderLive(ticks) {
    if (!chart) return;

    state.mode = "live";
    state.ticks = ticks || [];
    if (state.ticks.length) {
      state.lastTickId = state.ticks[state.ticks.length - 1].id;
    }

    // In live mode unified index currently has no pivots/zones overlays
    state.pivHilo = [];
    state.pivSwings = [];
    state.hhll = [];
    state.zonesHhll = [];

    render();
  }

  // ---------- Public API ----------

  async function loadWindow(fromId, windowSize) {
    const res = await fetch(
      `/api/review/window?from_id=${fromId}&window=${windowSize}`
    );
    if (!res.ok) throw new Error(`HTTP ${res.status}`);

    const data = await res.json();

    state.mode = "review";
    state.ticks = data.ticks || [];
    state.pivHilo = data.piv_hilo || [];
    state.pivSwings = data.piv_swings || [];
    state.hhll = data.hhll || [];
    state.zonesHhll = data.zones_hhll || [];
    state.lastTickId = state.ticks.length
      ? state.ticks[state.ticks.length - 1].id
      : null;

    render();
  }

  async function pollLiveOnce() {
    const url = `/api/live_window?limit=${state.liveLimit}`;
    const res = await fetch(url);
    if (!res.ok) {
      console.error("Live poll error", res.status);
      return;
    }

    const data = await res.json();
    renderLive(data.ticks || []);
  }

  async function loadLiveOnce(opts) {
    const limit = (opts && opts.limit) || state.liveLimit || 2000;
    const url = `/api/live_window?limit=${limit}`;
    const res = await fetch(url);
    if (!res.ok) {
      console.error("Live one-shot error", res.status);
      return;
    }

    const data = await res.json();
    state.liveLimit = limit;
    renderLive(data.ticks || []);
  }

  function startLive(opts) {
    const intervalMs = (opts && opts.intervalMs) || 2000;
    state.liveLimit = (opts && opts.limit) || state.liveLimit || 2000;

    if (state.liveTimer) {
      clearInterval(state.liveTimer);
      state.liveTimer = null;
    }

    // initial fetch
    pollLiveOnce();
    state.liveTimer = setInterval(pollLiveOnce, intervalMs);
  }

  function stopLive() {
    if (state.liveTimer) {
      clearInterval(state.liveTimer);
      state.liveTimer = null;
    }
    state.mode = "review";
  }

  // NEW: group-based visibility controller using state.visibility + re-render
  function setVisibility(group, visible) {
    if (!(group in state.visibility)) {
      console.warn("ChartCore: unknown visibility group", group);
    }
    state.visibility[group] = !!visible;
    render(); // preserves zoom (setOption(..., false))
  }

  function setWindowChangeHandler(fn) {
    state.windowChangeHandler = typeof fn === "function" ? fn : null;
    notifyWindowChange();
  }

  // NEW: recompute y-axis when user zooms/pans along x
  function handleDataZoom() {
    if (!chart || !state.ticks.length) return;

    const xVals = state.ticks.map((t) => t.ts);
    const yAxisPatch = buildYAxisPatch(xVals);
    if (!yAxisPatch) return;

    chart.setOption({ yAxis: yAxisPatch }, false);
  }

  return {
    init(domId) {
      const c = ensureChart(domId);
      if (!c) return;

      // attach zoom handler once
      c.off && c.off("dataZoom");
      c.on("dataZoom", handleDataZoom);
    },
    loadWindow,
    startLive,
    stopLive,
    setVisibility,
    setWindowChangeHandler,
    loadLiveOnce, // used by index-core for LIVE + STOP
  };
})();
