// PATH: frontend/chart-core.js
// ECharts core for datavis.au – live + review
// - Dark theme
// - Dynamic Y range (min-1 .. max+1) based on visible prices
// - Live mode (poll /api/live_window)
// - Review mode (load /api/review/window)
// - Overlays: piv_hilo, piv_swings, hhll_piv, zones_hhll
// - Tooltip shows enabled overlays; date & time separated; no timezone suffix
// - All numbers shown as integers (rounded)

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
    liveLimit: 5000
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
    window.addEventListener("resize", () => chart && chart.resize());
    return chart;
  }

  function formatDateTime(ts) {
    // ts like "2025-12-04T17:47:42.593000+11:00"
    if (!ts || typeof ts !== "string") return { date: "", time: "" };
    const tParts = ts.split("T");
    if (tParts.length < 2) return { date: ts, time: "" };

    const date = tParts[0];

    let rest = tParts[1];
    // strip timezone
    const plusIdx = rest.indexOf("+");
    const minusIdx = rest.indexOf("-");
    let cut = rest.length;
    if (plusIdx > -1) cut = plusIdx;
    if (minusIdx > 0 && minusIdx < cut) cut = minusIdx;
    rest = rest.slice(0, cut);

    // keep hh:mm:ss
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
            backgroundColor: "#333"
          }
        },
        // custom formatter – we’ll fill it in render()
        formatter: null
      },
      grid: {
        left: 60,
        right: 20,
        top: 20,
        bottom: 60
      },
      xAxis: {
        type: "category",
        boundaryGap: false,
        axisLine: { lineStyle: { color: "#555" } },
        axisLabel: {
          color: "#999",
          formatter: function (value) {
            // show just time hh:mm:ss on axis
            const dt = formatDateTime(value);
            return dt.time || value;
          }
        },
        splitLine: { show: true, lineStyle: { color: "#111" } }
      },
      yAxis: {
        type: "value",
        axisLine: { lineStyle: { color: "#555" } },
        axisLabel: {
          color: "#999",
          formatter: function (val) {
            return Math.round(val).toString();
          }
        },
        splitLine: { show: true, lineStyle: { color: "#111" } }
      },
      dataZoom: [
        {
          type: "inside",
          throttle: 50
        },
        {
          type: "slider",
          height: 20,
          bottom: 30
        }
      ],
      series: []
    };
  }

  function computeYBounds() {
    const values = [];
    state.ticks.forEach((t) => {
      if (t.mid != null) values.push(Number(t.mid));
      if (t.kal != null) values.push(Number(t.kal));
      if (t.bid != null) values.push(Number(t.bid));
      if (t.ask != null) values.push(Number(t.ask));
    });
    if (!values.length) return { min: 0, max: 1 };
    let min = Math.min(...values);
    let max = Math.max(...values);
    if (!isFinite(min) || !isFinite(max)) return { min: 0, max: 1 };
    min = Math.floor(min) - 1;
    max = Math.ceil(max) + 1;
    if (min === max) max = min + 2;
    return { min, max };
  }

  function buildSeries() {
    const xVals = state.ticks.map((t) => t.ts);
    const midData = state.ticks.map((t) => (t.mid != null ? Math.round(t.mid) : null));
    const kalData = state.ticks.map((t) => (t.kal != null ? Math.round(t.kal) : null));

    // Map tickId -> index on x axis for overlays
    const idxByTickId = new Map();
    state.ticks.forEach((t, idx) => {
      if (t.id != null) idxByTickId.set(Number(t.id), idx);
    });

    // piv_hilo scatter (highs vs lows)
    const pivHi = [];
    const pivLo = [];
    state.pivHilo.forEach((p) => {
      const i = idxByTickId.get(Number(p.tick_id));
      if (i == null) return;
      const y = p.mid != null ? Math.round(p.mid) : null;
      if (y == null) return;
      if (Number(p.ptype) > 0) {
        pivHi.push([xVals[i], y, p]);
      } else {
        pivLo.push([xVals[i], y, p]);
      }
    });

    // piv_swings scatter
    const swings = [];
    state.pivSwings.forEach((p) => {
      const i = idxByTickId.get(Number(p.tick_id));
      if (i == null) return;
      const y = p.mid != null ? Math.round(p.mid) : null;
      if (y == null) return;
      swings.push([xVals[i], y, p]);
    });

    // hhll scatter – coloured by class_text
    const hhllMap = {};
    state.hhll.forEach((p) => {
      const i = idxByTickId.get(Number(p.tick_id));
      if (i == null) return;
      const y = p.mid != null ? Math.round(p.mid) : null;
      if (y == null) return;
      const key = p.class_text || "HHLL";
      if (!hhllMap[key]) hhllMap[key] = [];
      hhllMap[key].push([xVals[i], y, p]);
    });

    // zones_hhll as markArea segments
    const zoneAreas = [];
    state.zonesHhll.forEach((z) => {
      // We use times for x, prices for y
      const start = z.start_time || z.start_ts || null;
      const end = z.end_time || z.end_ts || null;
      if (!start || !end) return;
      const top = z.top_price != null ? Math.round(z.top_price) : null;
      const bot = z.bot_price != null ? Math.round(z.bot_price) : null;
      if (top == null || bot == null) return;
      const yTop = Math.max(top, bot);
      const yBot = Math.min(top, bot);
      zoneAreas.push([
        { xAxis: start, yAxis: yBot, value: z },
        { xAxis: end, yAxis: yTop }
      ]);
    });

    const series = [];

    // main lines
    series.push({
      id: "mid",
      name: "Mid",
      type: "line",
      showSymbol: false,
      data: xVals.map((x, i) => [x, midData[i]]),
      lineStyle: { width: 1.2 },
      smooth: true
    });

    series.push({
      id: "kal",
      name: "Kal",
      type: "line",
      showSymbol: false,
      data: xVals.map((x, i) => [x, kalData[i]]),
      lineStyle: { width: 1.2 },
      smooth: true
    });

    // Hi / Lo pivots
    series.push({
      id: "piv_hi",
      name: "Hi/Lo Pivots (Hi)",
      type: "scatter",
      symbolSize: 6,
      data: pivHi,
      emphasis: { focus: "series" }
    });

    series.push({
      id: "piv_lo",
      name: "Hi/Lo Pivots (Lo)",
      type: "scatter",
      symbolSize: 6,
      data: pivLo,
      emphasis: { focus: "series" }
    });

    // Swings
    series.push({
      id: "swings",
      name: "Swings",
      type: "scatter",
      symbolSize: 7,
      data: swings,
      emphasis: { focus: "series" }
    });

    // HHLL classes – one series per class_text
    Object.keys(hhllMap).forEach((cls, idx) => {
      series.push({
        id: "hhll_" + cls,
        name: "HHLL " + cls,
        type: "scatter",
        symbolSize: 7,
        data: hhllMap[cls],
        emphasis: { focus: "series" }
      });
    });

    // Zones as markArea on an "empty" series
    series.push({
      id: "zones_hhll",
      name: "Zones HHLL",
      type: "line",
      data: [],
      markArea: {
        silent: true,
        itemStyle: {
          opacity: 0.08
        },
        data: zoneAreas
      }
    });

    return { xVals, series };
  }

  function buildTooltipFormatter(xVals) {
    return function (params) {
      // params is array of points on same x
      if (!params || !params.length) return "";

      // Use first param's x value as ts
      const ts = params[0].axisValue || params[0].data[0];
      const dt = formatDateTime(ts);
      let html = `<div style="font-size:12px;">` +
        `<div>${dt.date}</div>` +
        `<div>${dt.time}</div><hr/>`;

      // Group overlay payloads
      const extras = [];

      params.forEach((p) => {
        const seriesId = p.seriesId || p.seriesName;
        const yVal = Array.isArray(p.data) ? p.data[1] : p.data;
        if (seriesId === "mid" || seriesId === "kal") {
          html += `<div>${p.marker} ${p.seriesName}: ${Math.round(yVal)}</div>`;
        } else if (seriesId === "piv_hi" || seriesId === "piv_lo") {
          const payload = Array.isArray(p.data) ? p.data[2] : null;
          if (payload) {
            extras.push({
              label: "piv_hilo",
              text: `${seriesId === "piv_hi" ? "High" : "Low"} piv – mid:${Math.round(yVal)} ptype:${payload.ptype} winL:${payload.win_left} winR:${payload.win_right}`
            });
          }
        } else if (seriesId === "swings") {
          const payload = Array.isArray(p.data) ? p.data[2] : null;
          if (payload) {
            extras.push({
              label: "swings",
              text: `Swing – mid:${Math.round(yVal)} ptype:${payload.ptype} swing:${payload.swing_index}`
            });
          }
        } else if (String(seriesId).startsWith("hhll_")) {
          const payload = Array.isArray(p.data) ? p.data[2] : null;
          if (payload) {
            extras.push({
              label: "hhll",
              text: `HHLL – mid:${Math.round(yVal)} class:${payload.class_text} ptype:${payload.ptype}`
            });
          }
        }
      });

      if (extras.length) {
        html += "<hr/>";
        extras.forEach((e) => {
          html += `<div>${e.text}</div>`;
        });
      }

      html += "</div>";
      return html;
    };
  }

  function render() {
    if (!chart) return;
    const option = buildBaseOption();
    const { series, xVals } = buildSeries();
    const bounds = computeYBounds();

    option.xAxis.data = xVals;
    option.yAxis.min = bounds.min;
    option.yAxis.max = bounds.max;
    option.series = series;
    option.tooltip.formatter = buildTooltipFormatter(xVals);

    chart.setOption(option, true);
  }

  function renderLive(ticks) {
    if (!chart) return;
    state.mode = "live";
    state.ticks = ticks || [];
    if (state.ticks.length) {
      state.lastTickId = state.ticks[state.ticks.length - 1].id;
    }
    state.pivHilo = [];
    state.pivSwings = [];
    state.hhll = [];
    state.zonesHhll = [];
    render();
  }

  // ---------- Public API ----------

  async function loadWindow(fromId, window) {
    const res = await fetch(`/api/review/window?from_id=${fromId}&window=${window}`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();

    state.mode = "review";
    state.ticks = data.ticks || [];
    state.pivHilo = data.piv_hilo || [];
    state.pivSwings = data.piv_swings || [];
    state.hhll = data.hhll || [];
    state.zonesHhll = data.zones_hhll || [];
    state.lastTickId = state.ticks.length ? state.ticks[state.ticks.length - 1].id : null;

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

  function startLive(opts) {
    const intervalMs = (opts && opts.intervalMs) || 2000;
    state.liveLimit = (opts && opts.limit) || state.liveLimit || 5000;

    if (state.liveTimer) {
      clearInterval(state.liveTimer);
      state.liveTimer = null;
    }
    // first immediate poll
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

  function setVisibility(seriesId, visible) {
    if (!chart) return;
    const opt = chart.getOption();
    if (!opt || !opt.series) return;

    opt.series.forEach((s) => {
      if (s.id === seriesId) {
        s.show = visible;
      }
    });
    chart.setOption({ series: opt.series }, false);
  }

  return {
    init(domId) {
      ensureChart(domId);
    },
    loadWindow,
    startLive,
    stopLive,
    setVisibility
  };
})();
