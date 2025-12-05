// PATH: frontend/chart-core.js
// ECharts core for datavis.au – live + review

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
    liveLimit: 5000,
    windowChangeHandler: null, // callback(info)
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
        splitLine: { show: true, lineStyle: { color: "#111" } },
      },
      yAxis: {
        type: "value",
        axisLine: { lineStyle: { color: "#555" } },
        axisLabel: {
          color: "#999",
          formatter: function (val) {
            return Math.round(val).toString();
          },
        },
        splitLine: { show: true, lineStyle: { color: "#111" } },
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
          // NEW: vertical zoom
          type: "inside",
          yAxisIndex: 0,
          filterMode: "none",
        },
      ],
      series: [],
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

    // include payload with id so tooltip can show it
    const midData = state.ticks.map((t) =>
      t.mid != null ? [t.ts, Number(t.mid), { id: t.id }] : [t.ts, null, { id: t.id }]
    );
    const kalData = state.ticks.map((t) =>
      t.kal != null ? [t.ts, Number(t.kal), { id: t.id }] : [t.ts, null, { id: t.id }]
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

    // main lines
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

    // Hi / Lo pivots
    series.push({
      id: "piv_hi",
      name: "Hi Piv",
      type: "scatter",
      symbolSize: 6,
      data: pivHi,
      emphasis: { focus: "series" },
      show: true,
    });

    series.push({
      id: "piv_lo",
      name: "Lo Piv",
      type: "scatter",
      symbolSize: 6,
      data: pivLo,
      emphasis: { focus: "series" },
      show: true,
    });

    // Swings
    series.push({
      id: "swings",
      name: "Swings",
      type: "scatter",
      symbolSize: 7,
      data: swings,
      emphasis: { focus: "series" },
      show: true,
    });

    // HHLL – tied to swings visibility group
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

    // Zones
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

    return { xVals, series };
  }

  function buildTooltipFormatter(xVals, ticks) {
    // ts -> [id, mid, kal]
    const infoByTs = new Map();
    ticks.forEach((t) => {
      infoByTs.set(t.ts, {
        id: t.id,
        mid: t.mid != null ? Number(t.mid) : null,
        kal: t.kal != null ? Number(t.kal) : null,
      });
    });

    return function (params) {
      if (!params || !params.length) return "";

      const first = params[0];
      const ts = first.axisValue || (Array.isArray(first.data) ? first.data[0] : null);
      const dt = formatDateTime(ts);
      const info = infoByTs.get(ts) || {};
      const idText = info.id != null ? info.id : "";

      let html =
        `<div style="font-size:12px;">` +
        `<div><b>Id: ${idText}</b></div>` +
        `<div>${dt.date}</div>` +
        `<div>${dt.time}</div><hr/>`;

      const extras = [];

      params.forEach((p) => {
        const seriesId = p.seriesId || p.seriesName;
        const data = p.data;
        const yVal = Array.isArray(data) ? data[1] : data;
        const yText = yVal == null ? "" : Number(yVal).toFixed(2);

        if (seriesId === "mid" || seriesId === "kal") {
          html += `<div>${p.marker} ${p.seriesName}: ${yText}</div>`;
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
        html += "<hr/>";
        extras.forEach((e) => (html += `<div>${e}</div>`));
      }

      html += "</div>";
      return html;
    };
  }

  function notifyWindowChange() {
    if (!state.windowChangeHandler) return;
    const n = state.ticks.length;
    if (!n) {
      state.windowChangeHandler({ count: 0, firstId: null, lastId: null });
      return;
    }
    const firstId = state.ticks[0].id;
    const lastId = state.ticks[n - 1].id;
    state.windowChangeHandler({ count: n, firstId, lastId });
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
    option.tooltip.formatter = buildTooltipFormatter(xVals, state.ticks);

    chart.setOption(option, true);
    notifyWindowChange();
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

  // group-based visibility controller
  function setVisibility(group, visible) {
    if (!chart) return;
    const opt = chart.getOption();
    if (!opt || !opt.series) return;

    opt.series.forEach((s) => {
      const id = s.id || "";
      const name = s.name || "";

      let belongs = false;
      switch (group) {
        case "mid":
          belongs = id === "mid" || name === "Mid";
          break;
        case "kal":
          belongs = id === "kal" || name === "Kal";
          break;
        case "hipiv":
          belongs = id === "piv_hi" || name === "Hi Piv";
          break;
        case "lopiv":
          belongs = id === "piv_lo" || name === "Lo Piv";
          break;
        case "swings":
          belongs =
            id === "swings" ||
            name === "Swings" ||
            String(id).startsWith("hhll_") ||
            String(name).startsWith("HHLL ");
          break;
        case "zones":
          belongs = id === "zones_hhll" || name === "Zones";
          break;
        default:
          break;
      }
      if (belongs) {
        s.show = visible;
      }
    });

    chart.setOption({ series: opt.series }, false);
  }

  function setWindowChangeHandler(fn) {
    state.windowChangeHandler = typeof fn === "function" ? fn : null;
    notifyWindowChange();
  }

  return {
    init(domId) {
      ensureChart(domId);
    },
    loadWindow,
    startLive,
    stopLive,
    setVisibility,
    setWindowChangeHandler,
  };
})();
