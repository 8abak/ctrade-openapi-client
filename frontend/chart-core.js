// PATH: frontend/chart-core.js
// ECharts core for datavis.au – live + review
// - Dark theme
// - Grid at integer dollars, data kept as real decimals
// - Vertical + horizontal zoom
// - Live mode (last N ticks, auto-shift)
// - Review window mode (/api/review/window)
// - Overlays: piv_hilo, piv_swings, hhll_piv, zones_hhll
// - Tooltip: shows ID at top, date/time on two lines, extra tables when enabled
// - Window summary callback for "X ticks from A to B" display

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
    liveLimit: 4000,
    windowListener: null // fn(summaryText, info)
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
    // ts: "2025-12-04T17:47:42.593000+11:00"
    if (!ts || typeof ts !== "string") return { date: "", time: "" };
    const tParts = ts.split("T");
    if (tParts.length < 2) return { date: ts, time: "" };

    const date = tParts[0];
    let rest = tParts[1];

    // strip timezone (+11:00 / -05:00)
    const plusIdx = rest.indexOf("+");
    const minusIdx = rest.indexOf("-");
    let cut = rest.length;
    if (plusIdx > -1) cut = plusIdx;
    if (minusIdx > 0 && minusIdx < cut) cut = minusIdx;
    rest = rest.slice(0, cut);

    // strip fractional seconds
    const dotIdx = rest.indexOf(".");
    if (dotIdx > -1) rest = rest.slice(0, dotIdx);

    return { date, time: rest };
  }

  function notifyWindowChange() {
    if (!state.windowListener) return;
    const n = state.ticks.length;
    let firstId = null;
    let lastId = null;
    if (n > 0) {
      firstId = state.ticks[0].id;
      lastId = state.ticks[n - 1].id;
    }
    const summary =
      n > 0
        ? `${n} ticks from ${firstId} to ${lastId}`
        : "No ticks loaded";
    state.windowListener(summary, { count: n, firstId, lastId });
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
            const dt = formatDateTime(value);
            return dt.time || value;
          }
        },
        splitLine: { show: true, lineStyle: { color: "#111" } }
      },
      yAxis: {
        type: "value",
        scale: true, // allow vertical zoom
        axisLine: { lineStyle: { color: "#555" } },
        axisLabel: {
          color: "#999",
          // grid labels snapped to whole dollars
          formatter: function (val) {
            return Math.round(val).toString();
          }
        },
        splitLine: { show: true, lineStyle: { color: "#111" } },
        // min/max based on *visible* range (so vertical zoom works)
        min: function (val) {
          return Math.floor(val.min) - 1;
        },
        max: function (val) {
          return Math.ceil(val.max) + 1;
        }
      },
      dataZoom: [
        // horizontal inside zoom
        {
          type: "inside",
          xAxisIndex: 0,
          filterMode: "none",
          throttle: 50
        },
        // vertical inside zoom
        {
          type: "inside",
          yAxisIndex: 0,
          filterMode: "none",
          throttle: 50
        },
        // bottom slider (x only)
        {
          type: "slider",
          xAxisIndex: 0,
          height: 20,
          bottom: 30
        }
      ],
      series: []
    };
  }

  function buildSeries() {
    const xVals = state.ticks.map((t) => t.ts);

    // REAL decimal values
    const midData = state.ticks.map((t) =>
      t.mid != null ? Number(t.mid) : null
    );
    const kalData = state.ticks.map((t) =>
      t.kal != null ? Number(t.kal) : null
    );

    // Map tickId -> index on x axis
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
      if (Number(p.ptype) > 0) {
        pivHi.push([xVals[i], y, p]);
      } else {
        pivLo.push([xVals[i], y, p]);
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

    // hhll_piv by class_text
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

    // zones_hhll as markArea
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
      name: "Hi Piv",
      type: "scatter",
      symbolSize: 6,
      data: pivHi,
      emphasis: { focus: "series" }
    });

    series.push({
      id: "piv_lo",
      name: "Lo Piv",
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
    Object.keys(hhllMap).forEach((cls) => {
      series.push({
        id: "hhll_" + cls,
        name: "HHLL " + cls,
        type: "scatter",
        symbolSize: 7,
        data: hhllMap[cls],
        emphasis: { focus: "series" }
      });
    });

    // Zones as markArea on an "empty" line series
    series.push({
      id: "zones_hhll",
      name: "Zones",
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
      if (!params || !params.length) return "";

      const first = params[0];
      const index = first.dataIndex != null ? first.dataIndex : 0;
      const tick = state.ticks[index] || {};
      const tickId = tick.id;

      const ts = first.axisValue || first.data[0];
      const dt = formatDateTime(ts);

      let html =
        `<div style="font-size:12px;">` +
        `<div><strong>ID: ${tickId != null ? tickId : "?"}</strong></div>` +
        `<div>${dt.date}</div>` +
        `<div>${dt.time}</div><hr/>`;

      const extras = [];

      params.forEach((p) => {
        const seriesId = p.seriesId || p.seriesName;
        const yVal = Array.isArray(p.data) ? p.data[1] : p.data;
        const yText =
          yVal == null ? "" : Number(yVal).toFixed(2);

        if (seriesId === "mid" || seriesId === "kal") {
          html += `<div>${p.marker} ${p.seriesName}: ${yText}</div>`;
        } else if (seriesId === "piv_hi" || seriesId === "piv_lo") {
          const payload = Array.isArray(p.data) ? p.data[2] : null;
          if (payload) {
            extras.push({
              label: "piv_hilo",
              text: `${seriesId === "piv_hi" ? "High" : "Low"} Piv — mid:${yText} ptype:${payload.ptype} winL:${payload.win_left} winR:${payload.win_right}`
            });
          }
        } else if (seriesId === "swings") {
          const payload = Array.isArray(p.data) ? p.data[2] : null;
          if (payload) {
            extras.push({
              label: "swings",
              text: `Swing — mid:${yText} ptype:${payload.ptype} swing:${payload.swing_index}`
            });
          }
        } else if (String(seriesId).startsWith("hhll_")) {
          const payload = Array.isArray(p.data) ? p.data[2] : null;
          if (payload) {
            extras.push({
              label: "hhll",
              text: `HHLL — mid:${yText} class:${payload.class_text} ptype:${payload.ptype}`
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

    option.xAxis.data = xVals;
    option.series = series;
    option.tooltip.formatter = buildTooltipFormatter(xVals);

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
    const res = await fetch(
      `/api/review/window?from_id=${fromId}&window=${window}`
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

  function startLive(opts) {
    const intervalMs = (opts && opts.intervalMs) || 2000;
    state.liveLimit = (opts && opts.limit) || state.liveLimit || 4000;

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

  function setVisibility(idOrPrefix, visible) {
    if (!chart) return;
    const opt = chart.getOption();
    if (!opt || !opt.series) return;

    opt.series.forEach((s) => {
      if (
        s.id === idOrPrefix ||
        (idOrPrefix && s.id && s.id.startsWith(idOrPrefix))
      ) {
        s.show = visible;
      }
    });
    chart.setOption({ series: opt.series }, false);
  }

  function onWindowChange(fn) {
    state.windowListener = fn || null;
    // fire immediately with current state
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
    onWindowChange
  };
})();
