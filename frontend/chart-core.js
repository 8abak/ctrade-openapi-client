// PATH: frontend/chart-core.js

// Global namespace
window.ChartCore = (function () {
  const API_REVIEW = "/api/review/window";
  const API_LIVE = "/api/live_window";

  // Internal state
  let chart = null;
  let dom = null;
  let liveTimer = null;
  let lastTickId = null;       // last id we've seen (for live mode)
  let currentMode = "review";  // "review" | "live"

  // -------------- Helpers -----------------

  function toInt(v) {
    if (v == null || isNaN(v)) return v;
    return Math.round(Number(v));
  }

  function buildSeriesFromTicks(ticks) {
    const times = [];
    const mids = [];
    const kals = [];

    for (const t of ticks) {
      times.push(t.ts || t.timestamp || String(t.id));
      mids.push(toInt(t.mid));
      kals.push(toInt(t.kal != null ? t.kal : t.mid));
    }

    return { times, mids, kals };
  }

  function baseOption() {
    return {
      backgroundColor: "#050912",
      animation: true,
      tooltip: {
        trigger: "axis",
        backgroundColor: "rgba(15, 20, 35, 0.95)",
        borderColor: "#3ba272",
        textStyle: { color: "#fff" },
        axisPointer: {
          type: "cross",
          label: {
            backgroundColor: "#3ba272",
            formatter: function (params) {
              if (params.axisDimension === "y") {
                return String(toInt(params.value));
              }
              return params.value;
            }
          }
        },
        formatter: function (params) {
          // params: array of series points at this x
          const p0 = params[0];
          const ts = p0.axisValueLabel;
          let html = `<div style="font-size:12px;margin-bottom:4px;">${ts}</div>`;
          for (const p of params) {
            const val = toInt(p.data);
            html += `<div>
              <span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:${p.color};margin-right:6px;"></span>
              ${p.seriesName}: <b>${val}</b>
            </div>`;
          }
          return html;
        }
      },
      grid: {
        left: 50,
        right: 20,
        top: 30,
        bottom: 60
      },
      toolbox: {
        feature: {
          dataZoom: { yAxisIndex: "none" },
          restore: {},
          saveAsImage: {}
        },
        iconStyle: {
          borderColor: "#999"
        }
      },
      dataZoom: [
        {
          type: "inside",
          xAxisIndex: 0,
          filterMode: "none"
        },
        {
          type: "slider",
          xAxisIndex: 0,
          height: 24,
          bottom: 24,
          borderColor: "#333",
          backgroundColor: "rgba(12,16,30,0.8)",
          handleStyle: {
            color: "#3ba272",
            borderColor: "#3ba272"
          },
          textStyle: { color: "#888" }
        }
      ],
      xAxis: {
        type: "category",
        boundaryGap: false,
        axisLine: { lineStyle: { color: "#555" } },
        axisLabel: {
          color: "#aaa",
          formatter: function (val) {
            // Show only time part if ISO string
            if (typeof val === "string" && val.includes("T")) {
              return val.split("T")[1].slice(0, 8);
            }
            return val;
          }
        },
        splitLine: {
          show: true,
          lineStyle: { color: "rgba(255,255,255,0.04)" }
        }
      },
      yAxis: {
        type: "value",
        minInterval: 1, // <- integer steps
        axisLine: { lineStyle: { color: "#555" } },
        axisLabel: {
          color: "#aaa",
          formatter: function (val) {
            return String(toInt(val));
          }
        },
        splitLine: {
          show: true,
          lineStyle: { color: "rgba(255,255,255,0.06)" }
        }
      },
      series: [
        {
          name: "Mid",
          type: "line",
          showSymbol: false,
          smooth: true,
          lineStyle: {
            width: 1.5
          },
          areaStyle: {
            opacity: 0.15
          }
        },
        {
          name: "Kal",
          type: "line",
          showSymbol: false,
          smooth: true,
          lineStyle: {
            width: 1,
            type: "dashed"
          }
        }
      ]
    };
  }

  function ensureChart(domIdOrElement) {
    if (chart && dom) {
      return chart;
    }
    dom =
      typeof domIdOrElement === "string"
        ? document.getElementById(domIdOrElement)
        : domIdOrElement;

    if (!dom) {
      throw new Error("ChartCore: cannot find chart container element");
    }

    // assumes echarts is loaded globally
    chart = echarts.init(dom, null, { renderer: "canvas" });
    chart.setOption(baseOption(), true);
    window.addEventListener("resize", () => chart && chart.resize());
    return chart;
  }

  // -------------- Core rendering -----------------

  function renderTicks(ticks) {
    if (!ticks || ticks.length === 0) {
      if (chart) chart.clear();
      return;
    }

    const c = ensureChart(dom);
    const { times, mids, kals } = buildSeriesFromTicks(ticks);

    lastTickId = ticks[ticks.length - 1].id;

    c.setOption(
      {
        xAxis: { data: times },
        series: [
          { name: "Mid", data: mids },
          { name: "Kal", data: kals }
        ]
      },
      false
    );
  }

  // -------------- HTTP helpers -----------------

  async function fetchJson(url) {
    const resp = await fetch(url, { cache: "no-cache" });
    if (!resp.ok) {
      const text = await resp.text();
      throw new Error("HTTP " + resp.status + ": " + text);
    }
    return await resp.json();
  }

  // public: load a specific window (review mode)
  async function loadWindow(fromId, windowSize) {
    currentMode = "review";
    if (liveTimer) {
      clearInterval(liveTimer);
      liveTimer = null;
    }

    const url =
      API_REVIEW +
      "?from_id=" +
      encodeURIComponent(fromId) +
      "&window=" +
      encodeURIComponent(windowSize);

    const data = await fetchJson(url);
    renderTicks(data.ticks || []);
  }

  // -------------- Live mode -----------------

  async function fetchLiveOnce(limit) {
    const url =
      API_LIVE +
      "?limit=" +
      encodeURIComponent(limit || 5000);

    const data = await fetchJson(url);
    renderTicks(data.ticks || []);
  }

  function startLive(opts) {
    const limit = opts && opts.limit ? opts.limit : 5000;
    currentMode = "live";

    if (liveTimer) clearInterval(liveTimer);

    // initial fetch immediately
    fetchLiveOnce(limit).catch(console.error);

    // then poll every N ms
    const intervalMs = (opts && opts.intervalMs) || 2000;
    liveTimer = setInterval(() => {
      fetchLiveOnce(limit).catch(console.error);
    }, intervalMs);
  }

  function stopLive() {
    currentMode = "review";
    if (liveTimer) {
      clearInterval(liveTimer);
      liveTimer = null;
    }
  }

  // -------------- Public API -----------------

  return {
    /**
     * Init chart on a DOM element or id.
     * Example: ChartCore.init("chart");
     */
    init(domIdOrElement) {
      ensureChart(domIdOrElement);
    },

    /**
     * Load a historical review window.
     * Example: ChartCore.loadWindow(45001, 5000);
     */
    loadWindow,

    /**
     * Start live mode (poll /api/live_window).
     * Example: ChartCore.startLive({ limit: 5000, intervalMs: 2000 });
     */
    startLive,

    /**
     * Stop live polling (you can then call loadWindow(...) to study old data).
     */
    stopLive
  };
})();
