// frontend/chart-core.js
// ChartCore: single ECharts instance responsible for rendering tick price series
// and overlays (pivots / swings / zones). This module exposes a stable API used
// by page controllers (index-core.js, tick-core.js, htick-core.js, review-core.js).

const ChartCore = (function () {
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
    liveLimit: 2000, // default to last 2000 ticks for live

    windowChangeHandler: null, // callback(info)
    hasInit: false, // tracks whether we've set the base option (for zoom preservation)

    // group-level visibility flags (used when building series)
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

    // eval overlay state (non-breaking additive)
    evals: [], // raw eval rows currently attached to the window
    evalMinLevel: 1, // active minimum level
    evalVisibility: true, // master on/off for eval overlay
  };

  // ---------- Helpers ----------

  function ensureChart(domId) {
    const dom = document.getElementById(domId);
    if (!dom) {
      console.error("ChartCore: container not found:", domId);
      return null;
    }

    chart = echarts.init(dom, null, { useDirtyRect: true });
    chart.on("dataZoom", function () {
      // Recompute Y bounds for the new visible X window
      recomputeYFromVisibleWindow(chart, state);
    });

    state.hasInit = false;

    window.addEventListener("resize", () => chart && chart.resize());

    return chart;
  }

  function toISO(ts) {
    if (!ts) return "";
    try {
      const d = new Date(ts);
      return d.toISOString();
    } catch (e) {
      return String(ts);
    }
  }

  function safeNum(v) {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  }

  function computeYBoundsFromTicks(ticks, xFromTs, xToTs) {
    // Find points in [xFromTs, xToTs], collect numeric Y values from bid/ask/mid/kal (if present).
    const ys = [];

    for (const t of ticks) {
      if (t.ts < xFromTs || t.ts > xToTs) continue;

      const bid = safeNum(t.bid);
      const ask = safeNum(t.ask);
      const mid = safeNum(t.mid);
      const kal = safeNum(t.kal);

      if (bid != null) ys.push(bid);
      if (ask != null) ys.push(ask);
      if (mid != null) ys.push(mid);
      if (kal != null) ys.push(kal);
    }

    if (!ys.length) return null;

    const minY = Math.min(...ys);
    const maxY = Math.max(...ys);

    // Give some padding
    const paddedMin = Math.floor(minY);
    const paddedMax = Math.ceil(maxY);

    return { min: paddedMin, max: paddedMax };
  }

  function recomputeYFromVisibleWindow(chart, state) {
    if (!chart || !state.ticks || !state.ticks.length) return;

    const option = chart.getOption();
    const xAxis = option.xAxis && option.xAxis[0];
    const dataZoom = option.dataZoom && option.dataZoom[0];

    if (!xAxis || !dataZoom) return;

    // We use category axis with timestamps stored in xAxis.data (xVals).
    const xVals = xAxis.data || [];
    const startPct = dataZoom.start != null ? dataZoom.start : 0;
    const endPct = dataZoom.end != null ? dataZoom.end : 100;

    const n = xVals.length;
    if (!n) return;

    const i0 = Math.max(0, Math.floor((startPct / 100) * (n - 1)));
    const i1 = Math.min(n - 1, Math.ceil((endPct / 100) * (n - 1)));

    const xFromTs = xVals[i0];
    const xToTs = xVals[i1];

    const bounds = computeYBoundsFromTicks(state.ticks, xFromTs, xToTs);
    if (!bounds) return;

    chart.setOption(
      {
        yAxis: [{ min: bounds.min, max: bounds.max }],
      },
      { notMerge: false, lazyUpdate: true }
    );
  }

  function buildYAxisPatch(xVals) {
    // Called during render; keep initial y-axis auto, but after a zoom operation we patch it.
    // We compute bounds for the full window on initial render.
    if (!state.ticks.length) return {};
    const xFromTs = xVals[0];
    const xToTs = xVals[xVals.length - 1];
    const bounds = computeYBoundsFromTicks(state.ticks, xFromTs, xToTs);
    if (!bounds) return {};
    return { yAxis: [{ min: bounds.min, max: bounds.max }] };
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

    for (const p of state.pivHilo) {
      const i = idxByTickId.get(Number(p.tick_id));
      if (i == null) continue;
      const ts = xVals[i];

      // y uses mid, or p.mid if present
      const y =
        p.mid != null ? Number(p.mid) : state.ticks[i].mid != null ? Number(state.ticks[i].mid) : null;
      if (y == null) continue;

      const payload = {
        id: p.id,
        tick_id: p.tick_id,
        mid: p.mid,
        ptype: p.ptype,
        win_left: p.win_left,
        win_right: p.win_right,
        computed_at: p.computed_at,
      };

      if (p.ptype === "hi") pivHi.push([ts, y, payload]);
      if (p.ptype === "lo") pivLo.push([ts, y, payload]);
    }

    // piv_swings
    const swings = [];
    for (const p of state.pivSwings) {
      const i = idxByTickId.get(Number(p.tick_id));
      if (i == null) continue;
      const ts = xVals[i];
      const y =
        p.mid != null ? Number(p.mid) : state.ticks[i].mid != null ? Number(state.ticks[i].mid) : null;
      if (y == null) continue;

      const payload = {
        id: p.id,
        tick_id: p.tick_id,
        mid: p.mid,
        ptype: p.ptype,
        swing_index: p.swing_index,
        computed_at: p.computed_at,
      };
      swings.push([ts, y, payload]);
    }

    // hhll
    const hhllByClass = new Map();
    for (const p of state.hhll) {
      const i = idxByTickId.get(Number(p.tick_id));
      if (i == null) continue;
      const ts = xVals[i];
      const y =
        p.mid != null ? Number(p.mid) : state.ticks[i].mid != null ? Number(state.ticks[i].mid) : null;
      if (y == null) continue;

      const payload = {
        id: p.id,
        tick_id: p.tick_id,
        mid: p.mid,
        ptype: p.ptype,
        class_text: p.class_text,
        computed_at: p.computed_at,
      };

      const k = String(p.class_text || "");
      if (!hhllByClass.has(k)) hhllByClass.set(k, []);
      hhllByClass.get(k).push([ts, y, payload]);
    }

    // series list
    const series = [];

    // Price series
    series.push({
      id: "mid",
      name: "Mid",
      type: "line",
      data: midData,
      showSymbol: false,
      smooth: false,
      show: vis.mid,
    });

    series.push({
      id: "kal",
      name: "Kal",
      type: "line",
      data: kalData,
      showSymbol: false,
      smooth: false,
      show: vis.kal,
    });

    series.push({
      id: "bid",
      name: "Bid",
      type: "line",
      data: bidData,
      showSymbol: false,
      smooth: false,
      show: vis.bid,
    });

    series.push({
      id: "ask",
      name: "Ask",
      type: "line",
      data: askData,
      showSymbol: false,
      smooth: false,
      show: vis.ask,
    });

    // Pivots
    if (vis.hipiv) {
      series.push({
        id: "piv_hi",
        name: "PivHi",
        type: "scatter",
        data: pivHi,
        symbolSize: 8,
        show: true,
      });
    }
    if (vis.lopiv) {
      series.push({
        id: "piv_lo",
        name: "PivLo",
        type: "scatter",
        data: pivLo,
        symbolSize: 8,
        show: true,
      });
    }

    // Swings
    if (vis.swings) {
      series.push({
        id: "swings",
        name: "Swings",
        type: "scatter",
        data: swings,
        symbolSize: 6,
        show: true,
      });
    }

    // HHLL groups
    for (const [cls, pts] of hhllByClass.entries()) {
      series.push({
        id: `hhll_${cls}`,
        name: `HHLL ${cls}`,
        type: "scatter",
        data: pts,
        symbolSize: 6,
        show: true,
      });
    }

    // Zones
    if (vis.zones) {
      const zoneAreas = [];
      for (const z of state.zonesHhll) {
        const iFrom = idxByTickId.get(Number(z.tick_from));
        const iTo = idxByTickId.get(Number(z.tick_to));
        if (iFrom == null || iTo == null) continue;

        const x1 = xVals[iFrom];
        const x2 = xVals[iTo];

        const y1 = safeNum(z.low);
        const y2 = safeNum(z.high);
        if (y1 == null || y2 == null) continue;

        zoneAreas.push([{ xAxis: x1, yAxis: y1 }, { xAxis: x2, yAxis: y2 }]);
      }

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

    // --------------------------
    // Evals overlays (scatter)
    // Align eval.tick_id to the tick index -> xVals[index], so dots sit on same X grid.
    // --------------------------
    function colorForSign(sign) {
      if (sign > 0) return "#4caf50";
      if (sign < 0) return "#f44336";
      return "#9e9e9e";
    }
    function sizeForLevel(level) {
      const lvl = Number(level) || 1;
      return Math.max(4, Math.min(22, 4 + 2 * lvl));
    }

    if (state.evalVisibility && Array.isArray(state.evals) && state.evals.length) {
      const byLevel = new Map();

      for (const r of state.evals) {
        const level = Number(r.level);
        if (!Number.isFinite(level) || level < state.evalMinLevel) continue;

        const tickId = Number(r.tick_id);
        const mid = Number(r.mid);
        if (!Number.isFinite(tickId) || !Number.isFinite(mid)) continue;

        const i = idxByTickId.get(tickId);
        if (i == null) continue;

        if (!byLevel.has(level)) byLevel.set(level, []);
        byLevel.get(level).push({ i, mid, r });
      }

      for (const level of Array.from(byLevel.keys()).sort((a, b) => a - b)) {
        const rows = byLevel.get(level);
        const points = rows.map(({ i, mid, r }) => [xVals[i], mid, r]);

        series.push({
          id: `eval_L${level}`,
          name: `Eval L${level}`,
          type: "scatter",
          symbol: "circle",
          symbolSize: function (val) {
            const payload = Array.isArray(val) ? val[2] : null;
            return sizeForLevel(payload ? payload.level : level);
          },
          itemStyle: {
            color: function (p) {
              const payload = p && p.data ? p.data[2] : null;
              const s = payload ? Number(payload.base_sign) : 0;
              return colorForSign(s);
            },
          },
          data: points,
          emphasis: { focus: "series" },
          show: true,
          tooltip: { trigger: "item" },
        });
      }
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

      const axisValue = params[0].axisValue;
      const dt = (() => {
        const iso = toISO(axisValue);
        if (!iso) return { date: "", time: "" };
        const [d, t] = iso.split("T");
        return { date: d || "", time: (t || "").replace("Z", "") };
      })();

      const info = infoByTs.get(axisValue) || {};
      const idText = info.id != null ? info.id : "";

      let html = "";
      html += `<b>${axisValue}</b><br/>`;
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

        if (seriesId === "mid" || seriesId === "kal" || seriesId === "bid" || seriesId === "ask") {
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
            extras.push(`Swing – mid:${yText} ptype:${payload.ptype} swing:${payload.swing_index}`);
          }
        } else if (String(seriesId).startsWith("hhll_")) {
          const payload = Array.isArray(data) ? data[2] : null;
          if (payload) {
            extras.push(`HHLL – mid:${yText} class:${payload.class_text} ptype:${payload.ptype}`);
          }
        } else if (String(seriesId).startsWith("eval_L")) {
          const payload = Array.isArray(data) ? data[2] : null;
          if (payload) {
            const lvl = payload.level != null ? payload.level : "";
            const sign = payload.base_sign != null ? payload.base_sign : "";
            const imp = payload.signed_importance != null ? payload.signed_importance : "";
            extras.push(`Eval – mid:${yText} level:${lvl} sign:${sign} imp:${imp}`);
            if (payload.promotion_path) {
              extras.push(`Path – ${payload.promotion_path}`);
            }
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
      state.windowChangeHandler({ count: 0, firstId: null, lastId: null });
      return;
    }

    const firstId = state.ticks[0].id;
    const lastId = state.ticks[n - 1].id;

    state.windowChangeHandler({ count: n, firstId, lastId });
  }

  function render() {
    if (!chart) return;

    const { series, xVals } = buildSeries();
    const yAxisPatch = buildYAxisPatch(xVals);
    const tooltip = { formatter: buildTooltipFormatter(xVals, state.ticks) };

    if (!state.hasInit) {
      chart.setOption(
        {
          animation: false,
          grid: { left: 45, right: 25, top: 20, bottom: 40 },
          tooltip: { trigger: "axis", axisPointer: { type: "cross" }, ...tooltip },
          xAxis: {
            type: "category",
            data: xVals,
            axisLabel: { formatter: (v) => String(v).slice(11, 19) },
          },
          yAxis: { type: "value", scale: true },
          dataZoom: [
            { type: "inside", xAxisIndex: 0, filterMode: "none" },
            { type: "slider", xAxisIndex: 0, filterMode: "none" },
          ],
          series,
          ...yAxisPatch,
        },
        { notMerge: true, lazyUpdate: true }
      );
      state.hasInit = true;
    } else {
      const oldOpt = chart.getOption();
      const dz = oldOpt && oldOpt.dataZoom ? oldOpt.dataZoom[0] : null;

      chart.setOption(
        {
          tooltip: { trigger: "axis", axisPointer: { type: "cross" }, ...tooltip },
          xAxis: [{ data: xVals }],
          series,
          ...yAxisPatch,
        },
        { notMerge: false, lazyUpdate: true }
      );

      if (dz && dz.start != null && dz.end != null) {
        chart.dispatchAction({ type: "dataZoom", start: dz.start, end: dz.end });
      }
    }

    notifyWindowChange();
  }

  function handleDataZoom() {
    recomputeYFromVisibleWindow(chart, state);
  }

  async function loadLiveOnce(limit) {
    const lim = limit != null ? Number(limit) : state.liveLimit;
    const url = `/api/live_window?limit=${encodeURIComponent(lim)}`;

    const res = await fetch(url);
    if (!res.ok) throw new Error(`live_window failed: ${res.status}`);
    const data = await res.json();

    state.mode = "live";
    state.ticks = Array.isArray(data.ticks) ? data.ticks : [];
    state.pivHilo = [];
    state.pivSwings = [];
    state.hhll = [];
    state.zonesHhll = [];

    if (state.ticks.length) {
      state.lastTickId = state.ticks[state.ticks.length - 1].id;
    } else {
      state.lastTickId = null;
    }

    render();
    return data;
  }

  async function startLive(opts) {
    const limit = opts && opts.limit != null ? Number(opts.limit) : state.liveLimit;
    const intervalMs = opts && opts.intervalMs != null ? Number(opts.intervalMs) : 2000;

    stopLive();
    state.mode = "live";
    state.liveLimit = limit;

    await loadLiveOnce(limit);

    state.liveTimer = setInterval(async () => {
      try {
        const res = await fetch(`/api/live_last_tick`);
        if (!res.ok) return;
        const d = await res.json();
        const lastId = d && d.id != null ? Number(d.id) : null;
        if (!lastId) return;

        if (state.lastTickId == null || lastId > Number(state.lastTickId)) {
          await loadLiveOnce(limit);
        }
      } catch (e) {
        console.warn("ChartCore live poll failed:", e);
      }
    }, intervalMs);
  }

  function stopLive() {
    if (state.liveTimer) {
      clearInterval(state.liveTimer);
      state.liveTimer = null;
    }
  }

  // ✅ FIXED: review endpoint is /api/review/window (NOT /api/ticks/window)
  async function loadWindow(fromId, windowSize) {
    const from = Number(fromId);
    const win = Number(windowSize);
    const url =
      `/api/review/window?from_id=${encodeURIComponent(from)}` +
      `&window=${encodeURIComponent(win)}`;

    const res = await fetch(url);
    if (!res.ok) throw new Error(`review/window failed: ${res.status}`);
    const data = await res.json();

    state.mode = "review";
    state.ticks = Array.isArray(data.ticks) ? data.ticks : [];
    state.pivHilo = Array.isArray(data.piv_hilo) ? data.piv_hilo : [];
    state.pivSwings = Array.isArray(data.piv_swings) ? data.piv_swings : [];
    state.hhll = Array.isArray(data.hhll) ? data.hhll : [];
    state.zonesHhll = Array.isArray(data.zones_hhll) ? data.zones_hhll : [];

    if (state.ticks.length) {
      state.lastTickId = state.ticks[state.ticks.length - 1].id;
    } else {
      state.lastTickId = null;
    }

    render();
    return data;
  }

  function setVisibility(group, visible) {
    if (!(group in state.visibility)) return;
    state.visibility[group] = !!visible;
    render();
  }

  function setWindowChangeHandler(fn) {
    state.windowChangeHandler = typeof fn === "function" ? fn : null;
  }

  function setEvals(rows, minLevel) {
    state.evals = Array.isArray(rows) ? rows : [];
    state.evalMinLevel = typeof minLevel === "number" ? minLevel : 1;
    render();
  }

  function setEvalVisibility(visible) {
    state.evalVisibility = !!visible;
    render();
  }

  // ---------- Public API ----------

  return {
    init(domId) {
      const c = ensureChart(domId);
      if (!c) return;

      c.off && c.off("dataZoom");
      c.on("dataZoom", handleDataZoom);
    },
    loadWindow,
    startLive,
    stopLive,
    setVisibility,
    setWindowChangeHandler,
    loadLiveOnce,
    setEvals,
    setEvalVisibility,
  };
})();
