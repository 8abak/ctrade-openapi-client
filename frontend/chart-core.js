// frontend/chart-core.js
// ChartCore: single ECharts instance responsible for rendering tick price series
// and overlays (evals + segLines). Public API used by controllers.

const ChartCore = (function () {
  let chart = null;

  const state = {
    mode: "review", // "review" | "live"
    ticks: [],
    lastTickId: null,

    liveTimer: null,
    liveLimit: 2000,

    windowChangeHandler: null,
    hasInit: false,

    // visibility flags
    visibility: {
      mid: true,
      bid: true,
      ask: true,
      kal: true,
    },

    // eval overlay state
    evals: [],
    evalMinLevel: 1,
    evalVisibility: true,

    // segLines overlay state
    segLines: [],              // active lines for current segm
    segLinesVisibility: true,  // show/hide overlay
    selectedSegLineId: null,   // line selected in table (optional)
  };

  // ---------- Helpers ----------

  function ensureChart(domId) {
    const dom = document.getElementById(domId);
    if (!dom) {
      console.error("ChartCore: container not found:", domId);
      return null;
    }

    chart = echarts.init(dom, null, { useDirtyRect: true });

    // Avoid duplicate handlers (init() also wires it)
    chart.off && chart.off("dataZoom");
    chart.on("dataZoom", function () {
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
    } catch {
      return String(ts);
    }
  }

  function safeNum(v) {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  }

  function computeYBoundsFromTicks(ticks, xFromTs, xToTs) {
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

    return { min: Math.floor(minY), max: Math.ceil(maxY) };
  }

  function buildSegLineMarkers(lines) {
    return lines.map(ln => ({
      name: `L${ln.id}`,
      xAxis: ln.start_ts,
      label: {
        show: true,
        formatter: `L${ln.id}`,
        color: "#ff9800"
      },
      lineStyle: {
        color: "#ff9800",
        width: 1,
        type: "dashed"
      }
    }));
  }


  function recomputeYFromVisibleWindow(chart, state) {
    if (!chart || !state.ticks || !state.ticks.length) return;

    const option = chart.getOption();
    const xAxis = option.xAxis && option.xAxis[0];
    const dataZoom = option.dataZoom && option.dataZoom[0];
    if (!xAxis || !dataZoom) return;

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
      { yAxis: [{ min: bounds.min, max: bounds.max }] },
      { notMerge: false, lazyUpdate: true }
    );
  }

  function buildYAxisPatch(xVals) {
    if (!state.ticks.length || !xVals.length) return {};
    const bounds = computeYBoundsFromTicks(state.ticks, xVals[0], xVals[xVals.length - 1]);
    if (!bounds) return {};
    return { yAxis: [{ min: bounds.min, max: bounds.max }] };
  }

  function _slopePerSec(line) {
    const durMs = line && line.duration_ms != null ? Number(line.duration_ms) : null;
    if (!durMs || durMs <= 0) return null;
    return (Number(line.end_price) - Number(line.start_price)) / (durMs / 1000.0);
  }

  function _findLineCoveringTs(ts) {
    if (!state.segLinesVisibility) return null;
    if (!ts || !state.segLines || !state.segLines.length) return null;

    // ISO strings compare lexicographically if same format; we return iso from backend
    // We accept a simple containment match:
    for (const ln of state.segLines) {
      if (ln && ln.start_ts <= ts && ts <= ln.end_ts) return ln;
    }
    return null;
  }

  function buildSeries() {
    const xVals = state.ticks.map((t) => t.ts);
    const vis = state.visibility;

    const midData = state.ticks.map((t) => [t.ts, t.mid != null ? Number(t.mid) : null, { id: t.id }]);
    const kalData = state.ticks.map((t) => [t.ts, t.kal != null ? Number(t.kal) : null, { id: t.id }]);
    const bidData = state.ticks.map((t) => [t.ts, t.bid != null ? Number(t.bid) : null, { id: t.id }]);
    const askData = state.ticks.map((t) => [t.ts, t.ask != null ? Number(t.ask) : null, { id: t.id }]);

    // tickId -> index map (for eval alignment)
    const idxByTickId = new Map();
    state.ticks.forEach((t, idx) => {
      if (t.id != null) idxByTickId.set(Number(t.id), idx);
    });

    const series = [];

    // IMPORTANT: only push series if visible (ECharts ignores "show")
    if (vis.mid) {
      series.push({
        id: "mid",
        name: "Mid",
        type: "line",
        data: midData,
        showSymbol: false,
        smooth: false,
      });
    }

    if (vis.kal) {
      series.push({
        id: "kal",
        name: "Kal",
        type: "line",
        data: kalData,
        showSymbol: false,
        smooth: false,
      });
    }

    if (vis.bid) {
      series.push({
        id: "bid",
        name: "Bid",
        type: "line",
        data: bidData,
        showSymbol: false,
        smooth: false,
      });
    }

    if (vis.ask) {
      series.push({
        id: "ask",
        name: "Ask",
        type: "line",
        data: askData,
        showSymbol: false,
        smooth: false,
      });
    }

    // ---- segLines overlay (line) ----
    // We keep it as two series: selected + others, so selection can be highlighted.
    if (state.segLinesVisibility && Array.isArray(state.segLines) && state.segLines.length) {
      const selId = state.selectedSegLineId != null ? Number(state.selectedSegLineId) : null;

      const segDataOther = [];
      const segDataSel = [];

      for (const ln of state.segLines) {
        if (!ln) continue;
        const a = [ln.start_ts, Number(ln.start_price), ln];
        const b = [ln.end_ts, Number(ln.end_price), ln];
        const target = (selId != null && Number(ln.id) === selId) ? segDataSel : segDataOther;

        target.push(a);
        target.push(b);
        target.push([null, null, null]); // segment break
      }

      if (segDataOther.length) {
        series.push({
          id: "seglines_other",
          name: "segLines",
          type: "line",
          data: segDataOther,
          showSymbol: false,
          smooth: false,
          connectNulls: false,
          lineStyle: { width: 2 },
          emphasis: { focus: "series" },
          tooltip: { trigger: "item" },
          silent: true, // we use axis tooltip, not per-item hover
        });
      }

      if (segDataSel.length) {
        series.push({
          id: "seglines_selected",
          name: "segLines (selected)",
          type: "line",
          data: segDataSel,
          showSymbol: false,
          smooth: false,
          connectNulls: false,
          lineStyle: { width: 4 },
          emphasis: { focus: "series" },
          tooltip: { trigger: "item" },
          silent: true,
        });
      }
    }

    // ---- Evals overlays (scatter) ----
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
          tooltip: { trigger: "item" },
        });
      }
    }

    return { xVals, series };
  }

  function buildTooltipFormatter(xVals, ticks) {
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
        const parts = iso.split("T");
        const d = parts[0] || "";
        const t = (parts[1] || "").replace("Z", "");
        return { date: d, time: t };
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

      // show prices and eval details
      params.forEach((p) => {
        const seriesId = p.seriesId || p.seriesName;
        const data = p.data;
        const yVal = Array.isArray(data) ? data[1] : data;
        const yText = yVal == null ? "" : Number(yVal).toFixed(2);

        if (seriesId === "mid" || seriesId === "kal" || seriesId === "bid" || seriesId === "ask") {
          html += `${p.marker} ${p.seriesName}: ${yText}<br/>`;
        } else if (String(seriesId).startsWith("eval_L")) {
          const payload = Array.isArray(data) ? data[2] : null;
          if (payload) {
            const lvl = payload.level != null ? payload.level : "";
            const sign = payload.base_sign != null ? payload.base_sign : "";
            const imp = payload.signed_importance != null ? payload.signed_importance : "";
            extras.push(`Eval – mid:${yText} level:${lvl} sign:${sign} imp:${imp}`);
            if (payload.promotion_path) extras.push(`Path – ${payload.promotion_path}`);
          }
        }
      });

      // segLine info at this time (simple containment)
      const ln = _findLineCoveringTs(axisValue);
      if (ln) {
        const slope = _slopePerSec(ln);
        const slopeTxt = slope != null ? slope.toFixed(6) + "/s" : "";
        const maxd = ln.max_abs_dist != null ? Number(ln.max_abs_dist).toFixed(4) : "";
        extras.push(`segLine id:${ln.id} depth:${ln.depth} it:${ln.iteration} slope:${slopeTxt} max|dist|:${maxd}`);
      }

      if (extras.length) {
        html += `* * *<br/>`;
        extras.forEach((e) => (html += `${e}<br/>`));
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

    state.windowChangeHandler({
      count: n,
      firstId: state.ticks[0].id,
      lastId: state.ticks[n - 1].id,
    });
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
          markLines: {
            silent: true,
            data: buildSegLineMarkers(state.segLines)
          },
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

    if (state.ticks.length) state.lastTickId = state.ticks[state.ticks.length - 1].id;
    else state.lastTickId = null;

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
        // Backend requires limit >= 500
        const res = await fetch(`/api/live_window?limit=500`);
        if (!res.ok) return;

        const d = await res.json();
        const ticks = Array.isArray(d.ticks) ? d.ticks : [];
        const last = ticks.length ? ticks[ticks.length - 1] : null;
        const lastId = last && last.id != null ? Number(last.id) : null;
        if (!lastId) return;

        // No new ticks
        if (state.lastTickId != null && lastId <= Number(state.lastTickId)) return;

        // If this is the first time, just load the full live window
        if (state.lastTickId == null) {
          await loadLiveOnce(limit);
          return;
        }

        // Fetch only the missing ticks (handles bursts: 10 ticks in 2 seconds, etc.)
        const fromId = Number(state.lastTickId) + 1;
        const deltaUrl =
          `/api/window?tick_from=${encodeURIComponent(fromId)}` +
          `&tick_to=${encodeURIComponent(lastId)}` +
          `&min_level=${encodeURIComponent(state.evalMinLevel || 2)}` +
          `&max_rows=200000`;

        const r2 = await fetch(deltaUrl);
        if (!r2.ok) {
          // If delta endpoint isn't available/compatible, fallback to full reload
          await loadLiveOnce(limit);
          return;
        }

        const d2 = await r2.json();
        const newTicks = Array.isArray(d2.ticks) ? d2.ticks : [];

        if (newTicks.length) {
          // Append + keep last N ticks (your liveLimit)
          state.ticks = state.ticks.concat(newTicks);
          if (state.ticks.length > state.liveLimit) {
            state.ticks = state.ticks.slice(state.ticks.length - state.liveLimit);
          }
          state.lastTickId = state.ticks[state.ticks.length - 1].id;
          render();
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

    if (state.ticks.length) state.lastTickId = state.ticks[state.ticks.length - 1].id;
    else state.lastTickId = null;

    render();
    return data;
  }

  // NEW: inject ticks directly (used by segLines review page)
  function setTicks(ticks) {
    state.mode = "review";
    state.ticks = Array.isArray(ticks) ? ticks : [];
    if (state.ticks.length) state.lastTickId = state.ticks[state.ticks.length - 1].id;
    else state.lastTickId = null;
    render();
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

  // NEW: segLines overlay setters
  function setSegLines(lines, selectedId) {
    state.segLines = Array.isArray(lines) ? lines : [];
    state.selectedSegLineId = selectedId != null ? Number(selectedId) : null;
    render();
  }

  function setSegLinesVisibility(visible) {
    state.segLinesVisibility = !!visible;
    render();
  }

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

    // new API
    setTicks,
    setSegLines,
    setSegLinesVisibility,
  };
})();
