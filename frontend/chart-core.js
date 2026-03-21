// frontend/chart-core.js
// ChartCore: single ECharts instance responsible for rendering tick price series
// and overlays. Public API used by controllers.

const ChartCore = (function () {
  let chart = null;

  const state = {
    mode: "review", // "review" | "live"
    ticks: [],
    lastTickId: null,
    k2Candles: [],

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
      k2: true,
      piv: false,
      tpiv: false,
      tzone: true,
      tepisode: true,
      tconfirm: true,
      tscore: true,
    },

    layerAvailability: {
      k2: false,
    },
    layerAvailabilityHandler: null,

    // pivot overlay state
    pivots: [],
    tpivots: [],
    tzones: [],
    tepisodes: [],
    tconfirms: [],
    tscores: [],
    trulehits: [],
    pivotLevel: 1,
    clickHandler: null,

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

  function toFiniteNumber(v) {
    if (v === null || v === undefined) return null;
    const n = typeof v === "number" ? v : parseFloat(String(v));
    return Number.isFinite(n) ? n : null;
  }

  function tsToMs(ts) {
    if (ts == null || ts === "") return null;
    if (typeof ts === "number") return Number.isFinite(ts) ? ts : null;
    const ms = new Date(ts).getTime();
    return Number.isFinite(ms) ? ms : null;
  }

  function safeNum(v) {
    return toFiniteNumber(v);
  }

  function computeYBoundsFromTicks(ticks, xFromTs, xToTs) {
    const ys = [];

    for (const t of ticks) {
      if (t.ts < xFromTs || t.ts > xToTs) continue;

      const bid = safeNum(t.bid);
      const ask = safeNum(t.ask);
      const mid = safeNum(t.mid);
      const kal = safeNum(t.kal);
      const k2 = safeNum(t.k2);

      if (bid != null) ys.push(bid);
      if (ask != null) ys.push(ask);
      if (mid != null) ys.push(mid);
      if (kal != null) ys.push(kal);
      if (k2 != null) ys.push(k2);
    }

    if (state.visibility.piv && Array.isArray(state.pivots) && state.pivots.length) {
      const selectedLevel = Number(state.pivotLevel) || 1;
      for (const p of state.pivots) {
        if (!p || !p.ts || p.ts < xFromTs || p.ts > xToTs) continue;
        if (Number(p.level) !== selectedLevel) continue;
        const px = safeNum(p.px);
        if (px != null) ys.push(px);
      }
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
    const hasK2 = !!(state.layerAvailability && state.layerAvailability.k2);

    const midData = state.ticks.map((t) => [t.ts, t.mid != null ? Number(t.mid) : null, { id: t.id }]);
    const kalData = state.ticks.map((t) => [t.ts, t.kal != null ? Number(t.kal) : null, { id: t.id }]);
    const bidData = state.ticks.map((t) => [t.ts, t.bid != null ? Number(t.bid) : null, { id: t.id }]);
    const askData = state.ticks.map((t) => [t.ts, t.ask != null ? Number(t.ask) : null, { id: t.id }]);
    const k2Data = state.ticks.map((t) => [t.ts, safeNum(t.k2), { id: t.id }]);

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

    if (vis.k2 && hasK2) {
      series.push({
        id: "k2",
        name: "K2",
        type: "line",
        data: k2Data,
        showSymbol: false,
        smooth: false,
      });
    }

    if (vis.piv && Array.isArray(state.pivots) && state.pivots.length) {
      const selectedLevel = Number(state.pivotLevel) || 1;
      const hiPivots = [];
      const loPivots = [];

      for (const p of state.pivots) {
        if (!p || Number(p.level) !== selectedLevel) continue;
        const ts = p.ts;
        const px = safeNum(p.px);
        if (!ts || px == null) continue;
        const point = [ts, px, p];
        if (String(p.ptype || "").toLowerCase() === "h") hiPivots.push(point);
        else loPivots.push(point);
      }

      if (hiPivots.length) {
        series.push({
          id: "piv_hi",
          name: `Piv L${selectedLevel} High`,
          type: "scatter",
          data: hiPivots,
          symbol: "triangle",
          symbolSize: 9,
          itemStyle: { color: "#ffd166" },
          tooltip: { trigger: "item" },
          z: 7,
        });
      }

      if (loPivots.length) {
        series.push({
          id: "piv_lo",
          name: `Piv L${selectedLevel} Low`,
          type: "scatter",
          data: loPivots,
          symbol: "triangle",
          symbolRotate: 180,
          symbolSize: 9,
          itemStyle: { color: "#7bdff2" },
          tooltip: { trigger: "item" },
          z: 7,
        });
      }
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

    return { xVals, series };
  }

  function extractClickPayload(params) {
    if (!params) return null;
    const data = params.data;
    if (data && typeof data === "object" && data.payload) return data.payload;
    if (Array.isArray(data) && data.length >= 3 && data[2] && typeof data[2] === "object") {
      return data[2];
    }
    return null;
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
        k2: safeNum(t.k2),
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
      const seenSeries = new Set();
      const legendSelected =
        chart &&
        chart.getOption &&
        chart.getOption().legend &&
        chart.getOption().legend[0] &&
        chart.getOption().legend[0].selected
          ? chart.getOption().legend[0].selected
          : null;

      // show prices
      params.forEach((p) => {
        const seriesId = p.seriesId || p.seriesName;
        const seriesKey = seriesId || p.seriesName || "";
        if (seenSeries.has(seriesKey)) return;
        seenSeries.add(seriesKey);

        // Defensive: if legend exists and this series is hidden, skip it.
        if (legendSelected && p.seriesName && legendSelected[p.seriesName] === false) return;

        const data = p.data;
        const yVal = Array.isArray(data) ? data[1] : data;
        const yText = yVal == null ? "" : Number(yVal).toFixed(2);

        if (
          seriesId === "mid" ||
          seriesId === "kal" ||
          seriesId === "bid" ||
          seriesId === "ask" ||
          seriesId === "k2"
        ) {
          if (!Number.isFinite(Number(yVal))) return;
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

      params.forEach((p) => {
        const seriesId = p.seriesId || p.seriesName;
        if (seriesId !== "piv_hi" && seriesId !== "piv_lo") return;
        const data = p.data;
        const yVal = Array.isArray(data) ? data[1] : data;
        const yText = yVal == null ? "" : Number(yVal).toFixed(2);
        const payload = Array.isArray(data) ? data[2] : null;
        if (!payload) return;
        const lvl = payload.level != null ? payload.level : "";
        const ptype = payload.ptype === "h" ? "high" : "low";
        const pivotNo = payload.pivotno != null ? payload.pivotno : "";
        extras.push(`Pivot â€“ ${ptype} L${lvl} #${pivotNo} px:${yText}`);
      });

      const k2Info = toFiniteNumber(info.k2);
      const hasK2InSeries = params.some((p) => (p.seriesId || p.seriesName) === "k2");
      if (!hasK2InSeries && k2Info != null) {
        html += `K2: ${k2Info.toFixed(2)}<br/>`;
      }

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

  function buildK2CandleTooltipFormatter(candles) {
    const byId = new Map();
    candles.forEach((c) => byId.set(String(c.id), c));

    return function (params) {
      if (!params || !params.length) return "";
      const p = params[0];
      const axisValue = p.axisValue != null ? String(p.axisValue) : "";
      const c = byId.get(axisValue) || {};
      const v = Array.isArray(p.data) ? p.data : [];
      const o = Number(v[0]);
      const cl = Number(v[1]);
      const lo = Number(v[2]);
      const hi = Number(v[3]);

      const fmt = (n) => (Number.isFinite(Number(n)) ? Number(n).toFixed(2) : "");
      const k2o = c.k2o != null ? Number(c.k2o).toFixed(2) : "";
      const k2c = c.k2c != null ? Number(c.k2c).toFixed(2) : "";

      let html = "";
      html += `<b>Candle #${axisValue}</b><br/>`;
      if (c.start_tick_id != null || c.end_tick_id != null) {
        html += `ticks: ${c.start_tick_id ?? ""} -> ${c.end_tick_id ?? ""}<br/>`;
      }
      if (c.start_ts) html += `start: ${c.start_ts}<br/>`;
      if (c.end_ts) html += `end: ${c.end_ts}<br/>`;
      html += `O: ${fmt(o)} H: ${fmt(hi)} L: ${fmt(lo)} C: ${fmt(cl)}<br/>`;
      html += `k2o: ${k2o} k2c: ${k2c}<br/>`;
      if (c.dir != null) html += `dir: ${c.dir}<br/>`;
      if (c.tick_count != null) html += `tick_count: ${c.tick_count}<br/>`;
      return html;
    };
  }

  function structureTs(row) {
    return row && (row.ts || row.repts || row.centerts || row.firstts || row.startts || null);
  }

  function structurePx(row) {
    const px = row && (
      row.px != null ? row.px :
      row.topprice != null ? row.topprice :
      row.highprice != null ? row.highprice :
      row.lowprice != null ? row.lowprice :
      null
    );
    return safeNum(px);
  }

  function buildStructureBounds() {
    const ys = [];
    const vis = state.visibility;

    if (vis.mid || vis.kal) {
      for (const t of state.ticks || []) {
        if (vis.mid) {
          const mid = safeNum(t && t.mid);
          if (mid != null) ys.push(mid);
        }
        if (vis.kal) {
          const kal = safeNum(t && t.kal);
          if (kal != null) ys.push(kal);
        }
      }
    }

    if (vis.piv) {
      for (const p of state.pivots || []) {
        const px = structurePx(p);
        if (px != null) ys.push(px);
      }
    }

    if (vis.tpiv) {
      for (const p of state.tpivots || []) {
        const px = structurePx(p);
        if (px != null) ys.push(px);
      }
    }

    if (vis.tzone) {
      for (const z of state.tzones || []) {
        const lo = safeNum(z && z.lowprice);
        const hi = safeNum(z && z.highprice);
        if (lo != null) ys.push(lo);
        if (hi != null) ys.push(hi);
      }
    }

    if (vis.tepisode) {
      for (const e of state.tepisodes || []) {
        const lo = safeNum(e && e.lowprice);
        const hi = safeNum(e && e.highprice);
        const top = safeNum(e && e.topprice);
        if (lo != null) ys.push(lo);
        if (hi != null) ys.push(hi);
        if (top != null) ys.push(top);
      }
    }

    if (vis.tconfirm) {
      for (const c of state.tconfirms || []) {
        const px = safeNum(c && c.anchorprice);
        if (px != null) ys.push(px);
      }
    }

    if (vis.tscore) {
      for (const s of state.tscores || []) {
        const px = safeNum(s && s.anchorprice);
        if (px != null) ys.push(px);
      }
    }

    if (!ys.length) return null;
    const minY = Math.min(...ys);
    const maxY = Math.max(...ys);
    const pad = Math.max(0.25, (maxY - minY) * 0.04);
    return { min: minY - pad, max: maxY + pad };
  }

  function buildRectSeries(id, name, rows, opts) {
    if (!rows.length) return null;
    const data = [];

    for (const row of rows) {
      const x0 = tsToMs(row[opts.startKey]);
      const x1 = tsToMs(row[opts.endKey]);
      const y0 = safeNum(row[opts.lowKey]);
      const y1 = safeNum(row[opts.highKey]);
      if (x0 == null || x1 == null || y0 == null || y1 == null) continue;
      data.push({
        value: [x0, y0, x1, y1],
        payload: row,
      });
    }

    if (!data.length) return null;

    return {
      id,
      name,
      type: "custom",
      coordinateSystem: "cartesian2d",
      renderItem(params, api) {
        const start = api.coord([api.value(0), api.value(1)]);
        const end = api.coord([api.value(2), api.value(3)]);
        const rect = echarts.graphic.clipRectByRect(
          {
            x: Math.min(start[0], end[0]),
            y: Math.min(start[1], end[1]),
            width: Math.max(1, Math.abs(end[0] - start[0])),
            height: Math.max(1, Math.abs(end[1] - start[1])),
          },
          {
            x: params.coordSys.x,
            y: params.coordSys.y,
            width: params.coordSys.width,
            height: params.coordSys.height,
          }
        );
        if (!rect) return null;
        return {
          type: "rect",
          shape: rect,
          style: api.style({
            fill: opts.fill,
            stroke: opts.stroke,
            lineWidth: 1,
          }),
        };
      },
      encode: { x: [0, 2], y: [1, 3] },
      data,
      z: opts.z || 1,
      silent: false,
      tooltip: { trigger: "item" },
    };
  }

  function buildScatterSeries(id, name, rows, opts) {
    if (!rows.length) return null;
    const data = [];
    for (const row of rows) {
      const ts = tsToMs(opts.tsAccessor(row));
      const px = safeNum(opts.pxAccessor(row));
      if (ts == null || px == null) continue;
      const point = {
        value: [ts, px],
        payload: row,
      };
      if (typeof opts.itemStyleAccessor === "function") {
        const itemStyle = opts.itemStyleAccessor(row);
        if (itemStyle) point.itemStyle = itemStyle;
      }
      if (typeof opts.symbolAccessor === "function") {
        const symbol = opts.symbolAccessor(row);
        if (symbol) point.symbol = symbol;
      }
      if (typeof opts.symbolRotateAccessor === "function") {
        const rotate = opts.symbolRotateAccessor(row);
        if (rotate != null) point.symbolRotate = rotate;
      }
      if (typeof opts.symbolSizeAccessor === "function") {
        const size = opts.symbolSizeAccessor(row);
        if (size != null) point.symbolSize = size;
      }
      data.push(point);
    }
    if (!data.length) return null;

    const baseItemStyle = opts.itemStyle || { color: opts.color || "#ffffff" };
    return {
      id,
      name,
      type: "scatter",
      data,
      symbol: opts.symbol || "circle",
      symbolRotate: opts.symbolRotate || 0,
      symbolSize: opts.symbolSize || 8,
      itemStyle: baseItemStyle,
      z: opts.z || 5,
      tooltip: { trigger: "item" },
    };
  }

  function buildStructureSeries() {
    const vis = state.visibility;
    const series = [];

    if (vis.mid && Array.isArray(state.ticks) && state.ticks.length) {
      const midData = state.ticks
        .map((t) => {
          const ts = tsToMs(t.ts);
          const mid = safeNum(t.mid);
          if (ts == null || mid == null) return null;
          return { value: [ts, mid], payload: t };
        })
        .filter(Boolean);
      if (midData.length) {
        series.push({
          id: "structure_mid",
          name: "Ticks",
          type: "line",
          data: midData,
          showSymbol: false,
          smooth: false,
          lineStyle: { width: 1, color: "#7aa2ff" },
          z: 3,
        });
      }
    }

    if (vis.kal && Array.isArray(state.ticks) && state.ticks.length) {
      const kalData = state.ticks
        .map((t) => {
          const ts = tsToMs(t.ts);
          const kal = safeNum(t.kal);
          if (ts == null || kal == null) return null;
          return { value: [ts, kal], payload: t };
        })
        .filter(Boolean);
      if (kalData.length) {
        series.push({
          id: "structure_kal",
          name: "Kal",
          type: "line",
          data: kalData,
          showSymbol: false,
          smooth: false,
          lineStyle: { width: 1.4, color: "#8ee3c8" },
          z: 4,
        });
      }
    }

    if (vis.tzone && Array.isArray(state.tzones) && state.tzones.length) {
      const zoneSeries = buildRectSeries("structure_tzone", "TZone", state.tzones, {
        startKey: "startts",
        endKey: "endts",
        lowKey: "lowprice",
        highKey: "highprice",
        fill: "rgba(86, 180, 233, 0.15)",
        stroke: "#56b4e9",
        z: 1,
      });
      if (zoneSeries) series.push(zoneSeries);
    }

    if (vis.tepisode && Array.isArray(state.tepisodes) && state.tepisodes.length) {
      const episodeSeries = buildRectSeries("structure_tepisode", "TEpisode", state.tepisodes, {
        startKey: "firstts",
        endKey: "lastts",
        lowKey: "lowprice",
        highKey: "highprice",
        fill: "rgba(255, 196, 61, 0.22)",
        stroke: "#ffc43d",
        z: 2,
      });
      if (episodeSeries) series.push(episodeSeries);

      const repSeries = buildScatterSeries("structure_tepisode_rep", "TEpisode Rep", state.tepisodes, {
        tsAccessor: (row) => row.repts,
        pxAccessor: (row) => row.topprice,
        symbol: "diamond",
        symbolSize: 10,
        color: "#ffd166",
        z: 7,
      });
      if (repSeries) series.push(repSeries);
    }

    if (vis.tconfirm && Array.isArray(state.tconfirms) && state.tconfirms.length) {
      const confirmConfigs = [
        {
          state: "confirmed",
          id: "structure_tconfirm_confirmed",
          name: "TConfirm Confirmed",
          symbol: "triangle",
          size: 11,
          color: "#4cc9a6",
          rotate: 0,
        },
        {
          state: "invalidated",
          id: "structure_tconfirm_invalidated",
          name: "TConfirm Invalidated",
          symbol: "diamond",
          size: 11,
          color: "#ff6b6b",
          rotate: 0,
        },
        {
          state: "unfinished",
          id: "structure_tconfirm_unfinished",
          name: "TConfirm Unfinished",
          symbol: "circle",
          size: 10,
          color: "#ffc43d",
          rotate: 0,
        },
      ];

      for (const cfg of confirmConfigs) {
        const rows = state.tconfirms.filter(
          (row) => String((row && row.confirmstate) || "").trim().toLowerCase() === cfg.state
        );
        const confirmSeries = buildScatterSeries(cfg.id, cfg.name, rows, {
          tsAccessor: (row) => row.anchorts,
          pxAccessor: (row) => row.anchorprice,
          symbol: cfg.symbol,
          symbolRotate: cfg.rotate,
          symbolSize: cfg.size,
          color: cfg.color,
          itemStyle: {
            color: cfg.color,
            borderColor: "#08101d",
            borderWidth: 1.2,
            opacity: 0.95,
          },
          z: 8,
        });
        if (confirmSeries) series.push(confirmSeries);
      }
    }

    if (vis.tscore && Array.isArray(state.tscores) && state.tscores.length) {
      const scoreSeries = buildScatterSeries("structure_tscore", "TScore", state.tscores, {
        tsAccessor: (row) => row.anchorts,
        pxAccessor: (row) => row.anchorprice,
        symbol: "circle",
        symbolSizeAccessor: (row) => {
          const total = safeNum(row && row.totalscore);
          if (total == null) return 10;
          return Math.max(8, Math.min(26, 8 + total * 0.18));
        },
        itemStyleAccessor: (row) => {
          const grade = String((row && row.scoregrade) || "").trim().toUpperCase();
          const colorByGrade = {
            A: "#f94144",
            B: "#f8961e",
            C: "#f9c74f",
            D: "#90be6d",
            F: "#577590",
          };
          const color = colorByGrade[grade] || "#8ecae6";
          return {
            color,
            opacity: 0.78,
            borderColor: "#ffffff",
            borderWidth: 1.1,
            shadowBlur: 10,
            shadowColor: color,
          };
        },
        z: 10,
      });
      if (scoreSeries) series.push(scoreSeries);
    }

    if (vis.piv && Array.isArray(state.pivots) && state.pivots.length) {
      const pivotConfigs = [
        { layer: "nano", ptype: "h", id: "piv_nano_hi", name: "Piv Nano High", symbol: "circle", size: 6, color: "#ffb703" },
        { layer: "nano", ptype: "l", id: "piv_nano_lo", name: "Piv Nano Low", symbol: "circle", size: 6, color: "#7bdff2" },
        { layer: "micro", ptype: "h", id: "piv_micro_hi", name: "Piv Micro High", symbol: "triangle", size: 8, color: "#fb8500" },
        { layer: "micro", ptype: "l", id: "piv_micro_lo", name: "Piv Micro Low", symbol: "triangle", size: 8, color: "#4cc9f0", rotate: 180 },
        { layer: "macro", ptype: "h", id: "piv_macro_hi", name: "Piv Macro High", symbol: "diamond", size: 10, color: "#ff6b6b" },
        { layer: "macro", ptype: "l", id: "piv_macro_lo", name: "Piv Macro Low", symbol: "diamond", size: 10, color: "#72efdd" },
      ];

      for (const cfg of pivotConfigs) {
        const rows = state.pivots.filter((p) => p && p.layer === cfg.layer && String(p.ptype || "").toLowerCase() === cfg.ptype);
        const s = buildScatterSeries(cfg.id, cfg.name, rows, {
          tsAccessor: (row) => row.ts,
          pxAccessor: (row) => row.px,
          symbol: cfg.symbol,
          symbolRotate: cfg.rotate || 0,
          symbolSize: cfg.size,
          color: cfg.color,
          z: 6,
        });
        if (s) series.push(s);
      }
    }

    if (vis.tpiv && Array.isArray(state.tpivots) && state.tpivots.length) {
      const highRows = state.tpivots.filter((p) => String((p && (p.ptype || p.dir || "")) || "").toLowerCase().startsWith("h") || String((p && p.dir) || "").toLowerCase() === "top");
      const lowRows = state.tpivots.filter((p) => !highRows.includes(p));

      const highSeries = buildScatterSeries("structure_tpiv_hi", "TPivots High", highRows, {
        tsAccessor: (row) => structureTs(row),
        pxAccessor: (row) => structurePx(row),
        symbol: "pin",
        symbolSize: 14,
        color: "#ff4d6d",
        z: 8,
      });
      if (highSeries) series.push(highSeries);

      const lowSeries = buildScatterSeries("structure_tpiv_lo", "TPivots Low", lowRows, {
        tsAccessor: (row) => structureTs(row),
        pxAccessor: (row) => structurePx(row),
        symbol: "pin",
        symbolRotate: 180,
        symbolSize: 14,
        color: "#5eead4",
        z: 8,
      });
      if (lowSeries) series.push(lowSeries);
    }

    return series;
  }

  function buildStructureTooltipFormatter() {
    return function (params) {
      const payload = extractClickPayload(params);
      if (!payload) return params && params.seriesName ? params.seriesName : "";

      const ts =
        payload.anchorts ||
        payload.ts ||
        payload.repts ||
        payload.centerts ||
        payload.firstts ||
        payload.startts ||
        "";
      const px =
        payload.anchorprice != null ? payload.anchorprice :
        payload.px != null ? payload.px :
        payload.topprice != null ? payload.topprice :
        payload.highprice != null ? payload.highprice :
        "";

      let html = `<b>${params.seriesName || ""}</b><br/>`;
      if (payload.id != null) html += `id: ${payload.id}<br/>`;
      if (payload.tconfirmid != null) html += `tconfirmid: ${payload.tconfirmid}<br/>`;
      if (payload.tepisodeid != null) html += `tepisodeid: ${payload.tepisodeid}<br/>`;
      if (ts) html += `ts: ${ts}<br/>`;
      if (px !== "") html += `px: ${Number(px).toFixed ? Number(px).toFixed(2) : px}<br/>`;
      if (payload.layer) html += `layer: ${payload.layer}<br/>`;
      if (payload.confirmstate) html += `state: ${payload.confirmstate}<br/>`;
      if (payload.totalscore != null) html += `score: ${Number(payload.totalscore).toFixed(1)} (${payload.scoregrade || ""})<br/>`;
      if (payload.reason) html += `reason: ${payload.reason}<br/>`;
      if (payload.zonepos) html += `zonepos: ${payload.zonepos}<br/>`;
      if (payload.pivotcount != null) html += `pivotcount: ${payload.pivotcount}<br/>`;
      return html;
    };
  }

  function renderStructure() {
    const series = buildStructureSeries();
    const bounds = buildStructureBounds();

    const option = {
      animation: false,
      grid: { left: 55, right: 24, top: 22, bottom: 58 },
      tooltip: {
        trigger: "item",
        confine: true,
        formatter: buildStructureTooltipFormatter(),
      },
      legend: {
        top: 0,
        textStyle: { color: "#cdd6f4" },
      },
      xAxis: {
        type: "time",
        axisLabel: { formatter: (value) => toISO(value).slice(11, 19) },
      },
      yAxis: bounds ? { type: "value", scale: true, min: bounds.min, max: bounds.max } : { type: "value", scale: true },
      dataZoom: [
        { type: "inside", xAxisIndex: 0, filterMode: "none" },
        { type: "slider", xAxisIndex: 0, filterMode: "none" },
      ],
      series,
    };

    if (!state.hasInit) {
      chart.setOption(option, { notMerge: true, lazyUpdate: true });
    } else {
      const oldOpt = chart.getOption();
      const dz = oldOpt && oldOpt.dataZoom ? oldOpt.dataZoom[0] : null;
      chart.setOption(option, { notMerge: true, lazyUpdate: true });
      if (dz && dz.start != null && dz.end != null) {
        chart.dispatchAction({ type: "dataZoom", start: dz.start, end: dz.end });
      }
    }
    state.hasInit = true;
    notifyWindowChange();
  }

  function renderK2Candles() {
    if (!chart) return;

    const candles = Array.isArray(state.k2Candles) ? state.k2Candles : [];
    const xVals = candles.map((c) => String(c.id));
    const data = candles.map((c) => [
      toFiniteNumber(c.o),
      toFiniteNumber(c.c),
      toFiniteNumber(c.l),
      toFiniteNumber(c.h),
    ]);

    chart.setOption(
      {
        animation: false,
        grid: { left: 50, right: 20, top: 25, bottom: 60 },
        tooltip: {
          trigger: "axis",
          axisPointer: { type: "cross" },
          formatter: buildK2CandleTooltipFormatter(candles),
        },
        xAxis: {
          type: "category",
          data: xVals,
          boundaryGap: true,
        },
        yAxis: {
          type: "value",
          scale: true,
        },
        dataZoom: [
          { type: "inside", xAxisIndex: 0, filterMode: "none" },
          { type: "slider", xAxisIndex: 0, filterMode: "none" },
        ],
        series: [
          {
            id: "k2_candles",
            name: "K2 Flip Candles",
            type: "candlestick",
            data,
            itemStyle: {
              color: "#26a69a",
              color0: "#ef5350",
              borderColor: "#26a69a",
              borderColor0: "#ef5350",
            },
          },
        ],
      },
      { notMerge: true, lazyUpdate: true }
    );
  }

  function detectK2Availability(ticks) {
    if (!Array.isArray(ticks) || !ticks.length) return false;
    for (const t of ticks) {
      if (safeNum(t && t.k2) != null) return true;
    }
    return false;
  }

  function inferLayerAvailabilityFromTicks(ticks) {
    return {
      mid: true,
      bid: true,
      ask: true,
      kal: true,
      k2: detectK2Availability(ticks),
    };
  }

  function getLayerAvailability() {
    return inferLayerAvailabilityFromTicks(state.ticks);
  }

  function refreshLayerAvailability() {
    const nextK2 = detectK2Availability(state.ticks);
    const prevK2 = !!(state.layerAvailability && state.layerAvailability.k2);

    state.layerAvailability.k2 = nextK2;

    if (prevK2 !== nextK2 && typeof state.layerAvailabilityHandler === "function") {
      state.layerAvailabilityHandler(getLayerAvailability());
    }
  }

  function setLayerAvailabilityHandler(fn) {
    state.layerAvailabilityHandler = typeof fn === "function" ? fn : null;
    if (state.layerAvailabilityHandler) {
      state.layerAvailabilityHandler(getLayerAvailability());
    }
  }

  function render() {
    if (!chart) return;
    if (state.mode === "k2candles") {
      renderK2Candles();
      return;
    }
    if (state.mode === "structure") {
      renderStructure();
      return;
    }

    refreshLayerAvailability();

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
          animation: false,
          xAxis: {
            type: "category",
            data: xVals,
            axisLabel: { formatter: (v) => String(v).slice(11, 19) },
          },
          series,
          ...yAxisPatch,
        },
        { notMerge: false, lazyUpdate: true, replaceMerge: ["series"] }
      );

      if (dz && dz.start != null && dz.end != null) {
        chart.dispatchAction({ type: "dataZoom", start: dz.start, end: dz.end });
      }
    }

    notifyWindowChange();
  }

  function handleDataZoom() {
    if (state.mode === "structure") return;
    recomputeYFromVisibleWindow(chart, state);
  }

  function applyTickWindowData(mode, data) {
    const payload = data || {};
    state.mode = mode;
    state.ticks = Array.isArray(payload.ticks) ? payload.ticks : [];
    state.pivots = Array.isArray(payload.pivots) ? payload.pivots : [];
    state.tpivots = [];
    state.tzones = [];
    state.tepisodes = [];
    state.tconfirms = [];
    state.tscores = [];
    state.trulehits = [];

    if (state.ticks.length) state.lastTickId = state.ticks[state.ticks.length - 1].id;
    else state.lastTickId = null;
  }

  async function loadLiveOnce(limit) {
    const lim = limit != null ? Number(limit) : state.liveLimit;
    const url = `/api/live_window?limit=${encodeURIComponent(lim)}`;

    const res = await fetch(url);
    if (!res.ok) throw new Error(`live_window failed: ${res.status}`);
    const data = await res.json();

    applyTickWindowData("live", data);
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
        const res = await fetch(`/api/live_window?limit=${encodeURIComponent(state.liveLimit)}`);
        if (!res.ok) return;

        const d = await res.json();
        const ticks = Array.isArray(d.ticks) ? d.ticks : [];
        const pivots = Array.isArray(d.pivots) ? d.pivots : [];
        const last = ticks.length ? ticks[ticks.length - 1] : null;
        const lastId = last && last.id != null ? Number(last.id) : null;
        if (!lastId) return;

        applyTickWindowData("live", { ticks, pivots });
        state.lastTickId = lastId;
        render();
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

    applyTickWindowData("review", data);
    render();
    return data;
  }

  function setReviewData(data) {
    applyTickWindowData("review", data);
    render();
  }

  // NEW: inject ticks directly (used by segLines review page)
  function setTicks(ticks) {
    state.mode = "review";
    state.ticks = Array.isArray(ticks) ? ticks : [];
    state.pivots = [];
    state.tpivots = [];
    state.tzones = [];
    state.tepisodes = [];
    state.tconfirms = [];
    state.tscores = [];
    state.trulehits = [];
    if (state.ticks.length) state.lastTickId = state.ticks[state.ticks.length - 1].id;
    else state.lastTickId = null;
    render();
  }

  function setStructureData(payload) {
    const data = payload || {};
    state.mode = "structure";
    state.ticks = Array.isArray(data.ticks) ? data.ticks : [];
    state.pivots = Array.isArray(data.pivots) ? data.pivots : [];
    state.tpivots = Array.isArray(data.tpivots) ? data.tpivots : [];
    state.tzones = Array.isArray(data.tzone) ? data.tzone : [];
    state.tepisodes = Array.isArray(data.tepisode) ? data.tepisode : [];
    state.tconfirms = Array.isArray(data.tconfirm) ? data.tconfirm : [];
    state.tscores = Array.isArray(data.tscore) ? data.tscore : [];
    state.trulehits = Array.isArray(data.trulehit) ? data.trulehit : [];
    state.lastTickId = state.ticks.length ? state.ticks[state.ticks.length - 1].id : null;
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

  function setPivotLevel(level) {
    const next = Number(level);
    state.pivotLevel = next >= 1 && next <= 3 ? next : 1;
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

  function setK2Candles(candles) {
    state.mode = "k2candles";
    state.k2Candles = Array.isArray(candles) ? candles : [];
    render();
  }

  function setClickHandler(fn) {
    state.clickHandler = typeof fn === "function" ? fn : null;
  }

  function handleChartClick(params) {
    if (!state.clickHandler) return;
    const payload = extractClickPayload(params);
    if (!payload) return;
    state.clickHandler({
      seriesId: params.seriesId || "",
      seriesName: params.seriesName || "",
      payload,
    });
  }

  function resetZoom() {
    if (!chart) return;
    chart.dispatchAction({ type: "dataZoom", start: 0, end: 100 });
  }

  return {
    init(domId) {
      const c = ensureChart(domId);
      if (!c) return;

      c.off && c.off("dataZoom");
      c.on("dataZoom", handleDataZoom);
      c.off && c.off("click");
      c.on("click", handleChartClick);
    },
    loadWindow,
    startLive,
    stopLive,
    setVisibility,
    setWindowChangeHandler,
    loadLiveOnce,
    setPivotLevel,

    // new API
    setReviewData,
    setTicks,
    setStructureData,
    setSegLines,
    setSegLinesVisibility,
    setK2Candles,
    setClickHandler,
    resetZoom,

    setLayerAvailabilityHandler,
    getLayerAvailability,
    inferLayerAvailabilityFromTicks,
  };
})();
