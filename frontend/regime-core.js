// frontend/regime-core.js
// Regime page: full window load for selected segm + segLine range.

(() => {
  const state = {
    segmId: null,
    segmLabel: "",
    allLines: [],
    rangeLines: [],
    ticks: [],
    range: null,

    showMid: false,
    showKal: true,
    showBid: false,
    showAsk: false,
    showSegLines: false,
    showLegs: false,
    showZig: true,

    zigPivots: [],
  };

  let chart = null;
  let tickById = new Map();
  let legsByLine = new Map();
  let zoomPending = false;

  function $(id) { return document.getElementById(id); }

  async function fetchJSON(url, opts) {
    const res = await fetch(url, opts);
    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      throw new Error(`HTTP ${res.status} ${res.statusText} for ${url}\n${txt}`);
    }
    return res.json();
  }

  function setToggle(btn, on) {
    btn.classList.toggle("on", !!on);
  }

  function pad2(n) {
    return String(n).padStart(2, "0");
  }

  function formatDateTime(ts) {
    if (!ts) return { date: "", time: "" };
    const d = new Date(ts);
    if (Number.isNaN(d.getTime())) return { date: "", time: "" };
    const date = `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`;
    const ms = d.getMilliseconds();
    const timeBase = `${pad2(d.getHours())}:${pad2(d.getMinutes())}:${pad2(d.getSeconds())}`;
    const time = ms ? `${timeBase}.${String(ms).padStart(3, "0")}` : timeBase;
    return { date, time };
  }

  function lineValueAt(line, tickId) {
    if (!line || tickId == null) return null;
    const x0 = Number(line.start_tick_id);
    const x1 = Number(line.end_tick_id);
    if (!Number.isFinite(x0) || !Number.isFinite(x1) || x1 === x0) return null;
    if (tickId < Math.min(x0, x1) || tickId > Math.max(x0, x1)) return null;

    const slope = line.slope != null ? Number(line.slope) : null;
    const intercept = line.intercept != null ? Number(line.intercept) : null;
    if (slope != null && intercept != null) {
      return slope * tickId + intercept;
    }

    const y0 = Number(line.start_price);
    const y1 = Number(line.end_price);
    if (!Number.isFinite(y0) || !Number.isFinite(y1)) return null;
    const t = (tickId - x0) / (x1 - x0);
    return y0 + (y1 - y0) * t;
  }

  async function loadSegmList() {
    const segms = await fetchJSON("/api/regime/segms");
    const sel = $("segmSelect");
    sel.innerHTML = "";

    for (const s of segms) {
      const segmId = s.segm_id ?? s.id ?? s.segmId;
      const date = s.date ?? "";
      const opt = document.createElement("option");
      opt.value = String(segmId);
      opt.textContent = `${date} (#${segmId})`;
      sel.appendChild(opt);
    }

    if (segms.length) {
      state.segmId = parseInt(sel.value, 10);
    }
  }

  async function loadLinesForSegm() {
    if (!state.segmId) return;
    const data = await fetchJSON(`/api/regime/segm/${state.segmId}/lines`);
    const lines = Array.isArray(data) ? data : (data.lines ?? []);
    state.allLines = lines;

    const sel = $("lineSelect");
    sel.innerHTML = "";
    for (const ln of lines) {
      const opt = document.createElement("option");
      opt.value = String(ln.id);
      opt.textContent = `L${ln.id} ${ln.start_tick_id} -> ${ln.end_tick_id}`;
      sel.appendChild(opt);
    }

    if (lines.length) {
      sel.value = String(lines[0].id);
    }
  }

  async function loadLegsForSegm() {
    if (!state.segmId) return;
    try {
      const data = await fetchJSON(`/api/regime/legs?segm_id=${state.segmId}`);
      const legs = Array.isArray(data) ? data : (data.legs ?? []);
      legsByLine = new Map();
      for (const l of legs) {
        const id = Number(l.segline_id ?? l.segLine_id);
        if (!Number.isFinite(id)) continue;
        legsByLine.set(id, l);
      }
    } catch (e) {
      console.warn("legs load failed", e);
      legsByLine = new Map();
    }
  }

  async function loadZigPivots() {
    if (!state.segmId) return;
    try {
      const data = await fetchJSON(`/api/regime/segm/${state.segmId}/zig_pivots`);
      const pivots = Array.isArray(data) ? data : (data.pivots ?? []);
      state.zigPivots = pivots;
    } catch (e) {
      console.warn("zig pivots load failed", e);
      state.zigPivots = [];
    }
  }

  function buildTickCache() {
    state.tickIds = [];
    state.tickSeries = { mid: [], kal: [], bid: [], ask: [] };
    state.tickValues = { mid: [], kal: [], bid: [], ask: [] };

    for (const t of state.ticks) {
      const id = Number(t.id ?? t.tick_id);
      if (!Number.isFinite(id)) continue;
      state.tickIds.push(id);

      const mid = t.mid != null ? Number(t.mid) : null;
      const kal = t.kal != null ? Number(t.kal) : null;
      const bid = t.bid != null ? Number(t.bid) : null;
      const ask = t.ask != null ? Number(t.ask) : null;

      state.tickValues.mid.push(Number.isFinite(mid) ? mid : null);
      state.tickValues.kal.push(Number.isFinite(kal) ? kal : null);
      state.tickValues.bid.push(Number.isFinite(bid) ? bid : null);
      state.tickValues.ask.push(Number.isFinite(ask) ? ask : null);

      if (Number.isFinite(mid)) state.tickSeries.mid.push([id, mid]);
      if (Number.isFinite(kal)) state.tickSeries.kal.push([id, kal]);
      if (Number.isFinite(bid)) state.tickSeries.bid.push([id, bid]);
      if (Number.isFinite(ask)) state.tickSeries.ask.push([id, ask]);
    }
  }

  function buildTickData(field) {
    return (state.tickSeries && state.tickSeries[field]) ? state.tickSeries[field] : [];
  }

  function buildSegLineSeries(selectedId) {
    if (!state.showSegLines || !state.rangeLines.length) return [];

    const selId = selectedId != null ? Number(selectedId) : null;

    const series = [];
    for (const ln of state.rangeLines) {
      const isPrimary = selId != null && Number(ln.id) === selId;
      const a = [Number(ln.start_tick_id), Number(ln.start_price)];
      const b = [Number(ln.end_tick_id), Number(ln.end_price)];
      if (!Number.isFinite(a[0]) || !Number.isFinite(b[0])) continue;

      series.push({
        id: `segline_${ln.id}`,
        name: isPrimary ? "SegLine (primary)" : "SegLine",
        type: "line",
        data: [a, b],
        showSymbol: false,
        lineStyle: isPrimary
          ? { width: 4, opacity: 0.95, color: "#ffd54a" }
          : { width: 2, opacity: 0.65 },
        silent: true,
        z: isPrimary ? 6 : 5,
      });
    }
    return series;
  }

  function buildLegSeries() {
    if (!state.showLegs || !state.rangeLines.length || !legsByLine.size) return [];

    const out = [];
    const addSeg = (segLineId, label, p1, p2) => {
      if (!p1 || !p2) return;
      if (!Number.isFinite(p1[0]) || !Number.isFinite(p2[0])) return;
      if (!Number.isFinite(p1[1]) || !Number.isFinite(p2[1])) return;
      out.push({
        id: `leg_${segLineId}_${label}`,
        name: "Legs",
        type: "line",
        data: [p1, p2],
        showSymbol: false,
        lineStyle: { width: 1, opacity: 0.8, type: "dashed", color: "#6bd4ff" },
        silent: true,
        z: 4,
      });
    };

    for (const ln of state.rangeLines) {
      const leg = legsByLine.get(Number(ln.id));
      if (!leg || !leg.has_b || !leg.has_c) continue;

      const a = [Number(leg.a_tick_id), Number(leg.a_kal)];
      const b = [Number(leg.b_tick_id), Number(leg.b_kal)];
      const c = [Number(leg.c_tick_id), Number(leg.c_kal)];
      const d = leg.has_d ? [Number(leg.d_tick_id), Number(leg.d_kal)] : null;

      addSeg(ln.id, "ab", a, b);
      addSeg(ln.id, "bc", b, c);
      if (d) addSeg(ln.id, "cd", c, d);
    }

    return out;
  }

  function buildZigSeries() {
    if (!state.showZig || !state.zigPivots.length) return [];

    const sorted = [...state.zigPivots].sort((a, b) => {
      const ia = a.pivot_index ?? a.pivotIndex ?? 0;
      const ib = b.pivot_index ?? b.pivotIndex ?? 0;
      if (ia !== ib) return ia - ib;
      const ta = Number(a.tick_id ?? a.tickId) || 0;
      const tb = Number(b.tick_id ?? b.tickId) || 0;
      return ta - tb;
    });

    const pts = [];
    for (const p of sorted) {
      const x = Number(p.tick_id ?? p.tickId);
      const y = p.price;
      if (!Number.isFinite(x) || y == null) continue;
      pts.push([x, Number(y)]);
    }

    if (pts.length < 2) return [];
    return [{
      name: "Zig",
      type: "line",
      data: pts,
      showSymbol: true,
      symbolSize: 6,
      lineStyle: { width: 2.0, opacity: 0.9, color: "#ff9f40" },
      z: 4,
    }];
  }

  function lowerBound(arr, target) {
    let lo = 0;
    let hi = arr.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (arr[mid] < target) lo = mid + 1;
      else hi = mid;
    }
    return lo;
  }

  function upperBound(arr, target) {
    let lo = 0;
    let hi = arr.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (arr[mid] <= target) lo = mid + 1;
      else hi = mid;
    }
    return lo;
  }

  function getZoomWindow() {
    if (!chart || !state.tickIds || !state.tickIds.length) return null;
    const opt = chart.getOption();
    const dz = opt && opt.dataZoom ? opt.dataZoom[0] : null;
    if (!dz) return null;

    let x0 = null;
    let x1 = null;
    if (dz.startValue != null && dz.endValue != null) {
      x0 = Number(dz.startValue);
      x1 = Number(dz.endValue);
    } else {
      const startPct = (dz.start != null ? dz.start : 0) / 100;
      const endPct = (dz.end != null ? dz.end : 100) / 100;
      const lastIdx = state.tickIds.length - 1;
      const i0 = Math.max(0, Math.floor(startPct * lastIdx));
      const i1 = Math.max(0, Math.ceil(endPct * lastIdx));
      x0 = state.tickIds[i0];
      x1 = state.tickIds[i1];
    }

    if (!Number.isFinite(x0) || !Number.isFinite(x1)) return null;
    if (x0 > x1) [x0, x1] = [x1, x0];
    return { x0, x1 };
  }

  function includeLineMinMax(bounds, x0, x1, p1, p2) {
    const xA = Number(p1[0]);
    const yA = Number(p1[1]);
    const xB = Number(p2[0]);
    const yB = Number(p2[1]);
    if (!Number.isFinite(xA) || !Number.isFinite(yA) || !Number.isFinite(xB) || !Number.isFinite(yB)) return;

    const segMin = Math.min(xA, xB);
    const segMax = Math.max(xA, xB);
    if (x1 < segMin || x0 > segMax) return;

    if (xA === xB) {
      bounds.min = bounds.min == null ? Math.min(yA, yB) : Math.min(bounds.min, yA, yB);
      bounds.max = bounds.max == null ? Math.max(yA, yB) : Math.max(bounds.max, yA, yB);
      return;
    }

    const leftX = Math.max(x0, segMin);
    const rightX = Math.min(x1, segMax);
    const slope = (yB - yA) / (xB - xA);
    const yL = yA + slope * (leftX - xA);
    const yR = yA + slope * (rightX - xA);

    bounds.min = bounds.min == null ? Math.min(yL, yR) : Math.min(bounds.min, yL, yR);
    bounds.max = bounds.max == null ? Math.max(yL, yR) : Math.max(bounds.max, yL, yR);
  }

  function computeMinMaxForWindow(x0, x1) {
    const ids = state.tickIds || [];
    if (!ids.length) return null;

    const i0 = lowerBound(ids, x0);
    const i1 = upperBound(ids, x1) - 1;
    if (i0 > i1) return null;

    const keys = [];
    if (state.showMid) keys.push("mid");
    if (state.showKal) keys.push("kal");
    if (state.showBid) keys.push("bid");
    if (state.showAsk) keys.push("ask");

    const bounds = { min: null, max: null };
    const span = i1 - i0 + 1;
    let step = 1;
    if (span > 200000) step = Math.ceil(span / 50000);
    else if (span > 80000) step = Math.ceil(span / 30000);

    for (const key of keys) {
      const arr = state.tickValues[key] || [];
      for (let i = i0; i <= i1; i += step) {
        const v = arr[i];
        if (v == null || !Number.isFinite(v)) continue;
        bounds.min = bounds.min == null ? v : Math.min(bounds.min, v);
        bounds.max = bounds.max == null ? v : Math.max(bounds.max, v);
      }
      if (i1 >= i0) {
        const v = arr[i1];
        if (v != null && Number.isFinite(v)) {
          bounds.min = bounds.min == null ? v : Math.min(bounds.min, v);
          bounds.max = bounds.max == null ? v : Math.max(bounds.max, v);
        }
      }
    }

    if (state.showSegLines && state.rangeLines.length) {
      for (const ln of state.rangeLines) {
        const a = [Number(ln.start_tick_id), Number(ln.start_price)];
        const b = [Number(ln.end_tick_id), Number(ln.end_price)];
        includeLineMinMax(bounds, x0, x1, a, b);
      }
    }

    if (state.showLegs && legsByLine.size) {
      for (const ln of state.rangeLines) {
        const leg = legsByLine.get(Number(ln.id));
        if (!leg || !leg.has_b || !leg.has_c) continue;
        const a = [Number(leg.a_tick_id), Number(leg.a_kal)];
        const b = [Number(leg.b_tick_id), Number(leg.b_kal)];
        const c = [Number(leg.c_tick_id), Number(leg.c_kal)];
        includeLineMinMax(bounds, x0, x1, a, b);
        includeLineMinMax(bounds, x0, x1, b, c);
        if (leg.has_d) {
          const d = [Number(leg.d_tick_id), Number(leg.d_kal)];
          includeLineMinMax(bounds, x0, x1, c, d);
        }
      }
    }

    if (bounds.min == null || bounds.max == null) return null;
    return bounds;
  }

  function updateYAxisForZoom() {
    if (!chart) return;
    const win = getZoomWindow();
    if (!win) return;
    const bounds = computeMinMaxForWindow(win.x0, win.x1);
    if (!bounds) return;

    const range = bounds.max - bounds.min;
    const pad = Math.max(0.3, range * 0.03);
    const yMin = bounds.min - pad;
    const yMax = bounds.max + pad;
    if (!Number.isFinite(yMin) || !Number.isFinite(yMax)) return;

    chart.setOption(
      {
        yAxis: { min: yMin, max: yMax },
      },
      { notMerge: false, lazyUpdate: true }
    );
  }

  function scheduleRescale() {
    if (zoomPending) return;
    zoomPending = true;
    requestAnimationFrame(() => {
      zoomPending = false;
      updateYAxisForZoom();
    });
  }

  function renderChart() {
    if (!chart) return;

    const selId = Number($("lineSelect").value || 0) || null;

    const series = [];
    if (state.showMid) {
      series.push({
        name: "Mid",
        type: "scatter",
        data: buildTickData("mid"),
        symbolSize: 2,
        large: true,
        largeThreshold: 20000,
        itemStyle: { opacity: 0.8 },
        z: 3,
      });
    }
    if (state.showKal) {
      series.push({
        name: "Kal",
        type: "line",
        data: buildTickData("kal"),
        showSymbol: false,
        lineStyle: { width: 2.0, opacity: 0.9 },
        z: 2,
      });
    }
    if (state.showBid) {
      series.push({
        name: "Bid",
        type: "line",
        data: buildTickData("bid"),
        showSymbol: false,
        lineStyle: { width: 1.0, opacity: 0.7 },
        z: 1,
      });
    }
    if (state.showAsk) {
      series.push({
        name: "Ask",
        type: "line",
        data: buildTickData("ask"),
        showSymbol: false,
        lineStyle: { width: 1.0, opacity: 0.7 },
        z: 1,
      });
    }

    const segLineSeries = buildSegLineSeries(selId);
    const legSeries = buildLegSeries();
    const zigSeries = buildZigSeries();
    const allSeries = series.concat(segLineSeries, legSeries, zigSeries);

    let minX = null;
    let maxX = null;
    for (const v of state.tickIds || []) {
      if (!Number.isFinite(v)) continue;
      if (minX == null || v < minX) minX = v;
      if (maxX == null || v > maxX) maxX = v;
    }

    chart.setOption({
      animation: false,
      grid: { left: 55, right: 25, top: 20, bottom: 60 },
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "cross" },
        formatter: (params) => {
          if (!params || !params.length) return "";
          const axisValue = params[0].axisValue;
          const tickId = Number(axisValue);
          const tick = tickById.get(tickId);
          const dt = formatDateTime(tick && (tick.ts ?? tick.timestamp));

          const lines = [];
          lines.push(`<b>id:</b> ${Number.isFinite(tickId) ? tickId : ""}`);
          if (dt.date) lines.push(`<b>date:</b> ${dt.date}`);
          if (dt.time) lines.push(`<b>time:</b> ${dt.time}`);

          if (tick) {
            if (tick.mid != null) lines.push(`<b>mid:</b> ${Number(tick.mid).toFixed(4)}`);
            if (tick.kal != null) lines.push(`<b>kal:</b> ${Number(tick.kal).toFixed(4)}`);
            if (state.showBid && tick.bid != null) lines.push(`<b>bid:</b> ${Number(tick.bid).toFixed(4)}`);
            if (state.showAsk && tick.ask != null) lines.push(`<b>ask:</b> ${Number(tick.ask).toFixed(4)}`);
          }

          const primary = state.rangeLines.find((l) => Number(l.id) === selId);
          const lv = lineValueAt(primary, tickId);
          lines.push(`<b>line:</b> ${lv == null ? "n/a" : Number(lv).toFixed(4)}`);

          return lines.join("<br/>");
        },
      },
      xAxis: {
        type: "value",
        min: minX != null ? minX : null,
        max: maxX != null ? maxX : null,
        axisLabel: { formatter: (v) => Math.round(v) },
        splitLine: { show: true, lineStyle: { color: "rgba(26,43,85,35)" } },
      },
      yAxis: {
        type: "value",
        scale: true,
        axisLabel: { formatter: (v) => Math.round(v) },
        splitLine: { show: true, lineStyle: { color: "rgba(26,43,85,35)" } },
      },
      dataZoom: [
        { type: "inside", xAxisIndex: 0, filterMode: "none" },
        { type: "slider", xAxisIndex: 0, height: 18, bottom: 10 },
      ],
      series: allSeries,
    }, { notMerge: true });

    updateYAxisForZoom();
  }

  function renderMeta() {
    const el = $("meta");
    if (!el) return;
    const segm = state.segmId != null ? `segm:${state.segmId}` : "segm:-";
    const label = state.segmLabel ? ` ${state.segmLabel}` : "";
    const lineInfo = state.range ? ` lines:${state.range.line_count}` : "";
    const tickInfo = state.range ? ` ticks:${state.range.tick_count}` : "";
    el.textContent = `${segm}${label}${lineInfo}${tickInfo}`;
  }

  function showError(msg) {
    const el = $("meta");
    if (el) el.textContent = String(msg || "Error");
  }

  async function loadWindow() {
    const segmId = state.segmId;
    const lineId = parseInt($("lineSelect").value, 10);
    const lineCount = parseInt($("lineCount").value, 10) || 1;
    if (!segmId || !lineId) return;

    const payload = {
      segm_id: segmId,
      start_segline_id: lineId,
      line_count: lineCount,
    };

    const data = await fetchJSON("/api/regime/window", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    state.segmLabel = data.segm ? data.segm.label : "";
    state.range = data.range || null;
    state.ticks = Array.isArray(data.ticks) ? data.ticks : [];
    state.rangeLines = Array.isArray(data.seglines) ? data.seglines : [];

    tickById = new Map();
    for (const t of state.ticks) {
      const id = Number(t.id ?? t.tick_id);
      if (Number.isFinite(id)) tickById.set(id, t);
    }

    buildTickCache();

    renderChart();
    renderMeta();
  }

  function moveLine(delta) {
    const sel = $("lineSelect");
    const curId = parseInt(sel.value, 10);
    const idx = state.allLines.findIndex((l) => Number(l.id) === curId);
    if (idx < 0) return;
    const next = state.allLines[idx + delta];
    if (!next) return;
    sel.value = String(next.id);
    loadWindow().catch((e) => alert(String(e.message ?? e)));
  }

  function bindUI() {
    $("segmSelect").addEventListener("change", async () => {
      state.segmId = parseInt($("segmSelect").value, 10);
      applyDefaultToggles();
      await loadLinesForSegm();
      await loadLegsForSegm();
      await loadZigPivots();
      try {
        await loadWindow();
      } catch (e) {
        console.error(e);
        showError(e.message ?? String(e));
      }
    });

    $("lineSelect").addEventListener("change", () => {
      loadWindow().catch((e) => {
        console.error(e);
        showError(e.message ?? String(e));
      });
    });

    $("btnLoad").addEventListener("click", () => {
      loadWindow().catch((e) => {
        console.error(e);
        showError(e.message ?? String(e));
      });
    });

    $("btnPrev").addEventListener("click", () => moveLine(-1));
    $("btnNext").addEventListener("click", () => moveLine(1));

    const tMid = $("toggleMid");
    const tKal = $("toggleKal");
    const tBid = $("toggleBid");
    const tAsk = $("toggleAsk");
    const tLines = $("toggleSegLines");
    const tLegs = $("toggleLegs");
    const tZig = $("toggleZig");

    tMid.addEventListener("click", () => { state.showMid = !state.showMid; setToggle(tMid, state.showMid); renderChart(); });
    tKal.addEventListener("click", () => { state.showKal = !state.showKal; setToggle(tKal, state.showKal); renderChart(); });
    tBid.addEventListener("click", () => { state.showBid = !state.showBid; setToggle(tBid, state.showBid); renderChart(); });
    tAsk.addEventListener("click", () => { state.showAsk = !state.showAsk; setToggle(tAsk, state.showAsk); renderChart(); });
    tLines.addEventListener("click", () => { state.showSegLines = !state.showSegLines; setToggle(tLines, state.showSegLines); renderChart(); });
    tLegs.addEventListener("click", () => { state.showLegs = !state.showLegs; setToggle(tLegs, state.showLegs); renderChart(); });
    tZig.addEventListener("click", () => { state.showZig = !state.showZig; setToggle(tZig, state.showZig); renderChart(); });
  }

  function initChart() {
    chart = echarts.init($("chart"));
    window.addEventListener("resize", () => chart && chart.resize());
    chart.on("dataZoom", () => scheduleRescale());
  }

  function applyDefaultToggles() {
    state.showMid = false;
    state.showKal = true;
    state.showBid = false;
    state.showAsk = false;
    state.showSegLines = false;
    state.showLegs = false;
    state.showZig = true;

    setToggle($("toggleMid"), state.showMid);
    setToggle($("toggleKal"), state.showKal);
    setToggle($("toggleBid"), state.showBid);
    setToggle($("toggleAsk"), state.showAsk);
    setToggle($("toggleSegLines"), state.showSegLines);
    setToggle($("toggleLegs"), state.showLegs);
    setToggle($("toggleZig"), state.showZig);
  }

  async function init() {
    initChart();
    applyDefaultToggles();

    await loadSegmList();
    await loadLinesForSegm();
    await loadLegsForSegm();
    await loadZigPivots();
    const lineCount = $("lineCount");
    if (lineCount && !lineCount.value) lineCount.value = "1";
    bindUI();
    try {
      await loadWindow();
    } catch (e) {
      console.error(e);
      showError(e.message ?? String(e));
    }
  }

  document.addEventListener("DOMContentLoaded", () => {
    init().catch((e) => {
      console.error(e);
      const el = $("meta");
      if (el) el.textContent = "Init failed: " + (e.message ?? String(e));
    });
  });
})();
