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

    showMid: true,
    showKal: true,
    showBid: false,
    showAsk: false,
    showSegLines: true,
  };

  let chart = null;
  let tickById = new Map();

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

  function buildTickData(field) {
    const out = [];
    for (const t of state.ticks) {
      const x = Number(t.id);
      const y = t[field];
      if (!Number.isFinite(x) || y == null) continue;
      out.push([x, Number(y)]);
    }
    return out;
  }

  function buildSegLineSeries(selectedId) {
    if (!state.showSegLines || !state.rangeLines.length) return [];

    const selId = selectedId != null ? Number(selectedId) : null;
    const other = [];
    const primary = [];

    for (const ln of state.rangeLines) {
      const a = [Number(ln.start_tick_id), Number(ln.start_price)];
      const b = [Number(ln.end_tick_id), Number(ln.end_price)];
      const target = (selId != null && Number(ln.id) === selId) ? primary : other;
      target.push(a);
      target.push(b);
      target.push([null, null]);
    }

    const series = [];
    if (other.length) {
      series.push({
        id: "seglines_other",
        name: "SegLines",
        type: "line",
        data: other,
        showSymbol: false,
        connectNulls: false,
        lineStyle: { width: 2, opacity: 0.7 },
        silent: true,
        z: 5,
      });
    }
    if (primary.length) {
      series.push({
        id: "seglines_primary",
        name: "SegLine (primary)",
        type: "line",
        data: primary,
        showSymbol: false,
        connectNulls: false,
        lineStyle: { width: 4, opacity: 0.95, color: "#ffd54a" },
        silent: true,
        z: 6,
      });
    }
    return series;
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
        symbolSize: 3,
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
    const allSeries = series.concat(segLineSeries);

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
      await loadLinesForSegm();
      await loadWindow();
    });

    $("lineSelect").addEventListener("change", () => {
      loadWindow().catch((e) => alert(String(e.message ?? e)));
    });

    $("btnLoad").addEventListener("click", () => {
      loadWindow().catch((e) => alert(String(e.message ?? e)));
    });

    $("btnPrev").addEventListener("click", () => moveLine(-1));
    $("btnNext").addEventListener("click", () => moveLine(1));

    const tMid = $("toggleMid");
    const tKal = $("toggleKal");
    const tBid = $("toggleBid");
    const tAsk = $("toggleAsk");
    const tLines = $("toggleSegLines");

    tMid.addEventListener("click", () => { state.showMid = !state.showMid; setToggle(tMid, state.showMid); renderChart(); });
    tKal.addEventListener("click", () => { state.showKal = !state.showKal; setToggle(tKal, state.showKal); renderChart(); });
    tBid.addEventListener("click", () => { state.showBid = !state.showBid; setToggle(tBid, state.showBid); renderChart(); });
    tAsk.addEventListener("click", () => { state.showAsk = !state.showAsk; setToggle(tAsk, state.showAsk); renderChart(); });
    tLines.addEventListener("click", () => { state.showSegLines = !state.showSegLines; setToggle(tLines, state.showSegLines); renderChart(); });
  }

  function initChart() {
    chart = echarts.init($("chart"));
    window.addEventListener("resize", () => chart && chart.resize());
  }

  async function init() {
    initChart();
    setToggle($("toggleMid"), state.showMid);
    setToggle($("toggleKal"), state.showKal);
    setToggle($("toggleBid"), state.showBid);
    setToggle($("toggleAsk"), state.showAsk);
    setToggle($("toggleSegLines"), state.showSegLines);

    await loadSegmList();
    await loadLinesForSegm();
    bindUI();
    await loadWindow();
  }

  document.addEventListener("DOMContentLoaded", () => {
    init().catch((e) => {
      console.error(e);
      const el = $("meta");
      if (el) el.textContent = "Init failed: " + (e.message ?? String(e));
      alert("Init failed: " + (e.message ?? String(e)));
    });
  });
})();
