// frontend/review-core.js
// Review page for segLines + ticks overlay (uses backend endpoints under /api/review/*)

(() => {
  const state = {
    segmId: null,
    showMid: true,
    showKal: true,
    showBid: false,
    showAsk: false,
    showSegLines: true,

    // cached data
    ticks: [],
    lines: [],
    meta: null,
  };

  let chart = null;

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

  function fmtTime(ts) {
    if (!ts) return "";
    // backend returns ISO string; show HH:MM:SS
    try {
      const d = new Date(ts);
      if (Number.isNaN(d.getTime())) return String(ts);
      return d.toISOString().slice(11, 19);
    } catch {
      return String(ts);
    }
  }

  function tsToMs(ts) {
    if (ts == null) return null;
    if (typeof ts === "number") {
      // if already ms or seconds, assume ms if large
      return ts > 1e12 ? ts : ts * 1000;
    }
    const d = new Date(ts);
    const ms = d.getTime();
    return Number.isNaN(ms) ? null : ms;
  }

  async function loadSegmList() {
    // IMPORTANT: correct endpoint is /api/review/segms
    const segms = await fetchJSON("/api/review/segms");

    const sel = $("segmSelect");
    sel.innerHTML = "";

    // API returns objects like:
    // { segm_id, date, num_ticks, num_lines_active, global_max_abs_dist }
    for (const s of segms) {
      const segmId = s.segm_id ?? s.id ?? s.segmId;
      const date = s.date ?? (s.start_ts ? String(s.start_ts).slice(0, 10) : "");
      const opt = document.createElement("option");
      opt.value = String(segmId);
      const nLines = s.num_lines_active ?? 0;
      const worst = s.global_max_abs_dist;
      const worstTxt = (worst == null) ? "–" : Number(worst).toFixed(3);
      opt.textContent = `${date} (#${segmId}) lines:${nLines} worst:${worstTxt}`;
      sel.appendChild(opt);
    }

    // pick default segm
    try {
      const def = await fetchJSON("/api/review/default_segm");
      const defId = def.segm_id ?? def.segmId ?? def.id;
      if (defId != null) sel.value = String(defId);
    } catch {
      // ignore, keep first
    }

    state.segmId = parseInt(sel.value, 10);
  }

  async function loadMeta() {
    if (!state.segmId) return;
    // optional endpoint
    try {
      state.meta = await fetchJSON(`/api/review/segm/${state.segmId}/meta`);
    } catch {
      state.meta = null;
    }
  }

  async function loadTicks() {
    if (!state.segmId) return;
    const data = await fetchJSON(`/api/review/ticks_sample?segm_id=${state.segmId}`);

    // tolerate shapes:
    // - { ticks:[...] }
    // - [...] (array)
    // - { rows:[...] }
    const ticks = Array.isArray(data) ? data : (data.ticks ?? data.rows ?? []);
    state.ticks = ticks;
  }

  async function loadLines() {
    if (!state.segmId) return;
    const data = await fetchJSON(`/api/review/lines?segm_id=${state.segmId}`);
    const lines = Array.isArray(data) ? data : (data.lines ?? data.rows ?? []);
    state.lines = lines;
  }

  function buildSeriesFromTicks() {
    // ticks contain timestamp + bid/ask/mid/kal fields
    const pts = (field) => {
      const out = [];
      for (const t of state.ticks) {
        const x = tsToMs(t.timestamp ?? t.ts ?? t.time ?? t.t);
        const y = t[field];
        if (x == null || y == null) continue;
        out.push([x, Number(y)]);
      }
      return out;
    };

    const series = [];

    if (state.showBid) {
      series.push({
        name: "Bid",
        type: "line",
        showSymbol: false,
        sampling: "lttb",
        data: pts("bid"),
        lineStyle: { width: 1, opacity: 0.75 },
      });
    }
    if (state.showAsk) {
      series.push({
        name: "Ask",
        type: "line",
        showSymbol: false,
        sampling: "lttb",
        data: pts("ask"),
        lineStyle: { width: 1, opacity: 0.75 },
      });
    }
    if (state.showMid) {
      series.push({
        name: "Mid",
        type: "line",
        showSymbol: false,
        sampling: "lttb",
        data: pts("mid"),
        lineStyle: { width: 1.6, opacity: 0.95 },
      });
    }
    if (state.showKal) {
      series.push({
        name: "Kal",
        type: "line",
        showSymbol: false,
        sampling: "lttb",
        data: pts("kal"),
        lineStyle: { width: 2.0, opacity: 0.95 },
      });
    }

    return series;
  }

  function buildSeriesFromLines() {
    if (!state.showSegLines) return [];

    const out = [];
    for (const L of state.lines) {
      const x1 = tsToMs(L.start_ts ?? L.startTs ?? L.start_time);
      const x2 = tsToMs(L.end_ts ?? L.endTs ?? L.end_time);
      const y1 = L.start_price ?? L.startPrice;
      const y2 = L.end_price ?? L.endPrice;
      if (x1 == null || x2 == null || y1 == null || y2 == null) continue;

      out.push({
        name: `segLine#${L.id}`,
        type: "line",
        showSymbol: false,
        data: [[x1, Number(y1)], [x2, Number(y2)]],
        lineStyle: { width: 3, opacity: 0.95 },
        z: 10,
        silent: true,
      });
    }
    return out;
  }

  function renderChart() {
    if (!chart) return;

    const tickSeries = buildSeriesFromTicks();
    const lineSeries = buildSeriesFromLines();
    const allSeries = tickSeries.concat(lineSeries);

    chart.setOption({
      backgroundColor: "transparent",
      animation: false,
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "cross" },
      },
      legend: {
        top: 8,
        textStyle: { color: "#9ab1ff" },
      },
      grid: { left: 60, right: 25, top: 45, bottom: 40 },
      xAxis: {
        type: "time",
        axisLine: { lineStyle: { color: "#1a2b55" } },
        axisLabel: { color: "#9ab1ff" },
        splitLine: { show: true, lineStyle: { color: "rgba(26,43,85,.35)" } },
      },
      yAxis: {
        type: "value",
        scale: true,
        axisLine: { lineStyle: { color: "#1a2b55" } },
        axisLabel: { color: "#9ab1ff" },
        splitLine: { show: true, lineStyle: { color: "rgba(26,43,85,.35)" } },
      },
      dataZoom: [
        { type: "inside", xAxisIndex: 0, filterMode: "none" },
        { type: "slider", xAxisIndex: 0, height: 18, bottom: 10 },
      ],
      series: allSeries,
    }, { notMerge: true });
  }

  function renderLinesTable() {
    const body = $("linesBody");
    body.innerHTML = "";

    // Sort by start time so it looks like the day flow
    const rows = [...state.lines].sort((a,b) => {
      const ta = tsToMs(a.start_ts ?? a.startTs ?? a.start_time) ?? 0;
      const tb = tsToMs(b.start_ts ?? b.startTs ?? b.start_time) ?? 0;
      return ta - tb;
    });

    for (const L of rows) {
      const tr = document.createElement("tr");

      const id = L.id ?? "";
      const depth = L.depth ?? "";
      const iter = L.iteration ?? L.iter ?? "";
      const start = fmtTime(L.start_ts ?? L.startTs ?? L.start_time);
      const end = fmtTime(L.end_ts ?? L.endTs ?? L.end_time);
      const nTicks = L.num_ticks ?? L.tick_count ?? L.ticks ?? "";
      const slope = (L.slope == null) ? "" : Number(L.slope).toFixed(6);
      const maxd = (L.max_abs_dist == null) ? "" : Number(L.max_abs_dist).toFixed(4);

      tr.innerHTML = `
        <td class="mono">${id}</td>
        <td>${depth}</td>
        <td>${iter}</td>
        <td class="mono">${start}</td>
        <td class="mono">${end}</td>
        <td>${nTicks}</td>
        <td class="mono">${slope}</td>
        <td class="mono">${maxd}</td>
      `;
      body.appendChild(tr);
    }
  }

  function renderMeta() {
    const el = $("meta");
    if (!el) return;

    // If meta endpoint exists, show it; otherwise show quick derived
    const active = state.lines.length;
    let worst = null;
    for (const L of state.lines) {
      const v = L.max_abs_dist;
      if (v == null) continue;
      worst = (worst == null) ? Number(v) : Math.max(worst, Number(v));
    }
    const worstTxt = (worst == null) ? "–" : worst.toFixed(4);

    if (state.meta) {
      // keep short, but show something useful
      const date = state.meta.date ?? state.meta.start_date ?? "";
      el.textContent = `segm_id:${state.segmId} ${date} • active lines:${active} • worst:${worstTxt}`;
    } else {
      el.textContent = `segm_id:${state.segmId} • active lines:${active} • worst:${worstTxt}`;
    }
  }

  async function reloadAll() {
    $("meta").textContent = "Loading…";
    await Promise.all([loadMeta(), loadTicks(), loadLines()]);
    renderChart();
    renderLinesTable();
    renderMeta();
  }

  async function doBreak() {
    const btn = $("btnBreak");
    btn.disabled = true;
    const old = btn.textContent;
    btn.textContent = "Breaking…";
    try {
      // backend expects payload for breakLine job
      // keep it minimal: segm_id only
      await fetchJSON("/api/review/breakLine", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ segm_id: state.segmId }),
      });
      await loadLines();
      renderChart();
      renderLinesTable();
      renderMeta();
    } catch (e) {
      console.error(e);
      alert(String(e.message ?? e));
    } finally {
      btn.disabled = false;
      btn.textContent = old;
    }
  }

  function bindUI() {
    const sel = $("segmSelect");
    sel.addEventListener("change", async () => {
      state.segmId = parseInt(sel.value, 10);
      await reloadAll();
    });

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

    $("btnBreak").addEventListener("click", doBreak);

    // set initial toggle visuals
    setToggle(tMid, state.showMid);
    setToggle(tKal, state.showKal);
    setToggle(tBid, state.showBid);
    setToggle(tAsk, state.showAsk);
    setToggle(tLines, state.showSegLines);

    window.addEventListener("resize", () => chart && chart.resize());
  }

  async function init() {
    chart = echarts.init($("chart"));

    await loadSegmList();
    bindUI();
    await reloadAll();
  }

  window.addEventListener("DOMContentLoaded", () => {
    init().catch((e) => {
      console.error(e);
      alert(String(e.message ?? e));
    });
  });
})();
