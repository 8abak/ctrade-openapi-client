// frontend/review-core.js
// Review page for segLines + ticks overlay (uses backend endpoints under /api/review/*)

(() => {
  const state = {
  // next-break preview (max |dist| segtick)
  nextBreak: null, // { segtick_id, tick_id, segline_id, dist, ts_ms }

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

    // Backend route is path-param, not query-param.
    // Also, this endpoint is a downsampler (you can lower/raise target_points as needed).
    const data = await fetchJSON(`/api/review/segm/${state.segmId}/ticks_sample?target_points=50000`);

    // Shape:
    //   { segm_id, stride, points:[{id, ts, ask, bid, mid, kal}, ...] }
    // Tolerate older shapes too.
    const ticks = Array.isArray(data) ? data : (data.points ?? data.ticks ?? data.rows ?? []);
    state.ticks = ticks;
  }

  
async function loadNextBreak() {
  state.nextBreak = null;
  const segmId = state.segmId;
  if (!segmId) return;
  try {
    const nb = await fetchJSON(`/api/review/segm/${segmId}/next_break`);
    if (nb && nb.ts_ms) state.nextBreak = nb;
  } catch (e) {
    console.warn("next_break fetch failed", e);
  }
}

async function loadLines() {
    if (!state.segmId) return;

    // Backend route is path-param, not query-param.
    const data = await fetchJSON(`/api/review/segm/${state.segmId}/lines`);
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
        const tickId = t.id ?? t.tick_id ?? t.tickId;
        if (tickId != null) {
          out.push({ value: [x, Number(y)], tickId });
        } else {
          out.push([x, Number(y)]);
        }
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
    
if (state.nextBreak && state.nextBreak.ts_ms) {
  tickSeries.push({
    name: "NextBreak",
    type: "line",
    data: [],
    showSymbol: false,
    silent: true,
    lineStyle: { opacity: 0 },
    markLine: {
      symbol: ["none", "none"],
      label: {
        show: true,
        formatter: () =>
          `next: tick ${state.nextBreak.tick_id} (segtick ${state.nextBreak.segtick_id}) dist=${Number(state.nextBreak.dist).toFixed(3)}`,
        position: "insideEndTop",
      },
      data: [{ xAxis: state.nextBreak.ts_ms }],
    },
  });
}

const allSeries = tickSeries.concat(lineSeries);

    chart.setOption({
      animation: false,
      grid: { left: 50, right: 20, top: 25, bottom: 60 },
      tooltip: {
  trigger: "axis",
  axisPointer: { type: "cross" },
  formatter: (params) => {
    if (!params || !params.length) return "";
    const withTick = params.find(p => p && p.data && p.data.tickId != null);
    const tickId = withTick ? withTick.data.tickId : null;

    const lines = [];
    if (tickId != null) lines.push(`<b>tick_id:</b> ${tickId}`);

    for (const p of params) {
      const v = (p.data && p.data.value) ? p.data.value : p.value;
      const y = (v && v.length >= 2) ? v[1] : null;
      const yTxt = (y == null || Number.isNaN(y)) ? "" : Number(y).toFixed(3);
      lines.push(`${p.marker} ${p.seriesName}: ${yTxt}`);
    }
    return lines.join("<br/>");
  },
},
      xAxis: {
        type: "time",
        axisLabel: { hideOverlap: true },
        splitLine: { show: true, lineStyle: { color: "rgba(26,43,85,35)" } },
      },
      yAxis: {
        type: "value",
        scale: true,
        splitLine: { show: true, lineStyle: { color: "rgba(26,43,85,35)" } },
      },
      dataZoom: [
        { type: "inside", xAxisIndex: 0, filterMode: "none" },
        { type: "slider", xAxisIndex: 0, height: 18, bottom: 10 },
      ],
      series: allSeries,
    }, { notMerge: true });
  }

  function findNearestTickId(tsMs) {
    if (!state.ticks.length || tsMs == null) return null;
    let bestId = null;
    let bestDist = null;
    for (const t of state.ticks) {
      const tMs = tsToMs(t.timestamp ?? t.ts ?? t.time ?? t.t);
      if (tMs == null) continue;
      const d = Math.abs(tMs - tsMs);
      if (bestDist == null || d < bestDist) {
        bestDist = d;
        bestId = t.id ?? t.tick_id ?? t.tickId;
      }
    }
    return bestId;
  }

  function renderLinesTable() {
    const body = $("linesBody");
    if (!body) return;
    body.innerHTML = "";

    // Sort by start time so it looks like the day flow
    const rows = [...state.lines].sort((a, b) => {
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
    const metaEl = $("meta");
    if (metaEl) metaEl.textContent = "Loading…";
    await Promise.all([loadMeta(), loadTicks(), loadLines(), loadNextBreak()]);
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
      const forceVal = $("ForceTickId") ? $("ForceTickId").value.trim() : "";
      const forceTickId = forceVal ? parseInt(forceVal, 10) : null;
      const payload = { segm_id: state.segmId };
      if (Number.isFinite(forceTickId) && forceTickId > 0) {
        payload.tick_id = forceTickId;
      }
      // backend expects payload for breakLine job
      await fetchJSON("/api/review/breakLine", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      await loadLines();
    await loadNextBreak();
      renderChart();
      renderLinesTable();
      renderMeta();
      const forceInput = $("ForceTickId");
      if (forceInput) forceInput.value = "";
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
  }

  function initChart() {
    // expects echarts global already loaded
    chart = echarts.init($("chart"));
    chart.on("click", (params) => {
      if (!params || !params.seriesName) return;
      if (!String(params.seriesName).startsWith("segLine#")) return;
      const val = params.value;
      const tsMs = Array.isArray(val) ? val[0] : null;
      const tickId = findNearestTickId(tsMs);
      if (tickId == null) return;
      const input = $("ForceTickId");
      if (input) input.value = String(tickId);
    });
  }

  async function init() {
    initChart();
    await loadSegmList();

    // init toggle styles
    setToggle($("toggleMid"), state.showMid);
    setToggle($("toggleKal"), state.showKal);
    setToggle($("toggleBid"), state.showBid);
    setToggle($("toggleAsk"), state.showAsk);
    setToggle($("toggleSegLines"), state.showSegLines);

    bindUI();
    await reloadAll();
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
