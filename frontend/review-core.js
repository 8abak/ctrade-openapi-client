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

    // tick-id -> timestamp (ms) map from sampled ticks
    TickIdToMs: new Map(),

    // force preview
    forceTickId: null,
    forceTickMs: null,
    forceHint: "",

    // next-break preview (from backend-enriched line fields)
    nextBreak: null, // { segline_id, tick_id, ts_ms, max_abs_dist }
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
      return ts > 1e12 ? ts : ts * 1000;
    }
    const d = new Date(ts);
    const ms = d.getTime();
    return Number.isNaN(ms) ? null : ms;
  }

  function rebuildTickIdIndex() {
    state.TickIdToMs = new Map();
    for (const t of state.ticks) {
      const tickId = t.tick_id ?? t.tickId ?? t.id;
      const ms = tsToMs(t.timestamp ?? t.ts ?? t.time ?? t.t);
      if (tickId == null || ms == null) continue;
      state.TickIdToMs.set(Number(tickId), ms);
    }
  }

  function computeNextBreakFromLines() {
    // We expect backend to provide on each segline:
    //   - worst_tick_id (or worstTickId)
    //   - worst_ts (or worstTs)
    // If not present, we still show nothing (no guessing on frontend).
    let best = null;
    for (const L of state.lines) {
      const maxd = L.max_abs_dist ?? L.maxAbsDist;
      if (maxd == null) continue;
      const tickId = L.worst_tick_id ?? L.worstTickId;
      const ts = L.worst_ts ?? L.worstTs;
      const ms = tsToMs(ts);
      if (tickId == null || ms == null) continue;

      const cand = {
        segline_id: L.id,
        tick_id: Number(tickId),
        ts_ms: ms,
        max_abs_dist: Number(maxd),
      };

      if (!best || cand.max_abs_dist > best.max_abs_dist) best = cand;
    }
    state.nextBreak = best;
  }

  function normalizeForceTickInput(raw) {
    if (!raw) return null;
    const s = String(raw).trim();
    if (!s) return null;
    if (!/^\d+$/.test(s)) return null;
    return Number(s);
  }

  function updateForceTickFromUI() {
    const input = $("forceTickId");
    const hint = $("forceHint");
    const tickId = normalizeForceTickInput(input.value);

    state.forceTickId = tickId;
    state.forceTickMs = null;
    state.forceHint = "";

    if (!tickId) {
      hint.textContent = "";
      renderChart();
      return;
    }

    // best-effort: find exact tick_id in sampled ticks
    const ms = state.TickIdToMs.get(tickId);
    if (ms != null) {
      state.forceTickMs = ms;
      state.forceHint = "preview";
      hint.textContent = "preview";
      renderChart();
      return;
    }

    // if not found in sample, show guidance
    state.forceHint = "not in sample";
    hint.textContent = "not in sample (increase sample / zoom)";
    renderChart();
  }

  async function loadSegmList() {
    const segms = await fetchJSON("/api/review/segms");

    const sel = $("segmSelect");
    sel.innerHTML = "";

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

    try {
      const def = await fetchJSON("/api/review/default_segm");
      const defId = def.segm_id ?? def.segmId ?? def.id;
      if (defId != null) sel.value = String(defId);
    } catch {
      // ignore
    }

    state.segmId = parseInt(sel.value, 10);
  }

  async function loadMeta() {
    if (!state.segmId) return;
    try {
      state.meta = await fetchJSON(`/api/review/segm/${state.segmId}/meta`);
    } catch {
      state.meta = null;
    }
  }

  async function loadTicks() {
    if (!state.segmId) return;

    const data = await fetchJSON(`/api/review/segm/${state.segmId}/ticks_sample?target_points=50000`);
    const ticks = Array.isArray(data) ? data : (data.points ?? data.ticks ?? data.rows ?? []);
    state.ticks = ticks;

    rebuildTickIdIndex();
    updateForceTickFromUI(); // keep preview if user typed already
  }

  async function loadLines() {
    if (!state.segmId) return;

    const data = await fetchJSON(`/api/review/segm/${state.segmId}/lines`);
    const lines = Array.isArray(data) ? data : (data.lines ?? data.rows ?? []);
    state.lines = lines;

    computeNextBreakFromLines();
  }

  function buildSeriesFromTicks() {
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
        silent: false, // allow tooltip/click
      });
    }
    return out;
  }

  function buildMarkLines() {
    // We attach markLines to the first available tick series (or an empty helper series).
    const marks = [];

    // Next break candidate (backend-provided)
    if (state.nextBreak?.ts_ms != null) {
      marks.push({
        xAxis: state.nextBreak.ts_ms,
        lineStyle: { type: "dashed", width: 2, opacity: 0.9 },
        label: {
          show: true,
          formatter: `NEXT: line#${state.nextBreak.segline_id} tick#${state.nextBreak.tick_id} |dist|=${state.nextBreak.max_abs_dist.toFixed(3)}`,
          position: "insideEndTop",
        },
      });
    }

    // Force preview
    if (state.forceTickMs != null && state.forceTickId != null) {
      marks.push({
        xAxis: state.forceTickMs,
        lineStyle: { type: "solid", width: 2, opacity: 0.95 },
        label: {
          show: true,
          formatter: `FORCE tick#${state.forceTickId}`,
          position: "insideEndBottom",
        },
      });
    }

    if (!marks.length) return null;

    return {
      symbol: "none",
      data: marks,
    };
  }

  function renderChart() {
    if (!chart) return;

    const tickSeries = buildSeriesFromTicks();
    const lineSeries = buildSeriesFromLines();
    const allSeries = tickSeries.concat(lineSeries);

    // Apply markLine to the first tick series if possible; otherwise create a helper series
    const markLine = buildMarkLines();
    if (markLine) {
      if (allSeries.length) {
        allSeries[0] = { ...allSeries[0], markLine };
      } else {
        allSeries.push({
          name: "Marks",
          type: "line",
          data: [],
          markLine,
          silent: true,
        });
      }
    }

    chart.setOption({
      animation: false,
      grid: { left: 50, right: 20, top: 25, bottom: 60 },

      tooltip: {
        trigger: "axis",
        axisPointer: { type: "cross" },
        formatter: (params) => {
          // params is array for axis-trigger
          if (!Array.isArray(params) || !params.length) return "";

          // Find axis time
          const axisValue = params[0].axisValue;
          const d = new Date(axisValue);
          const timeTxt = Number.isNaN(d.getTime()) ? String(axisValue) : d.toISOString().replace("T", " ").slice(0, 19);

          const lines = [];
          lines.push(`<div class="mono">${timeTxt}</div>`);

          for (const p of params) {
            const name = p.seriesName ?? "";
            const val = (Array.isArray(p.data) ? p.data[1] : p.value);
            if (val == null) continue;

            if (name.startsWith("segLine#")) {
              // show segLine id prominently
              const id = name.slice("segLine#".length);
              lines.push(`<div><b>${name}</b> (id=${id}) : <span class="mono">${Number(val).toFixed(3)}</span></div>`);
            } else {
              lines.push(`<div>${name}: <span class="mono">${Number(val).toFixed(3)}</span></div>`);
            }
          }

          if (state.nextBreak) {
            lines.push(`<div style="margin-top:6px;color:rgba(154,177,255,.9)">NEXT break: line#${state.nextBreak.segline_id} tick#${state.nextBreak.tick_id}</div>`);
          }
          if (state.forceTickId) {
            lines.push(`<div style="color:rgba(154,177,255,.9)">FORCE tick: ${state.forceTickId} (${state.forceHint || "preview"})</div>`);
          }

          return lines.join("");
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

  function renderLinesTable() {
    const body = $("linesBody");
    body.innerHTML = "";

    const rows = [...state.lines].sort((a, b) => {
      const ta = tsToMs(a.start_ts ?? a.startTs ?? a.start_time) ?? 0;
      const tb = tsToMs(b.start_ts ?? b.startTs ?? b.start_time) ?? 0;
      return ta - tb;
    });

    for (const L of rows) {
      const tr = document.createElement("tr");
      tr.style.cursor = "pointer";

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

      // Click row: if backend provided worst_tick_id, auto-fill force box
      tr.addEventListener("click", () => {
        const wtid = L.worst_tick_id ?? L.worstTickId;
        if (wtid != null) {
          $("forceTickId").value = String(wtid);
          updateForceTickFromUI();
        } else {
          // fallback: no-op
          $("forceHint").textContent = "no worst_tick_id in backend response";
        }
      });

      body.appendChild(tr);
    }
  }

  function renderMeta() {
    const el = $("meta");
    if (!el) return;

    const active = state.lines.length;
    let worst = null;
    for (const L of state.lines) {
      const v = L.max_abs_dist;
      if (v == null) continue;
      worst = (worst == null) ? Number(v) : Math.max(worst, Number(v));
    }
    const worstTxt = (worst == null) ? "–" : worst.toFixed(4);

    let nextTxt = "";
    if (state.nextBreak) {
      nextTxt = ` • next: line#${state.nextBreak.segline_id} tick#${state.nextBreak.tick_id}`;
    } else {
      nextTxt = ` • next: –`;
    }

    if (state.meta) {
      const date = state.meta.date ?? state.meta.start_date ?? "";
      el.textContent = `segm_id:${state.segmId} ${date} • active lines:${active} • worst:${worstTxt}${nextTxt}`;
    } else {
      el.textContent = `segm_id:${state.segmId} • active lines:${active} • worst:${worstTxt}${nextTxt}`;
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

    const forceTickId = state.forceTickId; // may be null

    try {
      // IMPORTANT:
      // Backend should accept optional force_tick_id.
      // If absent/null -> break at default max|dist| tick.
      await fetchJSON("/api/review/breakLine", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          segm_id: state.segmId,
          force_tick_id: forceTickId,
        }),
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

    const forceInput = $("forceTickId");
    forceInput.addEventListener("input", () => updateForceTickFromUI());
    forceInput.addEventListener("change", () => updateForceTickFromUI());
  }

  function initChart() {
    chart = echarts.init($("chart"));

    // Optional: clicking a segLine on chart can auto-fill force tick id (if backend provided it)
    chart.on("click", (params) => {
      const name = params?.seriesName ?? "";
      if (!name.startsWith("segLine#")) return;

      const idStr = name.slice("segLine#".length);
      const id = Number(idStr);
      const L = state.lines.find(x => Number(x.id) === id);
      if (!L) return;

      const wtid = L.worst_tick_id ?? L.worstTickId;
      if (wtid != null) {
        $("forceTickId").value = String(wtid);
        updateForceTickFromUI();
      } else {
        $("forceHint").textContent = "no worst_tick_id in backend response";
      }
    });
  }

  async function init() {
    initChart();
    await loadSegmList();

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
