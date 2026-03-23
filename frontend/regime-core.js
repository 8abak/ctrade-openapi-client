// frontend/regime-core.js
// UNITY-first Regime Explorer with legacy fallback.

(() => {
  const state = {
    symbol: "XAUUSD",
    mode: "unity",
    availableModes: [],
    status: null,
    errors: [],

    legacySegms: [],
    segmId: null,
    legacyLines: [],
    legacyLegs: new Map(),
    zigPivots: [],

    unityContexts: [],
    focus: null,
    unityPivots: [],
    unitySwings: [],
    unitySignals: [],
    unityCandidates: [],
    unityEvents: [],
    unityTrades: [],

    ticks: [],
    range: null,

    showMid: false,
    showKal: true,
    showBid: false,
    showAsk: false,
    showSegLines: false,
    showLegs: false,
    showZig: false,
    showUnityPivots: true,
    showUnitySwings: true,
    showUnitySignals: true,
    showUnityCandidates: true,
    showUnityEvents: true,
  };

  let chart = null;
  let tickMap = new Map();
  let pivotMap = new Map();
  let signalMap = new Map();
  let candidateMap = new Map();
  let eventMap = new Map();

  function $(id) { return document.getElementById(id); }

  async function fetchJSON(url, opts) {
    const res = await fetch(url, opts);
    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      throw new Error(`HTTP ${res.status} ${res.statusText} for ${url}\n${txt}`);
    }
    return res.json();
  }

  async function safeFetch(url, opts, { silent = false } = {}) {
    try {
      const data = await fetchJSON(url, opts);
      clearError(url);
      return data;
    } catch (err) {
      pushError(url, err);
      if (!silent) throw err;
      return null;
    }
  }

  function pushError(endpoint, err) {
    const message = String(err && (err.message || err) || "Unknown error");
    const row = { endpoint, message };
    const idx = state.errors.findIndex((v) => v.endpoint === endpoint);
    if (idx >= 0) state.errors[idx] = row;
    else state.errors.push(row);
    renderErrors();
  }

  function clearError(endpoint) {
    const next = state.errors.filter((v) => v.endpoint !== endpoint);
    if (next.length !== state.errors.length) {
      state.errors = next;
      renderErrors();
    }
  }

  function setToggle(id, on) {
    const el = $(id);
    if (el) el.classList.toggle("on", !!on);
  }

  function pad2(n) {
    return String(n).padStart(2, "0");
  }

  function formatTs(ts) {
    if (!ts) return "-";
    const d = new Date(ts);
    if (Number.isNaN(d.getTime())) return String(ts);
    return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())} ${pad2(d.getHours())}:${pad2(d.getMinutes())}:${pad2(d.getSeconds())}`;
  }

  function fmt(v, digits = 2) {
    const n = Number(v);
    return Number.isFinite(n) ? n.toFixed(digits) : "-";
  }

  function colorForState(v) {
    if (v === "green" || v === "long") return "#4fd1a1";
    if (v === "red" || v === "short") return "#ff7c6b";
    if (v === "yellow") return "#f2b84b";
    return "#9bb0cb";
  }

  function rebuildMaps() {
    tickMap = new Map();
    pivotMap = new Map();
    signalMap = new Map();
    candidateMap = new Map();
    eventMap = new Map();

    const add = (map, key, row) => {
      if (!Number.isFinite(key)) return;
      if (!map.has(key)) map.set(key, []);
      map.get(key).push(row);
    };

    for (const row of state.ticks) add(tickMap, Number(row.id), row);
    for (const row of state.unityPivots) add(pivotMap, Number(row.tickid), row);
    for (const row of state.unitySignals) add(signalMap, Number(row.tickid), row);
    for (const row of state.unityCandidates) add(candidateMap, Number(row.focus_tick_id), row);
    for (const row of state.unityEvents) add(eventMap, Number(row.tickid), row);
  }

  function buildLineSeries(rows, field, name, opts = {}) {
    return {
      name,
      type: opts.type || "line",
      data: rows
        .map((r) => {
          if (r[field] == null) return null;
          return [Number(r.id), Number(r[field])];
        })
        .filter((v) => v && Number.isFinite(v[0]) && Number.isFinite(v[1])),
      showSymbol: false,
      symbolSize: opts.symbolSize || 6,
      lineStyle: opts.lineStyle || { width: 2, color: "#7ae3ff" },
      itemStyle: opts.itemStyle,
      z: opts.z || 2,
    };
  }

  function buildLegacySeries() {
    const out = [];
    if (state.showSegLines) {
      for (const row of state.legacyLines) {
        out.push({
          type: "line",
          data: [[Number(row.start_tick_id), Number(row.start_price)], [Number(row.end_tick_id), Number(row.end_price)]],
          showSymbol: false,
          lineStyle: { width: 2, color: "#ffd54a", opacity: 0.8 },
          z: 5,
        });
      }
    }
    if (state.showLegs) {
      for (const row of state.legacyLines) {
        const leg = state.legacyLegs.get(Number(row.id));
        if (!leg || !leg.has_b || !leg.has_c) continue;
        const parts = [
          [[Number(leg.a_tick_id), Number(leg.a_kal)], [Number(leg.b_tick_id), Number(leg.b_kal)]],
          [[Number(leg.b_tick_id), Number(leg.b_kal)], [Number(leg.c_tick_id), Number(leg.c_kal)]],
        ];
        if (leg.has_d) parts.push([[Number(leg.c_tick_id), Number(leg.c_kal)], [Number(leg.d_tick_id), Number(leg.d_kal)]]);
        for (const seg of parts) {
          out.push({
            type: "line",
            data: seg,
            showSymbol: false,
            lineStyle: { width: 1, color: "#6bd4ff", type: "dashed", opacity: 0.8 },
            z: 4,
          });
        }
      }
    }
    if (state.showZig && state.zigPivots.length) {
      out.push({
        name: "Zig",
        type: "line",
        data: state.zigPivots
          .map((r) => [Number(r.tick_id ?? r.tickId), Number(r.price)])
          .filter((v) => Number.isFinite(v[0]) && Number.isFinite(v[1])),
        showSymbol: true,
        symbolSize: 5,
        lineStyle: { width: 2, color: "#ff9f40" },
        z: 4,
      });
    }
    return out;
  }

  function buildUnitySeries() {
    const out = [];
    if (state.showUnitySwings) {
      for (const row of state.unitySwings) {
        out.push({
          type: "line",
          data: [[Number(row.starttick), Number(row.startprice)], [Number(row.endtick), Number(row.endprice)]],
          showSymbol: false,
          lineStyle: { width: 3, color: colorForState(row.state), opacity: 0.85 },
          z: 5,
        });
      }
    }
    if (state.showUnityPivots && state.unityPivots.length) {
      out.push({
        name: "UNITY Pivots",
        type: "scatter",
        symbol: "diamond",
        symbolSize: 9,
        data: state.unityPivots
          .map((r) => ({
            value: [Number(r.tickid), Number(r.price)],
            itemStyle: { color: r.kind === "high" ? "#ff8c69" : "#4fd1a1" },
          }))
          .filter((v) => Number.isFinite(v.value[0]) && Number.isFinite(v.value[1])),
        z: 7,
      });
    }
    if (state.showUnitySignals && state.unitySignals.length) {
      out.push({
        name: "UNITY Signals",
        type: "scatter",
        symbol: "rect",
        symbolSize: 8,
        data: state.unitySignals
          .map((r) => ({
            value: [Number(r.tickid), Number(r.price)],
            itemStyle: { color: r.favored ? "#7ae3ff" : (r.status === "rejected" ? "#8d99ad" : "#f2b84b") },
          }))
          .filter((v) => Number.isFinite(v.value[0]) && Number.isFinite(v.value[1])),
        z: 8,
      });
    }
    if (state.showUnityCandidates && state.unityCandidates.length) {
      const selectedId = Number(state.focus && state.focus.candidate_id);
      out.push({
        name: "UNITY Candidates",
        type: "scatter",
        symbol: "triangle",
        symbolSize: 11,
        data: state.unityCandidates
          .map((r) => ({
            value: [Number(r.focus_tick_id), Number(r.price)],
            symbolSize: Number(r.candidate_id) === selectedId ? 14 : 11,
            itemStyle: {
              color: r.outcome_status === "resolved" && r.firsthit === "tp"
                ? "#4fd1a1"
                : (r.outcome_status === "resolved" ? "#ff7c6b" : "#f2b84b"),
            },
          }))
          .filter((v) => Number.isFinite(v.value[0]) && Number.isFinite(v.value[1])),
        z: 9,
      });
    }
    if (state.showUnityEvents && state.unityEvents.length) {
      out.push({
        name: "UNITY Events",
        type: "scatter",
        symbol: "circle",
        symbolSize: 8,
        data: state.unityEvents
          .map((r) => ({
            value: [Number(r.tickid), Number(r.price)],
            itemStyle: { color: r.kind === "open" ? "#7ae3ff" : "#ffb454" },
          }))
          .filter((v) => Number.isFinite(v.value[0]) && Number.isFinite(v.value[1])),
        z: 10,
      });
    }
    return out;
  }

  function renderChart() {
    if (!chart) return;

    const series = [];
    if (state.showMid) series.push(buildLineSeries(state.ticks, "mid", "Mid", { type: "scatter", symbolSize: 2, z: 1, lineStyle: { width: 0 } }));
    if (state.showKal) series.push(buildLineSeries(state.ticks, "kal", "Kal", { z: 2, lineStyle: { width: 2, color: "#7ae3ff" } }));
    if (state.showBid) series.push(buildLineSeries(state.ticks, "bid", "Bid", { z: 1, lineStyle: { width: 1, color: "#8d99ad" } }));
    if (state.showAsk) series.push(buildLineSeries(state.ticks, "ask", "Ask", { z: 1, lineStyle: { width: 1, color: "#f2b84b" } }));

    series.push(...buildLegacySeries());
    series.push(...buildUnitySeries());

    const ids = state.ticks.map((r) => Number(r.id)).filter(Number.isFinite);
    const minX = ids.length ? Math.min(...ids) : null;
    const maxX = ids.length ? Math.max(...ids) : null;

    chart.setOption({
      animation: false,
      grid: { left: 60, right: 26, top: 24, bottom: 64 },
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "cross" },
        formatter: (params) => {
          if (!params || !params.length) return "";
          const tickId = Number(params[0].axisValue);
          const tick = tickMap.get(tickId);
          const lines = [
            `<b>tick:</b> ${tickId}`,
            `<b>time:</b> ${formatTs(tick && tick.ts)}`,
          ];
          if (tick) {
            if (tick.mid != null) lines.push(`<b>mid:</b> ${fmt(tick.mid, 4)}`);
            if (tick.kal != null) lines.push(`<b>kal:</b> ${fmt(tick.kal, 4)}`);
          }
          const pivots = pivotMap.get(tickId) || [];
          const signals = signalMap.get(tickId) || [];
          const candidates = candidateMap.get(tickId) || [];
          const events = eventMap.get(tickId) || [];
          if (pivots.length) lines.push(`<b>pivots:</b> ${pivots.map((r) => r.kind).join(", ")}`);
          if (signals.length) lines.push(`<b>signals:</b> ${signals.map((r) => `${r.side}/${r.status}`).join(", ")}`);
          if (candidates.length) lines.push(`<b>candidates:</b> ${candidates.map((r) => `#${r.candidate_id} ${r.outcome_status}`).join(", ")}`);
          if (events.length) lines.push(`<b>events:</b> ${events.map((r) => `${r.kind}/${r.reason}`).join(", ")}`);
          return lines.join("<br/>");
        },
      },
      xAxis: {
        type: "value",
        min: minX,
        max: maxX,
        axisLabel: { color: "#a7b6cd" },
        splitLine: { show: true, lineStyle: { color: "rgba(30,51,89,0.45)" } },
      },
      yAxis: {
        type: "value",
        scale: true,
        axisLabel: { color: "#a7b6cd" },
        splitLine: { show: true, lineStyle: { color: "rgba(30,51,89,0.45)" } },
      },
      dataZoom: [
        { type: "inside", xAxisIndex: 0, filterMode: "none" },
        { type: "slider", xAxisIndex: 0, height: 18, bottom: 12 },
      ],
      graphic: state.ticks.length ? [] : [{
        type: "text",
        left: "center",
        top: "middle",
        style: { text: "No chart data for the current selection.", fill: "#a7b6cd", fontSize: 16 },
      }],
      series,
    }, { notMerge: true });
  }

  function renderErrors() {
    const panel = $("errorPanel");
    const list = $("errorList");
    if (!panel || !list) return;
    if (!state.errors.length) {
      panel.hidden = true;
      list.innerHTML = "";
      return;
    }
    panel.hidden = false;
    list.innerHTML = state.errors
      .map((r) => `<li><span class="Endpoint">${r.endpoint}</span><span>${r.message.replace(/</g, "&lt;")}</span></li>`)
      .join("");
  }

  function renderWorkflow() {
    const el = $("workflowBody");
    if (!el) return;
    const summary = state.status && state.status.unity && state.status.unity.summary ? state.status.unity.summary : {};
    const stats = [
      summary.ticks ? `unitytick ${summary.ticks.count}` : null,
      summary.pivots ? `unitypivot ${summary.pivots.count}` : null,
      summary.signals ? `unitysignal ${summary.signals.count}` : null,
      summary.candidates ? `unitycandidate ${summary.candidates.count}` : null,
      summary.outcomes ? `unitycandoutcome ${summary.outcomes.count}` : null,
      summary.events ? `unityevent ${summary.events.count}` : null,
      summary.trades ? `unitytrade ${summary.trades.count}` : null,
    ].filter(Boolean).join(" · ");

    el.innerHTML = `
      <p><strong>UNITY is the current live chain.</strong> The verified code path is <code>ticks</code> -> derived tick calc -> <code>unitytick</code>/<code>unitypivot</code>/<code>unityswing</code> -> <code>unitysignal</code> -> <code>unitycandidate</code> -> <code>unitycandoutcome</code>/<code>unitycandscenario</code> -> <code>unityevent</code>/<code>unitytrade</code>.</p>
      <p><strong>Shadow vs actual journal:</strong> <code>unitycandoutcome</code> and <code>unitycandscenario</code> are shadow labels only. <code>unitytrade</code> and <code>unityevent</code> are the paper trade/event log.</p>
      <p><strong>Live summary:</strong> ${stats || "No UNITY summary counts were returned."}</p>
    `;
  }

  function renderFocus() {
    const el = $("focusBody");
    if (!el) return;
    if (!state.focus) {
      el.innerHTML = "<p>No live UNITY focus row was returned.</p>";
      return;
    }
    const trade = state.unityTrades.length ? state.unityTrades[state.unityTrades.length - 1] : null;
    el.innerHTML = `
      <p><strong>Focus:</strong> ${state.focus.candidate_id ? `candidate #${state.focus.candidate_id}` : `tick ${state.focus.focus_tick_id}`}</p>
      <p><strong>Time:</strong> ${formatTs(state.focus.time)}</p>
      <p><strong>Direction:</strong> ${state.focus.side || state.focus.regimeto || "-"} | <strong>Status:</strong> ${state.focus.signalstatus || state.focus.outcome_status || "-"}</p>
      <p><strong>Outcome:</strong> ${state.focus.outcome_status || "-"} ${state.focus.firsthit ? `(${state.focus.firsthit})` : ""} | <strong>PnL:</strong> ${fmt(state.focus.pnl, 2)}</p>
      <p><strong>Window:</strong> ${state.range ? `${state.range.start_tick_id} -> ${state.range.end_tick_id} (${state.range.tick_count} ticks)` : "-"}</p>
      <p><strong>Trade context:</strong> ${trade ? `${trade.side} ${trade.status} pnl ${fmt(trade.pnl, 2)} exit ${trade.exitreason || "-"}` : "No paper trade in this window."}</p>
    `;
  }

  function renderMeta() {
    const el = $("meta");
    if (!el) return;
    const parts = [`mode:${state.mode}`, `symbol:${state.symbol}`];
    if (state.focus && state.focus.focus_tick_id != null) parts.push(`focus:${state.focus.focus_tick_id}`);
    if (state.range && state.range.tick_count != null) parts.push(`ticks:${state.range.tick_count}`);
    if (state.mode === "legacy" && state.segmId != null) parts.push(`segm:${state.segmId}`);
    el.textContent = parts.join("  ");
  }

  function renderMode() {
    $("modeBadge").textContent = state.mode === "legacy" ? "Mode: Legacy Segm" : "Mode: UNITY";
    $("contextLabel").textContent = state.mode === "legacy" ? "Segm:" : "Context:";
    $("legacyControls").hidden = state.mode !== "legacy";
    $("unityControls").hidden = state.mode !== "unity";
    $("legacyToggles").hidden = state.mode !== "legacy";
    $("modeSelect").parentElement.hidden = state.availableModes.length <= 1;
  }

  function renderAll() {
    rebuildMaps();
    renderErrors();
    renderWorkflow();
    renderFocus();
    renderMeta();
    renderMode();
    renderChart();
  }

  function parseContextValue() {
    const value = $("contextSelect").value || "";
    if (value.startsWith("c:")) return { candidateId: Number(value.slice(2)), focusTickId: null };
    if (value.startsWith("t:")) return { candidateId: null, focusTickId: Number(value.slice(2)) };
    return { candidateId: null, focusTickId: null };
  }

  async function loadStatus() {
    const data = await safeFetch(`/api/regime/status?symbol=${encodeURIComponent(state.symbol)}`, null, { silent: true });
    state.status = data;
    const modes = [];
    if (data && data.unity && data.unity.available) modes.push({ value: "unity", label: "UNITY" });
    if (data && data.legacy && data.legacy.available) modes.push({ value: "legacy", label: "Legacy" });
    if (!modes.length) modes.push({ value: "unity", label: "UNITY" });
    state.availableModes = modes;
    if (!modes.some((v) => v.value === state.mode)) state.mode = data && data.preferred_mode ? data.preferred_mode : modes[0].value;

    const sel = $("modeSelect");
    sel.innerHTML = "";
    for (const row of modes) {
      const opt = document.createElement("option");
      opt.value = row.value;
      opt.textContent = row.label;
      sel.appendChild(opt);
    }
    sel.value = state.mode;
  }

  async function loadUnityContexts() {
    const data = await safeFetch(`/api/regime/unity/contexts?symbol=${encodeURIComponent(state.symbol)}&limit=200`, null, { silent: true });
    state.unityContexts = data && Array.isArray(data.rows) ? data.rows : [];
    const sel = $("contextSelect");
    sel.innerHTML = "";
    if (!state.unityContexts.length) {
      const opt = document.createElement("option");
      opt.value = "";
      opt.textContent = "Latest live UNITY window";
      sel.appendChild(opt);
      return;
    }
    for (const row of state.unityContexts) {
      const opt = document.createElement("option");
      opt.value = row.candidate_id != null ? `c:${row.candidate_id}` : `t:${row.focus_tick_id}`;
      opt.textContent = `${formatTs(row.time)} | ${row.candidate_id ? `#${row.candidate_id}` : `tick ${row.focus_tick_id}`} | ${row.side || row.regimeto || "-"} | ${row.signalstatus || row.outcome_status || "-"}`;
      sel.appendChild(opt);
    }
  }

  async function loadLegacySegms() {
    const rows = await safeFetch("/api/regime/segms", null, { silent: true });
    state.legacySegms = Array.isArray(rows) ? rows : [];
    const sel = $("contextSelect");
    sel.innerHTML = "";
    if (!state.legacySegms.length) {
      const opt = document.createElement("option");
      opt.value = "";
      opt.textContent = "No legacy segms available";
      sel.appendChild(opt);
      state.segmId = null;
      return;
    }
    for (const row of state.legacySegms) {
      const opt = document.createElement("option");
      opt.value = String(row.segm_id);
      opt.textContent = `${row.date || ""} (#${row.segm_id})`;
      sel.appendChild(opt);
    }
    state.segmId = Number(sel.value) || null;
  }

  async function loadLegacyLines() {
    if (!state.segmId) {
      state.legacyLines = [];
      $("lineSelect").innerHTML = "";
      return;
    }
    const data = await safeFetch(`/api/regime/segm/${state.segmId}/lines`, null, { silent: true });
    state.legacyLines = data && Array.isArray(data.lines) ? data.lines : [];
    const sel = $("lineSelect");
    sel.innerHTML = "";
    for (const row of state.legacyLines) {
      const opt = document.createElement("option");
      opt.value = String(row.id);
      opt.textContent = `L${row.id} ${row.start_tick_id} -> ${row.end_tick_id}`;
      sel.appendChild(opt);
    }
  }

  async function loadLegacyExtras() {
    state.legacyLegs = new Map();
    state.zigPivots = [];
    if (!state.segmId) return;

    const legs = await safeFetch(`/api/regime/legs?segm_id=${state.segmId}`, null, { silent: true });
    for (const row of (legs && Array.isArray(legs.legs) ? legs.legs : [])) {
      state.legacyLegs.set(Number(row.segline_id), row);
    }

    const zig = await safeFetch(`/api/regime/segm/${state.segmId}/zig_pivots`, null, { silent: true });
    state.zigPivots = zig && Array.isArray(zig.pivots) ? zig.pivots : [];
  }

  async function loadUnityWindow() {
    const { candidateId, focusTickId } = parseContextValue();
    const params = new URLSearchParams({
      symbol: state.symbol,
      ticks_before: String(Number($("ticksBefore").value || 900) || 900),
      ticks_after: String(Number($("ticksAfter").value || 450) || 450),
    });
    if (candidateId) params.set("candidate_id", String(candidateId));
    if (!candidateId && focusTickId) params.set("focus_tick_id", String(focusTickId));

    const data = await safeFetch(`/api/regime/unity/window?${params.toString()}`, null, { silent: true });
    state.focus = data ? data.focus || null : null;
    state.range = data ? data.range || null : null;
    state.ticks = data && Array.isArray(data.ticks) ? data.ticks : [];

    const unity = data && data.unity ? data.unity : {};
    state.unityPivots = Array.isArray(unity.pivots) ? unity.pivots : [];
    state.unitySwings = Array.isArray(unity.swings) ? unity.swings : [];
    state.unitySignals = Array.isArray(unity.signals) ? unity.signals : [];
    state.unityCandidates = Array.isArray(unity.candidates) ? unity.candidates : [];
    state.unityEvents = Array.isArray(unity.events) ? unity.events : [];
    state.unityTrades = Array.isArray(unity.trades) ? unity.trades : [];

    state.legacyLines = [];
    state.legacyLegs = new Map();
    state.zigPivots = [];
  }

  async function loadLegacyWindow() {
    const lineId = Number($("lineSelect").value || 0);
    const lineCount = Number($("lineCount").value || 1) || 1;
    if (!state.segmId || !lineId) {
      state.focus = null;
      state.range = null;
      state.ticks = [];
      state.legacyLines = [];
      return;
    }

    const data = await safeFetch("/api/regime/window", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ segm_id: state.segmId, start_segline_id: lineId, line_count: lineCount }),
    }, { silent: true });

    state.focus = null;
    state.range = data ? data.range || null : null;
    state.ticks = data && Array.isArray(data.ticks) ? data.ticks : [];
    state.legacyLines = data && Array.isArray(data.seglines) ? data.seglines : [];

    state.unityPivots = [];
    state.unitySwings = [];
    state.unitySignals = [];
    state.unityCandidates = [];
    state.unityEvents = [];
    state.unityTrades = [];
  }

  async function loadMode(mode) {
    state.mode = mode;
    if (state.mode === "legacy") {
      await loadLegacySegms();
      await loadLegacyLines();
      await loadLegacyExtras();
      await loadLegacyWindow();
    } else {
      await loadUnityContexts();
      await loadUnityWindow();
    }
    renderAll();
  }

  function bindUI() {
    $("modeSelect").addEventListener("change", () => loadMode($("modeSelect").value));
    $("contextSelect").addEventListener("change", async () => {
      if (state.mode === "legacy") {
        state.segmId = Number($("contextSelect").value || 0) || null;
        await loadLegacyLines();
        await loadLegacyExtras();
      }
      await (state.mode === "legacy" ? loadLegacyWindow() : loadUnityWindow());
      renderAll();
    });
    $("lineSelect").addEventListener("change", async () => {
      await loadLegacyWindow();
      renderAll();
    });
    $("btnLoad").addEventListener("click", async () => {
      await (state.mode === "legacy" ? loadLegacyWindow() : loadUnityWindow());
      renderAll();
    });
    $("btnPrev").addEventListener("click", () => stepSelection(-1));
    $("btnNext").addEventListener("click", () => stepSelection(1));

    const toggles = [
      ["toggleMid", "showMid"],
      ["toggleKal", "showKal"],
      ["toggleBid", "showBid"],
      ["toggleAsk", "showAsk"],
      ["toggleSegLines", "showSegLines"],
      ["toggleLegs", "showLegs"],
      ["toggleZig", "showZig"],
      ["toggleUnityPivots", "showUnityPivots"],
      ["toggleUnitySwings", "showUnitySwings"],
      ["toggleUnitySignals", "showUnitySignals"],
      ["toggleUnityCandidates", "showUnityCandidates"],
      ["toggleUnityEvents", "showUnityEvents"],
    ];
    for (const [id, key] of toggles) {
      $(id).addEventListener("click", () => {
        state[key] = !state[key];
        setToggle(id, state[key]);
        renderChart();
      });
    }
  }

  function stepSelection(delta) {
    const id = state.mode === "legacy" ? "lineSelect" : "contextSelect";
    const sel = $(id);
    if (!sel || !sel.options.length) return;
    const next = Math.max(0, Math.min(sel.selectedIndex + delta, sel.options.length - 1));
    if (next === sel.selectedIndex) return;
    sel.selectedIndex = next;
    sel.dispatchEvent(new Event("change"));
  }

  function initChart() {
    chart = echarts.init($("chart"));
    window.addEventListener("resize", () => chart.resize());
  }

  function initToggles() {
    for (const [id, value] of [
      ["toggleMid", state.showMid],
      ["toggleKal", state.showKal],
      ["toggleBid", state.showBid],
      ["toggleAsk", state.showAsk],
      ["toggleSegLines", state.showSegLines],
      ["toggleLegs", state.showLegs],
      ["toggleZig", state.showZig],
      ["toggleUnityPivots", state.showUnityPivots],
      ["toggleUnitySwings", state.showUnitySwings],
      ["toggleUnitySignals", state.showUnitySignals],
      ["toggleUnityCandidates", state.showUnityCandidates],
      ["toggleUnityEvents", state.showUnityEvents],
    ]) setToggle(id, value);
  }

  async function init() {
    initChart();
    initToggles();
    bindUI();
    await loadStatus();
    await loadMode(state.mode);
  }

  document.addEventListener("DOMContentLoaded", () => {
    init().catch((err) => {
      pushError("/regime:init", err);
      renderAll();
    });
  });
})();
