// PATH: frontend/review2-core.js
(function () {
  const el = (id) => document.getElementById(id);

  const segmSelect = el("segmSelect");
  const metaText = el("metaText");

  const btnMid = el("btnMid");
  const btnKal = el("btnKal");
  const btnBid = el("btnBid");
  const btnAsk = el("btnAsk");
  const btnBreak = el("btnBreak");

  const segLinesList = el("segLinesList");
  const selectedLineText = el("selectedLineText");

  const journalBox = el("journalBox");
  const journalLink = el("journalLink");
  const btnRefreshJournal = el("btnRefreshJournal");

  const state = {
    segmId: null,
    segmDate: null,

    ticks: [],
    lines: [],
    selectedSegLineId: null,

    vis: { mid: true, kal: true, bid: false, ask: false },

    chart: null,
  };

  function fmt(n, d = 2) {
    const x = Number(n);
    if (!Number.isFinite(x)) return "";
    return x.toFixed(d);
  }

  function todayYmdUtc() {
    const d = new Date();
    // toISOString() gives UTC
    return d.toISOString().slice(0, 10);
  }

  async function fetchJSON(url, opts) {
    const res = await fetch(url, opts);
    if (!res.ok) {
      const t = await res.text().catch(() => "");
      throw new Error(`${res.status} ${res.statusText} ${t}`.trim());
    }
    return await res.json();
  }

  // ----------------- Journal -----------------

  async function refreshJournal() {
    try {
      const d = await fetchJSON(`/api/journal/today?tail=80`);
      const lines = Array.isArray(d.lines) ? d.lines : [];
      journalBox.textContent = lines.length ? lines.join("\n") : "(no entries yet)";
      if (d.url_path) {
        journalLink.href = d.url_path;
        journalLink.textContent = d.url_path;
      } else {
        const fname = todayYmdUtc() + ".txt";
        const path = `/src/journal/${fname}`;
        journalLink.href = path;
        journalLink.textContent = path;
      }
    } catch (e) {
      // fallback: try reading /src directly
      const fname = todayYmdUtc() + ".txt";
      const path = `/src/journal/${fname}`;
      journalLink.href = path;
      journalLink.textContent = path;
      try {
        const res = await fetch(path + `?t=${Date.now()}`);
        if (!res.ok) throw new Error("journal file not found");
        const txt = await res.text();
        const lines = txt.split("\n").filter(Boolean).slice(-80);
        journalBox.textContent = lines.length ? lines.join("\n") : "(no entries yet)";
      } catch (e2) {
        journalBox.textContent = `Journal unavailable: ${String(e)}`;
      }
    }
  }

  async function writeJournal(event, segm_id, segline_id, details, extra) {
    try {
      await fetchJSON(`/api/journal/write`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          event,
          segm_id,
          segline_id,
          details,
          extra: extra || null,
        }),
      });
    } catch (e) {
      // journal should never block UX
      console.warn("journal write failed:", e);
    }
  }

  // ----------------- Data loading -----------------

  async function loadSegms() {
    const rows = await fetchJSON(`/api/review/segms?limit=400`);
    segmSelect.innerHTML = "";
    (rows || []).forEach((r) => {
      const opt = document.createElement("option");
      opt.value = String(r.segm_id);
      opt.textContent = `${r.date} (#${r.segm_id})`;
      segmSelect.appendChild(opt);
    });
  }

  async function loadDefaultSegm() {
    const d = await fetchJSON(`/api/review/default_segm`);
    return d && d.segm_id != null ? Number(d.segm_id) : null;
  }

  async function loadMeta(segmId) {
    return await fetchJSON(`/api/review/segm/${encodeURIComponent(segmId)}/meta`);
  }

  async function loadTicksSample(segmId, targetPoints = 8000) {
    const d = await fetchJSON(
      `/api/review/segm/${encodeURIComponent(segmId)}/ticks_sample?target_points=${encodeURIComponent(targetPoints)}`
    );
    return d && Array.isArray(d.points) ? d.points : [];
  }

  async function loadLines(segmId) {
    const d = await fetchJSON(`/api/review/segm/${encodeURIComponent(segmId)}/lines`);
    return d && Array.isArray(d.lines) ? d.lines : [];
  }

  // ----------------- Chart rendering -----------------

  function ensureChart() {
    if (state.chart) return state.chart;
    const dom = document.getElementById("chart");
    const c = echarts.init(dom, null, { useDirtyRect: true });
    window.addEventListener("resize", () => c.resize());
    state.chart = c;

    // Click on a segline on chart selects it
    c.on("click", (p) => {
      const sid = String(p.seriesId || "");
      if (sid.startsWith("segline_")) {
        const id = Number(sid.replace("segline_", ""));
        if (Number.isFinite(id)) {
          setSelectedLine(id);
        }
      }
    });

    return c;
  }

  function buildPriceSeries() {
    const x = state.ticks.map((t) => t.ts);
    const series = [];

    function addLine(id, name, ykey) {
      const data = state.ticks.map((t) => [t.ts, t[ykey] != null ? Number(t[ykey]) : null]);
      series.push({
        id,
        name,
        type: "line",
        showSymbol: false,
        smooth: false,
        data,
      });
    }

    if (state.vis.mid) addLine("mid", "Mid", "mid");
    if (state.vis.kal) addLine("kal", "Kal", "kal");
    if (state.vis.bid) addLine("bid", "Bid", "bid");
    if (state.vis.ask) addLine("ask", "Ask", "ask");

    return { x, series };
  }

  function buildSegLineSeries() {
    const series = [];
    const selected = state.selectedSegLineId;

    for (const ln of state.lines) {
      const id = Number(ln.id);
      const sid = `segline_${id}`;

      const isSel = selected != null && Number(selected) === id;

      series.push({
        id: sid,
        name: `SegLine ${id}`,
        type: "line",
        showSymbol: false,
        smooth: false,
        // two-point line from start to end
        data: [
          [ln.start_ts, Number(ln.start_price)],
          [ln.end_ts, Number(ln.end_price)],
        ],
        lineStyle: {
          type: "dashed",
          width: isSel ? 4 : 2,
          opacity: isSel ? 1 : 0.65,
        },
        emphasis: { focus: "series" },
        tooltip: {
          trigger: "item",
          formatter: () => {
            return [
              `<b>SegLine ${id}</b>`,
              `depth: ${ln.depth} iter: ${ln.iteration}`,
              `start: ${ln.start_ts}`,
              `end: ${ln.end_ts}`,
              `start_price: ${fmt(ln.start_price, 2)}`,
              `end_price: ${fmt(ln.end_price, 2)}`,
              `#ticks: ${ln.num_ticks ?? ""}`,
              `max|dist|: ${ln.max_abs_dist != null ? fmt(ln.max_abs_dist, 4) : ""}`,
            ].join("<br/>");
          },
        },
      });
    }

    return series;
  }

  function renderChart() {
    const c = ensureChart();
    const { x, series } = buildPriceSeries();
    const segSeries = buildSegLineSeries();

    // y-bounds from visible tick data
    const ys = [];
    for (const t of state.ticks) {
      if (state.vis.mid && t.mid != null) ys.push(Number(t.mid));
      if (state.vis.kal && t.kal != null) ys.push(Number(t.kal));
      if (state.vis.bid && t.bid != null) ys.push(Number(t.bid));
      if (state.vis.ask && t.ask != null) ys.push(Number(t.ask));
    }
    const minY = ys.length ? Math.floor(Math.min(...ys)) : null;
    const maxY = ys.length ? Math.ceil(Math.max(...ys)) : null;

    c.setOption(
      {
        animation: false,
        grid: { left: 50, right: 20, top: 20, bottom: 45 },
        tooltip: { trigger: "axis", axisPointer: { type: "cross" } },
        xAxis: {
          type: "category",
          data: x,
          axisLabel: { formatter: (v) => String(v).slice(11, 19) },
        },
        yAxis: { type: "value", scale: true, min: minY, max: maxY },
        dataZoom: [
          { type: "inside", xAxisIndex: 0, filterMode: "none" },
          { type: "slider", xAxisIndex: 0, filterMode: "none" },
        ],
        series: [...series, ...segSeries],
      },
      { notMerge: true, lazyUpdate: true }
    );
  }

  // ----------------- SegLine list UI -----------------

  function setSelectedLine(id) {
    state.selectedSegLineId = id != null ? Number(id) : null;

    // UI text
    if (state.selectedSegLineId != null) {
      selectedLineText.textContent = `selected: #${state.selectedSegLineId}`;
      btnBreak.disabled = false;
    } else {
      selectedLineText.textContent = "";
      btnBreak.disabled = true;
    }

    // re-render seg list highlight
    renderSegLineList();

    // re-render chart to thicken selected line
    renderChart();
  }

  function renderSegLineList() {
    segLinesList.innerHTML = "";

    if (!state.lines.length) {
      const div = document.createElement("div");
      div.className = "small";
      div.textContent = "(no active seglines yet)";
      segLinesList.appendChild(div);
      return;
    }

    for (const ln of state.lines) {
      const row = document.createElement("div");
      row.className = "seglineRow";
      if (state.selectedSegLineId != null && Number(ln.id) === Number(state.selectedSegLineId)) {
        row.classList.add("selected");
      }

      const left = document.createElement("div");
      left.innerHTML = `<b>#${ln.id}</b> <span class="seglineMeta">depth:${ln.depth} iter:${ln.iteration}</span>`;

      const right = document.createElement("div");
      right.className = "seglineMeta";
      right.style.marginLeft = "auto";
      right.textContent = `max|dist|: ${ln.max_abs_dist != null ? fmt(ln.max_abs_dist, 4) : "null"}   #ticks:${ln.num_ticks ?? ""}`;

      row.appendChild(left);
      row.appendChild(right);

      row.addEventListener("click", () => setSelectedLine(Number(ln.id)));
      segLinesList.appendChild(row);
    }
  }

  // ----------------- Break action -----------------

  async function doBreak() {
    if (state.segmId == null) return;
    if (state.selectedSegLineId == null) return;

    const segm_id = Number(state.segmId);
    const segLine_id = Number(state.selectedSegLineId);

    btnBreak.disabled = true;
    const oldText = btnBreak.textContent;
    btnBreak.textContent = "Working...";

    try {
      // 1) break
      const res = await fetchJSON(`/api/review/breakLine`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ segm_id, segLine_id }),
      });

      // 2) journal
      await writeJournal("break", segm_id, segLine_id, "breakLine called", {
        result: res && res.result ? "ok" : "unknown",
      });

      // 3) reload lines/meta and redraw
      const meta = await loadMeta(segm_id);
      const lines = await loadLines(segm_id);

      state.lines = lines;
      metaText.textContent =
        `segm_id: ${meta.segm_id}  date: ${meta.date}  active lines: ${meta.num_lines_active}  max|dist|: ${meta.global_max_abs_dist ?? "null"}`;

      // keep selection if it still exists; otherwise clear
      const still = state.lines.some((x) => Number(x.id) === segLine_id);
      if (!still) setSelectedLine(null);

      renderSegLineList();
      renderChart();
      await refreshJournal();
    } catch (e) {
      console.error(e);
      await writeJournal("break_error", state.segmId, state.selectedSegLineId, String(e));
      alert("Break failed: " + String(e));
    } finally {
      btnBreak.textContent = oldText || "Break";
      btnBreak.disabled = state.selectedSegLineId == null;
    }
  }

  // ----------------- Controls -----------------

  function hookPill(pillEl, key) {
    pillEl.addEventListener("click", () => {
      state.vis[key] = !state.vis[key];
      pillEl.classList.toggle("on", !!state.vis[key]);
      renderChart();
    });
  }

  // ----------------- Main flow -----------------

  async function loadSegm(segmId) {
    state.segmId = Number(segmId);
    state.selectedSegLineId = null;
    btnBreak.disabled = true;
    selectedLineText.textContent = "";

    const meta = await loadMeta(segmId);
    state.segmDate = meta.date || null;

    metaText.textContent =
      `segm_id: ${meta.segm_id}  date: ${meta.date}  active lines: ${meta.num_lines_active}  max|dist|: ${meta.global_max_abs_dist ?? "null"}`;

    // ticks + lines
    state.ticks = await loadTicksSample(segmId, 12000);
    state.lines = await loadLines(segmId);

    renderSegLineList();
    renderChart();
  }

  async function init() {
    hookPill(btnMid, "mid");
    hookPill(btnKal, "kal");
    hookPill(btnBid, "bid");
    hookPill(btnAsk, "ask");

    btnBreak.addEventListener("click", doBreak);
    btnRefreshJournal.addEventListener("click", refreshJournal);

    await loadSegms();

    let def = null;
    try { def = await loadDefaultSegm(); } catch {}
    if (def != null) segmSelect.value = String(def);

    segmSelect.addEventListener("change", async () => {
      await loadSegm(Number(segmSelect.value));
    });

    await loadSegm(Number(segmSelect.value));
    await refreshJournal();
  }

  init().catch((e) => {
    console.error(e);
    alert("review2 init failed: " + String(e));
  });
})();
