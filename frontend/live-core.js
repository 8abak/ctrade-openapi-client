// PATH: frontend/live-core.js
// Real-time viewer using /api/live_window
// Keeps zoom, keeps view position, appends new ticks efficiently
(function() {
  /* global echarts */

  // --- DOM elements ---
  const chartEl = document.getElementById("chart");
  const chart = echarts.init(chartEl);
  const btnReload = document.getElementById("btnReload");
  const btnJump = document.getElementById("btnJump");
  const jumpInput = document.getElementById("jumpId");
  const statusEl = document.getElementById("status");
  const chkKal = document.getElementById("chkKal");
  const chkZones = document.getElementById("chkZones");
  const chkSegs = document.getElementById("chkSegs");

  // --- State ---
  let ticks = []; // {id, ts, mid, kal, ...}
  let zones = [];
  let segs = [];
  let lastSeenId = null;
  let polling = null;

  // CHANGED: default window size from 6000 to 2000
  const WINDOW_DEFAULT = 2000;

  // ------------------------------------------
  // Helpers
  // ------------------------------------------
  function setStatus(msg) {
    statusEl.textContent = msg;
  }

  function safeInt(v) {
    const n = Number(v);
    return Number.isFinite(n) ? Math.floor(n) : null;
  }

  async function apiLiveWindow(args = {}) {
    // args: { limit, before_id, after_id }
    const p = new URLSearchParams();
    if (args.limit) p.set("limit", args.limit);
    if (args.before_id) p.set("before_id", args.before_id);
    if (args.after_id) p.set("after_id", args.after_id);
    const url = "/api/live_window?" + p.toString();
    const resp = await fetch(url);
    if (!resp.ok) throw new Error(await resp.text());
    return resp.json();
  }

  // ------------------------------------------
  // Build segment arrow points on kalman line
  // ------------------------------------------
  function buildSegPoints(ticksArr, segArr) {
    const byId = new Map(ticksArr.map(t => [t.id, t]));
    const pts = [];
    for (const s of segArr) {
      let st = byId.get(s.start_id);
      if (!st) {
        st = ticksArr.find(t => t.id >= s.start_id);
        if (!st) continue;
      }
      const price = st.kal != null ? st.kal : st.mid;
      const dir = (s.direction || "").toString().toLowerCase();
      pts.push({
        value: [st.ts, price],
        direction: dir,
        symbolRotate: (dir === "up" || dir === "1" || dir === "u") ? 0 : 180,
      });
    }
    return pts;
  }

  // ------------------------------------------
  // Build zone rectangles
  // ------------------------------------------
  function buildZones(ticksArr, zoneArr) {
    const bands = [];
    for (const z of zoneArr) {
      let min = Infinity, max = -Infinity;
      let tsStart = null, tsEnd = null;
      for (const t of ticksArr) {
        if (t.id < z.start_id || t.id > z.end_id) continue;
        const p = t.mid;
        if (p < min) min = p;
        if (p > max) max = p;
        if (!tsStart) tsStart = t.ts;
        tsEnd = t.ts;
      }
      if (!tsStart || !tsEnd) continue;

      let c = "rgba(56,139,253,0.18)";
      const d = (z.direction || "").toString().toLowerCase();
      if (d === "up" || d === "1" || d === "u") c = "rgba(46,160,67,0.18)";
      if (d === "down" || d === "dn" || d === "-1") c = "rgba(248,81,73,0.18)";

      bands.push({
        coord: [tsStart, tsEnd, min, max],
        color: c,
      });
    }
    return bands;
  }

  // ------------------------------------------
  // Render chart
  // ------------------------------------------
  function renderChart() {
    if (!ticks.length) {
      chart.setOption({
        backgroundColor: "#0d1117",
        series: [],
      });
      return;
    }

    const showKal = chkKal.checked;
    const showZones = chkZones.checked;
    const showSegs = chkSegs.checked;

    const mid = ticks.map(t => [t.ts, t.mid]);
    const kal = ticks.map(t => [t.ts, t.kal ?? t.mid]);
    const segPts = showSegs ? buildSegPoints(ticks, segs) : [];
    const zoneBands = showZones ? buildZones(ticks, zones) : [];

    const series = [];

    // zone rectangles
    if (showZones && zoneBands.length) {
      series.push({
        type: "custom",
        name: "Zones",
        renderItem: function(params, api) {
          const d = zoneBands[params.dataIndex];
          const [t1, t2, y1, y2] = d.coord;
          const p1 = api.coord([t1, y1]);
          const p2 = api.coord([t2, y2]);
          return {
            type: "rect",
            shape: echarts.graphic.clipRectByRect(
              {
                x: Math.min(p1[0], p2[0]),
                y: Math.min(p1[1], p2[1]),
                width: Math.abs(p2[0] - p1[0]),
                height: Math.abs(p2[1] - p1[1]),
              },
              {
                x: params.coordSys.x,
                y: params.coordSys.y,
                width: params.coordSys.width,
                height: params.coordSys.height,
              }
            ),
            style: { fill: d.color },
          };
        },
        data: zoneBands,
        silent: true,
        z: 0,
      });
    }

    // mid
    series.push({
      name: "Mid",
      type: "line",
      showSymbol: false,
      data: mid,
      lineStyle: { width: 1 },
      z: 1,
    });

    // kal
    if (showKal) {
      series.push({
        name: "Kalman",
        type: "line",
        showSymbol: false,
        data: kal,
        lineStyle: { width: 1 },
        z: 2,
      });
    }

    // segs
    if (showSegs && segPts.length) {
      series.push({
        name: "Segments",
        type: "scatter",
        symbol: "triangle",
        symbolSize: 12,
        data: segPts,
        itemStyle: {
          color: (d) => {
            const dir = (d.data.direction || "").toLowerCase();
            if (dir === "up" || dir === "1" || dir === "u") return "#2ea043";
            if (dir === "down" || dir === "dn" || dir === "-1") return "#f85149";
            return "#8b949e";
          },
        },
        z: 3,
      });
    }

    chart.setOption({
      backgroundColor: "#0d1117",
      legend: {
        top: 4,
        textStyle: { color: "#c9d1d9", fontSize: 11 },
      },
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "cross" },
      },
      grid: {
        left: 60,
        right: 20,
        top: 35,
        bottom: 60,
      },
      xAxis: {
        type: "time",
        axisLine: { lineStyle: { color: "#8b949e" } },
        axisLabel: { color: "#8b949e" },
      },
      yAxis: {
        type: "value",
        scale: true,
        axisLine: { lineStyle: { color: "#8b949e" } },
        axisLabel: { color: "#8b949e" },
      },
      dataZoom: [
        { type: "inside", throttle: 50 },
        { type: "slider", bottom: 30, height: 18 },
      ],
      series,
    });
  }

  // ------------------------------------------
  // Load latest N ticks
  // ------------------------------------------
  async function loadLatest() {
    const data = await apiLiveWindow({ limit: WINDOW_DEFAULT });

    ticks = data.ticks.map(t => ({
      ...t,
      id: Number(t.id),
      mid: Number(t.mid),
      kal: t.kal != null ? Number(t.kal) : null,
    }));
    zones = data.zones || [];
    segs = data.segments || [];

    if (ticks.length) lastSeenId = ticks[ticks.length - 1].id;

    setStatus(`Loaded ${ticks.length} ticks`);
    renderChart();
  }

  // ------------------------------------------
  // Poll for new ticks
  // ------------------------------------------
  async function pollNew() {
    if (!lastSeenId) return;
    try {
      const data = await apiLiveWindow({
        after_id: lastSeenId + 1,
        limit: 5000,
      });
      if (!data.ticks.length) return;

      const newticks = data.ticks.map(t => ({
        ...t,
        id: Number(t.id),
        mid: Number(t.mid),
        kal: t.kal != null ? Number(t.kal) : null,
      }));

      ticks.push(...newticks);
      zones.push(...(data.zones || []));
      segs.push(...(data.segments || []));
      lastSeenId = newticks[newticks.length - 1].id;

      setStatus(`+${newticks.length} ticks`);
      renderChart();
    } catch (err) {
      console.error("Polling error", err);
    }
  }

  // ------------------------------------------
  // Jump to id
  // ------------------------------------------
  async function jumpTo() {
    const id = safeInt(jumpInput.value);
    if (!id) return;

    const data = await apiLiveWindow({
      after_id: id,
      limit: WINDOW_DEFAULT,
    });

    ticks = data.ticks.map(t => ({
      ...t,
      id: Number(t.id),
      mid: Number(t.mid),
      kal: t.kal != null ? Number(t.kal) : null,
    }));
    zones = data.zones || [];
    segs = data.segments || [];

    if (ticks.length) lastSeenId = ticks[ticks.length - 1].id;

    renderChart();
    setStatus(`Jumped to ${id}`);
  }

  // ------------------------------------------
  // Events
  // ------------------------------------------
  btnReload.onclick = loadLatest;
  btnJump.onclick = jumpTo;
  chkKal.onchange = renderChart;
  chkZones.onchange = renderChart;
  chkSegs.onchange = renderChart;
  window.onresize = () => chart.resize();

  // ------------------------------------------
  // Start
  // ------------------------------------------
  (async () => {
    await loadLatest();
    polling = setInterval(pollNew, 1500);
  })();
})();
