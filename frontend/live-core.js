// PATH: frontend/live-core.js
// Live window viewer for ticks + Kalman + kalseg + zones.
// - Shows last 5000 ticks by default
// - Lets you page backward/forward in windows
// - Colors Kalman line by segment direction
// - Shades background by zone type (TREND / WEAK_TREND / CHOP)

const ApiBase = "/api/live_window";
const WindowSize = 5000;

const ChartEl = document.getElementById("chart");
if (!ChartEl) {
  console.error("live-core.js: #chart element not found");
}

const Chart = ChartEl ? echarts.init(ChartEl) : null;

// in-memory state
let CurrentTicks = [];
let CurrentSegments = [];
let CurrentZones = [];
let CurrentStartId = null;
let CurrentEndId = null;

// ------------- small helpers -------------

function $(id) {
  return document.getElementById(id);
}

function buildUrl(params = {}) {
  const url = new URL(ApiBase, window.location.origin);
  url.searchParams.set("limit", WindowSize.toString());
  if (params.before_id) url.searchParams.set("before_id", params.before_id);
  if (params.after_id) url.searchParams.set("after_id", params.after_id);
  return url.toString();
}

async function fetchWindow(params = {}) {
  const res = await fetch(buildUrl(params));
  if (!res.ok) {
    throw new Error(`HTTP ${res.status} from /api/live_window`);
  }
  return await res.json();
}

function buildTickIndexMap(ticks) {
  const map = new Map();
  for (let i = 0; i < ticks.length; i++) {
    map.set(ticks[i].id, i);
  }
  return map;
}

// ------------- chart setup -------------

function initChart() {
  if (!Chart) return;

  Chart.setOption({
    backgroundColor: "#0d1117",
    animation: false,
    tooltip: {
      trigger: "axis",
      axisPointer: { type: "cross" },
      formatter: (params) => {
        if (!params || !params.length) return "";
        // Prefer the mid series for meta
        const midP = params.find((p) => p.seriesName === "Mid") || params[0];
        const m = midP?.data?.meta;
        if (!m) return "";

        const dt = new Date(m.ts);
        const date = dt.toLocaleDateString();
        const time = dt.toLocaleTimeString();

        const fmt = (v) =>
          v === null || v === undefined ? "" : Number(v).toFixed(2);

        const lines = [
          `id: ${m.id}`,
          `${date} ${time}`,
          `mid: ${fmt(m.mid)}`,
          `kal: ${fmt(m.kal)}`,
        ];
        if (m.bid !== undefined || m.ask !== undefined) {
          lines.push(`bid: ${fmt(m.bid)}`);
          lines.push(`ask: ${fmt(m.ask)}`);
          lines.push(`spread: ${fmt(m.spread)}`);
        }
        return lines.join("<br/>");
      },
    },
    grid: { left: 52, right: 20, top: 30, bottom: 40 },
    xAxis: {
      type: "time",
      axisLabel: { color: "#c9d1d9" },
      axisLine: { lineStyle: { color: "#30363d" } },
      axisPointer: { show: true },
    },
    yAxis: {
      type: "value",
      scale: true,
      minInterval: 1,
      splitNumber: 8,
      axisLabel: {
        color: "#c9d1d9",
        formatter: (v) => String(Math.round(v)),
      },
      splitLine: { lineStyle: { color: "#30363d" } },
      axisPointer: { show: false },
    },
    dataZoom: [
      { type: "inside", xAxisIndex: 0, filterMode: "weakFilter" },
      { type: "slider", xAxisIndex: 0, bottom: 6 },
    ],
    legend: {
      data: ["Mid", "Kalman"],
      textStyle: { color: "#c9d1d9" },
    },
    // visualMap paints Kalman line by segment direction
    visualMap: [
      {
        show: false,
        dimension: 2, // segDir dimension on Kalman series
        seriesIndex: 1,
        pieces: [
          { value: 1, color: "#2ea043" }, // up
          { value: -1, color: "#f85149" }, // down
          { value: 0, color: "#8b949e" }, // flat / no segment
        ],
      },
    ],
    series: [
      {
        // 0: Mid line
        name: "Mid",
        type: "line",
        showSymbol: false,
        lineStyle: { width: 1.2 },
        data: [],
      },
      {
        // 1: Kalman line (with segDir dimension)
        name: "Kalman",
        type: "line",
        showSymbol: false,
        smooth: true,
        data: [], // [ts, kal, segDir]
        encode: { x: 0, y: 1 },
      },
      {
        // 2: TREND zones (greenish)
        name: "TREND",
        type: "line",
        data: [],
        markArea: {
          silent: true,
          itemStyle: { color: "rgba(46, 204, 113, 0.08)" },
          data: [],
        },
      },
      {
        // 3: WEAK_TREND zones (blueish)
        name: "WEAK_TREND",
        type: "line",
        data: [],
        markArea: {
          silent: true,
          itemStyle: { color: "rgba(52, 152, 219, 0.08)" },
          data: [],
        },
      },
      {
        // 4: CHOP zones (yellowish)
        name: "CHOP",
        type: "line",
        data: [],
        markArea: {
          silent: true,
          itemStyle: { color: "rgba(241, 196, 15, 0.08)" },
          data: [],
        },
      },
    ],
  });

  window.addEventListener("resize", () => Chart.resize());
}

function updateChart() {
  if (!Chart || !CurrentTicks.length) return;

  // Build index: tick id -> index in CurrentTicks
  const idxMap = buildTickIndexMap(CurrentTicks);

  // Default segDir=0 (no segment)
  const segDirPerIdx = new Array(CurrentTicks.length).fill(0);

  for (const s of CurrentSegments) {
    const dir = s.direction || 0;
    const startIdx = idxMap.get(s.start_id);
    const endIdx = idxMap.get(s.end_id);
    if (startIdx == null || endIdx == null) continue;
    const from = Math.min(startIdx, endIdx);
    const to = Math.max(startIdx, endIdx);
    for (let i = from; i <= to && i < segDirPerIdx.length; i++) {
      segDirPerIdx[i] = dir;
    }
  }

  const midData = [];
  const kalData = [];

  for (let i = 0; i < CurrentTicks.length; i++) {
    const r = CurrentTicks[i];
    const ts = new Date(r.ts);
    const meta = {
      id: r.id,
      ts: r.ts,
      mid: r.mid,
      kal: r.kal,
      bid: r.bid,
      ask: r.ask,
      spread: r.spread,
    };

    midData.push({ value: [ts, r.mid], meta });
    kalData.push([ts, r.kal, segDirPerIdx[i]]); // segDir in dimension 2
  }

  // Build zone markAreas (TREND / WEAK_TREND / CHOP)
  const trendAreas = [];
  const weakAreas = [];
  const chopAreas = [];

  if (CurrentZones && CurrentZones.length) {
    const firstId = CurrentTicks[0].id;
    const lastId = CurrentTicks[CurrentTicks.length - 1].id;

    for (const z of CurrentZones) {
      const zs = Math.max(z.start_id, firstId);
      const ze = Math.min(z.end_id, lastId);
      if (zs > ze) continue;

      const startIdx = idxMap.get(zs);
      const endIdx = idxMap.get(ze);
      if (startIdx == null || endIdx == null) continue;

      const xs = new Date(CurrentTicks[startIdx].ts);
      const xe = new Date(CurrentTicks[endIdx].ts);

      const area = [{ xAxis: xs }, { xAxis: xe }];

      switch (z.zone_type) {
        case "TREND":
          trendAreas.push(area);
          break;
        case "CHOP":
          chopAreas.push(area);
          break;
        case "WEAK_TREND":
        default:
          weakAreas.push(area);
          break;
      }
    }
  }

  Chart.setOption({
    series: [
      { data: midData },
      { data: kalData },
      { markArea: { data: trendAreas } },
      { markArea: { data: weakAreas } },
      { markArea: { data: chopAreas } },
    ],
  });
}

// ------------- window loading -------------

async function loadLatestWindow() {
  setStatus("Loading latest…");
  try {
    const data = await fetchWindow({});
    applyWindow(data, true);
  } catch (err) {
    console.error(err);
    setStatus("Error loading latest window");
  }
}

async function loadPrevWindow() {
  if (CurrentStartId == null) return;
  const beforeId = CurrentStartId - 1;
  setStatus(`Loading before ${beforeId}…`);
  try {
    const data = await fetchWindow({ before_id: beforeId });
    applyWindow(data, false);
  } catch (err) {
    console.error(err);
    setStatus("Error loading previous window");
  }
}

async function loadNextWindow() {
  if (CurrentEndId == null) return;
  const afterId = CurrentEndId + 1;
  setStatus(`Loading after ${afterId}…`);
  try {
    const data = await fetchWindow({ after_id: afterId });
    applyWindow(data, false);
  } catch (err) {
    console.error(err);
    setStatus("Error loading next window");
  }
}

function applyWindow(data, isLive) {
  CurrentTicks = data.ticks || [];
  CurrentSegments = data.segments || [];
  CurrentZones = data.zones || [];

  if (!CurrentTicks.length) {
    setStatus("No ticks for this window");
    updateChart();
    return;
  }

  CurrentStartId = CurrentTicks[0].id;
  CurrentEndId = CurrentTicks[CurrentTicks.length - 1].id;

  updateChart();

  setStatus(
    (isLive ? "Live window" : "Window") +
      ` · tick ids ${CurrentStartId} – ${CurrentEndId} · ` +
      `${CurrentTicks.length} ticks · ` +
      `${CurrentSegments.length} segs · ${CurrentZones.length} zones`
  );
}

// ------------- UI wiring -------------

function setStatus(msg) {
  const el = $("status");
  if (el) el.textContent = msg;
}

function initControls() {
  const btnLive = $("btnLive");
  const btnPrev = $("btnPrev");
  const btnNext = $("btnNext");

  if (btnLive) btnLive.addEventListener("click", loadLatestWindow);
  if (btnPrev) btnPrev.addEventListener("click", loadPrevWindow);
  if (btnNext) btnNext.addEventListener("click", loadNextWindow);
}

// ------------- bootstrap -------------

window.addEventListener("DOMContentLoaded", async () => {
  if (!Chart) return;
  initChart();
  initControls();
  await loadLatestWindow();
});
