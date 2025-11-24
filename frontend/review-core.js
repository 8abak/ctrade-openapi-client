// PATH: frontend/review-core.js
// Review chart – nothing loads by default.
// When you enter a "from tick id" and a "window", it loads:
//   - ticks (mid + kal + bid/ask/spread)
//   - kalseg (as vertical dashed lines)
//   - zones (as background bands)
// for [from_id, from_id + window - 1].

const API = "/api";
const chart = echarts.init(document.getElementById("chart"));

let currentData = {
  ticks: [],
  kalseg: [],
  zones: [],
};

function zoneColor(type) {
  switch (type) {
    case "TREND":
      return "rgba(46, 160, 67, 0.10)"; // greenish
    case "WEAK_TREND":
      return "rgba(139, 148, 158, 0.10)"; // gray
    case "CHOP":
      return "rgba(248, 81, 73, 0.08)"; // reddish
    default:
      return "rgba(56, 139, 253, 0.06)"; // bluish fallback
  }
}

function setupChart() {
  chart.setOption({
    backgroundColor: "#0d1117",
    animation: false,
    tooltip: {
      trigger: "axis",
      axisPointer: { type: "line" },
      formatter: (params) => {
        if (!params || !params.length) return "";
        // Prefer mid series
        const p =
          params.find((x) => x.seriesName === "Mid") ||
          params.find((x) => x.seriesName === "Kalman") ||
          params[0];
        const d = p && p.data ? p.data.meta : null;
        if (!d) return "";
        const dt = new Date(d.ts);
        const date = dt.toLocaleDateString();
        const time = dt.toLocaleTimeString();
        const fmt = (v) =>
          v === null || v === undefined ? "" : Number(v).toFixed(2);
        const lines = [
          `id: ${d.id}`,
          `${date} ${time}`,
          `mid: ${fmt(d.mid)}`,
          `kal: ${fmt(d.kal)}`,
          `bid: ${fmt(d.bid)}`,
          `ask: ${fmt(d.ask)}`,
          `spread: ${fmt(d.spread)}`,
        ];
        return lines.join("<br/>");
      },
    },
    grid: { left: 52, right: 24, top: 24, bottom: 48 },
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
    },
    dataZoom: [
      { type: "inside", xAxisIndex: 0, filterMode: "weakFilter" },
      { type: "slider", xAxisIndex: 0, bottom: 6 },
    ],
    series: [
      {
        name: "Mid",
        type: "line",
        showSymbol: false,
        lineStyle: { width: 1.3 },
        data: [],
      },
      {
        name: "Kalman",
        type: "line",
        showSymbol: false,
        lineStyle: { width: 1.3 },
        data: [],
        markLine: {
          symbol: "none",
          silent: true,
          lineStyle: { width: 1, type: "dashed", color: "#8b949e" },
          data: [], // filled later from kalseg
        },
        markArea: {
          silent: true,
          itemStyle: { opacity: 0.08 },
          data: [], // filled later from zones
        },
      },
    ],
  });
}

function render(data) {
  currentData = data || currentData;
  const { ticks, kalseg, zones } = currentData;

  if (!ticks || !ticks.length) {
    chart.setOption({
      series: [{ data: [] }, { data: [], markLine: { data: [] }, markArea: { data: [] } }],
    });
    return;
  }

  const midSeries = [];
  const kalSeries = [];

  for (const t of ticks) {
    const point = {
      value: [t.ts, t.mid],
      meta: t,
    };
    midSeries.push(point);

    const kalVal =
      t.kal === null || t.kal === undefined ? null : Number(t.kal);
    kalSeries.push({
      value: [t.ts, kalVal],
      meta: t,
    });
  }

  // Build kalseg vertical dashed lines anchored on Kalman series
  const segLines = (kalseg || []).map((s) => ({
    xAxis: s.start_ts,
    lineStyle: {
      color: s.direction > 0 ? "#2ea043" : "#f85149",
      width: 1,
      type: "dashed",
    },
  }));

  // Build zone markAreas as background bands
  const zoneAreas = (zones || []).map((z) => [
    {
      xAxis: z.start_ts,
      itemStyle: { color: zoneColor(z.zone_type || "OTHER") },
    },
    { xAxis: z.end_ts },
  ]);

  chart.setOption({
    series: [
      {
        name: "Mid",
        data: midSeries,
      },
      {
        name: "Kalman",
        data: kalSeries,
        markLine: {
          ...chart.getOption().series[1].markLine,
          data: segLines,
        },
        markArea: {
          ...chart.getOption().series[1].markArea,
          data: zoneAreas,
        },
      },
    ],
  });
}

async function loadWindow(fromId, win) {
  const btn = document.getElementById("go");
  btn.disabled = true;
  try {
    const url = `${API}/review/window?from_id=${fromId}&window=${win}`;
    const resp = await fetch(url);
    if (!resp.ok) {
      const txt = await resp.text();
      console.error("Backend error", resp.status, txt);
      alert("Error loading data – see console for details.");
      return;
    }
    const payload = await resp.json();
    render({
      ticks: payload.ticks || [],
      kalseg: payload.kalseg || [],
      zones: payload.zones || [],
    });
  } catch (e) {
    console.error(e);
    alert("Error loading data – see console for details.");
  } finally {
    btn.disabled = false;
  }
}

// --- wiring & bootstrap ---

document.getElementById("go").addEventListener("click", () => {
  const fromId = parseInt(document.getElementById("fromId").value, 10);
  const win = parseInt(document.getElementById("win").value, 10);
  if (!fromId || !win) {
    alert("Please enter both from-id and window.");
    return;
  }
  loadWindow(fromId, win);
});

window.addEventListener("resize", () => chart.resize());

setupChart();
// intentionally no auto-load – you choose the id/window manually
