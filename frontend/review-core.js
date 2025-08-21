// review-core.js

const $ = (sel) => document.querySelector(sel);

const state = {
  start: 1,
  chunk: 5000,
  offset: 0,
  series: {},
  labelCatalog: [],
  enabled: {
    raw: true,
    k1: true,
    k1_rts: false,
    k2_cv: false
  }
};

// ----- HTTP helper -----
async function getJSON(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
  return r.json();
}

// ----- UI wiring -----
function addLabelToggles(catalog) {
  const wrap = document.getElementById("labelsWrap");
  wrap.innerHTML = "";

  const builtins = [
    { id: "chk-raw", name: "Raw", key: "raw" },
    { id: "chk-k1", name: "Kalman (k1)", key: "k1" },
    { id: "chk-k1rts", name: "RTS (k1_rts)", key: "k1_rts" },
    { id: "chk-k2cv", name: "CV (k2_cv)", key: "k2_cv" }
  ];
  for (const b of builtins) {
    const div = document.createElement("div");
    div.className = "label-toggle";
    div.innerHTML = `<label><input type="checkbox" id="${b.id}"> ${b.name}</label>`;
    wrap.appendChild(div);
    const el = document.getElementById(b.id);
    el.checked = !!state.enabled[b.key];
    el.onchange = () => {
      state.enabled[b.key] = el.checked;
      draw();
    };
  }

  if (!catalog || catalog.length === 0) return;
  const hr = document.createElement("hr");
  wrap.appendChild(hr);

  for (const t of catalog) {
    const group = document.createElement("div");
    group.className = "label-group";
    group.innerHTML = `<div class="group-title">${t.table}</div>`;
    wrap.appendChild(group);

    for (const col of t.labels) {
      const key = `${t.table}.${col}`;
      const id = `chk-${t.table}-${col}`;
      const row = document.createElement("div");
      row.className = "label-toggle";
      row.innerHTML = `<label><input type="checkbox" id="${id}"> ${col}</label>`;
      group.appendChild(row);

      const el = document.getElementById(id);
      el.onchange = async () => {
        const on = el.checked;
        if (on && !state.series[key]) {
          const data = await getJSON(`/api/sql?query=${encodeURIComponent(
            `SELECT tickid AS x, ${col} AS y FROM "${t.table}" WHERE ${col} IS NOT NULL ORDER BY tickid`
          )}`);
          state.series[key] = Array.isArray(data)
            ? data.map((d) => ({ x: d.x, y: Number(d.y) }))
            : [];
        }
        draw();
      };
    }
  }
}

// ----- Data fetch -----
async function loadChunk() {
  const url = `/ml/review?start=${state.start}&offset=${state.offset}&limit=${state.chunk}`;
  const bundle = await getJSON(url);

  state.series.raw = bundle.series.raw || [];
  state.series.k1 = bundle.series.k1 || [];
  state.series.k1_rts = bundle.series.k1_rts || [];
  state.series.k2_cv = bundle.series.k2_cv || [];

  if (state.labelCatalog.length === 0) {
    try {
      state.labelCatalog = await getJSON("/api/labels/schema");
    } catch {
      state.labelCatalog = [];
    }
    addLabelToggles(state.labelCatalog);
  }
}

// ----- Drawing -----
let chart;
function initChart() {
  chart = echarts.init(document.getElementById("chart"));
  window.addEventListener("resize", () => chart && chart.resize());
}

function seriesFrom(key, name) {
  const data = state.series[key] || [];
  return {
    name,
    type: "line",
    showSymbol: false,
    data: data.map((p) => [p.x, p.y]),
    lineStyle: { width: 1.5 }
  };
}

function draw() {
  if (!chart) return;
  const s = [];
  if (state.enabled.raw) s.push(seriesFrom("raw", "Raw"));
  if (state.enabled.k1) s.push(seriesFrom("k1", "Kalman (k1)"));
  if (state.enabled.k1_rts) s.push(seriesFrom("k1_rts", "RTS (k1_rts)"));
  if (state.enabled.k2_cv) s.push(seriesFrom("k2_cv", "CV (k2_cv)"));

  Object.keys(state.series).forEach((k) => {
    if (k.includes(".") && state.series[k] && state.series[k].length) {
      s.push(seriesFrom(k, k));
    }
  });

  chart.setOption({
    tooltip: { trigger: "axis", axisPointer: { type: "cross" } },
    xAxis: { type: "value", name: "tick" },
    yAxis: { type: "value", name: "price", scale: true },
    grid: { left: 45, right: 15, top: 40, bottom: 30 },
    legend: { top: 5 },
    series: s
  });
}

// ----- Boot -----
async function boot() {
  state.start = Number($("#trainStart").value || 1);
  state.chunk = Number($("#chunkSize").value || 5000);
  initChart();
  await loadChunk();
  draw();
}

document.addEventListener("DOMContentLoaded", () => {
  document.getElementById("btnLoad").onclick = async () => {
    state.start = Number($("#trainStart").value || 1);
    state.chunk = Number($("#chunkSize").value || 5000);
    await loadChunk();
    draw();
  };
  boot();
});
