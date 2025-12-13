// frontend/review-core.js
// Controller for segLines review page

let currentSegmId = null;
let selectedSegLineId = null;
let segLinesVisible = true;

document.addEventListener("DOMContentLoaded", init);

async function init() {
  ChartCore.init("chart");

  await loadSegmList();
  bindUI();
}

function bindUI() {
  document.querySelectorAll(".toggle[data-series]").forEach(btn => {
    btn.addEventListener("click", () => {
      const s = btn.dataset.series;
      const on = !btn.classList.contains("on");
      btn.classList.toggle("on", on);
      btn.classList.toggle("off", !on);
      ChartCore.setVisibility(s, on);
    });
  });

  document.getElementById("toggleSegLines").addEventListener("click", e => {
    segLinesVisible = !segLinesVisible;
    e.target.classList.toggle("on", segLinesVisible);
    e.target.classList.toggle("off", !segLinesVisible);
    ChartCore.setSegLinesVisibility(segLinesVisible);
  });

  document.getElementById("evalLevel").addEventListener("change", loadEvals);

  document.getElementById("breakBtn").addEventListener("click", doBreak);
}

async function loadSegmList() {
  const res = await fetch("/api/review/segms");
  const rows = await res.json();

  const sel = document.getElementById("segmSelect");
  sel.innerHTML = "";

  rows.forEach(r => {
    const opt = document.createElement("option");
    opt.value = r.segm_id;
    opt.textContent = `${r.date} (#${r.segm_id})`;
    sel.appendChild(opt);
  });

  sel.addEventListener("change", () => selectSegm(Number(sel.value)));

  const dflt = await fetch("/api/review/default_segm").then(r => r.json());
  sel.value = dflt.segm_id;
  await selectSegm(dflt.segm_id);
}

async function selectSegm(segmId) {
  currentSegmId = segmId;
  selectedSegLineId = null;

  const ticks = await fetch(`/api/review/segm/${segmId}/ticks_sample`).then(r => r.json());
  ChartCore.setTicks(ticks.points);

  await loadLines();
  await loadMeta();
  await loadEvals();
}

async function loadLines() {
  const res = await fetch(`/api/review/segm/${currentSegmId}/lines`);
  const data = await res.json();

  ChartCore.setSegLines(data.lines, selectedSegLineId);
  renderLinesTable(data.lines);
}

function renderLinesTable(lines) {
  const tbody = document.querySelector("#linesTable tbody");
  tbody.innerHTML = "";

  lines.forEach(ln => {
    const tr = document.createElement("tr");
    if (ln.id === selectedSegLineId) tr.classList.add("selected");

    const slope = ln.duration_ms
      ? (ln.end_price - ln.start_price) / (ln.duration_ms / 1000)
      : null;

    tr.innerHTML = `
      <td>${ln.id}</td>
      <td>${ln.depth}</td>
      <td>${ln.iteration}</td>
      <td>${ln.start_ts.slice(11,19)}</td>
      <td>${ln.end_ts.slice(11,19)}</td>
      <td>${ln.duration_ms ?? ""}</td>
      <td>${slope != null ? slope.toFixed(6) : ""}</td>
      <td>${ln.num_ticks ?? ""}</td>
      <td>${ln.max_abs_dist != null ? ln.max_abs_dist.toFixed(4) : ""}</td>
    `;

    tr.addEventListener("click", () => {
      selectedSegLineId = ln.id;
      ChartCore.setSegLines(lines, selectedSegLineId);
      renderLinesTable(lines);
    });

    tbody.appendChild(tr);
  });
}

async function loadMeta() {
  const m = await fetch(`/api/review/segm/${currentSegmId}/meta`).then(r => r.json());
  document.getElementById("footerMeta").textContent =
    `segm_id: ${m.segm_id} · date: ${m.date} · active lines: ${m.num_lines_active} · max |dist|: ${m.global_max_abs_dist}`;
}

async function loadEvals() {
  const lvl = Number(document.getElementById("evalLevel").value);
  if (lvl <= 0) {
    ChartCore.setEvals([], 1);
    return;
  }

  const meta = await fetch(`/api/review/segm/${currentSegmId}/meta`).then(r => r.json());
  if (!meta.tick_from || !meta.tick_to) return;

  const res = await fetch(
    `/api/evals/window?tick_from=${meta.tick_from}&tick_to=${meta.tick_to}&min_level=${lvl}`
  );
  const data = await res.json();
  ChartCore.setEvals(data.evals || [], lvl);
}

async function doBreak() {
  const btn = document.getElementById("breakBtn");
  btn.disabled = true;
  btn.textContent = "Working…";

  try {
    const payload = {
      segm_id: currentSegmId,
      segLine_id: selectedSegLineId
    };

    const res = await fetch("/api/review/breakLine", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });

    await res.json();
    await loadLines();
    await loadMeta();
  } finally {
    btn.disabled = false;
    btn.textContent = "Break";
  }
}
