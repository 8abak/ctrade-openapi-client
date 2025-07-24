// ✅ UPDATED ztick-core.js — Fixes time alignment + chart refresh

let chart;
let dataMid = [], dataAsk = [], dataBid = [], labelSeries = [], selectedTickIds = [];
let lastChecked = "";

function initializeChart() {
  chart = echarts.init(document.getElementById("main"));
  chart.setOption({
    backgroundColor: "#111",
    tooltip: {
      trigger: 'axis',
      backgroundColor: '#222',
      borderColor: '#555',
      borderWidth: 1,
      textStyle: { color: '#fff', fontSize: 13 },
      formatter: function (params) {
        const d = new Date(params[0].value[0]);
        const timeStr = d.toLocaleTimeString("en-AU", { timeZone: "Australia/Sydney", hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });
        return `${timeStr}<br>${params.map(p => `${p.seriesName}: ${p.value[1]}`).join('<br>')}`;
      }
    },
    xAxis: {
      type: 'time',
      axisLabel: {
        color: '#ccc',
        formatter: function (val) {
          const d = new Date(val);
          return d.toLocaleTimeString("en-AU", { timeZone: "Australia/Sydney", hour: '2-digit', minute: '2-digit' });
        }
      },
      splitLine: { lineStyle: { color: '#333' } }
    },
    yAxis: {
      type: 'value',
      scale: true,
      axisLabel: { color: '#ccc' },
      splitLine: { lineStyle: { color: '#333' } }
    },
    dataZoom: [
      { type: 'inside' },
      { type: 'slider', height: 40, bottom: 0 }
    ],
    series: []
  });

  chart.on("click", function (params) {
    const id = params?.value?.[2];
    if (!id) return;
    selectedTickIds = [id];
    document.getElementById("selectedIdsText").textContent = id;
    updateZSeries(); // ensures re-render for thickness
  });
}

function parseDatetime(inputId) {
  const val = document.getElementById(inputId).value;
  return new Date(val);
}

function setDefaultTimeRange() {
  const now = new Date();
  const tenMinAgo = new Date(now.getTime() - 10 * 60 * 1000);
  document.getElementById("startTime").value = tenMinAgo.toISOString().slice(0, 16);
  document.getElementById("endTime").value = now.toISOString().slice(0, 16);
}

async function loadZTickChart() {
  const start = parseDatetime("startTime");
  const end = parseDatetime("endTime");
  const res = await fetch(`/ticks/range?start=${start.toISOString()}&end=${end.toISOString()}`);
  const ticks = await res.json();
  const parseTime = ts => Date.parse(ts);
  dataMid = ticks.map(t => [parseTime(t.timestamp), t.mid, t.id, ((t.ask - t.bid) / 2).toFixed(2)]);
  dataAsk = ticks.map(t => [parseTime(t.timestamp), t.ask, t.id]);
  dataBid = ticks.map(t => [parseTime(t.timestamp), t.bid, t.id]);
  chart.clear();
  updateZSeries();
}

function updateZSeries() {
  const mid = document.getElementById('midCheckbox').checked;
  const ask = document.getElementById('askCheckbox').checked;
  const bid = document.getElementById('bidCheckbox').checked;
  const checked = Array.from(document.querySelectorAll(".labelCheckbox:checked")).map(e => e.value).join(",");
  const state = `${mid}${ask}${bid}:${checked}:${selectedTickIds.join(',')}`;
  if (state === lastChecked) return;
  lastChecked = state;

  const base = [];
  if (ask) base.push({ name: 'Ask', type: 'scatter', symbolSize: 1, itemStyle: { color: '#f5a623' }, data: dataAsk });
  if (mid) base.push({ name: 'Mid', type: 'scatter', symbolSize: 1, itemStyle: { color: '#00bcd4' }, data: dataMid });
  if (bid) base.push({ name: 'Bid', type: 'scatter', symbolSize: 1, itemStyle: { color: '#4caf50' }, data: dataBid });

  const extras = labelSeries.filter(s => checked.includes(s.name));
  const selected = dataMid.filter(d => selectedTickIds.includes(d[2])).map(d => [d[0], d[1], d[2]]);
  if (selected.length)
    extras.push({ name: 'Selected', type: 'scatter', symbolSize: 7, itemStyle: { color: '#ff0', borderColor: '#fff', borderWidth: 1 }, data: selected });

  chart.setOption({ series: [...base, ...extras] }, { replaceMerge: ['series'], lazyUpdate: false });
}

async function loadLabelCheckboxes() {
  const container = document.getElementById("labelCheckboxes");
  const selector = document.getElementById("labelTableSelect");
  const tables = await fetch("/available").then(r => r.json());
  container.innerHTML = "";
  selector.innerHTML = "";
  for (const name of tables) {
    const id = `label_${name}`;
    container.insertAdjacentHTML('beforeend', `<label><input type="checkbox" class="labelCheckbox" value="${name}" onchange="updateZSeries()"> ${name}</label><br>`);
    selector.insertAdjacentHTML('beforeend', `<option value="${name}">${name}</option>`);
    const data = await fetch(`/labels/${name}`).then(r => r.json()).catch(() => []);
    const points = data.map(row => {
      const match = dataMid.find(p => p[2] === row.tickid);
      return match ? [match[0], 1, row.tickid] : null;
    }).filter(Boolean);
    labelSeries.push({ name, type: 'scatter', symbolSize: 6, itemStyle: { color: '#e91e63' }, data: points });
  }
}

async function submitLabel() {
  const table = document.getElementById("labelTableSelect").value;
  const note = document.getElementById("labelNote").value;
  const extraInput = document.getElementById("customTickIds").value;
  let extraIds = [];
  if (extraInput.includes("-")) {
    const [start, end] = extraInput.split("-").map(Number);
    extraIds = Array.from({ length: end - start + 1 }, (_, i) => start + i);
  } else if (extraInput.includes(",")) {
    extraIds = extraInput.split(",").map(x => parseInt(x.trim())).filter(Boolean);
  } else if (extraInput) {
    extraIds = [parseInt(extraInput)];
  }
  const idsToSubmit = [...selectedTickIds, ...extraIds];
  if (!table || idsToSubmit.length === 0) return;
  await fetch("/labels/assign", {
    method: "POST",
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ table, ids: idsToSubmit, note })
  });
  alert("Labels submitted");
}

function exportSelectedTicks() {
  const selected = dataMid.filter(d => selectedTickIds.includes(d[2]));
  if (selected.length === 0) return;
  const csv = ["timestamp,mid,id,spread"];
  selected.forEach(r => {
    csv.push(`${new Date(r[0]).toISOString()},${r[1]},${r[2]},${r[3]}`);
  });
  const blob = new Blob([csv.join("\n")], { type: "text/csv" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "selected_ticks.csv";
  a.click();
  URL.revokeObjectURL(url);
}

function loadVersion() {
  fetch("/version")
    .then(res => res.json())
    .then(v => {
      const val = v["ztick"] || {};
      const html = `J: ${val.js?.datetime || '-'} ${val.js?.message || ''}<br>` +
                   `B: ${val.py?.datetime || '-'} ${val.py?.message || ''}<br>` +
                   `H: ${val.html?.datetime || '-'} ${val.html?.message || ''}`;
      document.getElementById("version").innerHTML = html;
    })
    .catch(() => {
      document.getElementById("version").innerText = "Version: unknown";
    });
}

window.addEventListener("DOMContentLoaded", () => {
  initializeChart();
  setDefaultTimeRange();
  loadVersion();
  loadLabelCheckboxes();
  ["askCheckbox", "midCheckbox", "bidCheckbox"].forEach(id => {
    document.getElementById(id).addEventListener("change", updateZSeries);
  });
});
