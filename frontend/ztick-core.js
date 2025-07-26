// ✅ FINAL ztick-core.js with reload + label glow support

let chart;
let dataMid = [], dataAsk = [], dataBid = [], labelSeries = [], selectedTickIds = [], customHighlightIds = [];
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
      formatter: (params) => {
        const p = params[0];
        const date = new Date(p.value[0]);
        const timeStr = date.toLocaleTimeString('en-AU', {
          hour: '2-digit',
          minute: '2-digit',
          second: '2-digit',
          hour12: false
        });
        const dateStr = date.toLocaleDateString('en-AU', {
          day: '2-digit',
          month: '2-digit',
          year: 'numeric'
        });
        return `<div style="padding: 8px;">
          <strong>${timeStr}</strong><br>
          <span style="color: #ccc;">${dateStr}</span><br>
          Price: <strong style="color: #3fa9f5;">${p.value[1]?.toFixed(2)}</strong><br>
          ID: <span style="color:#aaa;">${p.value[2]}</span>
        </div>`;
      }
    },
    xAxis: { type: 'time', axisLabel: { color: '#ccc' }, splitLine: { lineStyle: { color: '#333' } } },
    yAxis: { type: 'value', scale: true, axisLabel: { color: '#ccc' }, splitLine: { lineStyle: { color: '#333' } } },
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
    updateZSeries();
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
  const startId = document.getElementById("startId").value.trim();
  const endId = document.getElementById("endId").value.trim();
  const startTime = document.getElementById("startTime").value.trim();
  const endTime = document.getElementById("endTime").value.trim();

  let url;

  if (startId && endId) {
    url = `/ticks/between-ids?start=${startId}&end=${endId}`;
  } else if (startTime && endTime) {
    const start = new Date(startTime);
    const end = new Date(endTime);
    url = `/ticks/range?start=${start.toISOString()}&end=${end.toISOString()}`;
  } else {
    alert("Please fill either both times or both IDs.");
    return;
  }

  const res = await fetch(url);
  const ticks = await res.json();
  const parseTime = ts => Date.parse(ts);
  dataMid = ticks.map(t => [parseTime(t.timestamp), t.mid, t.id, ((t.ask - t.bid) / 2).toFixed(2)]);
  dataAsk = ticks.map(t => [parseTime(t.timestamp), t.ask, t.id]);
  dataBid = ticks.map(t => [parseTime(t.timestamp), t.bid, t.id]);
  selectedTickIds = [];
  customHighlightIds = [];
  chart.setOption({ series: [] });
  lastChecked = "";
  updateZSeries();
}


function updateZSeries() {
  const mid = document.getElementById('midCheckbox').checked;
  const ask = document.getElementById('askCheckbox').checked;
  const bid = document.getElementById('bidCheckbox').checked;
  const checked = Array.from(document.querySelectorAll(".labelCheckbox:checked")).map(e => e.value).join(",");
  const state = `${mid}${ask}${bid}:${checked}:${selectedTickIds.join(',')}:${customHighlightIds.join(',')}`;
  if (state === lastChecked) return;
  lastChecked = state;

  const base = [];
  if (ask) base.push({
     name: 'Ask',
     type: 'scatter', 
     symbolSize: 1, 
     itemStyle: { color: '#f5a623' }, 
     data: dataAsk,
     dimention: ['timestamp', 'price', 'id', 'spread'],
     encode: {x:0, y:1, tooltip: [0,1,2]}
  });
  if (mid) base.push({
     name: 'Mid', 
     type: 'scatter', 
     symbolSize: 1, 
     itemStyle: { color: '#00bcd4' }, 
     data: dataMid,
     dimention: ['timestamp', 'price', 'id', 'spread'],
     encode: {x:0, y:1, tooltip: [0,1,2]}
  });
  if (bid) base.push({
     name: 'Bid', 
     type: 'scatter', 
     symbolSize: 1, 
     itemStyle: { color: '#4caf50' }, 
     data: dataBid,
     dimention: ['timestamp', 'price', 'id', 'spread'],
     encode: {x:0, y:1, tooltip: [0,1,2]}
  });

  const extras = labelSeries.filter(s => checked.includes(s.name));

  if (selectedTickIds.length) {
    const points = dataMid.filter(d => selectedTickIds.includes(d[2]));
    extras.push({ name: 'Selected', type: 'scatter', symbolSize: 7, itemStyle: { color: '#ff0', borderColor: '#fff', borderWidth: 1 }, data: points });
  }

  if (customHighlightIds.length) {
    const points = dataMid.filter(d => customHighlightIds.includes(d[2]));
    extras.push({ name: 'Extra', type: 'scatter', symbolSize: 6, itemStyle: { color: '#0ff', borderColor: '#fff', borderWidth: 1 }, data: points });
  }

  chart.setOption({ series: [...base, ...extras] }, { replaceMerge: ['series'], lazyUpdate: true });
}

async function loadLabelCheckboxes() {
  labelSeries = [];
  const container = document.getElementById("labelCheckboxes");
  const selector = document.getElementById("labelTableSelect");
  const tables = await fetch("/available").then(r => r.json());
  container.innerHTML = "";
  selector.innerHTML = "";
  for (const name of tables) {
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

function parseExtraTickIds() {
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
  customHighlightIds = extraIds;
  updateZSeries();
}

async function submitLabel() {
  const table = document.getElementById("labelTableSelect").value;
  const note = document.getElementById("labelNote").value;
  parseExtraTickIds();
  const idsToSubmit = [...selectedTickIds, ...customHighlightIds];
  if (!table || idsToSubmit.length === 0) return;
  const payload = { table, ids: idsToSubmit, note };
  console.log("Submitting labels:", JSON.stringify(payload));
  await fetch("/labels/assign", {
    method: "POST",
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
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
  document.getElementById("customTickIds").addEventListener("input", parseExtraTickIds);
});
