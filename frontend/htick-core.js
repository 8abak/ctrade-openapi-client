// ✅ FINAL VERSION of htick-core.js (Static Tick Viewer with Label Controls)

let chart;
let dataMid = [], dataAsk = [], dataBid = [];
let labelTables = [];

const bver = '2025.07.05.004', hver = '2025.07.07.002';
const SYDNEY_OFFSET = 600;
function toSydneyTime(date) {
  return new Date(date.getTime() + SYDNEY_OFFSET * 60000);
}

const option = {
  backgroundColor: "#111",
  tooltip: {
    trigger: "axis",
    backgroundColor: "#222",
    borderColor: "#555",
    borderWidth: 1,
    textStyle: { color: "#fff", fontSize: 13 },
    formatter: (params) => {
      const date = toSydneyTime(new Date(params[0].value[0]));
      const timeStr = date.toLocaleTimeString("en-AU", { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: true });
      const dateStr = date.toLocaleDateString("en-AU");
      let tooltip = `<div style=\"padding: 8px;\"><strong>${timeStr}</strong><br><span style=\"color: #ccc;\">${dateStr}</span><br>`;
      params.forEach(p => {
        tooltip += `${p.seriesName}: <strong style=\"color: ${p.color};\">${p.value[1].toFixed(2)}</strong><br>`;
      });
      tooltip += `ID: <span style=\"color:#aaa;\">${params[0].value[2]}</span></div>`;
      return tooltip;
    }
  },
  xAxis: {
    type: "time",
    axisLabel: {
      color: "#ccc",
      formatter: val => {
        const d = toSydneyTime(new Date(val));
        return `${d.getHours().toString().padStart(2, '0')}:${d.getMinutes().toString().padStart(2, '0')}` + `\n${d.toLocaleDateString('en-AU', { month: 'short', day: 'numeric' })}`;
      }
    },
    splitLine: { show: true, lineStyle: { color: "#333" } }
  },
  yAxis: {
    type: "value",
    axisLabel: { color: "#ccc", formatter: val => Math.floor(val) },
    splitLine: { show: true, lineStyle: { color: "#333" } }
  },
  dataZoom: [
    { type: 'inside', realtime: false },
    { type: 'slider', height: 40, bottom: 0, handleStyle: { color: '#3fa9f5' }, realtime: false }
  ],
  series: []
};

async function loadAvailableLabels() {
  try {
    const res = await fetch("/api/labels/available");
    const list = await res.json();
    if (!Array.isArray(list)) throw new Error("Invalid label list");
    labelTables = list;
    const container = document.getElementById("labelCheckboxes");
    container.innerHTML = '';
    labelTables.forEach(name => {
      const label = document.createElement("label");
      label.innerHTML = `<input type='checkbox' data-label='${name}'> ${name}`;
      container.appendChild(label);
    });
  } catch (err) {
    console.warn("⚠️ Could not load label tables:", err);
  }
}

function updateSeries() {
  const ask = document.getElementById("askCheckbox").checked;
  const mid = document.getElementById("midCheckbox").checked;
  const bid = document.getElementById("bidCheckbox").checked;
  const series = [];
  if (ask) series.push({ id: 'ask', name: 'Ask', type: 'scatter', data: dataAsk, symbolSize: 4, itemStyle: { color: '#f5a623' }});
  if (mid) series.push({ id: 'mid', name: 'Mid', type: 'scatter', data: dataMid, symbolSize: 4, itemStyle: { color: '#00bcd4' }});
  if (bid) series.push({ id: 'bid', name: 'Bid', type: 'scatter', data: dataBid, symbolSize: 4, itemStyle: { color: '#4caf50' }});
  chart.setOption({ series }, { replaceMerge: ['series'] });
}

async function loadDayTicks() {
  const date = document.getElementById("dayInput").value;
  const hour = parseInt(document.getElementById("hourSelect").value);
  if (!date) return;

  // Convert selected local time to UTC bounds
  const startLocal = new Date(`${date}T${hour.toString().padStart(2, '0')}:00:00`);
  const endLocal = new Date(startLocal);
  endLocal.setDate(startLocal.getDate() + 1);

  const startUTC = new Date(startLocal.getTime() - SYDNEY_OFFSET * 60000);
  const endUTC = new Date(endLocal.getTime() - SYDNEY_OFFSET * 60000);
  const endMillis = endUTC.getTime();

  // Data arrays
  dataMid = [];
  dataAsk = [];
  dataBid = [];

  let lastTickId = null;
  let keepLoading = true;
  let batch = [];

  while (keepLoading) {
    let url = '';
    if (lastTickId === null) {
      // First batch
      url = `/ticks/after/${startUTC.toISOString()}?limit=5000`;
    } else {
      url = `/ticks/after-id/${lastTickId}?limit=5000`;
    }

    const res = await fetch(url);
    batch = await res.json();

    if (batch.length === 0) break;

    // Filter by end time
    const usableTicks = batch.filter(t => new Date(t.timestamp).getTime() < endMillis);

    for (const t of usableTicks) {
      const ts = new Date(t.timestamp).getTime();
      dataMid.push([ts, t.mid, t.id]);
      dataAsk.push([ts, t.ask, t.id]);
      dataBid.push([ts, t.bid, t.id]);
    }

    lastTickId = batch[batch.length - 1].id;

    // Stop if no more usable ticks
    if (usableTicks.length < batch.length || new Date(batch[batch.length - 1].timestamp).getTime() >= endMillis) {
      keepLoading = false;
    }
  }

  // Set zoom and axis bounds
  chart.setOption({
    xAxis: { min: startUTC.getTime(), max: endMillis },
    dataZoom: [
      { type: 'inside', startValue: startUTC.getTime(), endValue: endMillis },
      { type: 'slider', startValue: startUTC.getTime(), endValue: endMillis, bottom: 0, height: 40 }
    ]
  });

  updateSeries();
}


async function createNewLabelTable() {
  const input = document.getElementById("newLabelInput");
  const name = input.value.trim();
  if (!name) return alert("Label name required");
  const query = `CREATE TABLE IF NOT EXISTS ${name} (id SERIAL PRIMARY KEY, tickid INT, content TEXT)`;
  const encoded = encodeURIComponent(query);
  const res = await fetch(`/sqlvw/query?query=${encoded}`);
  await res.json();
  input.value = '';
  await loadAvailableLabels();
  alert(`Label table '${name}' created.`);
}

window.addEventListener("DOMContentLoaded", () => {
  chart = echarts.init(document.getElementById("main"));
  chart.setOption(option);
  document.getElementById("loadButton").addEventListener("click", loadDayTicks);
  document.getElementById("createLabelBtn").addEventListener("click", createNewLabelTable);
  ['ask', 'mid', 'bid'].forEach(id => {
    document.getElementById(id + 'Checkbox').addEventListener("change", updateSeries);
  });
  loadAvailableLabels();

  const versionDiv = document.createElement('div');
  versionDiv.style.position = 'absolute';
  versionDiv.style.left = '10px';
  versionDiv.style.bottom = '8px';
  versionDiv.style.color = '#777';
  versionDiv.style.fontSize = '11px';
  versionDiv.innerText = `bver: ${bver}\n hver: ${hver}`;
  document.body.appendChild(versionDiv);
});
