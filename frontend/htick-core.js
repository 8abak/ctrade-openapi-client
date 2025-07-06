// ✅ INITIAL VERSION of htick-core.js (Static Tick Viewer with Label Controls)

let chart;
let dataMid = [], dataAsk = [], dataBid = [];
let labelTables = [];

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
  const res = await fetch("/api/labels/available");
  labelTables = await res.json();
  const container = document.getElementById("labelCheckboxes");
  container.innerHTML = '';
  labelTables.forEach(name => {
    const label = document.createElement("label");
    label.innerHTML = `<input type='checkbox' data-label='${name}'> ${name}`;
    container.appendChild(label);
  });
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

  const startLocal = new Date(`${date}T${hour.toString().padStart(2, '0')}:00:00`);
  const endLocal = new Date(startLocal);
  endLocal.setDate(startLocal.getDate() + 1);

  const startUTC = new Date(startLocal.getTime() - SYDNEY_OFFSET * 60000);
  const endUTC = new Date(endLocal.getTime() - SYDNEY_OFFSET * 60000);

  const res = await fetch(`/ticks/after/${startUTC.toISOString()}?limit=5000`);
  const allTicks = await res.json();

  const endMillis = endUTC.getTime();
  const inRange = allTicks.filter(t => new Date(t.timestamp).getTime() < endMillis);

  dataMid = inRange.map(t => [new Date(t.timestamp).getTime(), t.mid, t.id]);
  dataAsk = inRange.map(t => [new Date(t.timestamp).getTime(), t.ask, t.id]);
  dataBid = inRange.map(t => [new Date(t.timestamp).getTime(), t.bid, t.id]);

  chart.setOption({
    xAxis: { min: startUTC.getTime(), max: endUTC.getTime() },
    dataZoom: [
      { type: 'inside', startValue: startUTC.getTime(), endValue: endUTC.getTime() },
      { type: 'slider', startValue: startUTC.getTime(), endValue: endUTC.getTime(), bottom: 0, height: 40 }
    ]
  });

  updateSeries();
}

async function createNewLabelTable() {
  const input = document.getElementById("newLabelInput");
  const name = input.value.trim();
  if (!name) return alert("Label name required");
  const res = await fetch(`/sqlvw/query?query=CREATE TABLE IF NOT EXISTS ${name} (id SERIAL PRIMARY KEY, tickid INT, content TEXT)`);
  const result = await res.json();
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
});
