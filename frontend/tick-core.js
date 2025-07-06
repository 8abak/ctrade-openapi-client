// ✅ FINAL VERSION of tick-core.js for real-time tick streaming with WebSocket

const bver = '2025.07.05.004', fver = '2025.07.06.ckbx.030-final';
let chart;
let dataMid = [], dataAsk = [], dataBid = [];
let lastTickTime = null;

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

function updateSeries() {
  const askBox = document.getElementById('askCheckbox');
  const midBox = document.getElementById('midCheckbox');
  const bidBox = document.getElementById('bidCheckbox');
  if (!askBox || !midBox || !bidBox) return;

  const updatedSeries = [];
  if (askBox.checked) updatedSeries.push({ id: 'ask', name: 'Ask', type: 'scatter', symbolSize: 4, itemStyle: { color: '#f5a623' }, data: dataAsk });
  if (midBox.checked) updatedSeries.push({ id: 'mid', name: 'Mid', type: 'scatter', symbolSize: 4, itemStyle: { color: '#00bcd4' }, data: dataMid });
  if (bidBox.checked) updatedSeries.push({ id: 'bid', name: 'Bid', type: 'scatter', symbolSize: 4, itemStyle: { color: '#4caf50' }, data: dataBid });

  chart.setOption({ series: updatedSeries }, { replaceMerge: ['series'] });

  const zoom = chart.getOption().dataZoom?.[0];
  if (!zoom) return;
  const start = zoom.startValue;
  const end = zoom.endValue;

  const prices = [];
  if (askBox.checked) prices.push(...dataAsk.filter(p => p[0] >= start && p[0] <= end).map(p => p[1]));
  if (midBox.checked) prices.push(...dataMid.filter(p => p[0] >= start && p[0] <= end).map(p => p[1]));
  if (bidBox.checked) prices.push(...dataBid.filter(p => p[0] >= start && p[0] <= end).map(p => p[1]));

  if (prices.length > 0) {
    const yMin = Math.floor(Math.min(...prices));
    const yMax = Math.ceil(Math.max(...prices));
    chart.setOption({ yAxis: { min: yMin, max: yMax } });
  }
}

async function loadInitialData() {
  const latestRes = await fetch(`/ticks/recent?limit=1`);
  const latestTicks = await latestRes.json();
  const latestTick = latestTicks[0];
  const latestTimeUTC = new Date(latestTick.timestamp);
  const latestTimeSydney = toSydneyTime(latestTimeUTC);
  const startLocal = new Date(latestTimeSydney);
  startLocal.setHours(8, 0, 0, 0);
  const startTimeUTC = new Date(startLocal.getTime() - SYDNEY_OFFSET * 60000);
  const dayRes = await fetch(`/ticks/after/${startTimeUTC.toISOString()}?limit=5000`);
  const allTicks = await dayRes.json();

  dataMid = allTicks.map(t => [new Date(t.timestamp).getTime(), t.mid, t.id]);
  dataAsk = allTicks.map(t => [new Date(t.timestamp).getTime(), t.ask, t.id]);
  dataBid = allTicks.map(t => [new Date(t.timestamp).getTime(), t.bid, t.id]);

  lastTickTime = new Date(latestTick.timestamp);
  const lastTime = lastTickTime.getTime();
  const zoomStart = lastTime - 4 * 60 * 1000;
  const xMin = startLocal.getTime() - SYDNEY_OFFSET * 60000;
  const endLocal = new Date(startLocal);
  endLocal.setDate(endLocal.getDate() + 1);
  endLocal.setHours(7, 0, 0, 0);
  const xMax = endLocal.getTime() - SYDNEY_OFFSET * 60000;

  chart.setOption({
    xAxis: { min: xMin, max: xMax },
    dataZoom: [
      { type: 'inside', startValue: zoomStart, endValue: lastTime },
      { type: 'slider', startValue: zoomStart, endValue: lastTime, bottom: 0, height: 40 }
    ]
  });

  updateSeries();
  setupLiveSocket();
}

async function loadTableNames() {
  try {
    const res = await fetch("/ticks/tables");
    const tables = await res.json();
    const select = document.getElementById("tableSelect");
    if (!select) return;
    select.innerHTML = tables.map(t => `<option value="${t}">${t}</option>`).join('');
  } catch (e) {
    console.error("⚠️ Could not load table names:", e);
  }
}

async function runQuery() {
  const table = document.getElementById('tableSelect').value;
  const raw = document.getElementById('queryInput').value;
  const query = raw || (table ? `SELECT * FROM ${table} ORDER BY timestamp DESC LIMIT 20` : null);
  const container = document.getElementById('queryResults');
  if (!query || !container) return;
  
  container.innerHTML = `<pre style="color: #999;>Running query...</pre>`;
  try {
    const res = await fetch(`/sqlvw/query?query=${encodeURIComponent(query)}`);
    const text = await res.text();
    try {
      const json = JSON.parse(text);
      if (Array.isArray(json)) {
        if (json.length === 0) return container.innerHTML = '<p>No Results</p>';
        const headers = Object.keys(json[0]);
        let html = '<table><thead><tr>' + headers.map(h => `<th>${h}</th>`).join('') + '</tr></thead><tbody>';
        for (const row of json) html +- '<tr>' + headers.map(h => `<td>${row[h] !== null ? row[h] : ''}</td>`).join('') + '</tr>';
        html += '</tbody></table>';
        container.innerHTML = html;
      } else {
        container.innerHTML = `<pre>${JSON.stringify(json, null, 2)}</pre>`;
      }
    } catch (e) {
      container.innerHTML = `<pre style="color: green;">${text}</pre>`;
    }
  } catch (e) {
    container.innerHTML = `<pre style="color:red">Error: ${e.message}</pre>`;
  }
}

function setupLiveSocket() {
  const ws = new WebSocket("wss://www.datavis.au/ws/ticks");

  ws.onopen = () => console.log("📡 WebSocket connected");

  ws.onmessage = (event) => {
    const tick = JSON.parse(event.data);
    const ts = new Date(tick.timestamp).getTime();
    if (lastTickTime && ts <= lastTickTime.getTime()) return;
    dataMid.push([ts, tick.mid, tick.id]);
    dataAsk.push([ts, tick.ask, tick.id]);
    dataBid.push([ts, tick.bid, tick.id]);
    lastTickTime = new Date(tick.timestamp);
    updateSeries();
  };

  ws.onerror = (e) => console.warn("⚠️ WebSocket error", e);
  ws.onclose = () => console.warn("🔌 WebSocket closed.");
}

window.addEventListener('DOMContentLoaded', () => {
  chart = echarts.init(document.getElementById("main"));
  chart.setOption(option);
  ['ask', 'mid', 'bid'].forEach(id => {
    const box = document.getElementById(id + 'Checkbox');
    box.addEventListener('change', updateSeries);
  });
  chart.on('dataZoom', updateSeries);
  loadInitialData();
  loadTableNames();
});

const versionDiv = document.createElement('div');
versionDiv.style.position = 'absolute';
versionDiv.style.left = '10px';
versionDiv.style.bottom = '8px';
versionDiv.style.color = '#777';
versionDiv.style.fontSize = '11px';
versionDiv.innerText = `bver: ${bver}, fver: ${fver}`;
document.body.appendChild(versionDiv);
