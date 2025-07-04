// tick-core.js — Dot View, Locked Zoom Window, Dual Version
let data = [], lastTimestamp = null;
const chart = echarts.init(document.getElementById('main'));

const option = {
  backgroundColor: '#111',
  tooltip: {
    trigger: 'axis',
    backgroundColor: '#222',
    borderColor: '#555',
    borderWidth: 1,
    textStyle: { color: '#fff', fontSize: 13 },
    formatter: (params) => {
      const p = params[0];
      const date = new Date(p.value[0]);
      date.setMinutes(date.getMinutes() + 600);
      const timeStr = date.toLocaleTimeString('en-au', { hour: 'numeric', minute: '2-digit', second: '2-digit', hour12: true }).toLowerCase();
      const dateStr = date.toLocaleDateString('en-AU');
      return `<div style="padding: 8px;"><strong>${timeStr}</strong><br><span style="color: #ccc;">${dateStr}</span><br>Mid: <strong style="color: #3fa9f5;">${p.value[1].toFixed(2)}</strong><br>ID: <span style="color:#aaa;">${p.value[2]}</span></div>`;
    }
  },
  xAxis: {
    type: 'time',
    axisLabel: {
      color: '#ccc',
      formatter: val => {
        const d = new Date(val);
        d.setMinutes(d.getMinutes() + 600);
        return `${d.getHours()}:${String(d.getMinutes()).padStart(2, '0')}` + `\n${d.toLocaleDateString('en-AU', { month: 'short', day: 'numeric' })}`;
      }
    },
    splitNumber: 12,
    minInterval: 60 * 1000 * 5
  },
  yAxis: {
    type: 'value',
    scale: true,
    minInterval: 1,
    axisLabel: {
      color: '#ccc',
      formatter: val => val.toFixed(1)
    }
  },
  dataZoom: [
    { type: 'inside', realtime: false },
    { type: 'slider', height: 40, bottom: 0, handleStyle: { color: '#3fa9f5' }, realtime: false }
  ],
  series: [{
    name: 'Mid Price',
    type: 'scatter',
    symbolSize: 4,
    data: []
  }]
};

chart.setOption(option);

async function loadInitialData() {
  try {
    const res = await fetch(`/ticks/recent?limit=1`);
    const ticks = await res.json();
    if (!Array.isArray(ticks) || ticks.length === 0) return;

    const t = ticks[0];
    const ts = new Date(t.timestamp).getTime();
    data = [[ts, t.mid, t.id]];
    lastTimestamp = t.timestamp;

    const tickDate = new Date(ts);
    const leftStart = new Date(ts);
    leftStart.setMinutes(Math.floor(leftStart.getMinutes() / 5) * 5 - 5);
    leftStart.setSeconds(0);
    const rightEnd = new Date(ts);
    rightEnd.setMinutes(Math.floor(rightEnd.getMinutes() / 5) * 5);
    rightEnd.setSeconds(59);

    const start = leftStart.getTime();
    const end = rightEnd.getTime();

    const yTop = Math.ceil(t.mid);
    const yBottom = Math.floor(t.mid);

    chart.setOption({
      series: [{ data }],
      xAxis: {
        min: start,
        max: end
      },
      yAxis: {
        min: yBottom,
        max: yTop
      },
      dataZoom: [
        { type: 'inside', startValue: start, endValue: end, realtime: false },
        { type: 'slider', startValue: start, endValue: end, bottom: 0, height: 40, realtime: false }
      ]
    });
  } catch (err) {
    console.error("❌ loadInitialData() failed", err);
  }
}

async function pollNewData() {
  if (!lastTimestamp) return;
  const res = await fetch(`/ticks/latest?after=${encodeURIComponent(lastTimestamp)}`);
  const newTicks = await res.json();
  if (newTicks.length > 0) {
    newTicks.forEach(t => data.push([new Date(t.timestamp).getTime(), t.mid, t.id]));
    data = data.slice(-5000);
    lastTimestamp = newTicks[newTicks.length - 1].timestamp;
    chart.setOption({ series: [{ data }] });
  }
}

async function mannualLoadMoreLeft() {
  const count = parseInt(document.getElementById('tickLoadAmount').value) || 0;
  if (!count || isNaN(count)) return;
  const firstId = data[0]?.[2];
  const res = await fetch(`/ticks/before/${firstId}?limit=${count}`);
  const older = await res.json();
  if (older.length > 0) {
    const prepend = older.map(t => [new Date(t.timestamp).getTime(), t.mid, t.id]);
    data = prepend.concat(data);
    chart.setOption({ series: [{ data }] });
  }
}

async function loadVersion() {
  try {
    const res = await fetch('/version');
    const json = await res.json();
    document.getElementById('version').innerHTML = `bver: ${json.version}<br>fver: 2025.07.05.006`;
  } catch {
    document.getElementById('version').textContent = 'Version: unknown';
  }
}

async function loadTableNames() {
  const res = await fetch('/sqlvw/tables');
  const tables = await res.json();
  const select = document.getElementById('tableSelect');
  select.innerHTML = tables.map(t => `<option value="${t}">${t}</option>`).join('');
}

async function runQuery() {
  const table = document.getElementById('tableSelect').value;
  const raw = document.getElementById('queryInput').value.trim();
  const query = raw || `SELECT * FROM ${table} ORDER BY timestamp DESC LIMIT 20`;
  const container = document.getElementById('sqlResult');
  container.innerHTML = `<pre style="color: #999;">Running query...</pre>`;
  try {
    const res = await fetch(`/sqlvw/query?query=${encodeURIComponent(query)}`);
    const text = await res.text();
    try {
      const json = JSON.parse(text);
      if (Array.isArray(json)) {
        if (json.length === 0) return container.innerHTML = '<p>No results.</p>';
        const headers = Object.keys(json[0]);
        let html = '<table><thead><tr>' + headers.map(h => `<th>${h}</th>`).join('') + '</tr></thead><tbody>';
        for (const row of json) html += '<tr>' + headers.map(h => `<td>${row[h]}</td>`).join('') + '</tr>';
        html += '</tbody></table>';
        container.innerHTML = html;
      } else {
        container.innerHTML = `<pre>${JSON.stringify(json, null, 2)}</pre>`;
      }
    } catch {
      container.innerHTML = `<pre style="color: green;">${text}</pre>`;
    }
  } catch (e) {
    container.innerHTML = `<pre style="color:red">Error: ${e.message}</pre>`;
  }
}

loadInitialData();
loadVersion();
loadTableNames();
setInterval(pollNewData, 3000);
