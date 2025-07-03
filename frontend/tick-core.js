// tick-core.js
let data = [], lastTimestamp = null, isLoadingOld = false;
const labelLayers = {};
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
        return `${d.getHours()}:${String(d.getMinutes()).padStart(2, '0')}` + `\n${d.toLocaleDateString('en-AU', { month: 'short', day: 'numeric' })}`;
      }
    },
    splitLine: { show: false }
  },
  yAxis: {
    type: 'value',
    scale: true,
    min: value => Math.floor(value.min),
    max: value => Math.ceil(value.max),
    interval: 1,
    axisLabel: {
      color: '#ccc',
      formatter: val => val.toFixed(0)
    }
  },
  series: [{
    name: 'Mid Price',
    type: 'scatter',
    data: [],
    symbolSize: 5,
    itemStyle: { color: '#3fa9f5' }
  }],
  dataZoom: [
    { type: 'inside', start: 90, end: 100 },
    { type: 'slider', start: 90, end: 100, bottom: 0, height: 40, handleStyle: { color: '#3fa9f5' } }
  ],
  markLine: { data: [] }
};

chart.setOption(option);

chart.on('dataZoom', async function (event) {
  const startPercent = event.batch?.[0]?.start ?? event.start;
  const visibleLeftCount = Math.floor((startPercent / 100) * data.length);
  if (visibleLeftCount < 400 && !isLoadingOld) {
    isLoadingOld = true;
    const zoom = chart.getOption().dataZoom?.[0];
    const startIndex = Math.floor((zoom?.start / 100) * data.length);
    const endIndex = Math.floor((zoom?.end / 100) * data.length);
    const idStart = data[startIndex]?.[2];
    const idEnd = data[endIndex]?.[2];
    const firstId = data[0]?.[2];
    const res = await fetch(`/ticks/before/${firstId}?limit=2000`);
    const older = await res.json();
    if (older.length > 0) {
      const prepend = older.map(t => [t.timestamp, t.mid, t.id]);
      data = prepend.concat(data);
      const newStart = data.findIndex(d => d[2] === idStart);
      const newEnd = data.findIndex(d => d[2] === idEnd);
      const s = (newStart / data.length) * 100;
      const e = (newEnd / data.length) * 100;
      chart.setOption({ series: [{ data }], dataZoom: [{ start: s, end: e }, { start: s, end: e }] });
      updateLabelView();
    }
    isLoadingOld = false;
  }
});

function updateLabelView() {
  const all = Object.values(labelLayers).flat();
  chart.setOption({ markLine: { symbol: ['none', 'none'], label: { show: true }, data: all } });
}

async function toggleLabel(name, checked) {
  if (checked) {
    const res = await fetch(`/labels/${name}`);
    const rows = await res.json();
    const marks = rows.map(r => {
      const match = data.find(d => d[2] === r.tickid);
      return match ? { xAxis: match[0], label: { formatter: name }, lineStyle: { type: 'dashed', color: '#ffa500' } } : null;
    }).filter(Boolean);
    labelLayers[name] = marks;
  } else {
    delete labelLayers[name];
  }
  updateLabelView();
}

async function loadInitialData() {
  const res = await fetch("/ticks/recent?limit=2200");
  const ticks = await res.json();
  data = ticks.map(t => [t.timestamp, t.mid, t.id]);
  lastTimestamp = ticks[ticks.length - 1]?.timestamp;
  const visible = data.slice(-1500);
  const futurePad = 60 * 1000;
  const startTime = new Date(visible[0][0]).getTime();
  const endTime = new Date(visible[visible.length - 1][0]).getTime() + futurePad;
  chart.setOption({ xAxis: { min: startTime, max: endTime }, series: [{ data }] });
  applyTimeSeparator(5);
}

function applyTimeSeparator(minutes) {
  const ms = minutes * 60 * 1000;
  const marks = [];
  if (data.length === 0) return;
  const start = new Date(data[0][0]).getTime();
  const end = new Date(data[data.length - 1][0]).getTime();
  const firstGridTime = Math.ceil(start / ms) * ms;
  for (let t = firstGridTime; t <= end + 5 * ms; t += ms) {
    marks.push({ xAxis: t, lineStyle: { type: 'solid', color: '#333', width: 0.5 }, label: { show: false } });
  }
  chart.setOption({ markLine: { symbol: ['none', 'none'], data: marks.concat(...Object.values(labelLayers)) } });
}

async function mannualLoadMoreLeft() {
  const count = parseInt(document.getElementById('tickLoadAmount').value) || 0;
  if (!count || isNaN(count)) return;
  const firstId = data[0]?.[2];
  const res = await fetch(`/ticks/before/${firstId}?limit=${count}`);
  const older = await res.json();
  if (older.length > 0) {
    const prepend = older.map(t => [t.timestamp, t.mid, t.id]);
    data = prepend.concat(data);
    lastTimestamp = older[older.length - 1].timestamp;
    chart.setOption({ series: [{ data }] });
    updateLabelView();
  }
}

async function pollNewData() {
  if (!lastTimestamp) return;
  const res = await fetch(`/ticks/latest?after=${encodeURIComponent(lastTimestamp)}`);
  const newTicks = await res.json();
  if (newTicks.length > 0) {
    newTicks.forEach(t => data.push([t.timestamp, t.mid, t.id]));
    data = data.slice(-2200);
    lastTimestamp = newTicks[newTicks.length - 1].timestamp;
    chart.setOption({ series: [{ data }] });
    updateLabelView();
  }
}

async function loadVersion() {
  try {
    const res = await fetch('/version');
    const json = await res.json();
    document.getElementById('version').textContent = `Version: ${json.version}`;
  } catch {
    document.getElementById('version').textContent = 'Version: unknown';
  }
}

async function loadLabelToggles() {
  const res = await fetch('/api/labels/available');
  const tables = await res.json();
  const container = document.getElementById('labelToggles');
  container.innerHTML = '';
  tables.forEach(name => {
    const id = `label-${name}`;
    container.insertAdjacentHTML('beforeend', `
      <label style="display:flex;align-items: center;font-size:13px;margin-bottom:4px;gap: 6px;">
        <input type="checkbox" id="${id}" onchange="toggleLabel('${name}', this.checked)">
        <span>${name}</span>
      </label>`);
  });
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
  const query = raw || `SELECT * FROM ${table} ORDER BY id DESC LIMIT 20`;
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
loadLabelToggles();
loadTableNames();
setInterval(pollNewData, 3000);
