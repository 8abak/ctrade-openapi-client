// tick-core.js – Live mode aligned with htick core

let chart;
let dataMid = [], dataAsk = [], dataBid = [];
let lastTimestamp = null;
let tradingStartEpoch = null;
const MAX_VISIBLE_POINTS = 3000;

const option = {
  backgroundColor: '#111',
  tooltip: {
    trigger: 'axis',
    backgroundColor: '#222',
    borderColor: '#555',
    borderWidth: 1,
    textStyle: { color: '#fff', fontSize: 13 },
    formatter: (params) => {
      const d = new Date(params[0].value[0]);
      const timeStr = d.toLocaleTimeString('en-AU', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });
      const dateStr = d.toLocaleDateString('en-AU');
      let tooltip = `<div style='padding:8px'><strong>${timeStr}</strong><br><span style='color:#ccc'>${dateStr}</span><br>`;
      params.forEach(p => {
        tooltip += `${p.seriesName}: <strong style='color:${p.color}'>${p.value[1]}</strong><br>`;
      });
      tooltip += '</div>';
      return tooltip;
    }
  },
  xAxis: {
    type: 'time',
    axisLabel: {
      color: '#ccc',
      formatter: val => {
        const d = new Date(val);
        return `${d.getHours()}:${String(d.getMinutes()).padStart(2, '0')}` + `\n${d.getDate()} ${d.toLocaleString('default', { month: 'short' })}`;
      }
    },
    splitLine: { show: true, lineStyle: { color: '#333' } }
  },
  yAxis: {
    type: 'value',
    minInterval: 1,
    axisLabel: { color: '#ccc', formatter: val => Number(val).toFixed(0) },
    splitLine: { show: true, lineStyle: { color: '#333' } }
  },
  dataZoom: [
    { type: 'inside', throttle: 100 },
    { type: 'slider', height: 40, bottom: 0, handleStyle: { color: '#3fa9f5' } }
  ],
  series: []
};

function sampleData(data) {
  if (data.length <= MAX_VISIBLE_POINTS) return data;
  const step = Math.floor(data.length / MAX_VISIBLE_POINTS);
  return data.filter((_, i) => i % step === 0);
}

function updateSeries() {
  if (!chart || dataMid.length === 0) return;
  const ask = document.getElementById('askCheckbox')?.checked;
  const mid = document.getElementById('midCheckbox')?.checked;
  const bid = document.getElementById('bidCheckbox')?.checked;

  const updated = [];
  if (ask) updated.push({ id: 'ask', name: 'Ask', type: 'scatter', symbolSize: 2, itemStyle: { color: '#f5a623' }, data: sampleData(dataAsk) });
  if (mid) updated.push({ id: 'mid', name: 'Mid', type: 'scatter', symbolSize: 2, itemStyle: { color: '#00bcd4' }, data: sampleData(dataMid) });
  if (bid) updated.push({ id: 'bid', name: 'Bid', type: 'scatter', symbolSize: 2, itemStyle: { color: '#4caf50' }, data: sampleData(dataBid) });

  chart.setOption({ series: updated }, { replaceMerge: ['series'], lazyUpdate: true });
  adjustYAxisToZoom();
}

function adjustYAxisToZoom() {
  const zoom = chart.getOption()?.dataZoom?.[0];
  if (!zoom || zoom.startValue === undefined || zoom.endValue === undefined) return;
  const start = zoom.startValue;
  const end = zoom.endValue;
  const visible = [...dataMid, ...dataAsk, ...dataBid].filter(p => p[0] >= start && p[0] <= end).map(p => p[1]);
  if (!visible.length) return;
  chart.setOption({ yAxis: { min: Math.floor(Math.min(...visible)) - 1, max: Math.ceil(Math.max(...visible)) + 1 } });
}

async function loadInitialTickRange() {
  const res = await fetch('/ticks/lastid');
  const { lastId, timestamp } = await res.json();
  const latestTime = new Date(timestamp);
  const tradingStart = new Date(latestTime);
  if (tradingStart.getHours() < 8) tradingStart.setDate(tradingStart.getDate() - 1);
  tradingStart.setHours(8, 0, 0, 0);
  tradingStartEpoch = tradingStart.getTime();

  const startIso = tradingStart.toISOString();
  const endIso = latestTime.toISOString();
  lastTimestamp = latestTime.getTime();

  const range = await fetch(`/ticks/range?start=${startIso}&end=${endIso}`);
  const ticks = await range.json();
  if (!Array.isArray(ticks) || ticks.length === 0) return;

  const parse = ts => Date.parse(ts);
  dataMid = ticks.map(t => [parse(t.timestamp), t.mid, t.id]);
  dataAsk = ticks.map(t => [parse(t.timestamp), t.ask, t.id]);
  dataBid = ticks.map(t => [parse(t.timestamp), t.bid, t.id]);

  const tradingEnd = new Date(tradingStart);
  tradingEnd.setDate(tradingEnd.getDate() + 1);
  const zoomEnd = Math.ceil(lastTimestamp / (60 * 1000)) * 60 * 1000;
  const zoomStart = zoomEnd - 5 * 60 * 1000;

  chart.setOption({
    xAxis: { min: tradingStart.getTime(), max: tradingEnd.getTime() },
    dataZoom: [
      { startValue: zoomStart, endValue: zoomEnd },
      { startValue: zoomStart, endValue: zoomEnd }
    ]
  });

  
      // Removed broken appendData usage; replaced with updateSeries() below
  updateSeries();
  connectLiveSocket();
  showVersion();
}

async function loadTableNames() {
  try {
    const res = await fetch('/sqlvw/tables');
    const tables = await res.json();
    const select = document.getElementById('tableSelect');
    select.innerHTML = tables.map(t => `<option value="${t}">${t}</option>`).join('');
    if (tables.length > 0) {
      autoFillQuery();  // Fill default query when list loads
    }
  } catch (e) {
    document.getElementById('sqlResult').innerHTML = `<pre style="color: red;">Error loading tables: ${e}</pre>`;
  }
}

function autoFillQuery() {
  const table = document.getElementById('tableSelect').value;
  document.getElementById('queryInput').value = `SELECT * FROM ${table} ORDER BY timestamp DESC LIMIT 100`;
}

async function runQuery() {
  const query = document.getElementById('queryInput').value.trim();
  const container = document.getElementById('sqlResult');
  container.innerHTML = `<pre style="color: #999;">Running query...</pre>`;

  try {
    const res = await fetch(`/sqlvw/query?query=${encodeURIComponent(query)}`);
    const text = await res.text();
    try {
      const json = JSON.parse(text);
      if (Array.isArray(json) && json.length > 0) {
        const headers = Object.keys(json[0]);
        let html = '<table><thead><tr>' + headers.map(h => `<th>${h}</th>`).join('') + '</tr></thead><tbody>';
        for (const row of json) {
          html += '<tr>' + headers.map(h => `<td>${row[h]}</td>`).join('') + '</tr>';
        }
        html += '</tbody></table>';
        container.innerHTML = html;
      } else {
        container.innerHTML = '<p>No results.</p>';
      }
    } catch {
      container.innerHTML = `<pre style="color: green;">${text}</pre>`;
    }
  } catch (e) {
    container.innerHTML = `<pre style="color:red;">Error: ${e.message}</pre>`;
  }
}


function connectLiveSocket() {
  const ws = new WebSocket("wss://www.datavis.au/ws/ticks");
  ws.onopen = () => {
    console.log("📡 Connected to WebSocket");
    ws.send("ping from frontend");
  };
  ws.onmessage = (e) => {
    try {
      const tick = JSON.parse(e.data);
      const ts = Date.parse(tick.timestamp);
      if (ts <= lastTimestamp && tick.id <= dataMid[dataMid.length - 1]?.[2]) return;
      lastTimestamp = ts;
      dataMid.push([ts, tick.mid, tick.id]);
      dataAsk.push([ts, tick.ask, tick.id]);
      dataBid.push([ts, tick.bid, tick.id]);
      updateSeries();
    } catch (err) {
      console.warn("Invalid tick:", e.data);
    }
  };
  ws.onerror = (e) => console.warn("❌ WebSocket error", e);
  ws.onclose = () => console.warn("🔌 WebSocket closed");
}

function debounce(fn, delay) {
  let timeout;
  return (...args) => {
    clearTimeout(timeout);
    timeout = setTimeout(() => fn(...args), delay);
  };
}

function format(v) {
  return v ? `${v.datetime} ${v.message}` : '-';
}

function showVersion() {
  fetch("/version")
    .then(res => res.json())
    .then(v => {
      const val = v["tick"];
      const versionDiv = document.createElement('div');
      versionDiv.style.position = 'absolute';
      versionDiv.style.left = '10px';
      versionDiv.style.bottom = '8px';
      versionDiv.style.color = '#777';
      versionDiv.style.fontSize = '11px';
      versionDiv.style.whiteSpace = 'pre-line';
      versionDiv.innerHTML = `J: ${format(val?.js)}
B: ${format(val?.py)}
H: ${format(val?.html)}`;
      document.body.appendChild(versionDiv);
    });
}

window.addEventListener('DOMContentLoaded', () => {
  chart = echarts.init(document.getElementById("main"));
  chart.setOption(option);
  ['ask', 'mid', 'bid'].forEach(id => {
    const box = document.getElementById(id + 'Checkbox');
    if (box) box.addEventListener('change', updateSeries);
  });
  chart.on('dataZoom', debounce(() => {
    updateSeries();
    adjustYAxisToZoom();
  }, 100));
  updateSeries();
  loadInitialTickRange();
});
