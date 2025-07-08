// ✅ FINAL VERSION of tick-core.js with Sydney-aware viewport and dynamic zooming

const bver = '2025.07.05.004', fver = '2025.07.08.2';
let chart;
let dataMid = [], dataAsk = [], dataBid = [];
let lastId = null;

const SYDNEY_OFFSET = 600; // in minutes (+10 hours)
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
      let tooltip = `<div style="padding: 8px;"><strong>${timeStr}</strong><br><span style="color: #ccc;">${dateStr}</span><br>`;
      params.forEach(p => {
        tooltip += `${p.seriesName}: <strong style="color: ${p.color};">${p.value[1].toFixed(2)}</strong><br>`;
      });
      tooltip += `ID: <span style="color:#aaa;">${params[0].value[2]}</span></div>`;
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
  const res = await fetch('/ticks/lastid');
  const { lastId, timestamp } = await res.json();

  const tickTimeUTC = new Date(timestamp);
  const tickTimeSydney = toSydneyTime(tickTimeUTC);
  const tickPrice = await fetch(`/sqlvw/query?query=${encodeURIComponent(`SELECT mid FROM ticks WHERE id=${lastId}`)}`)
    .then(r => r.json()).then(d => d?.[0]?.mid || null);

  if (!tickPrice) return;

  // Sydney day boundaries
  const chartStart = new Date(tickTimeSydney);
  if (chartStart.getHours() < 8) chartStart.setDate(chartStart.getDate() - 1);
  chartStart.setHours(8, 0, 0, 0);

  const chartEnd = new Date(chartStart);
  chartEnd.setDate(chartEnd.getDate() + 1);
  chartEnd.setMinutes(chartEnd.getMinutes() - 1);  // 7:59 AM next day

  // Convert back to UTC for chart
  const xMin = new Date(chartStart.getTime() - SYDNEY_OFFSET * 60000).getTime();
  const xMax = new Date(chartEnd.getTime() - SYDNEY_OFFSET * 60000).getTime();
  const tickTime = tickTimeUTC.getTime();

  dataMid = [[tickTime, tickPrice, lastId]];

  chart.setOption({
    xAxis: { min: xMin, max: xMax },
    series: [{
      id: 'mid',
      name: 'Mid',
      type: 'scatter',
      symbolSize: 6,
      itemStyle: { color: '#00bcd4' },
      data: dataMid
    }]
  });
}


function setupLiveSocket() {
  const ws = new WebSocket("wss://www.datavis.au/ws/ticks");
  ws.onopen = () => console.log("📡 WebSocket connected");
  ws.onmessage = (event) => {
    const tick = JSON.parse(event.data);
    const ts = new Date(tick.timestamp).getTime();
    if (tick.id <= lastId) return;
    dataMid.push([ts, tick.mid, tick.id]);
    dataAsk.push([ts, tick.ask, tick.id]);
    dataBid.push([ts, tick.bid, tick.id]);
    lastId = tick.id;
    updateSeries();
  };
  ws.onerror = (e) => console.warn("⚠️ WebSocket error", e);
  ws.onclose = () => console.warn("🔌 WebSocket closed.");
}

// -- Additional UI Functions Unchanged --

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
