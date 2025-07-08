const bver = '2025.07.05.004', fver = '2025.07.09.13';
let chart;
let dataMid = [], dataAsk = [], dataBid = [];
let lastId = null;

const option = {
  backgroundColor: "#111",
  tooltip: {
    trigger: "axis",
    backgroundColor: "#222",
    borderColor: "#555",
    borderWidth: 1,
    textStyle: { color: "#fff", fontSize: 13 },
    formatter: (params) => {
      const d = new Date(params[0].value[0]);
      const timeStr = d.toLocaleTimeString("en-AU", {
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hour12: false,
        timeZone: "Australia/Sydney"
      });
      const dateStr = d.toLocaleDateString("en-AU", {
        timeZone: "Australia/Sydney"
      });
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
        const d = new Date(val);
        const time = d.toLocaleTimeString("en-AU", {
          hour: "2-digit",
          minute: "2-digit",
          hour12: false,
          timeZone: "Australia/Sydney"
        });
        const date = d.toLocaleDateString("en-AU", {
          month: "short",
          day: "numeric",
          timeZone: "Australia/Sydney"
        });
        return `${time}\n${date}`;
      }
    },
    splitLine: { show: true, lineStyle: { color: "#333" } }
  },
  yAxis: {
    type: "value"
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
  adjustYAxisToZoom();
}

function adjustYAxisToZoom() {
  const zoom = chart.getOption().dataZoom?.[0];
  if (!zoom) return;

  const start = zoom.startValue;
  const end = zoom.endValue;

  const allVisible = [
    ...dataMid.filter(p => p[0] >= start && p[0] <= end).map(p => p[1]),
    ...dataAsk.filter(p => p[0] >= start && p[0] <= end).map(p => p[1]),
    ...dataBid.filter(p => p[0] >= start && p[0] <= end).map(p => p[1])
  ];

  if (allVisible.length === 0) return;

  const rawMin = Math.min(...allVisible);
  const rawMax = Math.max(...allVisible);

  const yMin = Math.floor(rawMin) - 1;
  const yMax = Math.ceil(rawMax) + 1;

  chart.setOption({
    yAxis: {
      min: yMin,
      max: yMax,
      axisLabel: {
        color: "#ccc",
        formatter: val => Number(val).toFixed(0)
      },
      splitLine: {
        show: true,
        lineStyle: { color: "#333" },
        interval: (_, val) => Number(val) % 1 === 0
      }
    }
  });
}

async function loadInitialData() {
  const res = await fetch('/ticks/lastid');
  const { lastId: id, timestamp } = await res.json();
  lastId = id;

  const lastTickTime = new Date(timestamp);

  // Convert to Sydney time by re-parsing locale string
  const sydneyDateString = lastTickTime.toLocaleString("en-AU", { timeZone: "Australia/Sydney" });
  const lastSydney = new Date(sydneyDateString);

  // Define chart segment window: 8AM → 8AM
  const dayStart = new Date(lastSydney);
  if (dayStart.getHours() < 8) dayStart.setDate(dayStart.getDate() - 1);
  dayStart.setHours(8, 0, 0, 0);

  const dayEnd = new Date(dayStart);
  dayEnd.setDate(dayEnd.getDate() + 1);
  dayEnd.setHours(8, 0, 0, 0);

  const xMin = dayStart.getTime();
  const xMax = dayEnd.getTime();

  const tickRes = await fetch(`/sqlvw/query?query=${encodeURIComponent(`SELECT bid, ask, mid, timestamp FROM ticks WHERE id = ${lastId}`)}`);
  const tickData = await tickRes.json();
  const t = tickData[0];
  if (!t) return;

  const ts = new Date(t.timestamp).getTime();
  dataMid = [[ts, t.mid, lastId]];
  dataAsk = [[ts, t.ask, lastId]];
  dataBid = [[ts, t.bid, lastId]];

  const zoomStart = Math.max(xMin, ts - 2 * 60 * 1000);
  const zoomEnd = Math.min(xMax, ts + 2 * 60 * 1000);

  chart.setOption({
    xAxis: { min: xMin, max: xMax },
    dataZoom: [
      { type: 'inside', startValue: zoomStart, endValue: zoomEnd },
      { type: 'slider', startValue: zoomStart, endValue: zoomEnd, bottom: 0, height: 40 }
    ]
  });

  updateSeries();
  setupLiveSocket();
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

async function loadTableNames() {
  try {
    const res = await fetch("/sqlvw/tables");
    const tables = await res.json();
    const select = document.getElementById("tableSelect");
    if (!select) return;
    console.log("Available tables:", tables);
    select.innerHTML = tables.map(t => `<option value="${t}">${t}</option>`).join('');
  } catch (e) {
    console.error("⚠️ Could not load table names:", e);
  }
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