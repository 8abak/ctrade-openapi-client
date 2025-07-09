// tick-core.js (fully updated)

const bver = '2025.07.05.004', fver = '2025.07.09.19';
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
        hour12: false
      });
      const dateStr = d.toLocaleDateString("en-AU");
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
    minInterval: 60 * 1000,
    axisLabel: {
      color: "#ccc",
      formatter: val => {
        const d = new Date(val);
        const time = d.toLocaleTimeString("en-AU", {
          hour: "2-digit",
          minute: "2-digit",
          hour12: false
        });
        return `${time}\n${d.getDate()} ${d.toLocaleString('default', { month: 'short' })}`;
      }
    },
    splitLine: {
      show: true,
      lineStyle: { color: "#333" }
    }
  },
  yAxis: {
    type: "value",
    minInterval: 1,
    axisLabel: {
      color: "#ccc",
      formatter: val => Number(val).toFixed(0)
    },
    splitLine: {
      show: true,
      lineStyle: { color: "#333" }
    }
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
}

async function loadInitialData() {
  const res = await fetch('/ticks/lastid');
  const { lastId: id, timestamp } = await res.json();
  lastId = id;

  const latestTickTime = new Date(timestamp);
  const tradingStart = new Date(latestTickTime);
  if (tradingStart.getHours() < 8) tradingStart.setDate(tradingStart.getDate() - 1);
  tradingStart.setHours(8, 0, 0, 0);

  const tradingEnd = new Date(tradingStart);
  tradingEnd.setDate(tradingStart.getDate() + 1);

  const xMin = tradingStart.getTime();
  const xMax = tradingEnd.getTime();

  const now = new Date();
  now.setSeconds(0, 0);
  const zoomEnd = now.getTime();
  const zoomStart = zoomEnd - 5 * 60 * 1000;

  const tickRes = await fetch(`/sqlvw/query?query=${encodeURIComponent(`SELECT bid, ask, mid, timestamp FROM ticks WHERE id = ${lastId}`)}`);
  const tickData = await tickRes.json();
  const t = tickData[0];
  if (!t) return;

  const ts = new Date(t.timestamp).getTime();

  dataMid = [[ts, t.mid, lastId]];
  dataAsk = [[ts, t.ask, lastId]];
  dataBid = [[ts, t.bid, lastId]];

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

window.addEventListener('DOMContentLoaded', () => {
  chart = echarts.init(document.getElementById("main"));
  chart.setOption(option);
  ['ask', 'mid', 'bid'].forEach(id => {
    const box = document.getElementById(id + 'Checkbox');
    box.addEventListener('change', updateSeries);
  });
  chart.on('dataZoom', updateSeries);
  loadInitialData();
});

const versionDiv = document.createElement('div');
versionDiv.style.position = 'absolute';
versionDiv.style.left = '10px';
versionDiv.style.bottom = '8px';
versionDiv.style.color = '#777';
versionDiv.style.fontSize = '11px';
versionDiv.innerText = `bver: ${bver}, fver: ${fver}`;
document.body.appendChild(versionDiv);
