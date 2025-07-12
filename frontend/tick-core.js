// tick-core.js (live flow + recursive backward loader)

let chart;
let dataMid = [], dataAsk = [], dataBid = [];
let lastId = null;
let tradingStartEpoch = null;

// adding loading set to avoid repeated loading
const loadedIds = new Set();


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
  adjustYAxisToZoom();
}

function adjustYAxisToZoom() {
  const zoom = chart.getOption().dataZoom?.[0];
  if (!zoom || zoom.startValue === undefined || zoom.endValue === undefined) return;

  const start = zoom.startValue;
  const end = zoom.endValue;

  const visiblePrices = [
    ...dataMid.filter(p => p[0] >= start && p[0] <= end).map(p => p[1]),
    ...dataAsk.filter(p => p[0] >= start && p[0] <= end).map(p => p[1]),
    ...dataBid.filter(p => p[0] >= start && p[0] <= end).map(p => p[1])
  ];

  if (visiblePrices.length === 0) return;

  const min = Math.min(...visiblePrices);
  const max = Math.max(...visiblePrices);

  const paddedMin = Math.floor(min) - 1;
  const paddedMax = Math.ceil(max) + 1;

  chart.setOption({
    yAxis: {
      min: paddedMin,
      max: paddedMax
    }
  });
}

async function loadInitialData() {
  const res = await fetch('/ticks/lastid');
  const { lastId: id, timestamp } = await res.json();
  lastId = id;

  const latestTickTime = new Date(timestamp);
  const tradingStart = new Date(latestTickTime);
  if (tradingStart.getHours() < 8) tradingStart.setDate(tradingStart.getDate() - 1);
  tradingStart.setHours(8, 0, 0, 0);
  tradingStartEpoch = tradingStart.getTime();

  const tradingEnd = new Date(tradingStart);
  tradingEnd.setDate(tradingStart.getDate() + 1);

  const xMin = tradingStart.getTime();
  const xMax = tradingEnd.getTime();

  const tickRes = await fetch(`/sqlvw/query?query=${encodeURIComponent(`SELECT bid, ask, mid, timestamp FROM ticks WHERE id = ${lastId}`)}`);
  const tickData = await tickRes.json();
  const t = tickData[0];
  if (!t) return;

  const ts = new Date(t.timestamp).getTime();
  dataMid = [[ts, t.mid, lastId]];
  dataAsk = [[ts, t.ask, lastId]];
  dataBid = [[ts, t.bid, lastId]];

  const zoomEnd = Math.ceil(ts / (60 * 1000)) * 60 * 1000;
  const zoomStart = zoomEnd - 5 * 60 * 1000;

  chart.setOption({
    xAxis: { min: xMin, max: xMax },
    series: [{ data: dataMid }, { data: dataAsk }, { data: dataBid }],
    dataZoom: [
      { type: 'inside', startValue: zoomStart, endValue: zoomEnd },
      { type: 'slider', startValue: zoomStart, endValue: zoomEnd, bottom: 0, height: 40 }
    ]
  });

  updateSeries();
  setupLiveSocket();
  loadPreviousTicksRecursive();
  showVersion();
}

function setupLiveSocket() {
  const ws = new WebSocket("wss://www.datavis.au/ws/ticks");
  ws.onopen = () => console.log("📡 WebSocket connected");
  ws.onmessage = (event) => {
    try {
      const tick = JSON.parse(event.data);
      console.log("📩 Tick received via WS:", tick, "LastID:", lastId);
      const ts = new Date(tick.timestamp).getTime();
      if (tick.id <= lastId) {
        console.warn("Dropped tick (duplicate or stale):", tick.id, "LastID:", lastId);
        return;
      }
      if (loadedIds.has(tick.id)) return;
      dataMid.push([ts, tick.mid, tick.id]);
      dataAsk.push([ts, tick.ask, tick.id]);
      dataBid.push([ts, tick.bid, tick.id]);
      lastId = tick.id;
      loadedIds.add(tick.id);
      updateSeries();
    } catch (err) {
      console.warn("🔄 Bad tick payload:", event.data);
    }
  };
  ws.onerror = (e) => console.warn("⚠️ WebSocket error", e);
  ws.onclose = () => console.warn("🔌 WebSocket closed.");
}

function format(v){
  return v ? `${v.datetime} ${v.message}` : '-';
}

async function showVersion() {
  try {
    const res = await fetch('/version');
    const versions = await res.json();
    const v = versions["tick"];

    if (!v) {
      versionDiv.innerText = "Version data not available";
      return;
    }

    versionDiv.innerHTML = `J: ${format(v.js)}<br>B: ${format(v.py)}<br>H: ${format(v.html)}`;
  } catch {
    versionDiv.innerText = "Error loading version data";
  }
}

async function loadPreviousTicksRecursive() {
  const oldest = dataMid[0]?.[2];
  if (!oldest) return;

  const res = await fetch(`/ticks/before/${oldest}?limit=5000`);
  const prev = await res.json();
  if (!prev.length) return;

  const mappedMid = prev.map(t => [new Date(t.timestamp).getTime(), t.mid, t.id]);
  const mappedAsk = prev.map(t => [new Date(t.timestamp).getTime(), t.ask, t.id]);
  const mappedBid = prev.map(t => [new Date(t.timestamp).getTime(), t.bid, t.id]);

  const newMid = mappedMid.filter(t => !dataMid.some(d => d[2] === t[2]));
  const newAsk = mappedAsk.filter(t => !dataAsk.some(d => d[2] === t[2]));
  const newBid = mappedBid.filter(t => !dataBid.some(d => d[2] === t[2]));

  newMid.forEach(p => loadedIds.add(p[2]));
  newAsk.forEach(p => loadedIds.add(p[2]));
  newBid.forEach(p => loadedIds.add(p[2]));

  dataMid = [...newMid, ...dataMid];
  dataAsk = [...newAsk, ...dataAsk];
  dataBid = [...newBid, ...dataBid];

  updateSeries();

  const firstTimestamp = mappedMid[0][0];
  if (firstTimestamp > tradingStartEpoch) {
    setTimeout(() => loadPreviousTicksRecursive(), 50);
  }
}

const versionDiv = document.createElement('div');
versionDiv.style.position = 'absolute';
versionDiv.style.left = '10px';
versionDiv.style.bottom = '8px';
versionDiv.style.color = '#777';
versionDiv.style.fontSize = '11px';
versionDiv.style.whiteSpace = 'pre-line';
document.body.appendChild(versionDiv);

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


