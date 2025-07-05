const bver = '2025.07.05.004', fver = '2025.07.06.ckbx.027';
let chart;
let dataMid = [], dataAsk = [], dataBid = [];

const SYDNEY_OFFSET = 600; // in minutes

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
      const timeStr = date.toLocaleTimeString("en-AU", {
        hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: true
      });
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
        return `${d.getHours().toString().padStart(2, '0')}:${d.getMinutes().toString().padStart(2, '0')}` +
               `\n${d.toLocaleDateString('en-AU', { month: 'short', day: 'numeric' })}`;
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
  series: [
    { id: 'ask', name: 'Ask', type: 'scatter', data: [], symbolSize: 4, itemStyle: { color: '#f5a623' } },
    { id: 'mid', name: 'Mid', type: 'scatter', data: [], symbolSize: 4, itemStyle: { color: '#00bcd4' } },
    { id: 'bid', name: 'Bid', type: 'scatter', data: [], symbolSize: 4, itemStyle: { color: '#4caf50' } }
  ]
};

function updateSeries() {
  const askBox = document.getElementById('askCheckbox');
  const midBox = document.getElementById('midCheckbox');
  const bidBox = document.getElementById('bidCheckbox');
  if (!askBox || !midBox || !bidBox) return;

  chart.setOption({
    series: [
      { id: 'ask', data: dataAsk, show: askBox.checked },
      { id: 'mid', data: dataMid, show: midBox.checked },
      { id: 'bid', data: dataBid, show: bidBox.checked }
    ]
  });
}

async function loadInitialData() {
  try {
    const latestRes = await fetch(`/ticks/recent?limit=1`);
    const latestTicks = await latestRes.json();
    if (!Array.isArray(latestTicks) || latestTicks.length === 0) {
      console.warn("⚠️ No latest tick returned.");
      return;
    }
    const latestTick = latestTicks[0];
    const latestTimeUTC = new Date(latestTick.timestamp);
    const latestTimeSydney = toSydneyTime(latestTimeUTC);

    // Get 08:00 local time of that day (Sydney)
    const startLocal = new Date(latestTimeSydney);
    startLocal.setHours(8, 0, 0, 0);
    const startTimeUTC = new Date(startLocal.getTime() - SYDNEY_OFFSET * 60000);
    const startTimeISO = startTimeUTC.toISOString();

    // Load ticks
    const dayRes = await fetch(`/ticks/after/${startTimeISO}?limit=5000`);
    const allTicks = await dayRes.json();
    if (!Array.isArray(allTicks) || allTicks.length === 0) {
      console.warn("⚠️ No ticks loaded from server.");
      return;
    }

    dataMid = allTicks.map(t => [new Date(t.timestamp).getTime(), t.mid, t.id]);
    dataAsk = allTicks.map(t => [new Date(t.timestamp).getTime(), t.ask, t.id]);
    dataBid = allTicks.map(t => [new Date(t.timestamp).getTime(), t.bid, t.id]);

    const lastTickTime = new Date(latestTick.timestamp).getTime();
    const zoomStart = lastTickTime - 4 * 60 * 1000;

    // Set xAxis min/max (08:00 → next day 07:00)
    const xMin = startLocal.getTime() - SYDNEY_OFFSET * 60000;
    const endLocal = new Date(startLocal);
    endLocal.setDate(endLocal.getDate() + 1);
    endLocal.setHours(7, 0, 0, 0);
    const xMax = endLocal.getTime() - SYDNEY_OFFSET * 60000;

    chart.setOption({
      xAxis: { min: xMin, max: xMax },
      series: [
        { id: 'ask', data: dataAsk },
        { id: 'mid', data: dataMid },
        { id: 'bid', data: dataBid }
      ]
    }, false);

    chart.setOption({
      dataZoom: [
        { type: 'inside', startValue: zoomStart, endValue: lastTickTime, realtime: false },
        { type: 'slider', startValue: zoomStart, endValue: lastTickTime, bottom: 0, height: 40, realtime: false }
      ]
    }, false);

    updateSeries();
  } catch (err) {
    console.error("❌ loadInitialData() failed", err);
  }
}

window.addEventListener('DOMContentLoaded', () => {
  chart = echarts.init(document.getElementById("main"));
  chart.setOption(option);

  ['ask', 'mid', 'bid'].forEach(type => {
    const box = document.getElementById(`${type}Checkbox`);
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
