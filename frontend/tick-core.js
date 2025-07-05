const bver = '2025.07.05.004', fver = '2025.07.06.ckbx.018';
let chart;
let dataMid = [], dataAsk = [], dataBid = [], lastTimestamp = null;

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
  ]
};

function getZoomRange() {
  const zoom = chart.getOption().dataZoom?.[0];
  if (!zoom) return { start: 0, end: 100 };
  return { start: zoom.start ?? 0, end: zoom.end ?? 100 };
}

function getVisibleYRange(startPercent, endPercent) {
  const total = dataMid.length;
  const iStart = Math.floor((startPercent / 100) * total);
  const iEnd = Math.ceil((endPercent / 100) * total);

  const visiblePrices = [];

  const useSeries = [];
  if (document.getElementById('askCheckbox').checked) useSeries.push(dataAsk);
  if (document.getElementById('midCheckbox').checked) useSeries.push(dataMid);
  if (document.getElementById('bidCheckbox').checked) useSeries.push(dataBid);

  for (const series of useSeries) {
    for (let i = iStart; i < iEnd; i++) {
      const price = series[i]?.[1];
      if (typeof price === 'number') visiblePrices.push(price);
    }
  }

  if (visiblePrices.length === 0) return [null, null];
  return [Math.floor(Math.min(...visiblePrices)), Math.ceil(Math.max(...visiblePrices))];
}

function updateSeries() {
  const askBox = document.getElementById('askCheckbox');
  const midBox = document.getElementById('midCheckbox');
  const bidBox = document.getElementById('bidCheckbox');
  if (!askBox || !midBox || !bidBox || !chart) return;

  const zoom = getZoomRange();
  const [yMin, yMax] = getVisibleYRange(zoom.start, zoom.end);

  const updatedSeries = [];
  if (askBox.checked) updatedSeries.push({
    id: 'ask', name: 'Ask', type: 'scatter', symbolSize: 4,
    itemStyle: { color: '#f5a623' }, data: dataAsk
  });
  if (midBox.checked) updatedSeries.push({
    id: 'mid', name: 'Mid', type: 'scatter', symbolSize: 4,
    itemStyle: { color: '#00bcd4' }, data: dataMid
  });
  if (bidBox.checked) updatedSeries.push({
    id: 'bid', name: 'Bid', type: 'scatter', symbolSize: 4,
    itemStyle: { color: '#4caf50' }, data: dataBid
  });

  chart.setOption({
    backgroundColor: option.backgroundColor,
    tooltip: option.tooltip,
    xAxis: option.xAxis,
    yAxis: yMin !== null ? { ...option.yAxis, min: yMin, max: yMax } : option.yAxis,
    dataZoom: [
      { ...option.dataZoom[0], start: zoom.start, end: zoom.end },
      { ...option.dataZoom[1], start: zoom.start, end: zoom.end }
    ],
    series: updatedSeries
  }, true);
}

async function loadInitialData() {
  try {
    const latestRes = await fetch(`/ticks/recent?limit=1`);
    const latestTicks = await latestRes.json();
    if (!Array.isArray(latestTicks) || latestTicks.length === 0) return;

    const latest = latestTicks[0];
    const latestUtc = new Date(latest.timestamp);
    const localDate = toSydneyTime(latestUtc);
    lastTimestamp = latest.timestamp;

    const startOfDay = new Date(localDate);
    startOfDay.setHours(8, 0, 0, 0);
    const endOfDay = new Date(startOfDay);
    endOfDay.setDate(startOfDay.getDate() + 1);
    endOfDay.setHours(6, 59, 59, 999);

    const dayStartISO = new Date(startOfDay.getTime() - SYDNEY_OFFSET * 60000).toISOString();
    const dayRes = await fetch(`/ticks/after/${dayStartISO}?limit=5000`);
    const allTicks = await dayRes.json();
    if (!Array.isArray(allTicks)) return;

    dataMid = allTicks.map(t => [new Date(t.timestamp).getTime(), t.mid, t.id]);
    dataAsk = allTicks.map(t => [new Date(t.timestamp).getTime(), t.ask, t.id]);
    dataBid = allTicks.map(t => [new Date(t.timestamp).getTime(), t.bid, t.id]);

    updateSeries();
  } catch (err) {
    console.error("❌ loadInitialData() failed", err);
  }
}

window.addEventListener('DOMContentLoaded', () => {
  console.log("✅ DOM fully loaded");

  const main = document.getElementById('main');
  if (!main) return;

  chart = echarts.init(main);
  chart.setOption({
    backgroundColor: option.backgroundColor,
    tooltip: option.tooltip,
    xAxis: option.xAxis,
    yAxis: option.yAxis,
    dataZoom: option.dataZoom
  });

  const ask = document.getElementById('askCheckbox');
  const mid = document.getElementById('midCheckbox');
  const bid = document.getElementById('bidCheckbox');

  if (!ask || !mid || !bid) return;

  ask.addEventListener('change', updateSeries);
  mid.addEventListener('change', updateSeries);
  bid.addEventListener('change', updateSeries);

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
