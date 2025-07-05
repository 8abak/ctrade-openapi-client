const bver = '2025.07.05.004', fver = '2025.07.06.ckbx.021';
let chart;
let dataMid = [], dataAsk = [], dataBid = [];

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
  if (!zoom) return { startValue: null, endValue: null };
  return {
    startValue: zoom.startValue ?? null,
    endValue: zoom.endValue ?? null
  };
}

function getVisibleYRange(startTime, endTime) {
  const visiblePrices = [];

  const useSeries = [];
  if (document.getElementById('askCheckbox').checked) useSeries.push(dataAsk);
  if (document.getElementById('midCheckbox').checked) useSeries.push(dataMid);
  if (document.getElementById('bidCheckbox').checked) useSeries.push(dataBid);

  for (const series of useSeries) {
    for (const point of series) {
      const t = point[0];
      if (t >= startTime && t <= endTime) {
        const price = point[1];
        if (typeof price === 'number') visiblePrices.push(price);
      }
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
  const [yMin, yMax] = getVisibleYRange(zoom.startValue ?? 0, zoom.endValue ?? Infinity);

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
    dataZoom: chart.getOption().dataZoom,
    series: updatedSeries
  }, true);
}

async function loadInitialData() {
  try {
    const now = new Date();
    const endTime = now.getTime();
    const startZoom = endTime - 5 * 60 * 1000;

    const dayStart = new Date(now);
    dayStart.setHours(8, 0, 0, 0);
    const dayStartUTC = new Date(dayStart.getTime() - SYDNEY_OFFSET * 60000).toISOString();

    const dayRes = await fetch(`/ticks/after/${dayStartUTC}?limit=5000`);
    const allTicks = await dayRes.json();
    if (!Array.isArray(allTicks)) return;

    dataMid = allTicks.map(t => [new Date(t.timestamp).getTime(), t.mid, t.id]);
    dataAsk = allTicks.map(t => [new Date(t.timestamp).getTime(), t.ask, t.id]);
    dataBid = allTicks.map(t => [new Date(t.timestamp).getTime(), t.bid, t.id]);

    chart.setOption({
      backgroundColor: option.backgroundColor,
      tooltip: option.tooltip,
      xAxis: {
        ...option.xAxis,
        min: dayStart.getTime(),
        max: endTime
      },
      yAxis: option.yAxis,
      dataZoom: [
        { ...option.dataZoom[0], startValue: startZoom, endValue: endTime },
        { ...option.dataZoom[1], startValue: startZoom, endValue: endTime }
      ]
    });

    updateSeries();
  } catch (err) {
    console.error("❌ loadInitialData() failed", err);
  }
}

window.addEventListener('DOMContentLoaded', () => {
  const main = document.getElementById('main');
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

  ask.addEventListener('change', updateSeries);
  mid.addEventListener('change', updateSeries);
  bid.addEventListener('change', updateSeries);
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
