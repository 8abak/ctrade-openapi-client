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

  const zoomEnd = Math.ceil(lastTimestamp / (60 * 1000)) * 60 * 1000;
  const zoomStart = zoomEnd - 5 * 60 * 1000;

  chart.setOption({
    xAxis: { min: tradingStart.getTime(), max: zoomEnd + 60 * 1000 },
    dataZoom: [
      { startValue: zoomStart, endValue: zoomEnd },
      { startValue: zoomStart, endValue: zoomEnd }
    ]
  });

  updateSeries();
  connectLiveSocket();
}

function connectLiveSocket() {
  const ws = new WebSocket("wss://www.datavis.au/ws/ticks");
  ws.onopen = () => console.log("📡 Connected");
  ws.onmessage = (e) => {
    try {
      const tick = JSON.parse(e.data);
      const ts = Date.parse(tick.timestamp);
      if (ts <= lastTimestamp) return;
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
  loadInitialTickRange();
});
