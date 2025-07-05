// tick-core.js — Dot View, Locked Zoom Window, Sydney Day View (Last Tick + 2 Prior, Load All for Day)
const bver = '2025.07.05.004', fver = '2025.07.05.015';
let data = [], lastTimestamp = null;
const chart = echarts.init(document.getElementById('main'));

const SYDNEY_OFFSET = 600; // +10:00 UTC in minutes

function toSydneyTime(date) {
  return new Date(date.getTime() + SYDNEY_OFFSET * 60000);
}

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
      const date = toSydneyTime(new Date(p.value[0]));
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
        const d = toSydneyTime(new Date(val));
        return `${d.getHours()}:${String(d.getMinutes()).padStart(2, '0')}` + `\n${d.toLocaleDateString('en-AU', { month: 'short', day: 'numeric' })}`;
      }
    },
    splitLine: { show: true, lineStyle: { color: '#333' } }
  },
  yAxis: {
    type: 'value',
    scale: true,
    minInterval: 1,
    axisLabel: {
      color: '#ccc',
      formatter: val => Math.floor(val)
    },
    splitLine: { show: true, lineStyle: { color: '#333' } }
  },
  dataZoom: [
    { type: 'inside', realtime: false },
    { type: 'slider', height: 40, bottom: 0, handleStyle: { color: '#3fa9f5' }, realtime: false }
  ],
  series: [{
    name: 'Mid Price',
    type: 'scatter',
    symbolSize: 4,
    data: []
  }]
};

chart.setOption(option);

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
    const dayEndISO = new Date(endOfDay.getTime() - SYDNEY_OFFSET * 60000).toISOString();

    const dayRes = await fetch(`/ticks/after/${dayStartISO}?limit=5000`);
    const allTicks = await dayRes.json();
    if (!Array.isArray(allTicks)) return;

    data = allTicks.map(t => [new Date(t.timestamp).getTime(), t.mid, t.id]);

    const priceVals = allTicks.map(t => t.mid);
    const yMin = Math.floor(Math.min(...priceVals));
    const yMax = Math.ceil(Math.max(...priceVals));

    chart.setOption({
      series: [{ data }],
      xAxis: {
        min: startOfDay.getTime() - SYDNEY_OFFSET * 60000,
        max: endOfDay.getTime() - SYDNEY_OFFSET * 60000
      },
      yAxis: {
        min: yMin,
        max: yMax
      },
      dataZoom: [
        {
          type: 'inside',
          startValue: latestUtc.getTime() - 4 * 60000,
          endValue: latestUtc.getTime(),
          realtime: false
        },
        {
          type: 'slider',
          startValue: latestUtc.getTime() - 4 * 60000,
          endValue: latestUtc.getTime(),
          bottom: 0,
          height: 40,
          realtime: false
        }
      ]
    });
  } catch (err) {
    console.error("❌ loadInitialData() failed", err);
  }
}

loadInitialData();

// Version footer
const versionDiv = document.createElement('div');
versionDiv.style.position = 'absolute';
versionDiv.style.left = '10px';
versionDiv.style.bottom = '8px';
versionDiv.style.color = '#777';
versionDiv.style.fontSize = '11px';
versionDiv.innerText = `bver: ${bver}, fver: ${fver}`;
document.body.appendChild(versionDiv);
