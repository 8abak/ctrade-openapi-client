// tick-core.js — Dot View, Locked Zoom Window, Sydney Day View
const bver = '2025.07.05.004', fver = '2025.07.05.009';
let data = [], lastTimestamp = null;
const chart = echarts.init(document.getElementById('main'));

const SYDNEY_OFFSET = 600; // +10:00 UTC in minutes
const SYDNEY_DAY_START_HOUR = 8;
const SYDNEY_DAY_END_HOUR = 6;

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
    splitNumber: 12,
    minInterval: 60 * 1000 * 5,
    splitLine: {
      show: true,
      lineStyle: { color: '#333' }
    }
  },
  yAxis: {
    type: 'value',
    scale: true,
    minInterval: 1,
    axisLabel: {
      color: '#ccc',
      formatter: val => val.toFixed(1)
    },
    splitLine: {
      show: true,
      lineStyle: { color: '#333' }
    }
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
    const res = await fetch(`/ticks/recent?limit=1`);
    const ticks = await res.json();
    if (!Array.isArray(ticks) || ticks.length === 0) return;

    const t = ticks[0];
    const utcDate = new Date(t.timestamp);
    const localDate = toSydneyTime(utcDate);
    const tickTime = utcDate.getTime();
    lastTimestamp = t.timestamp;
    data = [[tickTime, t.mid, t.id]];

    const tickMinute = new Date(localDate);
    tickMinute.setSeconds(0, 0);
    const chartStart = new Date(tickMinute);
    chartStart.setMinutes(chartStart.getMinutes() - 4);
    const chartEnd = new Date(tickMinute);
    chartEnd.setMinutes(chartEnd.getMinutes() + 1);

    // Define Sydney session start/end
    const sydneyStart = new Date(localDate);
    if (sydneyStart.getHours() < SYDNEY_DAY_START_HOUR) {
      sydneyStart.setDate(sydneyStart.getDate() - 1);
    }
    sydneyStart.setHours(SYDNEY_DAY_START_HOUR, 0, 0, 0);
    const sydneyEnd = new Date(sydneyStart);
    sydneyEnd.setDate(sydneyStart.getDate() + 1);
    sydneyEnd.setHours(SYDNEY_DAY_END_HOUR, 59, 59, 999);

    const price = t.mid;
    const yMin = Math.floor(price);
    const yMax = Number.isInteger(price) ? price + 1 : Math.ceil(price);

    chart.setOption({
      series: [{ data }],
      xAxis: {
        min: sydneyStart.getTime() - SYDNEY_OFFSET * 60000,
        max: sydneyEnd.getTime() - SYDNEY_OFFSET * 60000
      },
      yAxis: {
        min: yMin,
        max: yMax
      },
      dataZoom: [
        {
          type: 'inside',
          startValue: chartStart.getTime(),
          endValue: chartEnd.getTime(),
          realtime: false
        },
        {
          type: 'slider',
          startValue: chartStart.getTime(),
          endValue: chartEnd.getTime(),
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
