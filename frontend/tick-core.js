// tick-core.js — Dot View, Locked Zoom Window, Dual Version
let data = [], lastTimestamp = null;
const chart = echarts.init(document.getElementById('main'));

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
      const date = new Date(p.value[0]);
      date.setMinutes(date.getMinutes() + 600);
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
        const d = new Date(val);
        d.setMinutes(d.getMinutes() + 600);
        return `${d.getHours()}:${String(d.getMinutes()).padStart(2, '0')}` + `\n${d.toLocaleDateString('en-AU', { month: 'short', day: 'numeric' })}`;
      }
    },
    splitNumber: 12,
    minInterval: 60 * 1000 * 5,
    splitLine: {
      show: true,
      lineStyle: {
        color: '#333'
      }
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
      lineStyle: {
        color: '#333'
      }
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
    const tickDate = new Date(t.timestamp);
    const tickTime = tickDate.getTime();
    lastTimestamp = t.timestamp;
    data = [[tickTime, t.mid, t.id]];

    const dayStart = new Date(tickDate);
    dayStart.setUTCHours(0, 0, 0, 0);
    const dayEnd = new Date(tickDate);
    dayEnd.setUTCHours(23, 59, 59, 999);

    const tickMinute = new Date(tickDate);
    tickMinute.setSeconds(0, 0);
    const chartStart = new Date(tickMinute);
    chartStart.setMinutes(chartStart.getMinutes() - 5);
    const chartEnd = new Date(tickMinute);
    chartEnd.setMinutes(chartEnd.getMinutes() + 1);

    const price = t.mid;
    const isInteger = price === Math.floor(price);
    const yMin = isInteger ? price - 1 : Math.floor(price);
    const yMax = isInteger ? price + 1 : Math.ceil(price);

    chart.setOption({
      series: [{ data }],
      xAxis: {
        min: dayStart.getTime(),
        max: dayEnd.getTime()
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
