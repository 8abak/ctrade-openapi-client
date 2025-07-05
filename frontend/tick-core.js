const bver = '2025.07.05.004', fver = '2025.07.06.ckbx.010';
let dataMid = [], dataAsk = [], dataBid = [], lastTimestamp = null;
const chart = echarts.init(document.getElementById("main"));

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
      const timeStr = date.toLocaleTimeString("en-AU", { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: true });
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
        return `${d.getHours().toString().padStart(2, '0')}:${d.getMinutes().toString().padStart(2, '0')}` + `\n${d.toLocaleDateString('en-AU', { month: 'short', day: 'numeric' })}`;
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
    const dayRes = await fetch(`/ticks/after/${dayStartISO}?limit=5000`);
    const allTicks = await dayRes.json();
    if (!Array.isArray(allTicks)) return;

    dataMid = allTicks.map(t => [new Date(t.timestamp).getTime(), t.mid, t.id]);
    dataAsk = allTicks.map(t => [new Date(t.timestamp).getTime(), t.ask, t.id]);
    dataBid = allTicks.map(t => [new Date(t.timestamp).getTime(), t.bid, t.id]);

    const yMin = Math.floor(Math.min(...allTicks.flatMap(t => [t.mid, t.ask, t.bid])));
    const yMax = Math.ceil(Math.max(...allTicks.flatMap(t => [t.mid, t.ask, t.bid])));

    updateSeries();
    chart.setOption({
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
          startValue: latestUtc.getTime() - 4 * 60 * 1000, // 4 minutes before latest tick
          endValue: latestUtc.getTime(),
          realtime: false
        },
        {
          type: 'slider',
          startValue: latestUtc.getTime() - 4 * 60 * 1000, // 4 minutes before latest tick
          endValue: latestUtc.getTime(),
          bottom: 0,
          height: 40,
          realtime: false,
        }
      ]
    });
  } catch (err) {
    console.error("❌ loadInitialData() failed", err);
  }
}





// ✅ Add update series function
function updateSeries() {
  console.log("Updating series visibility...");

  const askBox = document.getElementById('askCheckbox');
  const midBox = document.getElementById('midCheckbox');
  const bidBox = document.getElementById('bidCheckbox');
  if (!askBox || !midBox || !bidBox) return;

  const updatedSeries = [
    {
      id: 'ask',
      name: 'Ask',
      type: 'scatter',
      symbolSize: 4,
      itemStyle: { color: '#f5a623' },
      data: dataAsk,
      show: askBox.checked
    },
    {
      id: 'mid',
      name: 'Mid',
      type: 'scatter',
      symbolSize: 4,
      itemStyle: { color: '#00bcd4' },
      data: dataMid,
      show: midBox.checked
    },
    {
      id: 'bid',
      name: 'Bid',
      type: 'scatter',
      symbolSize: 4,
      itemStyle: { color: '#4caf50' },
      data: dataBid,
      show: bidBox.checked
    }
  ];

  chart.setOption({ series: updatedSeries });
}

//add checkbox listeners and initial data load
window.addEventListener('DOMContentLoaded', () => {
  console.log("✅ DOM fully loaded");

  const ask = document.getElementById('askCheckbox');
  const mid = document.getElementById('midCheckbox');
  const bid = document.getElementById('bidCheckbox');

  if (!ask || !mid || !bid) {
    console.error("❌ One or more checkboxes not found");
    return;
  }

  // Initial visibility
  updateSeries();

  // Bind listeners
  ask.addEventListener('change', () => {
    console.log("🔁 Ask toggled", ask.checked);
    updateSeries();
  });
  mid.addEventListener('change', () => {
    console.log("🔁 Mid toggled", mid.checked);
    updateSeries();
  });
  bid.addEventListener('change', () => {
    console.log("🔁 Bid toggled", bid.checked);
    updateSeries();
  });

  // Load data after DOM ready and bindings done
  loadInitialData();
});



// ✅ Version display
const versionDiv = document.createElement('div');
versionDiv.style.position = 'absolute';
versionDiv.style.left = '10px';
versionDiv.style.bottom = '8px';
versionDiv.style.color = '#777';
versionDiv.style.fontSize = '11px';
versionDiv.innerText = `bver: ${bver}, fver: ${fver}`;
document.body.appendChild(versionDiv);
