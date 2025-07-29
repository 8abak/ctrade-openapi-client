// ✅ FINAL zview-core.js – Zigzag-only optimized viewer with date selection + sorted control UI

let chart;
let zigzagSeries = {}, dataById = {}, activeZigzags = {}, currentStart = null, currentEnd = null;

async function initializeChart() {
  return new Promise((resolve) => {
    setTimeout(() => {
      chart = echarts.init(document.getElementById("main"));
      chart.setOption({
        backgroundColor: "#111",
        tooltip: {
          trigger: 'axis',
          axisPointer: { type: 'cross' },
          backgroundColor: '#222',
          borderColor: '#555',
          borderWidth: 1,
          textStyle: { color: '#fff', fontSize: 13 },
          formatter: (params) => {
            const p = params[0];
            const date = new Date(p.value[0]);
            const timeStr = date.toLocaleTimeString('en-AU', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });
            const dateStr = date.toLocaleDateString('en-AU');
            return `<div style="padding: 8px;">
              <strong>${timeStr}</strong><br>
              <span style="color: #ccc;">${dateStr}</span><br>
              Price: <strong style="color: #3fa9f5;">${p.value[1]?.toFixed(2)}</strong><br>
              ID: <span style="color:#aaa;">${p.value[2]}</span>
            </div>`;
          }
        },
        xAxis: { type: 'time', axisLabel: { color: '#ccc' }, splitLine: { lineStyle: { color: '#333' } } },
        yAxis: { type: 'value', scale: true, axisLabel: { color: '#ccc' }, splitLine: { lineStyle: { color: '#333' } } },
        dataZoom: [ { type: 'inside' }, { type: 'slider', height: 40, bottom: 0 } ],
        series: []
      });
      resolve();
    }, 100);
  });
}

async function fetchZigzagData() {
  const res = await fetch(`/labels/zigzag_pivots`);
  return await res.json();
}

function sortZigzagLevels(levels) {
  const order = level => {
    if (level.startsWith("zAtr")) return "1" + level;
    if (level.startsWith("zAbs")) return "2" + level;
    if (level.startsWith("zPct")) return "3" + level;
    return "9" + level;
  };
  return levels.sort((a, b) => order(a).localeCompare(order(b)));
}

function getDateRange() {
  const start = document.getElementById("startTime").value;
  const end = document.getElementById("endTime").value;
  if (!start || !end) return null;
  return [new Date(start).toISOString(), new Date(end).toISOString()];
}

function updateChartSeries() {
  const lines = Object.entries(activeZigzags).map(([level, conf]) => {
    return {
      name: level,
      type: 'line',
      data: conf.data,
      showSymbol: false,
      lineStyle: { width: conf.thickness, color: conf.color },
      itemStyle: { color: conf.color }
    };
  });
  chart.setOption({ series: lines }, { replaceMerge: ['series'], lazyUpdate: true });
}

async function handleZigzagToggle(level, checked) {
  if (!checked) {
    delete activeZigzags[level];
    updateChartSeries();
    return;
  }

  const raw = zigzagSeries[level];
  const ids = raw.map(p => p.tickid);
  const tickMap = await fetch(`/ticks/between-ids?start=${Math.min(...ids)}&end=${Math.max(...ids)}`).then(r => r.json());
  const idToPoint = {};
  for (const t of tickMap) idToPoint[t.id] = [Date.parse(t.timestamp), t.mid, t.id];
  dataById = { ...dataById, ...idToPoint };

  const final = raw.map(p => idToPoint[p.tickid]).filter(Boolean);
  activeZigzags[level] = {
    color: document.getElementById(`color-${level}`).value,
    thickness: parseFloat(document.getElementById(`width-${level}`).value),
    data: final
  };
  updateChartSeries();
}

function setupControls(levels) {
  const container = document.getElementById("zigzagControls");
  container.innerHTML = "";
  for (const level of sortZigzagLevels(levels)) {
    const row = document.createElement("div");
    row.className = "zigzag-row";
    row.innerHTML = `
      <label><input type="checkbox" onchange="handleZigzagToggle('${level}', this.checked)"> ${level}</label>
      <input type="color" id="color-${level}" value="#${Math.floor(Math.random()*16777215).toString(16).padStart(6, '0')}">
      <input type="number" id="width-${level}" value="1.5" step="0.5" min="0.5" max="5">
    `;
    container.appendChild(row);
  }
}

window.loadZigzags = async function loadZigzags() {
  if (!chart) await initializeChart();
  const range = getDateRange();
  if (!range) return alert("Please select both start and end time.");

  currentStart = range[0];
  currentEnd = range[1];

  const data = await fetchZigzagData();
  const filtered = data.filter(p => p.timestamp >= currentStart && p.timestamp <= currentEnd);
  zigzagSeries = {};
  for (const p of filtered) {
    if (!zigzagSeries[p.level]) zigzagSeries[p.level] = [];
    zigzagSeries[p.level].push(p);
  }
  setupControls(Object.keys(zigzagSeries));
  activeZigzags = {};
  chart.setOption({ series: [] });
};

fetch("/version").then(r => r.json()).then(v => {
  const el = document.getElementById("version");
  if (el) el.textContent = `zview-core.js version: ${v?.zview || 'unknown'}`;
});
