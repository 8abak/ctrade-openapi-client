// ✅ Minimal zview-core.js — Only Zigzag, Fast Rendering

let chart;
let zigzagSeries = {}, selectedLevels = {};

async function initializeZigzagChart() {
  chart = echarts.init(document.getElementById("main"));
  chart.setOption({
    backgroundColor: "#111",
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'line' },
      backgroundColor: '#222',
      borderColor: '#555',
      borderWidth: 1,
      textStyle: { color: '#fff', fontSize: 13 }
    },
    xAxis: { type: 'time', axisLabel: { color: '#ccc' }, splitLine: { lineStyle: { color: '#333' } } },
    yAxis: { type: 'value', scale: true, axisLabel: { color: '#ccc' }, splitLine: { lineStyle: { color: '#333' } } },
    dataZoom: [{ type: 'slider', bottom: 0 }, { type: 'inside' }],
    series: []
  });
}

async function loadZigzagLevels() {
  const raw = await fetch('/labels/zigzag_pivots').then(r => r.json());
  const levels = [...new Set(raw.map(r => r.level))];
  const container = document.getElementById("zigzagControls");
  container.innerHTML = "";
  zigzagSeries = {};

  for (const level of levels) {
    const color = '#' + Math.floor(Math.random() * 16777215).toString(16).padStart(6, '0');
    zigzagSeries[level] = {
      data: raw.filter(p => p.level === level).map(p => [new Date(p.timestamp).getTime(), p.price]),
      color,
      thickness: 1.5,
      visible: false
    };

    const row = document.createElement("div");
    row.className = "zigzag-row";
    row.innerHTML = `
      <label><input type="checkbox" onchange="toggleZigzag('${level}')"> ${level}</label><br>
      <input type="color" value="${color}" onchange="changeZigzagColor('${level}', this.value)">
      <input type="number" value="1.5" min="1" max="6" step="0.5" onchange="changeZigzagWidth('${level}', this.value)">
    `;
    container.appendChild(row);
  }
}

function toggleZigzag(level) {
  zigzagSeries[level].visible = !zigzagSeries[level].visible;
  updateZigzagSeries();
}
function changeZigzagColor(level, color) {
  zigzagSeries[level].color = color;
  updateZigzagSeries();
}
function changeZigzagWidth(level, width) {
  zigzagSeries[level].thickness = parseFloat(width);
  updateZigzagSeries();
}

function updateZigzagSeries() {
  const visibleSeries = Object.entries(zigzagSeries).filter(([_, conf]) => conf.visible).map(([name, conf]) => ({
    name,
    type: 'line',
    data: conf.data,
    lineStyle: { color: conf.color, width: conf.thickness },
    showSymbol: false
  }));
  chart.setOption({ series: visibleSeries }, { replaceMerge: ['series'] });
}

window.onload = async () => {
  await initializeZigzagChart();
  await loadZigzagLevels();
};
