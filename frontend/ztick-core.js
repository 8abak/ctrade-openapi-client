// ✅ FINAL ztick-core.js with zigzag level control (per table entry)

let chart;
let dataMid = [], dataAsk = [], dataBid = [], labelSeries = [], zigzagSeries = [], selectedTickIds = [], customHighlightIds = [];
let lastChecked = "";
let zigzagConfig = {}; // Store level style config

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
            const timeStr = date.toLocaleTimeString('en-AU', {
              hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false
            });
            const dateStr = date.toLocaleDateString('en-AU', {
              day: '2-digit', month: '2-digit', year: 'numeric'
            });
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
      chart.on("click", function (params) {
        const id = params?.value?.[2];
        if (!id) return;
        selectedTickIds = [id];
        document.getElementById("selectedIdsText").textContent = id;
        document.getElementById("labelControls").style.display = "block";
        updateZSeries();
      });
      resolve();
    }, 100);
  });
}

function updateZSeries() {
  const mid = document.getElementById('midCheckbox').checked;
  const ask = document.getElementById('askCheckbox').checked;
  const bid = document.getElementById('bidCheckbox').checked;
  const checked = Array.from(document.querySelectorAll(".labelCheckbox:checked")).map(e => e.value).join(",");
  const state = `${mid}${ask}${bid}:${checked}:${selectedTickIds.join(',')}:${customHighlightIds.join(',')}`;
  if (state === lastChecked) return;
  lastChecked = state;

  const base = [];
  if (ask) base.push({ name: 'Ask', type: 'scatter', symbolSize: 1, itemStyle: { color: '#f5a623' }, data: dataAsk, dimensions: ['timestamp', 'price', 'id', 'spread'], encode: {x:0, y:1, tooltip: [0,1,2]} });
  if (mid) base.push({ name: 'Mid', type: 'scatter', symbolSize: 1, itemStyle: { color: '#00bcd4' }, data: dataMid, dimensions: ['timestamp', 'price', 'id', 'spread'], encode: {x:0, y:1, tooltip: [0,1,2]} });
  if (bid) base.push({ name: 'Bid', type: 'scatter', symbolSize: 1, itemStyle: { color: '#4caf50' }, data: dataBid, dimensions: ['timestamp', 'price', 'id', 'spread'], encode: {x:0, y:1, tooltip: [0,1,2]} });

  const extras = labelSeries.filter(s => checked.includes(s.name));
  if (selectedTickIds.length) {
    const points = dataMid.filter(d => selectedTickIds.includes(d[2]));
    extras.push({ name: 'Selected', type: 'scatter', symbolSize: 7, itemStyle: { color: '#ff0', borderColor: '#fff', borderWidth: 1 }, data: points });
  }
  if (customHighlightIds.length) {
    const points = dataMid.filter(d => customHighlightIds.includes(d[2]));
    extras.push({ name: 'Extra', type: 'scatter', symbolSize: 6, itemStyle: { color: '#0ff', borderColor: '#fff', borderWidth: 1 }, data: points });
  }

  const zigLines = Object.entries(zigzagConfig).filter(([_, conf]) => conf.visible).map(([key, conf]) => {
    return {
      name: key,
      type: 'line',
      data: conf.data,
      showSymbol: false,
      lineStyle: { width: conf.thickness, color: conf.color },
      itemStyle: { color: conf.color }
    };
  });

  chart.setOption({ series: [...base, ...zigLines, ...extras] }, { replaceMerge: ['series'], lazyUpdate: true });
}

async function loadZigzagSettings() {
  const container = document.getElementById("zigzagSettingsContainer");
  const allData = await fetch(`/labels/zigzag_pivots`).then(r => r.json());
  const levels = [...new Set(allData.map(r => r.content))];
  zigzagConfig = {}; container.innerHTML = "";

  for (const level of levels) {
    const color = "#" + Math.floor(Math.random()*16777215).toString(16).padStart(6, '0');
    const filteredPoints = allData.filter(r => r.content === level).map(r => {
      const match = dataMid.find(d => d[2] === r.tickid);
      return match ? [match[0], match[1], r.tickid] : null;
    }).filter(Boolean);
    zigzagConfig[level] = { visible: false, color, thickness: 1.5, data: filteredPoints };

    const row = document.createElement("div");
    row.className = "zigzag-control";
    row.innerHTML = `
      <label><input type="checkbox" onchange="toggleZigzagLevel('${level}')"> ${level}</label><br>
      <input type="color" value="${color}" onchange="updateZigzagColor('${level}', this.value)">
      <input type="number" min="1" max="5" step="0.5" value="1.5" onchange="updateZigzagWidth('${level}', this.value)">
    `;
    container.appendChild(row);
  }
}

function toggleZigzagLevel(name) {
  zigzagConfig[name].visible = !zigzagConfig[name].visible;
  updateZSeries();
}
function updateZigzagColor(name, color) {
  zigzagConfig[name].color = color;
  updateZSeries();
}
function updateZigzagWidth(name, width) {
  zigzagConfig[name].thickness = parseFloat(width);
  updateZSeries();
}
