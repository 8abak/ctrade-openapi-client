// htick-core.js

let chart;
let dataMid = [], dataAsk = [], dataBid = [];
let labelSeries = [];
let currentStartEpoch = null;
let currentEndEpoch = null;

let zoomTimer = null;
chart.on('dataZoom', () => {
  if (zoomTimer) clearTimeout(zoomTimer);
  zoomTimer = setTimeout(() => updateSeries(), 80); // throttle to every 80ms
});


const option = {
  backgroundColor: "#111",
  tooltip: {
    trigger: "axis",
    backgroundColor: "#222",
    borderColor: "#555",
    borderWidth: 1,
    textStyle: { color: "#fff", fontSize: 13 },
    formatter: (params) => {
      const d = new Date(params[0].value[0]);
      const timeStr = d.toLocaleTimeString("en-AU", {
        hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false
      });
      const dateStr = d.toLocaleDateString("en-AU");
      let tooltip = `<div style="padding: 8px;"><strong>${timeStr}</strong><br><span style="color: #ccc;">${dateStr}</span><br>`;
      params.forEach(p => {
        tooltip += `${p.seriesName}: <strong style="color: ${p.color};">${p.value[1]}</strong><br>`;
      });
      tooltip += `</div>`;
      return tooltip;
    }
  },
  xAxis: {
    type: "time",
    minInterval: 60 * 1000,
    axisLabel: {
      color: "#ccc",
      formatter: val => {
        const d = new Date(val);
        return `${d.toLocaleTimeString("en-AU", { hour: '2-digit', minute: '2-digit', hour12: false })}\n${d.getDate()} ${d.toLocaleString('default', { month: 'short' })}`;
      }
    },
    splitLine: { show: true, lineStyle: { color: "#333" } }
  },
  yAxis: {
    type: "value",
    minInterval: 1,
    axisLabel: {
      color: "#ccc",
      formatter: val => Number(val).toFixed(0)
    },
    splitLine: { show: true, lineStyle: { color: "#333" } }
  },
  dataZoom: [
    { type: 'inside', realtime: false },
    { type: 'slider', height: 40, bottom: 0, handleStyle: { color: '#3fa9f5' }, realtime: false }
  ],
  series: []
};

function updateSeries() {
  const askBox = document.getElementById('askCheckbox');
  const midBox = document.getElementById('midCheckbox');
  const bidBox = document.getElementById('bidCheckbox');

  const baseProbs = {
    type: 'scatter',
    symbolSize: 3,
    progressive: 5000,
    large: true,
    largeThreshold: 10000,
    animation: false
  };

  const updated = [];
  if (askBox?.checked) updated.push({ ...baseProps, id: 'ask', name: 'Ask', itemStyle: { color: '#f5a623' }, data: dataAsk });
  if (midBox?.checked) updated.push({ ...baseProps, id: 'mid', name: 'Mid', itemStyle: { color: '#00bcd4' }, data: dataMid });
  if (bidBox?.checked) updated.push({ ...baseProps, id: 'bid', name: 'Bid', itemStyle: { color: '#4caf50' }, data: dataBid });

  const checkedLabels = Array.from(document.querySelectorAll(".labelCheckbox:checked")).map(c => c.value);
  const labelSeriesFiltered = labelSeries.filter(s => checkedLabels.includes(s.name));

  chart.setOption({ series: [...updated, ...labelSeriesFiltered] }, { replaceMerge: ['series'] });
  adjustYAxisToZoom();
}

function adjustYAxisToZoom() {
  const zoom = chart.getOption().dataZoom?.[0];
  if (!zoom || zoom.startValue === undefined || zoom.endValue === undefined) return;

  const start = zoom.startValue;
  const end = zoom.endValue;

  const prices = [...dataMid, ...dataAsk, ...dataBid].filter(p => p[0] >= start && p[0] <= end).map(p => p[1]);
  if (!prices.length) return;

  chart.setOption({ yAxis: { min: Math.floor(Math.min(...prices)) - 1, max: Math.ceil(Math.max(...prices)) + 1 } });
}

async function loadDayTicks() {
  const dateStr = document.getElementById("dateInput").value;
  const hour = parseInt(document.getElementById("hourInput").value, 10);

  if (!dateStr || isNaN(hour)) return;

  const start = new Date(`${dateStr}T${hour.toString().padStart(2, '0')}:00:00+10:00`);
  const end = new Date(start);
  end.setDate(start.getDate() + 1);

  currentStartEpoch = start.getTime();
  currentEndEpoch = end.getTime();

  const q = `SELECT id, timestamp, bid, ask, mid FROM ticks WHERE timestamp >= '${start.toISOString()}' AND timestamp < '${end.toISOString()}' ORDER BY id ASC`;
  const res = await fetch(`/sqlvw/query?query=${encodeURIComponent(q)}`);
  const ticks = await res.json();

  dataMid = ticks.map(t => [new Date(t.timestamp).getTime(), t.mid, t.id]);
  dataAsk = ticks.map(t => [new Date(t.timestamp).getTime(), t.ask, t.id]);
  dataBid = ticks.map(t => [new Date(t.timestamp).getTime(), t.bid, t.id]);

  const minTime = currentStartEpoch;
  const maxTime = currentEndEpoch;

  chart.setOption({
    xAxis: { min: minTime, max: maxTime },
    dataZoom: [
      { type: 'inside', startValue: minTime, endValue: maxTime },
      { type: 'slider', startValue: minTime, endValue: maxTime, bottom: 0, height: 40 }
    ]
  });

  await loadAllLabels(); // overlays from DB
  updateSeries();
  showVersion();
}

async function showVersion(){
  try{
    const res = await fetch('/version');
    const versions = await res.json();
    const v = vestions["htick"];

    if (!v) {
      versionDiv.innerText = "Version data not available";
      return;
    }

    versionDiv.innerHTML = `J: ${v.js || '-'}\nB: ${v.py || '-'}\nH: ${v.html || '-'}`;
  } catch {
    versionDiv.innerText = "Error loading version data";
  }
}

async function loadAllLabels() {
  const labelList = await fetch("/labels/available").then(res => res.json());
  const listContainer = document.getElementById("labelCheckboxes");
  listContainer.innerHTML = "";

  labelSeries = [];

  for (const table of labelList) {
    const div = document.createElement("div");
    const box = document.createElement("input");
    box.type = "checkbox";
    box.value = table;
    box.className = "labelCheckbox";
    box.id = `label_${table}`;
    box.addEventListener("change", updateSeries);
    div.appendChild(box);

    const lbl = document.createElement("label");
    lbl.innerText = table;
    lbl.setAttribute("for", box.id);
    lbl.style.color = "#fff";
    div.appendChild(lbl);
    listContainer.appendChild(div);

    const q = `SELECT tickid, label FROM ${table}`;
    const res = await fetch(`/sqlvw/query?query=${encodeURIComponent(q)}`).then(r => r.json());
    const points = res.map(row => [tickTimeById(row.tickid), row.label, row.tickid]).filter(p => p[0] !== null);

    const s = {
      id: table,
      name: table,
      type: 'scatter',
      symbolSize: 6,
      itemStyle: { color: '#e91e63' },
      data: points.map(p => [p[0], p[1], p[2]])
    };
    labelSeries.push(s);
  }
}

function tickTimeById(tickid) {
  const match = dataMid.find(p => p[2] === tickid);
  return match?.[0] ?? null;
}

window.addEventListener('DOMContentLoaded', () => {
  chart = echarts.init(document.getElementById("main"), null, { renderer: 'canvas' });
  chart = echarts.init(document.getElementById("main"));
  chart.setOption(option);
  chart.on('dataZoom', updateSeries);

  document.getElementById("loadButton").addEventListener("click", loadDayTicks);
});

const versionDiv = document.createElement('div');
versionDiv.style.position = 'absolute';
versionDiv.style.left = '10px';
versionDiv.style.bottom = '8px';
versionDiv.style.color = '#777';
versionDiv.style.fontSize = '11px';
document.body.appendChild(versionDiv);