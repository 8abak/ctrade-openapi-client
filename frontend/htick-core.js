// htick-core.js – Optimized for large datasets + clustering prep

const bver = '2025.07.05.004', fver = '2025.07.13.htick002';
let chart;
let dataMid = [], dataAsk = [], dataBid = [], labelSeries = [];
let currentStartEpoch = null, currentEndEpoch = null;
let adjusting = false;

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
      const timeStr = d.toLocaleTimeString("en-AU", { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });
      const dateStr = d.toLocaleDateString("en-AU");
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
    minInterval: 60 * 1000,
    axisLabel: {
      color: '#ccc',
      formatter: val => {
        const d = new Date(val);
        return `${d.toLocaleTimeString("en-AU", { hour: '2-digit', minute: '2-digit', hour12: false })}\n${d.getDate()} ${d.toLocaleString('default', { month: 'short' })}`;
      }
    },
    splitLine: { show: true, lineStyle: { color: '#333' } }
  },
  yAxis: {
    type: 'value',
    minInterval: 1,
    axisLabel: {
      color: '#ccc',
      formatter: val => Number(val).toFixed(0)
    },
    splitLine: { show: true, lineStyle: { color: '#333' } }
  },
  dataZoom: [
    { type: 'inside', throttle: 100 },
    { type: 'slider', height: 40, bottom: 0, handleStyle: { color: '#3fa9f5' } }
  ],
  series: []
};

function updateSeries() {
  if (!chart) return;
  const askBox = document.getElementById('askCheckbox');
  const midBox = document.getElementById('midCheckbox');
  const bidBox = document.getElementById('bidCheckbox');
  const updated = [];

  // Don't try updating chart if dataMid is empty
  if (dataMid.length === 0) return;

  if (askBox?.checked) updated.push({ id: 'ask', name: 'Ask', type: 'scatter', symbolSize: 2, itemStyle: { color: '#f5a623' }, data: sampleData(dataAsk) });
  if (midBox?.checked) updated.push({ id: 'mid', name: 'Mid', type: 'line', symbol: 'none', lineStyle: { width: 1, color: '#00bcd4' }, data: sampleData(dataMid) });
  if (bidBox?.checked) updated.push({ id: 'bid', name: 'Bid', type: 'scatter', symbolSize: 2, itemStyle: { color: '#4caf50' }, data: sampleData(dataBid) });

  const checkedLabels = Array.from(document.querySelectorAll(".labelCheckbox:checked")).map(c => c.value);
  const labelSeriesFiltered = labelSeries.filter(s => checkedLabels.includes(s.name));

  chart.setOption({ series: [...updated, ...labelSeriesFiltered] }, { replaceMerge: ['series'], lazyUpdate: true });

  adjustYAxisToZoom();
}



function adjustYAxisToZoom() {
  if (adjusting) return;
  adjusting = true;

  try {
    const zoom = chart.getOption().dataZoom?.[0];
    if (!zoom || zoom.startValue === undefined || zoom.endValue === undefined) return;

    const start = zoom.startValue;
    const end = zoom.endValue;
    const prices = [...dataMid, ...dataAsk, ...dataBid]
      .filter(p => p[0] >= start && p[0] <= end)
      .map(p => p[1]);

    if (!prices.length) return;

    const newMin = Math.floor(Math.min(...prices)) - 1;
    const newMax = Math.ceil(Math.max(...prices)) + 1;

    const current = chart.getOption()?.yAxis?.[0];
    if (current?.min === newMin && current?.max === newMax) return;

    chart.setOption({ yAxis: { min: newMin, max: newMax } }, true);
  } finally {
    adjusting = false;
  }
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
  console.log("Running tick query:", q);

  const res = await fetch(`/sqlvw/query?query=${encodeURIComponent(q)}`);
  const ticks = await res.json();

  if (!Array.isArray(ticks) || ticks.length === 0) {
    console.warn("⚠️ No tick data returned.");
    return;
  }

  console.log("Fetched tick count:", ticks.length);
  const parseTime = ts => Date.parse(ts);

  dataMid = ticks.map(t => [parseTime(t.timestamp), t.mid, t.id]);
  dataAsk = ticks.map(t => [parseTime(t.timestamp), t.ask, t.id]);
  dataBid = ticks.map(t => [parseTime(t.timestamp), t.bid, t.id]);

  console.log("Sample dataMid:", dataMid[0]);

  const zoomStart = currentStartEpoch;
  const zoomEnd = zoomStart + 2 * 60 * 60 * 1000; // limit to 2 hours

  chart.setOption({
    xAxis: { min: currentStartEpoch, max: currentEndEpoch },
    dataZoom: [
      { type: 'inside', startValue: zoomStart, endValue: zoomEnd },
      { type: 'slider', startValue: zoomStart, endValue: zoomEnd }
    ]
  });

  updateSeries();
  setTimeout(loadAllLabels, 300);
}

async function loadTicksByDay(dateStr) {
  const start = new Date(`${dateStr}T08:00:00Z`);
  const end = new Date(start.getTime() + 24 * 60 * 60 * 1000);  // +1 day

  const startIso = start.toISOString();
  const endIso = end.toISOString();

  const res = await fetch(`/ticks/range?start=${startIso}&end=${endIso}`);
  const ticks = await res.json();
  if (!Array.isArray(ticks) || ticks.length === 0) {
    console.warn("No ticks for this range.");
    return;
  }

  data = ticks.map(t => [t.timestamp, t.mid, t.id]);
  lastTimestamp = ticks[ticks.length - 1]?.timestamp;

  chart.setOption({
    xAxis: { data: data.map(d => d[0]) },
    series: [{ data }]
  });

  updateLabelView();
}


async function loadAllLabels() {
  let labelList;
  try {
    labelList = await fetch("/available").then(res => res.json());
  } catch (err) {
    console.error("❌ Failed to fetch label list:", err);
    return;
  }

  if (!Array.isArray(labelList)) {
    console.error("❌ Label list is not an array:", labelList);
    return;
  }

  const listContainer = document.getElementById("labelCheckboxes");
  listContainer.innerHTML = "";
  labelSeries = [];

  for (const table of labelList) {
    const div = document.createElement("div");
    const box = document.createElement("input");
    box.type = "checkbox"; box.value = table; box.className = "labelCheckbox";
    box.id = `label_${table}`; box.addEventListener("change", updateSeries);
    div.appendChild(box);

    const lbl = document.createElement("label");
    lbl.innerText = table;
    lbl.setAttribute("for", box.id);
    lbl.style.color = "#fff";
    div.appendChild(lbl);
    listContainer.appendChild(div);

    try {
      const q = `SELECT * FROM ${table}`;
      const res = await fetch(`/sqlvw/query?query=${encodeURIComponent(q)}`).then(r => r.json());

      if (!Array.isArray(res)) {
        console.warn(`⚠️ Table ${table} did not return rows.`);
        continue;
      }

      const sampleRow = res[0];
      if (!sampleRow) continue;

      if ('tickid' in sampleRow) {
        const points = res.map(row => {
          const ts = tickTimeById(row.tickid);
          return ts !== null ? [ts, row.label || 1, row.tickid] : null;
        }).filter(Boolean);

        labelSeries.push({
          id: table,
          name: table,
          type: 'scatter',
          symbolSize: 6,
          itemStyle: { color: '#e91e63' },
          data: points.map(p => [p[0], p[1], p[2]])
        });
      } else {
        console.warn(`ℹ️ Skipping label table ${table}: no tickid field.`);
      }

    } catch (err) {
      console.error(`❌ Failed to load label table ${table}:`, err);
    }
  }
}

function onHtDateChange() {
  const date = document.getElementById('htickDate').value;
  if (date) {
    loadDayTicks(date);
  }
}


function tickTimeById(tickid) {
  const match = dataMid.find(p => p[2] === tickid);
  return match?.[0] ?? null;
}

function sampleData(data) {
  const total = data.length;
  if (total < 5000) return data;
  const step = Math.floor(total / 3000); // limit to ~3000 points
  return data.filter((_, i) => i % step === 0);
}

window.addEventListener('DOMContentLoaded', () => {
  chart = echarts.init(document.getElementById("main"));
  chart.setOption(option);
  chart.on('dataZoom', debounce(updateSeries, 100));
  document.getElementById("loadButton").addEventListener("click", loadDayTicks);
});

const versionDiv = document.createElement('div');
versionDiv.style.position = 'absolute';
versionDiv.style.left = '10px';
versionDiv.style.bottom = '8px';
versionDiv.style.color = '#777';
versionDiv.style.fontSize = '11px';
document.body.appendChild(versionDiv);
fetch("/version").then(res => res.json()).then(v => {
  const val = v["htick"];
  versionDiv.innerHTML = `J: ${val?.js?.datetime || '-'} ${val?.js?.message || ''}<br>B: ${val?.py?.datetime || '-'} ${val?.py?.message || ''}<br>H: ${val?.html?.datetime || '-'} ${val?.html?.message || ''}`;
});

function debounce(fn, delay) {
  let timeout;
  return (...args) => {
    clearTimeout(timeout);
    timeout = setTimeout(() => fn(...args), delay);
  };
}
