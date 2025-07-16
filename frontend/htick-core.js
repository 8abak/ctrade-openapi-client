// htick-core.js – Cleaned and aligned with /ticks/range

const bver = '2025.07.16.001', fver = '2025.07.16.htick003';
let chart;
let dataMid = [], dataAsk = [], dataBid = [], labelSeries = [];
let currentStartEpoch = null, currentEndEpoch = null;

const MAX_VISIBLE_POINTS = 3000;
const MAX_TOTAL_POINTS = 60000;  // Can be adjusted based on browser memory

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

  const checkedLabels = Array.from(document.querySelectorAll(".labelCheckbox:checked")).map(c => c.value);
  const filteredLabels = labelSeries.filter(s => checkedLabels.includes(s.name));

  chart.setOption({ series: [...updated, ...filteredLabels] }, { replaceMerge: ['series'], lazyUpdate: true });
  adjustYAxisToZoom();
}

function adjustYAxisToZoom() {
  const zoom = chart.getOption()?.dataZoom?.[0];
  if (!zoom || zoom.startValue === undefined || zoom.endValue === undefined) return;

  const start = zoom.startValue;
  const end = zoom.endValue;

  const visiblePoints = [...dataMid, ...dataAsk, ...dataBid]
    .filter(p => p[0] >= start && p[0] <= end)
    .map(p => p[1]);

  if (visiblePoints.length === 0) return;

  const newMin = Math.floor(Math.min(...visiblePoints)) - 1;
  const newMax = Math.ceil(Math.max(...visiblePoints)) + 1;

  chart.setOption({ yAxis: { min: newMin, max: newMax } });
}

function tickTimeById(tickid) {
  return dataMid.find(p => p[2] === tickid)?.[0] ?? null;
}

async function loadDayTicks() {
  const dateStr = document.getElementById("htickDate").value;
  if (!dateStr) return;

  const localHour = parseInt(document.getElementById("hourInput")?.value || '8', 10);
  const start = new Date(`${dateStr}T${String(localHour).padStart(2, '0')}:00:00+10:00`);
  const end = new Date(start.getTime() + 24 * 60 * 60 * 1000);
  currentStartEpoch = start.getTime();
  currentEndEpoch = end.getTime();

  const res = await fetch(`/ticks/range?start=${start.toISOString()}&end=${end.toISOString()}`);
  const ticks = await res.json();
  if (!Array.isArray(ticks) || ticks.length === 0) return;

  const parseTime = ts => Date.parse(ts);
  dataMid = ticks.map(t => [parseTime(t.timestamp), t.mid, t.id]);
  dataAsk = ticks.map(t => [parseTime(t.timestamp), t.ask, t.id]);
  dataBid = ticks.map(t => [parseTime(t.timestamp), t.bid, t.id]);

  const totalWindow = currentEndEpoch - currentStartEpoch;
  const defaultZoomEnd = currentStartEpoch + totalWindow / 2; // half-day default zoom

  chart.setOption({
    xAxis: { min: currentStartEpoch, max: currentEndEpoch },
    dataZoom: [
      { startValue: currentStartEpoch, endValue: defaultZoomEnd },
      { startValue: currentStartEpoch, endValue: defaultZoomEnd }
    ]
  });

  updateSeries();
  setTimeout(loadAllLabels, 300);
}

async function loadAllLabels() {
  let labelList = await fetch("/available").then(res => res.json()).catch(console.error);
  if (!Array.isArray(labelList)) return;

  const container = document.getElementById("labelCheckboxes");
  container.innerHTML = "";
  labelSeries = [];

  for (const table of labelList) {
    const box = document.createElement("input");
    box.type = "checkbox";
    box.value = table;
    box.className = "labelCheckbox";
    box.id = `label_${table}`;
    box.addEventListener("change", updateSeries);

    const label = document.createElement("label");
    label.htmlFor = box.id;
    label.innerText = table;
    label.style.color = '#fff';

    const div = document.createElement("div");
    div.append(box, label);
    container.appendChild(div);

    const q = `SELECT * FROM ${table}`;
    const res = await fetch(`/sqlvw/query?query=${encodeURIComponent(q)}`).then(r => r.json()).catch(console.error);
    if (!Array.isArray(res)) continue;

    const points = res.map(row => {
      const ts = tickTimeById(row.tickid);
      return ts ? [ts, row.label || 1, row.tickid] : null;
    }).filter(Boolean);

    labelSeries.push({
      id: table,
      name: table,
      type: 'scatter',
      symbolSize: 6,
      itemStyle: { color: '#e91e63' },
      data: points.map(p => [p[0], p[1], p[2]])
    });
  }
}

function debounce(fn, delay) {
  let timeout;
  return (...args) => {
    clearTimeout(timeout);
    timeout = setTimeout(() => fn(...args), delay);
  };
}

window.addEventListener('DOMContentLoaded', () => {
  chart = echarts.init(document.getElementById("main"));
  chart.setOption(option);
  chart.on('dataZoom', debounce(() => {
    updateSeries();
    adjustYAxisToZoom();
  }, 100));

  const loadBtn = document.getElementById("loadButton");
  if (loadBtn) loadBtn.addEventListener("click", loadDayTicks);

  const dateInput = document.getElementById("htickDate");
  if (dateInput) {
    dateInput.valueAsDate = new Date();
    loadDayTicks();
  }

  fetch("/version").then(res => res.json()).then(v => {
    const val = v["htick"];
    const versionDiv = document.createElement('div');
    versionDiv.style.position = 'absolute';
    versionDiv.style.left = '10px';
    versionDiv.style.bottom = '8px';
    versionDiv.style.color = '#777';
    versionDiv.style.fontSize = '11px';
    versionDiv.innerHTML = `J: ${val?.js?.datetime || '-'} ${val?.js?.message || ''}<br>B: ${val?.py?.datetime || '-'} ${val?.py?.message || ''}<br>H: ${val?.html?.datetime || '-'} ${val?.html?.message || ''}`;
    document.body.appendChild(versionDiv);
  });
});
