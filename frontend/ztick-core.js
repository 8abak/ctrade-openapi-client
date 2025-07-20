let chart = echarts.init(document.getElementById('main'));
let dataMid = [], dataAsk = [], dataBid = [], labelSeries = [];
let selectedTickIds = [];

const option = {
  backgroundColor: '#111',
  tooltip: {
    trigger: 'axis',
    formatter: (params) => {
      const d = new Date(params[0].value[0]);
      const timeStr = d.toLocaleTimeString("en-AU", { hour: '2-digit', minute: '2-digit', second: '2-digit' });
      const dateStr = d.toLocaleDateString("en-AU");
      const tickId = params[0].value[2];
      const spread = params[0].value[3];
      let html = `<div>🆔 <strong>${tickId}</strong><br>${timeStr}<br>${dateStr}<br>`;
      params.forEach(p => {
        if (p.seriesName === 'Mid') {
          html += `${p.seriesName}: <strong style='color:${p.color}'>${p.value[1]}</strong> (Spread: ${spread})<br>`;
        } else {
          html += `${p.seriesName}: <strong style='color:${p.color}'>${p.value[1]}</strong><br>`;
        }
      });
      return html + '</div>';
    }
  },
  xAxis: { type: 'time', axisLabel: { color: '#ccc' }, splitLine: { lineStyle: { color: '#333' } } },
  yAxis: { type: 'value', scale: true, axisLabel: { color: '#ccc' }, splitLine: { lineStyle: { color: '#333' } } },
  dataZoom: [{ type: 'inside' }, { type: 'slider', height: 40, bottom: 0 }],
  series: []
};

chart.setOption(option);

chart.on('click', function (params) {
  if (!params || !params.value || params.seriesName === undefined) return;
  const id = params.value[2];
  if (window.event.ctrlKey) {
    if (!selectedTickIds.includes(id)) selectedTickIds.push(id);
  } else {
    selectedTickIds = [id];
  }
  updateSelectionBox();
});

function updateSelectionBox() {
  document.getElementById("selectedIdsText").textContent = selectedTickIds.join(", ") || "None";
}

function getDatetimeFromSelectors(prefix) {
  const y = document.getElementById(prefix + 'Year').value;
  const m = document.getElementById(prefix + 'Month').value;
  const d = document.getElementById(prefix + 'Day').value;
  const h = document.getElementById(prefix + 'Hour').value;
  const min = document.getElementById(prefix + 'Minute').value;
  return new Date(`${y}-${m}-${d}T${h.padStart(2, '0')}:${min.padStart(2, '0')}:00Z`);
}

async function loadZTickChart() {
  const start = getDatetimeFromSelectors('start');
  const end = getDatetimeFromSelectors('end');
  const res = await fetch(`/ticks/range?start=${start.toISOString()}&end=${end.toISOString()}`);
  const ticks = await res.json();
  if (!Array.isArray(ticks)) return;
  const parseTime = ts => Date.parse(ts);
  dataMid = ticks.map(t => [parseTime(t.timestamp), t.mid, t.id, ((t.ask - t.bid) / 2).toFixed(2)]);
  dataAsk = ticks.map(t => [parseTime(t.timestamp), t.ask, t.id]);
  dataBid = ticks.map(t => [parseTime(t.timestamp), t.bid, t.id]);
  updateZSeries();
}

function updateZSeries() {
  const mid = document.getElementById('midCheckbox').checked;
  const ask = document.getElementById('askCheckbox').checked;
  const bid = document.getElementById('bidCheckbox').checked;
  const base = [];
  if (ask) base.push({ name: 'Ask', type: 'scatter', symbolSize: 2, itemStyle: { color: '#f5a623' }, data: dataAsk });
  if (mid) base.push({ name: 'Mid', type: 'scatter', symbolSize: 2, itemStyle: { color: '#00bcd4' }, data: dataMid });
  if (bid) base.push({ name: 'Bid', type: 'scatter', symbolSize: 2, itemStyle: { color: '#4caf50' }, data: dataBid });

  const selectedLabels = Array.from(document.querySelectorAll('.labelCheckbox:checked')).map(cb => cb.value);
  const extras = labelSeries.filter(s => selectedLabels.includes(s.name));

  chart.setOption({ series: [...base, ...extras] }, { replaceMerge: ['series'], lazyUpdate: true });
}

function fillTimeSelectors() {
  const now = new Date();
  const years = [now.getFullYear() - 1, now.getFullYear(), now.getFullYear() + 1];
  const months = [...Array(12).keys()].map(m => String(m + 1).padStart(2, '0'));
  const days = [...Array(31).keys()].map(d => String(d + 1).padStart(2, '0'));
  const hours = [...Array(24).keys()].map(h => String(h).padStart(2, '0'));
  const minutes = [...Array(60).keys()].map(m => String(m).padStart(2, '0'));

  const fill = (id, values, selected) => {
    const el = document.getElementById(id);
    el.innerHTML = values.map(v => `<option value="${v}" ${v == selected ? 'selected' : ''}>${v}</option>`).join('');
  };

  fill('startYear', years, now.getFullYear());
  fill('endYear', years, now.getFullYear());
  fill('startMonth', months, String(now.getMonth() + 1).padStart(2, '0'));
  fill('endMonth', months, String(now.getMonth() + 1).padStart(2, '0'));
  fill('startDay', days, String(now.getDate()).padStart(2, '0'));
  fill('endDay', days, String(now.getDate()).padStart(2, '0'));
  fill('startHour', hours, '08');
  fill('endHour', hours, '23');
  fill('startMinute', minutes, '00');
  fill('endMinute', minutes, '59');
}

async function loadLabelCheckboxes() {
  const container = document.getElementById('labelCheckboxes');
  const selector = document.getElementById('labelTableSelect');
  const tables = await fetch("/labels/available").then(r => r.json()).catch(console.error);
  container.innerHTML = "";
  selector.innerHTML = "";

  for (const name of tables) {
    const id = `label_${name}`;
    container.insertAdjacentHTML('beforeend', `
      <label><input type="checkbox" class="labelCheckbox" id="${id}" value="${name}" onchange="updateZSeries()"> ${name}</label><br>
    `);
    selector.insertAdjacentHTML('beforeend', `<option value="${name}">${name}</option>`);
    const data = await fetch(`/labels/${name}`).then(r => r.json()).catch(() => []);
    const points = data.map(row => {
      const match = dataMid.find(p => p[2] === row.tickid);
      return match ? [match[0], 1, row.tickid] : null;
    }).filter(Boolean);
    labelSeries.push({ name, type: 'scatter', symbolSize: 6, itemStyle: { color: '#e91e63' }, data: points });
  }
}

async function submitLabel() {
  const table = document.getElementById('labelTableSelect').value;
  const note = document.getElementById('labelNote').value.trim();
  if (!table || selectedTickIds.length === 0) return;
  const res = await fetch("/labels/assign", {
    method: "POST",
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ table, ids: selectedTickIds, note })
  });
  if (res.ok) {
    alert("Labels assigned");
    selectedTickIds = [];
    updateSelectionBox();
  } else {
    alert("Error assigning labels");
  }
}

function exportSelectedTicks() {
  const selected = dataMid.filter(d => selectedTickIds.includes(d[2]));
  if (selected.length === 0) return;
  const csv = ["timestamp,mid,id,spread"];
  selected.forEach(r => {
    csv.push(`${new Date(r[0]).toISOString()},${r[1]},${r[2]},${r[3]}`);
  });
  const blob = new Blob([csv.join("\\n")], { type: "text/csv" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = \"selected_ticks.csv\";
  a.click();
  URL.revokeObjectURL(url);
}

window.addEventListener('DOMContentLoaded', () => {
  fillTimeSelectors();
  loadLabelCheckboxes();
  document.getElementById('askCheckbox').addEventListener('change', updateZSeries);
  document.getElementById('midCheckbox').addEventListener('change', updateZSeries);
  document.getElementById('bidCheckbox').addEventListener('change', updateZSeries);
  loadZTickChart();
});
