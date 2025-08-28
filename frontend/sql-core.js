//# PATH: frontend/sql-core.js
const API = '/sqlvw';
const ddl = document.getElementById('tables');
const sql = document.getElementById('sql');
const gridHead = document.querySelector('#grid thead');
const gridBody = document.querySelector('#grid tbody');

async function loadTables(){
  const res = await fetch(`${API}/tables`);
  const tabs = await res.json();
  ddl.innerHTML = '';
  tabs.forEach(t=>{
    const opt = document.createElement('option');
    opt.value = t; opt.textContent = t;
    ddl.appendChild(opt);
  });
  // focus on our new tables
  const prefer = ['outcome','segm','bigm','smal','pred','stat','ticks'];
  const first = prefer.find(p => tabs.includes(p)) || tabs[0];
  ddl.value = first;
  setDefaultQuery();
}
function setDefaultQuery(){
  const t = ddl.value;
  sql.value = `SELECT * FROM ${t} ORDER BY id DESC LIMIT 100;`;
}
ddl.onchange = setDefaultQuery;
document.getElementById('refresh').onclick = loadTables;

document.getElementById('run').onclick = async ()=>{
  const res = await fetch(`/sqlvw/query?query=${encodeURIComponent(sql.value)}`);
  const data = await res.json();
  renderTable(Array.isArray(data) ? data : []);
};

function renderTable(rows){
  gridHead.innerHTML = ''; gridBody.innerHTML = '';
  if (!rows.length){ return; }
  const cols = Object.keys(rows[0]);
  const trh = document.createElement('tr');
  cols.forEach(c=>{
    const th = document.createElement('th'); th.textContent = c;
    trh.appendChild(th);
  });
  gridHead.appendChild(trh);
  rows.forEach(r=>{
    const tr = document.createElement('tr');
    cols.forEach(c=>{
      const td = document.createElement('td');
      const v = r[c];
      td.textContent = (v===null || v===undefined) ? '' : v;
      tr.appendChild(td);
    });
    gridBody.appendChild(tr);
  });
}

loadTables();
