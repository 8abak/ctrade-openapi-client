// PATH: frontend/sql-core.js
// Simple SQL console UI. No ECharts. Works with both old (/sqlvw/*) and new (/api/sql/*) routes.

async function listTables() {
  const tries = ['/api/sql/tables', '/api/tables', '/sqlvw/tables'];
  for (const u of tries) {
    const r = await fetch(u);
    if (r.ok) return r.json();
  }
  throw new Error('404');
}

async function runSQL(q) {
  const attempts = [
    () => fetch('/api/sql', { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({sql:q}) }),
    () => fetch('/api/sql?q=' + encodeURIComponent(q)),
    () => fetch('/sqlvw/query?query=' + encodeURIComponent(q)),
  ];
  for (const fn of attempts) {
    const r = await fn();
    if (r.ok) return r.json();
  }
  throw new Error('Query endpoint not found');
}

async function execSQLDanger(sqlText) {
  const r = await fetch('/api/sql/exec?unsafe=true', {
    method:'POST',
    headers:{
      'content-type':'application/json',
      'X-Allow-Write':'yes'
    },
    body: JSON.stringify({ sql: sqlText })
  });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

document.getElementById('btnExec')?.addEventListener('click', async () => {
  const q = document.getElementById('query').value.trim();
  if (!q) return;
  try {
    const payload = await execSQLDanger(q);
    // show rowcounts / result sets
    const host = document.getElementById('results');
    host.innerHTML = '';
    (payload.results || []).forEach((res, i) => {
      const pre = document.createElement('pre');
      pre.style.whiteSpace = 'pre-wrap';
      pre.textContent = (res.type === 'rowcount')
        ? `Statement ${i+1}: ${res.rowcount} rows affected`
        : JSON.stringify(res.rows, null, 2);
      host.appendChild(pre);
    });
  } catch (e) {
    alert('Exec failed: ' + e.message);
  }
});


function renderTables(list) {
  const el = document.getElementById('tables');
  el.innerHTML = '';
  (list || []).forEach(name => {
    const div = document.createElement('div');
    div.textContent = name;
    div.style.cursor = 'pointer';
    div.onclick = () => {
      document.getElementById('query').value = `SELECT * FROM ${name} ORDER BY id DESC LIMIT 100;`;
    };
    el.appendChild(div);
  });
}

function renderResults(payload) {
  // payload may be {rows:[...]} or bare array
  const rows = Array.isArray(payload) ? payload : (payload?.rows || []);
  const host = document.getElementById('results');
  host.innerHTML = '';
  if (!rows.length) { host.innerHTML = '<div style="color:#93a4b8">No rows.</div>'; return; }

  const cols = Object.keys(rows[0]);
  const table = document.createElement('table');
  table.style.width = '100%'; table.style.borderCollapse = 'collapse';
  const thead = document.createElement('thead');
  const trh = document.createElement('tr');
  cols.forEach(c => { const th = document.createElement('th'); th.textContent = c; th.style.borderBottom='1px solid #1f2a37'; th.style.padding='6px 8px'; th.style.textAlign='left'; th.style.fontFamily='ui-monospace,Consolas,monaco,monospace'; trh.appendChild(th); });
  thead.appendChild(trh); table.appendChild(thead);

  const tbody = document.createElement('tbody');
  rows.forEach(r => {
    const tr = document.createElement('tr');
    cols.forEach(c => {
      const td = document.createElement('td');
      let v = r[c]; if (v === null || v === undefined) v = '';
      td.textContent = (typeof v === 'object') ? JSON.stringify(v) : String(v);
      td.style.borderBottom='1px solid #1f2a37'; td.style.padding='6px 8px';
      td.style.whiteSpace='nowrap'; td.style.fontFamily='ui-monospace,Consolas,monaco,monospace';
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
  host.appendChild(table);
}

// Wire buttons
document.getElementById('btnRefresh')?.addEventListener('click', async () => {
  try { renderTables(await listTables()); }
  catch (e) { alert('Failed to load tables: ' + e.message); }
});
document.getElementById('btnRun')?.addEventListener('click', async () => {
  const q = document.getElementById('query').value.trim();
  if (!q) return;
  try { renderResults(await runSQL(q)); }
  catch (e) { alert('Query failed: ' + e.message); }
});

// Initial load
document.getElementById('btnRefresh')?.click();
