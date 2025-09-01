// PATH: frontend/sql-core.js
const API = '/api';

async function fetchJSON(url, opts) {
  const r = await fetch(url, opts);
  if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
  const ct = r.headers.get('content-type') || '';
  return ct.includes('application/json') ? r.json() : r.text();
}

async function listTables() {
  // Try /api/tables then /api/sql/tables
  try { return await fetchJSON(`${API}/tables`); }
  catch { return await fetchJSON(`${API}/sqlvw/tables`); }
}

async function runSQL(q) {
  // Prefer POST /api/sql {sql:q}; fallback GET /api/sql?q=
  try {
    return await fetchJSON(`${API}/sql`, {
      method:'POST',
      headers:{'content-type':'application/json'},
      body: JSON.stringify({ sql:q, q }),
    });
  } catch {
    return await fetchJSON(`${API}/sql?q=` + encodeURIComponent(q));
  }
}

function renderTables(list) {
  const el = document.getElementById('tables');
  if (!Array.isArray(list)) list = (list?.tables) || [];
  el.innerHTML = '';
  if (!list.length) { el.textContent = '(no tables)'; return; }

  list.sort().forEach(name => {
    const a = document.createElement('div');
    a.textContent = name;
    a.style.cursor = 'pointer';
    a.onclick = () => {
      document.getElementById('query').value = `SELECT * FROM ${name} ORDER BY id DESC LIMIT 100;`;
    };
    el.appendChild(a);
  });
}

function renderResults(data) {
  const host = document.getElementById('results');
  host.innerHTML = '';
  let rows = [];
  if (Array.isArray(data)) rows = data;
  else if (Array.isArray(data?.rows)) rows = data.rows;
  else if (Array.isArray(data?.data)) rows = data.data;

  if (!rows.length) { host.innerHTML = '<div class="muted">No rows.</div>'; return; }

  const cols = Object.keys(rows[0]);
  const table = document.createElement('table');
  const thead = document.createElement('thead');
  const trh = document.createElement('tr');
  cols.forEach(c => { const th = document.createElement('th'); th.textContent = c; trh.appendChild(th); });
  thead.appendChild(trh);
  table.appendChild(thead);

  const tbody = document.createElement('tbody');
  rows.forEach(r => {
    const tr = document.createElement('tr');
    cols.forEach(c => {
      const td = document.createElement('td');
      let v = r[c];
      if (v === null || v === undefined) v = '';
      td.textContent = typeof v === 'object' ? JSON.stringify(v) : String(v);
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
  host.appendChild(table);
}

document.getElementById('btnRefresh').onclick = async () => {
  try { renderTables(await listTables()); }
  catch (e) { alert('Failed to load tables: '+ e.message); }
};
document.getElementById('btnRun').onclick = async () => {
  const q = document.getElementById('query').value.trim();
  if (!q) return;
  try { renderResults(await runSQL(q)); }
  catch (e) { alert('Query failed: ' + e.message); }
};

// initial load
document.getElementById('btnRefresh').click();
