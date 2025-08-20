// sql-core.js — uses /sqlvw/query and /sqlvw/tables
(() => {
  const $ = (s, el = document) => el.querySelector(s);

  // UI refs
  const resultDiv      = $('#result');
  const sqlBox         = $('#sqlBox');
  const statusEl       = $('#status');
  const tableSelect    = $('#tableSelect');
  const countEl        = $('#tblCount');
  const sortKeyEl      = $('#tblSort');
  const refreshBtn     = $('#refreshTables');
  const templateSelect = $('#templateSelect');
  const btnDescribe    = $('#btnDescribe');
  const btnPreview     = $('#btnPreview');
  const structureWrap  = $('#structure');
  const structureTable = $('#structureTable');
  const structureHint  = $('#structureHint');

  // Backend routes from backend/main.py
  const API = {
    tables: '/sqlvw/tables',
    query:  '/sqlvw/query',  // GET ?query=<SQL>
  };

  // ---------------- helpers ----------------
  function escapeHTML(s) {
    return String(s).replace(/[&<>"']/g, m => (
      {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]
    ));
  }

  function normalize(payload) {
    // Accept shapes: array, {rows:[...]}, {data:[...]}, {message:...}
    if (Array.isArray(payload)) return { rows: payload };
    if (payload && Array.isArray(payload.rows)) return { rows: payload.rows };
    if (payload && Array.isArray(payload.data)) return { rows: payload.data };
    if (payload && payload.message) {
      resultDiv.innerHTML = `<div class="muted">${escapeHTML(payload.message)}</div>`;
      return { rows: [] };
    }
    return { rows: [] };
  }

  async function runSQL(sql) {
    statusEl.textContent = 'Running…';
    try {
      const url = `${API.query}?query=${encodeURIComponent(sql)}`;
      const res = await fetch(url, { method: 'GET' });
      const payload = await res.json();
      if (!res.ok || payload?.error) throw new Error(payload?.error || res.statusText);
      statusEl.textContent = 'Success.';
      return normalize(payload);
    } catch (e) {
      statusEl.textContent = 'Error';
      resultDiv.innerHTML = `<div class="muted">SQL error: ${escapeHTML(e.message || e)}</div>`;
      return { rows: [] };
    }
  }

  function renderTable(data) {
    if (!data || !data.rows || !data.rows.length) {
      resultDiv.innerHTML = '<div class="muted">No rows.</div>';
      return;
    }
    const cols  = Object.keys(data.rows[0]);
    const thead = '<thead><tr>' + cols.map(c => `<th>${escapeHTML(c)}</th>`).join('') + '</tr></thead>';
    const tbody = '<tbody>' + data.rows.map(r =>
      '<tr>' + cols.map(c => `<td>${escapeHTML(r[c] ?? '')}</td>`).join('') + '</tr>'
    ).join('') + '</tbody>';
    resultDiv.innerHTML = `<div class="card" style="padding:0"><table>${thead}${tbody}</table></div>`;
  }

  function pickDefaultOrderKey(columns) {
    if (!columns) return null;
    if (columns.includes('tickid')) return 'tickid';
    if (columns.includes('id'))     return 'id';
    return null;
  }

  // Fast estimate first, then exact count
  async function fetchEstimateCount(table) {
    const sql = `
      SELECT COALESCE(reltuples::bigint,0) AS estimate
      FROM pg_class c
      JOIN pg_namespace n ON n.oid=c.relnamespace
      WHERE n.nspname='public' AND c.relname='${table}' AND c.relkind='r'
    `;
    const { rows } = await runSQL(sql);
    return rows?.[0]?.estimate ?? 0;
  }
  async function fetchExactCount(table) {
    const { rows } = await runSQL(`SELECT COUNT(*) AS n FROM ${table}`);
    return rows?.[0]?.n ?? 0;
  }

  // --------------- load tables ---------------
  async function loadTables() {
    tableSelect.innerHTML = '<option>Loading…</option>';
    const res = await fetch(API.tables);
    const names = await res.json(); // array of table names
    tableSelect.innerHTML = '';
    // We’ll look up columns lazily via information_schema when needed
    names.forEach(name => {
      const opt = document.createElement('option');
      opt.value = name;
      opt.textContent = name;
      tableSelect.appendChild(opt);
    });
    if (names.length) {
      tableSelect.selectedIndex = 0;
      onTableChange();
    }
  }

  async function fetchColumns(table) {
    const sql = `
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema='public' AND table_name='${table}'
      ORDER BY ordinal_position
    `;
    const { rows } = await runSQL(sql);
    return rows.map(r => r.column_name);
  }

  async function onTableChange() {
    structureWrap.style.display = 'none';
    resultDiv.innerHTML = '';
    const table = tableSelect.value;
    structureHint.textContent = table;

    // counts
    countEl.textContent = '…';
    fetchEstimateCount(table).then(est => { countEl.textContent = `~${est}`; }).catch(()=>{});
    fetchExactCount(table).then(exact => { countEl.textContent = `${exact}`; }).catch(()=>{});

    // sort key from actual columns
    const cols = await fetchColumns(table);
    const key = pickDefaultOrderKey(cols);
    sortKeyEl.textContent = key || '—';

    setQueryTemplate(table, key);
  }

  // --------------- describe / preview ---------------
  async function describeTable(table) {
    const sql = `
      SELECT ordinal_position, column_name, data_type, is_nullable
      FROM information_schema.columns
      WHERE table_schema='public' AND table_name='${table}'
      ORDER BY ordinal_position
    `;
    const { rows } = await runSQL(sql);
    return rows || [];
  }

  async function onDescribeClick() {
    const table = tableSelect.value;
    const cols = await describeTable(table);
    if (!cols.length) { structureWrap.style.display = 'none'; return; }

    const thead = `
      <thead><tr>
        <th>#</th><th>column</th><th>type</th><th>null</th>
      </tr></thead>`;
    const tbody = '<tbody>' + cols.map(r =>
      `<tr><td>${r.ordinal_position}</td><td>${escapeHTML(r.column_name)}</td><td>${escapeHTML(r.data_type)}</td><td>${escapeHTML(r.is_nullable)}</td></tr>`
    ).join('') + '</tbody>';
    structureTable.innerHTML = thead + tbody;
    structureWrap.style.display = 'block';
  }

  // --------------- query template ---------------
  function setQueryTemplate(table, sortKey) {
    const mode = templateSelect.value;
    if (mode === 'count')  { sqlBox.value = `SELECT COUNT(*) FROM ${table};`; return; }
    if (mode === 'all')    { sqlBox.value = `SELECT * FROM ${table};`; return; }
    const order = sortKey ? ` ORDER BY ${sortKey}` : '';
    sqlBox.value = `SELECT * FROM ${table}${order} LIMIT 100;`;
  }

  // --------------- events ---------------
  refreshBtn.addEventListener('click', loadTables);
  tableSelect.addEventListener('change', onTableChange);

  $('#run').addEventListener('click', async () => {
    const data = await runSQL(sqlBox.value);
    renderTable(data);
  });

  btnDescribe.addEventListener('click', onDescribeClick);
  btnPreview.addEventListener('click', () => {
    const table = tableSelect.value;
    const key   = sortKeyEl.textContent !== '—' ? sortKeyEl.textContent : null;
    setQueryTemplate(table, key);
  });

  templateSelect.addEventListener('change', () => {
    const table = tableSelect.value;
    const key   = sortKeyEl.textContent !== '—' ? sortKeyEl.textContent : null;
    setQueryTemplate(table, key);
  });

  // init
  loadTables();
})();
