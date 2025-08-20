// sql-core.js — drop-in replacement
(() => {
  const $ = (sel, el = document) => el.querySelector(sel);

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

  // ---- helpers -------------------------------------------------------------

  // Run raw SQL against backend: returns { rows, fields? }
  async function runSQL(sql) {
    try {
      statusEl.textContent = 'Running…';
      const res  = await fetch('/api/sql', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ sql })
      });
      const data = await res.json();
      statusEl.textContent = 'Success.';
      return data;
    } catch (e) {
      statusEl.textContent = 'Error';
      resultDiv.innerHTML = `<div class="muted">SQL error: ${String(e)}</div>`;
      return { rows: [] };
    }
  }

  function renderTable(data) {
    if (!data || !data.rows || !data.rows.length) {
      resultDiv.innerHTML = '<div class="muted">No rows.</div>';
      return;
    }
    const cols  = Object.keys(data.rows[0]);
    const thead = '<thead><tr>' + cols.map(c => `<th>${c}</th>`).join('') + '</tr></thead>';
    const tbody = '<tbody>' + data.rows.map(r =>
      '<tr>' + cols.map(c => `<td>${r[c] ?? ''}</td>`).join('') + '</tr>'
    ).join('') + '</tbody>';
    resultDiv.innerHTML = `<div class="card" style="padding:0"><table>${thead}${tbody}</table></div>`;
  }

  function pickDefaultOrderKey(columns) {
    if (!columns) return null;
    if (columns.includes('tickid')) return 'tickid';
    if (columns.includes('id'))     return 'id';
    return null;
  }

  // Fast estimate using pg_class (instant) — shown with a leading ~
  async function fetchEstimateCount(table) {
    const sql = `
      SELECT COALESCE(reltuples::bigint,0) AS estimate
      FROM pg_class c
      JOIN pg_namespace n ON n.oid=c.relnamespace
      WHERE n.nspname='public' AND c.relname='${table}' AND c.relkind='r'
    `;
    const { rows } = await runSQL(sql);
    return rows?.[0]?.estimate ?? 0n;
  }

  // Exact COUNT(*) — slower on big tables
  async function fetchExactCount(table) {
    const { rows } = await runSQL(`SELECT COUNT(*) AS n FROM ${table}`);
    return rows?.[0]?.n ?? 0;
  }

  // ---- table loading / metadata -------------------------------------------

  async function loadTables() {
    tableSelect.innerHTML = '<option>Loading…</option>';
    // Bring back all public tables + their columns
    const sql = `
      SELECT t.table_name,
             array_agg(c.column_name) FILTER (WHERE c.column_name IS NOT NULL) AS cols
      FROM information_schema.tables t
      LEFT JOIN information_schema.columns c
        ON c.table_schema=t.table_schema AND c.table_name=t.table_name
      WHERE t.table_schema='public'
      GROUP BY t.table_name
      ORDER BY t.table_name
    `;
    const { rows } = await runSQL(sql);

    tableSelect.innerHTML = '';
    rows.forEach(r => {
      const opt = document.createElement('option');
      opt.value = r.table_name;
      opt.textContent = r.table_name;
      opt.dataset.columns = JSON.stringify(r.cols || []);
      tableSelect.appendChild(opt);
    });

    if (rows.length) {
      tableSelect.selectedIndex = 0;
      onTableChange(); // auto-initialize first table
    }
  }

  async function onTableChange() {
    structureWrap.style.display = 'none';
    resultDiv.innerHTML = '';
    const table = tableSelect.value;
    structureHint.textContent = table;

    // 1) show estimate immediately, then replace with exact count
    countEl.textContent = '…';
    try {
      const est = await fetchEstimateCount(table);
      countEl.textContent = `~${est}`;
    } catch {
      /* ignore estimate errors */
    }
    // fire-and-forget exact count (updates when ready)
    fetchExactCount(table).then(exact => { countEl.textContent = `${exact}`; }).catch(()=>{});

    // 2) detect default sorting key
    let cols = [];
    try { cols = JSON.parse(tableSelect.selectedOptions[0].dataset.columns || '[]'); } catch {}
    const key = pickDefaultOrderKey(cols);
    sortKeyEl.textContent = key || '—';

    // 3) set query template for this table
    setQueryTemplate(table, key);
  }

  // ---- structure / describe -----------------------------------------------

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
      `<tr><td>${r.ordinal_position}</td><td>${r.column_name}</td><td>${r.data_type}</td><td>${r.is_nullable}</td></tr>`
    ).join('') + '</tbody>';
    structureTable.innerHTML = thead + tbody;
    structureWrap.style.display = 'block';
  }

  // ---- query templates -----------------------------------------------------

  function setQueryTemplate(table, sortKey) {
    const mode = templateSelect.value;
    if (mode === 'count') {
      sqlBox.value = `SELECT COUNT(*) FROM ${table};`;
      return;
    }
    if (mode === 'all') {
      sqlBox.value = `SELECT * FROM ${table};`;
      return;
    }
    // Auto (sorted)
    const order = sortKey ? ` ORDER BY ${sortKey}` : '';
    sqlBox.value = `SELECT * FROM ${table}${order} LIMIT 100;`;
  }

  // ---- events --------------------------------------------------------------

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

  // ---- init ---------------------------------------------------------------

  loadTables();
})();
