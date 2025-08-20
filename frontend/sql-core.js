// sql-core.js — hardened drop-in
(() => {
  const $ = (s, el = document) => el.querySelector(s);

  // UI
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

  // ------------------------------------------------------------------------
  // Backend compatibility: try multiple endpoints + payload keys, remember the
  // one that works in sessionStorage to keep it fast on subsequent calls.
  // ------------------------------------------------------------------------
  const CANDIDATE_ENDPOINTS = ['/api/sql', '/sql'];
  const CANDIDATE_KEYS      = ['sql', 'query', 'q'];

  async function postJSON(url, body, timeoutMs = 7000) {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), timeoutMs);
    try {
      const res = await fetch(url, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(body),
        signal: ctrl.signal,
      });
      const text = await res.text();
      // Some backends may return text/plain JSON
      const json = text ? JSON.parse(text) : {};
      if (!res.ok) throw new Error(json?.error || res.statusText);
      return json;
    } finally {
      clearTimeout(t);
    }
  }

  async function negotiate(sql) {
    // 1) Try cached winner
    const cached = sessionStorage.getItem('sqlCompat');
    if (cached) {
      const { url, key } = JSON.parse(cached);
      try {
        const data = await postJSON(url, { [key]: sql });
        return { data, url, key };
      } catch { /* fall through */ }
    }
    // 2) Probe candidates
    for (const url of CANDIDATE_ENDPOINTS) {
      for (const key of CANDIDATE_KEYS) {
        try {
          const data = await postJSON(url, { [key]: sql });
          sessionStorage.setItem('sqlCompat', JSON.stringify({ url, key }));
          return { data, url, key };
        } catch { /* try next */ }
      }
    }
    throw new Error('No SQL endpoint responded.');
  }

  async function runSQL(sql) {
    try {
      statusEl.textContent = 'Running…';
      const { data } = await negotiate(sql);
      statusEl.textContent = `Success.`;
      return normalizeRows(data);
    } catch (e) {
      statusEl.textContent = 'Error';
      resultDiv.innerHTML = `<div class="muted">SQL error: ${escapeHTML(String(e))}</div>`;
      return { rows: [] };
    }
  }

  function normalizeRows(payload) {
    // Accept shapes: {rows:[...]}, [...], {data:[...]}
    if (Array.isArray(payload)) return { rows: payload };
    if (payload?.rows && Array.isArray(payload.rows)) return { rows: payload.rows };
    if (payload?.data && Array.isArray(payload.data)) return { rows: payload.data };
    return { rows: [] };
  }

  function escapeHTML(s) {
    return s.replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
  }

  // ------------------------------------------------------------------------
  // Rendering
  // ------------------------------------------------------------------------
  function renderTable(data) {
    if (!data || !data.rows || !data.rows.length) {
      resultDiv.innerHTML = '<div class="muted">No rows.</div>';
      return;
    }
    const cols = Object.keys(data.rows[0]);
    const thead = '<thead><tr>' + cols.map(c => `<th>${escapeHTML(String(c))}</th>`).join('') + '</tr></thead>';
    const tbody = '<tbody>' + data.rows.map(r =>
      '<tr>' + cols.map(c => `<td>${escapeHTML(formatCell(r[c]))}</td>`).join('') + '</tr>'
    ).join('') + '</tbody>';
    resultDiv.innerHTML = `<div class="card" style="padding:0"><table>${thead}${tbody}</table></div>`;
  }

  function formatCell(v) {
    if (v === null || v === undefined) return '';
    if (typeof v === 'object') return JSON.stringify(v);
    return String(v);
  }

  // ------------------------------------------------------------------------
  // Metadata helpers
  // ------------------------------------------------------------------------
  function pickDefaultOrderKey(columns) {
    if (!columns) return null;
    if (columns.includes('tickid')) return 'tickid';
    if (columns.includes('id'))     return 'id';
    return null;
  }

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

  // ------------------------------------------------------------------------
  // Load tables
  // ------------------------------------------------------------------------
  async function loadTables() {
    tableSelect.innerHTML = '<option>Loading…</option>';
    const sql = `
      SELECT t.table_name,
             array_agg(c.column_name) FILTER (WHERE c.column_name IS NOT NULL) AS cols
      FROM information_schema.tables t
      LEFT JOIN information_schema.columns c
        ON c.table_schema=t.table_schema AND c.table_name=t.table_name
      WHERE t.table_schema='public'
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
      onTableChange();
    }
  }

  async function onTableChange() {
    structureWrap.style.display = 'none';
    resultDiv.innerHTML = '';
    const table = tableSelect.value;
    structureHint.textContent = table;

    // Show fast estimate first, then exact when ready
    countEl.textContent = '…';
    fetchEstimateCount(table).then(est => { countEl.textContent = `~${est}`; }).catch(()=>{});
    fetchExactCount(table).then(exact => { countEl.textContent = `${exact}`; }).catch(()=>{});

    // Detect default sort key
    let cols = [];
    try { cols = JSON.parse(tableSelect.selectedOptions[0].dataset.columns || '[]'); } catch {}
    const key = pickDefaultOrderKey(cols);
    sortKeyEl.textContent = key || '—';

    setQueryTemplate(table, key);
  }

  // ------------------------------------------------------------------------
  // Describe / preview
  // ------------------------------------------------------------------------
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
      <thead>
        <tr><th>#</th><th>column</th><th>type</th><th>null</th></tr>
      </thead>`;
    const tbody = '<tbody>' + cols.map(r =>
      `<tr><td>${r.ordinal_position}</td><td>${escapeHTML(r.column_name)}</td><td>${escapeHTML(r.data_type)}</td><td>${escapeHTML(r.is_nullable)}</td></tr>`
    ).join('') + '</tbody>';

    structureTable.innerHTML = thead + tbody;
    structureWrap.style.display = 'block';
  }

  // ------------------------------------------------------------------------
  // Query template
  // ------------------------------------------------------------------------
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
    const order = sortKey ? ` ORDER BY ${sortKey}` : '';
    sqlBox.value = `SELECT * FROM ${table}${order} LIMIT 100;`;
  }

  // ------------------------------------------------------------------------
  // Events
  // ------------------------------------------------------------------------
  refreshBtn.addEventListener('click', loadTables);
  tableSelect.addEventListener('change', onTableChange);

  $('#run').addEventListener('click', async () => {
    const data = await runSQL(sqlBox.value);
    renderTable(data);
  });

  btnDescribe.addEventListener('click', onDescribeClick);

  btnPreview.addEventListener('click', () => {
    const table = tableSelect.value;
    const key   = (sortKeyEl.textContent && sortKeyEl.textContent !== '—') ? sortKeyEl.textContent : null;
    setQueryTemplate(table, key);
  });

  templateSelect.addEventListener('change', () => {
    const table = tableSelect.value;
    const key   = (sortKeyEl.textContent && sortKeyEl.textContent !== '—') ? sortKeyEl.textContent : null;
    setQueryTemplate(table, key);
  });

  // Init
  loadTables();
})();
