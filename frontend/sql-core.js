// sql-core.js — dark UI + left controls + resizable columns + Abort + Force LIMIT
// Count logic fixed: estimate only for real tables; always replace with exact COUNT(*)
(() => {
  const $ = (s, el=document) => el.querySelector(s);

  // UI refs
  const resultWrap     = $('#resultWrap');
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
  const limitInput     = $('#limitInput');
  const forceLimitChk  = $('#forceLimit');
  const abortBtn       = $('#abort');

  // Backend routes from backend/main.py
  const API = { tables: '/sqlvw/tables', query: '/sqlvw/query' };

  let currentController = null;

  // ---------- helpers ----------
  const escapeHTML = s => String(s).replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;', "'":'&#39;'}[m]));
  const normalize   = payload =>
    Array.isArray(payload) ? {rows:payload}
    : (payload?.rows ? {rows:payload.rows}
    : (payload?.data ? {rows:payload.data} : {rows:[]}));

  function pickDefaultOrderKey(columns){
    if (!columns) return null;
    if (columns.includes('tickid')) return 'tickid';
    if (columns.includes('id'))     return 'id';
    return null;
  }

  // Add LIMIT guard for plain SELECT
  function buildSafeSQL(raw){
    const limit = Math.min(Math.max(parseInt(limitInput.value || '100', 10), 1), 10000);
    const text  = (raw || '').trim();
    const isSelect = /^select\b/i.test(text);
    const hasLimit = /\blimit\b/i.test(text);
    if (isSelect && forceLimitChk.checked && !hasLimit) {
      const noSemi = text.replace(/;\s*$/, '');
      return `SELECT * FROM (${noSemi}) AS sub LIMIT ${limit}`;
    }
    return text;
  }

  async function runSQL(sql){
    // cancel any in-flight fetch first
    if (currentController) { try { currentController.abort(); } catch {} }
    currentController = new AbortController();

    statusEl.textContent = 'Running…';
    try {
      const url = `${API.query}?query=${encodeURIComponent(sql)}`;
      const res = await fetch(url, { method: 'GET', signal: currentController.signal });
      const payload = await res.json();
      if (!res.ok || payload?.error) throw new Error(payload?.error || res.statusText);
      statusEl.textContent = 'Success.';
      return normalize(payload);
    } catch (e) {
      if (e.name === 'AbortError') {
        statusEl.textContent = 'Aborted.';
      } else {
        statusEl.textContent = 'Error';
        resultDiv.innerHTML = `<div class="card" style="padding:10px">SQL error: ${escapeHTML(e.message || e)}</div>`;
      }
      return { rows: [] };
    }
  }

  // ---------- rendering: data table with resizable columns ----------
  function renderTable(data){
    if (!data?.rows?.length) {
      resultDiv.innerHTML = '<div class="muted" style="padding:10px">No rows.</div>';
      return;
    }
    const rows = data.rows;
    const cols = Object.keys(rows[0]);

    const defaultWidth = Math.max(120, Math.floor(resultWrap.clientWidth / Math.max(cols.length, 1)));
    const colgroup = cols.map(() => `<col style="width:${defaultWidth}px">`).join('');

    const thead = '<thead><tr>' + cols.map((c,i) =>
      `<th data-col="${i}">
         <div class="th-inner">
           <span>${escapeHTML(c)}</span>
           <span class="col-resizer" data-col="${i}" title="Drag to resize"></span>
         </div>
       </th>`
    ).join('') + '</tr></thead>';

    const tbody = '<tbody>' + rows.map(r =>
      `<tr>${cols.map(c => `<td>${escapeHTML(r[c] ?? '')}</td>`).join('')}</tr>`
    ).join('') + '</tbody>';

    resultDiv.innerHTML = `<table>${colgroup}${thead}${tbody}</table>`;
    enableColumnResizing(resultDiv.querySelector('table'));
  }

  function enableColumnResizing(table){
    const resizers = table.querySelectorAll('.col-resizer');
    const cols = Array.from(table.querySelectorAll('col'));
    let startX = 0, startWidth = 0, targetColIndex = -1;

    function onMove(e){
      if (targetColIndex < 0) return;
      const dx = e.clientX - startX;
      const newW = Math.max(60, startWidth + dx);
      cols[targetColIndex].style.width = `${newW}px`;
    }
    function onUp(){
      targetColIndex = -1;
      window.removeEventListener('mousemove', onMove);
      window.removeEventListener('mouseup', onUp);
    }

    resizers.forEach(h => {
      h.addEventListener('mousedown', e => {
        targetColIndex = parseInt(e.currentTarget.dataset.col, 10);
        startX = e.clientX;
        startWidth = parseInt(cols[targetColIndex].style.width || '120', 10);
        window.addEventListener('mousemove', onMove);
        window.addEventListener('mouseup', onUp);
        e.preventDefault();
      });
    });
  }

  // ---------- metadata / structure ----------
  async function fetchColumns(table){
    const { rows } = await runSQL(
      `SELECT column_name
         FROM information_schema.columns
        WHERE table_schema='public' AND table_name='${table}'
        ORDER BY ordinal_position`
    );
    return rows.map(r => r.column_name);
  }

  // NEW: get relkind + reltuples; only use estimate for real/mat/foreign/partitioned tables
  async function fetchRelMeta(table){
    const { rows } = await runSQL(
      `SELECT c.relkind, COALESCE(c.reltuples,0)::bigint AS reltuples
         FROM pg_class c
         JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='public' AND c.relname='${table}'
        LIMIT 1`
    );
    return rows?.[0] || null;
  }

  async function fetchExactCount(table){
    const { rows } = await runSQL(`SELECT COUNT(*)::bigint AS n FROM ${table}`);
    return rows?.[0]?.n ?? 0;
  }

  async function refreshCountSmart(table){
    countEl.textContent = '…';
    try {
      const meta = await fetchRelMeta(table);
      if (meta && ['r','m','f','p'].includes(meta.relkind) && Number(meta.reltuples) > 0) {
        countEl.textContent = `~${meta.reltuples}`; // quick estimate for real tables
      } else {
        countEl.textContent = '…'; // for views/unknown, wait for exact
      }
    } catch { /* ignore estimate errors */ }

    // Always compute exact in background and replace when ready
    try {
      const exact = await fetchExactCount(table);
      countEl.textContent = `${exact}`;
    } catch {
      // leave estimate (or …) if exact fails
    }
  }

  async function describeTable(table){
    const { rows } = await runSQL(
      `SELECT ordinal_position, column_name, data_type, is_nullable
         FROM information_schema.columns
        WHERE table_schema='public' AND table_name='${table}'
        ORDER BY ordinal_position`
    );
    return rows || [];
  }

  // ---------- table list & selection ----------
  async function loadTables(){
    tableSelect.innerHTML = '<option>Loading…</option>';
    const res = await fetch(API.tables);
    const names = await res.json(); // array
    tableSelect.innerHTML = '';
    names.forEach(name => {
      const opt = document.createElement('option');
      opt.value = name; opt.textContent = name;
      tableSelect.appendChild(opt);
    });
    if (names.length) { tableSelect.selectedIndex = 0; onTableChange(); }
  }

  async function onTableChange(){
    structureWrap.style.display = 'none';
    resultDiv.innerHTML = '<div class="muted" style="padding:10px">Ready.</div>';
    const table = tableSelect.value;

    // Count (estimate for real tables, then exact)
    refreshCountSmart(table);

    const cols = await fetchColumns(table);
    const key  = pickDefaultOrderKey(cols);
    sortKeyEl.textContent = key || '—';
    setQueryTemplate(table, key);
  }

  function setQueryTemplate(table, sortKey){
    const mode  = templateSelect.value;
    const limit = Math.min(Math.max(parseInt(limitInput.value || '100', 10), 1), 10000);
    if (mode === 'count') { sqlBox.value = `SELECT COUNT(*) FROM ${table};`; return; }
    if (mode === 'all')   { sqlBox.value = `SELECT * FROM ${table};`; return; }
    const order = sortKey ? ` ORDER BY ${sortKey}` : '';
    sqlBox.value = `SELECT * FROM ${table}${order} LIMIT ${limit};`;
  }

  async function doDescribe(){
    const table = tableSelect.value;
    const rows  = await describeTable(table);
    if (!rows.length){ structureWrap.style.display = 'none'; return; }
    const thead = '<thead><tr><th>#</th><th>column</th><th>type</th><th>null</th></tr></thead>';
    const tbody = '<tbody>' + rows.map(r =>
      `<tr><td>${r.ordinal_position}</td><td>${escapeHTML(r.column_name)}</td><td>${escapeHTML(r.data_type)}</td><td>${escapeHTML(r.is_nullable)}</td></tr>`
    ).join('') + '</tbody>';
    structureTable.innerHTML = `<table style="width:100%;border-collapse:collapse">${thead}${tbody}</table>`;
    structureWrap.style.display = 'block';
  }

  // ---------- events ----------
  refreshBtn.addEventListener('click', loadTables);
  tableSelect.addEventListener('change', onTableChange);

  $('#run').addEventListener('click', async () => {
    const safe = buildSafeSQL(sqlBox.value);
    const data = await runSQL(safe);
    renderTable(data);
  });

  btnDescribe.addEventListener('click', doDescribe);
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

  limitInput.addEventListener('change', () => {
    const table = tableSelect.value;
    const key   = sortKeyEl.textContent !== '—' ? sortKeyEl.textContent : null;
    setQueryTemplate(table, key);
  });

  abortBtn.addEventListener('click', () => {
    if (currentController) { try { currentController.abort(); } catch {} }
  });

  // ---------- init ----------
  loadTables();
})();
