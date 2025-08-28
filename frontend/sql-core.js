(() => {
  const sel = document.getElementById('tableSelect');
  const txt = document.getElementById('sqlInput');
  const res = document.getElementById('result');
  const btnRun = document.getElementById('runBtn');
  const btnLatest = document.getElementById('latestBtn');

  const fetchJSON = (u, opt) => fetch(u, opt).then(r => {
    if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json();
  });

  function tableToHTML(rows) {
    if (!rows || !rows.length) return '<em>No rows.</em>';
    const cols = Object.keys(rows[0]);
    let html = '<table><thead><tr>' + cols.map(c => `<th>${c}</th>`).join('') + '</tr></thead><tbody>';
    for (const r of rows) {
      html += '<tr>' + cols.map(c => `<td>${r[c] !== null ? r[c] : ''}</td>`).join('') + '</tr>';
    }
    html += '</tbody></table>';
    return html;
  }

  function setDefaultQuery(name) {
    txt.value = `SELECT * FROM ${name} ORDER BY id DESC LIMIT 100;`;
  }

  async function loadTables() {
    const tables = await fetchJSON('/sqlvw/tables');
    sel.innerHTML = '';
    // prefer our new short tables first
    const preferred = ['segm', 'smal', 'pred', 'outcome', 'stat', 'ticks'];
    const sorted = [...preferred.filter(t => tables.includes(t)), ...tables.filter(t => !preferred.includes(t))];
    for (const t of sorted) {
      const opt = document.createElement('option');
      opt.value = t; opt.textContent = t;
      sel.appendChild(opt);
    }
    setDefaultQuery(sel.value);
  }

  async function run() {
    try {
      const rows = await fetchJSON('/sqlvw/query?query=' + encodeURIComponent(txt.value));
      res.innerHTML = tableToHTML(rows);
    } catch (e) {
      res.innerHTML = `<pre style="color:#b91c1c">${String(e)}</pre>`;
    }
  }

  sel.addEventListener('change', () => setDefaultQuery(sel.value));
  btnRun.addEventListener('click', run);
  btnLatest.addEventListener('click', () => setDefaultQuery(sel.value));

  loadTables().then(run).catch(err => res.innerHTML = `<pre>${String(err)}</pre>`);
})();
