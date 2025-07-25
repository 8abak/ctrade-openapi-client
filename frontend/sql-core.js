// ✅ sql-core.js — Admin SQL + Table Creator for datavis.au

async function runSqlQuery() {
  const table = document.getElementById("sqlTableSelect").value;
  const query = document.getElementById("sqlQueryInput").value;
  const res = await fetch(`/sql/query?table=${table}`, {
    method: "POST",
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query })
  });
  const data = await res.json();
  const out = document.getElementById("sqlResult");
  if (!data || !Array.isArray(data.rows)) {
    out.innerHTML = `<p style="color:red">Query Error</p>`;
    return;
  }
  if (data.rows.length === 0) {
    out.innerHTML = `<p style="color:gray">No results.</p>`;
    return;
  }
  const headers = Object.keys(data.rows[0]);
  let html = '<table><thead><tr>' + headers.map(h => `<th>${h}</th>`).join('') + '</tr></thead><tbody>';
  for (const row of data.rows) {
    html += '<tr>' + headers.map(h => `<td>${row[h]}</td>`).join('') + '</tr>';
  }
  html += '</tbody></table>';
  out.innerHTML = html;
}

async function refreshTableList() {
  const select = document.getElementById("sqlTableSelect");
  const res = await fetch("/sql/tables");
  const tables = await res.json();
  select.innerHTML = "";
  for (const name of tables) {
    const opt = document.createElement("option");
    opt.value = name;
    opt.textContent = name;
    select.appendChild(opt);
  }
}

async function deleteSelectedTable() {
  const table = document.getElementById("sqlTableSelect").value;
  if (!table || !confirm(`Delete table '${table}'?`)) return;
  await fetch(`/sql/delete?table=${table}`, { method: "POST" });
  await refreshTableList();
  document.getElementById("sqlResult").innerHTML = "<p>Table deleted.</p>";
}

async function createStandardTable() {
  const name = document.getElementById("newTableName").value.trim();
  const type = document.querySelector("input[name='tableType']:checked").value;
  if (!name) return alert("Enter table name");

  const res = await fetch("/sql/create", {
    method: "POST",
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name, type })
  });
  const result = await res.text();
  await refreshTableList();
  document.getElementById("sqlResult").innerHTML = `<pre>${result}</pre>`;
}

document.getElementById("sqlRunButton").addEventListener("click", runSqlQuery);
document.getElementById("deleteTableButton").addEventListener("click", deleteSelectedTable);
document.getElementById("createTableButton").addEventListener("click", createStandardTable);

window.addEventListener("DOMContentLoaded", () => {
  refreshTableList();
});
