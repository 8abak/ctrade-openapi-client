// ✅ FINAL sql-core.js with working run + table creation + version + fixed result rendering

let tableSelect, sqlEditor, resultBox, tableInput, tickRadio, areaRadio;

function runSql() {
  const query = sqlEditor.value;
  resultBox.innerHTML = "Loading...";
  fetch(`/sqlvw/query?query=${encodeURIComponent(query)}`)
    .then(res => res.json())
    .then(data => {
      if (Array.isArray(data)) {
        if (!data.length) return resultBox.innerHTML = "<p>No results.</p>";
        const headers = Object.keys(data[0]);
        const rows = data.map(r =>
          `<tr>${headers.map(h => `<td>${r[h]}</td>`).join("")}</tr>`
        ).join("");
        resultBox.innerHTML = `
          <table>
            <thead><tr>${headers.map(h => `<th>${h}</th>`).join("")}</tr></thead>
            <tbody>${rows}</tbody>
          </table>`;
        resultBox.scrollIntoView({ behavior: 'smooth', block: 'start' });
      } else {
        resultBox.innerHTML = `<p>${data.message || "Success."}</p>`;
      }
    })
    .catch(err => {
      resultBox.innerHTML = `<pre style="color:red">${err}</pre>`;
    });
}

function createLabelTable() {
  const table = tableInput.value.trim();
  const mode = tickRadio.checked ? "tick" : "area";
  if (!table) return alert("Please enter table name");
  fetch("/sqlvw/create", {
    method: "POST",
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ table, mode })
  })
    .then(res => res.json())
    .then(d => {
      alert(d.message || JSON.stringify(d));
      loadTables();
    })
    .catch(e => alert("Error creating table"));
}

function loadTables() {
  fetch("/sqlvw/tables")
    .then(res => res.json())
    .then(tables => {
      tableSelect.innerHTML = "";
      tables.forEach(t => {
        const opt = document.createElement("option");
        opt.value = t;
        opt.textContent = t;
        tableSelect.appendChild(opt);
      });
    });
}

function loadVersion() {
  fetch("/version")
    .then(res => res.json())
    .then(v => {
      const val = v["sql"] || {};
      const html = `J: ${val.js?.datetime || '-'} ${val.js?.message || ''}<br>` +
                   `B: ${val.py?.datetime || '-'} ${val.py?.message || ''}<br>` +
                   `H: ${val.html?.datetime || '-'} ${val.html?.message || ''}`;
      document.getElementById("version").innerHTML = html;
    })
    .catch(() => {
      document.getElementById("version").innerText = "Version: unknown";
    });
}

window.addEventListener("DOMContentLoaded", () => {
  tableSelect = document.getElementById("tableSelect");
  sqlEditor = document.getElementById("sqlEditor");
  resultBox = document.getElementById("resultBox");
  tableInput = document.getElementById("labelTableName");
  tickRadio = document.getElementById("tickBasedRadio");
  areaRadio = document.getElementById("areaBasedRadio");

  document.getElementById("runButton").addEventListener("click", runSql);
  document.getElementById("createButton").addEventListener("click", createLabelTable);
  tableSelect.addEventListener("change", () => {
    sqlEditor.value = `SELECT * FROM ${tableSelect.value} ORDER BY tickid DESC LIMIT 100`;
  });

  loadTables();
  loadVersion();
});
