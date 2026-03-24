(function () {
  const snippets = [
    { label: "SELECT * FROM table LIMIT 100", sql: "SELECT *\nFROM schema_name.table_name\nLIMIT 100;" },
    { label: "SELECT COUNT(*) FROM table", sql: "SELECT COUNT(*)\nFROM schema_name.table_name;" },
    { label: "SELECT columns FROM table WHERE ...", sql: "SELECT column_a, column_b\nFROM schema_name.table_name\nWHERE column_a = 'value'\nLIMIT 100;" },
    { label: "SELECT DISTINCT column FROM table", sql: "SELECT DISTINCT column_name\nFROM schema_name.table_name\nORDER BY column_name\nLIMIT 100;" },
    { label: "ORDER BY", sql: "SELECT *\nFROM schema_name.table_name\nORDER BY id DESC\nLIMIT 100;" },
    { label: "GROUP BY", sql: "SELECT symbol, COUNT(*) AS rows\nFROM public.ticks\nGROUP BY symbol\nORDER BY rows DESC;" },
    { label: "INNER JOIN", sql: "SELECT a.id, b.id\nFROM schema_a.table_a AS a\nINNER JOIN schema_b.table_b AS b ON b.id = a.id\nLIMIT 100;" },
    { label: "LEFT JOIN", sql: "SELECT a.id, b.id\nFROM schema_a.table_a AS a\nLEFT JOIN schema_b.table_b AS b ON b.id = a.id\nLIMIT 100;" },
    { label: "Aggregation example", sql: "SELECT date_trunc('minute', timestamp) AS minute_bucket,\n       MIN(mid) AS low_mid,\n       MAX(mid) AS high_mid,\n       AVG(mid) AS avg_mid\nFROM public.ticks\nGROUP BY 1\nORDER BY 1 DESC\nLIMIT 120;" },
  ];

  const editor = CodeMirror.fromTextArea(document.getElementById("sqlEditor"), {
    mode: "text/x-sql",
    theme: "material-darker",
    lineNumbers: true,
    matchBrackets: true,
    indentWithTabs: false,
    tabSize: 2,
  });

  const schemaTree = document.getElementById("schemaTree");
  const snippetSelect = document.getElementById("snippetSelect");
  const runQueryButton = document.getElementById("runQueryButton");
  const queryStatus = document.getElementById("queryStatus");
  const resultsMeta = document.getElementById("resultsMeta");
  const resultsHost = document.getElementById("resultsHost");

  snippets.forEach((snippet) => {
    const option = document.createElement("option");
    option.value = snippet.sql;
    option.textContent = snippet.label;
    snippetSelect.appendChild(option);
  });

  snippetSelect.addEventListener("change", () => {
    if (!snippetSelect.value) {
      return;
    }
    editor.setValue(snippetSelect.value);
    editor.focus();
  });

  function setStatus(message, isError) {
    queryStatus.textContent = message;
    queryStatus.classList.toggle("error", Boolean(isError));
  }

  function starterQuery(schemaName, tableName) {
    return "SELECT *\nFROM " + schemaName + "." + tableName + "\nLIMIT 100;";
  }

  function renderResults(payload) {
    const columns = payload.columns || [];
    const rows = payload.rows || [];

    if (!columns.length) {
      resultsHost.innerHTML = "<div class=\"muted\" style=\"padding:1rem;\">Query returned no columns.</div>";
      return;
    }

    const table = document.createElement("table");
    table.className = "result-table";

    const thead = document.createElement("thead");
    const headRow = document.createElement("tr");
    columns.forEach((column) => {
      const cell = document.createElement("th");
      cell.textContent = column;
      headRow.appendChild(cell);
    });
    thead.appendChild(headRow);

    const tbody = document.createElement("tbody");
    rows.forEach((row) => {
      const tr = document.createElement("tr");
      row.forEach((value) => {
        const cell = document.createElement("td");
        cell.textContent = value === null || value === undefined ? "" : String(value);
        tr.appendChild(cell);
      });
      tbody.appendChild(tr);
    });

    if (!rows.length) {
      const emptyRow = document.createElement("tr");
      const emptyCell = document.createElement("td");
      emptyCell.colSpan = columns.length;
      emptyCell.className = "muted";
      emptyCell.textContent = "Query executed successfully with 0 rows.";
      emptyRow.appendChild(emptyCell);
      tbody.appendChild(emptyRow);
    }

    table.appendChild(thead);
    table.appendChild(tbody);
    resultsHost.innerHTML = "";
    resultsHost.appendChild(table);
  }

  function makeColumnButton(column) {
    const button = document.createElement("div");
    button.className = "column-button";
    button.textContent = column.name + " | " + column.dataType;
    return button;
  }

  function makeTableButton(table) {
    const wrapper = document.createElement("div");
    const button = document.createElement("button");
    const group = document.createElement("div");

    button.type = "button";
    button.className = "table-button";
    button.textContent = table.name + "  (" + table.rowEstimate + ")";

    group.className = "table-group";
    group.hidden = true;

    table.columns.forEach((column) => {
      group.appendChild(makeColumnButton(column));
    });

    button.addEventListener("click", () => {
      editor.setValue(starterQuery(table.schema, table.name));
      group.hidden = !group.hidden;
      editor.focus();
    });

    wrapper.appendChild(button);
    wrapper.appendChild(group);
    return wrapper;
  }

  function renderSchema(schemas) {
    schemaTree.innerHTML = "";
    schemas.forEach((schema) => {
      const wrapper = document.createElement("div");
      const button = document.createElement("button");
      const group = document.createElement("div");

      button.type = "button";
      button.className = "schema-button";
      button.textContent = schema.schema;

      group.className = "tree-group";
      group.hidden = true;
      schema.tables.forEach((table) => {
        group.appendChild(makeTableButton(table));
      });

      button.addEventListener("click", () => {
        group.hidden = !group.hidden;
      });

      wrapper.appendChild(button);
      wrapper.appendChild(group);
      schemaTree.appendChild(wrapper);
    });
  }

  async function loadSchema() {
    try {
      const response = await fetch("/api/sql/schema");
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload.detail || "Schema request failed.");
      }
      renderSchema(payload.schemas || []);
    } catch (error) {
      schemaTree.textContent = error.message || "Failed to load schema.";
      schemaTree.classList.add("error");
    }
  }

  async function runQuery() {
    setStatus("Running query...", false);
    runQueryButton.disabled = true;

    try {
      const response = await fetch("/api/sql/query", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sql: editor.getValue() }),
      });
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload.detail || "Query failed.");
      }

      resultsMeta.textContent = [
        payload.rowCount + " row(s)",
        payload.elapsedMs + " ms",
        payload.truncated ? "truncated at " + payload.maxRows : "complete",
      ].join(" | ");
      renderResults(payload);
      setStatus("Query completed.", false);
    } catch (error) {
      resultsMeta.textContent = "Query failed.";
      resultsHost.innerHTML = "<div class=\"error\" style=\"padding:1rem;\">" + (error.message || "Unknown error.") + "</div>";
      setStatus(error.message || "Query failed.", true);
    } finally {
      runQueryButton.disabled = false;
    }
  }

  runQueryButton.addEventListener("click", runQuery);
  loadSchema();
  setTimeout(runQuery, 150);
}());
