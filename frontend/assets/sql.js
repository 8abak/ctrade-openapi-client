(function () {
  const state = {
    tables: [],
    activeTable: null,
    running: false,
  };

  const elements = {
    connectionMeta: document.getElementById("connectionMeta"),
    tableFilter: document.getElementById("tableFilter"),
    tableList: document.getElementById("tableList"),
    editor: document.getElementById("sqlEditor"),
    runButton: document.getElementById("runQueryButton"),
    status: document.getElementById("queryStatus"),
    resultsMeta: document.getElementById("resultsMeta"),
    resultsError: document.getElementById("resultsError"),
    resultsHost: document.getElementById("resultsHost"),
  };

  function escapeHtml(value) {
    return String(value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function quoteIdentifier(identifier) {
    const value = String(identifier || "");
    if (/^[a-z_][a-z0-9_]*$/.test(value)) {
      return value;
    }
    return "\"" + value.replace(/"/g, "\"\"") + "\"";
  }

  function tableReference(table) {
    return quoteIdentifier(table.schema || "public") + "." + quoteIdentifier(table.name);
  }

  function tableQuery(table) {
    const relation = tableReference(table);
    if (table.hasId) {
      return "select * from " + relation + " order by id desc limit 100";
    }
    return "select * from " + relation + " limit 100";
  }

  function setStatus(message, tone) {
    elements.status.textContent = message;
    elements.status.classList.remove("error", "success");
    if (tone) {
      elements.status.classList.add(tone);
    }
  }

  function clearError() {
    elements.resultsError.hidden = true;
    elements.resultsError.innerHTML = "";
  }

  function errorMessage(error) {
    const detail = error && error.detail ? error.detail : error;
    if (!detail) {
      return "Request failed.";
    }
    if (typeof detail === "string") {
      return detail;
    }
    const parts = [detail.message || "SQL failed."];
    if (detail.line && detail.column) {
      parts.push("line " + detail.line + ", column " + detail.column);
    }
    if (detail.detail) {
      parts.push(detail.detail);
    }
    if (detail.hint) {
      parts.push("hint: " + detail.hint);
    }
    if (detail.sqlstate) {
      parts.push("SQLSTATE " + detail.sqlstate);
    }
    return parts.join(" | ");
  }

  function showError(error) {
    const message = errorMessage(error);
    elements.resultsError.hidden = false;
    elements.resultsError.innerHTML = escapeHtml(message);
    setStatus(message, "error");
  }

  async function fetchJson(url, options) {
    const response = await fetch(url, options);
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw payload;
    }
    return payload;
  }

  function renderConnection(context) {
    if (!context) {
      elements.connectionMeta.textContent = "Public schema";
      return;
    }
    elements.connectionMeta.textContent = [
      context.database || "database",
      context.currentUser || "user",
      context.serverVersion ? "PG " + context.serverVersion : "postgres",
    ].join(" / ");
  }

  function renderTables() {
    const filter = elements.tableFilter.value.trim().toLowerCase();
    const tables = state.tables.filter((table) => {
      return !filter || (table.schema + "." + table.name).toLowerCase().includes(filter);
    });

    if (!tables.length) {
      elements.tableList.innerHTML = "<div class=\"sql-empty\">No matching public tables.</div>";
      return;
    }

    elements.tableList.innerHTML = tables.map((table) => {
      const key = table.schema + "." + table.name;
      const active = state.activeTable && state.activeTable.schema === table.schema && state.activeTable.name === table.name;
      return [
        "<button class=\"sql-table-button", active ? " active" : "", "\" type=\"button\"",
        " data-schema=\"", escapeHtml(table.schema), "\"",
        " data-name=\"", escapeHtml(table.name), "\">",
        "<span>", escapeHtml(key), "</span>",
        "<small>", escapeHtml(table.hasId ? "id desc" : "no id order"), " | est ", escapeHtml(table.rowEstimate || 0), "</small>",
        "</button>",
      ].join("");
    }).join("");
  }

  function renderResultGrid(result) {
    const columns = result.columns || [];
    const rows = result.rows || [];
    if (!columns.length) {
      return [
        "<section class=\"sql-result-block\">",
        "<div class=\"sql-result-title\">", escapeHtml(result.commandTag || "SQL"), "</div>",
        "<div class=\"sql-empty\">Statement completed without a result set. Row count: ",
        escapeHtml(result.rowCount == null ? 0 : result.rowCount),
        "</div></section>",
      ].join("");
    }

    const head = columns.map((column) => "<th>" + escapeHtml(column.name) + "</th>").join("");
    const body = rows.length
      ? rows.map((row) => {
          return "<tr>" + row.map((value) => {
            if (value === null || value === undefined) {
              return "<td><span class=\"null-pill\">NULL</span></td>";
            }
            return "<td>" + escapeHtml(value) + "</td>";
          }).join("") + "</tr>";
        }).join("")
      : "<tr><td class=\"sql-empty-cell\" colspan=\"" + columns.length + "\">Query returned 0 rows.</td></tr>";

    return [
      "<section class=\"sql-result-block\">",
      "<div class=\"sql-result-title\">", escapeHtml(result.commandTag || "SQL"),
      " | ", escapeHtml(result.rowCount == null ? rows.length : result.rowCount), " row(s)",
      result.truncated ? " | truncated at " + escapeHtml(result.maxRows) : "",
      "</div>",
      "<table class=\"sql-results-table\"><thead><tr>", head, "</tr></thead><tbody>", body, "</tbody></table>",
      "</section>",
    ].join("");
  }

  function renderResults(payload) {
    const results = payload.results || [];
    elements.resultsMeta.textContent = payload.elapsedMs + " ms | " + (payload.statementCount || results.length) + " statement(s)";
    if (!results.length) {
      elements.resultsHost.innerHTML = "<div class=\"sql-empty\">No statements were executed.</div>";
      return;
    }
    elements.resultsHost.innerHTML = results.map(renderResultGrid).join("");
  }

  async function loadTables() {
    clearError();
    const payload = await fetchJson("/api/sql/schema");
    state.tables = payload.tables || [];
    renderConnection(payload.context);
    renderTables();
    setStatus("Loaded " + state.tables.length + " public table(s).", "success");
  }

  async function runQuery() {
    if (state.running) {
      return;
    }
    const sql = elements.editor.value.trim();
    if (!sql) {
      setStatus("SQL text is required.", "error");
      return;
    }

    state.running = true;
    elements.runButton.disabled = true;
    clearError();
    setStatus("Running SQL...", null);
    try {
      const payload = await fetchJson("/api/sql/query", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sql: sql }),
      });
      renderResults(payload);
      setStatus("SQL completed.", "success");
    } catch (error) {
      showError(error);
    } finally {
      state.running = false;
      elements.runButton.disabled = false;
    }
  }

  function selectTable(schema, name) {
    const table = state.tables.find((candidate) => candidate.schema === schema && candidate.name === name);
    if (!table) {
      return;
    }
    state.activeTable = table;
    elements.editor.value = tableQuery(table);
    renderTables();
    runQuery();
  }

  elements.runButton.addEventListener("click", runQuery);

  elements.tableFilter.addEventListener("input", renderTables);

  elements.tableList.addEventListener("click", function (event) {
    const button = event.target.closest(".sql-table-button");
    if (!button) {
      return;
    }
    selectTable(button.dataset.schema, button.dataset.name);
  });

  elements.editor.addEventListener("keydown", function (event) {
    if ((event.ctrlKey || event.metaKey) && event.key === "Enter") {
      event.preventDefault();
      runQuery();
    }
  });

  document.addEventListener("keydown", function (event) {
    if ((event.ctrlKey || event.metaKey) && event.key === "Enter") {
      event.preventDefault();
      runQuery();
    }
  });

  loadTables().catch((error) => {
    showError(error);
    elements.tableList.innerHTML = "<div class=\"sql-empty\">Could not load public tables.</div>";
  });
}());
