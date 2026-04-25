(function () {
  const state = {
    publicTables: [],
    activeTable: null,
    running: false,
    exporting: false,
  };

  const elements = {
    connectionMeta: document.getElementById("connectionMeta"),
    tableFilter: document.getElementById("tableFilter"),
    tableList: document.getElementById("tableList"),
    publicTableList: document.getElementById("publicTableList"),
    editor: document.getElementById("sqlEditor"),
    exportFilename: document.getElementById("exportFilename"),
    exportButton: document.getElementById("exportCsvButton"),
    runButton: document.getElementById("runQueryButton"),
    status: document.getElementById("queryStatus"),
    resultsMeta: document.getElementById("resultsMeta"),
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

  function setStatus(message, tone, options) {
    elements.status.classList.remove("error", "success");
    if (tone) {
      elements.status.classList.add(tone);
    }
    elements.status.replaceChildren();
    const text = document.createElement("span");
    text.textContent = message;
    elements.status.appendChild(text);
    if (options && options.linkUrl && options.linkLabel) {
      elements.status.appendChild(document.createTextNode(" "));
      const link = document.createElement("a");
      link.href = options.linkUrl;
      link.textContent = options.linkLabel;
      elements.status.appendChild(link);
    }
  }

  function syncActionControls() {
    const busy = state.running || state.exporting;
    elements.runButton.disabled = busy;
    elements.exportButton.disabled = busy;
    elements.exportFilename.disabled = busy;
  }

  function errorMessage(error) {
    const detail = error && error.detail
      ? (typeof error.detail === "object" && error.error && !error.detail.error
        ? Object.assign({ error: error.error }, error.detail)
        : error.detail)
      : error;
    if (!detail) {
      return "Request failed.";
    }
    if (typeof detail === "string") {
      return detail;
    }
    const parts = [detail.error || detail.message || "SQL failed."];
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
    if (detail.stage) {
      parts.push("stage: " + detail.stage);
    }
    if (detail.path) {
      parts.push(detail.path);
    }
    return parts.join(" | ");
  }

  function showRunError(error) {
    const message = errorMessage(error);
    elements.resultsMeta.textContent = "Query failed.";
    setStatus(message, "error");
  }

  async function fetchJson(url, options) {
    const response = await fetch(url, Object.assign({ credentials: "same-origin" }, options || {}));
    const rawText = await response.text();
    let payload = {};
    if (rawText) {
      try {
        payload = JSON.parse(rawText);
      } catch (_error) {
        payload = response.ok ? {} : {
          ok: false,
          error: rawText || response.statusText || ("HTTP " + response.status),
          detail: rawText || response.statusText || ("HTTP " + response.status),
        };
      }
    }
    if (!response.ok) {
      throw payload;
    }
    return payload;
  }

  function postJson(url, payload) {
    return fetchJson(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "same-origin",
      body: JSON.stringify(payload),
    });
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

  function allTables() {
    return state.publicTables;
  }

  function renderTableButtons(tables) {
    return tables.map((table) => {
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

  function renderTableSection(host, tables, options) {
    const filter = elements.tableFilter.value.trim().toLowerCase();
    const filteredTables = tables.filter((table) => {
      return !filter || (table.schema + "." + table.name).toLowerCase().includes(filter);
    });

    if (!tables.length) {
      host.innerHTML = "<div class=\"sql-empty\">" + escapeHtml(options.emptyMessage) + "</div>";
      return;
    }

    if (!filteredTables.length) {
      host.innerHTML = "<div class=\"sql-empty\">" + escapeHtml(options.noMatchesMessage) + "</div>";
      return;
    }

    host.innerHTML = renderTableButtons(filteredTables);
  }

  function renderTables() {
    renderTableSection(elements.publicTableList, state.publicTables, {
      emptyMessage: "No public tables found.",
      noMatchesMessage: "No matching public tables.",
    });
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
    const payload = await fetchJson("/api/sql/schema");
    state.publicTables = Array.isArray(payload.public) ? payload.public : (payload.tables || []).filter((table) => table.schema === "public");
    renderConnection(payload.context);
    renderTables();
    setStatus("Ready.", null);
  }

  async function runQuery() {
    if (state.running || state.exporting) {
      return;
    }
    const sql = elements.editor.value.trim();
    if (!sql) {
      setStatus("SQL text is required.", "error");
      return;
    }

    state.running = true;
    syncActionControls();
    elements.resultsMeta.textContent = "Running SQL...";
    setStatus("Running SQL...", null);
    try {
      const payload = await postJson("/api/sql/query", { sql: sql });
      renderResults(payload);
      setStatus("SQL completed.", "success");
    } catch (error) {
      showRunError(error);
    } finally {
      state.running = false;
      syncActionControls();
    }
  }

  async function exportCsv() {
    if (state.running || state.exporting) {
      return;
    }
    const query = elements.editor.value.trim();
    if (!query) {
      setStatus("SQL text is required.", "error");
      return;
    }

    const payload = { query: query };
    const filename = elements.exportFilename.value.trim();
    if (filename) {
      payload.filename = filename;
    }

    state.exporting = true;
    syncActionControls();
    setStatus("Export running... This may take a while for large result sets.", null);
    try {
      const response = await postJson("/api/sql/export-csv", payload);
      const parts = [
        "CSV export completed:",
        response.filename || "unnamed.csv",
      ];
      if (typeof response.rows === "number") {
        parts.push("rows: " + response.rows);
      }
      setStatus(parts.join(" "), "success", response.download_url ? {
        linkUrl: response.download_url,
        linkLabel: "Download CSV",
      } : null);
    } catch (error) {
      setStatus(errorMessage(error), "error");
    } finally {
      state.exporting = false;
      syncActionControls();
    }
  }

  function selectTable(schema, name) {
    const table = allTables().find((candidate) => candidate.schema === schema && candidate.name === name);
    if (!table) {
      return;
    }
    state.activeTable = table;
    elements.editor.value = tableQuery(table);
    renderTables();
    runQuery();
  }

  elements.exportButton.addEventListener("click", exportCsv);
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
    setStatus(errorMessage(error), "error");
    elements.publicTableList.innerHTML = "<div class=\"sql-empty\">Could not load public tables.</div>";
  });
}());
