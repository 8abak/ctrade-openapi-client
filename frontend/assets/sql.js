(function () {
  const STORAGE_KEYS = {
    editor: "datavis.sql.editor",
    history: "datavis.sql.history",
  };

  const state = {
    schemaPayload: null,
    schemaFilter: "",
    activeObject: null,
    objectPayload: null,
    activeResultIndex: -1,
    results: [],
    history: loadHistory(),
    lastContext: null,
  };

  const editor = CodeMirror.fromTextArea(document.getElementById("sqlEditor"), {
    mode: "text/x-pgsql",
    theme: "material-darker",
    lineNumbers: true,
    matchBrackets: true,
    autofocus: true,
    indentWithTabs: false,
    tabSize: 2,
    indentUnit: 2,
    extraKeys: {
      "Ctrl-Enter": function () { runQuery(false); },
      "Cmd-Enter": function () { runQuery(false); },
      "Shift-Ctrl-Enter": function () { runQuery(true); },
      "Shift-Cmd-Enter": function () { runQuery(true); },
      Tab: function (cm) {
        if (cm.somethingSelected()) {
          cm.indentSelection("add");
          return;
        }
        cm.replaceSelection("  ", "end", "+input");
      },
    },
  });

  const elements = {
    schemaTree: document.getElementById("schemaTree"),
    schemaSearch: document.getElementById("schemaSearch"),
    refreshSchemaButton: document.getElementById("refreshSchemaButton"),
    connectionMeta: document.getElementById("connectionMeta"),
    clearEditorButton: document.getElementById("clearEditorButton"),
    runAllButton: document.getElementById("runAllButton"),
    runQueryButton: document.getElementById("runQueryButton"),
    queryStatus: document.getElementById("queryStatus"),
    editorContext: document.getElementById("editorContext"),
    objectActions: document.getElementById("objectActions"),
    objectDetail: document.getElementById("objectDetail"),
    refreshObjectButton: document.getElementById("refreshObjectButton"),
    resultsMeta: document.getElementById("resultsMeta"),
    resultsError: document.getElementById("resultsError"),
    resultTabs: document.getElementById("resultTabs"),
    statementSummary: document.getElementById("statementSummary"),
    resultsHost: document.getElementById("resultsHost"),
    previewToolbar: document.getElementById("previewToolbar"),
    previewMeta: document.getElementById("previewMeta"),
    previewPrevButton: document.getElementById("previewPrevButton"),
    previewNextButton: document.getElementById("previewNextButton"),
    copyTsvButton: document.getElementById("copyTsvButton"),
    copyCsvButton: document.getElementById("copyCsvButton"),
    clearResultsButton: document.getElementById("clearResultsButton"),
  };

  applyStoredEditor();
  updateEditorContext();
  bindEvents();
  loadSchema();

  function bindEvents() {
    editor.on("change", debounce(function () {
      localStorage.setItem(STORAGE_KEYS.editor, editor.getValue());
      updateEditorContext();
    }, 120));
    editor.on("cursorActivity", updateEditorContext);

    elements.schemaSearch.addEventListener("input", function () {
      state.schemaFilter = elements.schemaSearch.value.trim().toLowerCase();
      renderSchema();
    });

    elements.refreshSchemaButton.addEventListener("click", loadSchema);
    elements.clearEditorButton.addEventListener("click", function () {
      editor.setValue("");
      editor.focus();
    });
    elements.runQueryButton.addEventListener("click", function () { runQuery(false); });
    elements.runAllButton.addEventListener("click", function () { runQuery(true); });
    elements.refreshObjectButton.addEventListener("click", function () {
      if (state.activeObject) {
        loadObject(state.activeObject.schema, state.activeObject.name, state.activeObject.kind);
      }
    });
    elements.copyTsvButton.addEventListener("click", function () { copyActiveResult("\t"); });
    elements.copyCsvButton.addEventListener("click", function () { copyActiveResult(","); });
    elements.clearResultsButton.addEventListener("click", clearResults);
    elements.previewPrevButton.addEventListener("click", function () { paginatePreview(-1); });
    elements.previewNextButton.addEventListener("click", function () { paginatePreview(1); });
  }

  function applyStoredEditor() {
    const stored = localStorage.getItem(STORAGE_KEYS.editor);
    if (stored) {
      editor.setValue(stored);
    }
  }

  function loadHistory() {
    try {
      return JSON.parse(localStorage.getItem(STORAGE_KEYS.history) || "[]");
    } catch (error) {
      return [];
    }
  }

  function saveHistory() {
    localStorage.setItem(STORAGE_KEYS.history, JSON.stringify(state.history));
  }

  function addHistory(sql) {
    const value = (sql || "").trim();
    if (!value) {
      return;
    }
    state.history = state.history.filter(function (item) { return item !== value; });
    state.history.unshift(value);
    state.history = state.history.slice(0, 18);
    saveHistory();
  }

  function compactLabel(sql) {
    return sql.replace(/\s+/g, " ").trim().slice(0, 78);
  }

  function debounce(fn, delay) {
    let timeoutId = 0;
    return function () {
      const args = arguments;
      clearTimeout(timeoutId);
      timeoutId = window.setTimeout(function () {
        fn.apply(null, args);
      }, delay);
    };
  }

  function setStatus(message, tone) {
    elements.queryStatus.textContent = message;
    elements.queryStatus.classList.remove("error", "success");
    if (tone === "error") {
      elements.queryStatus.classList.add("error");
    }
    if (tone === "success") {
      elements.queryStatus.classList.add("success");
    }
  }

  function updateEditorContext() {
    const selected = editor.getSelection();
    if (selected && selected.trim()) {
      const lineCount = selected.split("\n").length;
      elements.editorContext.textContent = "Selection mode: " + selected.length + " chars across " + lineCount + " line(s).";
      return;
    }
    const value = editor.getValue();
    elements.editorContext.textContent = "Editor mode: " + value.length + " chars across " + (value ? value.split("\n").length : 0) + " line(s).";
  }

  async function fetchJson(url, options) {
    const response = await fetch(url, options);
    const payload = await response.json().catch(function () { return {}; });
    if (!response.ok) {
      throw normalizeError(payload.detail || payload);
    }
    return payload;
  }

  function normalizeError(payload) {
    if (!payload) {
      return { message: "Request failed." };
    }
    if (typeof payload === "string") {
      return { message: payload };
    }
    if (payload.message) {
      return payload;
    }
    return { message: "Request failed.", raw: payload };
  }

  async function loadSchema() {
    elements.schemaTree.innerHTML = "<div class=\"muted\">Loading public tables...</div>";
    clearError();
    try {
      const payload = await fetchJson("/api/sql/schema");
      state.schemaPayload = payload;
      state.lastContext = payload.context || null;
      renderConnectionMeta();
      renderSchema();
      setStatus("Schema browser refreshed.", "success");
    } catch (error) {
      elements.schemaTree.innerHTML = "<div class=\"error\">" + escapeHtml(error.message || "Schema load failed.") + "</div>";
      elements.connectionMeta.textContent = error.message || "Schema request failed.";
      setStatus(error.message || "Schema load failed.", "error");
    }
  }

  function renderConnectionMeta() {
    const context = state.lastContext;
    if (!context) {
      elements.connectionMeta.textContent = "Protected admin route.";
      return;
    }
    elements.connectionMeta.textContent = [
      context.database || "unknown-db",
      context.currentSchema || "no-schema",
      context.currentUser || "no-user",
      "PG " + (context.serverVersion || "unknown"),
    ].join(" / ");
  }

  function renderSchema() {
    const schema = publicSchema();
    if (!schema) {
      elements.schemaTree.innerHTML = "<div class=\"muted\">The public schema is not available.</div>";
      return;
    }

    const filter = state.schemaFilter;
    elements.schemaTree.innerHTML = "";
    const tables = filterPublicTables(schema, filter);
    if (!tables.length) {
      elements.schemaTree.innerHTML = "<div class=\"muted\">No public tables match the current filter.</div>";
      return;
    }

    const wrapper = document.createElement("div");
    wrapper.className = "schema-section";
    wrapper.innerHTML = "<div class=\"schema-section-header\"><div class=\"schema-name\">public</div><div class=\"schema-counts\">" + tables.length + " table(s)</div></div>";

    const section = document.createElement("div");
    section.className = "schema-group";
    tables.forEach(function (item) {
      section.appendChild(renderObjectButton(item));
    });
    wrapper.appendChild(section);
    elements.schemaTree.appendChild(wrapper);
  }

  function publicSchema() {
    const schemas = (state.schemaPayload && state.schemaPayload.schemas) || [];
    return schemas.find(function (schema) { return schema.schema === "public"; }) || null;
  }

  function filterPublicTables(schema, filter) {
    const tables = ((schema.objects || {}).tables || []).slice();
    if (!filter) {
      return tables;
    }
    return tables.filter(function (item) {
      const haystack = [
        item.name || "",
        item.kind || "",
        (item.columns || []).map(function (column) { return column.name; }).join(" "),
      ].join(" ").toLowerCase();
      return haystack.indexOf(filter) >= 0;
    });
  }

  function renderObjectButton(item) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "schema-object-button";
    if (state.activeObject && state.activeObject.schema === item.schema && state.activeObject.name === item.name && state.activeObject.kind === item.kind) {
      button.classList.add("active");
    }
    button.innerHTML = "<span class=\"object-name\">" + escapeHtml(item.name) + "</span><span class=\"object-meta\">" + escapeHtml(objectMeta(item)) + "</span>";
    button.addEventListener("click", function () {
      loadObject(item.schema, item.name, item.kind);
    });
    return button;
  }

  function objectMeta(item) {
    if (item.kind === "function") {
      return item.signature || "function";
    }
    if (typeof item.rowEstimate === "number") {
      return "est " + item.rowEstimate;
    }
    return item.kind || "object";
  }

  async function loadObject(schemaName, objectName, objectKind) {
    state.activeObject = { schema: schemaName, name: objectName, kind: objectKind };
    elements.refreshObjectButton.disabled = false;
    elements.objectActions.innerHTML = "";
    elements.objectDetail.innerHTML = "<div class=\"muted\">Loading object metadata...</div>";
    renderSchema();

    try {
      const payload = await fetchJson("/api/sql/object?" + new URLSearchParams({
        schema: schemaName,
        name: objectName,
        kind: objectKind,
      }).toString());
      state.objectPayload = payload;
      state.lastContext = payload.context || state.lastContext;
      renderConnectionMeta();
      renderObject();
      setStatus("Loaded metadata for " + schemaName + "." + objectName + ".", "success");
    } catch (error) {
      elements.objectDetail.innerHTML = "<div class=\"error\">" + escapeHtml(error.message || "Object load failed.") + "</div>";
      setStatus(error.message || "Object load failed.", "error");
    }
  }

  function renderObject() {
    const payload = state.objectPayload;
    if (!payload || !payload.object) {
      elements.objectActions.innerHTML = "";
      elements.objectDetail.innerHTML = "<div class=\"muted\">Select an object to inspect it.</div>";
      return;
    }

    const object = payload.object;
    const actions = payload.actions || {};
    elements.objectActions.innerHTML = "";
    if (actions.insertSelect) {
      addObjectAction("Insert SELECT", function () {
        editor.setValue(actions.insertSelect || "");
        editor.focus();
      });
    }
    if (actions.insertExplain) {
      addObjectAction("Insert EXPLAIN", function () {
        editor.setValue(actions.insertExplain || "");
        editor.focus();
      });
    }
    if (object.kind === "table" || object.kind === "view" || object.kind === "materialized_view") {
      addObjectAction("Preview 100", function () {
        openPreview(object.schema, object.name, { limit: 100, offset: 0, orderBy: "", orderDir: "asc" });
      });
    }
    addObjectAction("Copy Name", function () {
      writeClipboard(object.schema + "." + object.name, "Object name copied.");
    });

    const parts = [];
    parts.push("<div class=\"object-heading\"><div class=\"object-kind\">" + escapeHtml((object.kind || "object").replace(/_/g, " ")) + "</div><h2>" + escapeHtml(object.schema + "." + object.name) + "</h2></div>");
    parts.push("<div class=\"object-metrics\">");
    if (typeof object.rowEstimate === "number") {
      parts.push("<div class=\"object-metric\"><span>Rows est.</span><strong>" + escapeHtml(String(object.rowEstimate)) + "</strong></div>");
    }
    if (object.totalSize) {
      parts.push("<div class=\"object-metric\"><span>Size</span><strong>" + escapeHtml(object.totalSize) + "</strong></div>");
    }
    if (object.comment) {
      parts.push("<div class=\"object-metric object-metric-wide\"><span>Comment</span><strong>" + escapeHtml(object.comment) + "</strong></div>");
    }
    parts.push("</div>");
    if (object.overloads && object.overloads.length) {
      parts.push("<div class=\"detail-block\"><div class=\"detail-title\">Functions</div>");
      object.overloads.forEach(function (item) {
        parts.push("<div class=\"function-block\"><div class=\"function-signature\">" + escapeHtml(item.proname + "(" + (item.arguments || "") + ") -> " + (item.returns || "")) + "</div><pre>" + escapeHtml(item.definition || "") + "</pre></div>");
      });
      parts.push("</div>");
    }
    if (object.columns && object.columns.length) {
      parts.push("<div class=\"detail-block\"><div class=\"detail-title\">Columns</div><table class=\"inspector-table\"><thead><tr><th>Name</th><th>Type</th><th>Flags</th></tr></thead><tbody>");
      object.columns.forEach(function (column) {
        const flags = [];
        if (column.notNull) { flags.push("not null"); }
        if (column.isIdentity) { flags.push("identity"); }
        if (column.isGenerated) { flags.push("generated"); }
        if (column.default) { flags.push("default"); }
        parts.push("<tr><td>" + escapeHtml(column.name) + "</td><td>" + escapeHtml(column.dataType || "") + "</td><td>" + escapeHtml(flags.join(", ") || "-") + "</td></tr>");
      });
      parts.push("</tbody></table></div>");
    }
    if (object.indexes && object.indexes.length) {
      parts.push("<div class=\"detail-block\"><div class=\"detail-title\">Indexes</div>");
      object.indexes.forEach(function (index) {
        parts.push("<div class=\"definition-block\"><div class=\"definition-title\">" + escapeHtml(index.name) + "</div><pre>" + escapeHtml(index.definition || "") + "</pre></div>");
      });
      parts.push("</div>");
    }
    if (object.definition) {
      parts.push("<div class=\"detail-block\"><div class=\"detail-title\">Definition</div><pre>" + escapeHtml(object.definition) + "</pre></div>");
    }
    if (object.sequence) {
      parts.push("<div class=\"detail-block\"><div class=\"detail-title\">Sequence State</div><pre>" + escapeHtml(JSON.stringify(object.sequence, null, 2)) + "</pre></div>");
    }
    elements.objectDetail.innerHTML = parts.join("");
  }

  function addObjectAction(label, handler) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "ghost-button compact-button";
    button.textContent = label;
    button.addEventListener("click", handler);
    elements.objectActions.appendChild(button);
  }

  async function openPreview(schemaName, objectName, options) {
    clearError();
    setStatus("Loading preview for " + schemaName + "." + objectName + "...", "");
    try {
      const params = new URLSearchParams({
        schema: schemaName,
        name: objectName,
        limit: String(options.limit || 100),
        offset: String(options.offset || 0),
        orderDir: options.orderDir || "asc",
      });
      if (options.orderBy) {
        params.set("orderBy", options.orderBy);
      }
      const payload = await fetchJson("/api/sql/table-preview?" + params.toString());
      const result = payload.result;
      result.resultKey = "preview:" + schemaName + "." + objectName;
      result.title = schemaName + "." + objectName;
      state.lastContext = payload.context || state.lastContext;
      renderConnectionMeta();

      const existingIndex = state.results.findIndex(function (item) { return item.resultKey === result.resultKey; });
      if (existingIndex >= 0) {
        state.results[existingIndex] = result;
        state.activeResultIndex = existingIndex;
      } else {
        state.results.unshift(result);
        state.activeResultIndex = 0;
      }

      elements.resultsMeta.textContent = "Preview " + schemaName + "." + objectName + " | " + result.elapsedMs + " ms";
      renderStatements([result]);
      renderTabs();
      renderActiveResult();
      setStatus("Preview loaded for " + schemaName + "." + objectName + ".", "success");
    } catch (error) {
      showError(error);
      setStatus(error.message || "Preview failed.", "error");
    }
  }

  async function runQuery(forceAll) {
    const selection = editor.getSelection();
    const sql = (selection && selection.trim() && !forceAll ? selection : editor.getValue()).trim();
    if (!sql) {
      setStatus("SQL text is required.", "error");
      return;
    }

    clearError();
    elements.runQueryButton.disabled = true;
    elements.runAllButton.disabled = true;
    setStatus(forceAll ? "Running full editor..." : (selection && selection.trim() ? "Running current selection..." : "Running current script..."), "");

    try {
      const payload = await fetchJson("/api/sql/query", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sql: sql }),
      });
      addHistory(sql);
      state.lastContext = payload.context || state.lastContext;
      renderConnectionMeta();
      state.results = (payload.results || []).map(function (result) {
        result.resultKey = "statement:" + payload.elapsedMs + ":" + result.index;
        result.title = result.commandTag || ("Statement " + result.index);
        return result;
      });
      state.activeResultIndex = chooseActiveResultIndex(state.results);
      elements.resultsMeta.textContent = [
        payload.statementCount + " statement(s)",
        payload.elapsedMs + " ms",
        payload.transactionMode === "explicit" ? "explicit transaction control" : "single transaction",
      ].join(" | ");
      renderStatements(state.results);
      renderTabs();
      renderActiveResult();
      setStatus("Execution completed.", "success");
    } catch (error) {
      showError(error);
      elements.resultsMeta.textContent = "Execution failed.";
      setStatus(error.message || "Execution failed.", "error");
    } finally {
      elements.runQueryButton.disabled = false;
      elements.runAllButton.disabled = false;
    }
  }

  function chooseActiveResultIndex(results) {
    let lastResultSet = -1;
    results.forEach(function (result, index) {
      if (result.hasResultSet) {
        lastResultSet = index;
      }
    });
    return lastResultSet >= 0 ? lastResultSet : (results.length ? results.length - 1 : -1);
  }

  function renderStatements(results) {
    elements.statementSummary.innerHTML = "";
    if (!results.length) {
      elements.statementSummary.innerHTML = "<div class=\"muted\">No statements returned.</div>";
      return;
    }
    results.forEach(function (result, index) {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "statement-pill";
      if (index === state.activeResultIndex) {
        button.classList.add("active");
      }
      button.innerHTML = "<strong>" + escapeHtml(String(result.index || index + 1)) + "</strong><span>" + escapeHtml(result.commandTag || result.statementType || "statement") + "</span><span>" + escapeHtml(String(result.rowCount || 0)) + " rows</span><span>" + escapeHtml(String(result.elapsedMs || 0)) + " ms</span>";
      button.addEventListener("click", function () {
        state.activeResultIndex = index;
        renderTabs();
        renderActiveResult();
      });
      elements.statementSummary.appendChild(button);
    });
  }

  function renderTabs() {
    elements.resultTabs.innerHTML = "";
    if (!state.results.length) {
      elements.resultTabs.innerHTML = "<div class=\"results-empty\">No active result set.</div>";
      return;
    }
    state.results.forEach(function (result, index) {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "result-tab";
      if (index === state.activeResultIndex) {
        button.classList.add("active");
      }
      button.innerHTML = "<span class=\"result-tab-title\">" + escapeHtml(compactLabel(result.title || ("Statement " + (result.index || index + 1)))) + "</span><span class=\"result-tab-meta\">" + escapeHtml(String(result.rowCount || 0)) + " rows</span>";
      button.addEventListener("click", function () {
        state.activeResultIndex = index;
        renderTabs();
        renderActiveResult();
      });
      elements.resultTabs.appendChild(button);
    });
  }

  function renderActiveResult() {
    const result = state.results[state.activeResultIndex];
    elements.previewToolbar.hidden = true;
    if (!result) {
      elements.resultsHost.innerHTML = "<div class=\"results-empty\">No active result set.</div>";
      return;
    }
    if (result.source && result.source.kind === "preview") {
      elements.previewToolbar.hidden = false;
      elements.previewMeta.textContent = "Offset " + (result.source.offset || 0) + " | Limit " + (result.source.limit || 100) + (result.source.orderBy ? " | Sort " + result.source.orderBy + " " + (result.source.orderDir || "asc").toUpperCase() : "");
      elements.previewPrevButton.disabled = (result.source.offset || 0) <= 0;
      elements.previewNextButton.disabled = !result.truncated && (result.rows || []).length < (result.source.limit || 100);
    }
    if (!result.hasResultSet) {
      elements.resultsHost.innerHTML = "<div class=\"command-result\"><div class=\"command-result-tag\">" + escapeHtml(result.commandTag || result.statementType || "Command") + "</div><div class=\"command-result-meta\">" + escapeHtml(String(result.rowCount || 0)) + " row(s) affected in " + escapeHtml(String(result.elapsedMs || 0)) + " ms</div><pre>" + escapeHtml(result.statement || "") + "</pre></div>";
      return;
    }
    renderGrid(result);
  }

  function renderGrid(result) {
    const table = document.createElement("table");
    table.className = "result-table";
    const thead = document.createElement("thead");
    const headRow = document.createElement("tr");
    (result.columns || []).forEach(function (column) {
      const cell = document.createElement("th");
      if (result.source && result.source.kind === "preview") {
        const button = document.createElement("button");
        button.type = "button";
        button.className = "column-sort-button";
        const currentOrder = result.source.orderBy === column.name ? (result.source.orderDir || "asc") : "";
        button.textContent = column.name + (currentOrder ? " " + (currentOrder === "asc" ? "^" : "v") : "");
        button.addEventListener("click", function () {
          const nextDirection = result.source.orderBy === column.name && result.source.orderDir === "asc" ? "desc" : "asc";
          openPreview(result.source.schema, result.source.name, {
            limit: result.source.limit || 100,
            offset: 0,
            orderBy: column.name,
            orderDir: nextDirection,
          });
        });
        cell.appendChild(button);
      } else {
        cell.textContent = column.name;
      }
      headRow.appendChild(cell);
    });
    thead.appendChild(headRow);
    table.appendChild(thead);

    const tbody = document.createElement("tbody");
    if (!(result.rows || []).length) {
      const emptyRow = document.createElement("tr");
      const emptyCell = document.createElement("td");
      emptyCell.colSpan = (result.columns || []).length;
      emptyCell.className = "muted";
      emptyCell.textContent = "Query completed successfully with 0 rows.";
      emptyRow.appendChild(emptyCell);
      tbody.appendChild(emptyRow);
    } else {
      result.rows.forEach(function (row) {
        const tr = document.createElement("tr");
        row.forEach(function (value) {
          const cell = document.createElement("td");
          if (value === null) {
            cell.innerHTML = "<span class=\"null-pill\">NULL</span>";
          } else {
            cell.textContent = String(value);
          }
          tr.appendChild(cell);
        });
        tbody.appendChild(tr);
      });
    }
    table.appendChild(tbody);

    elements.resultsHost.innerHTML = "";
    const wrapper = document.createElement("div");
    wrapper.className = "result-grid-wrap";
    wrapper.appendChild(table);
    elements.resultsHost.appendChild(wrapper);
    if (result.truncated) {
      const note = document.createElement("div");
      note.className = "result-note";
      note.textContent = "Result truncated at " + result.maxRows + " row(s).";
      elements.resultsHost.appendChild(note);
    }
  }

  function clearResults() {
    state.results = [];
    state.activeResultIndex = -1;
    elements.resultsMeta.textContent = "Results cleared.";
    elements.statementSummary.innerHTML = "";
    elements.resultTabs.innerHTML = "";
    elements.resultsHost.innerHTML = "<div class=\"results-empty\">No active result set.</div>";
    elements.previewToolbar.hidden = true;
    clearError();
  }

  function paginatePreview(direction) {
    const result = state.results[state.activeResultIndex];
    if (!result || !result.source || result.source.kind !== "preview") {
      return;
    }
    const limit = result.source.limit || 100;
    const offset = Math.max(0, (result.source.offset || 0) + (direction * limit));
    openPreview(result.source.schema, result.source.name, {
      limit: limit,
      offset: offset,
      orderBy: result.source.orderBy || "",
      orderDir: result.source.orderDir || "asc",
    });
  }

  function showError(error) {
    const detail = error.detail ? "<div class=\"error-detail-row\"><span>Detail</span><strong>" + escapeHtml(error.detail) + "</strong></div>" : "";
    const hint = error.hint ? "<div class=\"error-detail-row\"><span>Hint</span><strong>" + escapeHtml(error.hint) + "</strong></div>" : "";
    const code = error.sqlstate ? "<div class=\"error-detail-row\"><span>SQLSTATE</span><strong>" + escapeHtml(error.sqlstate) + "</strong></div>" : "";
    const line = error.line ? "<div class=\"error-detail-row\"><span>Position</span><strong>line " + escapeHtml(String(error.line)) + ", col " + escapeHtml(String(error.column || 1)) + "</strong></div>" : "";
    const statement = error.statement ? "<pre>" + escapeHtml(error.statement) + "</pre>" : "";
    elements.resultsError.hidden = false;
    elements.resultsError.innerHTML = "<div class=\"error-title\">" + escapeHtml(error.message || "Execution failed.") + "</div>" + detail + hint + code + line + statement;
  }

  function clearError() {
    elements.resultsError.hidden = true;
    elements.resultsError.innerHTML = "";
  }

  async function copyActiveResult(delimiter) {
    const result = state.results[state.activeResultIndex];
    if (!result || !result.hasResultSet) {
      setStatus("There is no tabular result to copy.", "error");
      return;
    }
    const lines = [];
    lines.push((result.columns || []).map(function (column) { return escapeDelimited(column.name, delimiter); }).join(delimiter));
    (result.rows || []).forEach(function (row) {
      lines.push(row.map(function (value) { return escapeDelimited(value, delimiter); }).join(delimiter));
    });
    await writeClipboard(lines.join("\n"), delimiter === "," ? "CSV copied." : "TSV copied.");
  }

  function escapeDelimited(value, delimiter) {
    const text = value === null || value === undefined ? "NULL" : String(value);
    if (delimiter !== "," && text.indexOf("\n") === -1 && text.indexOf("\t") === -1) {
      return text;
    }
    return "\"" + text.replace(/"/g, "\"\"") + "\"";
  }

  async function writeClipboard(value, successMessage) {
    try {
      await navigator.clipboard.writeText(value);
      setStatus(successMessage, "success");
    } catch (error) {
      setStatus("Clipboard write failed.", "error");
    }
  }

  function escapeHtml(value) {
    return String(value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/\"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }
}());
