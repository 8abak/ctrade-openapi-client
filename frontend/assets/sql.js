(function () {
  const editor = CodeMirror.fromTextArea(document.getElementById("sqlEditor"), {
    mode: "text/x-pgsql",
    theme: "material-darker",
    lineNumbers: true,
    matchBrackets: true,
    autofocus: true,
    indentWithTabs: false,
    tabSize: 2,
    indentUnit: 2,
  });

  const state = {
    previewOffset: 0,
    previewLimit: 100,
    previewOrderBy: "id",
    previewOrderDir: "desc",
    activePreview: false,
    tables: [],
    activeObject: null,
  };

  const elements = {
    schemaTree: document.getElementById("schemaTree"),
    refreshSchemaButton: document.getElementById("refreshSchemaButton"),
    inspectorSection: document.getElementById("inspectorSection"),
    inspectorToggle: document.getElementById("inspectorToggle"),
    inspectorToggleState: document.getElementById("inspectorToggleState"),
    inspectorBody: document.getElementById("inspectorBody"),
    previewButton: document.getElementById("previewButton"),
    previewTopButton: document.getElementById("previewTopButton"),
    connectionMeta: document.getElementById("connectionMeta"),
    objectDetail: document.getElementById("objectDetail"),
    queryStatus: document.getElementById("queryStatus"),
    editorContext: document.getElementById("editorContext"),
    runQueryButton: document.getElementById("runQueryButton"),
    resultsMeta: document.getElementById("resultsMeta"),
    resultsError: document.getElementById("resultsError"),
    previewToolbar: document.getElementById("previewToolbar"),
    previewMeta: document.getElementById("previewMeta"),
    previewPrevButton: document.getElementById("previewPrevButton"),
    previewNextButton: document.getElementById("previewNextButton"),
    resultsHost: document.getElementById("resultsHost"),
  };

  let runInFlight = false;

  function setInspectorCollapsed(collapsed) {
    const isCollapsed = Boolean(collapsed);
    elements.inspectorSection.classList.toggle("is-collapsed", isCollapsed);
    elements.inspectorBody.classList.toggle("is-collapsed", isCollapsed);
    elements.inspectorToggle.setAttribute("aria-expanded", String(!isCollapsed));
    elements.inspectorToggleState.textContent = isCollapsed ? "collapsed" : "open";
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

  function escapeHtml(value) {
    return String(value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  async function fetchJson(url, options) {
    const response = await fetch(url, options);
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw payload.detail || payload || { message: "Request failed." };
    }
    return payload;
  }

  function objectLabel(object) {
    return object ? (object.schema + "." + object.name) : "no-object";
  }

  function activeObject() {
    return state.activeObject;
  }

  function renderConnectionMeta(context) {
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

  function renderSchema(payload) {
    const tables = (((payload || {}).schemas || [])[0] || {}).objects?.tables || [];
    state.tables = tables;
    if (!tables.length) {
      elements.schemaTree.innerHTML = "<div class=\"muted\">No admin tables are available.</div>";
      return;
    }
    elements.schemaTree.innerHTML = tables.map((table) => {
      const active = activeObject() && activeObject().schema === table.schema && activeObject().name === table.name;
      return [
        "<button type=\"button\" class=\"schema-object-button",
        active ? " active" : "",
        "\" data-schema=\"", escapeHtml(table.schema),
        "\" data-name=\"", escapeHtml(table.name),
        "\" data-kind=\"", escapeHtml(table.kind || "table"),
        "\"><span class=\"object-name\">", escapeHtml(table.schema + "." + table.name),
        "</span><span class=\"object-meta\">est ", escapeHtml(String(table.rowEstimate || 0)),
        "</span></button>",
      ].join("");
    }).join("");
  }

  function renderObject(payload) {
    const object = payload.object;
    const columns = (object.columns || []).map((column) => {
      return "<tr><td>" + escapeHtml(column.name) + "</td><td>" + escapeHtml(column.dataType || "") + "</td><td>" + escapeHtml(column.notNull ? "not null" : "-") + "</td></tr>";
    }).join("");
    const indexes = (object.indexes || []).map((index) => {
      return "<div class=\"definition-block\"><div class=\"definition-title\">" + escapeHtml(index.name) + "</div><pre>" + escapeHtml(index.definition || "") + "</pre></div>";
    }).join("");
    state.activeObject = {
      schema: object.schema,
      name: object.name,
      kind: object.kind,
      actions: payload.actions || {},
      preview: payload.preview || {},
    };
    state.previewOrderBy = state.activeObject.preview.orderBy || state.previewOrderBy;
    state.previewOrderDir = state.activeObject.preview.orderDir || state.previewOrderDir;
    elements.objectDetail.innerHTML = [
      "<div class=\"object-heading\"><div class=\"object-kind\">table</div><h2>" + escapeHtml(object.schema + "." + object.name) + "</h2></div>",
      "<div class=\"object-metrics\">",
      "<div class=\"object-metric\"><span>Rows est.</span><strong>" + escapeHtml(String(object.rowEstimate || 0)) + "</strong></div>",
      "<div class=\"object-metric\"><span>Size</span><strong>" + escapeHtml(object.totalSize || "-") + "</strong></div>",
      "</div>",
      "<div class=\"detail-block\"><div class=\"detail-title\">Columns</div><table class=\"inspector-table\"><thead><tr><th>Name</th><th>Type</th><th>Flags</th></tr></thead><tbody>" + columns + "</tbody></table></div>",
      "<div class=\"detail-block\"><div class=\"detail-title\">Indexes</div>" + indexes + "</div>",
    ].join("");
    elements.editorContext.textContent = "Admin SQL console | active " + objectLabel(state.activeObject);
  }

  function renderResultGrid(result) {
    const columns = result.columns || [];
    const rows = result.rows || [];
    if (!columns.length) {
      return "<div class=\"results-empty\">Statement completed without a result set.</div>";
    }

    const head = columns.map((column) => "<th>" + escapeHtml(column.name) + "</th>").join("");
    const body = rows.length
      ? rows.map((row) => "<tr>" + row.map((value) => "<td>" + (value === null ? "<span class=\"null-pill\">NULL</span>" : escapeHtml(String(value))) + "</td>").join("") + "</tr>").join("")
      : "<tr><td colspan=\"" + columns.length + "\" class=\"muted\">Query completed successfully with 0 rows.</td></tr>";

    return "<div class=\"result-grid-wrap\"><table class=\"result-table\"><thead><tr>" + head + "</tr></thead><tbody>" + body + "</tbody></table></div>";
  }

  function renderPreviewResult(result, previewMeta) {
    elements.previewToolbar.hidden = !previewMeta;
    if (previewMeta) {
      elements.previewMeta.textContent = objectLabel(activeObject()) + " | Offset " + previewMeta.offset + " | Limit " + previewMeta.limit + " | Sort " + previewMeta.orderBy + " " + previewMeta.orderDir.toUpperCase();
      elements.previewPrevButton.disabled = previewMeta.offset <= 0;
      elements.previewNextButton.disabled = !result.truncated && (result.rows || []).length < previewMeta.limit;
    }
    elements.resultsHost.innerHTML = renderResultGrid(result);
  }

  function renderQueryResults(results) {
    elements.previewToolbar.hidden = true;
    if (!results.length) {
      elements.resultsHost.innerHTML = "<div class=\"results-empty\">No statements were executed.</div>";
      return;
    }
    elements.resultsHost.innerHTML = results.map((result) => {
      return [
        "<section class=\"detail-block\">",
        "<div class=\"definition-title\">Statement ", escapeHtml(String(result.index || 0)), " · ", escapeHtml(result.commandTag || result.statementType || "SQL"), "</div>",
        "<div class=\"muted\">", escapeHtml(result.statement || ""), "</div>",
        renderResultGrid(result),
        "</section>",
      ].join("");
    }).join("");
  }

  function showError(error) {
    const message = typeof error === "string" ? error : (error.message || "Request failed.");
    elements.resultsError.hidden = false;
    elements.resultsError.innerHTML = "<div class=\"error-title\">" + escapeHtml(message) + "</div>";
  }

  function clearError() {
    elements.resultsError.hidden = true;
    elements.resultsError.innerHTML = "";
  }

  async function loadObject(schema, name, kind, replaceEditor) {
    const payload = await fetchJson("/api/sql/object?" + new URLSearchParams({
      schema: schema,
      name: name,
      kind: kind,
    }).toString());
    renderObject(payload);
    renderSchema({ schemas: [{ objects: { tables: state.tables } }] });
    if (replaceEditor && payload.actions?.insertSelect) {
      editor.setValue(payload.actions.insertSelect);
    }
    return payload;
  }

  async function loadSchemaAndObject(replaceEditor) {
    clearError();
    const schemaPayload = await fetchJson("/api/sql/schema");
    renderConnectionMeta(schemaPayload.context);
    renderSchema(schemaPayload);
    const tables = (((schemaPayload || {}).schemas || [])[0] || {}).objects?.tables || [];
    if (!tables.length) {
      elements.objectDetail.innerHTML = "<div class=\"muted\">No admin tables are available.</div>";
      elements.resultsHost.innerHTML = "<div class=\"results-empty\">No admin tables are available.</div>";
      setStatus("No admin tables were found.", "error");
      return;
    }
    const current = activeObject();
    const selected = tables.find((table) => current && table.schema === current.schema && table.name === current.name) || tables[0];
    await loadObject(selected.schema, selected.name, selected.kind || "table", replaceEditor);
    setStatus("Admin SQL metadata loaded.", "success");
  }

  async function loadPreview() {
    clearError();
    if (!activeObject()) {
      setStatus("Select a table first.", "error");
      return;
    }
    const payload = await fetchJson("/api/sql/table-preview?" + new URLSearchParams({
      schema: activeObject().schema,
      name: activeObject().name,
      limit: String(state.previewLimit),
      offset: String(state.previewOffset),
      orderBy: state.previewOrderBy,
      orderDir: state.previewOrderDir,
    }).toString());
    renderConnectionMeta(payload.context);
    state.activePreview = true;
    elements.resultsMeta.textContent = "Preview " + objectLabel(activeObject()) + " | " + payload.result.elapsedMs + " ms";
    renderPreviewResult(payload.result, payload.result.source);
    setStatus("Preview loaded for " + objectLabel(activeObject()) + ".", "success");
  }

  async function runQuery() {
    if (runInFlight) {
      return;
    }
    runInFlight = true;
    clearError();
    const sql = editor.getValue().trim();
    if (!sql) {
      setStatus("SQL text is required.", "error");
      runInFlight = false;
      return;
    }
    try {
      const payload = await fetchJson("/api/sql/query", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sql: sql }),
      });
      renderConnectionMeta(payload.context);
      state.activePreview = false;
      elements.resultsMeta.textContent = payload.elapsedMs + " ms | " + payload.statementCount + " statement(s)";
      renderQueryResults(payload.results || []);
      setStatus("Admin SQL statement(s) completed.", "success");
    } finally {
      runInFlight = false;
    }
  }

  function triggerRunQuery() {
    runQuery().catch((error) => {
      showError(error);
      setStatus(error.message || "Query failed.", "error");
    });
  }

  elements.refreshSchemaButton.addEventListener("click", function () {
    loadSchemaAndObject(false).catch((error) => {
      showError(error);
      setStatus(error.message || "Schema load failed.", "error");
    });
  });

  elements.inspectorToggle.addEventListener("click", function () {
    setInspectorCollapsed(!elements.inspectorSection.classList.contains("is-collapsed"));
  });

  elements.previewButton.addEventListener("click", function () {
    state.previewOffset = 0;
    loadPreview().catch((error) => {
      showError(error);
      setStatus(error.message || "Preview failed.", "error");
    });
  });

  elements.previewTopButton.addEventListener("click", function () {
    state.previewOffset = 0;
    loadPreview().catch((error) => {
      showError(error);
      setStatus(error.message || "Preview failed.", "error");
    });
  });

  elements.runQueryButton.addEventListener("click", function () {
    triggerRunQuery();
  });

  elements.previewPrevButton.addEventListener("click", function () {
    state.previewOffset = Math.max(0, state.previewOffset - state.previewLimit);
    loadPreview().catch((error) => {
      showError(error);
      setStatus(error.message || "Preview failed.", "error");
    });
  });

  elements.previewNextButton.addEventListener("click", function () {
    state.previewOffset += state.previewLimit;
    loadPreview().catch((error) => {
      showError(error);
      setStatus(error.message || "Preview failed.", "error");
    });
  });

  elements.schemaTree.addEventListener("click", function (event) {
    const button = event.target.closest(".schema-object-button");
    if (!button) {
      return;
    }
    state.previewOffset = 0;
    loadObject(button.dataset.schema, button.dataset.name, button.dataset.kind || "table", true)
      .then(loadPreview)
      .catch((error) => {
        showError(error);
        setStatus(error.message || "Object load failed.", "error");
      });
  });

  editor.addKeyMap({
    "Ctrl-Enter": function () {
      triggerRunQuery();
    },
    "Cmd-Enter": function () {
      triggerRunQuery();
    },
  });

  editor.getWrapperElement().addEventListener("click", function (event) {
    if (!event.ctrlKey && !event.metaKey) {
      return;
    }
    event.preventDefault();
    event.stopPropagation();
    triggerRunQuery();
  });

  editor.on("cursorActivity", function () {
    const value = editor.getValue();
    elements.editorContext.textContent = "Admin SQL console | active " + objectLabel(activeObject()) + " | " + value.length + " chars";
  });

  setInspectorCollapsed(true);

  loadSchemaAndObject(true)
    .then(loadPreview)
    .catch((error) => {
      showError(error);
      setStatus(error.message || "Initial SQL load failed.", "error");
    });
}());
