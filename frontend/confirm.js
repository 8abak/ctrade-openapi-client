// Path to the new tags CSV, relative to /src/frontend/confirm.html
// /src/frontend/confirm.html  ->  /src/train/tags/tags_XAUUSD_tags_1_600.csv
const TAGS_CSV_PATH = '../train/tags/tags_XAUUSD_tags_1_600.csv';

// Simple CSV parser that handles quotes and commas inside quotes.
// Not intended to be super-optimized; just robust enough to inspect tags.
function parseCSV(text) {
    const rows = [];
    let row = [];
    let cur = '';
    let inQuotes = false;

    for (let i = 0; i < text.length; i++) {
        const ch = text[i];
        const next = i + 1 < text.length ? text[i + 1] : null;

        if (inQuotes) {
            if (ch === '"' && next === '"') {
                // Escaped quote
                cur += '"';
                i++;
            } else if (ch === '"') {
                inQuotes = false;
            } else {
                cur += ch;
            }
        } else {
            if (ch === '"') {
                inQuotes = true;
            } else if (ch === ',') {
                row.push(cur);
                cur = '';
            } else if (ch === '\r') {
                // ignore, handle on '\n'
                continue;
            } else if (ch === '\n') {
                row.push(cur);
                cur = '';
                // avoid pushing trailing empty line
                if (row.length > 1 || row[0] !== '') {
                    rows.push(row);
                }
                row = [];
            } else {
                cur += ch;
            }
        }
    }

    // leftover
    if (cur.length > 0 || row.length > 0) {
        row.push(cur);
        rows.push(row);
    }

    return rows;
}

function setStatus(text, kind) {
    const el = document.getElementById('status');
    el.textContent = text;
    el.className = '';
    if (kind) {
        el.classList.add(kind);
    }
}

function renderTable(rows, limitRows) {
    const table = document.getElementById('csvTable');
    const thead = table.querySelector('thead');
    const tbody = table.querySelector('tbody');

    thead.innerHTML = '';
    tbody.innerHTML = '';

    if (!rows || rows.length === 0) {
        setStatus('CSV is empty or could not be parsed.', 'error');
        return;
    }

    const header = rows[0];
    const dataRows = rows.slice(1);
    const totalRows = dataRows.length;

    const maxRows = limitRows ? Math.min(200, totalRows) : totalRows;

    // Header row
    const trHead = document.createElement('tr');

    // First sticky index column
    const thIndex = document.createElement('th');
    thIndex.textContent = '#';
    thIndex.classList.add('col-index');
    trHead.appendChild(thIndex);

    header.forEach((h, idx) => {
        const th = document.createElement('th');
        th.textContent = h === '' ? `col_${idx}` : h;
        trHead.appendChild(th);
    });

    thead.appendChild(trHead);

    // Data rows
    for (let i = 0; i < maxRows; i++) {
        const r = dataRows[i];
        const tr = document.createElement('tr');

        const tdIndex = document.createElement('td');
        tdIndex.textContent = i + 1;
        tdIndex.classList.add('col-index');
        tr.appendChild(tdIndex);

        for (let j = 0; j < header.length; j++) {
            const td = document.createElement('td');
            td.textContent = r && r[j] !== undefined ? r[j] : '';
            tr.appendChild(td);
        }

        tbody.appendChild(tr);
    }

    const summary = document.getElementById('summary');
    summary.textContent = `Columns: ${header.length}, Rows: ${totalRows} (showing ${maxRows})`;
}

async function loadCSV() {
    const reloadBtn = document.getElementById('reloadBtn');
    const limitRowsCheckbox = document.getElementById('limitRowsCheckbox');

    reloadBtn.disabled = true;
    setStatus(`Loading CSV from ${TAGS_CSV_PATH} …`, null);

    try {
        const res = await fetch(TAGS_CSV_PATH, {
            // no-cache so you can see fresh output after re-running buildTags
            cache: 'no-cache'
        });

        if (!res.ok) {
            throw new Error(`HTTP ${res.status} ${res.statusText}`);
        }

        const text = await res.text();
        const rows = parseCSV(text);

        renderTable(rows, limitRowsCheckbox.checked);
        setStatus('CSV loaded OK.', 'ok');
    } catch (err) {
        console.error('Error loading CSV:', err);
        setStatus(`Failed to load CSV: ${err.message}`, 'error');
    } finally {
        reloadBtn.disabled = false;
    }
}

document.addEventListener('DOMContentLoaded', () => {
    const reloadBtn = document.getElementById('reloadBtn');
    const limitRowsCheckbox = document.getElementById('limitRowsCheckbox');

    reloadBtn.addEventListener('click', () => {
        loadCSV();
    });

    limitRowsCheckbox.addEventListener('change', () => {
        // Re-render with the new limit if we already loaded data
        // Easiest is just to reload; cheap enough for a 600-row file.
        loadCSV();
    });

    // Initial load
    loadCSV();
});
