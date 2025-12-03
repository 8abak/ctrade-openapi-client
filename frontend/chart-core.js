// ================================
// chart-core.js
// Pure chart behavior engine
// ================================

(function () {
    // Global state
    let chart = null;

    // Data returned from backend (/api/review/window)
    let currentData = null;

    // Window id range
    let fromId = null;
    let baseWindowSize = 5000;

    // Modes
    let liveMode = true;   // true = live, false = review (historical)
    let runMode = true;    // true = auto-refresh, false = manual

    let runTimer = null;

    // ------------------------------------------
    // DOM Ready
    // ------------------------------------------
    document.addEventListener("DOMContentLoaded", () => {
        chart = echarts.init(document.getElementById("chart"));

        setupControls();
        initChartOption();

        // Default: LIVE + RUN
        liveMode = true;
        runMode = true;

        // Initial load: we don't know fromId yet
        // Start by grabbing a window from near the end
        baseWindowSize = readChunkSize();
        initialLiveBootstrap();
    });

    // ------------------------------------------
    // Controls
    // ------------------------------------------
    function setupControls() {
        const chkLive = document.getElementById("chkLive");
        const btnRun = document.getElementById("btnRun");
        const btnStop = document.getElementById("btnStop");
        const btnJump = document.getElementById("btnJump");
        const btnLoadMore = document.getElementById("btnLoadMore");

        chkLive.addEventListener("change", () => {
            liveMode = chkLive.checked;
            if (liveMode) {
                // Switch back to live: re-bootstrap from latest region
                initialLiveBootstrap();
                startRunLoop();
            } else {
                // Historical mode: stop auto-adjusting fromId to latest
                stopRunLoop(); // will be restarted if Run is pressed
            }
        });

        btnRun.addEventListener("click", () => {
            runMode = true;
            startRunLoop();
        });

        btnStop.addEventListener("click", () => {
            runMode = false;
            stopRunLoop();
        });

        btnJump.addEventListener("click", () => {
            const jumpId = parseInt(document.getElementById("inputJumpId").value, 10);
            if (!jumpId || jumpId <= 0) return;
            baseWindowSize = readChunkSize();
            liveMode = false;
            document.getElementById("chkLive").checked = false;

            fromId = Math.max(1, jumpId - baseWindowSize + 1);
            fetchAndRender(fromId, baseWindowSize, { keepZoom: false });
        });

        btnLoadMore.addEventListener("click", () => {
            if (fromId === null) return;
            const extra = readChunkSize();
            fromId = Math.max(1, fromId - extra);
            // extend window backwards while preserving zoom
            fetchAndRender(fromId, (currentWindowSize() + extra), { keepZoom: true });
        });

        // Layer toggles cause re-render from existing data
        [
            "chkTicks",
            "chkPivHiLo",
            "chkPivSwings",
            "chkHHLL",
            "chkZones",
        ].forEach(id => {
            const el = document.getElementById(id);
            el.addEventListener("change", () => {
                if (currentData) {
                    renderFromCurrentData({ keepZoom: true });
                }
            });
        });
    }

    // ------------------------------------------
    // Chart base option
    // ------------------------------------------
    function initChartOption() {
        const opt = {
            backgroundColor: "#111",
            animation: false,
            tooltip: {
                trigger: "axis",
                axisPointer: { type: "cross" },
                backgroundColor: "rgba(0,0,0,0.8)",
                borderColor: "#555",
                borderWidth: 1,
                textStyle: { color: "#eee", fontSize: 11 },
                formatter: defaultTooltipFormatter
            },
            xAxis: {
                type: "time",
                axisLine: { lineStyle: { color: "#888" } },
                axisLabel: { color: "#aaa" },
                splitLine: { show: false }
            },
            yAxis: {
                type: "value",
                axisLine: { lineStyle: { color: "#888" } },
                axisLabel: { color: "#aaa" },
                splitLine: {
                    show: true,
                    lineStyle: { color: "rgba(255,255,255,0.08)" }
                },
                scale: true
            },
            dataZoom: [
                {
                    type: "inside",
                    xAxisIndex: 0,
                },
                {
                    type: "slider",
                    xAxisIndex: 0,
                    height: 18,
                    bottom: 8,
                    backgroundColor: "#222",
                    borderColor: "#444",
                    textStyle: { color: "#aaa" }
                }
            ],
            series: []
        };

        chart.setOption(opt, true);
    }

    // ------------------------------------------
    // Initial live bootstrap
    // ------------------------------------------
    function initialLiveBootstrap() {
        stopRunLoop();

        // First get some ticks from beginning (or mid), then adjust fromId to end region
        const tmpFrom = 1;
        const tmpWindow = baseWindowSize;

        fetch(`/api/review/window?from_id=${tmpFrom}&window=${tmpWindow}`)
            .then(r => r.json())
            .then(data => {
                if (!data.ticks || data.ticks.length === 0) {
                    // Empty DB, just keep calm
                    currentData = data;
                    renderFromCurrentData({ keepZoom: false });
                    return;
                }

                const lastTick = data.ticks[data.ticks.length - 1];
                const lastId = lastTick.id || lastTick.tick_id || tmpWindow;
                fromId = Math.max(1, lastId - baseWindowSize + 1);

                // Now load the real live window at the end
                return fetch(`/api/review/window?from_id=${fromId}&window=${baseWindowSize}`)
                    .then(r => r.json());
            })
            .then(data2 => {
                if (!data2) return;
                currentData = data2;
                renderFromCurrentData({ keepZoom: false });
                if (runMode) startRunLoop();
            })
            .catch(err => {
                console.error("initialLiveBootstrap error:", err);
            });
    }

    // ------------------------------------------
    // Run loop
    // ------------------------------------------
    function startRunLoop() {
        stopRunLoop();
        if (!runMode) return;

        // Poll every second
        runTimer = setInterval(() => {
            if (!runMode) return;
            if (liveMode) {
                // live: keep window near trailing edge (end of data)
                liveStep();
            } else {
                // historical "run": walk forward in DB chunks
                reviewStep();
            }
        }, 1000);
    }

    function stopRunLoop() {
        if (runTimer) {
            clearInterval(runTimer);
            runTimer = null;
        }
    }

    // ------------------------------------------
    // Live step: poll from the same fromId/window
    // and let backend give us the latest ticks
    // starting from that id.
    // We keep zoom.
    // ------------------------------------------
    function liveStep() {
        if (fromId === null) return;
        const win = currentWindowSize() || baseWindowSize;
        fetchAndRender(fromId, win, { keepZoom: true });
    }

    // ------------------------------------------
    // Review step: move window forward by chunk
    // ------------------------------------------
    function reviewStep() {
        if (fromId === null) return;
        baseWindowSize = readChunkSize();
        const win = currentWindowSize() || baseWindowSize;
        fromId = fromId + Math.floor(win * 0.5); // half-window step forward
        fetchAndRender(fromId, win, { keepZoom: false });
    }

    // ------------------------------------------
    // Helpers: read UI, window size
    // ------------------------------------------
    function readChunkSize() {
        const v = parseInt(document.getElementById("inputChunk").value, 10);
        return (!v || v <= 0) ? 5000 : v;
    }

    function currentWindowSize() {
        if (!currentData || !currentData.ticks || currentData.ticks.length === 0) {
            return baseWindowSize;
        }
        const ticks = currentData.ticks;
        const firstId = ticks[0].id || ticks[0].tick_id;
        const lastId = ticks[ticks.length - 1].id || ticks[ticks.length - 1].tick_id;
        if (!firstId || !lastId) return baseWindowSize;
        return (lastId - firstId + 1);
    }

    // ------------------------------------------
    // Fetch + Render (single call)
    // ------------------------------------------
    function fetchAndRender(startId, windowSize, { keepZoom }) {
        const url = `/api/review/window?from_id=${startId}&window=${windowSize}`;
        const prevZoom = keepZoom ? chart.getOption().dataZoom : null;

        fetch(url)
            .then(r => r.json())
            .then(data => {
                currentData = data;
                renderFromCurrentData({ keepZoom, prevZoom });
            })
            .catch(err => {
                console.error("fetchAndRender error:", err);
            });
    }

    // ------------------------------------------
    // Render from currentData
    // ------------------------------------------
    function renderFromCurrentData({ keepZoom, prevZoom } = {}) {
        if (!chart || !currentData) return;

        // Update fromId based on returned ticks (leftmost)
        const ticks = currentData.ticks || [];
        if (ticks.length > 0) {
            const firstId = ticks[0].id || ticks[0].tick_id;
            if (firstId) fromId = firstId;
        }

        // Layer settings from checkboxes
        const layerSettings = {
            ticks: document.getElementById("chkTicks").checked,
            pivHiLo: document.getElementById("chkPivHiLo").checked,
            pivSwings: document.getElementById("chkPivSwings").checked,
            hhll: document.getElementById("chkHHLL").checked,
            zones: document.getElementById("chkZones").checked
        };

        // Ask ChartLayers to build series/options
        const rendered = (window.ChartLayers && window.ChartLayers.renderLayers)
            ? window.ChartLayers.renderLayers(currentData, chart, layerSettings)
            : { series: [], yRange: null };

        const baseOption = chart.getOption();

        const newOption = {
            xAxis: baseOption.xAxis,
            yAxis: {
                ...baseOption.yAxis[0],
            },
            series: rendered.series || []
        };

        // Optional: price range adjustment from layers (if provided)
        if (rendered.yRange) {
            const [minPrice, maxPrice] = rendered.yRange;
            if (minPrice != null && maxPrice != null && minPrice < maxPrice) {
                newOption.yAxis.min = minPrice;
                newOption.yAxis.max = maxPrice;
            }
        }

        // Preserve zoom if requested
        if (keepZoom && prevZoom) {
            newOption.dataZoom = prevZoom;
        }

        chart.setOption(newOption, true);
    }

    // ------------------------------------------
    // Default tooltip if layers don't override
    // ------------------------------------------
    function defaultTooltipFormatter(params) {
        if (!params || params.length === 0) return "";
        const p = params[0];
        const ts = p.value[0];
        const price = p.value[1];
        return `${ts}<br/>Price: ${price}`;
    }

    // Expose a small hook if layers want to override tooltip later
    window.ChartCore = {
        getChart: () => chart,
        getCurrentData: () => currentData,
        renderFromCurrentData
    };
})();