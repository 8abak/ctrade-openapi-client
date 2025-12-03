// ================================
// chart-layers.js
// All data / layers logic
// ================================

(function () {

    function renderLayers(data, chart, settings) {
        const series = [];
        let allPrices = [];

        // -----------------------------
        // Ticks (main line)
        // -----------------------------
        if (settings.ticks && data.ticks && data.ticks.length > 0) {
            const tickData = data.ticks
                .filter(t => t.ts != null && t.mid != null)
                .map(t => {
                    const mid = typeof t.mid === "number" ? t.mid : parseFloat(t.mid);
                    allPrices.push(mid);
                    return [t.ts, mid];
                });

            series.push({
                name: "Ticks",
                type: "line",
                data: tickData,
                symbol: "none",
                lineStyle: { width: 1, color: "#ffffff" },
                z: 2
            });
        }

        // -----------------------------
        // piv_hilo (raw highs/lows)
        // -----------------------------
        if (settings.pivHiLo && data.piv_hilo && data.piv_hilo.length > 0) {
            const highs = [];
            const lows = [];

            data.piv_hilo.forEach(p => {
                if (!p.ts || p.mid == null) return;
                const mid = num(p.mid);
                allPrices.push(mid);

                if (p.ptype === 1) {
                    highs.push([p.ts, mid]);
                } else if (p.ptype === -1) {
                    lows.push([p.ts, mid]);
                }
            });

            series.push({
                name: "HiLo Highs",
                type: "scatter",
                data: highs,
                symbol: "triangle",
                symbolSize: 7,
                itemStyle: { color: "#ff44ff" },
                z: 3
            });

            series.push({
                name: "HiLo Lows",
                type: "scatter",
                data: lows,
                symbol: "triangle",
                symbolSize: 7,
                symbolRotate: 180,
                itemStyle: { color: "#33eaff" },
                z: 3
            });
        }

        // -----------------------------
        // piv_swings (yellow circles)
        // -----------------------------
        if (settings.pivSwings && data.piv_swings && data.piv_swings.length > 0) {
            const swings = data.piv_swings
                .filter(s => s.ts && s.mid != null)
                .map(s => {
                    const mid = num(s.mid);
                    allPrices.push(mid);
                    return [s.ts, mid];
                });

            series.push({
                name: "Swings",
                type: "scatter",
                data: swings,
                symbol: "circle",
                symbolSize: 9,
                itemStyle: { color: "#ffee55" },
                z: 4
            });
        }

        // -----------------------------
        // HHLL pivots (diamonds)
        // -----------------------------
        if (settings.hhll && data.hhll && data.hhll.length > 0) {
            const colorMap = {
                "HH": "#00ff44",
                "HL": "#33aaff",
                "LH": "#ff9933",
                "LL": "#ff3333"
            };

            const hhllData = data.hhll
                .filter(p => p.ts && p.mid != null)
                .map(p => {
                    const mid = num(p.mid);
                    allPrices.push(mid);
                    return {
                        value: [p.ts, mid],
                        itemStyle: { color: colorMap[p.class_text] || "#ffffff" }
                    };
                });

            series.push({
                name: "HHLL",
                type: "scatter",
                data: hhllData,
                symbol: "diamond",
                symbolSize: 11,
                z: 5
            });
        }

        // -----------------------------
        // Zones (zones_hhll) as markArea
        // -----------------------------
        if (settings.zones && data.zones_hhll && data.zones_hhll.length > 0) {
            const markAreas = [];

            data.zones_hhll.forEach(z => {
                if (!z.start_time || !z.end_time || z.top_price == null || z.bot_price == null) return;
                const top = num(z.top_price);
                const bot = num(z.bot_price);
                if (!(top > bot)) return;

                allPrices.push(top, bot);

                const isActive = (z.state === "active" || z.state === "forming");
                const opacity = isActive ? 0.40 : 0.15;

                // Simple color rule for now:
                const baseColor = (z.break_dir === -1) ? "255,0,0" : "0,255,0";
                const color = `rgba(${baseColor},${opacity})`;

                markAreas.push([
                    { xAxis: z.start_time, yAxis: bot },
                    { xAxis: z.end_time,   yAxis: top, itemStyle: { color } }
                ]);
            });

            series.push({
                name: "Zones",
                type: "line",
                data: [],
                markArea: {
                    silent: true,
                    itemStyle: {
                        borderWidth: 0
                    },
                    data: markAreas
                },
                z: 1
            });
        }

        // -----------------------------
        // Suggested Y range (optional)
        // -----------------------------
        let yRange = null;
        if (allPrices.length > 0) {
            let minP = Math.min.apply(null, allPrices);
            let maxP = Math.max.apply(null, allPrices);
            // Small padding
            const pad = (maxP - minP) * 0.05 || 1.0;
            minP = Math.floor((minP - pad) * 10) / 10;
            maxP = Math.ceil((maxP + pad) * 10) / 10;
            yRange = [minP, maxP];
        }

        return { series, yRange };
    }

    function num(v) {
        if (typeof v === "number") return v;
        return parseFloat(v);
    }

    // Expose to global
    window.ChartLayers = {
        renderLayers
    };

})();