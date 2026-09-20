const assert = require("node:assert/strict");
const fs = require("node:fs");
const {SmartBandTracker, sourceVersion} = require("./frontend/assets/smart-band.js");

assert.equal(sourceVersion, "smart-band-causal-v1-2026-09-19");
const html = fs.readFileSync("./frontend/live.html", "utf8");
assert.ok(html.indexOf("/assets/smart-band.js") < html.indexOf("/assets/live.js"));
assert.match(html, /id="showSmartBand"[^>]*checked/);
const base = Date.UTC(2026, 8, 21);
const rows = Array.from({length: 200}, (_, index) => {
  const mid = 100 + .005 * index + .13 * Math.sin(index / 7);
  return {id: index + 1, timestampMs: base + 200 * index,
    bid: mid - .04, ask: mid + .04, mid};
});
const tracker = new SmartBandTracker();
const points = rows.map((row) => tracker.process(row));
const expected = [
  [0, 100, 100.12, 100.24, "WARMUP", "INSIDE"],
  [39, 100.02995419559304, 100.14995419559304, 100.26995419559303, "RANGING", "INSIDE"],
  [79, 100.11055909246093, 100.27692388892115, 100.38113854869748, "TRANSITION", "INSIDE"],
  [119, 100.21846729061231, 100.44278419714672, 100.5487177637523, "RISING", "INSIDE"],
  [199, 100.47730162592941, 100.83696590283539, 100.9534744717954, "RISING", "ACCEPTED_UP"],
];
for (const [index, center, innerUpper, outerUpper, phase, boundaryState] of expected) {
  assert.ok(Math.abs(points[index].center - center) < 1e-10);
  assert.ok(Math.abs(points[index].innerUpper - innerUpper) < 1e-10);
  assert.ok(Math.abs(points[index].outerUpper - outerUpper) < 1e-10);
  assert.equal(points[index].phase, phase);
  assert.equal(points[index].boundaryState, boundaryState);
}
const incremental = new SmartBandTracker();
rows.slice(0, 100).forEach((row) => incremental.process(row));
rows.slice(100).forEach((row) => incremental.process(row));
assert.equal(incremental.points.at(-1).center, points.at(-1).center);
assert.equal(incremental.process(rows.at(-1)), null);
const afterGap = incremental.process({id: 201, timestampMs: base + 20 * 60000,
  bid: 101.9, ask: 102.1, mid: 102});
assert.equal(afterGap.segment, 1);
assert.equal(afterGap.phase, "WARMUP");
assert.equal(afterGap.center, 102);
console.log("Live Smart Band matches local causal reference and resets after a market gap.");
