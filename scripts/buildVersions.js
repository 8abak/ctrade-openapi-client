const { execSync } = require('child_process');
const fs = require('fs');

const pages = [
  {
    key: "tick",
    js: "frontend/tick-core.js",
    html: "frontend/index.html",
    py: "backend/main.py"
  },
  {
    key: "htick",
    js: "frontend/htick-core.js",
    html: "frontend/htick.html",
    py: "backend/main.py"
  },
  {
    key: "ztick",
    js: "frontend/ztick-core.js",
    html: "frontend/zTick.html",
    py: "backend/main.py"
  }
];


function getLastChange(file){
    try {
        const log = execSync(`git log -1 --format="%cd|%s" --date=format:%m/%d-%H:%M -- "${file}"`)
        .toString()
        .trim()
    const [datetime, message] = log.split('|');
    return {datetime, message};
    } catch {
        return null;
    }
}


const output = {};

pages.forEach(({key, js, py, html}) => {
    const result = {};

    const j=getLastChange(js);
    const b=getLastChange(py);
    const h=getLastChange(html);
    if (j) result.js = j;
    if (b) result.py = b;
    if (h) result.html = h;

    output[key] = result;
});

fs.mkdirSync("static", { recursive: true });
fs.writeFileSync("static/version.json", JSON.stringify(output, null, 2));
console.log("✔ version.json updated:");
console.table(output);