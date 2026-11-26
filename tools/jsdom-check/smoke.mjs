import { JSDOM, VirtualConsole } from "jsdom";
import { readFileSync } from "node:fs";

const html = readFileSync("/workspace/app/static/index.html", "utf8");
const errors = [];
const vc = new VirtualConsole();
vc.on("jsdomError", (e) => errors.push("jsdomError: " + (e.detail?.stack || e.message)));
vc.on("error", (...a) => errors.push("console.error: " + a.join(" ")));

const dom = new JSDOM(html, {
  url: "http://127.0.0.1:8000/",
  runScripts: "dangerously",
  virtualConsole: vc,
  beforeParse(window) {
    window.fetch = (input, init) => {
      const u = new URL(typeof input === "string" ? input : input.url, "http://127.0.0.1:8000").href;
      return fetch(u, init);
    };
  },
});
const { window } = dom;
const document = window.document;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function assert(cond, msg) {
  if (!cond) { console.error("FAIL:", msg); process.exitCode = 1; }
  else console.log("PASS:", msg);
}

await sleep(3000); // let 700ms state polls + 2500ms agent poll render

assert(!errors.length, "page produces no JS errors" + (errors.length ? "\n" + errors.join("\n") : ""));
assert(document.getElementById("accepting-pill").textContent.includes("接单中"),
  "acceptance pill rendered: " + document.getElementById("accepting-pill").textContent);
assert(document.querySelectorAll("#rounds .round").length >= 4,
  "round cards rendered from live API: " + document.querySelectorAll("#rounds .round").length);
assert(document.querySelectorAll("#agents .agent").length === 2, "two agent cards rendered");
assert(document.querySelectorAll("#artifacts-body tr").length >= 1, "artifact rows rendered");

// Publish interaction through the real handler (short interval).
document.getElementById("art-name").value = "jsdom 冒烟制品";
const iv = document.getElementById("write-interval");
iv.value = "0.2";
iv.dispatchEvent(new window.Event("input", { bubbles: true }));
document.getElementById("btn-publish").click();
await sleep(900);
const bodyText = document.getElementById("artifacts-body").textContent;
assert(bodyText.includes("jsdom 冒烟制品"), "clicked publish -> artifact appears");
assert(bodyText.includes("两卷写齐"), "artifact reaches 两卷写齐");

// Backup interaction.
document.getElementById("btn-backup").click();
await sleep(500);
assert(document.getElementById("btn-backup").disabled === true, "backup button disables while round active");
// Wait for the round to finish (index agent may have ~1.2s latency configured).
let ok = false;
for (let i = 0; i < 40; i++) {
  await sleep(250);
  const txt = document.getElementById("rounds").textContent;
  if (txt.includes("成功 · 整组可下载") || txt.includes("失败 · 本轮不供下载")) { ok = true; break; }
}
assert(ok, "new backup round reaches a terminal rendered state");
assert(!errors.length, "still no JS errors after interactions" + (errors.length ? "\n" + errors.join("\n") : ""));

// Focus preservation while editing agent latency.
const delayInput = document.getElementById("agent-delay-index");
delayInput.focus();
delayInput.value = "9.9";
await sleep(2800); // cross a 2.5s agent re-poll
assert(document.activeElement && document.activeElement.id === "agent-delay-index",
  "agent delay input keeps focus across polls");
assert(document.getElementById("agent-delay-index").value === "9.9",
  "typed value not wiped by background re-render");

window.close();
setTimeout(() => process.exit(process.exitCode || 0), 200);
