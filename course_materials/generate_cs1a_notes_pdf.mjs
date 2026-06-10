import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { execFileSync } from "node:child_process";
import { marked } from "../.codex_tools/node_modules/marked/lib/marked.esm.js";
import katex from "../.codex_tools/node_modules/katex/dist/katex.mjs";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const workspace = path.resolve(__dirname, "..");

const inputPath = process.argv[2]
  ? path.resolve(process.argv[2])
  : path.resolve(
      "C:/Users/amol charpe/.codex/attachments/b3db8a59-1957-472e-b826-2e037e25d6da/pasted-text.txt",
    );

const outMd = path.join(__dirname, "CS1A_Actuarial_Statistics_Formatted_Notes.md");
const outHtml = path.join(__dirname, "CS1A_Actuarial_Statistics_Formatted_Notes.html");
const outPdf = path.join(__dirname, "CS1A_Actuarial_Statistics_Formatted_Notes.pdf");
const outFonts = path.join(__dirname, "fonts");

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function formatMarkdown(source) {
  return source
    .replace(/\r\n/g, "\n")
    .replace(/^# CS1A Actuarial Statistics -/m, "# CS1A Actuarial Statistics:")
    .replace(/^## Chapter (\d+) - /gm, "## Chapter $1: ")
    .replace(/\n{3,}/g, "\n\n")
    .trimEnd()
    .concat("\n");
}

const proseHints = /\b(number|amount|time|takes|values|claim|claims|policy|policies|probability of|expected value|average|fixed|continuous|discrete|events|trials|where|then it|given that|low frequency|high severity|uncertainty|randomness|condition|distribution)\b/i;
const mathHints = /(?:=|~|\^|\\|>=|<=|\+|-|\*|\/|P\(|E\[|Var\(|SD\(|SE\(|sqrt|exp|lambda|sigma|alpha|beta|gamma|mu|theta|chi|int|sum|H0|H1|CI)/i;

function looksLikeFormula(line) {
  const trimmed = line.trim();
  if (!trimmed || trimmed.length > 130) return false;
  if (!mathHints.test(trimmed)) return false;
  if (proseHints.test(trimmed) && !/^(E|P|Var|SD|SE|CI|H0|H1|M_|K_|SSE|SST|SSR|X_bar|p_hat|beta_hat|alpha_hat|\w+\s*=)/.test(trimmed)) {
    return false;
  }
  return true;
}

function toLatex(line) {
  let out = line.trim();
  out = out
    .replaceAll("approximately", "\\approx")
    .replaceAll("+/-", "\\pm")
    .replaceAll(">=", "\\ge ")
    .replaceAll("<=", "\\le ")
    .replaceAll("!=", "\\ne ")
    .replaceAll("lambda", "\\lambda")
    .replaceAll("sigma", "\\sigma")
    .replaceAll("mu", "\\mu")
    .replaceAll("alpha", "\\alpha")
    .replaceAll("beta", "\\beta")
    .replaceAll("gamma", "\\gamma")
    .replaceAll("theta", "\\theta")
    .replace(/\bX_bar\b/g, "\\bar X")
    .replace(/\bp_hat\b/g, "\\hat p")
    .replace(/\bbeta_hat\b/g, "\\hat\\beta")
    .replace(/\balpha_hat\b/g, "\\hat\\alpha")
    .replace(/\bgamma_hat_w\b/g, "\\hat\\gamma_w")
    .replace(/\bgamma_hat\b/g, "\\hat\\gamma")
    .replace(/\bVar\(/g, "\\operatorname{Var}(")
    .replace(/\bSD\(/g, "\\operatorname{SD}(")
    .replace(/\bSE\(/g, "\\operatorname{SE}(")
    .replace(/\bNormal\(/g, "\\operatorname{Normal}(")
    .replace(/\bPoisson\(/g, "\\operatorname{Poisson}(")
    .replace(/\bBinomial\(/g, "\\operatorname{Binomial}(")
    .replace(/\bBeta\(/g, "\\operatorname{Beta}(")
    .replace(/\blog\b/g, "\\log")
    .replace(/\bexp\(([^()]*)\)/g, "e^{$1}")
    .replace(/\bsqrt\(([^()]*)\)/g, "\\sqrt{$1}")
    .replace(/integral from ([^ ]+) to ([^ ]+) of (.+) dx/i, "\\int_{$1}^{$2} $3\\,dx")
    .replace(/sum from ([^ ]+) to infinity (.+)/i, "\\sum_{$1}^{\\infty} $2")
    .replace(/sum over ([^ ]+) (.+)/i, "\\sum_{$1} $2")
    .replace(/\bsum\b/g, "\\sum")
    .replace(/\be\^\(([^()]*)\)/g, "e^{$1}")
    .replace(/([A-Za-z0-9)\]])\^\(([^()]*)\)/g, "$1^{$2}")
    .replace(/([A-Za-z0-9)\]])\^([A-Za-z0-9]+)/g, "$1^{$2}")
    .replace(/\bC\(([^,]+),([^)]+)\)/g, "\\binom{$1}{$2}")
    .replace(/ -> /g, "\\to ");
  return out;
}

function renderMaybeMath(line) {
  if (!looksLikeFormula(line)) return `<div class="code-line">${escapeHtml(line)}</div>`;
  const latex = toLatex(line);
  try {
    return `<div class="math-line">${katex.renderToString(latex, {
      displayMode: true,
      throwOnError: true,
      strict: "ignore",
    })}</div>`;
  } catch {
    return `<div class="code-line formula-text">${escapeHtml(line)}</div>`;
  }
}

const renderer = new marked.Renderer();

renderer.code = ({ text, lang }) => {
  const normalized = String(text).replace(/\n+$/g, "");
  const lines = normalized.split("\n");
  if (lang === "text" || !lang) {
    const formulaCount = lines.filter(looksLikeFormula).length;
    const className = formulaCount > 0 ? "formula-card" : "note-card";
    return `<div class="${className}">${lines.map(renderMaybeMath).join("")}</div>`;
  }
  return `<pre><code>${escapeHtml(normalized)}</code></pre>`;
};

function renderMathDelimiters(html) {
  return html
    .replace(/\\\[([\s\S]*?)\\\]/g, (_, expr) => {
      try {
        return katex.renderToString(expr, {
          displayMode: true,
          throwOnError: false,
          strict: "ignore",
        });
      } catch {
        return `<pre class="formula-card">${escapeHtml(expr)}</pre>`;
      }
    })
    .replace(/\\\(([\s\S]*?)\\\)/g, (_, expr) =>
      katex.renderToString(expr, {
        displayMode: false,
        throwOnError: false,
        strict: "ignore",
      }),
    );
}

function buildHtml(markdown) {
  marked.setOptions({ gfm: true, breaks: false, renderer });
  const body = renderMathDelimiters(marked.parse(markdown));
  const katexCss = fs.readFileSync(
    path.join(workspace, ".codex_tools/node_modules/katex/dist/katex.min.css"),
    "utf8",
  );

  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>CS1A Actuarial Statistics Notes</title>
<style>${katexCss}</style>
<style>
  @page { size: A4; margin: 18mm 16mm 20mm; }
  * { box-sizing: border-box; }
  body {
    margin: 0;
    color: #15202b;
    background: #ffffff;
    font-family: "Segoe UI", Arial, sans-serif;
    font-size: 11.2pt;
    line-height: 1.58;
  }
  main { max-width: 820px; margin: 0 auto; }
  h1 {
    margin: 0 0 18px;
    padding: 22px 0 16px;
    color: #0f3a4a;
    border-bottom: 3px solid #1f8a9e;
    font-size: 25pt;
    line-height: 1.16;
  }
  h2 {
    break-before: page;
    margin: 0 0 14px;
    padding-top: 6px;
    color: #0f3a4a;
    font-size: 18pt;
    line-height: 1.24;
    border-bottom: 1px solid #c8d8dd;
  }
  h2:first-of-type { break-before: auto; }
  h3 {
    margin: 18px 0 8px;
    color: #24556a;
    font-size: 13.5pt;
    line-height: 1.28;
  }
  p { margin: 7px 0 10px; }
  ul, ol { margin: 7px 0 12px 23px; padding: 0; }
  li { margin: 3px 0; }
  table {
    width: 100%;
    border-collapse: collapse;
    margin: 12px 0 16px;
    font-size: 10.5pt;
  }
  th, td {
    border: 1px solid #bfd2d8;
    padding: 6px 8px;
    text-align: left;
  }
  th { background: #e8f3f5; color: #0f3a4a; }
  .note-card, .formula-card {
    margin: 9px 0 13px;
    padding: 9px 12px;
    border: 1px solid #d4e1e5;
    border-left: 4px solid #1f8a9e;
    border-radius: 6px;
    background: #f7fbfc;
    break-inside: avoid;
  }
  .formula-card { background: #fbfefd; }
  .code-line {
    min-height: 1.45em;
    font-family: Consolas, "Courier New", monospace;
    white-space: pre-wrap;
    color: #20333b;
  }
  .formula-text {
    color: #0f3a4a;
    font-weight: 600;
  }
  .math-line {
    margin: 2px 0;
    overflow-wrap: anywhere;
  }
  .katex-display {
    margin: 0.25em 0;
    overflow: visible;
  }
  code {
    color: #0d5363;
    background: #eef7f8;
    border-radius: 4px;
    padding: 0.08em 0.28em;
    font-family: Consolas, "Courier New", monospace;
    font-size: 0.92em;
  }
  strong { color: #102f3a; }
  blockquote {
    margin: 10px 0;
    padding: 6px 12px;
    border-left: 4px solid #b4cbd2;
    background: #f8fbfc;
  }
</style>
</head>
<body>
<main>
${body}
</main>
</body>
</html>`;
}

function findChrome() {
  const candidates = [
    "C:/Program Files/Google/Chrome/Application/chrome.exe",
    "C:/Program Files (x86)/Google/Chrome/Application/chrome.exe",
    "C:/Program Files/Microsoft/Edge/Application/msedge.exe",
    "C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe",
  ];
  const found = candidates.find((candidate) => fs.existsSync(candidate));
  if (!found) throw new Error("Chrome or Edge was not found.");
  return found;
}

const raw = fs.readFileSync(inputPath, "utf8");
const formatted = formatMarkdown(raw);
const html = buildHtml(formatted);

fs.cpSync(path.join(workspace, ".codex_tools/node_modules/katex/dist/fonts"), outFonts, {
  recursive: true,
});
fs.writeFileSync(outMd, formatted, "utf8");
fs.writeFileSync(outHtml, html, "utf8");

const chrome = findChrome();
execFileSync(chrome, [
  "--headless=new",
  "--disable-gpu",
  "--no-sandbox",
  `--print-to-pdf=${outPdf}`,
  `file:///${outHtml.replaceAll("\\", "/")}`,
], { stdio: "inherit" });

console.log(outPdf);
