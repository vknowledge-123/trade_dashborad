const fs = require("fs");
const path = require("path");

const outDir = __dirname;
const outPath = path.join(outDir, "SL_Hunting_Mastery_Program_Detailed.pdf");

const program = [
  {
    title: "SL Hunting Mastery Program",
    subtitle: "Decode Smart Money, Trade Liquidity, Stop Getting Trapped",
    type: "cover",
  },
  {
    title: "Course Overview",
    body: [
      "Positioning: This is not a beginner stock market course. It is designed for active traders who already understand basic charts, candles, orders, and risk, but still struggle with consistency, repeated stop-loss hits, fake breakouts, and emotional execution.",
      "Fees: Rs. 15,000 for the full 30-session program.",
      "Session length: 60 minutes each.",
      "Duration: 7 to 10 weeks, with 3 to 5 sessions per week.",
      "Delivery style: Concept explanation, live chart marking, case studies, dashboard walkthroughs, heuristic-engine scoring, homework, review, and final rulebook creation.",
      "Important note: This is an education and skill-building program. It does not promise profit, fixed returns, or guaranteed trade outcomes.",
    ],
  },
  {
    title: "Who This Program Is For",
    bullets: [
      "Traders who already trade intraday or options but lose consistency.",
      "Traders who face frequent stop-loss hits near obvious support and resistance.",
      "Traders who want to understand operator behavior, liquidity grabs, and trap formation.",
      "Traders who want a structured trading framework instead of random entries.",
      "Traders who want to use a trader dashboard and heuristic engine for disciplined decision-making.",
    ],
  },
  {
    title: "Core Objective",
    body: [
      "By the end of this program, students should be able to understand how market movement is driven by liquidity, identify likely liquidity zones before price reaches them, detect stop-loss hunting behavior, execute trades using a defined entry-SL-target framework, avoid common retail traps, and build a personal intraday rulebook.",
      "The program also introduces a heuristic trading engine: a rule-based scoring approach that combines liquidity, structure, volume, OI, option positioning, and risk filters. The goal is not to blindly automate trading. The goal is to make decision quality measurable, repeatable, and reviewable.",
    ],
  },
  {
    title: "Program Structure",
    bullets: [
      "Module 1: Market Reality and Liquidity Foundation - Sessions 1 to 5.",
      "Module 2: Types of Liquidity and Chart Mapping - Sessions 6 to 10.",
      "Module 3: SL Hunting Patterns and Trap Recognition - Sessions 11 to 15.",
      "Module 4: Entry, Stop-Loss, Target, and Risk System - Sessions 16 to 20.",
      "Module 5: Advanced Edge with OI, Volume, Greeks, Dashboard, and Heuristic Engine - Sessions 21 to 25.",
      "Module 6: Live Market Execution, Review, Psychology, and Personal Rulebook - Sessions 26 to 30.",
    ],
  },
  {
    title: "Module 1 - Market Reality and Liquidity Foundation",
    sessions: [
      ["1", "Why Most Traders Lose", "Why indicators lag, why obvious levels fail, how retail traders become predictable, and why discipline alone is not enough without context."],
      ["2", "Market Moves Because of Liquidity", "What liquidity means in practical trading, where resting orders exist, why price travels toward order clusters, and how liquidity differs from simple support/resistance."],
      ["3", "Stop-Loss as Market Fuel", "How stop-loss orders convert into market orders, why obvious stops are targeted, and how liquidity expansion creates fast moves."],
      ["4", "Smart Money and Operator Behavior", "How large participants need liquidity to enter or exit, why moves often begin after trapping one side, and what operator zones look like on chart."],
      ["5", "Foundation Chart Lab", "Marking visible liquidity, weak highs/lows, recent swing points, and common retail trap areas on live or replay charts."],
    ],
  },
  {
    title: "Module 2 - Types of Liquidity",
    sessions: [
      ["6", "Day High and Day Low Liquidity", "How intraday highs/lows attract breakout traders and stop-loss clusters, and how to plan around those zones."],
      ["7", "Equal Highs and Equal Lows", "Why equal levels are rarely accidental, how they form liquidity pools, and how sweeps around them create reversal opportunities."],
      ["8", "Previous Day High, Low, and Close", "Using prior session levels as decision points, including gap-up, gap-down, and range-bound market behavior."],
      ["9", "Range Liquidity and Internal Liquidity", "Understanding the top, bottom, and middle of a range; identifying when price is collecting internal liquidity before external liquidity."],
      ["10", "Psychological and Option Levels", "Round numbers, strike prices, weekly expiry zones, and how visible levels influence retail option positioning."],
    ],
  },
  {
    title: "Module 3 - SL Hunting Patterns",
    sessions: [
      ["11", "Liquidity Sweep", "Fake breakout anatomy, wick behavior, close back inside range, and the difference between a true breakout and a sweep."],
      ["12", "Reversal After Sweep", "Confirmation through candle close, structure shift, rejection volume, failed follow-through, and retest behavior."],
      ["13", "Single Sweep vs Multiple Sweep", "How repeated stop hunting builds stronger confirmation or warns that the level is still being engineered."],
      ["14", "Trend Day vs Trap Day", "How to avoid shorting every breakout or buying every breakdown; identifying trend acceptance versus trap rejection."],
      ["15", "When Sweeps Fail", "Failure conditions, news/event risk, strong directional OI, high-volume acceptance, and when to stand aside."],
    ],
  },
  {
    title: "Module 4 - Entry, SL, Target, and Risk System",
    sessions: [
      ["16", "Entry Confirmation Logic", "Candle confirmation, break of minor structure, pullback entry, retest entry, and avoiding entries inside noise."],
      ["17", "Stop-Loss Placement", "Placing SL beyond the hunted liquidity, not at the obvious retail level; using invalidation instead of fear-based exits."],
      ["18", "Target Selection", "Selecting next liquidity zone, partial booking areas, range midpoints, previous highs/lows, and option strike magnets."],
      ["19", "Risk-Reward and Position Sizing", "Minimum reward-to-risk, fixed risk per trade, daily loss limit, position sizing, and avoiding revenge trades."],
      ["20", "Complete Trade Framework", "Building the trade template: bias, liquidity zone, trigger, entry, SL, target, reason to avoid, and post-trade review."],
    ],
  },
  {
    title: "Module 5 - Advanced Edge, Dashboard, and Heuristic Engine",
    sessions: [
      ["21", "Open Interest Reality", "Using OI to understand participant positioning, call writing, put writing, unwinding, short covering, and trap areas."],
      ["22", "Volume Confirmation", "Using volume expansion, dry-up, absorption, and rejection volume to confirm or reject a liquidity sweep."],
      ["23", "Option Greeks Basics", "Delta, Gamma, Theta, and Vega explained only as needed for intraday options; why Gamma zones can accelerate moves."],
      ["24", "Trader Dashboard Workflow", "Dashboard panels: market bias, liquidity map, OI heatmap, volume confirmation, active alerts, risk limits, watchlist, and trade journal."],
      ["25", "Heuristic Engine Scoring", "Rule-based scoring for setup quality: liquidity proximity, sweep quality, structure shift, OI support, volume confirmation, trend filter, volatility filter, and risk-reward score."],
    ],
  },
  {
    title: "Heuristic Engine Concept",
    body: [
      "The heuristic engine is a decision-support layer. It converts chart observations into a structured score so the trader can compare setups consistently. A sample scoring model can use 100 points: liquidity zone quality 15, sweep and rejection 20, structure shift 15, OI alignment 15, volume confirmation 10, trend/day-type filter 10, risk-reward 10, and psychology/rule compliance 5.",
      "Example decision bands: 80 to 100 means high-quality setup if risk is acceptable; 60 to 79 means valid but needs stronger confirmation or smaller size; below 60 means avoid or observe only.",
      "The engine should never replace trader judgment. It should prevent impulsive entries, highlight missing confirmation, and create a clean review trail after the market closes.",
    ],
  },
  {
    title: "Trader Dashboard Concept",
    bullets: [
      "Liquidity Map: day high/low, previous day high/low, equal highs/lows, range extremes, and round-number levels.",
      "Market Bias Panel: trend day, trap day, range day, volatility status, and key invalidation level.",
      "OI and Strike Panel: call/put buildup, unwinding, short covering, long buildup, max pain, and important strikes.",
      "Volume Panel: breakout volume, rejection volume, absorption, and low-volume pullback areas.",
      "Signal Panel: heuristic score, setup type, entry trigger, SL, target, and warning flags.",
      "Risk Panel: trades taken, daily loss used, max trades allowed, open risk, and no-trade status.",
      "Journal Panel: screenshot, reason for trade, rule followed or broken, emotional state, result, and learning.",
    ],
  },
  {
    title: "Module 6 - Live Market and Rulebook",
    sessions: [
      ["26", "Pre-Market Planning", "Preparing liquidity zones, expected day type, important strikes, event risk, and no-trade conditions before entry."],
      ["27", "Live Market Analysis", "Reading price near planned zones, waiting for sweep or acceptance, and avoiding early emotional trades."],
      ["28", "High-Probability Trade Selection", "Choosing fewer but cleaner trades using the heuristic score, dashboard warnings, and risk-reward filter."],
      ["29", "Psychology and Discipline", "Handling missed trades, SL hits, overtrading, FOMO, revenge trading, and rule-breaking patterns."],
      ["30", "Personal Rulebook Creation", "Final framework: instruments, timeframes, setup checklist, dashboard process, entry rules, SL rules, target rules, risk limits, and review process."],
    ],
  },
  {
    title: "Student Deliverables",
    bullets: [
      "Liquidity marking checklist.",
      "SL hunting pattern recognition sheet.",
      "Entry-SL-target framework template.",
      "Heuristic engine scoring sheet.",
      "Trader dashboard usage checklist.",
      "Daily pre-market planning format.",
      "Post-trade journal format.",
      "Personal intraday trading rulebook.",
    ],
  },
  {
    title: "Expected Outcome",
    body: [
      "Students should leave the program with a practical understanding of why price often moves against obvious retail levels, how to anticipate liquidity grabs, how to wait for confirmation, and how to trade from a structured framework.",
      "The final goal is not more indicators. The final goal is cleaner decision-making: plan liquidity, wait for trap confirmation, score the setup, manage risk, execute only when the rules align, and review every trade honestly.",
    ],
  },
];

const page = { w: 595.28, h: 841.89, margin: 52 };
const fonts = {
  regular: "F1",
  bold: "F2",
};

function esc(text) {
  return String(text)
    .replace(/[\\()]/g, "\\$&")
    .replace(/[^\x09\x0A\x0D\x20-\x7E]/g, "");
}

function wrap(text, maxChars) {
  const words = String(text).split(/\s+/);
  const lines = [];
  let line = "";
  for (const word of words) {
    if (!line) {
      line = word;
    } else if ((line + " " + word).length <= maxChars) {
      line += " " + word;
    } else {
      lines.push(line);
      line = word;
    }
  }
  if (line) lines.push(line);
  return lines;
}

class Pdf {
  constructor() {
    this.pages = [];
    this.current = [];
    this.y = page.h - page.margin;
    this.pageNo = 0;
  }

  newPage() {
    if (this.current.length) this.pages.push(this.current.join("\n"));
    this.current = [];
    this.y = page.h - page.margin;
    this.pageNo += 1;
    this.footer();
  }

  footer() {
    this.current.push("q 0.88 0.88 0.88 rg 0 0 595.28 34 re f Q");
    this.text(`SL Hunting Mastery Program | Page ${this.pageNo}`, page.margin, 18, 8, fonts.regular, [0.28, 0.28, 0.28]);
  }

  ensure(height) {
    if (this.y - height < page.margin) this.newPage();
  }

  text(txt, x, y, size = 11, font = fonts.regular, color = [0, 0, 0]) {
    const [r, g, b] = color;
    this.current.push(`BT ${r} ${g} ${b} rg /${font} ${size} Tf ${x.toFixed(2)} ${y.toFixed(2)} Td (${esc(txt)}) Tj ET`);
  }

  paragraph(txt, opts = {}) {
    const size = opts.size || 10.5;
    const leading = opts.leading || 15;
    const maxChars = opts.maxChars || 86;
    const x = opts.x || page.margin;
    const color = opts.color || [0.1, 0.1, 0.1];
    const lines = wrap(txt, maxChars);
    this.ensure(lines.length * leading + 8);
    for (const line of lines) {
      this.y -= leading;
      this.text(line, x, this.y, size, opts.font || fonts.regular, color);
    }
    this.y -= opts.after || 6;
  }

  heading(txt) {
    this.ensure(42);
    this.y -= 20;
    this.text(txt, page.margin, this.y, 18, fonts.bold, [0.04, 0.18, 0.28]);
    this.y -= 10;
    this.current.push(`q 0.05 0.48 0.66 rg ${page.margin} ${this.y.toFixed(2)} 490 2 re f Q`);
    this.y -= 10;
  }

  bullet(txt) {
    const lines = wrap(txt, 82);
    this.ensure(lines.length * 14 + 8);
    this.y -= 14;
    this.text("-", page.margin + 6, this.y, 10.5, fonts.bold, [0.04, 0.18, 0.28]);
    this.text(lines[0], page.margin + 22, this.y, 10.5, fonts.regular, [0.08, 0.08, 0.08]);
    for (let i = 1; i < lines.length; i++) {
      this.y -= 14;
      this.text(lines[i], page.margin + 22, this.y, 10.5, fonts.regular, [0.08, 0.08, 0.08]);
    }
    this.y -= 4;
  }

  session(no, title, desc) {
    this.ensure(64);
    this.y -= 15;
    this.text(`Session ${no}: ${title}`, page.margin, this.y, 12, fonts.bold, [0.04, 0.18, 0.28]);
    this.y -= 4;
    this.paragraph(desc, { x: page.margin + 18, maxChars: 80, size: 10, leading: 13, after: 3 });
  }

  cover(title, subtitle) {
    this.current.push("q 0.04 0.18 0.28 rg 0 0 595.28 841.89 re f Q");
    this.current.push("q 0.06 0.55 0.68 rg 0 0 595.28 126 re f Q");
    this.text("COURSE BROCHURE", page.margin, 690, 12, fonts.bold, [0.7, 0.9, 0.95]);
    wrap(title, 26).forEach((line, idx) => {
      this.text(line, page.margin, 625 - idx * 42, 34, fonts.bold, [1, 1, 1]);
    });
    this.text(subtitle, page.margin, 505, 17, fonts.regular, [0.9, 0.98, 1]);
    this.text("30 Sessions | 60 Minutes Each | 7-10 Weeks", page.margin, 440, 15, fonts.bold, [1, 1, 1]);
    this.text("Course Fees: Rs. 15,000", page.margin, 410, 15, fonts.bold, [1, 1, 1]);
    this.text("Includes Liquidity Concepts, SL Hunting, OI, Volume, Greeks,", page.margin, 340, 12, fonts.regular, [0.9, 0.98, 1]);
    this.text("Trader Dashboard Workflow, and Heuristic Engine Scoring", page.margin, 320, 12, fonts.regular, [0.9, 0.98, 1]);
    this.text("Educational program for active traders. No profit guarantee.", page.margin, 80, 10, fonts.regular, [0.75, 0.92, 0.96]);
  }

  addSection(section) {
    if (section.type === "cover") {
      this.newPage();
      this.cover(section.title, section.subtitle);
      return;
    }
    this.newPage();
    this.heading(section.title);
    if (section.body) section.body.forEach((p) => this.paragraph(p));
    if (section.bullets) section.bullets.forEach((b) => this.bullet(b));
    if (section.sessions) section.sessions.forEach(([no, title, desc]) => this.session(no, title, desc));
  }

  render() {
    this.pages.push(this.current.join("\n"));
    const objects = [];
    const add = (s) => {
      objects.push(s);
      return objects.length;
    };

    const fontRegular = add("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>");
    const fontBold = add("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold >>");
    const pageRefs = [];
    const contentRefs = [];
    for (const stream of this.pages) {
      const content = `<< /Length ${Buffer.byteLength(stream, "binary")} >>\nstream\n${stream}\nendstream`;
      contentRefs.push(add(content));
      pageRefs.push(null);
    }
    const pagesRef = objects.length + this.pages.length + 1;
    for (let i = 0; i < this.pages.length; i++) {
      pageRefs[i] = add(`<< /Type /Page /Parent ${pagesRef} 0 R /MediaBox [0 0 ${page.w} ${page.h}] /Resources << /Font << /F1 ${fontRegular} 0 R /F2 ${fontBold} 0 R >> >> /Contents ${contentRefs[i]} 0 R >>`);
    }
    const kids = pageRefs.map((ref) => `${ref} 0 R`).join(" ");
    const pagesObj = add(`<< /Type /Pages /Kids [${kids}] /Count ${pageRefs.length} >>`);
    const catalog = add(`<< /Type /Catalog /Pages ${pagesObj} 0 R >>`);

    let pdf = "%PDF-1.4\n";
    const xref = [0];
    objects.forEach((obj, i) => {
      xref.push(Buffer.byteLength(pdf, "binary"));
      pdf += `${i + 1} 0 obj\n${obj}\nendobj\n`;
    });
    const start = Buffer.byteLength(pdf, "binary");
    pdf += `xref\n0 ${objects.length + 1}\n0000000000 65535 f \n`;
    for (let i = 1; i < xref.length; i++) {
      pdf += `${String(xref[i]).padStart(10, "0")} 00000 n \n`;
    }
    pdf += `trailer\n<< /Size ${objects.length + 1} /Root ${catalog} 0 R >>\nstartxref\n${start}\n%%EOF`;
    return Buffer.from(pdf, "binary");
  }
}

const pdf = new Pdf();
program.forEach((section) => pdf.addSection(section));
fs.writeFileSync(outPath, pdf.render());
console.log(outPath);
