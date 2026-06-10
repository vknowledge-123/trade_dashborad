import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { execFileSync } from "node:child_process";
import { marked } from "../.codex_tools/node_modules/marked/lib/marked.esm.js";
import katex from "../.codex_tools/node_modules/katex/dist/katex.mjs";
import yazl from "../.codex_tools/node_modules/yazl/index.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const workspace = path.resolve(__dirname, "..");

const sourcePath = path.join(__dirname, "CS1A_Concept_Theory_Derivation_Case_Study_Notes.md");
const outMd = path.join(__dirname, "CS1A_Integrated_Full_Study_Notes.md");
const outHtml = path.join(__dirname, "CS1A_Integrated_Full_Study_Notes.html");
const outPdf = path.join(__dirname, "CS1A_Integrated_Full_Study_Notes.pdf");
const outEpub = path.join(__dirname, "CS1A_Integrated_Full_Study_Notes.epub");
const author = "Amu chaps";
const title = "CS1A Actuarial Statistics: Integrated Full Study Notes";

const groups = [
  {
    name: "Probability Foundations and Random Variables",
    test: /Random Variables|Conditional Probability|Conditional Expectation|Independence|Joint Density|Joint PMF|Covariance|Linear Combinations|Law of Total|Grouped Observations|Censored/i,
    frame: "Treat every probability question as a model of uncertainty. Define the sample space, identify the random variable, write the probability law, then decide whether new information changes the population being averaged.",
  },
  {
    name: "Core Distributions and Distribution Theory",
    test: /Binomial|Geometric|Poisson Models|Normal|Lognormal|Gamma|Exponential|Erlang|Weibull|Pareto|Uniform|Chi-Square|F Distribution|Student t|Truncated Poisson|Shifted Geometric|Heavy-Tail|Conditional Excess|MGF|CGF|Probability Generating/i,
    frame: "A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.",
  },
  {
    name: "Aggregate Claims, Poisson Processes, and Risk Capital",
    test: /Compound|Aggregate|Poisson Process|Arrival|Thinning|Waiting Times|Claim Testing|Capital Requirement|Benefit Payouts|Sum of Exponential|Poisson Aggregation/i,
    frame: "Aggregate loss work separates frequency from severity, then recombines them. Pricing needs the expected loss, while solvency and risk loading need spread, skewness, and tail behaviour.",
  },
  {
    name: "Data Analysis, Summaries, Correlation, and Dimension Reduction",
    test: /Descriptive|Data Analysis|Data Types|Correlation|PCA|Principal Components|Factor Analysis|Rebased|Kendall|Pearson|Rank|Coefficient of Variation|Big Data/i,
    frame: "Data analysis starts before modelling. Check the variable type, missingness, outliers, scale, dependence, and whether the summary statistic actually answers the business question.",
  },
  {
    name: "Estimation, Likelihood, Bias, and Simulation",
    test: /Method of Moments|MLE|Likelihood|Bias|MSE|CRLB|Estimator|Estimators|Boundary MLE|Implicit MLE|Linear Interpolation|Inverse Transform|Simulation|Sample Mean|Sample Variance|Sampling Distribution|Invariance/i,
    frame: "Estimation is the bridge between data and assumptions. You choose an estimator, understand its sampling behaviour, and check whether the method is stable for the data available.",
  },
  {
    name: "Confidence Intervals, Hypothesis Tests, Power, and Non-Parametric Tests",
    test: /Hypothesis|Confidence Interval|Prediction Interval|Type I|Type II|Power|Critical|Goodness-of-Fit|Association|Fisher|Non-parametric|Contingency|Two-Sample|Paired|One-Sample|Variance and Mean Tests|Two-Proportion|Diagnostic Test|Specificity|False Positive|False Negative|p-Values|Outliers|Sample Size/i,
    frame: "Inference converts sample evidence into a controlled decision. State the parameter, write hypotheses, choose the statistic, use the correct reference distribution, and translate the result into business language.",
  },
  {
    name: "Regression, ANOVA, and Predictive Modelling",
    test: /Regression|ANOVA|Least Squares|Slope|R-Squared|Adjusted R-Squared|Overfitting|Mean Response|Individual Prediction|Gompertz|Transformed Predictor|One-Parameter|Weighted Least Squares|Origin|Multiple Regression|Backward Selection/i,
    frame: "Regression is a conditional mean model. The formula is only useful if the assumptions, residual behaviour, interpretation of coefficients, and prediction uncertainty are understood.",
  },
  {
    name: "Generalised Linear Models and Exponential Family",
    test: /GLM|Generalised Linear|Exponential Family|Canonical|Logit|Log Link|Poisson Regression|Scaled Deviance|Deviance|AIC|Saturated Models|Pearson Residuals|Variance Functions|Interaction/i,
    frame: "GLMs extend regression to non-normal responses. Identify the random component, systematic component, link function, variance function, likelihood, and deviance before interpreting coefficients.",
  },
  {
    name: "Bayesian Statistics and Credibility",
    test: /Bayesian|Bayes|Credibility|Empirical Bayes|EBCT|Posterior|Prior|Beta|Gamma-Poisson|Normal-Normal|Inverse-Gamma|Posterior Odds|Credible|Loss Function/i,
    frame: "Bayesian and credibility methods update prior belief with experience. The actuarial skill is to recognise what information is prior, what information is data, and how the posterior should affect decisions.",
  },
  {
    name: "Model Choice, Diagnostics, and Practical Actuarial Judgement",
    test: /Model Comparison|Model Choice|Residual|Diagnostics|Deviance Tables|Equivalent Parameterisations|Quick Distribution Checks|Simpson|Accuracy|Practical|Limitations/i,
    frame: "A technically fitted model can still be poor for pricing. Diagnostics, stability, explainability, operational cost, fairness, and sensitivity matter before a model becomes an actuarial assumption.",
  },
];

function splitChapters(markdown) {
  const parts = markdown.split(/^## Chapter /m);
  const intro = parts.shift().trim();
  const chapters = parts
    .map((part) => part.trim())
    .filter((part) => part && !part.startsWith("Master Template for Future Notes"))
    .map((part) => {
    const text = "## Chapter " + part.trim();
    const firstLine = text.split("\n", 1)[0];
    const match = firstLine.match(/^## Chapter\s+(\d+)\s*[:-]\s*(.+)$/);
    return {
      number: Number(match?.[1] ?? 0),
      title: match?.[2] ?? firstLine.replace(/^##\s*/, ""),
      text,
    };
  });
  return { intro, chapters };
}

function groupFor(chapter) {
  if (/Moment Generating|Cumulant Generating|Probability Generating|MGF|CGF|PGF/i.test(chapter.title)) {
    return groups.find((group) => group.name.startsWith("Core Distributions"));
  }
  if (/PCA|Principal Components|Factor Analysis|Correlation|Rebased|Kendall|Pearson|Rank|Coefficient of Variation|Descriptive|Data Analysis|Big Data/i.test(chapter.title)) {
    return groups.find((group) => group.name.startsWith("Data Analysis"));
  }
  if (/Chi-Square Tests|Goodness-of-Fit|Association|Contingency|Diagnostic Test|Specificity|False Positive|False Negative|Two-Proportion|Fisher|Non-parametric|Hypothesis|Confidence|Power|Type I|Type II|Critical/i.test(chapter.title)) {
    return groups.find((group) => group.name.startsWith("Confidence Intervals"));
  }
  if (/Regression|ANOVA|Least Squares|R-Squared|Slope|Prediction|Mean Response|Gompertz|Transformed Predictor|Weighted Least Squares|Origin|Backward Selection/i.test(chapter.title)) {
    return groups.find((group) => group.name.startsWith("Regression"));
  }
  if (/GLM|Generalised Linear|Exponential Family|Canonical|Logit|Log Link|Poisson Regression|Scaled Deviance|Deviance|AIC|Saturated Models|Pearson Residuals|Variance Functions|Interaction/i.test(chapter.title)) {
    return groups.find((group) => group.name.startsWith("Generalised"));
  }
  if (/Bayesian|Bayes|Credibility|Empirical Bayes|EBCT|Posterior|Prior|Beta-Binomial|Gamma-Poisson|Normal-Normal|Inverse-Gamma|Posterior Odds|Credible|Loss Function/i.test(chapter.title)) {
    return groups.find((group) => group.name.startsWith("Bayesian"));
  }
  return groups.find((group) => group.test.test(chapter.title)) ?? groups[groups.length - 1];
}

function topicType(titleText) {
  if (/Bayes|Prior|Posterior|Credibility|EBCT|Empirical/i.test(titleText)) return "bayes";
  if (/Regression|ANOVA|Least Squares|R-Squared|Slope|Prediction/i.test(titleText)) return "regression";
  if (/GLM|Exponential Family|Deviance|AIC|Link|Logit/i.test(titleText)) return "glm";
  if (/Hypothesis|Confidence|Test|Power|Type I|Type II|Goodness|Fisher|Interval/i.test(titleText)) return "inference";
  if (/Compound|Aggregate|Poisson Process|Arrival|Claims|Capital/i.test(titleText)) return "aggregate";
  if (/Simulation|MLE|Method of Moments|Estimator|CRLB|Likelihood|Bias/i.test(titleText)) return "estimation";
  if (/Correlation|PCA|Data|Descriptive|Coefficient/i.test(titleText)) return "data";
  return "distribution";
}

const deepBlocks = {
  distribution: {
    theory: [
      "Begin by checking support. A count model lives on non-negative integers, a severity model often lives on positive real values, and a transformed model may live on the whole real line after taking logs. Many exam errors happen before calculation because the wrong support is chosen.",
      "Separate the probability law from its moments. The probability law tells you every probability statement; the mean and variance are summaries. In actuarial work a model with the correct mean but the wrong tail can still be dangerous because solvency questions are tail questions.",
      "Use generating functions when sums are involved. MGFs, CGFs, and PGFs turn convolutions into algebra, but only under the conditions where they exist and where independence assumptions are valid.",
    ],
    steps: [
      "State the random variable and its support.",
      "Write the probability mass function, density, or distribution function.",
      "Identify parameters and their actuarial meaning.",
      "Calculate the required moment, probability, percentile, or transform.",
      "Check whether the answer is sensible using units, range, and limiting cases.",
    ],
    examples: [
      "Claim count example: if a motor portfolio has many zero-claim policies and a few multi-claim policies, compare Poisson and negative binomial assumptions by checking whether sample variance is close to, below, or above the sample mean.",
      "Severity example: for small routine medical claims a lognormal or gamma model may be adequate; for catastrophe-like property claims a heavier tail such as Pareto or Weibull may be needed.",
      "Exam example: after finding a probability, ask whether the question wanted an unconditional probability or a conditional probability after observing a claim, survival, or selection event.",
    ],
  },
  aggregate: {
    theory: [
      "Aggregate loss modelling combines frequency and severity. Frequency explains how often losses occur; severity explains how large they are when they occur. Their product gives expected loss only when the frequency and severity assumptions are aligned.",
      "The variance of aggregate loss has two sources: random claim sizes and random claim counts. This is why aggregate variance usually contains both a severity variance term and a squared mean severity term.",
      "Capital work is more demanding than pricing work. Pricing may focus on expected loss, while capital and reinsurance decisions need standard deviation, percentiles, stress tests, and tail scenarios.",
    ],
    steps: [
      "Define the claim count variable and the individual severity variables.",
      "Check independence and identical distribution assumptions.",
      "Condition on the claim count first.",
      "Use total expectation and total variance.",
      "Translate the result into premium, reserve, or capital language.",
    ],
    examples: [
      "Pricing example: expected annual loss equals expected claim count times expected claim size, then expenses, commission, profit margin, and risk loading are added.",
      "Capital example: two portfolios with the same expected loss can require different capital if one has low-frequency high-severity claims.",
      "Operational example: for cyber insurance, one ransomware event may produce legal, notification, downtime, and recovery costs, so severity volatility drives risk loading.",
    ],
  },
  inference: {
    theory: [
      "Inference is a disciplined way of deciding whether observed experience is consistent with an assumption. A confidence interval estimates a plausible range for a parameter; a hypothesis test checks whether the data are surprising under a stated null hypothesis.",
      "Type I error is rejecting a true null hypothesis. Type II error is failing to reject a false null hypothesis. Power is the probability of detecting an effect when it is truly present.",
      "A prediction interval is wider than a confidence interval for a mean because it includes both uncertainty about the average and the randomness of a new observation.",
    ],
    steps: [
      "Identify the parameter being estimated or tested.",
      "Write the null and alternative hypotheses, if a test is required.",
      "Choose the reference distribution and degrees of freedom.",
      "Compute the statistic, p-value, interval, or critical region.",
      "Finish with a plain-English actuarial conclusion.",
    ],
    examples: [
      "Fraud model example: a higher observed detection rate is useful only if the improvement is statistically convincing and operationally valuable after false positives are considered.",
      "Claims inflation example: test whether average claim cost has changed before changing a pricing basis, but also review exposure mix and large claims.",
      "Reserve example: a confidence interval for mean settlement cost should not be confused with a prediction interval for a single future claim.",
    ],
  },
  regression: {
    theory: [
      "Regression estimates a conditional mean, not a causal law by default. The coefficient tells how the fitted average response changes when a predictor changes, usually holding other predictors fixed.",
      "The residuals are as important as the fitted line. Patterns, changing spread, influential points, and non-normal residuals can reveal that the chosen model is too simple or the scale is wrong.",
      "Prediction requires two layers of uncertainty: uncertainty in the fitted mean and random variation of the individual outcome. This is why individual prediction intervals are wider than mean response intervals.",
    ],
    steps: [
      "Plot or inspect the relationship before fitting.",
      "Fit the model and write the fitted equation.",
      "Interpret each coefficient in business units.",
      "Check residuals, leverage, fit statistics, and uncertainty.",
      "Decide whether the model is suitable for pricing, reserving, or explanation.",
    ],
    examples: [
      "Motor pricing example: claim frequency may increase with driver age band, vehicle group, region, and prior claims, but coefficient interpretation must respect all variables included in the model.",
      "Life insurance example: a transformed mortality rating can produce a smooth premium formula, but the formula must be checked against underwriting judgement and table constraints.",
      "Exam example: if asked for an individual prediction, include the extra 1 inside the square root of the prediction standard error.",
    ],
  },
  glm: {
    theory: [
      "A GLM has three parts: a random component for the response distribution, a systematic component for the linear predictor, and a link function connecting the mean to the linear predictor.",
      "The link function determines coefficient interpretation. With a log link, a one-unit increase multiplies the mean by an exponential factor; with a logit link, a coefficient changes log-odds.",
      "Deviance compares fitted likelihoods. A smaller deviance can mean better fit, but model selection must balance fit, complexity, interpretability, and out-of-sample stability.",
    ],
    steps: [
      "Choose the response distribution based on the data type.",
      "Choose the link function and write the linear predictor.",
      "Include exposure or offset terms where required.",
      "Fit the model and interpret coefficients on the correct scale.",
      "Use residuals, deviance, AIC, and validation to judge suitability.",
    ],
    examples: [
      "Frequency example: Poisson GLM with log exposure offset estimates claim frequency per unit exposure.",
      "Binary example: a logit model can estimate probability of fraud, lapse, or claim occurrence.",
      "Severity example: gamma GLM with log link is often used for positive skewed claim amounts.",
    ],
  },
  bayes: {
    theory: [
      "Bayesian analysis combines prior information and data through the likelihood. The posterior distribution is proportional to prior times likelihood.",
      "Credibility theory has the same actuarial instinct: blend individual experience with collective experience. The more stable and relevant the individual data, the more credibility it receives.",
      "Conjugate priors are useful because the posterior stays in the same family. This makes updating transparent and exam calculations faster.",
    ],
    steps: [
      "Write the prior distribution and identify its parameters.",
      "Write the likelihood from the observed data.",
      "Multiply prior and likelihood, keeping parameter-dependent terms.",
      "Recognise the posterior family or normalise if required.",
      "Use the posterior mean, mode, interval, or credibility form for the decision.",
    ],
    examples: [
      "Renewal example: a beta prior for renewal probability can be updated after observing renewals and non-renewals.",
      "Claim count example: a gamma prior for a Poisson rate leads to a gamma posterior and a credibility-style blended estimate.",
      "Pricing example: a small employer group should not be priced only from its own volatile claims experience; credibility blends group experience with portfolio experience.",
    ],
  },
  estimation: {
    theory: [
      "An estimator is a rule for turning data into a parameter estimate. Different estimators can have different bias, variance, mean squared error, and robustness.",
      "Maximum likelihood chooses the parameter that makes the observed data most probable under the model. Method of moments matches sample moments to theoretical moments.",
      "Large-sample theory can give approximate intervals, but boundary estimates, censored observations, small samples, and heavy tails can make approximations unreliable.",
    ],
    steps: [
      "Write the model and parameter space.",
      "Choose MOM, MLE, least squares, or another estimator.",
      "Derive or solve the estimating equation.",
      "Check bias, variance, consistency, and practical stability where possible.",
      "Interpret the estimate in actuarial units.",
    ],
    examples: [
      "Severity example: estimating a Pareto tail parameter from a few large losses may be highly unstable, so sensitivity testing is essential.",
      "Frequency example: the Poisson MLE for a constant rate is total claims divided by total exposure.",
      "Simulation example: inverse transform simulation uses uniform random numbers to generate losses from a fitted distribution for stress testing.",
    ],
  },
  data: {
    theory: [
      "Good actuarial modelling starts with good data understanding. The same numerical value can mean count, amount, rate, duration, category code, or index depending on context.",
      "Correlation measures association, not causation. A high correlation may disappear after controlling for exposure mix, calendar period, or another confounding variable.",
      "Dimension reduction methods such as PCA summarise variation, but components must be interpreted carefully because mathematical variance is not the same as business importance.",
    ],
    steps: [
      "Classify each variable as categorical, ordinal, count, continuous, or time-based.",
      "Check missing data, outliers, exposure, and units.",
      "Summarise centre, spread, skewness, and dependence.",
      "Visualise before modelling.",
      "Document limitations before making assumptions.",
    ],
    examples: [
      "Portfolio example: average claim cost can rise because severity increased, or because the mix shifted toward higher-risk policies.",
      "Investment example: correlation between asset returns matters for diversification, but correlations can increase during market stress.",
      "Health example: large hospital claims can dominate the mean, so median and percentile summaries may tell a different story.",
    ],
  },
};

function blockFor(chapter) {
  return deepBlocks[topicType(chapter.title)] ?? deepBlocks.distribution;
}

function expansion(chapter, group, newNumber) {
  const block = blockFor(chapter);
  return `
### Expanded deep explanation

${group.frame}

${block.theory.map((item) => `- ${item}`).join("\n")}

### Step-by-step working method

${block.steps.map((item, index) => `${index + 1}. ${item}`).join("\n")}

### Extra practical actuarial examples

${block.examples.map((item) => `- ${item}`).join("\n")}

### Mini worked practice pattern

For this topic, use the following study routine:

1. Define the actuarial question in one sentence.
2. Translate the business wording into mathematical notation.
3. Choose the relevant model, statistic, or estimation method.
4. Perform the calculation step by step.
5. Check units, assumptions, and whether the answer is reasonable.
6. State the result as a pricing, reserving, capital, underwriting, or risk decision.

### Practical checklist

- What is random, and what is fixed?
- Is the question about a mean, probability, percentile, parameter, or future observation?
- Are we conditioning on new information?
- Are assumptions such as independence, identical distribution, normality, or constant variance justified?
- What would go wrong in real insurance work if this assumption is false?

### Exam answer structure

Start with definitions, show the formula, substitute values cleanly, calculate one line at a time, and finish with an interpretation. In CS1A-style questions, marks are often awarded for method and assumptions, not only for the final number.

`;
}

function buildExpandedMarkdown(source) {
  const { intro, chapters } = splitChapters(source);
  const buckets = new Map(groups.map((group) => [group.name, []]));
  for (const chapter of chapters) {
    buckets.get(groupFor(chapter).name).push(chapter);
  }

  let output = `# ${title}\n\n**Author:** ${author}\n\n`;
  output += "## How This Expanded Edition Is Organised\n\n";
  output += "The original paper-by-paper notes have been rearranged into a sequential study guide. Related concepts now sit together in larger master chapters, so probability comes before distributions, distributions before estimation and inference, inference before regression, and regression before GLMs, Bayesian methods, credibility, and model judgement.\n\n";
  output += "Each topic keeps the requested eight-part format: concept theory, actuarial use, mathematical derivation, simple example, exam-style case study, real-world actuarial case study, common mistakes, and revision checkpoint. After each topic, the notes add deeper explanation, more examples, a practical checklist, and an exam answer structure.\n\n";
  output += "This guide covers every topic currently collected from your CS1A notes and past-paper work. It is designed for revision, actuarial exam preparation, and practical modelling intuition. It is still a companion to the official syllabus, examiner reports, and your own timed paper practice.\n\n";
  output += `Total topics integrated: **${chapters.length}**.\n\n`;
  output += intro.replace(/^# .+\n+/, "") + "\n\n";

  output += "## Syllabus Flow Map\n\n";
  output += "| Study order | Master chapter | What it gives you |\n";
  output += "|---:|---|---|\n";
  groups.forEach((group, index) => {
    const count = buckets.get(group.name)?.length ?? 0;
    if (count) output += `| ${index + 1} | ${group.name} | ${count} integrated topics |\n`;
  });
  output += "\n";
  output += "Use the flow like this: first learn the foundations, then the standard distributions, then estimation and inference, then regression and GLMs, and finally Bayesian credibility and model-choice judgement. This order reduces repetition and makes old-paper questions easier to recognise.\n\n";

  let masterNumber = 1;
  let topicNumber = 1;
  for (const group of groups) {
    const list = buckets.get(group.name) ?? [];
    if (!list.length) continue;
    output += `## Master Chapter ${masterNumber}: ${group.name}\n\n`;
    output += `${group.frame}\n\n`;
    output += "### Topics in this master chapter\n\n";
    output += list.map((chapter) => `- Topic ${topicNumber + list.indexOf(chapter)}: ${chapter.title}`).join("\n") + "\n\n";

    for (const chapter of list) {
      const rewritten = chapter.text.replace(
        /^## Chapter\s+\d+\s*[:-]\s*(.+)$/m,
        `### Topic ${topicNumber}: $1`,
      );
      output += rewritten.replace(/^### (?!Topic )/gm, "#### ") + "\n";
      output += expansion(chapter, group, topicNumber);
      topicNumber += 1;
    }
    masterNumber += 1;
  }

  output += "## Final Revision Framework\n\n";
  output += "For every actuarial statistics problem, move through these questions: What is the random variable? What distribution or model is being assumed? What parameter is unknown? What information has been observed? What calculation is required? What assumption would change the answer? What decision does the result support?\n\n";
  output += "End every answer with business meaning. A number without interpretation is not yet actuarial work.\n";
  return output;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function renderMath(text, displayMode) {
  return katex.renderToString(text, {
    displayMode,
    throwOnError: false,
    strict: "ignore",
  });
}

const proseHints = /\b(number|amount|time|takes|values|claim|claims|policy|policies|probability of|expected value|average|fixed|continuous|discrete|events|trials|where|then it|given that|uncertainty|randomness|condition|distribution|example|response|support|parameter)\b/i;
const mathHints = /(?:=|~|\^|\\|>=|<=|\+|-|\*|\/|P\(|E\[|E\(|Var\(|SD\(|SE\(|sqrt|exp|lambda|sigma|alpha|beta|gamma|mu|theta|chi|int|sum|H0|H1|CI|SSE|SST|SSR|M_|K_)/i;

function looksLikeFormula(line) {
  const trimmed = line.trim();
  if (!trimmed || trimmed.length > 140) return false;
  if (!mathHints.test(trimmed)) return false;
  if (/^[A-Za-z ]+:$/.test(trimmed)) return false;
  if (proseHints.test(trimmed) && !/^(E|P|Var|SD|SE|CI|H0|H1|M_|K_|SSE|SST|SSR|X_bar|p_hat|beta_hat|alpha_hat|\w+\s*=)/.test(trimmed)) {
    return false;
  }
  return true;
}

function toLatex(line) {
  return line
    .trim()
    .replaceAll("approximately", "\\approx")
    .replaceAll("+/-", "\\pm")
    .replaceAll(">=", "\\ge ")
    .replaceAll("<=", "\\le ")
    .replaceAll("!=", "\\ne ")
    .replace(/\blambda\b/g, "\\lambda")
    .replace(/\bsigma\b/g, "\\sigma")
    .replace(/\bmu\b/g, "\\mu")
    .replace(/\balpha\b/g, "\\alpha")
    .replace(/\bbeta\b/g, "\\beta")
    .replace(/\bgamma\b/g, "\\gamma")
    .replace(/\btheta\b/g, "\\theta")
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
}

function renderCodeLine(line) {
  if (!looksLikeFormula(line)) return `<div>${escapeHtml(line)}</div>`;
  try {
    return `<div class="math-line">${renderMath(toLatex(line), true)}</div>`;
  } catch {
    return `<div>${escapeHtml(line)}</div>`;
  }
}

const renderer = new marked.Renderer();
renderer.code = ({ text }) => {
  const lines = String(text).replace(/\n+$/g, "").split("\n");
  const formulaCount = lines.filter(looksLikeFormula).length;
  const className = formulaCount > 0 ? "formula-box" : "note-box";
  return `<div class="${className}">${lines.map(renderCodeLine).join("")}</div>`;
};

function renderHtml(markdown, forEpub = false) {
  marked.setOptions({ gfm: true, breaks: false, renderer });
  let body = marked.parse(markdown);
  body = body
    .replace(/\\\[([\s\S]*?)\\\]/g, (_, expr) => renderMath(expr, true))
    .replace(/\\\(([\s\S]*?)\\\)/g, (_, expr) => renderMath(expr, false));
  const katexCss = fs.readFileSync(
    path.join(workspace, ".codex_tools/node_modules/katex/dist/katex.min.css"),
    "utf8",
  );
  const printCss = forEpub ? "" : "@page { size: A4; margin: 18mm 15mm 20mm; }";
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>${escapeHtml(title)}</title>
<meta name="author" content="${escapeHtml(author)}">
<style>${katexCss}</style>
<style>
${printCss}
body {
  margin: 0 auto;
  max-width: 880px;
  padding: ${forEpub ? "1em" : "0"};
  color: #14232b;
  font-family: "Segoe UI", Arial, sans-serif;
  font-size: 11.2pt;
  line-height: 1.58;
}
h1 { color: #0f3a4a; font-size: 25pt; line-height: 1.15; border-bottom: 3px solid #1f8a9e; padding-bottom: 14px; }
h2 { break-before: page; color: #0f3a4a; font-size: 19pt; border-bottom: 1px solid #bfd2d8; padding-bottom: 5px; }
h3 { color: #174f61; font-size: 15pt; margin-top: 22px; }
h4 { color: #24556a; font-size: 12.6pt; margin-top: 17px; }
p { margin: 7px 0 10px; }
ul, ol { margin: 7px 0 12px 24px; padding: 0; }
li { margin: 3px 0; }
table { width: 100%; border-collapse: collapse; margin: 12px 0 16px; font-size: 10.2pt; }
th, td { border: 1px solid #bfd2d8; padding: 6px 8px; }
th { background: #e8f3f5; color: #0f3a4a; }
code { color: #0d5363; background: #eef7f8; border-radius: 4px; padding: 0.08em 0.28em; }
.note-box {
  margin: 9px 0 13px;
  padding: 9px 12px;
  border: 1px solid #d4e1e5;
  border-left: 4px solid #1f8a9e;
  border-radius: 6px;
  background: #f7fbfc;
  font-family: Consolas, "Courier New", monospace;
  white-space: pre-wrap;
  break-inside: avoid;
}
.formula-box {
  margin: 9px 0 13px;
  padding: 9px 12px;
  border: 1px solid #d4e1e5;
  border-left: 4px solid #126782;
  border-radius: 6px;
  background: #f8fcfd;
  break-inside: avoid;
}
.formula-box > div:not(.math-line) {
  font-family: Consolas, "Courier New", monospace;
  white-space: pre-wrap;
}
.math-line { overflow: visible; }
.katex-display { overflow: visible; margin: 0.35em 0; }
</style>
</head>
<body>
${body}
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

function zipBuffer(files, outPath) {
  return new Promise((resolve, reject) => {
    const zip = new yazl.ZipFile();
    const output = fs.createWriteStream(outPath);
    output.on("close", resolve);
    output.on("error", reject);
    zip.outputStream.on("error", reject);
    zip.outputStream.pipe(output);
    for (const file of files) {
      zip.addBuffer(Buffer.from(file.data), file.name, file.options ?? {});
    }
    zip.end();
  });
}

async function createEpub(html) {
  const id = "urn:uuid:cs1a-expanded-amu-chaps";
  const cleanBody = html.match(/<body>([\s\S]*)<\/body>/)?.[1] ?? html;
  const chapterHtml = `<?xml version="1.0" encoding="utf-8"?>
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml" lang="en">
<head><title>${escapeHtml(title)}</title><meta charset="utf-8" /></head>
<body>${cleanBody}</body>
</html>`;
  const container = `<?xml version="1.0" encoding="UTF-8"?>
<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
  <rootfiles>
    <rootfile full-path="OEBPS/content.opf" media-type="application/oebps-package+xml"/>
  </rootfiles>
</container>`;
  const opf = `<?xml version="1.0" encoding="UTF-8"?>
<package version="3.0" unique-identifier="bookid" xmlns="http://www.idpf.org/2007/opf">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
    <dc:identifier id="bookid">${id}</dc:identifier>
    <dc:title>${escapeHtml(title)}</dc:title>
    <dc:creator>${escapeHtml(author)}</dc:creator>
    <dc:language>en</dc:language>
    <meta property="dcterms:modified">${new Date().toISOString().replace(/\.\d{3}Z$/, "Z")}</meta>
  </metadata>
  <manifest>
    <item id="nav" href="nav.xhtml" media-type="application/xhtml+xml" properties="nav"/>
    <item id="book" href="book.xhtml" media-type="application/xhtml+xml"/>
  </manifest>
  <spine>
    <itemref idref="book"/>
  </spine>
</package>`;
  const nav = `<?xml version="1.0" encoding="utf-8"?>
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml" lang="en">
<head><title>Contents</title></head>
<body>
<nav epub:type="toc" id="toc">
<h1>Contents</h1>
<ol><li><a href="book.xhtml">${escapeHtml(title)}</a></li></ol>
</nav>
</body>
</html>`;
  await zipBuffer([
    { name: "mimetype", data: "application/epub+zip", options: { compress: false } },
    { name: "META-INF/container.xml", data: container },
    { name: "OEBPS/content.opf", data: opf },
    { name: "OEBPS/nav.xhtml", data: nav },
    { name: "OEBPS/book.xhtml", data: chapterHtml },
  ], outEpub);
}

const source = fs.readFileSync(sourcePath, "utf8");
const expanded = buildExpandedMarkdown(source);
const html = renderHtml(expanded);
const epubHtml = renderHtml(expanded, true);

fs.writeFileSync(outMd, expanded, "utf8");
fs.writeFileSync(outHtml, html, "utf8");

const chrome = findChrome();
execFileSync(chrome, [
  "--headless=new",
  "--disable-gpu",
  "--no-sandbox",
  `--print-to-pdf=${outPdf}`,
  `file:///${outHtml.replaceAll("\\", "/")}`,
], { stdio: "inherit" });

await createEpub(epubHtml);

console.log(outPdf);
console.log(outEpub);
