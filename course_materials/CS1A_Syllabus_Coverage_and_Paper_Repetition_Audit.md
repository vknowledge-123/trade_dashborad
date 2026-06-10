# CS1A Syllabus Coverage and Paper Repetition Audit

Based on:

- CS1 syllabus for 2026 examinations
- Feb 2025 CS1A paper and indicative solution
- May 2025 CS1A paper and indicative solution
- Nov 2025 CS1A paper and indicative solution
- May 2026 CS1A paper
- Current notes file: `CS1A_Concept_Theory_Derivation_Case_Study_Notes.md`

## Overall Answer

We have covered most of the high-frequency exam concepts from the syllabus.

Approximate coverage status:

```text
Fully or strongly covered:       75% to 80%
Partially covered:               15% to 20%
Still needs dedicated notes:      5% to 10%
```

The most repeated areas across the papers are:

```text
Regression and GLMs
Random variables and distributions
Statistical inference
Bayesian credibility
Correlation and data analysis
```

## Syllabus Topic Coverage

| Syllabus Area | Syllabus Weight | Current Coverage | Status |
|---|---:|---:|---|
| 1. Data Analysis | 10% | Around 70% | Partially covered |
| 2. Random Variables and Distributions | 20% | Around 80% | Strongly covered |
| 3. Statistical Inference | 25% | Around 75% | Strongly covered, with gaps |
| 4. Regression Theory and Applications | 30% | Around 85% | Strongly covered |
| 5. Bayesian Statistics | 15% | Around 85% | Strongly covered |

## Topic-by-Topic Coverage Details

### 1. Data Analysis

Covered:

- Descriptive analysis
- Central tendency and dispersion
- Pearson correlation
- Spearman correlation
- Kendall correlation
- Fisher transformation for correlation
- PCA basics and Kaiser's criterion
- Cross-sectional, longitudinal, censored, truncated data

Partially covered / needs more:

- Full data analysis workflow
- Reproducible research
- Large data sets
- Exploratory visualisations
- Use of software for EDA

Current chapters:

```text
Chapter 11 - Bootstrapping, Correlation, and PCA
Chapter 17 - Descriptive Analysis, Central Tendency, and Dispersion
Chapter 21 - Two-Sample Confidence Intervals and Correlation Tests
```

### 2. Random Variables and Distributions

Covered:

- Discrete and continuous random variables
- Marginal and conditional distributions
- Conditional expectation
- Mean and variance
- Linear combinations and covariance
- MGF and CGF
- Geometric, Binomial, Poisson
- Exponential, Gamma, Normal, Lognormal
- Poisson process and thinning
- CLT
- Mixture distributions
- Minimum of geometric variables

Partially covered / needs more:

- Negative binomial
- Hypergeometric
- Uniform distribution as a standalone topic
- Chi-square, t and F distributions as standalone sampling distributions
- Inverse transform method
- Simulation using software

Current chapters:

```text
Chapters 1 to 6
Chapter 12
Chapter 13
Chapter 18
Chapter 20
Chapter 27
```

### 3. Statistical Inference

Covered:

- Method of moments
- Maximum likelihood
- Bias
- MSE
- CRLB
- Standard error
- Confidence intervals
- Prediction intervals
- Type I and Type II errors
- Power
- Chi-square goodness-of-fit
- Contingency table test
- Bootstrap basics
- Censored/grouped likelihood

Partially covered / needs more:

- Consistency and efficiency in more detail
- Asymptotic distribution of MLEs
- Bootstrap confidence intervals
- Paired data tests
- Permutation tests
- Confidence intervals for variance
- Confidence intervals for binomial and Poisson parameters
- Likelihood ratio tests as a standalone inference topic

Current chapters:

```text
Chapter 7
Chapter 14
Chapter 15
Chapter 19
Chapter 21
Chapter 29
Chapter 34
```

### 4. Regression Theory and Applications

Covered:

- Simple linear regression
- Multiple regression interpretation
- Least squares
- ANOVA for regression
- Slope tests
- Slope confidence intervals
- R-squared and adjusted R-squared
- Regression through origin
- GLMs
- Exponential family rewriting
- Canonical links
- Factors and interactions
- Scaled deviance
- AIC
- Gamma GLM with log link
- GLM prediction

Partially covered / needs more:

- Residual diagnostics
- Pearson and deviance residuals
- Prediction interval for regression response
- Polynomial models
- Full software output interpretation
- Likelihood-ratio tests for GLMs in more detail

Current chapters:

```text
Chapter 8
Chapter 9
Chapter 16
Chapter 22
Chapter 24
Chapter 25
Chapter 30
Chapter 32
Chapter 35
```

### 5. Bayesian Statistics

Covered:

- Bayes theorem idea
- Prior, likelihood, posterior
- Conjugate priors
- Beta-Binomial
- Gamma-Exponential
- Beta-Geometric
- Posterior mean, median, mode under loss functions
- Credibility factor
- Bayesian credibility
- Empirical Bayes Model 1
- Empirical Bayes Model 2

Partially covered / needs more:

- Credible intervals beyond Beta normal approximation
- Bayes vs Empirical Bayes comparison as a standalone chapter
- More loss functions and decision theory

Current chapters:

```text
Chapter 10
Chapter 23
Chapter 26
Chapter 31
Chapter 33
```

## Paper-Wise Topic Mix

These percentages are approximate because some questions test more than one syllabus area. I classified each question by its main exam skill.

| Paper | Data Analysis | Random Variables | Statistical Inference | Regression / GLM | Bayesian |
|---|---:|---:|---:|---:|---:|
| Feb 2025 | 10% | 19% | 21% | 30% | 20% |
| May 2025 | 4% | 21% | 31% | 30% | 14% |
| Nov 2025 | 9% | 23% | 19% | 28% | 21% |
| May 2026 | 8% | 21% | 30% | 29% | 12% |
| Average observed | 7.75% | 21.00% | 25.25% | 29.25% | 16.75% |
| Syllabus weight | 10% | 20% | 25% | 30% | 15% |

## What Repeats Most Across Papers

Repeated in 4 out of 4 papers:

```text
Regression / GLM
Random variables and distributions
Statistical inference
Bayesian statistics / credibility
Correlation or data analysis
```

High-frequency subtopics:

| Concept | Appears in Papers | Repetition |
|---|---:|---:|
| Regression / GLM model interpretation | 4 of 4 | 100% |
| Confidence intervals / hypothesis tests | 4 of 4 | 100% |
| Correlation / rank correlation / Fisher transform | 4 of 4 | 100% |
| Bayesian posterior / credibility | 4 of 4 | 100% |
| Poisson / Poisson process / count models | 4 of 4 | 100% |
| Exponential / Gamma / waiting time or severity | 4 of 4 | 100% |
| MGF / CGF / generating functions | 3 of 4 | 75% |
| Chi-square / goodness-of-fit / contingency table | 3 of 4 | 75% |
| PCA / dimension reduction | 1 of 4 | 25% |
| Regression through origin | 1 of 4 | 25% |
| Conditional independence | 1 of 4 | 25% |
| EBCT Model 2 | 1 of 4 | 25% |

## Practical Priority Ranking

Study first:

```text
1. Regression and GLMs
2. Statistical inference
3. Random variables and distributions
4. Bayesian credibility
5. Correlation and data analysis
```

Do not skip these:

```text
Poisson, exponential, gamma, normal, lognormal
MGF and CGF
CLT
Confidence intervals
Hypothesis tests
Chi-square tests
Simple regression and ANOVA
GLM links, factors, interactions, deviance and AIC
Bayesian conjugacy and credibility
```

## Remaining Notes to Build

To reach close to full syllabus coverage, create dedicated chapters for:

```text
1. Negative binomial and hypergeometric distributions
2. Uniform, chi-square, t and F distributions
3. Inverse transform simulation
4. Sampling distribution of sample variance
5. Confidence interval for variance
6. Binomial and Poisson confidence intervals
7. Paired data tests and paired confidence intervals
8. Permutation tests
9. Asymptotic distribution, efficiency and consistency of estimators
10. Regression residual diagnostics
11. Pearson and deviance residuals in GLMs
12. Bayes vs Empirical Bayes comparison
13. Reproducible research and data-analysis workflow
```

## Bottom Line

Yes, our notes now cover the majority of the syllabus and the highest-repetition exam concepts.

The strongest coverage is:

```text
Regression / GLM
Bayesian credibility
Random variables
Inference
Correlation
```

The biggest remaining gaps are:

```text
simulation methods
some less-tested distributions
paired and permutation tests
residual diagnostics
software/reproducibility/data workflow
```

