# CS1A Actuarial Statistics - Basic to Advanced Study Material

This file is a study path for IAI CS1A Actuarial Statistics. It is based on the May 2026 paper you shared and is designed to help you move from first principles to exam-level application.

Use it with:

- `CS1A_Actuarial_Statistics_Practical_Notes.md` for question-wise notes.
- Your official solution paper for final answer style and marking scheme.

## How to Study This Subject

Actuarial statistics becomes easier when you study each topic in this order:

```text
1. What real-world problem are we solving?
2. What random variable is being modelled?
3. What distribution or model is appropriate?
4. What parameter or probability is being asked?
5. What formula connects the model to the answer?
6. What assumption could fail in real life?
```

For exams, do not memorise formulas alone. You must know the story behind the formula.

## Level 0 - Mathematical Foundation

### What to know

Before probability and statistics feel natural, these tools must be comfortable:

- Summation notation
- Differentiation
- Integration
- Exponential and logarithm rules
- Mean, variance, and standard deviation
- Basic algebraic rearrangement

### Practical actuarial meaning

An actuary almost always works with uncertain cashflows. Mathematics helps convert uncertain events into expected values, variability, and risk margins.

### Example

If an insurer has:

```text
Expected number of claims = 100
Expected cost per claim = 20,000
```

then expected total claim cost is:

```text
100 * 20,000 = 2,000,000
```

But pricing also needs variability, not only the average.

### You are ready when

You can calculate means and variances from a small table without looking at notes.

## Level 1 - Random Variables and Probability

### What to know

A random variable is a numerical outcome of an uncertain event.

Examples:

```text
X = number of claims in a year
Y = amount of one claim
T = time until next claim
I = fraud indicator, 1 if fraud and 0 otherwise
```

Important ideas:

- Discrete random variables use probability mass functions.
- Continuous random variables use probability density functions.
- Marginal distribution removes unwanted variables.
- Conditional distribution updates probabilities after new information.

### Real-world case study

A motor insurer tracks:

```text
A = number of reported claims from a policyholder
B = number of claims that become complaints
```

The claims manager may ask:

```text
E[B | B >= 1]
```

This means: once at least one complaint has happened, how many complaints should we expect? This supports staffing and reserving.

### Exam links

- Q2: equal probability and symmetry
- Q4: conditional expectation
- Q15: marginal density

### Key formulas

```text
E[X] = sum x P(X=x)                 discrete
E[X] = integral x f(x) dx           continuous

P(A | B) = P(A and B) / P(B)

f_X(x) = integral f_X,Y(x,y) dy
```

### Common traps

- Forgetting to divide by the conditioning probability.
- Summing when the variable is continuous.
- Integrating when the variable is discrete.
- Treating density as probability. A density can be greater than 1; area gives probability.

## Level 2 - Expected Value, Variance, MGF, and CGF

### What to know

Expected value is the long-run average. Variance measures spread.

The moment generating function is:

```text
M_X(t) = E[e^(tX)]
```

The cumulant generating function is:

```text
K_X(t) = log M_X(t)
```

For a cumulant generating function:

```text
E[X] = K'_X(0)
Var(X) = K''_X(0)
```

For a joint MGF:

```text
M_X,Y(s1, s2) = E[e^(s1 X + s2 Y)]
```

If `U = X - Y`:

```text
M_U(t) = M_X,Y(t, -t)
```

### Real-world case study

An insurer defines:

```text
U = premium income - claim outgo
```

The distribution of `U` is the profit distribution. MGFs help derive the expected profit and variance of profit, especially when multiple random components interact.

### Exam links

- Q1: cumulant generating function
- Q7: MGF of `X - Y`

### Common traps

- For CGF, the coefficient of `t^2` is not directly the variance.
- Do not use `M_X(t)M_Y(-t)` unless independence is given.

## Level 3 - Standard Distributions

### 3.1 Binomial Distribution

Use when there are:

- fixed number of trials
- two outcomes per trial
- constant probability
- independent trials

```text
X ~ Binomial(n, p)
E[X] = np
Var(X) = np(1-p)
```

Case study:

A fraud detection system checks whether each claim is fraudulent. If 8% of claims are fraudulent, the number of fraudulent claims in 10,000 claims is approximately binomial.

Exam links:

- Q8: rare health condition
- Q21: fraud sensitivity

### 3.2 Geometric Distribution

Use when counting trials until first success.

```text
P(X=k) = (1-p)^(k-1)p
```

Case study:

Time until first claim after policy issue can be modelled with a geometric distribution if periods are discrete and independent.

Exam link:

- Q9: first 6 on a die

### 3.3 Poisson Distribution

Use for counts over a fixed exposure period.

```text
X ~ Poisson(lambda)
E[X] = Var(X) = lambda
```

Case study:

Number of cyber incidents per company per year.

Exam links:

- Q11: compound Poisson aggregate loss
- Q17: checking Poisson fit

Trap:

If sample variance is much larger than sample mean, the data may be overdispersed and Poisson may be unsuitable.

### 3.4 Exponential Distribution

Use for waiting times between events.

```text
T ~ Exponential(lambda)
f(t) = lambda e^(-lambda t)
E[T] = 1/lambda
```

Case study:

Time between cyber attacks, drone detections, claim notifications, or hospital admissions.

Exam link:

- Q16: exponential likelihood with Gamma prior

### 3.5 Normal Distribution

Use for continuous measurements and large-sample approximations.

```text
Z = (X - mean) / sd
```

Case study:

Average claim amount or average medical cost may be approximately normal for large samples due to the Central Limit Theorem.

Exam links:

- Q8: CLT approximation
- Q13: comparing normal tails
- Q19: confidence and prediction intervals

### 3.6 Lognormal Distribution

Use for positive skewed amounts.

If:

```text
ln Y ~ Normal(mu, sigma^2)
```

then:

```text
E[Y] = exp(mu + sigma^2 / 2)
```

Case study:

Insurance claim severity, property rent, salary, medical cost, and cyber loss sizes are often positive and right-skewed.

Exam link:

- Q6: lognormal GLM percentage increase

## Level 4 - Sampling, Estimation, and CLT

### What to know

A statistic is calculated from a sample. An estimator is a statistic used to estimate a population parameter.

The sample mean has standard error:

```text
SE(mean) = s / sqrt(n)
```

The Central Limit Theorem says that for large `n`, the sample mean is approximately normal even if individual observations are not normal.

### Real-world case study

A health actuary samples 500 policyholders to estimate average annual hospital cost. The standard error shows how reliable the sample mean is as an estimate of the true portfolio mean.

### Exam links

- Q5: standard error
- Q8: CLT for binomial count
- Q19: confidence interval for mean

### Common traps

- Standard deviation describes individual observations.
- Standard error describes the sample mean.
- Large sample size reduces standard error, but not individual variability.

## Level 5 - Confidence Intervals, Prediction Intervals, and Hypothesis Tests

### Confidence interval

A confidence interval estimates an unknown population parameter.

```text
sample estimate +/- critical value * standard error
```

### Prediction interval

A prediction interval estimates a future individual observation.

It is wider because a future observation has its own randomness.

### Hypothesis test

General structure:

```text
1. State H0 and H1.
2. Choose test statistic.
3. Calculate observed statistic.
4. Compare with critical value or p-value.
5. Conclude in business language.
```

### Real-world case study

A fraud model claims sensitivity above 80%. The insurer tests whether observed detection performance is strong enough to support that claim statistically.

### Exam links

- Q19: confidence interval and prediction interval
- Q21: sensitivity test and confidence interval

### Common traps

- A 95% confidence interval does not mean there is a 95% probability the fixed true parameter lies in this one interval.
- A one-sided test and two-sided confidence interval answer related but not identical questions.
- Always interpret in the context of the problem.

## Level 6 - Compound Risk Models

### What to know

Aggregate claim amount:

```text
S = X_1 + X_2 + ... + X_N
```

where:

```text
N = number of claims
X_i = size of claim i
```

If `N ~ Poisson(lambda)`:

```text
E[S] = lambda E[X]
Var(S) = lambda E[X^2]
       = lambda (Var(X) + E[X]^2)
```

### Real-world case study

Cyber insurance pricing:

```text
number of cyber incidents = frequency
loss per incident = severity
total annual loss = aggregate loss
```

Premium may be:

```text
Expected loss + risk margin
```

### Exam link

- Q11: cyber risk premium

### Common traps

- Aggregate variance includes both claim count risk and claim size risk.
- Do not use only severity standard deviation.

## Level 7 - Regression

### Simple linear regression

```text
Y = a + bX + error
```

The slope means:

```text
expected change in Y for a one-unit increase in X
```

### Multiple linear regression

```text
Y = alpha + beta_1 X_1 + beta_2 X_2 + error
```

Each coefficient is interpreted holding other predictors constant.

### ANOVA test for regression slope

Tests:

```text
H0: slope = 0
H1: slope != 0
```

### R-squared

```text
R^2 = explained variation / total variation
```

It measures goodness of fit, not causation.

### Real-world case study

An insurer models claim frequency using driver age, vehicle type, and past claims. Regression helps quantify which variables matter and by how much.

### Exam links

- Q3: interpreting multiple regression
- Q18: simple regression, ANOVA, R-squared, residual
- Q22: outlier effect on regression slope

### Common traps

- A strong regression relationship does not prove causation.
- Outliers can strongly affect slope.
- A coefficient in multiple regression is not a simple standalone relationship.

## Level 8 - Generalised Linear Models

### What to know

A GLM has three parts:

```text
1. Random component: distribution of response variable
2. Systematic component: linear predictor
3. Link function: connects mean to linear predictor
```

Common examples:

```text
Claim count       -> Poisson GLM, log link
Claim severity    -> Gamma GLM, often log link in practice
Claim occurrence  -> Binomial GLM, logit link
Continuous score  -> Normal GLM, identity link
```

Canonical links:

```text
Poisson  -> log
Gamma    -> inverse
Binomial -> logit
Normal   -> identity
```

### Factor variables

Categorical variables are represented using dummy variables.

If a factor has `k` levels and one base level:

```text
number of parameters = k - 1
```

### Interactions

```text
A:B means interaction only
A*B means A + B + A:B
```

### Real-world case study

A health actuary models mental health score using sleep, dopamine level, mindfulness time, marital status, and employment status. Factor variables capture group differences; interactions capture situations where one predictor's effect changes across groups.

### Exam links

- Q6: lognormal model
- Q10: canonical links
- Q20: factor variables, interactions, deviance model selection

### Common traps

- Count factor parameters carefully.
- Include base level in the intercept, not as a separate dummy.
- Interaction terms can multiply quickly.

## Level 9 - Bayesian Statistics and Credibility

### What to know

Bayesian statistics updates prior belief using data.

```text
posterior proportional to likelihood * prior
```

### Beta-Binomial model

Use for unknown probability.

```text
theta ~ Beta(alpha, beta)
x | theta ~ Binomial(n, theta)
theta | x ~ Beta(alpha + x, beta + n - x)
```

Credibility idea:

```text
posterior estimate = Z * sample estimate + (1-Z) * prior estimate
```

### Exponential-Gamma model

Use for unknown event rate with waiting-time data.

```text
t_i | lambda ~ Exponential(lambda)
lambda ~ Gamma(alpha, beta)
lambda | data ~ Gamma(alpha + n, beta + sum t_i)
```

Under quadratic loss:

```text
Bayes estimate = posterior mean
```

Under absolute error loss:

```text
Bayes estimate = posterior median
```

Under all-or-nothing loss:

```text
Bayes estimate = posterior mode
```

### Real-world case study

A small insurer has limited claim experience for a new product. It combines:

```text
prior = industry experience
data = its own emerging claims
posterior = updated pricing assumption
```

### Exam links

- Q14: Beta-Binomial credibility
- Q16: Exponential-Gamma posterior

### Common traps

- Check whether Gamma uses rate or scale parameterisation.
- Posterior mean, median, and mode correspond to different loss functions.

## Level 10 - Non-Parametric Thinking and Bootstrapping

### What to know

Parametric methods assume a distribution, such as Poisson or Normal.

Non-parametric methods use the empirical data more directly.

Bootstrapping means repeatedly resampling from observed data to approximate uncertainty or summaries.

### Real-world case study

If claim counts are irregular and Poisson does not fit well, bootstrapping actual historical claim counts can preserve the observed shape without forcing the wrong model.

### Exam link

- Q17: bootstrap samples and Poisson model limitations

### Common traps

- Bootstrapping from observed data is non-parametric.
- Simulating from a fitted Poisson model is parametric.

## Level 11 - Correlation, Rank Correlation, and Data Types

### Pearson correlation

Measures linear association.

```text
r = covariance(X,Y) / (sd_X sd_Y)
```

It is sensitive to outliers.

### Kendall correlation

Uses concordant and discordant pairs.

```text
concordant: both variables move in same ranked direction
discordant: variables move in opposite ranked direction
```

Kendall's tau is positive if concordant pairs dominate and negative if discordant pairs dominate.

### Dataset types

```text
Cross-sectional: many units at one time
Longitudinal: same units over time
Censored: value partly unknown beyond a limit
Truncated: observations outside a range are not included
```

### Real-world case study

In capital modelling, correlation between risks affects diversification benefit. If motor claims and weather claims are highly correlated during storms, total capital requirement may increase.

### Exam link

- Q22: dataset type, Pearson correlation, Kendall correlation, outliers

### Common traps

- Weak correlation does not mean no relationship; it may mean no linear relationship.
- Outliers can change Pearson correlation and regression slope sharply.
- Rank correlation is often more robust than Pearson correlation.

## Recommended 21-Day Study Plan

### Days 1-3: Probability foundations

Study:

- random variables
- expectation and variance
- conditional probability
- marginal distributions

Practice:

- Q2, Q4, Q15

Goal:

You can identify whether to sum, integrate, or condition.

### Days 4-6: Distributions

Study:

- Binomial
- Geometric
- Poisson
- Exponential
- Normal
- Lognormal

Practice:

- Q6, Q8, Q9, Q13

Goal:

You can match a story to the correct distribution.

### Days 7-8: MGF, CGF, and aggregate risk

Study:

- MGF
- CGF
- joint MGF
- compound Poisson

Practice:

- Q1, Q7, Q11

Goal:

You can derive mean and variance from generating functions and aggregate loss models.

### Days 9-11: Sampling and inference

Study:

- standard error
- confidence intervals
- prediction intervals
- hypothesis testing

Practice:

- Q5, Q19, Q21

Goal:

You can write statistical conclusions in business language.

### Days 12-14: Regression

Study:

- simple regression
- multiple regression
- ANOVA
- R-squared
- residuals
- outliers

Practice:

- Q3, Q18, Q22

Goal:

You can interpret coefficients and test whether a slope matters.

### Days 15-17: GLMs

Study:

- GLM components
- link functions
- factor variables
- interactions
- deviance

Practice:

- Q6, Q10, Q20

Goal:

You can count model parameters and choose a model using deviance.

### Days 18-19: Bayesian and credibility

Study:

- prior, likelihood, posterior
- Beta-Binomial
- Gamma-Exponential
- credibility factor
- Bayes estimates under loss functions

Practice:

- Q14, Q16

Goal:

You can derive a posterior distribution and choose the correct Bayesian estimator.

### Days 20-21: Mixed exam practice

Practice:

- Re-solve Q1 to Q22 without looking at notes.
- Mark each question as:
  - clear
  - formula known but slow
  - concept unclear
  - calculation error

Goal:

You know exactly what to revise before the exam.

## One-Page Formula Sheet

```text
E[X] = sum x p(x)
Var(X) = E[X^2] - E[X]^2

P(A | B) = P(A and B) / P(B)

M_X(t) = E[e^(tX)]
K_X(t) = log M_X(t)
E[X] = K'_X(0)
Var(X) = K''_X(0)

Binomial:
E[X] = np
Var(X) = np(1-p)

Geometric:
P(X=k) = (1-p)^(k-1)p

Poisson:
E[X] = Var(X) = lambda

Exponential:
f(t) = lambda e^(-lambda t)
E[T] = 1/lambda

Lognormal:
E[Y] = exp(mu + sigma^2/2)

Standard error:
SE(mean) = s / sqrt(n)

Confidence interval:
estimate +/- critical value * SE

Compound Poisson:
E[S] = lambda E[X]
Var(S) = lambda(Var(X) + E[X]^2)

Simple regression:
b = S_xy / S_xx
a = y_bar - b x_bar
R^2 = SSR / SST

Pearson correlation:
r = covariance(X,Y) / (sd_X sd_Y)

Beta-Binomial:
Beta(alpha, beta) -> Beta(alpha + x, beta + n - x)

Gamma-Exponential:
Gamma(alpha, beta) -> Gamma(alpha + n, beta + sum t_i)
```

## How to Convert Any Exam Question into Notes

For every solved question, write:

```text
Question number:
Topic:
Random variable:
Distribution/model:
Formula:
Calculation:
Final answer:
Real-world actuarial use:
Trap:
```

This turns past papers into your personal textbook.

