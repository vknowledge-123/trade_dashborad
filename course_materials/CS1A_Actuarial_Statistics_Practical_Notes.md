# CS1A Actuarial Statistics - Practical Notes

Source: IAI CS1A May 2026 question paper, `Untitled document.pdf`.

These notes are built to connect exam concepts with actuarial work. The aim is not only to know the formula, but to know when an actuary would use it.

## 1. Moment Generating Functions and Cumulants

### Core idea

The moment generating function is:

```text
M_X(t) = E[exp(tX)]
```

The cumulant generating function is:

```text
K_X(t) = log M_X(t)
```

The first two derivatives of the cumulant generating function at zero give:

```text
E[X] = K'_X(0)
Var(X) = K''_X(0)
```

For `K_X(t) = 2t + 3t^2`:

```text
K'_X(t) = 2 + 6t, so E[X] = 2
K''_X(t) = 6, so Var(X) = 6
```

### Real-world actuarial case study

Suppose an insurer models the number of emergency roadside assistance calls per policyholder. The cumulant generating function can summarize the entire claim count distribution. The first derivative gives expected call frequency, while the second derivative gives volatility. Pricing needs both: the average cost and the uncertainty around it.

### Exam trap

Do not read the coefficient of `t^2` as variance. For a cumulant generating function, variance is the second derivative, so the coefficient of `t^2` gets multiplied by 2.

## 2. Symmetry and Equal Chance Arguments

### Core idea

In a fair elimination game with `n` identical players and one final winner, each player has the same chance of winning. The total probability across all players is 1, so:

```text
P(a particular player wins) = 1 / n
```

### Real-world actuarial case study

In exposure modelling, if a portfolio is split into identical risk cells and no cell has a distinguishing feature, each cell receives equal allocation. For example, allocating a pooled expected claim amount across identical policyholders uses the same symmetry logic.

### Exam trap

Do not multiply all round-by-round survival probabilities unless needed. Symmetry solves the problem faster.

## 3. Multiple Linear Regression Interpretation

### Core idea

For:

```text
Y = alpha + beta_1 X_1 + beta_2 X_2
```

`beta_2` means the expected change in `Y` for a one-unit increase in `X_2`, holding `X_1` constant.

If:

```text
Y = 150 + 3X_1 + 2X_2
```

then each extra mock test increases predicted score by 2 marks, holding study hours constant.

### Real-world actuarial case study

A health insurer may model annual claim cost using:

```text
Claim cost = alpha + beta_1 age + beta_2 BMI + beta_3 smoker_indicator
```

The smoker coefficient is interpreted after controlling for age and BMI. That matters because smokers may also differ in age or health profile.

### Exam trap

"Holding other variables constant" is the key phrase. Without it, the statement is usually too strong.

## 4. Conditional Expectation from a Joint Distribution

### Core idea

To calculate `E[B | B >= 1]`:

```text
E[B | B >= 1] = sum_b b * P(B=b | B>=1)
```

Using the paper's distribution:

```text
P(B=0) = 10/75
P(B=1) = 25/75
P(B=2) = 40/75

E[B | B>=1] = (1*(25/75) + 2*(40/75)) / ((25+40)/75)
             = 105/65
             = 1.615
```

### Real-world actuarial case study

A claims team may ask: "Given that at least one complaint occurred on a policy, how many complaints do we expect?" This is more useful for operational staffing than the unconditional expected number of complaints.

### Exam trap

After conditioning, always divide by the probability of the condition. Forgetting the denominator gives an unconditional partial expectation, not a conditional expectation.

## 5. Standard Error of the Sample Mean

### Core idea

```text
SE(sample mean) = sample standard deviation / sqrt(n)
```

If sample variance is `400,000` and `n = 36`:

```text
s = sqrt(400000) = 632.46
SE = 632.46 / 6 = 105.41
```

### Real-world actuarial case study

If a bank estimates average customer withdrawal amount from a sample, the standard error tells how much the sample mean may move from sample to sample. It supports confidence intervals and pricing assumptions.

### Exam trap

The standard deviation measures individual variability. The standard error measures variability of the sample mean.

## 6. Lognormal GLM and Percentage Change

### Core idea

If:

```text
ln Y ~ Normal(mu, sigma^2)
E[Y] = exp(mu + sigma^2 / 2)
```

and:

```text
mu = beta_0 + beta_1 X
```

then increasing `X` by `d` multiplies `E[Y]` by:

```text
exp(beta_1 d)
```

For floor 2 to floor 4, `d = 2`, `beta_1 = 0.5`:

```text
increase factor = exp(1) = 2.718
percentage increase = 171.8%, approximately 172%
```

### Real-world actuarial case study

Severity models are often lognormal. If claim severity is modelled on a log scale, a coefficient does not add a fixed amount to expected cost; it multiplies expected cost.

### Exam trap

For a lognormal model, do not say the increase is just `beta_1 * d = 100%`. Convert back from log scale using the exponential.

## 7. MGF of a Linear Combination

### Core idea

For `U = X - Y`:

```text
M_U(t) = E[exp(t(X-Y))]
       = E[exp(tX - tY)]
       = M_X,Y(t, -t)
```

### Real-world actuarial case study

An insurer may define underwriting profit as premium minus claims. The MGF of profit uses a joint distribution if premium and claims are dependent.

### Exam trap

`M_X(t) * M_Y(-t)` is valid only if `X` and `Y` are independent. The joint MGF answer is safer and more general.

## 8. Central Limit Theorem for Binomial Counts

### Core idea

If `X ~ Binomial(n, p)` and `n` is large:

```text
X approximately Normal(np, np(1-p))
```

For `n = 2000`, `p = 0.02`:

```text
mean = 40
variance = 39.2
sd = 6.26
P(X > 50) approximately P(Z > (50.5 - 40)/6.26)
                 approximately P(Z > 1.68)
                 approximately 0.05
```

### Real-world actuarial case study

A health insurer may estimate the probability that rare high-cost diagnoses exceed a threshold in a year. The CLT lets actuaries approximate portfolio-level probabilities without calculating every binomial term.

### Exam trap

For a discrete count approximated by a continuous normal distribution, use continuity correction when appropriate.

## 9. Geometric Distribution

### Core idea

If each trial has success probability `p`, the probability the first success occurs on trial `k` is:

```text
P(X=k) = (1-p)^(k-1) p
```

For first 6 on the third die roll:

```text
P(X=3) = (5/6)^2 * (1/6) = 25/216
```

### Real-world actuarial case study

The geometric distribution can model time until first claim, first lapse, first missed premium, or first fraud flag in repeated independent periods.

### Exam trap

The first `k-1` attempts must fail, and the `k`th attempt must succeed.

## 10. Canonical Links in GLMs

### Core idea

Common canonical links:

```text
Poisson  -> log link
Gamma    -> inverse link
Binomial -> logit link
Normal   -> identity link
```

### Real-world actuarial case study

In non-life insurance:

- Claim frequency is often Poisson with log link.
- Claim severity may be Gamma with log or inverse link.
- Claim occurrence or fraud indicator may be Binomial with logit link.

### Exam trap

The most commonly used link is not always the canonical link. For example, Gamma models often use log links in practice, but the canonical link is inverse.

## 11. Compound Poisson Risk Premium

### Core idea

If:

```text
N ~ Poisson(lambda)
Claim amounts X_i have mean m and variance s^2
S = X_1 + ... + X_N
```

then:

```text
E[S] = lambda m
Var(S) = lambda (s^2 + m^2)
```

For the cyber risk example:

```text
E[S] = 2.7 * 7350 = 19,845
SD(S) = sqrt(2.7 * (5120^2 + 7350^2)) = 14,718.68
Premium = E[S] + 30% * SD(S)
        = 19,845 + 0.30 * 14,718.68
        = 24,260.60
```

### Real-world actuarial case study

Cyber insurance pricing often uses frequency-severity models. Expected loss covers the average cost, while a margin based on standard deviation compensates for uncertainty and capital strain.

### Exam trap

For compound Poisson variance, use `E[X^2] = Var(X) + E[X]^2`, not only severity variance.

## 12. Normal Spread and Tail Probability

### Core idea

For normal variables with the same mean, the variable with the largest variance has the widest curve. If the threshold is above the common mean, the widest distribution has the largest right-tail probability.

So if:

```text
sigma_J > sigma_B > sigma_S
```

then:

```text
P(X_J > 6) > P(X_B > 6) > P(X_S > 6)
```

### Real-world actuarial case study

Two portfolios may have the same average claim cost, but the portfolio with higher volatility has a higher chance of extreme claim outcomes.

### Exam trap

Equal means do not imply equal tail probabilities. Variance drives tail thickness around the mean.

## 13. Bayesian Credibility with Beta-Binomial

### Core idea

For a binomial likelihood with Beta prior:

```text
theta ~ Beta(alpha, beta)
x | theta ~ Binomial(n, theta)
theta | x ~ Beta(alpha + x, beta + n - x)
```

The credibility factor can be written as:

```text
Z = n / (n + alpha + beta)
```

For `alpha = 2`, `beta = 4`, `n = 12`:

```text
Z = 12 / (12 + 6) = 0.6667
```

### Real-world actuarial case study

In experience rating, an insurer blends a policyholder's own claims experience with portfolio-level experience. More observations means higher credibility for the individual's data.

### Exam trap

Credibility increases with `n`, but the prior sample size `alpha + beta` still pulls the result toward the prior mean.

## 14. Marginal Density from Joint Density

### Core idea

To find the marginal density of `X`, integrate out the other variable:

```text
f_X(x) = integral f_X,P(x,p) dp
```

For:

```text
f_X,P(x,p) = (x+p)/300, 0 <= p <= 20
```

```text
f_X(x) = integral_0^20 (x+p)/300 dp
       = (20x + 200)/300
       = (x + 10)/15
```

### Real-world actuarial case study

Joint modelling may include claim count and claim size. If the actuary only needs the claim count distribution, the claim size variable is integrated out.

### Exam trap

For continuous variables, marginalisation uses integration. For discrete variables, it uses summation.

## 15. Bayesian Exponential-Gamma Model

### Core idea

If inter-arrival times are exponential:

```text
t_i | lambda ~ Exponential(lambda)
L(lambda) = lambda^n exp(-lambda * sum t_i)
```

With Gamma prior using rate parameterisation:

```text
lambda ~ Gamma(alpha, beta)
prior proportional to lambda^(alpha-1) exp(-beta lambda)
```

Posterior:

```text
lambda | t ~ Gamma(alpha + n, beta + sum t_i)
```

Under quadratic loss, the Bayesian estimate is the posterior mean:

```text
E[lambda | t] = (alpha + n) / (beta + sum t_i)
```

Under absolute error loss, the Bayesian estimate is the posterior median.

Under all-or-nothing / 0-1 style loss, the estimate is the posterior mode.

### Real-world actuarial case study

An operational risk team may model the time between cyber events or system outages. A Gamma prior captures previous belief about event frequency, and new data updates that belief.

### Exam trap

Be clear whether the Gamma distribution is parameterised by rate or scale. The paper uses the rate form because the posterior rate is `beta + sum t_i`.

## 16. Poisson Fit, Overdispersion, and Bootstrapping

### Core idea

For a Poisson distribution:

```text
E[X] = Var(X) = lambda
```

From grouped data, using `>=5` as 5 for approximation:

```text
sample mean = 0.632
sample variance = approximately 0.750
```

Method of moments estimate:

```text
lambda_hat = sample mean = 0.632
```

Expected frequencies under Poisson:

```text
0 passengers: 265.76
1 passenger : 167.96
2 passengers: 53.08
3 passengers: 11.18
4 passengers: 1.77
>=5        : 0.25
```

Observed frequencies:

```text
0: 280, 1: 150, 2: 50, 3: 15, 4: 4, >=5: 1
```

Bootstrap average frequencies from the five samples:

```text
0: 280.0
1: 150.0
2: 50.0
3: 14.6
4: 4.2
>=5: 1.2
```

### Real-world actuarial case study

For claim counts per policy, a Poisson model may understate variability if claims are clustered. Bootstrapping can preserve the empirical shape of the observed data without forcing a Poisson assumption.

### Exam trap

If sample variance is noticeably larger than sample mean, check for overdispersion. A plain Poisson model may be too restrictive.

## 17. Simple Linear Regression and ANOVA

### Core idea

For simple regression:

```text
y_hat = a + bx
b = S_xy / S_xx
a = y_bar - b x_bar
```

Using the provided sums in Q18:

```text
S_xx = 2,310
S_yy = 22.5
S_xy = -225

b = -225 / 2310 = -0.0974
a = 11

y_hat = 11 - 0.0974x
```

ANOVA test for slope:

```text
SSR = S_xy^2 / S_xx = 21.916
SSE = S_yy - SSR = 0.584
F = (SSR / 1) / (SSE / (n - 2)) = 300
```

Coefficient of determination:

```text
R^2 = SSR / S_yy = 0.9740
```

### Real-world actuarial case study

An actuary may test whether risk-control training reduces incident frequency. A negative slope means more training is associated with fewer incidents. The ANOVA F-test checks whether this relationship is statistically meaningful.

### Exam trap

High `R^2` does not prove causation. It only says the fitted model explains a large portion of sample variation.

Note: The residual for a named observation requires that observation's actual `x` and `y` values. That part of the PDF appears image/table-based in extraction.

## 18. Confidence Interval vs Prediction Interval

### Core idea

A confidence interval estimates the population mean. A prediction interval estimates a future individual observation.

For the giant wheel:

```text
original mean maximum height = 22 + 19 = 41
n = 300
s = 3
```

95% confidence interval for mean:

```text
41 +/- 1.96 * 3 / sqrt(300)
= 41 +/- 0.34
= (40.66, 41.34)
```

95% prediction interval for next ride:

```text
41 +/- 1.96 * 3 * sqrt(1 + 1/300)
approximately 41 +/- 5.89
= (35.11, 46.89)
```

The prediction interval is wider because it includes both uncertainty in the estimated mean and randomness of the next ride.

For the restructured wheel:

```text
mean = 35
s = 2
P(X > 36) = P(Z > (36 - 35)/2)
          = P(Z > 0.5)
          = 0.3085
```

Difference in means:

```text
original mean - restructured mean = 41 - 35 = 6
SE = sqrt(3^2/300 + 2^2/300) = 0.208
CI = 6 +/- 1.96 * 0.208
   = (5.59, 6.41)
```

### Real-world actuarial case study

In reserving, a confidence interval may estimate the average claim size. A prediction interval is needed when forecasting the next claim, because the next claim has its own volatility.

### Exam trap

Prediction intervals are wider than confidence intervals. If they are not, something is wrong.

## 19. GLM Factor Variables, Interactions, and Deviance

### Core idea

Factor variables are categorical variables. In Q20:

```text
MRT = marital status: factor with 4 levels, so 3 parameters after base level
EMP = employment status: factor with 5 levels, so 4 parameters after base level
```

Continuous variables:

```text
REM, DPM, MDT
```

Parameter counts:

```text
Model 1: REM + DPM
1 intercept + 2 continuous = 3

Model 2: REM + DPM + MDT + MRT
1 + 3 continuous + 3 MRT dummies = 7

Model 3: REM + DPM + MDT + MRT + MDT:MRT
7 + 3 interaction terms = 10

Model 4: REM + DPM + MDT + MRT + EMP + MDT:MRT + MRT:EMP + MDT:EMP
1 + 3 continuous + 3 MRT + 4 EMP + 3 + 12 + 4 = 30

Model 5: MRT*EMP*REM
1 + MRT(3) + EMP(4) + REM(1)
+ MRT:EMP(12) + MRT:REM(3) + EMP:REM(4)
+ MRT:EMP:REM(12)
= 40
```

For nested models, if adding complexity does not significantly improve fit, choose the simpler adequate model. From the deviance table, Model 4 is the practical choice because Model 5's extra improvement is not significant at 5%.

### Real-world actuarial case study

A health actuary may model mental health score or claim cost using categorical variables like marital status and employment. Interactions allow the effect of mindfulness hours to differ by marital or employment group.

### Exam trap

`A:B` means interaction only. `A*B` means main effects plus interaction.

## 20. Classification, Type I/II Errors, and Sensitivity

### Core idea

In fraud detection:

```text
Type I error  = genuine claim wrongly flagged as fraud
Type II error = fraudulent claim wrongly cleared as genuine
```

For 10,000 claims with 8% fraud:

```text
actual fraud = 800
actual genuine = 9,200

sensitivity = 85%, so true positives = 0.85 * 800 = 680
specificity = 90%, so false positives = 10% * 9,200 = 920
```

Testing sensitivity:

```text
H0: p = 0.80
H1: p > 0.80

observed true positives = 680 out of 800
expected true positives under H0 = 640
expected false negatives under H0 = 160

chi-square = (680-640)^2/640 + (120-160)^2/160
           = 12.5
```

At 1% significance with 1 degree of freedom, critical value is about 6.635. Since `12.5 > 6.635`, reject `H0`. The sensitivity is significantly greater than 80%.

95% confidence interval:

```text
p_hat = 680/800 = 0.85
SE = sqrt(0.85 * 0.15 / 800) = 0.0126
CI = 0.85 +/- 1.96 * 0.0126
   = (0.825, 0.875)
```

This supports the claim that sensitivity exceeds 80%, because the lower bound is above 80%.

### Real-world actuarial case study

Fraud systems reduce leakage but can create friction. False positives increase investigation cost and customer dissatisfaction. False negatives allow fraud losses through. The actuarial decision is not just statistical; it is economic.

### Exam trap

Sensitivity is calculated only among actual fraud cases. Specificity is calculated only among actual genuine cases.

## 21. Dataset Type, Correlation, Kendall's Tau, and Outliers

### Core idea

If data are collected once across 10 spacecraft missions, the dataset is cross-sectional.

Karl Pearson correlation:

```text
r = [n sum xy - sum x sum y] /
    sqrt([n sum x^2 - (sum x)^2][n sum y^2 - (sum y)^2])
```

Using Q22's sums:

```text
r = 0.1145
```

This is a weak positive linear relationship between launch mass and travel time.

Kendall's correlation:

```text
concordant pair: higher value of one variable comes with higher value of the other
discordant pair: higher value of one variable comes with lower value of the other
```

If discordant pairs exceed concordant pairs, Kendall's tau is negative.

### Real-world actuarial case study

Correlation is used in capital modelling. If two risk drivers are strongly positively correlated, diversification benefit is lower. If correlation is weak, combining them may reduce total portfolio volatility.

### Exam trap

Pearson correlation is sensitive to outliers because it uses actual magnitudes. Kendall's tau uses ranks, so it is usually more robust.

## Quick Answer Map for Complete Extracted Questions

```text
Q1  B
Q2  E
Q3  C
Q4  D
Q5  B
Q6  D
Q7  A
Q8  C
Q9  E
Q10 B
Q11 B
Q13 D
Q14 A
Q15 E
```

Q12, parts of Q16, the residual in Q18, and the full dataset table in Q22 need the image-only details from the PDF or the official solution before finalising.

