# CS1A Actuarial Statistics: Expanded Theory, Derivations, and Practical Examples

**Author:** Amu chaps

## How This Expanded Edition Is Organised

The original notes have been rearranged so related topics sit together in larger master chapters. Each topic keeps its core theory, derivations, examples, mistakes, and revision checkpoint, then adds deeper explanation, a step-by-step method, practical actuarial examples, a study routine, and an exam answer structure.

This ebook is designed for CS1A revision, actuarial exam preparation, and practical modelling intuition. It is not a replacement for the official syllabus or examiner reports; use it as a structured companion.

This is the main study format to use for every CS1A topic.

Each topic follows this structure:

```text
1. Concept theory
2. Why actuaries care
3. Mathematical derivation
4. Simple example
5. Exam-style case study
6. Real-world actuarial case study
7. Common mistakes
8. Revision checkpoint
```

## Master Chapter 1: Probability Foundations and Random Variables

Treat every probability question as a model of uncertainty. Define the sample space, identify the random variable, write the probability law, then decide whether new information changes the population being averaged.

### Topics in this master chapter

- Topic 1: Random Variables and Distributions
- Topic 2: Conditional Probability and Conditional Expectation
- Topic 3: Chi-Square Tests: Independence and Goodness of Fit
- Topic 4: Variance of Linear Combinations and Law of Total Variance
- Topic 5: Minimum of Geometric Random Variables
- Topic 6: Conditional Independence vs Unconditional Independence
- Topic 7: Censored or Grouped Observations and Likelihood
- Topic 8: Principal Components and the Variance-Covariance Matrix
- Topic 9: Joint Density on a Triangular Region and Conditional Probability
- Topic 10: Discrete Bayesian Updating for Coin Tosses and Independence Testing
- Topic 11: Linear Combinations of Independent Normal Claim Amounts
- Topic 12: Independent Exponential Joint Density, Normalising Constant, and Marginals
- Topic 13: Joint PMF, Covariance, and Variance of a Sum

### Topic 1: Random Variables and Distributions

#### 1. Concept theory

A random variable converts an uncertain event into a number.

Examples:

```text
X = number of claims in one year
Y = amount of one claim
T = time until next claim
I = 1 if a claim is fraudulent, 0 otherwise
```

There are two main types:

```text
Discrete random variable:
Takes countable values, such as 0, 1, 2, 3.

Continuous random variable:
Takes values over an interval, such as claim size, lifetime, waiting time.
```

For discrete variables we use a probability mass function:

```text
P(X = x)
```

For continuous variables we use a probability density function:

```text
f(x)
```

For continuous variables:

```text
P(a < X < b) = integral from a to b of f(x) dx
```

#### 2. Why actuaries care

Actuaries do not price policies based on certainty. They price based on distributions.

A motor insurer needs:

```text
claim count distribution
claim size distribution
total claim cost distribution
```

The average helps with expected premium. The spread helps with solvency capital and risk margin.

#### 3. Mathematical derivation

For a discrete random variable:

```text
E[X] = sum x P(X=x)
E[X^2] = sum x^2 P(X=x)
Var(X) = E[X^2] - E[X]^2
```

For a continuous random variable:

```text
E[X] = integral x f(x) dx
E[X^2] = integral x^2 f(x) dx
Var(X) = E[X^2] - E[X]^2
```

The variance formula comes from:

```text
Var(X) = E[(X - mu)^2]
       = E[X^2 - 2mu X + mu^2]
       = E[X^2] - 2mu E[X] + mu^2
       = E[X^2] - 2mu^2 + mu^2
       = E[X^2] - mu^2
```

So:

```text
Var(X) = E[X^2] - E[X]^2
```

#### 4. Simple example

Suppose claim count `X` has:

```text
P(X=0) = 0.60
P(X=1) = 0.30
P(X=2) = 0.10
```

Expected claim count:

```text
E[X] = 0(0.60) + 1(0.30) + 2(0.10)
     = 0.50
```

Second moment:

```text
E[X^2] = 0^2(0.60) + 1^2(0.30) + 2^2(0.10)
       = 0.30 + 0.40
       = 0.70
```

Variance:

```text
Var(X) = 0.70 - 0.50^2
       = 0.45
```

#### 5. Exam-style case study

An online retailer records:

```text
A = number of orders
B = number of complaints
```

If the question asks:

```text
E[B | B >= 1]
```

then it is not asking for average complaints across all hours. It asks for average complaints only in hours where at least one complaint occurred.

Use:

```text
E[B | B >= 1] = sum b P(B=b | B>=1)
```

and:

```text
P(B=b | B>=1) = P(B=b) / P(B>=1), for b >= 1
```

#### 6. Real-world actuarial case study

A health insurer monitors hospital admissions:

```text
X = number of admissions per policyholder per year
Y = cost per admission
```

If the insurer only studies admitted policyholders, it is working with a conditional distribution:

```text
cost | admission occurred
```

This is different from the total policyholder population, where many policyholders have zero admissions.

#### 7. Common mistakes

- Treating density as probability.
- Forgetting to condition after receiving new information.
- Using variance as if it were standard deviation.
- Ignoring zero-claim policyholders in pricing.

#### 8. Revision checkpoint

You understand this chapter if you can explain the difference between:

```text
E[claim cost]
E[claim cost | claim occurred]
```

### Expanded deep explanation

Treat every probability question as a model of uncertainty. Define the sample space, identify the random variable, write the probability law, then decide whether new information changes the population being averaged.

- Begin by checking support. A count model lives on non-negative integers, a severity model often lives on positive real values, and a transformed model may live on the whole real line after taking logs. Many exam errors happen before calculation because the wrong support is chosen.
- Separate the probability law from its moments. The probability law tells you every probability statement; the mean and variance are summaries. In actuarial work a model with the correct mean but the wrong tail can still be dangerous because solvency questions are tail questions.
- Use generating functions when sums are involved. MGFs, CGFs, and PGFs turn convolutions into algebra, but only under the conditions where they exist and where independence assumptions are valid.

### Step-by-step working method

1. State the random variable and its support.
2. Write the probability mass function, density, or distribution function.
3. Identify parameters and their actuarial meaning.
4. Calculate the required moment, probability, percentile, or transform.
5. Check whether the answer is sensible using units, range, and limiting cases.

### Extra practical actuarial examples

- Claim count example: if a motor portfolio has many zero-claim policies and a few multi-claim policies, compare Poisson and negative binomial assumptions by checking whether sample variance is close to, below, or above the sample mean.
- Severity example: for small routine medical claims a lognormal or gamma model may be adequate; for catastrophe-like property claims a heavier tail such as Pareto or Weibull may be needed.
- Exam example: after finding a probability, ask whether the question wanted an unconditional probability or a conditional probability after observing a claim, survival, or selection event.

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

### Topic 2: Conditional Probability and Conditional Expectation

#### 1. Concept theory

Conditional probability updates the probability of an event after we know another event has occurred.

```text
P(A | B) = probability of A given that B has occurred
```

Conditional expectation updates the average after restricting the population.

```text
E[X | B] = expected value of X among cases where B occurred
```

#### 2. Why actuaries care

Actuarial questions are often conditional:

```text
Expected claim size given a claim occurred
Expected number of complaints given at least one complaint occurred
Expected fraud cost given claim is flagged
Expected mortality given impaired health status
```

Conditional thinking prevents mixing two different populations.

#### 3. Mathematical derivation

Start from:

```text
P(A | B) = P(A and B) / P(B)
```

For a discrete random variable:

```text
E[X | B] = sum x P(X=x | B)
```

Using the conditional probability definition:

```text
E[X | B] = sum x P(X=x and B) / P(B)
```

If the event `B` is `X >= 1`:

```text
E[X | X >= 1] = sum over x>=1 x P(X=x) / P(X>=1)
```

#### 4. Simple example

Let:

```text
P(X=0) = 0.70
P(X=1) = 0.20
P(X=2) = 0.10
```

Find:

```text
E[X | X >= 1]
```

First:

```text
P(X>=1) = 0.20 + 0.10 = 0.30
```

Then:

```text
E[X | X>=1] = [1(0.20) + 2(0.10)] / 0.30
             = 0.40 / 0.30
             = 1.333
```

#### 5. Exam-style case study

If a question says:

```text
Given that at least one delivery complaint occurred
```

you must restrict the denominator to those outcomes where complaints occurred.

This is why conditional expectation is often higher than unconditional expectation.

#### 6. Real-world actuarial case study

For a motor insurer:

```text
Average cost per policy = includes policies with no claims
Average cost per claim = only includes claims
```

If a pricing actuary accidentally uses average cost per claim as average cost per policy, the premium will be severely overstated.

#### 7. Common mistakes

- Calculating numerator correctly but forgetting denominator.
- Confusing `P(A | B)` with `P(B | A)`.
- Using unconditional probabilities after the question says "given that".

#### 8. Revision checkpoint

When you see the words "given that", immediately write:

```text
new denominator = probability of the given condition
```

### Expanded deep explanation

Treat every probability question as a model of uncertainty. Define the sample space, identify the random variable, write the probability law, then decide whether new information changes the population being averaged.

- Begin by checking support. A count model lives on non-negative integers, a severity model often lives on positive real values, and a transformed model may live on the whole real line after taking logs. Many exam errors happen before calculation because the wrong support is chosen.
- Separate the probability law from its moments. The probability law tells you every probability statement; the mean and variance are summaries. In actuarial work a model with the correct mean but the wrong tail can still be dangerous because solvency questions are tail questions.
- Use generating functions when sums are involved. MGFs, CGFs, and PGFs turn convolutions into algebra, but only under the conditions where they exist and where independence assumptions are valid.

### Step-by-step working method

1. State the random variable and its support.
2. Write the probability mass function, density, or distribution function.
3. Identify parameters and their actuarial meaning.
4. Calculate the required moment, probability, percentile, or transform.
5. Check whether the answer is sensible using units, range, and limiting cases.

### Extra practical actuarial examples

- Claim count example: if a motor portfolio has many zero-claim policies and a few multi-claim policies, compare Poisson and negative binomial assumptions by checking whether sample variance is close to, below, or above the sample mean.
- Severity example: for small routine medical claims a lognormal or gamma model may be adequate; for catastrophe-like property claims a heavier tail such as Pareto or Weibull may be needed.
- Exam example: after finding a probability, ask whether the question wanted an unconditional probability or a conditional probability after observing a claim, survival, or selection event.

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

### Topic 3: Chi-Square Tests: Independence and Goodness of Fit

#### 1. Concept theory

Chi-square tests compare observed counts with expected counts.

Two common forms:

```text
Test of independence:
Checks whether two categorical variables are associated.

Goodness-of-fit test:
Checks whether observed data fits a proposed distribution or model.
```

#### 2. Why actuaries care

Actuaries use chi-square tests to check:

```text
whether claim frequency differs by region
whether lapse behaviour differs by age group
whether fraud flags are associated with claim type
whether a fitted distribution is acceptable
```

#### 3. Mathematical derivation

The test statistic is:

```text
chi-square = sum (Observed - Expected)^2 / Expected
```

For independence in a contingency table:

```text
Expected cell count = row total * column total / grand total
```

Degrees of freedom:

```text
(number of rows - 1)(number of columns - 1)
```

For goodness of fit:

```text
degrees of freedom = number of cells - number of estimated parameters - 1
```

#### 4. Simple example

Suppose a 2 by 2 table has:

```text
rows = age group
columns = product preference
```

Under independence, each expected count is:

```text
row total * column total / grand total
```

Then calculate:

```text
sum (O-E)^2/E
```

If the test statistic is less than the critical value, we fail to reject independence.

#### 5. Exam-style case study

In the November 2025 solution, the chi-square statistic for age group versus preference was:

```text
chi-square = 0.423
df = 1
critical value at 5% = 3.841
```

Since:

```text
0.423 < 3.841
```

we fail to reject the null hypothesis. There is no significant evidence of association between age group and preference.

For the proposed behavioural model:

```text
chi-square = 542.34
df = 2
critical value at 5% = 5.991
```

Since:

```text
542.34 > 5.991
```

the model does not fit the observed data.

#### 6. Real-world actuarial case study

A life insurer may test whether policy lapse is independent of distribution channel. If lapse behaviour differs significantly between online and agent-sold policies, pricing and persistency assumptions should differ by channel.

#### 7. Common mistakes

- Using observed counts instead of expected counts in the denominator.
- Forgetting to reduce degrees of freedom for estimated parameters.
- Saying two variables are independent just because the observed percentages look close.

#### 8. Revision checkpoint

For every chi-square question, write:

```text
Observed table
Expected table
Test statistic
Degrees of freedom
Critical value
Conclusion in words
```

### Expanded deep explanation

Treat every probability question as a model of uncertainty. Define the sample space, identify the random variable, write the probability law, then decide whether new information changes the population being averaged.

- Inference is a disciplined way of deciding whether observed experience is consistent with an assumption. A confidence interval estimates a plausible range for a parameter; a hypothesis test checks whether the data are surprising under a stated null hypothesis.
- Type I error is rejecting a true null hypothesis. Type II error is failing to reject a false null hypothesis. Power is the probability of detecting an effect when it is truly present.
- A prediction interval is wider than a confidence interval for a mean because it includes both uncertainty about the average and the randomness of a new observation.

### Step-by-step working method

1. Identify the parameter being estimated or tested.
2. Write the null and alternative hypotheses, if a test is required.
3. Choose the reference distribution and degrees of freedom.
4. Compute the statistic, p-value, interval, or critical region.
5. Finish with a plain-English actuarial conclusion.

### Extra practical actuarial examples

- Fraud model example: a higher observed detection rate is useful only if the improvement is statistically convincing and operationally valuable after false positives are considered.
- Claims inflation example: test whether average claim cost has changed before changing a pricing basis, but also review exposure mix and large claims.
- Reserve example: a confidence interval for mean settlement cost should not be confused with a prediction interval for a single future claim.

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

### Topic 4: Variance of Linear Combinations and Law of Total Variance

#### 1. Concept theory

When variables are combined, their variances do not simply add unless the variables are independent and have coefficient 1.

For:

```text
Z = aX + bY + c
```

the constant `c` does not affect variance.

#### 2. Why actuaries care

Insurance results combine many random items:

```text
profit = premium - claims - expenses
surplus = assets - liabilities
aggregate loss = frequency * severity
```

Dependencies matter. A negative covariance can reduce total volatility; a positive covariance can increase it.

#### 3. Mathematical derivation

For two variables:

```text
Var(aX + bY + c) = a^2 Var(X) + b^2 Var(Y) + 2ab Cov(X,Y)
```

For conditional models, use the law of total variance:

```text
Var(X) = E[Var(X | Y)] + Var(E[X | Y])
```

This separates:

```text
average conditional uncertainty
+ uncertainty due to the conditioning variable
```

#### 4. Simple example

Suppose:

```text
X ~ Poisson(11), so Var(X)=11
Y ~ Poisson(19), so Var(Y)=19
Cov(X,Y) = -24
Z = 2X - 3Y + 4
```

Then:

```text
Var(Z) = 2^2 Var(X) + (-3)^2 Var(Y) + 2(2)(-3)Cov(X,Y)
       = 4(11) + 9(19) - 12(-24)
       = 44 + 171 + 288
       = 503
```

#### 5. Exam-style case study

If:

```text
X | Y=y ~ Normal(y + 3, 0.01y^2 + 5)
Y ~ Normal(100, 60)
```

then:

```text
E[X | Y] = Y + 3
Var(X | Y) = 0.01Y^2 + 5
```

Use:

```text
Var(X) = E[0.01Y^2 + 5] + Var(Y + 3)
```

Since:

```text
E[Y^2] = Var(Y) + E[Y]^2 = 60 + 100^2 = 10,060
```

we get:

```text
E[0.01Y^2 + 5] = 0.01(10,060) + 5 = 105.6
Var(Y + 3) = Var(Y) = 60
Var(X) = 105.6 + 60 = 165.6
```

#### 6. Real-world actuarial case study

A health insurer may model claim cost `X` conditional on age `Y`. Even within a fixed age, claim costs vary. Across ages, expected claim costs also vary. Total variance includes both within-age variation and between-age variation.

#### 7. Common mistakes

- Forgetting the covariance term.
- Treating constants as adding variance.
- Using `E[Var(X|Y)]` but forgetting `Var(E[X|Y])`.

#### 8. Revision checkpoint

Remember:

```text
total variance = within-group variance + between-group variance
```

### Expanded deep explanation

Treat every probability question as a model of uncertainty. Define the sample space, identify the random variable, write the probability law, then decide whether new information changes the population being averaged.

- Begin by checking support. A count model lives on non-negative integers, a severity model often lives on positive real values, and a transformed model may live on the whole real line after taking logs. Many exam errors happen before calculation because the wrong support is chosen.
- Separate the probability law from its moments. The probability law tells you every probability statement; the mean and variance are summaries. In actuarial work a model with the correct mean but the wrong tail can still be dangerous because solvency questions are tail questions.
- Use generating functions when sums are involved. MGFs, CGFs, and PGFs turn convolutions into algebra, but only under the conditions where they exist and where independence assumptions are valid.

### Step-by-step working method

1. State the random variable and its support.
2. Write the probability mass function, density, or distribution function.
3. Identify parameters and their actuarial meaning.
4. Calculate the required moment, probability, percentile, or transform.
5. Check whether the answer is sensible using units, range, and limiting cases.

### Extra practical actuarial examples

- Claim count example: if a motor portfolio has many zero-claim policies and a few multi-claim policies, compare Poisson and negative binomial assumptions by checking whether sample variance is close to, below, or above the sample mean.
- Severity example: for small routine medical claims a lognormal or gamma model may be adequate; for catastrophe-like property claims a heavier tail such as Pareto or Weibull may be needed.
- Exam example: after finding a probability, ask whether the question wanted an unconditional probability or a conditional probability after observing a claim, survival, or selection event.

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

### Topic 5: Minimum of Geometric Random Variables

#### 1. Concept theory

A geometric random variable counts the number of trials needed to get the first success.

If:

```text
X ~ Geometric(p)
```

where `X = 1, 2, 3, ...`, then:

```text
P(X = x) = (1-p)^(x-1)p
P(X >= x) = (1-p)^(x-1)
E[X] = 1/p
Var(X) = (1-p)/p^2
```

If we have several independent geometric variables:

```text
X_1, X_2, ..., X_n
```

and:

```text
Y = min(X_1, X_2, ..., X_n)
```

then `Y` is the first time at least one of the chances succeeds.

#### 2. Why actuaries care

Minimum and maximum random variables appear in insurance and risk problems:

```text
time until first claim among several policies
time until first default in a loan portfolio
time until first system failure
time until first market circuit breaker
```

The minimum is useful when the first event triggers a payment, investigation, or operational response.

#### 3. Mathematical derivation

For one chance:

```text
P(X_i >= x) = (1-p)^(x-1)
```

For:

```text
Y = min(X_1, ..., X_n)
```

the event `Y >= y` means every `X_i` is at least `y`.

So:

```text
P(Y >= y) = P(X_1 >= y, ..., X_n >= y)
```

Using independence:

```text
P(Y >= y) = product P(X_i >= y)
          = [(1-p)^(y-1)]^n
          = (1-p)^(n(y-1))
```

This can also be written as:

```text
P(Y >= y) = [(1-p)^n]^(y-1)
```

#### 4. Simple example

Suppose each trial succeeds with:

```text
p = 1/6
```

and there are:

```text
n = 3 chances
```

The probability that the minimum number of throws is at least 4 is:

```text
P(Y >= 4) = (1-p)^(n(4-1))
          = (5/6)^9
```

This means all three chances failed to get success in their first three throws.

#### 5. Exam-style case study

In the "Power of Six" game, two dice are thrown until the sum is 6.

The ways to get sum 6 are:

```text
(1,5), (2,4), (3,3), (4,2), (5,1)
```

So:

```text
p = 5/36
```

For each chance:

```text
X_i ~ Geometric(5/36)
```

and:

```text
Var(X_i) = (1-p)/p^2
```

If there are 10 chances and the player loses money when reward is less than the entry charge, first translate the rupee condition into a condition on `Y`, then use:

```text
P(Y >= y) = (1-p)^(10(y-1))
```

#### 6. Real-world actuarial case study

A cyber insurer monitors 10 insured companies. Let `X_i` be the number of days until company `i` reports its first cyber incident. The insurer may care about:

```text
Y = min(X_1, ..., X_10)
```

because the first incident triggers emergency response staffing. Even if each company has low daily incident probability, the first incident across a portfolio may arrive much sooner.

#### 7. Common mistakes

- Using `P(X > x)` when the question asks `P(X >= x)`.
- Forgetting that geometric distributions may be defined differently in different textbooks.
- Treating the minimum as if only one chance matters.
- Forgetting independence when multiplying probabilities.

#### 8. Revision checkpoint

You should be able to derive:

```text
P(min(X_1,...,X_n) >= y)
```

from the survival probability of one geometric random variable.

### Expanded deep explanation

Treat every probability question as a model of uncertainty. Define the sample space, identify the random variable, write the probability law, then decide whether new information changes the population being averaged.

- Begin by checking support. A count model lives on non-negative integers, a severity model often lives on positive real values, and a transformed model may live on the whole real line after taking logs. Many exam errors happen before calculation because the wrong support is chosen.
- Separate the probability law from its moments. The probability law tells you every probability statement; the mean and variance are summaries. In actuarial work a model with the correct mean but the wrong tail can still be dangerous because solvency questions are tail questions.
- Use generating functions when sums are involved. MGFs, CGFs, and PGFs turn convolutions into algebra, but only under the conditions where they exist and where independence assumptions are valid.

### Step-by-step working method

1. State the random variable and its support.
2. Write the probability mass function, density, or distribution function.
3. Identify parameters and their actuarial meaning.
4. Calculate the required moment, probability, percentile, or transform.
5. Check whether the answer is sensible using units, range, and limiting cases.

### Extra practical actuarial examples

- Claim count example: if a motor portfolio has many zero-claim policies and a few multi-claim policies, compare Poisson and negative binomial assumptions by checking whether sample variance is close to, below, or above the sample mean.
- Severity example: for small routine medical claims a lognormal or gamma model may be adequate; for catastrophe-like property claims a heavier tail such as Pareto or Weibull may be needed.
- Exam example: after finding a probability, ask whether the question wanted an unconditional probability or a conditional probability after observing a claim, survival, or selection event.

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

### Topic 6: Conditional Independence vs Unconditional Independence

#### 1. Concept theory

Two variables `N` and `G` are independent if knowing one does not change the distribution of the other.

Unconditional independence:

```text
P(N=n, G=g) = P(N=n)P(G=g)
```

Conditional independence given `E`:

```text
P(N=n, G=g | E=e) = P(N=n | E=e)P(G=g | E=e)
```

Important idea:

```text
Variables can be conditionally independent given E,
but not independent overall.
```

This happens when the conditioning variable changes the mixture of groups.

#### 2. Why actuaries care

This is a major pricing and risk-classification issue.

Examples:

```text
claim frequency and age may look dependent overall
but become independent after controlling for occupation

health cost and region may look dependent overall
but become independent after controlling for age
```

If an actuary ignores the conditioning variable, they may find misleading relationships.

#### 3. Mathematical derivation

To test unconditional independence, compute marginal probabilities:

```text
P(N=n) = sum_g P(N=n, G=g)
P(G=g) = sum_n P(N=n, G=g)
```

Then check whether:

```text
P(N=n, G=g) = P(N=n)P(G=g)
```

for every cell.

To test conditional independence given `E=e`, use the conditional table for that `E` and check:

```text
P(N=n, G=g | E=e)
= P(N=n | E=e)P(G=g | E=e)
```

for every cell.

#### 4. Simple example

Suppose within employed individuals:

```text
P(N=0, Gen X | E) = 0.175
P(N=0 | E) = 0.350
P(Gen X | E) = 0.500
```

Then:

```text
P(N=0 | E)P(Gen X | E) = 0.350 * 0.500 = 0.175
```

This cell supports conditional independence.

If all cells satisfy this, then `N` and `G` are conditionally independent for employed individuals.

#### 5. Exam-style case study

In the supermarket example, the actuary has:

```text
N = number of supermarket visits
G = age group
E = employment type
```

The employment-specific tables can show conditional independence:

```text
N independent of G given E = employed
N independent of G given E = self-employed
```

But the original overall table may still show dependence between `N` and `G`.

So the correct conceptual conclusion can be:

```text
N and G are conditionally independent given E,
but not unconditionally independent.
```

#### 6. Real-world actuarial case study

In motor insurance, claim frequency and vehicle type may look strongly related. But after conditioning on driver age, part of that relationship may disappear because younger drivers prefer certain vehicles.

This matters because pricing should avoid double-counting the same risk signal through multiple variables.

#### 7. Common mistakes

- Assuming conditional independence implies unconditional independence.
- Checking only one cell and declaring independence.
- Forgetting to use conditional probabilities inside each `E` group.
- Mixing row percentages and joint probabilities.

#### 8. Revision checkpoint

You should be able to explain:

```text
independent overall
independent after conditioning
why these are not the same thing
```

### Expanded deep explanation

Treat every probability question as a model of uncertainty. Define the sample space, identify the random variable, write the probability law, then decide whether new information changes the population being averaged.

- Begin by checking support. A count model lives on non-negative integers, a severity model often lives on positive real values, and a transformed model may live on the whole real line after taking logs. Many exam errors happen before calculation because the wrong support is chosen.
- Separate the probability law from its moments. The probability law tells you every probability statement; the mean and variance are summaries. In actuarial work a model with the correct mean but the wrong tail can still be dangerous because solvency questions are tail questions.
- Use generating functions when sums are involved. MGFs, CGFs, and PGFs turn convolutions into algebra, but only under the conditions where they exist and where independence assumptions are valid.

### Step-by-step working method

1. State the random variable and its support.
2. Write the probability mass function, density, or distribution function.
3. Identify parameters and their actuarial meaning.
4. Calculate the required moment, probability, percentile, or transform.
5. Check whether the answer is sensible using units, range, and limiting cases.

### Extra practical actuarial examples

- Claim count example: if a motor portfolio has many zero-claim policies and a few multi-claim policies, compare Poisson and negative binomial assumptions by checking whether sample variance is close to, below, or above the sample mean.
- Severity example: for small routine medical claims a lognormal or gamma model may be adequate; for catastrophe-like property claims a heavier tail such as Pareto or Weibull may be needed.
- Exam example: after finding a probability, ask whether the question wanted an unconditional probability or a conditional probability after observing a claim, survival, or selection event.

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

### Topic 7: Censored or Grouped Observations and Likelihood

#### 1. Concept theory

Sometimes we do not observe exact values. We only observe whether the value falls in a category.

Examples:

```text
claim amount > deductible
policyholder survived beyond age 80
swimmer can or cannot swim underwater
loan default occurred or did not occur
```

This is grouped or censored information. The likelihood must be written using the probability of what was observed, not the density of exact unobserved values.

#### 2. Why actuaries care

Actuarial data is often incomplete:

```text
right-censored survival times
claims grouped into bands
deductible-truncated claim amounts
default/non-default indicators
```

Correct likelihood construction is essential for reliable estimation.

#### 3. Mathematical derivation

For exact exponential observations:

```text
X_i ~ Exponential(lambda)
f(x_i) = lambda e^(-lambda x_i)
```

Likelihood:

```text
L(lambda) = product lambda e^(-lambda x_i)
          = lambda^n e^(-lambda sum x_i)
```

Log-likelihood:

```text
l(lambda) = n log(lambda) - lambda sum x_i
```

Differentiate:

```text
dl/dlambda = n/lambda - sum x_i
```

Set equal to zero:

```text
lambda_hat = n / sum x_i
```

For grouped Poisson-style observation where we only know:

```text
m observations are zero
n-m observations are greater than zero
```

and:

```text
X ~ Poisson(lambda)
```

then:

```text
P(X=0) = e^(-lambda)
P(X>0) = 1 - e^(-lambda)
```

Likelihood:

```text
L(lambda) = [e^(-lambda)]^m [1 - e^(-lambda)]^(n-m)
```

#### 4. Simple example

If 4 swimmers out of 10 cannot swim underwater and the remaining 6 can, under the grouped Poisson setup:

```text
m = 4
n-m = 6
```

Likelihood:

```text
L(lambda) = e^(-4lambda)(1 - e^(-lambda))^6
```

The MLE from this grouped likelihood satisfies:

```text
e^(-lambda_hat) = m/n
```

So:

```text
lambda_hat = log(n/m)
```

This is finite only if:

```text
m > 0
```

#### 5. Exam-style case study

For the underwater-swimmer question:

Exact observations:

```text
lambda_hat = n / sum x_i
CRLB = lambda^2 / n
```

Grouped observations:

```text
L(lambda) = e^(-lambda m)(1 - e^(-lambda))^(n-m)
lambda_hat = log(n/m)
```

At least one zero observation is needed for the MLE to be finite.

#### 6. Real-world actuarial case study

In life insurance, we may not observe exact lifetime for every policyholder. If a policyholder is still alive at the end of the study, their lifetime is censored. The likelihood must reflect "survived beyond time t", not pretend the death occurred at time `t`.

#### 7. Common mistakes

- Using exact-data likelihood when only grouped data is observed.
- Forgetting that `P(X>0) = 1 - P(X=0)`.
- Producing an infinite MLE without checking boundary cases.
- Treating censored observations as missing data to be ignored.

#### 8. Revision checkpoint

You should be able to write likelihoods for:

```text
exact observation
zero vs positive observation
greater-than threshold observation
```

### Expanded deep explanation

Treat every probability question as a model of uncertainty. Define the sample space, identify the random variable, write the probability law, then decide whether new information changes the population being averaged.

- An estimator is a rule for turning data into a parameter estimate. Different estimators can have different bias, variance, mean squared error, and robustness.
- Maximum likelihood chooses the parameter that makes the observed data most probable under the model. Method of moments matches sample moments to theoretical moments.
- Large-sample theory can give approximate intervals, but boundary estimates, censored observations, small samples, and heavy tails can make approximations unreliable.

### Step-by-step working method

1. Write the model and parameter space.
2. Choose MOM, MLE, least squares, or another estimator.
3. Derive or solve the estimating equation.
4. Check bias, variance, consistency, and practical stability where possible.
5. Interpret the estimate in actuarial units.

### Extra practical actuarial examples

- Severity example: estimating a Pareto tail parameter from a few large losses may be highly unstable, so sensitivity testing is essential.
- Frequency example: the Poisson MLE for a constant rate is total claims divided by total exposure.
- Simulation example: inverse transform simulation uses uniform random numbers to generate losses from a fitted distribution for stress testing.

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

### Topic 8: Principal Components and the Variance-Covariance Matrix

#### 1. Concept theory

Principal component analysis transforms correlated variables into new variables called principal components.

The principal components are linear combinations of the original variables, and they are uncorrelated with each other.

This means the covariance between different principal components is zero.

#### 2. Why actuaries care

PCA is used when there are many correlated risk variables, such as economic indicators, stock returns, yield curve points, claim characteristics, or demographic factors.

It helps reduce dimension while retaining most of the variation in the data.

#### 3. Mathematical derivation

Let the original covariance matrix be:

```text
Sigma
```

PCA finds new variables:

```text
Z1, Z2, ..., Zp
```

where each `Zi` is a linear combination of the original variables.

The covariance matrix of the principal components is diagonal:

```text
Cov(Z) =
[lambda1   0       0
 0       lambda2   0
 0         0     lambda3]
```

The diagonal values are variances of the principal components. The off-diagonal values are covariances, and they are zero.

For `p` principal components, the covariance matrix is `p x p`, but the maximum number of non-zero entries is:

```text
p
```

because only the diagonal entries can be non-zero.

#### 4. Simple example

For 3 principal components, the covariance matrix may be:

```text
[8  0  0
 0  3  0
 0  0  1]
```

There are 9 entries in the matrix, but only 3 non-zero entries.

#### 5. Exam-style case study

In the November 2023 solution, a 30 by 30 covariance matrix is built for 30 principal components.

Total entries:

```text
30 x 30 = 900
```

But principal components are uncorrelated, so all off-diagonal covariances are zero.

Only the diagonal variances can be non-zero:

```text
maximum non-zero entries = 30
```

#### 6. Real-world actuarial case study

An investment actuary studies returns from 30 stocks in an index. Many stock returns move together because of common market factors.

PCA can transform the 30 stock returns into principal components such as market level, sector rotation, and residual stock-specific variation. The components are uncorrelated, making the risk structure easier to analyse.

#### 7. Common mistakes

- Thinking a 30 by 30 PCA covariance matrix must have 900 non-zero entries.
- Forgetting that principal components are uncorrelated.
- Confusing uncorrelated with independent.
- Assuming every principal component is equally important.
- Forgetting that diagonal entries are variances, not covariances between different components.

#### 8. Revision checkpoint

Without notes, you should be able to explain why a `p x p` principal-component covariance matrix has at most:

```text
p non-zero entries
```

and why those entries lie on the diagonal.

### Expanded deep explanation

Treat every probability question as a model of uncertainty. Define the sample space, identify the random variable, write the probability law, then decide whether new information changes the population being averaged.

- Begin by checking support. A count model lives on non-negative integers, a severity model often lives on positive real values, and a transformed model may live on the whole real line after taking logs. Many exam errors happen before calculation because the wrong support is chosen.
- Separate the probability law from its moments. The probability law tells you every probability statement; the mean and variance are summaries. In actuarial work a model with the correct mean but the wrong tail can still be dangerous because solvency questions are tail questions.
- Use generating functions when sums are involved. MGFs, CGFs, and PGFs turn convolutions into algebra, but only under the conditions where they exist and where independence assumptions are valid.

### Step-by-step working method

1. State the random variable and its support.
2. Write the probability mass function, density, or distribution function.
3. Identify parameters and their actuarial meaning.
4. Calculate the required moment, probability, percentile, or transform.
5. Check whether the answer is sensible using units, range, and limiting cases.

### Extra practical actuarial examples

- Claim count example: if a motor portfolio has many zero-claim policies and a few multi-claim policies, compare Poisson and negative binomial assumptions by checking whether sample variance is close to, below, or above the sample mean.
- Severity example: for small routine medical claims a lognormal or gamma model may be adequate; for catastrophe-like property claims a heavier tail such as Pareto or Weibull may be needed.
- Exam example: after finding a probability, ask whether the question wanted an unconditional probability or a conditional probability after observing a claim, survival, or selection event.

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

### Topic 9: Joint Density on a Triangular Region and Conditional Probability

#### 1. Concept theory

A joint density must be defined over a valid region. Sometimes the region is not a rectangle.

In the group term and rider example, an employee can only take the accidental death rider if they first take the basic group term cover. Therefore:

```text
0 <= Y <= X <= 1
```

The support is triangular.

#### 2. Why actuaries care

Actuarial variables are often constrained. For example:

- rider take-up cannot exceed base policy take-up
- expenses cannot exceed total revenue in some models
- claim paid cannot exceed claim incurred after limits
- recovery amount cannot exceed gross claim

Ignoring the correct support gives wrong probabilities.

#### 3. Mathematical derivation

Suppose:

```text
f(x,y) = 2(x + y), 0 <= y <= x <= 1
```

Marginal density of `X`:

```text
fX(x) = integral from y = 0 to x of 2(x + y) dy
```

```text
= [2xy + y^2] from 0 to x
= 2x^2 + x^2
= 3x^2
```

So:

```text
fX(x) = 3x^2, 0 <= x <= 1
```

Conditional density:

```text
fY|X(y | x) = f(x,y) / fX(x)
            = 2(x + y) / (3x^2), 0 <= y <= x
```

Conditional probability:

```text
P(Y < a | X = x) = integral from 0 to a of fY|X(y | x) dy
```

provided `0 <= a <= x`.

#### 4. Simple example

If:

```text
x = 0.2
a = 0.1
```

then:

```text
P(Y < 0.1 | X = 0.2)
= integral 0 to 0.1 of 2(0.2 + y) / [3(0.2)^2] dy
```

```text
= [2(0.2)y + y^2] from 0 to 0.1 / 0.12
= (0.04 + 0.01) / 0.12
= 0.4167
```

#### 5. Exam-style case study

In the December 2022 question, if:

```text
X = 0.10
```

and we need:

```text
P(Y < 0.05 | X = 0.10)
```

then:

```text
fX(0.10) = 3(0.10)^2 = 0.03
```

and:

```text
P(Y < 0.05 | X = 0.10)
= integral 0 to 0.05 of 2(0.10 + y) / 0.03 dy
```

```text
= [2(0.10)y + y^2] from 0 to 0.05 / 0.03
= (0.01 + 0.0025) / 0.03
= 0.4167
```

#### 6. Real-world actuarial case study

A company offers a base life cover and an optional accidental death rider. Rider take-up cannot exceed base cover take-up.

When modelling these proportions jointly, the actuary must use a triangular support. A rectangular support would allow impossible outcomes, such as more people having the rider than having the base policy.

#### 7. Common mistakes

- Using bounds `0 <= x <= 1`, `0 <= y <= 1` and forgetting `y <= x`.
- Integrating over the wrong variable for the marginal density.
- Forgetting to divide by `fX(x)` for conditional density.
- Treating `P(Y < a | X = x)` as an unconditional probability.
- Using `a > x` without adjusting the upper bound.

#### 8. Revision checkpoint

Without notes, you should be able to set up:

```text
fX(x) = integral f(x,y) dy
fY|X(y | x) = f(x,y) / fX(x)
P(Y < a | X = x) = integral fY|X(y | x) dy
```

with correct triangular bounds.

### Expanded deep explanation

Treat every probability question as a model of uncertainty. Define the sample space, identify the random variable, write the probability law, then decide whether new information changes the population being averaged.

- Begin by checking support. A count model lives on non-negative integers, a severity model often lives on positive real values, and a transformed model may live on the whole real line after taking logs. Many exam errors happen before calculation because the wrong support is chosen.
- Separate the probability law from its moments. The probability law tells you every probability statement; the mean and variance are summaries. In actuarial work a model with the correct mean but the wrong tail can still be dangerous because solvency questions are tail questions.
- Use generating functions when sums are involved. MGFs, CGFs, and PGFs turn convolutions into algebra, but only under the conditions where they exist and where independence assumptions are valid.

### Step-by-step working method

1. State the random variable and its support.
2. Write the probability mass function, density, or distribution function.
3. Identify parameters and their actuarial meaning.
4. Calculate the required moment, probability, percentile, or transform.
5. Check whether the answer is sensible using units, range, and limiting cases.

### Extra practical actuarial examples

- Claim count example: if a motor portfolio has many zero-claim policies and a few multi-claim policies, compare Poisson and negative binomial assumptions by checking whether sample variance is close to, below, or above the sample mean.
- Severity example: for small routine medical claims a lognormal or gamma model may be adequate; for catastrophe-like property claims a heavier tail such as Pareto or Weibull may be needed.
- Exam example: after finding a probability, ask whether the question wanted an unconditional probability or a conditional probability after observing a claim, survival, or selection event.

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

### Topic 10: Discrete Bayesian Updating for Coin Tosses and Independence Testing

#### 1. Concept theory

Bayesian updating can be done with a discrete prior. Each possible parameter value receives a prior probability, then the likelihood from the data updates those probabilities.

Coin tosses also provide a simple way to test independence using a contingency table of current toss versus previous toss.

#### 2. Why actuaries care

Discrete Bayesian updating is useful when judgement is expressed through a few possible scenarios. Independence testing matters in claims, lapses, fraud flags, and operational events.

#### 3. Mathematical derivation

For coin tosses:

```text
theta = P(head)
```

If the observed data contain:

```text
h heads and t tails
```

then the likelihood is:

```text
L(theta) = theta^h (1 - theta)^t
```

With discrete prior values:

```text
theta_1, theta_2, ..., theta_m
```

posterior probability is:

```text
P(theta_j | data) =
prior(theta_j)L(theta_j) / sum prior(theta_k)L(theta_k)
```

Posterior mean:

```text
E[theta | data] = sum theta_j P(theta_j | data)
```

For independence in a contingency table:

```text
Expected count = row total x column total / grand total
X^2 = sum (O - E)^2 / E
```

#### 4. Simple example

Prior:

```text
theta = 0.25 or 0.75, each with probability 0.5
```

Data:

```text
3 heads, 1 tail
```

Likelihoods:

```text
L(0.25) = 0.25^3 x 0.75
L(0.75) = 0.75^3 x 0.25
```

The posterior gives more weight to `theta = 0.75` because heads dominated the data.

#### 5. Exam-style case study

In July 2022:

```text
observed first 5 tosses = H,H,H,H,T
```

Likelihood:

```text
L(theta) = theta^4(1 - theta)
```

MLE:

```text
theta_hat = 4 / 5 = 0.8
```

Under:

```text
H0: theta = 0.5
```

probability of the exact sequence is:

```text
(0.5)^5
```

For p-value of four or more heads:

```text
P(X >= 4), X ~ Binomial(5, 0.5)
```

For independence of tosses using the 2 by 2 table, calculate expected counts and use chi-square. If expected counts are too small, the chi-square test is not appropriate.

#### 6. Real-world actuarial case study

An insurer tests whether claim occurrence in one month is independent of claim occurrence in the previous month. A contingency table can reveal clustering.

If claims are not independent, pricing and reserving models based on independent claim indicators may understate risk.

#### 7. Common mistakes

- Including the binomial coefficient when likelihood is for one exact observed sequence.
- Forgetting to normalise posterior probabilities.
- Using chi-square when expected counts are too small.
- Confusing posterior mean with MLE.
- Treating independence as automatic because tosses are from the same coin.

#### 8. Revision checkpoint

Without notes, you should be able to write:

```text
L(theta) = theta^h(1 - theta)^t
posterior proportional to prior x likelihood
posterior mean = sum theta x posterior probability
```

and set up a 2 by 2 chi-square independence test.

### Expanded deep explanation

Treat every probability question as a model of uncertainty. Define the sample space, identify the random variable, write the probability law, then decide whether new information changes the population being averaged.

- Bayesian analysis combines prior information and data through the likelihood. The posterior distribution is proportional to prior times likelihood.
- Credibility theory has the same actuarial instinct: blend individual experience with collective experience. The more stable and relevant the individual data, the more credibility it receives.
- Conjugate priors are useful because the posterior stays in the same family. This makes updating transparent and exam calculations faster.

### Step-by-step working method

1. Write the prior distribution and identify its parameters.
2. Write the likelihood from the observed data.
3. Multiply prior and likelihood, keeping parameter-dependent terms.
4. Recognise the posterior family or normalise if required.
5. Use the posterior mean, mode, interval, or credibility form for the decision.

### Extra practical actuarial examples

- Renewal example: a beta prior for renewal probability can be updated after observing renewals and non-renewals.
- Claim count example: a gamma prior for a Poisson rate leads to a gamma posterior and a credibility-style blended estimate.
- Pricing example: a small employer group should not be priced only from its own volatile claims experience; credibility blends group experience with portfolio experience.

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

### Topic 11: Linear Combinations of Independent Normal Claim Amounts

#### 1. Concept theory

A linear combination of independent normal random variables is also normally distributed.

This is useful when comparing two totals, such as indemnity claims against fixed-benefit claims.

#### 2. Why actuaries care

Actuaries often compare total losses from different product lines, portfolios, or scenarios.

If claim amounts are approximately normal and independent, the total and difference of totals can be handled using normal distribution rules.

#### 3. Mathematical derivation

If:

```text
X_i ~ N(mu_X, sigma_X^2)
Y_j ~ N(mu_Y, sigma_Y^2)
```

and all variables are independent, then:

```text
sum X_i ~ N(n_X mu_X, n_X sigma_X^2)
sum Y_j ~ N(n_Y mu_Y, n_Y sigma_Y^2)
```

For a difference:

```text
D = sum Y_j - sum X_i
```

Then:

```text
E[D] = n_Y mu_Y - n_X mu_X
Var(D) = n_Y sigma_Y^2 + n_X sigma_X^2
```

The variances add even when subtracting, because:

```text
Var(A - B) = Var(A) + Var(B)
```

when `A` and `B` are independent.

#### 4. Simple example

Suppose:

```text
Y1, Y2 ~ N(100, 20^2)
X1, X2, X3 ~ N(60, 10^2)
```

Then:

```text
D = Y1 + Y2 - X1 - X2 - X3
```

has:

```text
E[D] = 2(100) - 3(60) = 20
Var(D) = 2(20^2) + 3(10^2) = 1100
```

So:

```text
D ~ N(20, 1100)
```

#### 5. Exam-style case study

In July 2022:

```text
Fixed benefit claims: X ~ N(900, 100^2)
Indemnity claims: Y ~ N(1400, 300^2)
```

There are:

```text
4 further fixed claims
3 indemnity claims
900 already paid under fixed benefit
```

Need:

```text
P(Y1 + Y2 + Y3 > X1 + X2 + X3 + X4 + 900)
```

Define:

```text
D = Y1 + Y2 + Y3 - X1 - X2 - X3 - X4
```

Then:

```text
E[D] = 3(1400) - 4(900) = 600
Var(D) = 3(300^2) + 4(100^2) = 310000
```

So:

```text
D ~ N(600, 310000)
```

Required:

```text
P(D > 900)
= P(Z > (900 - 600) / sqrt(310000))
= P(Z > 0.54)
```

#### 6. Real-world actuarial case study

A health insurer compares expected next-year payouts from two benefit designs. Fixed-benefit claims are stable, but indemnity claims have much higher variance.

Even if indemnity claims have higher mean, the actuary also needs the probability that indemnity total exceeds the fixed-benefit total. This helps compare product risk.

#### 7. Common mistakes

- Subtracting variances when subtracting random variables.
- Forgetting the already incurred amount.
- Mixing up standard deviation and variance.
- Forgetting to multiply variance by the number of claims.
- Standardising with variance instead of standard deviation.

#### 8. Revision checkpoint

Without notes, you should be able to write:

```text
sum normals -> normal
E(aX + bY) = aE[X] + bE[Y]
Var(A - B) = Var(A) + Var(B) for independent A, B
```

and calculate probabilities for differences of normal totals.

### Expanded deep explanation

Treat every probability question as a model of uncertainty. Define the sample space, identify the random variable, write the probability law, then decide whether new information changes the population being averaged.

- Begin by checking support. A count model lives on non-negative integers, a severity model often lives on positive real values, and a transformed model may live on the whole real line after taking logs. Many exam errors happen before calculation because the wrong support is chosen.
- Separate the probability law from its moments. The probability law tells you every probability statement; the mean and variance are summaries. In actuarial work a model with the correct mean but the wrong tail can still be dangerous because solvency questions are tail questions.
- Use generating functions when sums are involved. MGFs, CGFs, and PGFs turn convolutions into algebra, but only under the conditions where they exist and where independence assumptions are valid.

### Step-by-step working method

1. State the random variable and its support.
2. Write the probability mass function, density, or distribution function.
3. Identify parameters and their actuarial meaning.
4. Calculate the required moment, probability, percentile, or transform.
5. Check whether the answer is sensible using units, range, and limiting cases.

### Extra practical actuarial examples

- Claim count example: if a motor portfolio has many zero-claim policies and a few multi-claim policies, compare Poisson and negative binomial assumptions by checking whether sample variance is close to, below, or above the sample mean.
- Severity example: for small routine medical claims a lognormal or gamma model may be adequate; for catastrophe-like property claims a heavier tail such as Pareto or Weibull may be needed.
- Exam example: after finding a probability, ask whether the question wanted an unconditional probability or a conditional probability after observing a claim, survival, or selection event.

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

### Topic 12: Independent Exponential Joint Density, Normalising Constant, and Marginals

#### 1. Concept theory

A joint density must integrate to 1 over its support.

If a joint density factorises into a function of `x` times a function of `y`, then the variables are independent.

Exponential-looking joint densities are common in waiting-time models.

#### 2. Why actuaries care

Actuaries use joint distributions when modelling two claim delays, two risk factors, or severity and duration together.

Checking independence matters because assuming independence when variables are dependent can understate risk.

#### 3. Mathematical derivation

Suppose:

```text
f(x,y) = k exp[-(3x + y/5)], x > 0, y > 0
```

To find `k`, integrate over the whole support:

```text
1 = integral_0^infinity integral_0^infinity k exp[-(3x + y/5)] dy dx
```

Separate the integrals:

```text
1 = k [integral_0^infinity exp(-3x) dx][integral_0^infinity exp(-y/5) dy]
```

Calculate:

```text
integral exp(-3x) dx = 1/3
integral exp(-y/5) dy = 5
```

So:

```text
1 = k(1/3)(5)
k = 3/5
```

Marginal of `X`:

```text
fX(x) = integral f(x,y) dy
      = 3 exp(-3x)
```

Marginal of `Y`:

```text
fY(y) = integral f(x,y) dx
      = (1/5) exp(-y/5)
```

Since:

```text
f(x,y) = fX(x)fY(y)
```

the variables are independent.

#### 4. Simple example

If:

```text
fX(x) = 2 exp(-2x)
fY(y) = 3 exp(-3y)
```

and:

```text
f(x,y) = 6 exp(-2x - 3y)
```

then:

```text
f(x,y) = fX(x)fY(y)
```

so `X` and `Y` are independent.

#### 5. Exam-style case study

In March 2022, after finding:

```text
k = 3/5
fX(x) = 3 exp(-3x)
fY(y) = (1/5)exp(-y/5)
```

state:

```text
X and Y are independent because f(x,y) = fX(x)fY(y)
```

For conditional excess:

```text
f(x | X > 5) = fX(x) / P(X > 5), x > 5
```

Since `X ~ Exponential(rate 3)`:

```text
P(X > 5) = exp(-15)
f(x | X > 5) = 3 exp(15 - 3x), x > 5
```

#### 6. Real-world actuarial case study

An insurer models time to claim report and time to claim settlement. If the joint density factorises, report delay and settlement delay can be modelled independently.

If not, long reporting delays may be associated with long settlement delays, and the claims operation model must allow dependence.

#### 7. Common mistakes

- Forgetting to solve for the normalising constant.
- Integrating over the wrong support.
- Saying variables are independent just because the density contains `x + y`.
- Forgetting independence requires product of marginals.
- Forgetting the conditional density must divide by the conditioning probability.

#### 8. Revision checkpoint

Without notes, you should be able to:

```text
find k from total integral = 1
find marginal densities
check f(x,y) = fX(x)fY(y)
derive f(x | X > a)
```

### Expanded deep explanation

Treat every probability question as a model of uncertainty. Define the sample space, identify the random variable, write the probability law, then decide whether new information changes the population being averaged.

- Begin by checking support. A count model lives on non-negative integers, a severity model often lives on positive real values, and a transformed model may live on the whole real line after taking logs. Many exam errors happen before calculation because the wrong support is chosen.
- Separate the probability law from its moments. The probability law tells you every probability statement; the mean and variance are summaries. In actuarial work a model with the correct mean but the wrong tail can still be dangerous because solvency questions are tail questions.
- Use generating functions when sums are involved. MGFs, CGFs, and PGFs turn convolutions into algebra, but only under the conditions where they exist and where independence assumptions are valid.

### Step-by-step working method

1. State the random variable and its support.
2. Write the probability mass function, density, or distribution function.
3. Identify parameters and their actuarial meaning.
4. Calculate the required moment, probability, percentile, or transform.
5. Check whether the answer is sensible using units, range, and limiting cases.

### Extra practical actuarial examples

- Claim count example: if a motor portfolio has many zero-claim policies and a few multi-claim policies, compare Poisson and negative binomial assumptions by checking whether sample variance is close to, below, or above the sample mean.
- Severity example: for small routine medical claims a lognormal or gamma model may be adequate; for catastrophe-like property claims a heavier tail such as Pareto or Weibull may be needed.
- Exam example: after finding a probability, ask whether the question wanted an unconditional probability or a conditional probability after observing a claim, survival, or selection event.

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

### Topic 13: Joint PMF, Covariance, and Variance of a Sum

#### 1. Concept theory

When two random variables are defined on the same experiment, their relationship is described by a joint probability mass function.

To calculate:

\[
Var(X+Y)
\]

you need:

\[
Var(X+Y)=Var(X)+Var(Y)+2Cov(X,Y)
\]

If \(X\) and \(Y\) are not independent, the covariance matters.

#### 2. Why actuaries care

Actuaries often combine risks:

- claim frequency and claim severity
- investment return and inflation
- mortality and lapse
- number of claims and claim investigation cost
- two benefit triggers from the same policyholder

If the risks are related, ignoring covariance gives the wrong capital or reserve variance.

#### 3. Mathematical derivation

The covariance is:

\[
Cov(X,Y)=E(XY)-E(X)E(Y)
\]

The variance of a sum is:

\[
Var(X+Y)=E[(X+Y)^2]-[E(X+Y)]^2
\]

Expand:

\[
E[(X+Y)^2]=E(X^2)+2E(XY)+E(Y^2)
\]

and:

\[
[E(X+Y)]^2=[E(X)+E(Y)]^2
\]

\[
=E(X)^2+2E(X)E(Y)+E(Y)^2
\]

Subtract:

\[
Var(X+Y)=Var(X)+Var(Y)+2Cov(X,Y)
\]

From a joint PMF:

\[
E(XY)=\sum_x\sum_y xyP(X=x,Y=y)
\]

#### 4. Simple example

Suppose:

\[
E(X)=3,\quad E(Y)=2,\quad E(X^2)=11,\quad E(Y^2)=6,\quad E(XY)=7
\]

Then:

\[
Var(X)=11-3^2=2
\]

\[
Var(Y)=6-2^2=2
\]

\[
Cov(X,Y)=7-3(2)=1
\]

So:

\[
Var(X+Y)=2+2+2(1)=6
\]

#### 5. Exam-style case study

Two dice are rolled, one yellow and one black.

- \(X\) is the number on the yellow die.
- \(Y\) is the number of times 3 appears across the two dice.

To calculate \(Var(X+Y)\):

1. Build the joint PMF for \(X\) and \(Y\).
2. Use the given or calculated values of \(E(X),E(Y),E(X^2),E(Y^2)\).
3. Calculate \(E(XY)\) from the joint PMF.
4. Calculate:

\[
Cov(X,Y)=E(XY)-E(X)E(Y)
\]

5. Use:

\[
Var(X+Y)=Var(X)+Var(Y)+2Cov(X,Y)
\]

The key exam point is that \(Y\) depends partly on the yellow die, so \(X\) and \(Y\) are not independent.

#### 6. Real-world actuarial case study

In health insurance, the number of hospital visits and the number of specialist consultations may be related. A policyholder with a severe condition may generate both.

If the insurer estimates total utilisation variance by adding variances only, it ignores covariance and may understate risk. The joint distribution or covariance must be considered.

#### 7. Common mistakes

- Assuming independence just because two variables are named separately.
- Forgetting the \(2Cov(X,Y)\) term.
- Calculating \(E(XY)\) as \(E(X)E(Y)\) without proving independence.
- Building a marginal table instead of a joint table.
- Mixing up \(E(X^2)\) with \([E(X)]^2\).

#### 8. Revision checkpoint

You should be able to:

- build or interpret a joint PMF
- calculate \(E(XY)\)
- calculate covariance
- use \(Var(X+Y)=Var(X)+Var(Y)+2Cov(X,Y)\)

### Expanded deep explanation

Treat every probability question as a model of uncertainty. Define the sample space, identify the random variable, write the probability law, then decide whether new information changes the population being averaged.

- Begin by checking support. A count model lives on non-negative integers, a severity model often lives on positive real values, and a transformed model may live on the whole real line after taking logs. Many exam errors happen before calculation because the wrong support is chosen.
- Separate the probability law from its moments. The probability law tells you every probability statement; the mean and variance are summaries. In actuarial work a model with the correct mean but the wrong tail can still be dangerous because solvency questions are tail questions.
- Use generating functions when sums are involved. MGFs, CGFs, and PGFs turn convolutions into algebra, but only under the conditions where they exist and where independence assumptions are valid.

### Step-by-step working method

1. State the random variable and its support.
2. Write the probability mass function, density, or distribution function.
3. Identify parameters and their actuarial meaning.
4. Calculate the required moment, probability, percentile, or transform.
5. Check whether the answer is sensible using units, range, and limiting cases.

### Extra practical actuarial examples

- Claim count example: if a motor portfolio has many zero-claim policies and a few multi-claim policies, compare Poisson and negative binomial assumptions by checking whether sample variance is close to, below, or above the sample mean.
- Severity example: for small routine medical claims a lognormal or gamma model may be adequate; for catastrophe-like property claims a heavier tail such as Pareto or Weibull may be needed.
- Exam example: after finding a probability, ask whether the question wanted an unconditional probability or a conditional probability after observing a claim, survival, or selection event.

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

## Master Chapter 2: Core Distributions and Distribution Theory

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

### Topics in this master chapter

- Topic 14: Binomial, Geometric, and Poisson Models
- Topic 15: Normal, Lognormal, CLT, and Standard Error
- Topic 16: Transformations of Normal Variables and Percentiles
- Topic 17: Mixture Distributions and MGFs
- Topic 18: Exponential Family Rewriting and Canonical Links
- Topic 19: Beta-Geometric Bayesian Updating and Credibility
- Topic 20: Two Independent Normal Samples and the Confidence Interval Overlap Rule
- Topic 21: Inverse Transform Simulation and Normal Quantiles
- Topic 22: Compound Binomial Aggregate Claims
- Topic 23: Pareto Tail Probabilities in Testing and Bayesian Evidence
- Topic 24: Bayesian Normal Mean and Credible Intervals
- Topic 25: Binomial GLM with Logit Link, Significance, and Interaction
- Topic 26: Uniform Distribution Parameter Estimation and Boundary MLE
- Topic 27: Exponential Mean with Inverse-Gamma Prior and Credibility Form
- Topic 28: Compound Poisson-Gamma Aggregate Loss and Capital Requirement
- Topic 29: F Distribution for Comparing Two Sample Variances
- Topic 30: Standard Normal MGF and Zero Skewness of the Normal Distribution
- Topic 31: Binomial Normal Approximation, Continuity Correction, P-Values, and Critical Values
- Topic 32: Gamma-Poisson Bayesian Updating and EBCT Model 1 Comparison
- Topic 33: Poisson Aggregation, Negative Binomial Counts, and Exponential/Erlang Waiting Times
- Topic 34: Binomial Exponential Family and Canonical Logit GLM
- Topic 35: Conditional Excess Distribution and Memoryless Exponential Behaviour
- Topic 36: Exact Bernoulli Likelihood, Binomial Likelihood, and Rare-Claim Proportion CI
- Topic 37: Gamma MGF, Chi-Square Scaling, and Sums of Gamma Variables
- Topic 38: Normal-Normal Bayesian Credibility Factor Intuition
- Topic 39: Gamma Prior for Exponential Rate and Credibility for Mean Waiting Time
- Topic 40: Exponential Family Variance Functions and GLM Interpretation
- Topic 41: Good Estimators, Consistency, Fisher's Exact Test, and Hypergeometric Tables
- Topic 42: Poisson MLE with Related Group Means and Lognormal MLE Invariance
- Topic 43: CRLB, Large-Sample CI, and Exact Chi-Square CI for Exponential Rate
- Topic 44: Poisson-Gamma Posterior, Posterior Mode, and All-or-Nothing Loss
- Topic 45: Full Normal Regression: Error Variance CI, Slope Test, and Mean Prediction
- Topic 46: Exponential GLM, Canonical Link, and Likelihood Equations
- Topic 47: Sum of Exponential Claims: Exact Gamma, Chi-Square Scaling, and CLT Approximation
- Topic 48: Exponential GLM Canonical Link Sign Convention
- Topic 49: Coefficient of Variation and Inverse Transform Simulation for Exponential Variables
- Topic 50: Beta-Binomial Posterior with Uniform Prior and Credibility Form
- Topic 51: Gamma Claim Model with Known Shape and Confidence Interval for Rate
- Topic 52: Weibull and Heavy-Tail Inverse Transform Simulation
- Topic 53: Truncated Poisson Distribution and Conditional Likelihood
- Topic 54: Probability Generating Functions and Variance
- Topic 55: Normal-Normal Posterior Probability Can Increase Even When Posterior Mean Falls
- Topic 56: Normal Sample Mean and Variance Joint Probability
- Topic 57: Chi-Square Association Test for a Two-by-Three Table
- Topic 58: Shifted Geometric Distribution, MGF, CGF, and Mean
- Topic 59: Binomial Occupancy MLE and Two-Proportion Confidence Interval
- Topic 60: Student t Distribution and Confidence Interval for a Normal Mean

### Topic 14: Binomial, Geometric, and Poisson Models

#### 1. Concept theory

These are core count distributions.

Binomial:

```text
Fixed number of trials
Each trial success/failure
Constant success probability
Independent trials
```

Geometric:

```text
Number of trials until first success
```

Poisson:

```text
Number of events in a fixed interval
```

#### 2. Why actuaries care

Count models are used for:

```text
number of claims
number of deaths
number of fraud cases
number of hospital admissions
number of cyber incidents
```

#### 3. Mathematical derivation

Binomial probability:

```text
P(X=x) = C(n,x) p^x (1-p)^(n-x)
```

Mean and variance:

```text
E[X] = np
Var(X) = np(1-p)
```

Geometric probability:

```text
P(X=k) = (1-p)^(k-1)p
```

Poisson probability:

```text
P(X=x) = e^(-lambda) lambda^x / x!
```

Mean and variance:

```text
E[X] = lambda
Var(X) = lambda
```

Poisson MGF derivation:

```text
M_X(t) = E[e^(tX)]
       = sum from x=0 to infinity e^(tx) e^(-lambda) lambda^x / x!
       = e^(-lambda) sum from x=0 to infinity (lambda e^t)^x / x!
       = e^(-lambda) e^(lambda e^t)
       = e^(lambda(e^t - 1))
```

#### 4. Simple examples

Geometric example:

First 6 on third die roll:

```text
p = 1/6
P(X=3) = (5/6)^2(1/6) = 25/216
```

Poisson example:

If claims occur at rate 2 per year:

```text
P(X=0) = e^(-2)
P(X=1) = 2e^(-2)
```

#### 5. Exam-style case study

If:

```text
X_1, X_2, ..., X_30 are iid Poisson(2)
S = sum X_i
```

then:

```text
S ~ Poisson(30 * 2) = Poisson(60)
```

The MGF is:

```text
M_S(t) = [e^(2(e^t - 1))]^30
       = e^(60(e^t - 1))
```

#### 6. Real-world actuarial case study

A cyber insurer models:

```text
N = number of cyber incidents in a year
N ~ Poisson(lambda)
```

If each incident has an average cost, the insurer can estimate expected annual cyber loss. If the number of incidents is overdispersed, a negative binomial model may be better than Poisson.

#### 7. Common mistakes

- Using binomial when number of trials is not fixed.
- Using Poisson when the variance is far larger than mean.
- Forgetting that geometric counts failures before success in some textbooks and trials until success in others.

#### 8. Revision checkpoint

You should be able to identify:

```text
fixed trials -> Binomial
first success -> Geometric
events per interval -> Poisson
```

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- Begin by checking support. A count model lives on non-negative integers, a severity model often lives on positive real values, and a transformed model may live on the whole real line after taking logs. Many exam errors happen before calculation because the wrong support is chosen.
- Separate the probability law from its moments. The probability law tells you every probability statement; the mean and variance are summaries. In actuarial work a model with the correct mean but the wrong tail can still be dangerous because solvency questions are tail questions.
- Use generating functions when sums are involved. MGFs, CGFs, and PGFs turn convolutions into algebra, but only under the conditions where they exist and where independence assumptions are valid.

### Step-by-step working method

1. State the random variable and its support.
2. Write the probability mass function, density, or distribution function.
3. Identify parameters and their actuarial meaning.
4. Calculate the required moment, probability, percentile, or transform.
5. Check whether the answer is sensible using units, range, and limiting cases.

### Extra practical actuarial examples

- Claim count example: if a motor portfolio has many zero-claim policies and a few multi-claim policies, compare Poisson and negative binomial assumptions by checking whether sample variance is close to, below, or above the sample mean.
- Severity example: for small routine medical claims a lognormal or gamma model may be adequate; for catastrophe-like property claims a heavier tail such as Pareto or Weibull may be needed.
- Exam example: after finding a probability, ask whether the question wanted an unconditional probability or a conditional probability after observing a claim, survival, or selection event.

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

### Topic 15: Normal, Lognormal, CLT, and Standard Error

#### 1. Concept theory

The normal distribution is symmetric and bell-shaped:

```text
X ~ Normal(mu, sigma^2)
```

Standardisation:

```text
Z = (X - mu) / sigma
```

The lognormal distribution is used for positive skewed values:

```text
ln Y ~ Normal(mu, sigma^2)
```

The Central Limit Theorem says sample means are approximately normal for large samples.

#### 2. Why actuaries care

Normal models are used for averages and approximations.

Lognormal models are used for positive financial amounts:

```text
claim severity
medical cost
property loss
salary
rent
```

#### 3. Mathematical derivation

For the sample mean:

```text
X_bar = (X_1 + ... + X_n) / n
```

If each observation has:

```text
E[X_i] = mu
Var(X_i) = sigma^2
```

then:

```text
E[X_bar] = mu
Var(X_bar) = sigma^2 / n
SD(X_bar) = sigma / sqrt(n)
```

The standard deviation of the sample mean is called standard error:

```text
SE(X_bar) = s / sqrt(n)
```

For lognormal:

```text
E[Y] = exp(mu + sigma^2 / 2)
```

If:

```text
mu = beta_0 + beta_1 X
```

then increasing `X` by `d` changes expected `Y` by factor:

```text
exp(beta_1 d)
```

#### 4. Simple examples

Standard error:

```text
n = 36
sample variance = 400,000
s = sqrt(400,000) = 632.46
SE = 632.46 / sqrt(36) = 105.41
```

Lognormal percentage increase:

```text
beta_1 = 0.5
d = 4 - 2 = 2
factor = exp(0.5 * 2) = exp(1) = 2.718
percentage increase = 171.8%
```

#### 5. Exam-style case study

If 2% of 2,000 screened patients have a condition:

```text
X ~ Binomial(2000, 0.02)
E[X] = 40
Var(X) = 2000(0.02)(0.98) = 39.2
SD = 6.26
```

Using CLT:

```text
P(X > 50) approximately P(Z > (50.5 - 40)/6.26)
                 approximately P(Z > 1.68)
                 approximately 0.05
```

#### 6. Real-world actuarial case study

A health insurer estimates the number of rare cancer claims next year. Each policyholder has a small probability of such a claim, but the portfolio is large. The binomial distribution is natural, and CLT gives a quick probability estimate for exceeding a threshold.

#### 7. Common mistakes

- Confusing standard error and standard deviation.
- Forgetting continuity correction for normal approximation to counts.
- Treating log-scale coefficients as additive on the original scale.

#### 8. Revision checkpoint

You understand this chapter if you can explain why:

```text
sample mean becomes more stable as n increases
```

but:

```text
individual claim amounts remain volatile
```

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- Begin by checking support. A count model lives on non-negative integers, a severity model often lives on positive real values, and a transformed model may live on the whole real line after taking logs. Many exam errors happen before calculation because the wrong support is chosen.
- Separate the probability law from its moments. The probability law tells you every probability statement; the mean and variance are summaries. In actuarial work a model with the correct mean but the wrong tail can still be dangerous because solvency questions are tail questions.
- Use generating functions when sums are involved. MGFs, CGFs, and PGFs turn convolutions into algebra, but only under the conditions where they exist and where independence assumptions are valid.

### Step-by-step working method

1. State the random variable and its support.
2. Write the probability mass function, density, or distribution function.
3. Identify parameters and their actuarial meaning.
4. Calculate the required moment, probability, percentile, or transform.
5. Check whether the answer is sensible using units, range, and limiting cases.

### Extra practical actuarial examples

- Claim count example: if a motor portfolio has many zero-claim policies and a few multi-claim policies, compare Poisson and negative binomial assumptions by checking whether sample variance is close to, below, or above the sample mean.
- Severity example: for small routine medical claims a lognormal or gamma model may be adequate; for catastrophe-like property claims a heavier tail such as Pareto or Weibull may be needed.
- Exam example: after finding a probability, ask whether the question wanted an unconditional probability or a conditional probability after observing a claim, survival, or selection event.

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

### Topic 16: Transformations of Normal Variables and Percentiles

#### 1. Concept theory

If `X` is normally distributed, we can calculate probabilities by converting `X` into a standard normal variable:

```text
Z = (X - mu) / sigma
```

Sometimes the question asks about a transformed variable, such as:

```text
X^2
Y = exp(X)
Profit = a + bX
```

The first job is to translate the transformed event back into a statement about `X`.

#### 2. Why actuaries care

Actuarial models often transform variables:

```text
log claim size
profit as a linear function of risk
capital requirement as a percentile
asset return transformed into fund value
```

The actuarial skill is not only using the normal table. It is understanding what event the business question is really asking.

#### 3. Mathematical derivation

For a linear transformation:

```text
P = a + bX
```

If:

```text
X ~ Normal(mu_X, sigma_X^2)
```

then:

```text
P ~ Normal(a + b mu_X, b^2 sigma_X^2)
```

The `q`th percentile is:

```text
P_q = mu_P + z_q sigma_P
```

If one percentile is known, we can solve for `sigma_P`.

For an exponential transformation:

```text
Y = exp(X)
```

Then:

```text
P(Y > c) = P(exp(X) > c)
         = P(X > log(c))
```

#### 4. Simple example

Let:

```text
X ~ Normal(5, 2^2)
```

Find:

```text
P(X^2 > 9)
```

The event:

```text
X^2 > 9
```

means:

```text
X < -3 or X > 3
```

Standardise:

```text
P(X < -3) = P(Z < (-3 - 5)/2) = P(Z < -4)
P(X > 3) = P(Z > (3 - 5)/2) = P(Z > -1)
```

So:

```text
P(X^2 > 9) = P(Z < -4) + P(Z > -1)
            approximately 0.00003 + 0.84134
            approximately 0.8414
```

#### 5. Exam-style case study

Profit is normally distributed with:

```text
mean profit = 10
80th percentile = 16
```

For a normal variable:

```text
P_80 = mu + z_0.80 sigma
```

Using:

```text
z_0.80 = 0.8416
```

we get:

```text
16 = 10 + 0.8416 sigma
sigma = 6 / 0.8416 = 7.1291
```

The 90th percentile:

```text
P_90 = 10 + 1.2816(7.1291)
     = 19.14
```

#### 6. Real-world actuarial case study

A solvency actuary may estimate the 99.5th percentile of annual loss. This percentile is linked to capital: the company wants enough assets to survive extreme but plausible loss years.

If expected annual loss is known and one risk percentile is available from past modelling, the actuary can infer the volatility and estimate other percentiles.

#### 7. Common mistakes

- Thinking `X^2 > 9` means only `X > 3`. It also includes `X < -3`.
- Forgetting that `exp(X) > c` means `X > log(c)`.
- Using the wrong percentile z-value.

#### 8. Revision checkpoint

Before using the normal table, rewrite the event purely in terms of `X`.

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- Begin by checking support. A count model lives on non-negative integers, a severity model often lives on positive real values, and a transformed model may live on the whole real line after taking logs. Many exam errors happen before calculation because the wrong support is chosen.
- Separate the probability law from its moments. The probability law tells you every probability statement; the mean and variance are summaries. In actuarial work a model with the correct mean but the wrong tail can still be dangerous because solvency questions are tail questions.
- Use generating functions when sums are involved. MGFs, CGFs, and PGFs turn convolutions into algebra, but only under the conditions where they exist and where independence assumptions are valid.

### Step-by-step working method

1. State the random variable and its support.
2. Write the probability mass function, density, or distribution function.
3. Identify parameters and their actuarial meaning.
4. Calculate the required moment, probability, percentile, or transform.
5. Check whether the answer is sensible using units, range, and limiting cases.

### Extra practical actuarial examples

- Claim count example: if a motor portfolio has many zero-claim policies and a few multi-claim policies, compare Poisson and negative binomial assumptions by checking whether sample variance is close to, below, or above the sample mean.
- Severity example: for small routine medical claims a lognormal or gamma model may be adequate; for catastrophe-like property claims a heavier tail such as Pareto or Weibull may be needed.
- Exam example: after finding a probability, ask whether the question wanted an unconditional probability or a conditional probability after observing a claim, survival, or selection event.

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

### Topic 17: Mixture Distributions and MGFs

#### 1. Concept theory

A mixture distribution combines several distributions with weights.

Example:

```text
80% of claims are ordinary
20% of claims are special
```

The total density is a weighted sum of component densities.

#### 2. Why actuaries care

Claim size distributions often have mixed behaviour:

```text
small routine claims
medium claims
large catastrophic claims
```

A single simple distribution may not fit all claim types.

#### 3. Mathematical derivation

If:

```text
f_X(x) = w f_1(x) + (1-w) f_2(x)
```

then:

```text
M_X(t) = w M_1(t) + (1-w) M_2(t)
```

This is because:

```text
M_X(t) = E[e^(tX)]
```

and expectation is linear.

#### 4. Simple example

Suppose:

```text
f(x) = (4/5)e^(-x) + (2/5)e^(-2x), x >= 0
```

The MGF is:

```text
M_X(t) = integral_0^infinity e^(tx)[(4/5)e^(-x) + (2/5)e^(-2x)] dx
```

Separate terms:

```text
M_X(t) = 4/[5(1-t)] + 2/[5(2-t)]
```

The mean is:

```text
E[X] = M'_X(0) = 0.9
```

The second moment is:

```text
E[X^2] = M''_X(0) = 1.7
```

Variance:

```text
Var(X) = 1.7 - 0.9^2 = 0.89
```

#### 5. Exam-style case study

When the density is written as a sum, do not force it into one standard distribution. Find the MGF term by term.

#### 6. Real-world actuarial case study

A travel insurer may model baggage claims as a mixture:

```text
low-cost delayed baggage claims
high-cost lost baggage claims
```

The mixture model captures two different claim mechanisms.

#### 7. Common mistakes

- Assuming the coefficients are direct probabilities without checking the component densities integrate correctly.
- Forgetting that MGFs of mixtures are weighted sums, not products.
- Using product of MGFs, which applies to sums of independent variables, not mixtures.

#### 8. Revision checkpoint

Remember:

```text
sum of independent variables -> product of MGFs
mixture of distributions -> weighted sum of MGFs
```

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- Begin by checking support. A count model lives on non-negative integers, a severity model often lives on positive real values, and a transformed model may live on the whole real line after taking logs. Many exam errors happen before calculation because the wrong support is chosen.
- Separate the probability law from its moments. The probability law tells you every probability statement; the mean and variance are summaries. In actuarial work a model with the correct mean but the wrong tail can still be dangerous because solvency questions are tail questions.
- Use generating functions when sums are involved. MGFs, CGFs, and PGFs turn convolutions into algebra, but only under the conditions where they exist and where independence assumptions are valid.

### Step-by-step working method

1. State the random variable and its support.
2. Write the probability mass function, density, or distribution function.
3. Identify parameters and their actuarial meaning.
4. Calculate the required moment, probability, percentile, or transform.
5. Check whether the answer is sensible using units, range, and limiting cases.

### Extra practical actuarial examples

- Claim count example: if a motor portfolio has many zero-claim policies and a few multi-claim policies, compare Poisson and negative binomial assumptions by checking whether sample variance is close to, below, or above the sample mean.
- Severity example: for small routine medical claims a lognormal or gamma model may be adequate; for catastrophe-like property claims a heavier tail such as Pareto or Weibull may be needed.
- Exam example: after finding a probability, ask whether the question wanted an unconditional probability or a conditional probability after observing a claim, survival, or selection event.

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

### Topic 18: Exponential Family Rewriting and Canonical Links

#### 1. Concept theory

Many CS1 questions ask whether a distribution belongs to the exponential family.

The standard exponential family form is:

```text
f(y; theta, phi) = exp{[y theta - b(theta)] / a(phi) + c(y, phi)}
```

For simpler one-parameter cases, think of it as:

```text
f(y; theta) = exp{y theta - b(theta) + c(y)}
```

The goal is to rewrite the probability function so that the part involving `y` and the parameter appears as:

```text
y * theta
```

The canonical link connects the mean `mu` to the natural parameter `theta`:

```text
g(mu) = theta
```

#### 2. Why actuaries care

GLMs are built on exponential family distributions. In actuarial pricing, this is everywhere:

```text
Poisson GLM for claim counts
Gamma GLM for claim severity
Binomial GLM for claim occurrence
Normal GLM for continuous outcomes
```

If you can identify the natural parameter and canonical link, GLM questions become much less mechanical.

#### 3. Mathematical derivation

Suppose a modified geometric distribution is:

```text
p(y | alpha) = alpha^(y-1) / (1 + alpha)^y, y = 1, 2, 3, ...
```

Take logs inside the exponential:

```text
p(y | alpha)
= exp{(y-1)log(alpha) - y log(1 + alpha)}
```

Expand:

```text
= exp{y log(alpha) - log(alpha) - y log(1 + alpha)}
```

Group the `y` terms:

```text
= exp{y[log(alpha) - log(1 + alpha)] - log(alpha)}
```

So:

```text
= exp{y log(alpha/(1 + alpha)) - log(alpha)}
```

The natural parameter is:

```text
theta = log(alpha/(1 + alpha))
```

To express `b(theta)`, solve for `alpha`:

```text
e^theta = alpha/(1 + alpha)
e^theta(1 + alpha) = alpha
e^theta = alpha(1 - e^theta)
alpha = e^theta / (1 - e^theta)
```

Since the exponential family form is:

```text
exp{y theta - b(theta)}
```

and we have:

```text
exp{y theta - log(alpha)}
```

we identify:

```text
b(theta) = log(alpha)
         = log(e^theta / (1 - e^theta))
         = theta - log(1 - e^theta)
```

Some solutions may express the same relationship with sign conventions depending on the chosen canonical form. The important step is correctly isolating the `y theta` term.

#### 4. Simple example

For Bernoulli:

```text
P(Y=y) = p^y(1-p)^(1-y)
```

Rewrite:

```text
= exp{y log(p) + (1-y)log(1-p)}
= exp{y log(p/(1-p)) + log(1-p)}
```

Natural parameter:

```text
theta = log(p/(1-p))
```

This is the logit transformation.

So the canonical link for Bernoulli/Binomial is:

```text
logit(p) = log(p/(1-p))
```

#### 5. Exam-style case study

If a Gamma distribution is used in a GLM, the canonical link is the inverse link:

```text
g(mu) = 1/mu
```

In practice, actuaries often use the log link for Gamma severity models:

```text
g(mu) = log(mu)
```

But exam questions may ask specifically for the canonical link, so read the wording carefully.

For a lognormal response, a natural link is:

```text
g(mu) = log(mu)
```

because the model is naturally expressed on the log scale.

#### 6. Real-world actuarial case study

A non-life insurer models claim severity. Claim amounts are positive and skewed. A Gamma GLM with log link is often practical because fitted values remain positive and coefficients are easy to interpret multiplicatively.

Example:

```text
coefficient for high-risk vehicle = 0.25
cost multiplier = exp(0.25) = 1.284
```

So high-risk vehicles have about 28.4% higher expected claim severity, all else equal.

#### 7. Common mistakes

- Confusing canonical link with commonly used link.
- Forgetting to rewrite the probability using exponentials and logs.
- Losing a negative sign while identifying `b(theta)`.
- Treating a categorical response as if identity link is always acceptable.

#### 8. Revision checkpoint

You should be able to take a probability function and rewrite it as:

```text
exp{y theta - b(theta) + c(y)}
```

without needing to memorise every distribution separately.

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- A GLM has three parts: a random component for the response distribution, a systematic component for the linear predictor, and a link function connecting the mean to the linear predictor.
- The link function determines coefficient interpretation. With a log link, a one-unit increase multiplies the mean by an exponential factor; with a logit link, a coefficient changes log-odds.
- Deviance compares fitted likelihoods. A smaller deviance can mean better fit, but model selection must balance fit, complexity, interpretability, and out-of-sample stability.

### Step-by-step working method

1. Choose the response distribution based on the data type.
2. Choose the link function and write the linear predictor.
3. Include exposure or offset terms where required.
4. Fit the model and interpret coefficients on the correct scale.
5. Use residuals, deviance, AIC, and validation to judge suitability.

### Extra practical actuarial examples

- Frequency example: Poisson GLM with log exposure offset estimates claim frequency per unit exposure.
- Binary example: a logit model can estimate probability of fraud, lapse, or claim occurrence.
- Severity example: gamma GLM with log link is often used for positive skewed claim amounts.

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

### Topic 19: Beta-Geometric Bayesian Updating and Credibility

#### 1. Concept theory

A geometric distribution models the time until first success.

If the success probability `p` is unknown, we can place a Beta prior on `p`.

Uniform prior:

```text
p ~ Uniform(0,1)
```

is the same as:

```text
p ~ Beta(1,1)
```

#### 2. Why actuaries care

This appears in time-to-first-event problems:

```text
first claim
first default
first market crash trigger
first lapse
first fraud event
```

Bayesian updating lets actuaries revise the probability after observing when the first event occurred.

#### 3. Mathematical derivation

If the first success occurs on day `n_1`, then:

```text
P(X = n_1 | p) = (1-p)^(n_1-1)p
```

Likelihood:

```text
L(p) proportional to p(1-p)^(n_1-1)
```

With prior:

```text
p ~ Beta(1,1)
```

prior density is proportional to:

```text
p^(1-1)(1-p)^(1-1)
```

Posterior:

```text
posterior proportional to p^1(1-p)^(n_1-1)
```

So:

```text
p | data ~ Beta(2, n_1)
```

Prior mean:

```text
1/(1+1) = 1/2
```

Posterior mean:

```text
2/(n_1 + 2)
```

MLE:

```text
log L(p) = log p + (n_1 - 1)log(1-p)
```

Differentiate:

```text
1/p - (n_1 - 1)/(1-p) = 0
```

Solve:

```text
1 - p = p(n_1 - 1)
1 = n_1 p
p_hat = 1/n_1
```

#### 4. Simple example

If the first circuit breaker occurs on day:

```text
n_1 = 10
```

then:

```text
MLE = 1/10 = 0.10
posterior = Beta(2,10)
posterior mean = 2/12 = 0.1667
```

The posterior mean is higher than the MLE because the uniform prior adds prior weight.

#### 5. Exam-style case study

For the stock exchange circuit breaker:

```text
X = number of trading days until first trigger
```

Then:

```text
X ~ Geometric(p)
```

With:

```text
p ~ Uniform(0,1) = Beta(1,1)
```

and observed first trigger on day `n_1`:

```text
p | data ~ Beta(2, n_1)
prior mean = 1/2
posterior mean = 2/(n_1 + 2)
MLE = 1/n_1
```

#### 6. Real-world actuarial case study

A financial risk team monitors the first large market fall in a financial year. Before observing the year, they may use a broad prior for crash-trigger probability. Once the first trigger occurs, they update the probability for stress testing and risk reporting.

#### 7. Common mistakes

- Using Binomial updating directly without recognising the geometric likelihood.
- Forgetting that observing first success on day `n_1` includes `n_1 - 1` failures.
- Confusing posterior mean with MLE.
- Forgetting that Uniform(0,1) is Beta(1,1).

#### 8. Revision checkpoint

You should be able to derive:

```text
Geometric likelihood: p(1-p)^(n_1-1)
MLE: 1/n_1
Posterior with Beta(1,1): Beta(2,n_1)
Posterior mean: 2/(n_1+2)
```

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- Bayesian analysis combines prior information and data through the likelihood. The posterior distribution is proportional to prior times likelihood.
- Credibility theory has the same actuarial instinct: blend individual experience with collective experience. The more stable and relevant the individual data, the more credibility it receives.
- Conjugate priors are useful because the posterior stays in the same family. This makes updating transparent and exam calculations faster.

### Step-by-step working method

1. Write the prior distribution and identify its parameters.
2. Write the likelihood from the observed data.
3. Multiply prior and likelihood, keeping parameter-dependent terms.
4. Recognise the posterior family or normalise if required.
5. Use the posterior mean, mode, interval, or credibility form for the decision.

### Extra practical actuarial examples

- Renewal example: a beta prior for renewal probability can be updated after observing renewals and non-renewals.
- Claim count example: a gamma prior for a Poisson rate leads to a gamma posterior and a credibility-style blended estimate.
- Pricing example: a small employer group should not be priced only from its own volatile claims experience; credibility blends group experience with portfolio experience.

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

### Topic 20: Two Independent Normal Samples and the Confidence Interval Overlap Rule

#### 1. Concept theory

For two independent samples from normal populations with known variance:

```text
X_bar_N ~ Normal(mu_N, sigma^2/n)
X_bar_S ~ Normal(mu_S, sigma^2/n)
```

The difference in sample means is:

```text
X_bar_N - X_bar_S ~ Normal(mu_N - mu_S, 2sigma^2/n)
```

The clean hypothesis test for equal means is based on the difference. Some exam questions instead give a two-step rule based on whether individual confidence intervals overlap.

#### 2. Why actuaries care

Comparing two groups is common:

```text
claim cost in two regions
mortality between two populations
exam scores between two cohorts
lapse rates between two sales channels
hospital cost between two provider networks
```

The actuary must know what statistic is being compared and what rule is being used.

#### 3. Mathematical derivation

For each group:

```text
95% CI for mu_N = X_bar_N +/- 1.96 sigma/sqrt(n)
95% CI for mu_S = X_bar_S +/- 1.96 sigma/sqrt(n)
```

If North is expected to have a higher mean, the two intervals do not overlap when:

```text
lower limit of North interval > upper limit of South interval
```

That is:

```text
X_bar_N - 1.96 sigma/sqrt(n)
>
X_bar_S + 1.96 sigma/sqrt(n)
```

Rearrange:

```text
X_bar_N - X_bar_S > 2 * 1.96 sigma/sqrt(n)
```

This is the rule used in that exam question. It is not the same as the standard two-sample z-test, whose standard error for the difference is:

```text
sqrt(2) sigma/sqrt(n)
```

#### 4. Simple example

Suppose:

```text
X_bar_N = 561.4
X_bar_S = 547.2
n = 10
sigma = 10
```

North lower limit:

```text
561.4 - 1.96(10)/sqrt(10) = 555.20
```

South upper limit:

```text
547.2 + 1.96(10)/sqrt(10) = 553.40
```

Since:

```text
555.20 > 553.40
```

the intervals do not overlap, so under the exam's rule we reject equality.

#### 5. Exam-style case study

The exam defines:

```text
Step 1: compute separate 95% confidence intervals.
Step 2: reject H0 if intervals do not overlap.
```

So do not automatically use the usual two-sample z-test unless asked.

Under the exam rule:

```text
Reject H0 if
X_bar_N - 1.96 sigma/sqrt(n)
>
X_bar_S + 1.96 sigma/sqrt(n)
```

If the intervals overlap, do not reject under this rule.

#### 6. Real-world actuarial case study

A health actuary compares average treatment cost between two hospital networks. A quick management dashboard may use interval overlap as a visual rule. But for formal actuarial work, the actuary should use the confidence interval for the difference or a proper hypothesis test.

#### 7. Common mistakes

- Confusing the overlap rule with the standard two-sample test.
- Using variance instead of standard deviation in interval width.
- Forgetting that the sample mean variance is `sigma^2/n`.
- Saying "accept H0" too strongly; better wording is "do not reject H0".

#### 8. Revision checkpoint

You should be able to write:

```text
distribution of each sample mean
distribution of the difference
individual CI overlap condition
standard error for the difference
```

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- Inference is a disciplined way of deciding whether observed experience is consistent with an assumption. A confidence interval estimates a plausible range for a parameter; a hypothesis test checks whether the data are surprising under a stated null hypothesis.
- Type I error is rejecting a true null hypothesis. Type II error is failing to reject a false null hypothesis. Power is the probability of detecting an effect when it is truly present.
- A prediction interval is wider than a confidence interval for a mean because it includes both uncertainty about the average and the randomness of a new observation.

### Step-by-step working method

1. Identify the parameter being estimated or tested.
2. Write the null and alternative hypotheses, if a test is required.
3. Choose the reference distribution and degrees of freedom.
4. Compute the statistic, p-value, interval, or critical region.
5. Finish with a plain-English actuarial conclusion.

### Extra practical actuarial examples

- Fraud model example: a higher observed detection rate is useful only if the improvement is statistically convincing and operationally valuable after false positives are considered.
- Claims inflation example: test whether average claim cost has changed before changing a pricing basis, but also review exposure mix and large claims.
- Reserve example: a confidence interval for mean settlement cost should not be confused with a prediction interval for a single future claim.

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

### Topic 21: Inverse Transform Simulation and Normal Quantiles

#### 1. Concept theory

Simulation means generating artificial observations from a chosen distribution.

The inverse transform method starts with:

```text
U ~ Uniform(0,1)
```

and converts it into a value from the required distribution using the inverse cumulative distribution function.

For a continuous distribution:

```text
X = F^{-1}(U)
```

where `F` is the CDF of `X`.

#### 2. Why actuaries care

Simulation is used when exact formulas are difficult or when actuaries need scenario distributions:

```text
aggregate annual claims
investment returns
solvency capital
reinsurance recoveries
stress testing
```

In CS1A, exam questions often test whether you understand how a uniform random number becomes a simulated value.

#### 3. Mathematical derivation

Let:

```text
U ~ Uniform(0,1)
X = F^{-1}(U)
```

Then:

```text
P(X <= x) = P(F^{-1}(U) <= x)
```

Since `F` is increasing:

```text
P(F^{-1}(U) <= x) = P(U <= F(x))
```

For `U ~ Uniform(0,1)`:

```text
P(U <= F(x)) = F(x)
```

So `X` has the required distribution.

For normal simulation:

```text
Z = Phi^{-1}(U)
X = mu + sigma Z
```

#### 4. Simple example

Suppose:

```text
X ~ Normal(25, 6^2)
U = 0.40
```

Since `U < 0.5`, find the positive `z` such that:

```text
P(Z <= z) = 1 - 0.40 = 0.60
```

From normal tables:

```text
z approximately 0.253
```

Because `U < 0.5`, use:

```text
Z = -0.253
```

Then:

```text
X = 25 + 6(-0.253)
  = 23.48
```

#### 5. Exam-style case study

If `Y = e^X` is lognormal with:

```text
X ~ Normal(25, 36)
```

and the uniform random number is:

```text
u = 0.40
```

then the simulated normal value is:

```text
x = 25 + 6z
```

where `z` is the standard normal quantile corresponding to `u`. Using the table symmetry gives:

```text
x approximately 23.48
```

#### 6. Real-world actuarial case study

A solvency actuary simulates 100,000 possible annual claim outcomes. Each simulation begins with uniform random numbers and transforms them into claim counts, claim sizes, inflation rates, and investment returns.

The final distribution helps estimate capital requirements.

#### 7. Common mistakes

- Using `u` directly as the simulated value.
- Forgetting to transform standard normal `z` into `x = mu + sigma z`.
- Ignoring symmetry when `u < 0.5`.
- Confusing variance and standard deviation.

#### 8. Revision checkpoint

You should be able to explain:

```text
Uniform random number -> quantile -> simulated value
```

and calculate a normal simulated value from a table.

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- An estimator is a rule for turning data into a parameter estimate. Different estimators can have different bias, variance, mean squared error, and robustness.
- Maximum likelihood chooses the parameter that makes the observed data most probable under the model. Method of moments matches sample moments to theoretical moments.
- Large-sample theory can give approximate intervals, but boundary estimates, censored observations, small samples, and heavy tails can make approximations unreliable.

### Step-by-step working method

1. Write the model and parameter space.
2. Choose MOM, MLE, least squares, or another estimator.
3. Derive or solve the estimating equation.
4. Check bias, variance, consistency, and practical stability where possible.
5. Interpret the estimate in actuarial units.

### Extra practical actuarial examples

- Severity example: estimating a Pareto tail parameter from a few large losses may be highly unstable, so sensitivity testing is essential.
- Frequency example: the Poisson MLE for a constant rate is total claims divided by total exposure.
- Simulation example: inverse transform simulation uses uniform random numbers to generate losses from a fitted distribution for stress testing.

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

### Topic 22: Compound Binomial Aggregate Claims

#### 1. Concept theory

Aggregate claims combine:

```text
N = number of claims
X_i = amount of each claim
Y = X_1 + ... + X_N
```

If the number of claims follows a Binomial distribution, this is a compound Binomial model.

#### 2. Why actuaries care

Some insurance products have a fixed number of exposed persons or events.

Examples:

```text
audience members who may sue a performer
insured devices that may claim
loans that may default
employees who may make health claims
```

The Binomial frequency model is natural when each exposure can produce at most one claim.

#### 3. Mathematical derivation

Condition on `N`.

Expected aggregate claim:

```text
E[Y | N] = N E[X]
```

Therefore:

```text
E[Y] = E[E[Y | N]]
     = E[N]E[X]
```

Variance:

```text
Var(Y) = E[Var(Y | N)] + Var(E[Y | N])
```

Given `N`:

```text
Var(Y | N) = N Var(X)
E[Y | N] = N E[X]
```

So:

```text
Var(Y) = E[N]Var(X) + Var(N)E[X]^2
```

If:

```text
N ~ Binomial(m,p)
```

then:

```text
E[N] = mp
Var(N) = mp(1-p)
```

#### 4. Simple example

Suppose:

```text
N ~ Binomial(1000, 0.005)
E[X] = 5
Var(X) = 50
```

Then:

```text
E[N] = 1000 * 0.005 = 5
Var(N) = 1000 * 0.005 * 0.995 = 4.975
```

Aggregate mean:

```text
E[Y] = 5 * 5 = 25
```

Aggregate variance:

```text
Var(Y) = 5(50) + 4.975(5^2)
       = 250 + 124.375
       = 374.375
```

Standard deviation:

```text
SD(Y) = sqrt(374.375) = 19.35
```

#### 5. Exam-style case study

For performer liability:

```text
10 events
100 audience members per event
total exposure = 1000
p = 0.005
N ~ Binomial(1000, 0.005)
```

If severity:

```text
X_i ~ Gamma(alpha = 1/2, beta = 1/10)
```

using rate parameterisation:

```text
E[X_i] = alpha / beta = 5
Var(X_i) = alpha / beta^2 = 50
```

Then:

```text
E[Y] = E[N]E[X] = 5 * 5 = 25 lakhs
SD(Y) = 19.35 lakhs
```

#### 6. Real-world actuarial case study

Event liability insurance often has bounded exposure: each attendee may or may not sue, and each person can generate at most one claim. A compound Binomial model estimates both expected payout and volatility.

#### 7. Common mistakes

- Using compound Poisson variance when frequency is Binomial.
- Forgetting the `Var(N)E[X]^2` term.
- Mixing Gamma rate and scale parameters.
- Using the number of events instead of total audience exposure.

#### 8. Revision checkpoint

You should be able to write:

```text
E[Y] = E[N]E[X]
Var(Y) = E[N]Var(X) + Var(N)E[X]^2
```

and apply it for Binomial frequency.

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- Aggregate loss modelling combines frequency and severity. Frequency explains how often losses occur; severity explains how large they are when they occur. Their product gives expected loss only when the frequency and severity assumptions are aligned.
- The variance of aggregate loss has two sources: random claim sizes and random claim counts. This is why aggregate variance usually contains both a severity variance term and a squared mean severity term.
- Capital work is more demanding than pricing work. Pricing may focus on expected loss, while capital and reinsurance decisions need standard deviation, percentiles, stress tests, and tail scenarios.

### Step-by-step working method

1. Define the claim count variable and the individual severity variables.
2. Check independence and identical distribution assumptions.
3. Condition on the claim count first.
4. Use total expectation and total variance.
5. Translate the result into premium, reserve, or capital language.

### Extra practical actuarial examples

- Pricing example: expected annual loss equals expected claim count times expected claim size, then expenses, commission, profit margin, and risk loading are added.
- Capital example: two portfolios with the same expected loss can require different capital if one has low-frequency high-severity claims.
- Operational example: for cyber insurance, one ransomware event may produce legal, notification, downtime, and recovery costs, so severity volatility drives risk loading.

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

### Topic 23: Pareto Tail Probabilities in Testing and Bayesian Evidence

#### 1. Concept theory

The Pareto distribution is used for heavy-tailed positive quantities.

Heavy-tailed means large values are more likely than they would be under a Normal or Exponential model.

For the two-parameter Pareto with shape `alpha = 1`:

```text
f(x | lambda) = lambda / (lambda + x)^2, x > 0
```

The distribution function is:

```text
F(x | lambda) = 1 - lambda/(lambda + x)
```

The survival function is:

```text
P(X > x | lambda) = lambda/(lambda + x)
```

#### 2. Why actuaries care

Pareto distributions are common for large insurance losses:

```text
catastrophe claims
large liability claims
market index jumps
operational risk losses
cyber losses
```

Many actuarial decisions are based on tail probabilities, not averages.

#### 3. Mathematical derivation

Given:

```text
f(x | lambda) = lambda / (lambda + x)^2
```

Find the CDF:

```text
F(x) = integral from 0 to x lambda/(lambda+t)^2 dt
```

Let:

```text
u = lambda + t
du = dt
```

Then:

```text
F(x) = integral from lambda to lambda+x lambda/u^2 du
     = lambda[-1/u] from lambda to lambda+x
     = -lambda/(lambda+x) + lambda/lambda
     = 1 - lambda/(lambda+x)
```

So:

```text
P(X > x) = 1 - F(x) = lambda/(lambda+x)
```

#### 4. Simple example

If:

```text
lambda = 100
x = 500
```

then:

```text
P(X > 500 | lambda=100)
= 100/(100+500)
= 1/6
= 0.1667
```

If:

```text
lambda = 300
```

then:

```text
P(X > 500 | lambda=300)
= 300/(300+500)
= 0.375
```

The larger `lambda` gives a higher probability of exceeding 500 in this version of the distribution.

#### 5. Exam-style case study

For a test:

```text
H0: lambda = 50
H1: lambda = 60
Reject H0 if X > 93.50
```

Type I error:

```text
P(reject H0 | H0 true)
= P(X > 93.50 | lambda=50)
= 50/(50+93.50)
```

For the density in the Nov 2024 paper:

```text
f(x) = 3lambda^3(lambda+x)^(-4)
```

the survival function is:

```text
P(X > x) = [lambda/(lambda+x)]^3
```

So:

```text
Type I error = [50/(50+93.50)]^3 = 4.23%
```

Type II error:

```text
P(fail to reject H0 | H1 true)
= P(X <= 93.50 | lambda=60)
= 1 - [60/(60+93.50)]^3
= 94.03%
```

#### 6. Real-world actuarial case study

A financial risk team models annual market index growth using two possible economic regimes. Large growth may be more likely under one regime. Bayesian updating uses tail probabilities to update which regime is more plausible after observing strong market growth.

#### 7. Common mistakes

- Using the PDF value instead of the tail probability.
- Forgetting that Type I error uses the null distribution.
- Forgetting that Type II error uses the alternative distribution.
- Mixing different Pareto parameterisations.

#### 8. Revision checkpoint

You should be able to derive and use:

```text
F(x) = 1 - lambda/(lambda+x)
P(X > x) = lambda/(lambda+x)
```

and know which distribution to use for Type I and Type II errors.

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- Bayesian analysis combines prior information and data through the likelihood. The posterior distribution is proportional to prior times likelihood.
- Credibility theory has the same actuarial instinct: blend individual experience with collective experience. The more stable and relevant the individual data, the more credibility it receives.
- Conjugate priors are useful because the posterior stays in the same family. This makes updating transparent and exam calculations faster.

### Step-by-step working method

1. Write the prior distribution and identify its parameters.
2. Write the likelihood from the observed data.
3. Multiply prior and likelihood, keeping parameter-dependent terms.
4. Recognise the posterior family or normalise if required.
5. Use the posterior mean, mode, interval, or credibility form for the decision.

### Extra practical actuarial examples

- Renewal example: a beta prior for renewal probability can be updated after observing renewals and non-renewals.
- Claim count example: a gamma prior for a Poisson rate leads to a gamma posterior and a credibility-style blended estimate.
- Pricing example: a small employer group should not be priced only from its own volatile claims experience; credibility blends group experience with portfolio experience.

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

### Topic 24: Bayesian Normal Mean and Credible Intervals

#### 1. Concept theory

Bayesian inference treats an unknown parameter as uncertain and updates that uncertainty after observing data.

For a normal sample with known variance, the unknown mean `mu` can be updated using the likelihood from the data and the prior belief about `mu`.

If the prior is flat or uniform over a wide enough range, the posterior distribution of `mu` is centred at the sample mean.

#### 2. Why actuaries care

Actuaries often combine prior judgement with observed experience. This appears in credibility theory, mortality investigations, claim frequency analysis, and reserving.

For example, a new insurance product may have only limited claims data. Bayesian analysis gives a formal way to combine earlier assumptions with emerging experience.

#### 3. Mathematical derivation

Let:

```text
X1, X2, ..., Xn | mu ~ N(mu, sigma^2)
```

where `sigma^2` is known.

The likelihood is:

```text
L(mu) proportional to exp[-sum(xi - mu)^2 / (2sigma^2)]
```

Use the identity:

```text
sum(xi - mu)^2 = sum(xi - xbar)^2 + n(xbar - mu)^2
```

The first term does not depend on `mu`, so:

```text
L(mu) proportional to exp[-n(mu - xbar)^2 / (2sigma^2)]
```

If the prior is flat over the relevant range:

```text
posterior proportional to likelihood
```

Therefore:

```text
mu | data ~ N(xbar, sigma^2 / n)
```

A 95 percent equal-tailed credible interval is:

```text
xbar +/- 1.96 sigma / sqrt(n)
```

For a symmetric normal posterior:

```text
posterior mean = posterior median = posterior mode
```

So the Bayes estimate is the same under:

- squared-error loss: posterior mean
- absolute-error loss: posterior median
- all-or-nothing loss: posterior mode

#### 4. Simple example

Suppose:

```text
xbar = 5
sigma^2 = 25
n = 100
```

Then:

```text
mu | data ~ N(5, 25 / 100)
mu | data ~ N(5, 0.25)
```

The posterior standard deviation is:

```text
sqrt(0.25) = 0.5
```

A 95 percent credible interval is:

```text
5 +/- 1.96(0.5)
= 5 +/- 0.98
= (4.02, 5.98)
```

#### 5. Exam-style case study

An exam question gives:

```text
n = 150
xbar = 5
sigma^2 = 25
```

Assume a flat prior for `mu`.

Posterior distribution:

```text
mu | data ~ N(5, 25 / 150)
```

Posterior standard deviation:

```text
sqrt(25 / 150) = 0.4082
```

95 percent credible interval:

```text
5 +/- 1.96(0.4082)
= 5 +/- 0.8002
= (4.1998, 5.8002)
```

Because the posterior is normal and symmetric, the equal-tailed interval and highest posterior density interval are the same.

#### 6. Real-world actuarial case study

An insurer is estimating the average claim cost for a new cyber product. The technical pricing team initially believes the mean claim size is somewhere around a plausible range, but early claims data becomes available.

If the prior is weak and the observed data is reasonably large, the posterior mean will be close to the sample mean. The credible interval gives management a direct probability statement about the unknown mean claim cost.

This is useful for setting early-stage pricing margins and deciding whether more underwriting restrictions are needed.

#### 7. Common mistakes

- Calling a Bayesian credible interval a frequentist confidence interval without explaining the interpretation.
- Forgetting to divide `sigma^2` by `n` in the posterior variance.
- Using `S^2` when the question says `sigma^2` is known.
- Assuming equal-tailed and HPD intervals are always identical. They coincide here because the posterior is symmetric and unimodal.
- Forgetting that different loss functions can give different Bayes estimates for asymmetric posteriors.

#### 8. Revision checkpoint

Without notes, you should be able to derive:

```text
mu | data ~ N(xbar, sigma^2 / n)
```

for a normal sample with known variance and flat prior, calculate a credible interval, and state which posterior summary is used under squared-error, absolute-error, and all-or-nothing loss.

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- Bayesian analysis combines prior information and data through the likelihood. The posterior distribution is proportional to prior times likelihood.
- Credibility theory has the same actuarial instinct: blend individual experience with collective experience. The more stable and relevant the individual data, the more credibility it receives.
- Conjugate priors are useful because the posterior stays in the same family. This makes updating transparent and exam calculations faster.

### Step-by-step working method

1. Write the prior distribution and identify its parameters.
2. Write the likelihood from the observed data.
3. Multiply prior and likelihood, keeping parameter-dependent terms.
4. Recognise the posterior family or normalise if required.
5. Use the posterior mean, mode, interval, or credibility form for the decision.

### Extra practical actuarial examples

- Renewal example: a beta prior for renewal probability can be updated after observing renewals and non-renewals.
- Claim count example: a gamma prior for a Poisson rate leads to a gamma posterior and a credibility-style blended estimate.
- Pricing example: a small employer group should not be priced only from its own volatile claims experience; credibility blends group experience with portfolio experience.

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

### Topic 25: Binomial GLM with Logit Link, Significance, and Interaction

#### 1. Concept theory

A binomial GLM models the probability of an event. The event could be a customer buying a policy, a claimant recovering, a policyholder lapsing, or a traveller choosing a train.

The logit link transforms a probability between 0 and 1 into a value that can range from negative infinity to positive infinity:

```text
logit(p) = log(p / (1 - p))
```

The model is:

```text
log(p / (1 - p)) = eta
```

where `eta` is the linear predictor.

#### 2. Why actuaries care

Actuaries use binomial GLMs for binary outcomes:

- claim or no claim
- lapse or no lapse
- fraud or no fraud
- survive or die within a time period
- accept or reject a quote

The logit model helps convert risk factors into an estimated probability.

#### 3. Mathematical derivation

Start with:

```text
log(p / (1 - p)) = eta
```

Exponentiate both sides:

```text
p / (1 - p) = exp(eta)
```

Rearrange:

```text
p = exp(eta)(1 - p)
p = exp(eta) - p exp(eta)
p[1 + exp(eta)] = exp(eta)
```

Therefore:

```text
p = exp(eta) / [1 + exp(eta)]
```

Equivalently:

```text
p = 1 / [1 + exp(-eta)]
```

For coefficient significance, a common exam rule of thumb is:

```text
absolute value of estimate > 2 x standard error
```

This is an approximate 5 percent significance check.

With an interaction term:

```text
eta = beta0 + beta1 x + beta2 z + beta3 xz
```

The effect of `x` depends on the value of `z`.

#### 4. Simple example

Suppose:

```text
eta = -1.386
```

Then:

```text
p = exp(-1.386) / [1 + exp(-1.386)]
```

Since:

```text
exp(-1.386) approximately 0.25
```

we get:

```text
p = 0.25 / 1.25
  = 0.20
```

So the estimated probability is 20 percent.

If 4 independent people each have probability 0.20 of using a train, then:

```text
X ~ Binomial(4, 0.20)
```

Probability at least 2 use the train:

```text
P(X >= 2) = 1 - P(X = 0) - P(X = 1)
```

```text
= 1 - 0.8^4 - 4(0.2)(0.8^3)
= 1 - 0.4096 - 0.4096
= 0.1808
```

#### 5. Exam-style case study

A fitted logistic model is:

```text
log(p / (1 - p)) = beta0 + beta1 weekend + beta2 kms
```

For a weekday:

```text
weekend = 0
eta = beta0 + beta2 kms
```

For a weekend:

```text
weekend = 1
eta = beta0 + beta1 + beta2 kms
```

Then convert:

```text
p = exp(eta) / [1 + exp(eta)]
```

If an interaction is added:

```text
log(p / (1 - p)) = beta0 + beta1 weekend + beta2 kms + beta3(weekend x kms)
```

then for weekend journeys:

```text
eta = beta0 + beta1 + beta2 kms + beta3 kms
```

So the slope with respect to distance changes on weekends.

#### 6. Real-world actuarial case study

A life insurer wants to model whether customers renew a term insurance policy. The outcome is:

```text
renew = 1
not renew = 0
```

Possible predictors include age, premium increase, policy duration, channel, and whether the customer bought online.

A binomial GLM estimates the probability of renewal for each policyholder. The insurer can then identify groups with high lapse risk and adjust retention actions.

If an interaction between premium increase and sales channel is significant, it may mean customers from one channel are more sensitive to price increases than customers from another channel.

#### 7. Common mistakes

- Treating the linear predictor `eta` as the probability.
- Forgetting to convert from log-odds to probability.
- Using a normal model for binary data when a binomial GLM is needed.
- Interpreting an interaction term as a separate fixed effect only.
- Saying a coefficient is significant without checking its standard error.

#### 8. Revision checkpoint

Without notes, you should be able to write:

```text
logit(p) = log(p / (1 - p))
p = exp(eta) / [1 + exp(eta)]
```

and calculate predicted probabilities, simple binomial probabilities, coefficient significance, and interaction effects.

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- A GLM has three parts: a random component for the response distribution, a systematic component for the linear predictor, and a link function connecting the mean to the linear predictor.
- The link function determines coefficient interpretation. With a log link, a one-unit increase multiplies the mean by an exponential factor; with a logit link, a coefficient changes log-odds.
- Deviance compares fitted likelihoods. A smaller deviance can mean better fit, but model selection must balance fit, complexity, interpretability, and out-of-sample stability.

### Step-by-step working method

1. Choose the response distribution based on the data type.
2. Choose the link function and write the linear predictor.
3. Include exposure or offset terms where required.
4. Fit the model and interpret coefficients on the correct scale.
5. Use residuals, deviance, AIC, and validation to judge suitability.

### Extra practical actuarial examples

- Frequency example: Poisson GLM with log exposure offset estimates claim frequency per unit exposure.
- Binary example: a logit model can estimate probability of fraud, lapse, or claim occurrence.
- Severity example: gamma GLM with log link is often used for positive skewed claim amounts.

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

### Topic 26: Uniform Distribution Parameter Estimation and Boundary MLE

#### 1. Concept theory

For a continuous uniform distribution on `[0, theta]`, every value between 0 and `theta` is equally likely.

The parameter `theta` is the upper limit. If the largest observed value is 0.77, then `theta` cannot be below 0.77, because the observed data would be impossible.

This creates a boundary maximum likelihood problem. The MLE is not found by ordinary differentiation; it is found by looking at the allowed values of `theta`.

#### 2. Why actuaries care

Actuaries sometimes estimate boundary-like quantities such as maximum stress factors, maximum delay assumptions, or upper limits in simulation inputs.

The bigger lesson is exam-important: likelihoods can depend on the parameter through the support, and then the maximum may occur at a boundary.

#### 3. Mathematical derivation

Let:

```text
X1, X2, ..., Xn ~ Uniform(0, theta)
```

The density is:

```text
f(x) = 1 / theta, 0 <= x <= theta
```

The likelihood is:

```text
L(theta) = 1 / theta^n, if theta >= max(xi)
         = 0, otherwise
```

Since `1 / theta^n` decreases as `theta` increases, the maximum occurs at the smallest possible allowed value:

```text
theta_hat_MLE = max(xi)
```

For method of moments:

```text
E[X] = theta / 2
xbar = theta / 2
theta_hat_MOM = 2xbar
```

For `Z = max(X1, ..., Xn)`:

```text
FZ(z) = P(Z <= z) = (z / theta)^n
fZ(z) = n z^(n - 1) / theta^n
```

Then:

```text
E[Z] = n theta / (n + 1)
```

So:

```text
Bias(theta_hat_MLE) = E[Z] - theta
                    = -theta / (n + 1)
```

Also:

```text
MSE = Variance + Bias^2
```

#### 4. Simple example

Suppose the sample is:

```text
0.2, 0.4, 0.5
```

Then:

```text
xbar = 1.1 / 3 = 0.3667
theta_hat_MOM = 2(0.3667) = 0.7334
theta_hat_MLE = max(xi) = 0.5
```

The MOM estimate uses the sample average. The MLE uses the largest observation.

#### 5. Exam-style case study

In the November 2023 style question:

```text
sum x = 5.13
n = 10
max(xi) = 0.77
```

MOM:

```text
xbar = 5.13 / 10 = 0.513
theta_hat_MOM = 2xbar = 1.026
```

MLE:

```text
theta_hat_MLE = max(xi) = 0.77
```

Differentiation does not give the answer because the likelihood is zero below `max(xi)` and then decreases after `max(xi)`.

#### 6. Real-world actuarial case study

An insurer estimates the maximum size of a simple stress multiplier using observed stress factors. If the highest observed multiplier is 1.35, the MLE of the upper limit is 1.35.

But this is likely to underestimate the true maximum, because the largest observation is usually below the real upper bound. The actuary may add a margin or use an adjusted estimator.

#### 7. Common mistakes

- Forgetting the condition `theta >= max(xi)` in the likelihood.
- Trying to solve this MLE only by differentiation.
- Confusing `theta_hat_MOM = 2xbar` with `theta_hat_MLE = max(xi)`.
- Forgetting the MLE is biased downward.
- Saying MSE is only variance.

#### 8. Revision checkpoint

Without notes, you should be able to write:

```text
theta_hat_MOM = 2xbar
theta_hat_MLE = max(xi)
Bias(theta_hat_MLE) = -theta / (n + 1)
MSE = Variance + Bias^2
```

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- An estimator is a rule for turning data into a parameter estimate. Different estimators can have different bias, variance, mean squared error, and robustness.
- Maximum likelihood chooses the parameter that makes the observed data most probable under the model. Method of moments matches sample moments to theoretical moments.
- Large-sample theory can give approximate intervals, but boundary estimates, censored observations, small samples, and heavy tails can make approximations unreliable.

### Step-by-step working method

1. Write the model and parameter space.
2. Choose MOM, MLE, least squares, or another estimator.
3. Derive or solve the estimating equation.
4. Check bias, variance, consistency, and practical stability where possible.
5. Interpret the estimate in actuarial units.

### Extra practical actuarial examples

- Severity example: estimating a Pareto tail parameter from a few large losses may be highly unstable, so sensitivity testing is essential.
- Frequency example: the Poisson MLE for a constant rate is total claims divided by total exposure.
- Simulation example: inverse transform simulation uses uniform random numbers to generate losses from a fitted distribution for stress testing.

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

### Topic 27: Exponential Mean with Inverse-Gamma Prior and Credibility Form

#### 1. Concept theory

For exponential waiting-time data, the unknown mean `mu` can be estimated using Bayesian methods.

If the prior distribution for `mu` is inverse-gamma, the posterior distribution is also inverse-gamma. This is called conjugacy.

#### 2. Why actuaries care

This appears in waiting times, claim reporting delays, repair times, survival times, and time between rare events.

Bayesian estimation lets actuaries combine prior knowledge with new experience data.

#### 3. Mathematical derivation

Let:

```text
X1, X2, ..., Xn | mu ~ Exponential(mean mu)
```

The density is:

```text
f(x | mu) = (1 / mu) exp(-x / mu)
```

The likelihood is:

```text
L(mu) = mu^(-n) exp[-sum x / mu]
```

Use the prior:

```text
pi(mu) = theta^alpha exp(-theta / mu) / [mu^(alpha + 1) Gamma(alpha)]
```

Posterior is proportional to likelihood times prior:

```text
pi(mu | data) proportional to mu^(-(alpha + n + 1)) exp[-(theta + sum x) / mu]
```

Therefore:

```text
mu | data ~ Inverse-Gamma(alpha + n, theta + sum x)
```

The posterior mean is:

```text
mu_hat = (theta + sum x) / (alpha + n - 1)
```

This can be written as:

```text
mu_hat = Z xbar + (1 - Z) prior mean
```

where:

```text
prior mean = theta / (alpha - 1)
Z = n / (n + alpha - 1)
```

#### 4. Simple example

Suppose:

```text
alpha = 3
theta = 100
n = 8
sum x = 360
```

Prior mean:

```text
100 / (3 - 1) = 50
```

Sample mean:

```text
360 / 8 = 45
```

Bayesian estimate:

```text
(100 + 360) / (3 + 8 - 1) = 46
```

Credibility factor:

```text
Z = 8 / 10 = 0.8
```

#### 5. Exam-style case study

In the November 2023 style question:

```text
alpha = 1.5
theta = 40
n = 100
sum x = 9000
```

Prior mean:

```text
40 / (1.5 - 1) = 80
```

Sample mean:

```text
9000 / 100 = 90
```

Bayesian estimate:

```text
(40 + 9000) / (100 + 1.5 - 1)
= 9040 / 100.5
= 89.95 approximately
```

Credibility factor:

```text
Z = 100 / 100.5 = 0.995 approximately
```

The estimate is close to the sample mean because the sample size is large.

#### 6. Real-world actuarial case study

A reinsurer models the time between large catastrophe claims. Old experience suggests a mean waiting time of 80 days, but 100 recent observations have average waiting time 90 days.

The Bayesian estimate will be close to 90 because the data has high credibility. The prior still contributes, but only slightly.

#### 7. Common mistakes

- Confusing the exponential mean `mu` with the exponential rate.
- Forgetting the posterior shape is `alpha + n`.
- Using `alpha + n` instead of `alpha + n - 1` in the posterior mean.
- Saying conjugate means prior and posterior are exactly identical. It means same family.
- Forgetting that squared-error Bayes estimate is the posterior mean.

#### 8. Revision checkpoint

Without notes, you should be able to derive:

```text
L(mu) = mu^(-n) exp[-sum x / mu]
mu | data ~ Inverse-Gamma(alpha + n, theta + sum x)
mu_hat = (theta + sum x) / (alpha + n - 1)
Z = n / (n + alpha - 1)
```

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- Bayesian analysis combines prior information and data through the likelihood. The posterior distribution is proportional to prior times likelihood.
- Credibility theory has the same actuarial instinct: blend individual experience with collective experience. The more stable and relevant the individual data, the more credibility it receives.
- Conjugate priors are useful because the posterior stays in the same family. This makes updating transparent and exam calculations faster.

### Step-by-step working method

1. Write the prior distribution and identify its parameters.
2. Write the likelihood from the observed data.
3. Multiply prior and likelihood, keeping parameter-dependent terms.
4. Recognise the posterior family or normalise if required.
5. Use the posterior mean, mode, interval, or credibility form for the decision.

### Extra practical actuarial examples

- Renewal example: a beta prior for renewal probability can be updated after observing renewals and non-renewals.
- Claim count example: a gamma prior for a Poisson rate leads to a gamma posterior and a credibility-style blended estimate.
- Pricing example: a small employer group should not be priced only from its own volatile claims experience; credibility blends group experience with portfolio experience.

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

### Topic 28: Compound Poisson-Gamma Aggregate Loss and Capital Requirement

#### 1. Concept theory

Aggregate loss is the total loss from all claims:

```text
Y = X1 + X2 + ... + XN
```

where `N` is the number of claims and `Xi` is the size of each claim.

If `N` is Poisson and claim sizes are Gamma, the model is a compound Poisson-Gamma model.

#### 2. Why actuaries care

This is a core model for solvency capital, reserving, reinsurance, and risk management.

Capital requirements are often based on high percentiles, such as the 99.5th percentile of aggregate annual loss.

#### 3. Mathematical derivation

Given:

```text
Y = sum Xi from i = 1 to N
```

and `N` independent of the `Xi`:

```text
E[Y | N] = N E[X]
Var(Y | N) = N Var(X)
```

Using total expectation:

```text
E[Y] = E[E(Y | N)] = E[N]E[X]
```

Using total variance:

```text
Var(Y) = E[Var(Y | N)] + Var[E(Y | N)]
       = E[N]Var(X) + Var(N)(E[X])^2
```

If:

```text
N ~ Poisson(mu)
X ~ Gamma(alpha, scale beta)
```

then:

```text
E[N] = Var(N) = mu
E[X] = alpha beta
Var(X) = alpha beta^2
```

Therefore:

```text
E[Y] = mu alpha beta
Var(Y) = mu alpha beta^2(1 + alpha)
```

#### 4. Simple example

Suppose:

```text
N ~ Poisson(5)
X ~ Gamma(alpha = 2, scale beta = 10)
```

Then:

```text
E[Y] = 5 x 2 x 10 = 100
Var(Y) = 5 x 2 x 10^2 x (1 + 2) = 3000
```

#### 5. Exam-style case study

In the November 2023 style question:

```text
mu = 10
alpha = 2/3
beta = 15
```

Mean:

```text
E[Y] = 10 x (2/3) x 15 = 100
```

Variance:

```text
Var(Y) = 10 x (2/3) x 15^2 x (1 + 2/3)
       = 2500
```

If capital is defined by:

```text
P(Y <= y) = 0.995
```

then `y` is the 99.5th percentile of aggregate loss, found from a table or simulation.

#### 6. Real-world actuarial case study

A general insurer simulates annual flood losses. First it simulates the number of claims, then claim amounts, then total annual loss.

Repeating this many times gives the full aggregate loss distribution. The 99.5th percentile can be used as a solvency capital estimate.

#### 7. Common mistakes

- Treating claim count `N` as continuous.
- Forgetting that severity `X` is usually continuous.
- Missing the `Var[E(Y | N)]` part of total variance.
- Confusing the capital percentile with the expected loss.
- Forgetting that iid means both independent and identically distributed.

#### 8. Revision checkpoint

Without notes, you should be able to derive:

```text
E[Y] = E[N]E[X]
Var(Y) = E[N]Var(X) + Var(N)(E[X])^2
```

and apply:

```text
E[Y] = mu alpha beta
Var(Y) = mu alpha beta^2(1 + alpha)
```

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- Aggregate loss modelling combines frequency and severity. Frequency explains how often losses occur; severity explains how large they are when they occur. Their product gives expected loss only when the frequency and severity assumptions are aligned.
- The variance of aggregate loss has two sources: random claim sizes and random claim counts. This is why aggregate variance usually contains both a severity variance term and a squared mean severity term.
- Capital work is more demanding than pricing work. Pricing may focus on expected loss, while capital and reinsurance decisions need standard deviation, percentiles, stress tests, and tail scenarios.

### Step-by-step working method

1. Define the claim count variable and the individual severity variables.
2. Check independence and identical distribution assumptions.
3. Condition on the claim count first.
4. Use total expectation and total variance.
5. Translate the result into premium, reserve, or capital language.

### Extra practical actuarial examples

- Pricing example: expected annual loss equals expected claim count times expected claim size, then expenses, commission, profit margin, and risk loading are added.
- Capital example: two portfolios with the same expected loss can require different capital if one has low-frequency high-severity claims.
- Operational example: for cyber insurance, one ransomware event may produce legal, notification, downtime, and recovery costs, so severity volatility drives risk loading.

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

### Topic 29: F Distribution for Comparing Two Sample Variances

#### 1. Concept theory

The F distribution is used to compare two sample variances from independent normal samples.

It is built from the ratio of two independent chi-square variables divided by their degrees of freedom.

#### 2. Why actuaries care

Actuaries compare volatility across portfolios, products, funds, regions, and claim types.

Variance comparison matters for capital, risk appetite, reserving margins, and reinsurance.

#### 3. Mathematical derivation

For independent normal samples:

```text
(n1 - 1)S1^2 / sigma1^2 ~ chi-square(n1 - 1)
(n2 - 1)S2^2 / sigma2^2 ~ chi-square(n2 - 1)
```

Therefore:

```text
[(S1^2 / sigma1^2)] / [(S2^2 / sigma2^2)] ~ F(n1 - 1, n2 - 1)
```

If testing equal variances:

```text
sigma1^2 = sigma2^2
```

so:

```text
F = S1^2 / S2^2
```

#### 4. Simple example

Suppose:

```text
n1 = 10, n2 = 10
S1^2 = 25, S2^2 = 10
```

Then:

```text
F = 25 / 10 = 2.5
```

with degrees of freedom:

```text
9 and 9
```

#### 5. Exam-style case study

If a question asks for:

```text
P(Sx <= Sy)
```

rewrite it as:

```text
P(Sx^2 <= Sy^2)
```

Then use:

```text
F = (Sx^2 / sigma_x^2) / (Sy^2 / sigma_y^2)
```

and read the probability from the F distribution table.

#### 6. Real-world actuarial case study

An insurer compares claim severity volatility between private cars and commercial vehicles.

Even if average claims are similar, the commercial portfolio may need more capital if its variance is higher. An F test helps assess whether the observed difference in sample variances is statistically meaningful.

#### 7. Common mistakes

- Comparing standard deviations without squaring them.
- Forgetting degrees of freedom are `n1 - 1` and `n2 - 1`.
- Using an F test without independent samples.
- Assuming variance comparison tells us about mean comparison.
- Putting the wrong sample variance in the numerator.

#### 8. Revision checkpoint

Without notes, you should be able to write:

```text
[(S1^2 / sigma1^2)] / [(S2^2 / sigma2^2)] ~ F(n1 - 1, n2 - 1)
```

and use it to compare two independent normal sample variances.

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- Begin by checking support. A count model lives on non-negative integers, a severity model often lives on positive real values, and a transformed model may live on the whole real line after taking logs. Many exam errors happen before calculation because the wrong support is chosen.
- Separate the probability law from its moments. The probability law tells you every probability statement; the mean and variance are summaries. In actuarial work a model with the correct mean but the wrong tail can still be dangerous because solvency questions are tail questions.
- Use generating functions when sums are involved. MGFs, CGFs, and PGFs turn convolutions into algebra, but only under the conditions where they exist and where independence assumptions are valid.

### Step-by-step working method

1. State the random variable and its support.
2. Write the probability mass function, density, or distribution function.
3. Identify parameters and their actuarial meaning.
4. Calculate the required moment, probability, percentile, or transform.
5. Check whether the answer is sensible using units, range, and limiting cases.

### Extra practical actuarial examples

- Claim count example: if a motor portfolio has many zero-claim policies and a few multi-claim policies, compare Poisson and negative binomial assumptions by checking whether sample variance is close to, below, or above the sample mean.
- Severity example: for small routine medical claims a lognormal or gamma model may be adequate; for catastrophe-like property claims a heavier tail such as Pareto or Weibull may be needed.
- Exam example: after finding a probability, ask whether the question wanted an unconditional probability or a conditional probability after observing a claim, survival, or selection event.

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

### Topic 30: Standard Normal MGF and Zero Skewness of the Normal Distribution

#### 1. Concept theory

The moment generating function, or MGF, is a compact way to generate moments of a distribution.

For a standard normal random variable:

```text
Z ~ N(0, 1)
```

the MGF is:

```text
MZ(t) = exp(t^2 / 2)
```

Because this expansion contains only even powers of `t`, all odd central moments are zero. In particular:

```text
E[Z^3] = 0
```

This proves the normal distribution is symmetric and has zero skewness.

#### 2. Why actuaries care

The normal distribution appears in claim approximations, investment returns, credibility models, regression errors, and large-sample theory.

Actuaries must know not only how to use the normal distribution, but also why it is symmetric and why skewed insurance losses may need non-normal models.

#### 3. Mathematical derivation

Start with the standard normal density:

```text
f(z) = (1 / sqrt(2pi)) exp(-z^2 / 2)
```

The MGF is:

```text
MZ(t) = E[exp(tZ)]
      = integral from -infinity to infinity exp(tz)(1 / sqrt(2pi))exp(-z^2 / 2) dz
```

Combine the exponent:

```text
tz - z^2 / 2
= -1/2(z^2 - 2tz)
```

Complete the square:

```text
z^2 - 2tz = (z - t)^2 - t^2
```

So:

```text
tz - z^2 / 2 = -1/2(z - t)^2 + t^2 / 2
```

Therefore:

```text
MZ(t) = exp(t^2 / 2) integral (1 / sqrt(2pi)) exp[-(z - t)^2 / 2] dz
```

The integral is 1 because it is the total area under a normal density with mean `t` and variance 1.

So:

```text
MZ(t) = exp(t^2 / 2)
```

Taylor expansion:

```text
exp(t^2 / 2) = 1 + t^2 / 2 + t^4 / 8 + ...
```

There is no `t^3 / 3!` term, so:

```text
E[Z^3] = 0
```

For:

```text
X ~ N(mu, delta^2)
```

we have:

```text
X - mu = delta Z
```

So:

```text
E[(X - mu)^3] = E[(delta Z)^3]
              = delta^3 E[Z^3]
              = 0
```

Hence the skewness is zero.

#### 4. Simple example

If:

```text
X ~ N(100, 15^2)
```

then the distribution is symmetric about 100.

The third central moment is:

```text
E[(X - 100)^3] = 0
```

This does not mean there is no variation. It means positive deviations and negative deviations balance symmetrically.

#### 5. Exam-style case study

If an exam asks:

```text
Derive the MGF of standard normal using first principles.
```

write:

```text
MZ(t) = integral exp(tz)(1 / sqrt(2pi))exp(-z^2 / 2) dz
```

then complete the square:

```text
tz - z^2 / 2 = -1/2(z - t)^2 + t^2 / 2
```

and conclude:

```text
MZ(t) = exp(t^2 / 2)
```

If the next part asks for skewness, use the expansion and state:

```text
E[Z^3] = 0
E[(X - mu)^3] = delta^3 E[Z^3] = 0
```

#### 6. Real-world actuarial case study

An investment actuary models annual equity index returns as normal. The normal model assumes upside and downside deviations around the mean are symmetric.

But insurance claim sizes are often right-skewed: many small claims and a few very large claims. For those, a Gamma, lognormal, Pareto, or compound model may be more realistic than a normal model.

#### 7. Common mistakes

- Forgetting to complete the square in the exponent.
- Writing the standard normal MGF as `exp(t)` or `exp(t^2)`.
- Confusing raw moment `E[X^3]` with central moment `E[(X - mu)^3]`.
- Saying zero skewness means zero variance.
- Assuming every symmetric-looking data set is exactly normal.

#### 8. Revision checkpoint

Without notes, you should be able to derive:

```text
MZ(t) = exp(t^2 / 2)
E[Z^3] = 0
E[(X - mu)^3] = 0 for X ~ N(mu, delta^2)
```

and explain why this means the normal distribution has zero skewness.

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- Begin by checking support. A count model lives on non-negative integers, a severity model often lives on positive real values, and a transformed model may live on the whole real line after taking logs. Many exam errors happen before calculation because the wrong support is chosen.
- Separate the probability law from its moments. The probability law tells you every probability statement; the mean and variance are summaries. In actuarial work a model with the correct mean but the wrong tail can still be dangerous because solvency questions are tail questions.
- Use generating functions when sums are involved. MGFs, CGFs, and PGFs turn convolutions into algebra, but only under the conditions where they exist and where independence assumptions are valid.

### Step-by-step working method

1. State the random variable and its support.
2. Write the probability mass function, density, or distribution function.
3. Identify parameters and their actuarial meaning.
4. Calculate the required moment, probability, percentile, or transform.
5. Check whether the answer is sensible using units, range, and limiting cases.

### Extra practical actuarial examples

- Claim count example: if a motor portfolio has many zero-claim policies and a few multi-claim policies, compare Poisson and negative binomial assumptions by checking whether sample variance is close to, below, or above the sample mean.
- Severity example: for small routine medical claims a lognormal or gamma model may be adequate; for catastrophe-like property claims a heavier tail such as Pareto or Weibull may be needed.
- Exam example: after finding a probability, ask whether the question wanted an unconditional probability or a conditional probability after observing a claim, survival, or selection event.

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

### Topic 31: Binomial Normal Approximation, Continuity Correction, P-Values, and Critical Values

#### 1. Concept theory

A binomial random variable counts the number of successes in a fixed number of independent trials.

When `n` is reasonably large, the binomial distribution can be approximated by a normal distribution:

```text
N ~ Binomial(n, p)
N approximately Normal(np, np(1 - p))
```

Because the binomial is discrete and the normal is continuous, we use a continuity correction.

#### 2. Why actuaries care

Binomial models appear in lapse counts, mortality counts, claim occurrence, conversion rates, fraud flags, and medical test outcomes.

The normal approximation is useful for quick hypothesis tests and exam calculations when exact binomial probabilities are hard to compute.

#### 3. Mathematical derivation

If:

```text
N ~ Binomial(n, p)
```

then:

```text
E[N] = np
Var(N) = np(1 - p)
```

For large `n`:

```text
Z = (N - np) / sqrt(np(1 - p)) approximately N(0,1)
```

Continuity correction examples:

```text
P(N > 40) = P(N >= 41) approximately P(Y > 40.5)
P(N >= 50) approximately P(Y > 49.5)
P(N <= 40) approximately P(Y < 40.5)
```

For a one-sided test:

```text
H0: p = p0
H1: p > p0
```

the p-value is:

```text
P(N >= observed value under H0)
```

using the null distribution.

Critical value method:

```text
Reject H0 if observed count is at least c
```

where `c` is chosen so:

```text
P(N >= c | H0) <= significance level
```

#### 4. Simple example

Suppose:

```text
N ~ Binomial(100, 0.5)
```

Then:

```text
mean = 50
variance = 25
standard deviation = 5
```

Approximate:

```text
P(N > 60) = P(N >= 61)
```

Using continuity correction:

```text
P(N > 60) approximately P(Y > 60.5)
```

Standardise:

```text
Z = (60.5 - 50) / 5 = 2.1
```

So:

```text
P(N > 60) approximately P(Z > 2.1)
```

#### 5. Exam-style case study

In the December 2022 coin-toss question:

```text
n = 85
observed heads = 40
```

Estimate:

```text
p_hat = 40 / 85
```

For testing:

```text
H0: p = 0.5
H1: p > 0.5
```

under `H0`:

```text
mean = 85(0.5) = 42.5
variance = 85(0.5)(0.5) = 21.25
```

To approximate:

```text
P(N > 40) = P(N >= 41)
```

use:

```text
P(Y >= 40.5)
```

The observed number 40 is below the null mean 42.5, so there is no evidence for `p > 0.5`.

For a 5 percent upper-tail critical value, solve:

```text
(c - 0.5 - 42.5) / sqrt(21.25) = 1.645
```

and reject only for sufficiently large counts.

#### 6. Real-world actuarial case study

An insurer tests whether more than 50 percent of policyholders choose a new digital claim process. Out of 500 policyholders, 270 use it.

The actuary can use a binomial test or normal approximation to check whether adoption is significantly above 50 percent. This informs whether the company should invest further in digital servicing.

#### 7. Common mistakes

- Forgetting the continuity correction.
- Using `p_hat` instead of `p0` when calculating the test distribution under `H0`.
- Testing the wrong tail.
- Rejecting just because the observed proportion is different from 0.5.
- Confusing p-value with the estimated probability of success.

#### 8. Revision checkpoint

Without notes, you should be able to write:

```text
N ~ Binomial(n, p)
E[N] = np
Var(N) = np(1 - p)
Z = (N - np) / sqrt(np(1 - p))
```

and use continuity correction for `>`, `>=`, `<`, and `<=` probability questions.

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- Begin by checking support. A count model lives on non-negative integers, a severity model often lives on positive real values, and a transformed model may live on the whole real line after taking logs. Many exam errors happen before calculation because the wrong support is chosen.
- Separate the probability law from its moments. The probability law tells you every probability statement; the mean and variance are summaries. In actuarial work a model with the correct mean but the wrong tail can still be dangerous because solvency questions are tail questions.
- Use generating functions when sums are involved. MGFs, CGFs, and PGFs turn convolutions into algebra, but only under the conditions where they exist and where independence assumptions are valid.

### Step-by-step working method

1. State the random variable and its support.
2. Write the probability mass function, density, or distribution function.
3. Identify parameters and their actuarial meaning.
4. Calculate the required moment, probability, percentile, or transform.
5. Check whether the answer is sensible using units, range, and limiting cases.

### Extra practical actuarial examples

- Claim count example: if a motor portfolio has many zero-claim policies and a few multi-claim policies, compare Poisson and negative binomial assumptions by checking whether sample variance is close to, below, or above the sample mean.
- Severity example: for small routine medical claims a lognormal or gamma model may be adequate; for catastrophe-like property claims a heavier tail such as Pareto or Weibull may be needed.
- Exam example: after finding a probability, ask whether the question wanted an unconditional probability or a conditional probability after observing a claim, survival, or selection event.

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

### Topic 32: Gamma-Poisson Bayesian Updating and EBCT Model 1 Comparison

#### 1. Concept theory

For Poisson claim counts, a Gamma prior for the Poisson mean is conjugate. This means the posterior distribution is also Gamma.

Empirical Bayes Credibility Theory Model 1, or EBCT Model 1, estimates credibility from a group of similar risks instead of starting with a fixed prior distribution.

Both methods blend individual experience with external or collective information, but they do it differently.

#### 2. Why actuaries care

Actuaries often estimate claim frequency for risk categories with limited data.

Examples include dog insurance categories, motor vehicle classes, health segments, or regional claim counts.

Bayesian updating and EBCT help avoid overreacting to small samples.

#### 3. Mathematical derivation

#### Gamma-Poisson Bayesian update

Let:

```text
N_j | theta ~ Poisson(theta), j = 1, ..., m
```

The likelihood is:

```text
L(theta) proportional to exp(-m theta) theta^(sum n_j)
```

Let the prior be:

```text
theta ~ Gamma(alpha, rate lambda)
```

with:

```text
E[theta] = alpha / lambda
Var(theta) = alpha / lambda^2
```

Posterior:

```text
theta | data ~ Gamma(alpha + sum n_j, rate lambda + m)
```

Under quadratic loss, the Bayes estimate is the posterior mean:

```text
theta_hat = (alpha + sum n_j) / (lambda + m)
```

#### EBCT Model 1

For risk `i`, with `m` years of experience:

```text
estimate_i = Z xbar_i + (1 - Z) overall mean
```

where:

```text
Z = m / (m + EPV / VHM)
```

Here:

```text
EPV = expected process variance
VHM = variance of hypothetical means
```

#### 4. Simple example

Suppose a category has:

```text
5 years of claims: 8, 10, 9, 11, 12
sum = 50
```

Prior:

```text
theta ~ Gamma(alpha = 20, rate lambda = 2)
```

Posterior:

```text
theta | data ~ Gamma(20 + 50, 2 + 5)
```

Bayes estimate:

```text
70 / 7 = 10
```

#### 5. Exam-style case study

In the December 2022 dog insurance question, prior beliefs for category 1 are:

```text
mean = 50
variance = 25
```

For a Gamma prior with rate `lambda`:

```text
mean = alpha / lambda
variance = alpha / lambda^2
```

So:

```text
lambda = mean / variance = 50 / 25 = 2
alpha = mean x lambda = 50 x 2 = 100
```

Category 1 has:

```text
sum claims = 232
m = 5
```

Posterior:

```text
theta_1 | data ~ Gamma(100 + 232, rate 2 + 5)
```

Bayes estimate:

```text
332 / 7 = 47.43
```

For EBCT Model 1, the estimate uses:

```text
Z category mean + (1 - Z) overall mean
```

In the solution, `Z = 0.8411`, so the category estimates are pulled toward the overall portfolio mean.

#### 6. Real-world actuarial case study

A pet insurer has three dog categories. Large pedigree dogs have their own claim history, but the experience is only five years long.

Bayesian updating can use a prior belief about large pedigree claims. EBCT can use experience from all dog categories to estimate how credible each category's own average is.

If exposure has grown each year, a fixed Poisson mean may be wrong. A better model would allow:

```text
mean claims = exposure x theta
```

or use EBCT Model 2 with exposure volumes.

#### 7. Common mistakes

- Confusing Gamma rate and Gamma scale parameterisation.
- Forgetting to add the number of years to the prior rate.
- Using only category 1 data in EBCT Model 1 when all categories are needed.
- Saying Bayesian and EBCT methods are identical.
- Ignoring exposure growth over time.

#### 8. Revision checkpoint

Without notes, you should be able to write:

```text
theta | data ~ Gamma(alpha + sum n, rate lambda + m)
Bayes estimate = (alpha + sum n) / (lambda + m)
EBCT estimate = Z xbar_i + (1 - Z) overall mean
Z = m / (m + EPV / VHM)
```

and explain the difference between fixed-prior Bayesian updating and empirical Bayes credibility.

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- Bayesian analysis combines prior information and data through the likelihood. The posterior distribution is proportional to prior times likelihood.
- Credibility theory has the same actuarial instinct: blend individual experience with collective experience. The more stable and relevant the individual data, the more credibility it receives.
- Conjugate priors are useful because the posterior stays in the same family. This makes updating transparent and exam calculations faster.

### Step-by-step working method

1. Write the prior distribution and identify its parameters.
2. Write the likelihood from the observed data.
3. Multiply prior and likelihood, keeping parameter-dependent terms.
4. Recognise the posterior family or normalise if required.
5. Use the posterior mean, mode, interval, or credibility form for the decision.

### Extra practical actuarial examples

- Renewal example: a beta prior for renewal probability can be updated after observing renewals and non-renewals.
- Claim count example: a gamma prior for a Poisson rate leads to a gamma posterior and a credibility-style blended estimate.
- Pricing example: a small employer group should not be priced only from its own volatile claims experience; credibility blends group experience with portfolio experience.

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

### Topic 33: Poisson Aggregation, Negative Binomial Counts, and Exponential/Erlang Waiting Times

#### 1. Concept theory

Several exam questions use standard distribution-recognition:

- sum of independent Poisson variables is Poisson
- binomial counts deaths or successes in a fixed group
- negative binomial counts trials needed to reach a fixed number of successes
- sum of independent exponential waiting times is Gamma/Erlang

Recognising the distribution is often half the marks.

#### 2. Why actuaries care

Actuaries model claim counts, deaths, large claims, and waiting times. These distributions appear naturally in insurance operations and risk modelling.

#### 3. Mathematical derivation

If:

```text
Xi ~ Poisson(lambda_i), independent
```

then:

```text
sum Xi ~ Poisson(sum lambda_i)
```

For 100 policyholders each with claim count:

```text
Xi ~ Poisson(0.03)
```

total claims:

```text
S ~ Poisson(100 x 0.03) = Poisson(3)
```

Negative binomial form for the number of trials `N` needed to get `r` successes:

```text
P(N = n) = C(n - 1, r - 1) p^r (1 - p)^(n - r), n = r, r+1, ...
```

For exponential waiting time with mean `mu`:

```text
MX(t) = 1 / (1 - mu t), t < 1 / mu
```

If:

```text
Y = X1 + X2 + ... + Xk
```

where the `Xi` are independent exponentials with mean `mu`, then:

```text
MY(t) = [1 / (1 - mu t)]^k
```

and:

```text
Y ~ Gamma(shape k, scale mu)
```

#### 4. Simple example

If 20 policies each have annual claim count `Poisson(0.1)`, total claims are:

```text
Poisson(20 x 0.1) = Poisson(2)
```

Probability of fewer than 3 claims:

```text
P(S < 3) = P(0) + P(1) + P(2)
```

#### 5. Exam-style case study

July 2022 patterns:

For 100 policyholders:

```text
S ~ Poisson(3)
P(S < 6) = sum from k = 0 to 5 of exp(-3)3^k/k!
```

For claims examined up to and including the 4th claim exceeding 50,000, with exceedance probability 0.4:

```text
N ~ Negative Binomial(r = 4, p = 0.4)
```

The event "less than 7 claims examined" means:

```text
N < 7, so N = 4, 5, 6
```

For deaths among 1000 policyholders:

```text
D ~ Binomial(1000, 0.015)
```

#### 6. Real-world actuarial case study

A health insurer tracks number of claims per year, time between claims, and number of files reviewed until four high-value claims are found.

Poisson handles total claim counts, exponential/Gamma handles waiting times, and negative binomial handles search-until-target questions.

#### 7. Common mistakes

- Forgetting to multiply Poisson mean by number of independent policyholders.
- Using binomial when the number of trials is random until the 4th success.
- Confusing negative binomial `N` as failures only versus total trials.
- Forgetting MGF of a sum is the product of MGFs for independent variables.

#### 8. Revision checkpoint

Without notes, you should be able to identify:

```text
sum Poisson -> Poisson(sum means)
fixed trials with deaths -> Binomial
trials until r successes -> Negative Binomial
sum exponentials -> Gamma/Erlang
```

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- Begin by checking support. A count model lives on non-negative integers, a severity model often lives on positive real values, and a transformed model may live on the whole real line after taking logs. Many exam errors happen before calculation because the wrong support is chosen.
- Separate the probability law from its moments. The probability law tells you every probability statement; the mean and variance are summaries. In actuarial work a model with the correct mean but the wrong tail can still be dangerous because solvency questions are tail questions.
- Use generating functions when sums are involved. MGFs, CGFs, and PGFs turn convolutions into algebra, but only under the conditions where they exist and where independence assumptions are valid.

### Step-by-step working method

1. State the random variable and its support.
2. Write the probability mass function, density, or distribution function.
3. Identify parameters and their actuarial meaning.
4. Calculate the required moment, probability, percentile, or transform.
5. Check whether the answer is sensible using units, range, and limiting cases.

### Extra practical actuarial examples

- Claim count example: if a motor portfolio has many zero-claim policies and a few multi-claim policies, compare Poisson and negative binomial assumptions by checking whether sample variance is close to, below, or above the sample mean.
- Severity example: for small routine medical claims a lognormal or gamma model may be adequate; for catastrophe-like property claims a heavier tail such as Pareto or Weibull may be needed.
- Exam example: after finding a probability, ask whether the question wanted an unconditional probability or a conditional probability after observing a claim, survival, or selection event.

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

### Topic 34: Binomial Exponential Family and Canonical Logit GLM

#### 1. Concept theory

The binomial distribution belongs to the exponential family. Its canonical link in a GLM is the logit link:

```text
logit(mu) = log(mu / (1 - mu))
```

This is used when modelling probabilities such as passing an exam, dying, lapsing, or making a claim.

#### 2. Why actuaries care

Binomial GLMs are used in mortality, lapse, fraud, underwriting acceptance, claim occurrence, and conversion modelling.

The July 2022 exam uses this to model probability of passing based on assignments, mock score, and tutorial attendance.

#### 3. Mathematical derivation

For:

```text
Z ~ Binomial(n, mu)
```

the probability is:

```text
P(Z = z) = C(n,z) mu^z (1 - mu)^(n - z)
```

Let:

```text
Y = Z / n
```

Ignoring constants, rewrite:

```text
P(Z = z) = exp[z log(mu / (1 - mu)) + n log(1 - mu) + constant]
```

Natural parameter:

```text
theta = log(mu / (1 - mu))
```

Invert it:

```text
mu = exp(theta) / [1 + exp(theta)]
```

For a fitted GLM:

```text
theta = eta = alpha_i + beta1 N + beta2 S
```

#### 4. Simple example

If:

```text
eta = 0
```

then:

```text
mu = exp(0) / [1 + exp(0)] = 1 / 2
```

If:

```text
eta = 1
```

then:

```text
mu = exp(1) / [1 + exp(1)] = 0.731
```

#### 5. Exam-style case study

July 2022 fitted model:

```text
eta = alpha_Y + beta1 N + beta2 S
```

for a student who attends tutorials, submits 4 assignments, and scores 65:

```text
eta = -1.501 + 0.5459(4) + 0.0251(65)
```

Then:

```text
p = exp(eta) / [1 + exp(eta)]
```

For significance of number of assignments:

```text
estimate / standard error = 0.5459 / 0.08352
```

This is much greater than 2, so it is significant by the common exam rule.

#### 6. Real-world actuarial case study

A life insurer models whether an applicant accepts an online quote. Predictors include number of reminders, premium amount, age, and distribution channel.

The binomial logit GLM estimates the probability of acceptance and identifies significant drivers.

#### 7. Common mistakes

- Forgetting to use the tutorial-specific intercept.
- Treating `eta` as the probability.
- Not converting logit to probability.
- Ignoring standard errors when judging significance.
- Confusing binomial response count `Z` with observed proportion `Y = Z/n`.

#### 8. Revision checkpoint

Without notes, you should be able to write:

```text
theta = log(mu / (1 - mu))
mu = exp(eta) / [1 + exp(eta)]
```

and use a fitted binomial GLM to calculate a predicted probability.

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- A GLM has three parts: a random component for the response distribution, a systematic component for the linear predictor, and a link function connecting the mean to the linear predictor.
- The link function determines coefficient interpretation. With a log link, a one-unit increase multiplies the mean by an exponential factor; with a logit link, a coefficient changes log-odds.
- Deviance compares fitted likelihoods. A smaller deviance can mean better fit, but model selection must balance fit, complexity, interpretability, and out-of-sample stability.

### Step-by-step working method

1. Choose the response distribution based on the data type.
2. Choose the link function and write the linear predictor.
3. Include exposure or offset terms where required.
4. Fit the model and interpret coefficients on the correct scale.
5. Use residuals, deviance, AIC, and validation to judge suitability.

### Extra practical actuarial examples

- Frequency example: Poisson GLM with log exposure offset estimates claim frequency per unit exposure.
- Binary example: a logit model can estimate probability of fraud, lapse, or claim occurrence.
- Severity example: gamma GLM with log link is often used for positive skewed claim amounts.

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

### Topic 35: Conditional Excess Distribution and Memoryless Exponential Behaviour

#### 1. Concept theory

The conditional distribution of a random variable given it exceeds a threshold answers:

```text
If the value is already above 4, how much larger do we expect it to be?
```

For exponential distributions, the excess over a threshold has the same distribution as the original waiting time. This is called the memoryless property.

#### 2. Why actuaries care

This appears in waiting times, claim delays, survival times, and large claim thresholds.

For example, if a claim has already remained open for 4 months, the expected additional waiting time may still follow the same exponential pattern under a memoryless assumption.

#### 3. Mathematical derivation

From the July 2022 joint density:

```text
f(x,y) = 3 exp[-(x + 3y)], x > 0, y > 0
```

Marginal density of `Y`:

```text
fY(y) = integral from 0 to infinity 3 exp[-(x + 3y)] dx
      = 3 exp(-3y) integral from 0 to infinity exp(-x) dx
      = 3 exp(-3y)
```

So:

```text
Y ~ Exponential(rate 3)
```

For `y > 4`:

```text
f(y | Y > 4) = fY(y) / P(Y > 4)
```

Since:

```text
P(Y > 4) = exp(-12)
```

we get:

```text
f(y | Y > 4) = 3 exp(-3y) / exp(-12)
             = 3 exp(12 - 3y), y > 4
```

Let:

```text
t = y - 4
```

Then:

```text
f(t) = 3 exp(-3t), t > 0
```

So the excess `Y - 4` is exponential with rate 3.

#### 4. Simple example

If:

```text
Y ~ Exponential(rate 2)
```

then:

```text
E[Y] = 1 / 2
```

Memoryless property:

```text
E[Y | Y > 5] = 5 + 1/2
```

The expected total value, given it has exceeded 5, is 5 plus the usual mean excess.

#### 5. Exam-style case study

For:

```text
Y ~ Exponential(rate 3)
```

the conditional expectation is:

```text
E[Y | Y > 4] = 4 + 1/3
```

The solution writes it as:

```text
integral 0 to infinity 3t exp(-3t) dt
+ integral 0 to infinity 12 exp(-3t) dt
```

This is the same as:

```text
E[t + 4] = E[t] + 4
```

where:

```text
t ~ Exponential(rate 3)
```

#### 6. Real-world actuarial case study

An income protection claim has already lasted 12 months. If claim duration is modelled exponentially, the expected remaining duration does not depend on the 12 months already elapsed.

This is mathematically convenient, but often unrealistic. Real disability recovery rates may change with duration.

#### 7. Common mistakes

- Forgetting to divide by `P(Y > 4)` when forming the conditional density.
- Integrating from 0 instead of 4 without changing variable.
- Treating `E[Y | Y > 4]` as just `E[Y]`.
- Missing the memoryless interpretation.
- Assuming memoryless behaviour is realistic for all insurance durations.

#### 8. Revision checkpoint

Without notes, you should be able to derive:

```text
f(y | Y > a) = fY(y) / P(Y > a), y > a
```

and for exponential variables:

```text
E[Y | Y > a] = a + 1 / rate
```

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- Begin by checking support. A count model lives on non-negative integers, a severity model often lives on positive real values, and a transformed model may live on the whole real line after taking logs. Many exam errors happen before calculation because the wrong support is chosen.
- Separate the probability law from its moments. The probability law tells you every probability statement; the mean and variance are summaries. In actuarial work a model with the correct mean but the wrong tail can still be dangerous because solvency questions are tail questions.
- Use generating functions when sums are involved. MGFs, CGFs, and PGFs turn convolutions into algebra, but only under the conditions where they exist and where independence assumptions are valid.

### Step-by-step working method

1. State the random variable and its support.
2. Write the probability mass function, density, or distribution function.
3. Identify parameters and their actuarial meaning.
4. Calculate the required moment, probability, percentile, or transform.
5. Check whether the answer is sensible using units, range, and limiting cases.

### Extra practical actuarial examples

- Claim count example: if a motor portfolio has many zero-claim policies and a few multi-claim policies, compare Poisson and negative binomial assumptions by checking whether sample variance is close to, below, or above the sample mean.
- Severity example: for small routine medical claims a lognormal or gamma model may be adequate; for catastrophe-like property claims a heavier tail such as Pareto or Weibull may be needed.
- Exam example: after finding a probability, ask whether the question wanted an unconditional probability or a conditional probability after observing a claim, survival, or selection event.

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

### Topic 36: Exact Bernoulli Likelihood, Binomial Likelihood, and Rare-Claim Proportion CI

#### 1. Concept theory

There is an important difference between:

- observing exactly which policies claimed
- observing only the total number of claims

If policy-wise outcomes are known, the likelihood is the product of Bernoulli probabilities for the exact observed pattern.

If only the total count is known, the binomial coefficient appears because many arrangements could produce the same count.

#### 2. Why actuaries care

This distinction appears in mortality, accident claims, lapse studies, and underwriting outcomes.

Actuaries often estimate a rare probability from a large portfolio, such as accidental death probability.

#### 3. Mathematical derivation

Suppose:

```text
n policies
x claims
claim probability q
```

If exact policy-wise claim status is known, the likelihood is:

```text
L(q) = q^x(1 - q)^(n - x)
```

If only the total number of claims is known:

```text
P(X = x) = C(n,x) q^x(1 - q)^(n - x)
```

The binomial coefficient does not affect the MLE because it does not involve `q`, but it matters when writing the probability of a count.

Estimator:

```text
q_hat = x / n
```

Approximate variance:

```text
Var(q_hat) = q(1 - q) / n
```

Use plug-in estimate:

```text
SE(q_hat) = sqrt[q_hat(1 - q_hat) / n]
```

Approximate 95 percent confidence interval:

```text
q_hat +/- 1.96 SE(q_hat)
```

For very rare probabilities, truncate the lower bound at zero if the normal approximation gives a negative value.

#### 4. Simple example

Suppose:

```text
n = 1000
x = 2
```

Then:

```text
q_hat = 2 / 1000 = 0.002
```

Approximate standard error:

```text
sqrt(0.002 x 0.998 / 1000)
```

The lower confidence limit may be close to zero.

#### 5. Exam-style case study

In July 2022:

```text
n = 10000
x = 3
```

Estimator:

```text
q_hat = 3 / 10000 = 0.0003 = 0.3 per mille
```

Approximate 95 percent interval:

```text
q_hat +/- 1.96 sqrt[q_hat(1 - q_hat) / n]
```

This gives approximately:

```text
(0 per mille, 0.64 per mille)
```

If the null hypothesis is:

```text
q = 0.2 per mille
```

then 0.2 per mille lies inside the interval, so there is insufficient evidence to reject it at the 5 percent level.

#### 6. Real-world actuarial case study

A personal accident insurer observes 3 accidental death claims out of 10,000 policies. The crude estimate is 0.3 per mille, but the uncertainty is large because deaths are rare.

The actuary should not overreact to the small difference between 0.3 per mille and 0.2 per mille without checking sampling uncertainty.

#### 7. Common mistakes

- Including `C(n,x)` when the exact policy-wise pattern is the likelihood requested.
- Forgetting that `C(n,x)` does not affect the MLE.
- Reporting a negative lower confidence limit for a probability.
- Mixing probability units and per-mille units.
- Rejecting a null value that lies inside the confidence interval.

#### 8. Revision checkpoint

Without notes, you should be able to distinguish:

```text
Exact Bernoulli likelihood = q^x(1 - q)^(n - x)
Binomial count probability = C(n,x)q^x(1 - q)^(n - x)
q_hat = x / n
SE(q_hat) = sqrt[q_hat(1 - q_hat) / n]
```

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- An estimator is a rule for turning data into a parameter estimate. Different estimators can have different bias, variance, mean squared error, and robustness.
- Maximum likelihood chooses the parameter that makes the observed data most probable under the model. Method of moments matches sample moments to theoretical moments.
- Large-sample theory can give approximate intervals, but boundary estimates, censored observations, small samples, and heavy tails can make approximations unreliable.

### Step-by-step working method

1. Write the model and parameter space.
2. Choose MOM, MLE, least squares, or another estimator.
3. Derive or solve the estimating equation.
4. Check bias, variance, consistency, and practical stability where possible.
5. Interpret the estimate in actuarial units.

### Extra practical actuarial examples

- Severity example: estimating a Pareto tail parameter from a few large losses may be highly unstable, so sensitivity testing is essential.
- Frequency example: the Poisson MLE for a constant rate is total claims divided by total exposure.
- Simulation example: inverse transform simulation uses uniform random numbers to generate losses from a fitted distribution for stress testing.

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

### Topic 37: Gamma MGF, Chi-Square Scaling, and Sums of Gamma Variables

#### 1. Concept theory

The Gamma distribution is often used for positive claim amounts.

If:

```text
X ~ Gamma(alpha, rate lambda)
```

then:

```text
MX(t) = (1 - t/lambda)^(-alpha)
```

Equivalently, if the scale is `theta = 1/lambda`:

```text
MX(t) = (1 - theta t)^(-alpha)
```

A chi-square distribution is a special Gamma distribution.

#### 2. Why actuaries care

Gamma models are used for claim severity, aggregate losses, waiting-time sums, and GLM response distributions.

The March 2022 paper uses Gamma MGFs to test whether a transformed claim amount follows a chi-square distribution.

#### 3. Mathematical derivation

If:

```text
X ~ Gamma(alpha, rate lambda)
```

then:

```text
MX(t) = (lambda / (lambda - t))^alpha
      = (1 - t/lambda)^(-alpha)
```

If `Y = cX`, then:

```text
MY(t) = MX(ct)
```

For:

```text
X ~ Gamma(5, rate 1/8)
```

the scale is 8, so:

```text
MX(t) = (1 - 8t)^(-5)
```

Let:

```text
Y = X / 4
```

Then:

```text
MY(t) = MX(t/4)
      = (1 - 8t/4)^(-5)
      = (1 - 2t)^(-5)
```

For chi-square with `v` degrees of freedom:

```text
M(t) = (1 - 2t)^(-v/2)
```

So:

```text
X / 4 ~ chi-square(10)
```

Sums of independent Gamma variables are Gamma only when they have the same rate parameter.

#### 4. Simple example

If:

```text
X ~ Gamma(3, rate 2)
Y ~ Gamma(4, rate 2)
```

and `X` and `Y` are independent:

```text
X + Y ~ Gamma(7, rate 2)
```

But if the rates differ, the sum is generally not Gamma.

#### 5. Exam-style case study

March 2022 gives:

```text
X ~ Gamma(5, rate 1/8)
Y ~ Gamma(3, rate 1/4)
```

The rates differ:

```text
1/8 != 1/4
```

So the analyst's claim:

```text
X + Y ~ Gamma(8, rate 3/8)
```

is not valid.

Use MGFs:

```text
MX+Y(t) = MX(t)MY(t)
        = (1 - 8t)^(-5)(1 - 4t)^(-3)
```

This is not equal to:

```text
(1 - t/(3/8))^(-8)
```

#### 6. Real-world actuarial case study

An insurer models outpatient claims and surgical claims using Gamma distributions with different severity scales.

The total of the two claim types is not automatically Gamma. Assuming it is Gamma just because both components are Gamma can understate or distort tail probabilities.

#### 7. Common mistakes

- Confusing rate and scale.
- Forgetting to transform MGFs correctly for `cX`.
- Assuming sums of Gamma variables are always Gamma.
- Using the wrong chi-square MGF.
- Forgetting claim amounts may be in units such as INR 1,000.

#### 8. Revision checkpoint

Without notes, you should be able to write:

```text
MX(t) = (1 - t/lambda)^(-alpha)
M(cX)(t) = MX(ct)
chi-square(v) MGF = (1 - 2t)^(-v/2)
```

and explain why Gamma sums require a common rate.

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- Begin by checking support. A count model lives on non-negative integers, a severity model often lives on positive real values, and a transformed model may live on the whole real line after taking logs. Many exam errors happen before calculation because the wrong support is chosen.
- Separate the probability law from its moments. The probability law tells you every probability statement; the mean and variance are summaries. In actuarial work a model with the correct mean but the wrong tail can still be dangerous because solvency questions are tail questions.
- Use generating functions when sums are involved. MGFs, CGFs, and PGFs turn convolutions into algebra, but only under the conditions where they exist and where independence assumptions are valid.

### Step-by-step working method

1. State the random variable and its support.
2. Write the probability mass function, density, or distribution function.
3. Identify parameters and their actuarial meaning.
4. Calculate the required moment, probability, percentile, or transform.
5. Check whether the answer is sensible using units, range, and limiting cases.

### Extra practical actuarial examples

- Claim count example: if a motor portfolio has many zero-claim policies and a few multi-claim policies, compare Poisson and negative binomial assumptions by checking whether sample variance is close to, below, or above the sample mean.
- Severity example: for small routine medical claims a lognormal or gamma model may be adequate; for catastrophe-like property claims a heavier tail such as Pareto or Weibull may be needed.
- Exam example: after finding a probability, ask whether the question wanted an unconditional probability or a conditional probability after observing a claim, survival, or selection event.

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

### Topic 38: Normal-Normal Bayesian Credibility Factor Intuition

#### 1. Concept theory

In a normal-normal Bayesian model, the credibility factor measures how much weight is given to sample data versus prior belief.

More reliable data means higher credibility. More uncertainty in the prior also means higher credibility for the data.

#### 2. Why actuaries care

This appears when estimating average claim amounts, average portfolio loss, mortality improvement, or expense assumptions using both prior judgement and observed experience.

#### 3. Mathematical derivation

Suppose:

```text
Xbar | mu ~ N(mu, s^2 / n)
mu ~ N(mu0, sigma^2)
```

Posterior mean can be written:

```text
posterior mean = Z xbar + (1 - Z)mu0
```

where:

```text
Z = sigma^2 / (sigma^2 + s^2 / n)
```

Equivalent form:

```text
Z = n sigma^2 / (n sigma^2 + s^2)
```

Relationship:

- as `sigma^2` increases, prior is less certain, so `Z` increases
- as `s^2` increases, sample data is noisier, so `Z` decreases
- `mu0` affects the posterior mean level, but not the credibility factor

#### 4. Simple example

If:

```text
n = 10
sigma^2 = 100
s^2 = 25
```

then:

```text
Z = 10(100) / [10(100) + 25]
  = 1000 / 1025
  = 0.976
```

The data gets high credibility because prior variance is large relative to sampling variance.

#### 5. Exam-style case study

If asked how `Z` changes with:

```text
mu0, sigma^2, s^2
```

answer:

```text
mu0: no direct effect on Z
sigma^2: increasing effect on Z
s^2: decreasing effect on Z
```

Reason:

```text
Z = n sigma^2 / (n sigma^2 + s^2)
```

#### 6. Real-world actuarial case study

A pricing actuary has a prior assumption for average claim severity from last year's pricing basis. New claim data arrives.

If the prior assumption is uncertain, the actuary gives more weight to the new data. If the new claims are highly volatile, the actuary gives less weight to the new data.

#### 7. Common mistakes

- Saying the prior mean `mu0` changes the credibility factor.
- Thinking higher sample variance increases credibility.
- Forgetting that larger sample size increases credibility.
- Confusing prior variance with process variance.

#### 8. Revision checkpoint

Without notes, you should be able to write:

```text
Z = n sigma^2 / (n sigma^2 + s^2)
```

and state the direction of change with `mu0`, `sigma^2`, and `s^2`.

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- Bayesian analysis combines prior information and data through the likelihood. The posterior distribution is proportional to prior times likelihood.
- Credibility theory has the same actuarial instinct: blend individual experience with collective experience. The more stable and relevant the individual data, the more credibility it receives.
- Conjugate priors are useful because the posterior stays in the same family. This makes updating transparent and exam calculations faster.

### Step-by-step working method

1. Write the prior distribution and identify its parameters.
2. Write the likelihood from the observed data.
3. Multiply prior and likelihood, keeping parameter-dependent terms.
4. Recognise the posterior family or normalise if required.
5. Use the posterior mean, mode, interval, or credibility form for the decision.

### Extra practical actuarial examples

- Renewal example: a beta prior for renewal probability can be updated after observing renewals and non-renewals.
- Claim count example: a gamma prior for a Poisson rate leads to a gamma posterior and a credibility-style blended estimate.
- Pricing example: a small employer group should not be priced only from its own volatile claims experience; credibility blends group experience with portfolio experience.

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

### Topic 39: Gamma Prior for Exponential Rate and Credibility for Mean Waiting Time

#### 1. Concept theory

If observations follow an exponential distribution with rate `lambda`, then a Gamma prior for `lambda` is conjugate.

This means the posterior distribution of `lambda` is also Gamma.

#### 2. Why actuaries care

Exponential models are used for claim waiting times, failure times, recovery times, and time between events.

The conjugate prior makes Bayesian updating fast and transparent.

#### 3. Mathematical derivation

Let:

```text
Xi | lambda ~ Exponential(rate lambda)
```

Density:

```text
f(x | lambda) = lambda exp(-lambda x)
```

Likelihood:

```text
L(lambda) = lambda^n exp(-lambda sum xi)
```

Prior:

```text
lambda ~ Gamma(alpha, rate beta)
```

Prior density is proportional to:

```text
lambda^(alpha - 1) exp(-beta lambda)
```

Posterior:

```text
lambda | data ~ Gamma(alpha + n, rate beta + sum xi)
```

If `lambda ~ Gamma(alpha, rate beta)`, then:

```text
E[1 / lambda] = beta / (alpha - 1), alpha > 1
```

So:

```text
E[n / lambda] = n beta / (alpha - 1)
```

For posterior:

```text
E[n / lambda | data] = n(beta + sum xi) / (alpha + n - 1)
```

This can be written as a credibility blend of sample total and prior mean of `n/lambda`.

#### 4. Simple example

Suppose:

```text
alpha = 4
beta = 30
n = 5
sum x = 40
```

Posterior:

```text
lambda | data ~ Gamma(9, rate 70)
```

Posterior mean of `n/lambda`:

```text
5 x 70 / (9 - 1) = 43.75
```

#### 5. Exam-style case study

March 2022 asks to prove Gamma is conjugate for an exponential rate.

The clean exam route:

```text
likelihood proportional to lambda^n exp(-lambda sum x)
prior proportional to lambda^(alpha - 1) exp(-beta lambda)
posterior proportional to lambda^(alpha + n - 1) exp[-lambda(beta + sum x)]
```

Therefore:

```text
lambda | data ~ Gamma(alpha + n, beta + sum x)
```

#### 6. Real-world actuarial case study

A claims team models time between hospital admissions using an exponential distribution. Prior experience suggests a rate, but new observations arrive.

The Gamma prior updates naturally and gives a posterior distribution for the admission rate and mean waiting time.

#### 7. Common mistakes

- Confusing exponential rate `lambda` with mean `mu`.
- Using an inverse-gamma prior for rate instead of Gamma.
- Forgetting to add `sum x` to the rate parameter.
- Forgetting the condition `alpha > 1` for `E[1/lambda]`.

#### 8. Revision checkpoint

Without notes, you should be able to derive:

```text
lambda | data ~ Gamma(alpha + n, beta + sum x)
E[1/lambda] = beta / (alpha - 1)
```

for an exponential likelihood with Gamma prior on rate.

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- Bayesian analysis combines prior information and data through the likelihood. The posterior distribution is proportional to prior times likelihood.
- Credibility theory has the same actuarial instinct: blend individual experience with collective experience. The more stable and relevant the individual data, the more credibility it receives.
- Conjugate priors are useful because the posterior stays in the same family. This makes updating transparent and exam calculations faster.

### Step-by-step working method

1. Write the prior distribution and identify its parameters.
2. Write the likelihood from the observed data.
3. Multiply prior and likelihood, keeping parameter-dependent terms.
4. Recognise the posterior family or normalise if required.
5. Use the posterior mean, mode, interval, or credibility form for the decision.

### Extra practical actuarial examples

- Renewal example: a beta prior for renewal probability can be updated after observing renewals and non-renewals.
- Claim count example: a gamma prior for a Poisson rate leads to a gamma posterior and a credibility-style blended estimate.
- Pricing example: a small employer group should not be priced only from its own volatile claims experience; credibility blends group experience with portfolio experience.

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

### Topic 40: Exponential Family Variance Functions and GLM Interpretation

#### 1. Concept theory

In the exponential family, the variance function tells how the variance changes with the mean.

Common exam facts:

```text
Poisson: V(mu) = mu
Gamma: V(mu) = mu^2
Binomial: V(mu) = mu(1 - mu)
```

These variance functions help choose an appropriate GLM family.

#### 2. Why actuaries care

GLMs are central to actuarial pricing and risk modelling.

Choosing the wrong family can give wrong standard errors, wrong significance tests, and poor predictions.

#### 3. Mathematical derivation

For a one-parameter exponential family:

```text
f(y) = exp[y theta - b(theta) + c(y)]
```

Then:

```text
E[Y] = b'(theta)
Var(Y) = b''(theta)
```

For Poisson:

```text
theta = log(mu)
b(theta) = exp(theta)
```

So:

```text
b'(theta) = exp(theta) = mu
b''(theta) = exp(theta) = mu
```

Therefore:

```text
V(mu) = mu
```

For Gamma-style form in the exam:

```text
b(theta) = -log(-theta)
theta = -1/mu
```

Then:

```text
b'(theta) = -1/theta = mu
b''(theta) = 1/theta^2 = mu^2
```

So:

```text
V(mu) = mu^2
```

#### 4. Simple example

If claim count has mean 5 and follows a Poisson GLM:

```text
variance = 5
```

If claim severity has mean 5 and follows a Gamma GLM:

```text
variance proportional to 5^2 = 25
```

This reflects that severity variability often grows faster with the mean.

#### 5. Exam-style case study

March 2022 asks for the Poisson variance function.

Since:

```text
Poisson variance = mean
```

answer:

```text
V(mu) = mu
```

For:

```text
b(theta) = -log(-theta)
theta = -1/mu
```

derive:

```text
mean = mu
variance = mu^2
```

For a renewal-rate GLM, the response is a rate between 0 and 1, so a logit link is suitable:

```text
eta = log(mu / (1 - mu))
mu = 1 / [1 + exp(-eta)]
```

#### 6. Real-world actuarial case study

A motor insurer models claim frequency using a Poisson GLM and claim severity using a Gamma GLM.

The frequency model assumes variance grows roughly with mean. The severity model allows variance to grow with the square of the mean, which is often more realistic for claim amounts.

#### 7. Common mistakes

- Confusing link function with variance function.
- Saying Poisson variance function is `log(mu)`.
- Forgetting Gamma variance is proportional to `mu^2`.
- Using a linear model for renewal probabilities without checking predictions stay between 0 and 1.
- Treating significant interaction as always necessary after main effects are included.

#### 8. Revision checkpoint

Without notes, you should be able to state:

```text
Poisson V(mu) = mu
Gamma V(mu) = mu^2
Logit link: eta = log(mu / (1 - mu))
```

and derive variance from `b''(theta)` for simple exponential-family forms.

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- A GLM has three parts: a random component for the response distribution, a systematic component for the linear predictor, and a link function connecting the mean to the linear predictor.
- The link function determines coefficient interpretation. With a log link, a one-unit increase multiplies the mean by an exponential factor; with a logit link, a coefficient changes log-odds.
- Deviance compares fitted likelihoods. A smaller deviance can mean better fit, but model selection must balance fit, complexity, interpretability, and out-of-sample stability.

### Step-by-step working method

1. Choose the response distribution based on the data type.
2. Choose the link function and write the linear predictor.
3. Include exposure or offset terms where required.
4. Fit the model and interpret coefficients on the correct scale.
5. Use residuals, deviance, AIC, and validation to judge suitability.

### Extra practical actuarial examples

- Frequency example: Poisson GLM with log exposure offset estimates claim frequency per unit exposure.
- Binary example: a logit model can estimate probability of fraud, lapse, or claim occurrence.
- Severity example: gamma GLM with log link is often used for positive skewed claim amounts.

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

### Topic 41: Good Estimators, Consistency, Fisher's Exact Test, and Hypergeometric Tables

#### 1. Concept theory

A good estimator is usually expected to have desirable properties such as:

- unbiasedness
- low variance
- consistency
- efficiency

A consistent estimator gets closer to the true parameter as sample size increases.

Fisher's exact test is used for small contingency tables when chi-square expected counts may be too small.

#### 2. Why actuaries care

Actuaries estimate claim rates, mortality rates, expenses, and model parameters. Good estimator properties matter because decisions depend on these estimates.

Fisher's exact test can appear in small underwriting or HR-style contingency tables, where chi-square approximation is unreliable.

#### 3. Mathematical derivation

For a 2 by 2 table:

```text
                 Success   Failure   Total
Group 1             A        C        r1
Group 2             B        D        r2
Total               c1       c2       n
```

If row and column totals are fixed, the probability of observing `A = a` is hypergeometric:

```text
P(A = a) = C(r1, a) C(r2, c1 - a) / C(n, c1)
```

The p-value is found by summing probabilities of tables as or more extreme than the observed table, depending on the alternative.

#### 4. Simple example

Suppose:

```text
5 seniors
6 others
3 ESOPs granted
```

Probability all 3 ESOPs go to seniors:

```text
C(5,3)C(6,0) / C(11,3)
= 10 / 165
= 0.0606
```

#### 5. Exam-style case study

In September 2021, the table has:

```text
5 senior employees
6 other employees
3 ESOPs
8 non-ESOPs
```

For `A = 3` senior employees getting ESOPs:

```text
P(A = 3) = C(5,3)C(6,0) / C(11,3) = 0.0606
```

If the observed table has two senior employees with ESOPs, use the provided probabilities for tables as extreme as the observed table to form the p-value.

#### 6. Real-world actuarial case study

A small insurer checks whether senior underwriters receive a disproportionate share of performance-linked stock options. Since the sample is tiny, a chi-square test may be unreliable.

Fisher's exact test gives an exact probability based on fixed margins.

#### 7. Common mistakes

- Using chi-square automatically even when expected counts are small.
- Forgetting Fisher's test conditions on fixed margins.
- Summing the wrong tail for the p-value.
- Confusing consistency with unbiasedness.
- Saying a low-variance estimator is always unbiased.

#### 8. Revision checkpoint

Without notes, you should be able to state properties of a good estimator and compute Fisher table probabilities using:

```text
C(r1,a)C(r2,c1-a) / C(n,c1)
```

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- Inference is a disciplined way of deciding whether observed experience is consistent with an assumption. A confidence interval estimates a plausible range for a parameter; a hypothesis test checks whether the data are surprising under a stated null hypothesis.
- Type I error is rejecting a true null hypothesis. Type II error is failing to reject a false null hypothesis. Power is the probability of detecting an effect when it is truly present.
- A prediction interval is wider than a confidence interval for a mean because it includes both uncertainty about the average and the randomness of a new observation.

### Step-by-step working method

1. Identify the parameter being estimated or tested.
2. Write the null and alternative hypotheses, if a test is required.
3. Choose the reference distribution and degrees of freedom.
4. Compute the statistic, p-value, interval, or critical region.
5. Finish with a plain-English actuarial conclusion.

### Extra practical actuarial examples

- Fraud model example: a higher observed detection rate is useful only if the improvement is statistically convincing and operationally valuable after false positives are considered.
- Claims inflation example: test whether average claim cost has changed before changing a pricing basis, but also review exposure mix and large claims.
- Reserve example: a confidence interval for mean settlement cost should not be confused with a prediction interval for a single future claim.

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

### Topic 42: Poisson MLE with Related Group Means and Lognormal MLE Invariance

#### 1. Concept theory

Maximum likelihood estimation can combine data from related groups when their means are linked by a parameter.

MLEs also have an invariance property: if `theta_hat` is the MLE of `theta`, then `g(theta_hat)` is the MLE of `g(theta)`.

#### 2. Why actuaries care

Actuaries often model related portfolios where one group has claim frequency proportional to another.

MLE invariance is useful for lognormal models, where parameters are estimated on the log scale but business quantities such as mean and variance are needed on the original claim scale.

#### 3. Mathematical derivation

Suppose:

```text
Group A claim count ~ Poisson(mu) per policy
Group B claim count ~ Poisson(3mu) per policy
```

For:

```text
nA policies, total claims xA
nB policies, total claims xB
```

The likelihood is proportional to:

```text
exp(-nA mu) mu^xA x exp(-3nB mu)(3mu)^xB
```

Ignoring constants:

```text
l(mu) = -(nA + 3nB)mu + (xA + xB)log(mu) + constant
```

Differentiate:

```text
dl/dmu = -(nA + 3nB) + (xA + xB)/mu
```

Set to zero:

```text
mu_hat = (xA + xB) / (nA + 3nB)
```

For lognormal:

If:

```text
log X ~ N(mu, sigma^2)
```

then:

```text
E[X] = exp(mu + sigma^2 / 2)
Var(X) = exp(2mu + sigma^2)[exp(sigma^2) - 1]
```

Using invariance:

```text
MLE of E[X] = exp(mu_hat + sigma_hat^2 / 2)
```

#### 4. Simple example

Group A:

```text
nA = 100, xA = 12
```

Group B:

```text
nB = 80, xB = 18
```

Then:

```text
mu_hat = (12 + 18) / (100 + 3(80))
       = 30 / 340
       = 0.0882
```

#### 5. Exam-style case study

September 2021 gives the same numbers:

```text
100 Group-A policies: 12 claims
80 Group-B policies: 18 claims
Group B mean = 3mu
```

Use:

```text
mu_hat = total claims / total weighted exposure
       = 30 / 340
       = 0.088
```

For lognormal with:

```text
mu_hat = 1
sigma_hat^2 = 0.5
```

MLE of mean:

```text
exp(1 + 0.5 / 2) = exp(1.25)
```

MLE of variance:

```text
exp(2(1) + 0.5)[exp(0.5) - 1]
```

#### 6. Real-world actuarial case study

A motor insurer knows commercial vehicles have about three times the claim frequency of private vehicles. Instead of estimating two unrelated frequencies, the actuary estimates one base frequency and applies the multiplier.

For claim severity, if log claim sizes are normal, the actuary estimates lognormal parameters and then converts them into mean and variance for pricing.

#### 7. Common mistakes

- Averaging the two group claim rates without exposure weighting.
- Forgetting Group B has mean `3mu`, not `mu`.
- Ignoring the factor 3 in the likelihood exposure.
- Using lognormal `mu` as the original-scale mean.
- Forgetting the lognormal variance formula.

#### 8. Revision checkpoint

Without notes, you should be able to derive:

```text
mu_hat = (xA + xB) / (nA + 3nB)
E[X] = exp(mu + sigma^2/2)
Var(X) = exp(2mu + sigma^2)(exp(sigma^2) - 1)
```

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- An estimator is a rule for turning data into a parameter estimate. Different estimators can have different bias, variance, mean squared error, and robustness.
- Maximum likelihood chooses the parameter that makes the observed data most probable under the model. Method of moments matches sample moments to theoretical moments.
- Large-sample theory can give approximate intervals, but boundary estimates, censored observations, small samples, and heavy tails can make approximations unreliable.

### Step-by-step working method

1. Write the model and parameter space.
2. Choose MOM, MLE, least squares, or another estimator.
3. Derive or solve the estimating equation.
4. Check bias, variance, consistency, and practical stability where possible.
5. Interpret the estimate in actuarial units.

### Extra practical actuarial examples

- Severity example: estimating a Pareto tail parameter from a few large losses may be highly unstable, so sensitivity testing is essential.
- Frequency example: the Poisson MLE for a constant rate is total claims divided by total exposure.
- Simulation example: inverse transform simulation uses uniform random numbers to generate losses from a fitted distribution for stress testing.

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

### Topic 43: CRLB, Large-Sample CI, and Exact Chi-Square CI for Exponential Rate

#### 1. Concept theory

The Cramer-Rao lower bound, or CRLB, gives the approximate minimum variance of an unbiased estimator under regularity conditions.

For exponential data, the MLE of the rate can also be given an exact confidence interval using a chi-square distribution.

#### 2. Why actuaries care

Actuaries estimate delay rates, claim arrival rates, and termination rates. Large-sample intervals are quick, but exact intervals can be better for smaller samples.

#### 3. Mathematical derivation

For exponential data with rate `lambda`:

```text
f(x) = lambda exp(-lambda x)
```

Log-likelihood:

```text
l(lambda) = n log(lambda) - lambda sum x
```

MLE:

```text
lambda_hat = n / sum x = 1 / xbar
```

Second derivative:

```text
d2l/dlambda2 = -n / lambda^2
```

Fisher information:

```text
I(lambda) = n / lambda^2
```

Approximate variance:

```text
Var(lambda_hat) approximately 1 / I(lambda)
                         = lambda^2 / n
```

Use plug-in estimate:

```text
estimated Var(lambda_hat) = lambda_hat^2 / n
```

Approximate 95 percent CI:

```text
lambda_hat +/- 1.96 sqrt(lambda_hat^2 / n)
```

Exact result:

```text
2 lambda sum Xi ~ chi-square(2n)
```

Since:

```text
sum Xi = n xbar
```

solve:

```text
chi-square lower <= 2 lambda n xbar <= chi-square upper
```

for `lambda`.

#### 4. Simple example

Suppose:

```text
n = 25
lambda_hat = 0.2
```

Approximate variance:

```text
0.2^2 / 25 = 0.0016
```

Standard error:

```text
sqrt(0.0016) = 0.04
```

Approximate 95 percent CI:

```text
0.2 +/- 1.96(0.04)
```

#### 5. Exam-style case study

September 2021 gives:

```text
n = 20
xbar = 10
lambda_hat = 0.1
```

Approximate variance:

```text
lambda_hat^2 / n = 0.1^2 / 20 = 0.0005
```

Approximate CI:

```text
0.1 +/- 1.96 sqrt(0.0005)
```

For the exact CI, use:

```text
2 lambda n xbar ~ chi-square(40)
```

Given chi-square critical values:

```text
24.43 and 59.34
```

solve:

```text
24.43 <= 2 lambda (20)(10) <= 59.34
```

So:

```text
24.43 / 400 <= lambda <= 59.34 / 400
```

#### 6. Real-world actuarial case study

A claims department estimates the rate at which delayed claims are settled. With only 20 observed delays, the actuary compares the quick normal approximation with the exact chi-square interval.

If the two intervals differ materially, the exact interval may be preferred.

#### 7. Common mistakes

- Using the variance of the exponential observation instead of variance of the estimator.
- Forgetting to plug in `lambda_hat` for approximate variance.
- Using `sum x` incorrectly when converting to `n xbar`.
- Reversing the chi-square limits.
- Assuming approximate and exact intervals must be identical.

#### 8. Revision checkpoint

Without notes, you should be able to write:

```text
lambda_hat = 1 / xbar
Var(lambda_hat) approximately lambda_hat^2 / n
2 lambda n xbar ~ chi-square(2n)
```

and solve an exact CI for `lambda`.

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- An estimator is a rule for turning data into a parameter estimate. Different estimators can have different bias, variance, mean squared error, and robustness.
- Maximum likelihood chooses the parameter that makes the observed data most probable under the model. Method of moments matches sample moments to theoretical moments.
- Large-sample theory can give approximate intervals, but boundary estimates, censored observations, small samples, and heavy tails can make approximations unreliable.

### Step-by-step working method

1. Write the model and parameter space.
2. Choose MOM, MLE, least squares, or another estimator.
3. Derive or solve the estimating equation.
4. Check bias, variance, consistency, and practical stability where possible.
5. Interpret the estimate in actuarial units.

### Extra practical actuarial examples

- Severity example: estimating a Pareto tail parameter from a few large losses may be highly unstable, so sensitivity testing is essential.
- Frequency example: the Poisson MLE for a constant rate is total claims divided by total exposure.
- Simulation example: inverse transform simulation uses uniform random numbers to generate losses from a fitted distribution for stress testing.

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

### Topic 44: Poisson-Gamma Posterior, Posterior Mode, and All-or-Nothing Loss

#### 1. Concept theory

For Poisson data, a Gamma prior is conjugate. The posterior is Gamma.

Under all-or-nothing loss, the Bayes estimate is the posterior mode, not the posterior mean.

#### 2. Why actuaries care

Poisson-Gamma updating is used for claim counts, mortality counts, and incident frequencies.

Different loss functions lead to different actuarial estimates. This matters when the business wants the most likely parameter value rather than the average parameter value.

#### 3. Mathematical derivation

Let annual claim count:

```text
Xj | mu ~ Poisson(mu), j = 1, ..., t
```

Likelihood:

```text
L(mu) proportional to exp(-t mu) mu^(sum x)
```

Prior:

```text
mu ~ Gamma(alpha, rate lambda)
```

Posterior:

```text
mu | data ~ Gamma(alpha + sum x, rate lambda + t)
```

For a Gamma distribution with shape `a` and rate `b`, mode is:

```text
(a - 1) / b, if a > 1
```

So under all-or-nothing loss:

```text
estimate = (posterior shape - 1) / posterior rate
```

For Beta distribution:

```text
theta ~ Beta(a,b)
```

mode is:

```text
(a - 1) / (a + b - 2), if a > 1 and b > 1
```

#### 4. Simple example

Prior:

```text
mu ~ Gamma(4, 2)
```

Data over 3 years:

```text
total claims = 5
```

Posterior:

```text
Gamma(9, 5)
```

All-or-nothing estimate:

```text
(9 - 1) / 5 = 1.6
```

#### 5. Exam-style case study

September 2021:

```text
prior = Gamma(alpha, lambda)
total claims in previous 5 years = 3
```

Posterior:

```text
Gamma(alpha + 3, lambda + 5)
```

If more claims are observed over the same time horizon, the posterior shape increases and the posterior mode generally increases.

If the observation period is longer with the same number of claims, the posterior rate increases and the posterior mode generally decreases.

For:

```text
theta ~ Beta(5,15)
```

all-or-nothing estimate is the mode:

```text
(5 - 1) / (5 + 15 - 2) = 4 / 18
```

#### 6. Real-world actuarial case study

An insurer estimates claim frequency for a small product line. The prior is based on industry data, and the company observes claims over several years.

If management asks for the most likely frequency, the actuary may report the posterior mode. If management asks for expected claim cost, the posterior mean may be more relevant.

#### 7. Common mistakes

- Using posterior mean under all-or-nothing loss.
- Adding years to the Gamma shape instead of to the rate.
- Adding claims to the rate instead of to the shape.
- Forgetting Beta mode is `(a - 1)/(a + b - 2)`, not `a/(a+b)`.
- Assuming a longer observation period always increases the estimate.

#### 8. Revision checkpoint

Without notes, you should be able to write:

```text
Poisson-Gamma posterior = Gamma(alpha + total claims, lambda + exposure time)
Gamma mode = (a - 1) / b
Beta mode = (a - 1) / (a + b - 2)
```

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- Bayesian analysis combines prior information and data through the likelihood. The posterior distribution is proportional to prior times likelihood.
- Credibility theory has the same actuarial instinct: blend individual experience with collective experience. The more stable and relevant the individual data, the more credibility it receives.
- Conjugate priors are useful because the posterior stays in the same family. This makes updating transparent and exam calculations faster.

### Step-by-step working method

1. Write the prior distribution and identify its parameters.
2. Write the likelihood from the observed data.
3. Multiply prior and likelihood, keeping parameter-dependent terms.
4. Recognise the posterior family or normalise if required.
5. Use the posterior mean, mode, interval, or credibility form for the decision.

### Extra practical actuarial examples

- Renewal example: a beta prior for renewal probability can be updated after observing renewals and non-renewals.
- Claim count example: a gamma prior for a Poisson rate leads to a gamma posterior and a credibility-style blended estimate.
- Pricing example: a small employer group should not be priced only from its own volatile claims experience; credibility blends group experience with portfolio experience.

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

### Topic 45: Full Normal Regression: Error Variance CI, Slope Test, and Mean Prediction

#### 1. Concept theory

In the full normal linear regression model, errors are assumed normally distributed with constant variance.

This allows confidence intervals not only for the slope and mean response, but also for the error variance.

#### 2. Why actuaries care

Regression is used for claim trends, mortality, COVID claim rates, expenses, and pricing. Actuaries need to know whether the model explains enough variation and whether residuals show a pattern.

#### 3. Mathematical derivation

For simple regression:

```text
Y = alpha + beta X + error
```

Residual sum of squares:

```text
SSE = Syy - Sxy^2 / Sxx
```

Error variance estimate:

```text
s^2 = SSE / (n - 2)
```

For normal errors:

```text
(n - 2)s^2 / sigma^2 ~ chi-square(n - 2)
```

So a confidence interval for `sigma^2` is:

```text
[(n - 2)s^2 / chi-square upper, (n - 2)s^2 / chi-square lower]
```

Slope estimate:

```text
beta_hat = Sxy / Sxx
```

Test positive correlation:

```text
H0: beta = 0
H1: beta > 0
```

Test statistic:

```text
t = beta_hat / sqrt(s^2 / Sxx)
```

Mean response at `x0`:

```text
y_hat0 = alpha_hat + beta_hat x0
```

Standard error:

```text
sqrt[s^2(1/n + (x0 - xbar)^2 / Sxx)]
```

#### 4. Simple example

If:

```text
Sxx = 100
Sxy = 250
Syy = 700
n = 10
```

then:

```text
beta_hat = 250 / 100 = 2.5
SSE = 700 - 250^2 / 100 = 75
s^2 = 75 / 8 = 9.375
```

#### 5. Exam-style case study

September 2021 gives:

```text
Sxx = 2800
Syy = 270832
Sxy = 25300
```

Fitted slope:

```text
beta_hat = 25300 / 2800
```

The intercept is:

```text
alpha_hat = ybar - beta_hat xbar
```

Proportion of variance explained:

```text
R^2 = Sxy^2 / (Sxx Syy)
```

For the error variance:

```text
SSE = Syy - Sxy^2 / Sxx
s^2 = SSE / (n - 2)
```

Then use chi-square with:

```text
n - 2 degrees of freedom
```

For age 60 mean prediction:

```text
y_hat_60 = alpha_hat + beta_hat(60)
```

and use the mean-response standard error.

#### 6. Real-world actuarial case study

A life insurer studies COVID claim frequency per 10,000 policies by age. A linear model may show a strong positive relationship, but residuals may reveal curvature.

If residuals are patterned, a linear model may not be appropriate even when `R^2` is high. A transformation, polynomial term, or GLM may be better.

#### 7. Common mistakes

- Using total degrees of freedom instead of `n - 2` for residual variance.
- Forgetting chi-square limits invert in variance confidence intervals.
- Testing correlation without stating the slope hypothesis.
- Confusing confidence interval for mean response with prediction interval.
- Ignoring residual patterns.

#### 8. Revision checkpoint

Without notes, you should be able to write:

```text
SSE = Syy - Sxy^2/Sxx
s^2 = SSE/(n-2)
(n-2)s^2/sigma^2 ~ chi-square(n-2)
t = beta_hat/sqrt(s^2/Sxx)
```

and build a confidence interval for a mean response.

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- Regression estimates a conditional mean, not a causal law by default. The coefficient tells how the fitted average response changes when a predictor changes, usually holding other predictors fixed.
- The residuals are as important as the fitted line. Patterns, changing spread, influential points, and non-normal residuals can reveal that the chosen model is too simple or the scale is wrong.
- Prediction requires two layers of uncertainty: uncertainty in the fitted mean and random variation of the individual outcome. This is why individual prediction intervals are wider than mean response intervals.

### Step-by-step working method

1. Plot or inspect the relationship before fitting.
2. Fit the model and write the fitted equation.
3. Interpret each coefficient in business units.
4. Check residuals, leverage, fit statistics, and uncertainty.
5. Decide whether the model is suitable for pricing, reserving, or explanation.

### Extra practical actuarial examples

- Motor pricing example: claim frequency may increase with driver age band, vehicle group, region, and prior claims, but coefficient interpretation must respect all variables included in the model.
- Life insurance example: a transformed mortality rating can produce a smooth premium formula, but the formula must be checked against underwriting judgement and table constraints.
- Exam example: if asked for an individual prediction, include the extra 1 inside the square root of the prediction standard error.

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

### Topic 46: Exponential GLM, Canonical Link, and Likelihood Equations

#### 1. Concept theory

The exponential distribution can be written as a member of the exponential family.

For an exponential response with mean `mu`, the canonical link is related to the reciprocal of the mean.

#### 2. Why actuaries care

Claim amounts and waiting times are positive and often skewed. Exponential and Gamma GLMs can be useful alternatives to normal regression.

#### 3. Mathematical derivation

For exponential with mean `mu`:

```text
f(y) = (1/mu) exp(-y/mu), y > 0
```

Write:

```text
f(y) = exp[-y/mu - log(mu)]
```

Let:

```text
theta = -1/mu
```

Then:

```text
f(y) = exp[y theta + log(-theta)]
```

This is exponential-family form with:

```text
b(theta) = -log(-theta)
```

Mean:

```text
b'(theta) = -1/theta = mu
```

Variance function:

```text
b''(theta) = 1/theta^2 = mu^2
```

Canonical link:

```text
g(mu) = theta = -1/mu
```

If the linear predictor is:

```text
eta_i = alpha + beta x_i
```

then under the canonical link:

```text
-1/mu_i = alpha + beta x_i
```

The log-likelihood score equations come from differentiating:

```text
sum[-log(mu_i) - y_i/mu_i]
```

with respect to `alpha` and `beta`, after substituting the model for `mu_i`.

#### 4. Simple example

If:

```text
mu = 100
```

then the canonical parameter is:

```text
theta = -1/100 = -0.01
```

The variance function is:

```text
V(mu) = mu^2 = 10000
```

#### 5. Exam-style case study

September 2021 gives medical claim amounts and asks which likelihood equations are correct under the canonical link.

Method:

1. write:

```text
eta_i = alpha + beta x_i
```

2. use canonical link:

```text
eta_i = -1/mu_i
```

3. therefore:

```text
mu_i = -1 / (alpha + beta x_i)
```

4. substitute into the exponential log-likelihood and differentiate with respect to `alpha` and `beta`.

The correct option must have one score equation for `alpha` and one weighted by `x_i` for `beta`.

#### 6. Real-world actuarial case study

A health insurer models claim amount by age. A normal linear model can predict negative claims, which is impossible. An exponential GLM keeps the response positive and allows variance to increase with the square of the mean.

#### 7. Common mistakes

- Using identity link automatically for positive skewed claim amounts.
- Forgetting the canonical link for exponential is `-1/mu`.
- Confusing exponential distribution with Poisson distribution.
- Ignoring that fitted means must be positive.
- Forgetting the beta score equation includes the covariate multiplier.

#### 8. Revision checkpoint

Without notes, you should be able to write:

```text
theta = -1/mu
b(theta) = -log(-theta)
V(mu) = mu^2
canonical link g(mu) = -1/mu
```

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- A GLM has three parts: a random component for the response distribution, a systematic component for the linear predictor, and a link function connecting the mean to the linear predictor.
- The link function determines coefficient interpretation. With a log link, a one-unit increase multiplies the mean by an exponential factor; with a logit link, a coefficient changes log-odds.
- Deviance compares fitted likelihoods. A smaller deviance can mean better fit, but model selection must balance fit, complexity, interpretability, and out-of-sample stability.

### Step-by-step working method

1. Choose the response distribution based on the data type.
2. Choose the link function and write the linear predictor.
3. Include exposure or offset terms where required.
4. Fit the model and interpret coefficients on the correct scale.
5. Use residuals, deviance, AIC, and validation to judge suitability.

### Extra practical actuarial examples

- Frequency example: Poisson GLM with log exposure offset estimates claim frequency per unit exposure.
- Binary example: a logit model can estimate probability of fraud, lapse, or claim occurrence.
- Severity example: gamma GLM with log link is often used for positive skewed claim amounts.

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

### Topic 47: Sum of Exponential Claims: Exact Gamma, Chi-Square Scaling, and CLT Approximation

#### 1. Concept theory

The sum of independent exponential variables follows a Gamma distribution. This gives an exact result.

For a moderate or large number of terms, the Central Limit Theorem can also approximate the sum by a normal distribution.

#### 2. Why actuaries care

Aggregate claim amounts are often sums of individual claim amounts. Actuaries need to know when to use an exact distribution and when a normal approximation is acceptable.

#### 3. Mathematical derivation

Let:

```text
Xi ~ Exponential(rate lambda), independent
```

The MGF is:

```text
MX(t) = (1 - t/lambda)^(-1)
```

For:

```text
Y = X1 + X2 + ... + Xn
```

independence gives:

```text
MY(t) = [MX(t)]^n
      = (1 - t/lambda)^(-n)
```

So:

```text
Y ~ Gamma(shape n, rate lambda)
```

If:

```text
2lambda Y
```

then:

```text
2lambda Y ~ chi-square(2n)
```

For CLT:

```text
E[Y] = n/lambda
Var(Y) = n/lambda^2
```

so:

```text
Y approximately N(n/lambda, n/lambda^2)
```

#### 4. Simple example

If:

```text
n = 5
lambda = 2
```

then:

```text
Y ~ Gamma(5, rate 2)
E[Y] = 5/2 = 2.5
Var(Y) = 5/4 = 1.25
```

and:

```text
4Y ~ chi-square(10)
```

#### 5. Exam-style case study

In September 2021:

```text
lambda = 1.25
n = 10
Y = total of 10 claims
```

Exact:

```text
Y ~ Gamma(10, 1.25)
2.5Y ~ chi-square(20)
```

For:

```text
P(Y > 10)
```

use:

```text
P(2.5Y > 25)
```

and read from chi-square tables.

CLT approximation:

```text
E[Y] = 10 / 1.25 = 8
Var(Y) = 10 / 1.25^2 = 6.4
Y approximately N(8, 6.4)
```

Then:

```text
P(Y > 10) = P(Z > (10 - 8)/sqrt(6.4))
```

#### 6. Real-world actuarial case study

A motor insurer models small claim amounts as exponential. For 10 claims, the exact Gamma distribution is available and should usually be preferred.

For hundreds of independent claims, a normal approximation may be simpler and accurate enough for quick estimates.

#### 7. Common mistakes

- Forgetting the rate/scale convention.
- Using CLT automatically when `n` is only 10.
- Forgetting `2lambda Y` has chi-square distribution.
- Calculating `Var(Y)` as `n/lambda` instead of `n/lambda^2`.
- Comparing the exact and approximate probabilities without commenting on sample size.

#### 8. Revision checkpoint

Without notes, you should be able to write:

```text
sum of n Exponential(lambda) variables ~ Gamma(n, lambda)
2lambda Y ~ chi-square(2n)
Y approximately N(n/lambda, n/lambda^2)
```

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- Aggregate loss modelling combines frequency and severity. Frequency explains how often losses occur; severity explains how large they are when they occur. Their product gives expected loss only when the frequency and severity assumptions are aligned.
- The variance of aggregate loss has two sources: random claim sizes and random claim counts. This is why aggregate variance usually contains both a severity variance term and a squared mean severity term.
- Capital work is more demanding than pricing work. Pricing may focus on expected loss, while capital and reinsurance decisions need standard deviation, percentiles, stress tests, and tail scenarios.

### Step-by-step working method

1. Define the claim count variable and the individual severity variables.
2. Check independence and identical distribution assumptions.
3. Condition on the claim count first.
4. Use total expectation and total variance.
5. Translate the result into premium, reserve, or capital language.

### Extra practical actuarial examples

- Pricing example: expected annual loss equals expected claim count times expected claim size, then expenses, commission, profit margin, and risk loading are added.
- Capital example: two portfolios with the same expected loss can require different capital if one has low-frequency high-severity claims.
- Operational example: for cyber insurance, one ransomware event may produce legal, notification, downtime, and recovery costs, so severity volatility drives risk loading.

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

### Topic 48: Exponential GLM Canonical Link Sign Convention

#### 1. Concept theory

For an exponential response with mean `mu`, the canonical parameter is:

```text
theta = -1/mu
```

Some exam solutions write the link using `1/mu` and absorb the sign into the linear predictor or likelihood equations. The important point is to stay internally consistent.

#### 2. Why actuaries care

Exponential GLMs are used for positive skewed quantities such as claim amounts, delays, and waiting times.

The sign convention affects the likelihood equations and can create exam confusion.

#### 3. Mathematical derivation

Start with:

```text
f(y) = (1/mu) exp(-y/mu)
```

Write:

```text
f(y) = exp[-y/mu - log(mu)]
```

Let:

```text
theta = -1/mu
```

Then:

```text
mu = -1/theta
```

and:

```text
-log(mu) = log(-theta)
```

So:

```text
f(y) = exp[y theta + log(-theta)]
```

The canonical link is:

```text
g(mu) = theta = -1/mu
```

If instead the exam writes:

```text
eta = 1/mu
```

then the log-likelihood is written with a negative `y eta` term. The same fitted means can be reached if the sign is handled consistently.

#### 4. Simple example

If:

```text
mu = 100
```

canonical parameter:

```text
theta = -1/100 = -0.01
```

positive reciprocal form:

```text
1/mu = 0.01
```

They differ by a minus sign.

#### 5. Exam-style case study

In September 2021, the solution uses equations of the form:

```text
sum 1/(alpha + beta xi) - sum yi = 0
sum xi/(alpha + beta xi) - sum xi yi = 0
```

This corresponds to using a reciprocal mean expression in the likelihood equations.

When solving multiple-choice options, focus on:

- one score equation without `xi`
- one score equation with `xi`
- the first subtracts total claim amount
- the second subtracts weighted total claim amount

#### 6. Real-world actuarial case study

A health insurer models medical claim amount by age using an exponential GLM. The modeller must check the software's link convention, because some software may use inverse link while theoretical notes describe the canonical parameter with a negative sign.

The fitted mean must remain positive.

#### 7. Common mistakes

- Mixing `-1/mu` and `1/mu` within the same derivation.
- Forgetting fitted means must be positive.
- Choosing a likelihood equation that misses the `xi` weighting in the beta equation.
- Treating sign convention differences as different distributions.

#### 8. Revision checkpoint

Without notes, you should be able to state:

```text
canonical parameter theta = -1/mu
inverse mean form = 1/mu
```

and explain that exam likelihood equations must be checked for sign consistency.

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- A GLM has three parts: a random component for the response distribution, a systematic component for the linear predictor, and a link function connecting the mean to the linear predictor.
- The link function determines coefficient interpretation. With a log link, a one-unit increase multiplies the mean by an exponential factor; with a logit link, a coefficient changes log-odds.
- Deviance compares fitted likelihoods. A smaller deviance can mean better fit, but model selection must balance fit, complexity, interpretability, and out-of-sample stability.

### Step-by-step working method

1. Choose the response distribution based on the data type.
2. Choose the link function and write the linear predictor.
3. Include exposure or offset terms where required.
4. Fit the model and interpret coefficients on the correct scale.
5. Use residuals, deviance, AIC, and validation to judge suitability.

### Extra practical actuarial examples

- Frequency example: Poisson GLM with log exposure offset estimates claim frequency per unit exposure.
- Binary example: a logit model can estimate probability of fraud, lapse, or claim occurrence.
- Severity example: gamma GLM with log link is often used for positive skewed claim amounts.

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

### Topic 49: Coefficient of Variation and Inverse Transform Simulation for Exponential Variables

#### 1. Concept theory

The coefficient of variation compares standard deviation with mean:

```text
CV = standard deviation / mean
```

It is a relative measure of variability.

Inverse transform simulation converts a uniform random number into a simulated value from a target distribution.

#### 2. Why actuaries care

CV helps compare variability across portfolios with different scales. Inverse transform simulation is a building block of Monte Carlo methods used in pricing, reserving, and capital modelling.

#### 3. Mathematical derivation

#### Coefficient of variation

For Poisson:

```text
mean = lambda
variance = lambda
CV = sqrt(lambda) / lambda = 1 / sqrt(lambda)
```

As variance and mean increase together, CV decreases as `lambda` increases.

For Exponential with rate `lambda`:

```text
mean = 1/lambda
variance = 1/lambda^2
standard deviation = 1/lambda
CV = 1
```

So the exponential CV is constant.

For chi-square with `v` degrees of freedom:

```text
mean = v
variance = 2v
CV = sqrt(2v) / v = sqrt(2/v)
```

As `v` increases, variance increases but CV decreases.

#### Inverse transform for Exponential

For:

```text
X ~ Exponential(rate lambda)
```

CDF:

```text
F(x) = 1 - exp(-lambda x)
```

Set:

```text
U = F(x)
```

Then:

```text
U = 1 - exp(-lambda x)
exp(-lambda x) = 1 - U
x = -log(1 - U) / lambda
```

Since `1 - U` is also uniform, often:

```text
x = -log(U) / lambda
```

#### 4. Simple example

If:

```text
lambda = 0.5
U = 0.8
```

then:

```text
X = -log(1 - 0.8) / 0.5
  = -log(0.2) / 0.5
  = 3.2189
```

#### 5. Exam-style case study

In April 2021:

```text
X ~ Exp(0.5)
```

For random number:

```text
U = 0.769
```

use:

```text
X = -log(1 - 0.769) / 0.5
```

For:

```text
U = 0.004
```

use:

```text
X = -log(1 - 0.004) / 0.5
```

#### 6. Real-world actuarial case study

An insurer simulates waiting time until the next claim. Random uniform numbers are generated by software and transformed into exponential waiting times.

These waiting times feed into a claim arrival simulation for capital modelling.

#### 7. Common mistakes

- Thinking higher variance always means higher CV.
- Forgetting exponential CV is always 1.
- Using `log(U)` without the negative sign.
- Confusing rate and mean in the exponential formula.
- Using `-log(U)/lambda` and `-log(1-U)/lambda` inconsistently without understanding both are valid.

#### 8. Revision checkpoint

Without notes, you should be able to write:

```text
CV = sd / mean
Poisson CV = 1/sqrt(lambda)
Exponential CV = 1
X = -log(1-U)/lambda
```

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- An estimator is a rule for turning data into a parameter estimate. Different estimators can have different bias, variance, mean squared error, and robustness.
- Maximum likelihood chooses the parameter that makes the observed data most probable under the model. Method of moments matches sample moments to theoretical moments.
- Large-sample theory can give approximate intervals, but boundary estimates, censored observations, small samples, and heavy tails can make approximations unreliable.

### Step-by-step working method

1. Write the model and parameter space.
2. Choose MOM, MLE, least squares, or another estimator.
3. Derive or solve the estimating equation.
4. Check bias, variance, consistency, and practical stability where possible.
5. Interpret the estimate in actuarial units.

### Extra practical actuarial examples

- Severity example: estimating a Pareto tail parameter from a few large losses may be highly unstable, so sensitivity testing is essential.
- Frequency example: the Poisson MLE for a constant rate is total claims divided by total exposure.
- Simulation example: inverse transform simulation uses uniform random numbers to generate losses from a fitted distribution for stress testing.

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

### Topic 50: Beta-Binomial Posterior with Uniform Prior and Credibility Form

#### 1. Concept theory

For binomial data, a Beta prior is conjugate. A Uniform(0,1) prior is the same as:

```text
Beta(1,1)
```

After observing successes and failures, the posterior is also Beta.

#### 2. Why actuaries care

This is useful for estimating claim probabilities, lapse rates, mortality probabilities, conversion rates, and large-claim proportions.

#### 3. Mathematical derivation

Let:

```text
X | p ~ Binomial(n, p)
```

Likelihood:

```text
L(p) proportional to p^x(1-p)^(n-x)
```

Prior:

```text
p ~ Beta(a,b)
```

Prior density is proportional to:

```text
p^(a-1)(1-p)^(b-1)
```

Posterior:

```text
p | data ~ Beta(a + x, b + n - x)
```

With Uniform prior:

```text
a = 1, b = 1
```

so:

```text
p | data ~ Beta(x + 1, n - x + 1)
```

Posterior mean:

```text
E[p | data] = (x + 1) / (n + 2)
```

MLE:

```text
p_hat = x / n
```

Credibility form:

```text
(x + 1)/(n + 2) = [n/(n+2)](x/n) + [2/(n+2)](1/2)
```

So:

```text
Z = n / (n + 2)
```

#### 4. Simple example

Suppose:

```text
n = 10
x = 4
```

MLE:

```text
4 / 10 = 0.4
```

Bayesian estimate with Uniform prior:

```text
(4 + 1) / (10 + 2) = 5/12 = 0.4167
```

#### 5. Exam-style case study

April 2021 gives:

```text
n = 500
x = 200
```

MLE:

```text
p_hat = 200 / 500 = 0.4
```

Posterior with Uniform prior:

```text
p | data ~ Beta(201, 301)
```

Bayesian estimator under quadratic loss:

```text
201 / 502
```

Credibility factor:

```text
Z = 500 / 502
```

The Bayesian estimate is very close to the MLE because the sample size is large.

#### 6. Real-world actuarial case study

A motor insurer estimates the proportion of claim policies with claim amounts above INR 10,000.

With 500 observations, the data receives very high credibility. The Uniform prior has only a small smoothing effect.

#### 7. Common mistakes

- Forgetting Uniform(0,1) is Beta(1,1).
- Writing posterior as Beta(x, n-x) instead of Beta(x+1, n-x+1) under Uniform prior.
- Confusing MLE with posterior mean.
- Forgetting the credibility factor is close to 1 for large `n`.

#### 8. Revision checkpoint

Without notes, you should be able to write:

```text
p | data ~ Beta(a+x, b+n-x)
Uniform prior -> Beta(x+1, n-x+1)
Posterior mean = (x+1)/(n+2)
Z = n/(n+2)
```

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- Bayesian analysis combines prior information and data through the likelihood. The posterior distribution is proportional to prior times likelihood.
- Credibility theory has the same actuarial instinct: blend individual experience with collective experience. The more stable and relevant the individual data, the more credibility it receives.
- Conjugate priors are useful because the posterior stays in the same family. This makes updating transparent and exam calculations faster.

### Step-by-step working method

1. Write the prior distribution and identify its parameters.
2. Write the likelihood from the observed data.
3. Multiply prior and likelihood, keeping parameter-dependent terms.
4. Recognise the posterior family or normalise if required.
5. Use the posterior mean, mode, interval, or credibility form for the decision.

### Extra practical actuarial examples

- Renewal example: a beta prior for renewal probability can be updated after observing renewals and non-renewals.
- Claim count example: a gamma prior for a Poisson rate leads to a gamma posterior and a credibility-style blended estimate.
- Pricing example: a small employer group should not be priced only from its own volatile claims experience; credibility blends group experience with portfolio experience.

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

### Topic 51: Gamma Claim Model with Known Shape and Confidence Interval for Rate

#### 1. Concept theory

If claim amounts follow a Gamma distribution with known shape and unknown rate, the sum of claims also follows a Gamma distribution.

This can be transformed into a chi-square distribution and used to build a confidence interval for the unknown rate.

#### 2. Why actuaries care

Gamma severity models are common for positive claim amounts. Estimating the rate parameter controls the mean claim size:

```text
mean = alpha / lambda
```

So uncertainty in `lambda` directly affects pricing and reserving.

#### 3. Mathematical derivation

Let:

```text
Xi ~ Gamma(alpha, rate lambda)
```

For `n` independent observations:

```text
sum Xi ~ Gamma(n alpha, rate lambda)
```

If:

```text
Y ~ Gamma(A, rate lambda)
```

then:

```text
2 lambda Y ~ chi-square(2A)
```

Here:

```text
Y = sum Xi = n Xbar
A = n alpha
```

Therefore:

```text
2 lambda n Xbar ~ chi-square(2n alpha)
```

For a 95 percent confidence interval:

```text
chi-square lower <= 2 lambda n Xbar <= chi-square upper
```

Solve for `lambda`:

```text
lambda lower = chi-square lower / (2n Xbar)
lambda upper = chi-square upper / (2n Xbar)
```

#### 4. Simple example

Suppose:

```text
alpha = 5
n = 10
xbar = 100
```

Then:

```text
sum Xi ~ Gamma(50, lambda)
2 lambda n xbar ~ chi-square(100)
```

So:

```text
2 lambda (10)(100) = 2000 lambda
```

Use chi-square critical values with 100 degrees of freedom and divide by 2000.

#### 5. Exam-style case study

In April 2021:

```text
Xi ~ Gamma(alpha = 5, rate lambda)
n = 10
xbar = 100
```

Therefore:

```text
sum Xi ~ Gamma(50, lambda)
2 lambda n xbar ~ chi-square(100)
```

The confidence interval for `lambda` is:

```text
(chi-square lower / 2000, chi-square upper / 2000)
```

because:

```text
2n xbar = 2 x 10 x 100 = 2000
```

#### 6. Real-world actuarial case study

A motor insurer models claim severities using a Gamma distribution with a fixed shape from past studies. A fresh sample gives a mean claim size.

The actuary estimates the rate and builds a confidence interval to understand pricing uncertainty.

#### 7. Common mistakes

- Forgetting the shape of the sum is `n alpha`.
- Using `2 alpha lambda Xbar` instead of `2 lambda n Xbar`.
- Using chi-square degrees of freedom `2n` instead of `2n alpha`.
- Confusing rate with scale.
- Forgetting to solve the inequality for `lambda`.

#### 8. Revision checkpoint

Without notes, you should be able to write:

```text
sum Xi ~ Gamma(n alpha, lambda)
2 lambda n Xbar ~ chi-square(2n alpha)
CI for lambda = chi-square limits / (2n Xbar)
```

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- Inference is a disciplined way of deciding whether observed experience is consistent with an assumption. A confidence interval estimates a plausible range for a parameter; a hypothesis test checks whether the data are surprising under a stated null hypothesis.
- Type I error is rejecting a true null hypothesis. Type II error is failing to reject a false null hypothesis. Power is the probability of detecting an effect when it is truly present.
- A prediction interval is wider than a confidence interval for a mean because it includes both uncertainty about the average and the randomness of a new observation.

### Step-by-step working method

1. Identify the parameter being estimated or tested.
2. Write the null and alternative hypotheses, if a test is required.
3. Choose the reference distribution and degrees of freedom.
4. Compute the statistic, p-value, interval, or critical region.
5. Finish with a plain-English actuarial conclusion.

### Extra practical actuarial examples

- Fraud model example: a higher observed detection rate is useful only if the improvement is statistically convincing and operationally valuable after false positives are considered.
- Claims inflation example: test whether average claim cost has changed before changing a pricing basis, but also review exposure mix and large claims.
- Reserve example: a confidence interval for mean settlement cost should not be confused with a prediction interval for a single future claim.

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

### Topic 52: Weibull and Heavy-Tail Inverse Transform Simulation

#### 1. Concept theory

Inverse transform simulation turns a uniform random number into a simulated value from another distribution.

The method is:

```text
Set F(x) = u and solve for x
```

This works whenever the distribution function can be inverted.

#### 2. Why actuaries care

Actuaries simulate claim sizes, waiting times, lifetimes, and stress losses. Weibull models are used for survival and failure times. Heavy-tailed distributions are used for large claims.

#### 3. Mathematical derivation

If:

```text
F(x) = 1 - exp(-x^2), x > 0
```

set:

```text
u = 1 - exp(-x^2)
```

Then:

```text
exp(-x^2) = 1 - u
-x^2 = log(1 - u)
x = sqrt[-log(1 - u)]
```

For:

```text
f(x) = (1 + x)^(-2), x > 0
```

the distribution function is:

```text
F(x) = integral from 0 to x of (1+t)^(-2) dt
     = 1 - (1+x)^(-1)
```

Set:

```text
u = 1 - 1/(1+x)
```

Then:

```text
1/(1+x) = 1 - u
1 + x = 1/(1-u)
x = 1/(1-u) - 1
```

#### 4. Simple example

For Weibull-style:

```text
u = 0.75
x = sqrt[-log(0.25)]
  = 1.177
```

For heavy-tail:

```text
u = 0.5
x = 1/(1 - 0.5) - 1
  = 1
```

#### 5. Exam-style case study

In November 2020, for:

```text
F(x) = 1 - exp(-x^2)
u = 0.75
```

simulate:

```text
x = sqrt[-log(1 - 0.75)]
```

For:

```text
f(x) = (1+x)^(-2)
```

first derive:

```text
F(x) = 1 - (1+x)^(-1)
```

then use:

```text
x = 1/(1-u) - 1
```

#### 6. Real-world actuarial case study

A general insurer simulates large liability claims from a heavy-tailed distribution. A uniform random number from software is transformed into a simulated claim amount using the inverse CDF.

This is the foundation of Monte Carlo aggregate loss modelling.

#### 7. Common mistakes

- Forgetting to solve for `x`; the simulated value is not `u`.
- Dropping the square root in the Weibull example.
- Using `log(1-u)` without the negative sign.
- Forgetting to derive the CDF from the PDF before simulation.
- Confusing rate, scale, and shape parameters.

#### 8. Revision checkpoint

Without notes, you should be able to solve:

```text
F(x) = u
```

for `x`, especially for:

```text
F(x) = 1 - exp(-x^2)
F(x) = 1 - (1+x)^(-1)
```

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- An estimator is a rule for turning data into a parameter estimate. Different estimators can have different bias, variance, mean squared error, and robustness.
- Maximum likelihood chooses the parameter that makes the observed data most probable under the model. Method of moments matches sample moments to theoretical moments.
- Large-sample theory can give approximate intervals, but boundary estimates, censored observations, small samples, and heavy tails can make approximations unreliable.

### Step-by-step working method

1. Write the model and parameter space.
2. Choose MOM, MLE, least squares, or another estimator.
3. Derive or solve the estimating equation.
4. Check bias, variance, consistency, and practical stability where possible.
5. Interpret the estimate in actuarial units.

### Extra practical actuarial examples

- Severity example: estimating a Pareto tail parameter from a few large losses may be highly unstable, so sensitivity testing is essential.
- Frequency example: the Poisson MLE for a constant rate is total claims divided by total exposure.
- Simulation example: inverse transform simulation uses uniform random numbers to generate losses from a fitted distribution for stress testing.

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

### Topic 53: Truncated Poisson Distribution and Conditional Likelihood

#### 1. Concept theory

A truncated distribution is a distribution observed only after excluding some values.

If a portfolio only includes policies with more than one claim, then claim counts 0 and 1 are not observed. The Poisson probabilities must be conditioned on:

```text
X > 1
```

#### 2. Why actuaries care

Insurance data is often truncated. Examples include:

- only large claims above a threshold
- only policies with at least one claim
- only claimants with more than one claim
- only losses reported after a delay

Ignoring truncation biases parameter estimates.

#### 3. Mathematical derivation

For:

```text
X ~ Poisson(lambda)
```

the original probability is:

```text
P(X = x) = exp(-lambda)lambda^x / x!
```

If we observe only:

```text
X > 1
```

then the truncated probability is:

```text
P(X = x | X > 1) = P(X=x) / P(X>1), x = 2,3,...
```

Now:

```text
P(X > 1) = 1 - P(0) - P(1)
         = 1 - exp(-lambda) - lambda exp(-lambda)
```

So:

```text
P(X = x | X > 1)
= exp(-lambda)lambda^x / [x!(1 - exp(-lambda) - lambda exp(-lambda))]
```

The conditional mean is:

```text
E[X | X > 1] = E[X 1_{X>1}] / P(X>1)
```

Since only the `x=1` term is removed from the first moment:

```text
E[X 1_{X>1}] = lambda - lambda exp(-lambda)
             = lambda[1 - exp(-lambda)]
```

Therefore:

```text
E[X | X > 1] =
lambda[1 - exp(-lambda)] / [1 - exp(-lambda) - lambda exp(-lambda)]
```

Set this equal to the observed truncated sample mean to estimate `lambda`.

#### 4. Simple example

If:

```text
lambda = 1
```

then:

```text
P(X > 1) = 1 - e^-1 - e^-1 = 1 - 2e^-1
```

and:

```text
P(X = 2 | X > 1) = e^-1 / [2(1 - 2e^-1)]
```

#### 5. Exam-style case study

In November 2020, the observed table includes:

```text
2 claims: 230
3 claims: 54
4 or more: 6
```

This is not an ordinary Poisson sample. It is conditional on:

```text
X > 1
```

Use the truncated probability:

```text
P(X=x | X>1)
```

and solve the likelihood or conditional mean equation for `lambda`, often by trial values and interpolation.

#### 6. Real-world actuarial case study

A motor insurer studies only repeat claimants with at least two claims. If the actuary fits an ordinary Poisson model to this data, claim frequency will be overestimated for the whole portfolio.

The correct model conditions on being a repeat claimant.

#### 7. Common mistakes

- Fitting ordinary Poisson probabilities to truncated data.
- Forgetting to divide by `P(X>1)`.
- Treating the `>=4` group as exactly 4 without care.
- Using the untruncated mean `lambda` as the sample mean.
- Forgetting that truncation changes both probabilities and expected value.

#### 8. Revision checkpoint

Without notes, you should be able to write:

```text
P(X=x | X>1) = P(X=x) / [1 - P(0) - P(1)]
E[X | X>1] = lambda[1 - exp(-lambda)] / [1 - exp(-lambda) - lambda exp(-lambda)]
```

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- An estimator is a rule for turning data into a parameter estimate. Different estimators can have different bias, variance, mean squared error, and robustness.
- Maximum likelihood chooses the parameter that makes the observed data most probable under the model. Method of moments matches sample moments to theoretical moments.
- Large-sample theory can give approximate intervals, but boundary estimates, censored observations, small samples, and heavy tails can make approximations unreliable.

### Step-by-step working method

1. Write the model and parameter space.
2. Choose MOM, MLE, least squares, or another estimator.
3. Derive or solve the estimating equation.
4. Check bias, variance, consistency, and practical stability where possible.
5. Interpret the estimate in actuarial units.

### Extra practical actuarial examples

- Severity example: estimating a Pareto tail parameter from a few large losses may be highly unstable, so sensitivity testing is essential.
- Frequency example: the Poisson MLE for a constant rate is total claims divided by total exposure.
- Simulation example: inverse transform simulation uses uniform random numbers to generate losses from a fitted distribution for stress testing.

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

### Topic 54: Probability Generating Functions and Variance

#### 1. Concept theory

A probability generating function, or PGF, is used for non-negative integer-valued random variables.

It is defined as:

```text
G(s) = E[s^X]
```

PGFs are especially useful for claim counts.

#### 2. Why actuaries care

Claim count distributions such as Poisson, binomial, geometric, and compound distributions are often handled using PGFs.

PGFs can quickly give means, variances, and aggregate claim count results.

#### 3. Mathematical derivation

PGF:

```text
G(s) = E[s^X]
```

Differentiate:

```text
G'(s) = E[X s^(X-1)]
```

At `s = 1`:

```text
G'(1) = E[X]
```

Second derivative:

```text
G''(s) = E[X(X-1)s^(X-2)]
```

At `s = 1`:

```text
G''(1) = E[X(X-1)]
```

Now:

```text
E[X^2] = E[X(X-1)] + E[X]
```

Therefore:

```text
Var(X) = E[X^2] - [E[X]]^2
       = G''(1) + G'(1) - [G'(1)]^2
```

#### 4. Simple example

If:

```text
X ~ Poisson(lambda)
```

then:

```text
G(s) = exp(lambda(s-1))
```

So:

```text
G'(1) = lambda
G''(1) = lambda^2
```

Variance:

```text
lambda^2 + lambda - lambda^2 = lambda
```

#### 5. Exam-style case study

If the exam asks for variance in terms of PGF, the correct formula is:

```text
Var(X) = G''(1) + G'(1) - [G'(1)]^2
```

The trap is that PGF derivatives are evaluated at:

```text
s = 1
```

not at zero.

#### 6. Real-world actuarial case study

An actuary uses PGFs to study the total number of claims from a portfolio. The mean and variance of claim count are needed before attaching severity assumptions.

PGFs make these calculations neat and reduce algebra.

#### 7. Common mistakes

- Evaluating PGF derivatives at 0 instead of 1.
- Forgetting the `+ G'(1)` term.
- Confusing PGF with MGF.
- Using PGF for continuous variables.
- Forgetting PGF applies to non-negative integer counts.

#### 8. Revision checkpoint

Without notes, you should be able to write:

```text
G(s) = E[s^X]
E[X] = G'(1)
Var(X) = G''(1) + G'(1) - [G'(1)]^2
```

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- Begin by checking support. A count model lives on non-negative integers, a severity model often lives on positive real values, and a transformed model may live on the whole real line after taking logs. Many exam errors happen before calculation because the wrong support is chosen.
- Separate the probability law from its moments. The probability law tells you every probability statement; the mean and variance are summaries. In actuarial work a model with the correct mean but the wrong tail can still be dangerous because solvency questions are tail questions.
- Use generating functions when sums are involved. MGFs, CGFs, and PGFs turn convolutions into algebra, but only under the conditions where they exist and where independence assumptions are valid.

### Step-by-step working method

1. State the random variable and its support.
2. Write the probability mass function, density, or distribution function.
3. Identify parameters and their actuarial meaning.
4. Calculate the required moment, probability, percentile, or transform.
5. Check whether the answer is sensible using units, range, and limiting cases.

### Extra practical actuarial examples

- Claim count example: if a motor portfolio has many zero-claim policies and a few multi-claim policies, compare Poisson and negative binomial assumptions by checking whether sample variance is close to, below, or above the sample mean.
- Severity example: for small routine medical claims a lognormal or gamma model may be adequate; for catastrophe-like property claims a heavier tail such as Pareto or Weibull may be needed.
- Exam example: after finding a probability, ask whether the question wanted an unconditional probability or a conditional probability after observing a claim, survival, or selection event.

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

### Topic 55: Normal-Normal Posterior Probability Can Increase Even When Posterior Mean Falls

#### 1. Concept theory

In Bayesian normal updating, the posterior mean is a weighted average of the prior mean and the sample mean. If the sample mean is below the prior mean, the posterior mean usually falls.

But a probability such as \(P(\mu > 60 \mid data)\) depends on two things:

- where the posterior mean is
- how small the posterior variance is

So it is possible for the posterior mean to decrease, but the posterior probability of being above a threshold to increase. This happens when the data greatly reduce uncertainty.

#### 2. Why actuaries care

Actuaries often update assumptions after receiving new experience:

- average claim cost after new claims data
- mortality improvement after recent experience
- lapse rates after a new product launch
- average fraud loss after an investigation sample
- investment return assumptions after new market data

The important actuarial point is this: more data may shift the estimate slightly but can greatly increase confidence.

#### 3. Mathematical derivation

Assume:

\[
\mu \sim N(m_0, s_0^2)
\]

and observations:

\[
X_1,\ldots,X_n \mid \mu \sim N(\mu,\sigma^2)
\]

where \(\sigma^2\) is known.

The sample mean has distribution:

\[
\bar X \mid \mu \sim N\left(\mu,\frac{\sigma^2}{n}\right)
\]

The posterior precision is prior precision plus data precision:

\[
\frac{1}{s_1^2}=\frac{1}{s_0^2}+\frac{n}{\sigma^2}
\]

So:

\[
s_1^2=\frac{1}{\frac{1}{s_0^2}+\frac{n}{\sigma^2}}
\]

The posterior mean is:

\[
m_1=s_1^2\left(\frac{m_0}{s_0^2}+\frac{n\bar x}{\sigma^2}\right)
\]

Equivalently:

\[
m_1=\frac{\frac{n\bar x}{\sigma^2}+\frac{m_0}{s_0^2}}{\frac{n}{\sigma^2}+\frac{1}{s_0^2}}
\]

Then:

\[
P(\mu>a \mid data)=P\left(Z>\frac{a-m_1}{s_1}\right)
\]

#### 4. Simple example

Suppose:

\[
\mu \sim N(65,17^2)
\]

\[
\sigma=20,\quad n=150,\quad \bar x=63
\]

Prior probability that \(\mu>60\):

\[
P(\mu>60)=P\left(Z>\frac{60-65}{17}\right)
\]

\[
=P(Z>-0.294)=0.6157
\]

Posterior mean:

\[
m_1=
\frac{\frac{150(63)}{20^2}+\frac{65}{17^2}}
{\frac{150}{20^2}+\frac{1}{17^2}}
=63.02
\]

Posterior variance:

\[
s_1^2=
\frac{1}{\frac{150}{400}+\frac{1}{289}}
=2.6423
\]

So:

\[
s_1=1.6255
\]

Posterior probability:

\[
P(\mu>60\mid data)
=P\left(Z>\frac{60-63.02}{1.6255}\right)
\]

\[
=P(Z>-1.858)=0.9686
\]

The posterior mean moved down from 65 to 63.02, but the probability of exceeding 60 increased from about 61.57% to 96.86%.

#### 5. Exam-style case study

An insurer believes the average claim handling score for a claims team has prior distribution:

\[
\mu \sim N(65,17^2)
\]

A sample of 150 cases has mean score 63. Individual scores have known standard deviation 20.

Find:

1. the prior probability that \(\mu>60\)
2. the posterior distribution of \(\mu\)
3. the posterior probability that \(\mu>60\)
4. explain why the probability increased despite a lower posterior mean

Solution:

\[
P(\mu>60)=P(Z>-0.294)=0.6157
\]

\[
m_1=63.02,\quad s_1^2=2.6423
\]

\[
\mu\mid data \sim N(63.02,2.6423)
\]

\[
P(\mu>60\mid data)=P(Z>-1.858)=0.9686
\]

The data made the estimate much more precise. Since 60 is now many posterior standard deviations below the posterior mean, the posterior probability is high.

#### 6. Real-world actuarial case study

A health insurer estimates the average cost of a telemedicine consultation. The prior mean is high because earlier experience came from specialist-heavy usage. New data from 150 consultations gives a slightly lower mean.

Management asks: "Has the new evidence weakened our confidence that average cost is above the pricing threshold?"

The actuary explains that the best estimate has fallen slightly, but the uncertainty has fallen much more. Therefore, the probability that the true mean is above the pricing threshold may actually increase.

This matters for pricing, reserving, and risk appetite communication.

#### 7. Common mistakes

- Thinking lower posterior mean always means lower tail probability.
- Forgetting to use the posterior standard deviation, not posterior variance, in the Z-score.
- Mixing up \(\sigma^2\), the individual observation variance, with \(\sigma^2/n\), the variance of the sample mean.
- Ignoring the prior precision \(1/s_0^2\).
- Saying "posterior probability" as if it is the same as a frequentist confidence level.

#### 8. Revision checkpoint

You should be able to:

- calculate normal-normal posterior mean and variance
- explain prior precision plus data precision
- calculate a posterior tail probability
- explain why reduced uncertainty can increase a probability even if the posterior mean falls

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- Bayesian analysis combines prior information and data through the likelihood. The posterior distribution is proportional to prior times likelihood.
- Credibility theory has the same actuarial instinct: blend individual experience with collective experience. The more stable and relevant the individual data, the more credibility it receives.
- Conjugate priors are useful because the posterior stays in the same family. This makes updating transparent and exam calculations faster.

### Step-by-step working method

1. Write the prior distribution and identify its parameters.
2. Write the likelihood from the observed data.
3. Multiply prior and likelihood, keeping parameter-dependent terms.
4. Recognise the posterior family or normalise if required.
5. Use the posterior mean, mode, interval, or credibility form for the decision.

### Extra practical actuarial examples

- Renewal example: a beta prior for renewal probability can be updated after observing renewals and non-renewals.
- Claim count example: a gamma prior for a Poisson rate leads to a gamma posterior and a credibility-style blended estimate.
- Pricing example: a small employer group should not be priced only from its own volatile claims experience; credibility blends group experience with portfolio experience.

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

### Topic 56: Normal Sample Mean and Variance Joint Probability

#### 1. Concept theory

For a random sample from a normal distribution, the sample mean and sample variance are independent.

This is a special property of normal samples. It allows probabilities involving both \(\bar X\) and \(S^2\) to be multiplied.

#### 2. Why actuaries care

Actuaries often need to judge both:

- the average level of claims
- the variability of claims

For normal models, mean risk and variance risk can be separated neatly. This is useful in claim amount modelling, expense analysis, investment return analysis, and quality-control testing.

#### 3. Mathematical derivation

Let:

\[
X_1,\ldots,X_n \sim N(\mu,\sigma^2)
\]

Then:

\[
\bar X \sim N\left(\mu,\frac{\sigma^2}{n}\right)
\]

and:

\[
\frac{(n-1)S^2}{\sigma^2}\sim \chi^2_{n-1}
\]

For a normal sample:

\[
\bar X \perp S^2
\]

Therefore:

\[
P(\bar X<a,\ S<b)
=P(\bar X<a)P(S<b)
\]

For the variance part:

\[
P(S<b)=P(S^2<b^2)
\]

\[
=P\left(\frac{(n-1)S^2}{\sigma^2}<\frac{(n-1)b^2}{\sigma^2}\right)
\]

Then use the chi-square distribution with \(n-1\) degrees of freedom.

#### 4. Simple example

Suppose:

\[
X_i\sim N(2000,500^2),\quad n=10
\]

Find:

\[
P(\bar X<1700,\ S<250)
\]

For the mean:

\[
\bar X\sim N\left(2000,\frac{500^2}{10}\right)
\]

\[
Z=\frac{1700-2000}{500/\sqrt{10}}=-1.897
\]

\[
P(\bar X<1700)=0.0289
\]

For the standard deviation:

\[
\frac{9S^2}{500^2}\sim \chi^2_9
\]

If \(S<250\):

\[
\frac{9S^2}{500^2}<\frac{9(250)^2}{500^2}
\]

\[
=2.25
\]

So:

\[
P(S<250)=P(\chi^2_9<2.25)=0.0131
\]

Using independence:

\[
P(\bar X<1700,\ S<250)=0.0289(0.0131)
\]

\[
=0.000379
\]

That is approximately:

\[
0.038\%
\]

#### 5. Exam-style case study

Invoice amounts in a portfolio are normally distributed with mean 2000 and standard deviation 500. A random sample of 10 invoices is selected.

Find the probability that:

- the sample mean is less than 1700
- the sample standard deviation is less than 250

Solution:

\[
P(\bar X<1700)=P\left(Z<\frac{1700-2000}{500/\sqrt{10}}\right)=0.0289
\]

\[
P(S<250)=P\left(\chi^2_9<\frac{9(250)^2}{500^2}\right)
\]

\[
=P(\chi^2_9<2.25)=0.0131
\]

Since the sample is normal:

\[
\bar X \text{ and } S^2 \text{ are independent}
\]

\[
P=0.0289\times 0.0131=0.000379
\]

#### 6. Real-world actuarial case study

An insurer monitors average repair invoice amounts from a garage network. A low sample mean may indicate discounted repairs, while a low sample standard deviation may indicate unusually consistent billing.

If invoice amounts are approximately normal, the actuary can calculate the probability of seeing both events together. A very small probability may trigger an audit, but the actuary should also check whether normality is realistic before escalating.

#### 7. Common mistakes

- Multiplying probabilities without justifying independence.
- Forgetting that \(\bar X\) and \(S^2\) are independent only for normal samples.
- Using \(n\) instead of \(n-1\) in the chi-square statistic.
- Comparing \(S\) directly with chi-square instead of converting \(S^2\).
- Treating 0.000379 as 0.379% instead of 0.0379%.

#### 8. Revision checkpoint

You should be able to:

- write the sampling distribution of \(\bar X\)
- write the chi-square distribution of \((n-1)S^2/\sigma^2\)
- use independence of \(\bar X\) and \(S^2\) for normal samples
- calculate a joint probability by multiplying the two parts

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- Begin by checking support. A count model lives on non-negative integers, a severity model often lives on positive real values, and a transformed model may live on the whole real line after taking logs. Many exam errors happen before calculation because the wrong support is chosen.
- Separate the probability law from its moments. The probability law tells you every probability statement; the mean and variance are summaries. In actuarial work a model with the correct mean but the wrong tail can still be dangerous because solvency questions are tail questions.
- Use generating functions when sums are involved. MGFs, CGFs, and PGFs turn convolutions into algebra, but only under the conditions where they exist and where independence assumptions are valid.

### Step-by-step working method

1. State the random variable and its support.
2. Write the probability mass function, density, or distribution function.
3. Identify parameters and their actuarial meaning.
4. Calculate the required moment, probability, percentile, or transform.
5. Check whether the answer is sensible using units, range, and limiting cases.

### Extra practical actuarial examples

- Claim count example: if a motor portfolio has many zero-claim policies and a few multi-claim policies, compare Poisson and negative binomial assumptions by checking whether sample variance is close to, below, or above the sample mean.
- Severity example: for small routine medical claims a lognormal or gamma model may be adequate; for catastrophe-like property claims a heavier tail such as Pareto or Weibull may be needed.
- Exam example: after finding a probability, ask whether the question wanted an unconditional probability or a conditional probability after observing a claim, survival, or selection event.

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

### Topic 57: Chi-Square Association Test for a Two-by-Three Table

#### 1. Concept theory

A chi-square test of association checks whether two categorical variables are related.

The observed counts are compared with expected counts that would occur if the two variables were independent.

#### 2. Why actuaries care

Actuaries use association tests to investigate whether experience differs across categories, such as:

- claim type and policyholder age band
- lapse status and distribution channel
- fraud flag and claim settlement route
- mortality experience and underwriting class
- complaint category and product type

This helps decide whether a rating factor or risk classification may be useful.

#### 3. Mathematical derivation

For a contingency table:

\[
E_{ij}=\frac{(\text{row total}_i)(\text{column total}_j)}{\text{grand total}}
\]

The chi-square statistic is:

\[
X^2=\sum_{i}\sum_{j}\frac{(O_{ij}-E_{ij})^2}{E_{ij}}
\]

where:

- \(O_{ij}\) is the observed frequency
- \(E_{ij}\) is the expected frequency

For an \(r\times c\) table:

\[
df=(r-1)(c-1)
\]

For a \(2\times 3\) table:

\[
df=(2-1)(3-1)=2
\]

Reject independence if:

\[
X^2>\chi^2_{\alpha,df}
\]

#### 4. Simple example

Suppose a sample of 500 policies is classified by gender and product type.

If one row total is 250 and one column total is 200, the expected count for that cell under independence is:

\[
E=\frac{250(200)}{500}=100
\]

If the observed count is 130, its contribution is:

\[
\frac{(130-100)^2}{100}=9
\]

Repeat for every cell and add the contributions.

#### 5. Exam-style case study

A \(2\times3\) contingency table gives a chi-square statistic:

\[
X^2=95.74
\]

Test at the 1% significance level whether the two classifications are independent.

Degrees of freedom:

\[
df=(2-1)(3-1)=2
\]

At 1% significance:

\[
\chi^2_{0.01,2}=9.21
\]

Since:

\[
95.74>9.21
\]

reject the null hypothesis of independence.

Conclusion: there is strong evidence of association between the two categorical variables.

#### 6. Real-world actuarial case study

A life insurer compares policy lapse behaviour across sales channels and policy size bands. A chi-square association test shows strong evidence that lapse category is associated with channel-size combination.

The actuary does not immediately conclude causation. Instead, the result motivates further modelling, such as logistic regression, to control for age, duration, premium frequency, and product type.

#### 7. Common mistakes

- Using \(r+c-2\) instead of \((r-1)(c-1)\) for degrees of freedom.
- Calculating expected counts using percentages instead of row and column totals.
- Saying the test proves causation.
- Forgetting to state the null hypothesis: no association or independence.
- Using the test when expected counts are too small without considering grouping or exact methods.

#### 8. Revision checkpoint

You should be able to:

- calculate expected counts in a contingency table
- compute or interpret a chi-square statistic
- find degrees of freedom for an \(r\times c\) table
- state the conclusion in actuarial language

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- Inference is a disciplined way of deciding whether observed experience is consistent with an assumption. A confidence interval estimates a plausible range for a parameter; a hypothesis test checks whether the data are surprising under a stated null hypothesis.
- Type I error is rejecting a true null hypothesis. Type II error is failing to reject a false null hypothesis. Power is the probability of detecting an effect when it is truly present.
- A prediction interval is wider than a confidence interval for a mean because it includes both uncertainty about the average and the randomness of a new observation.

### Step-by-step working method

1. Identify the parameter being estimated or tested.
2. Write the null and alternative hypotheses, if a test is required.
3. Choose the reference distribution and degrees of freedom.
4. Compute the statistic, p-value, interval, or critical region.
5. Finish with a plain-English actuarial conclusion.

### Extra practical actuarial examples

- Fraud model example: a higher observed detection rate is useful only if the improvement is statistically convincing and operationally valuable after false positives are considered.
- Claims inflation example: test whether average claim cost has changed before changing a pricing basis, but also review exposure mix and large claims.
- Reserve example: a confidence interval for mean settlement cost should not be confused with a prediction interval for a single future claim.

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

### Topic 58: Shifted Geometric Distribution, MGF, CGF, and Mean

#### 1. Concept theory

A shifted geometric random variable counts the trial number on which the first success occurs.

If:

\[
P(X=x)=p(1-p)^{x-1},\quad x=1,2,3,\ldots
\]

then \(X=1\) means success on the first trial, \(X=2\) means one failure followed by success, and so on.

#### 2. Why actuaries care

This model appears whenever the question is "how many attempts until the first event?":

- number of policy contacts until a sale
- number of premium reminders until payment
- number of monthly periods until first claim
- number of inspections until first defect
- number of medical tests until first positive result

It is a simple waiting-time model for repeated independent trials.

#### 3. Mathematical derivation

The moment generating function is:

\[
M_X(t)=E(e^{tX})
\]

\[
=\sum_{x=1}^{\infty}e^{tx}p(1-p)^{x-1}
\]

Take out \(pe^t\):

\[
M_X(t)=pe^t\sum_{x=1}^{\infty}\left(e^t(1-p)\right)^{x-1}
\]

Using the geometric series:

\[
\sum_{j=0}^{\infty}r^j=\frac{1}{1-r}
\]

where \(|r|<1\), we get:

\[
M_X(t)=\frac{pe^t}{1-(1-p)e^t}
\]

The cumulant generating function is:

\[
K_X(t)=\log M_X(t)
\]

\[
K_X(t)=\log p+t-\log\left(1-(1-p)e^t\right)
\]

To find the mean:

\[
E(X)=M_X'(0)
\]

or:

\[
E(X)=K_X'(0)
\]

Differentiate:

\[
K_X'(t)=1+\frac{(1-p)e^t}{1-(1-p)e^t}
\]

At \(t=0\):

\[
K_X'(0)=1+\frac{1-p}{p}
\]

\[
=\frac{1}{p}
\]

So:

\[
E(X)=\frac{1}{p}
\]

#### 4. Simple example

If the probability of a policyholder responding to a reminder is \(p=0.25\), then:

\[
E(X)=\frac{1}{0.25}=4
\]

So on average, the first response occurs on the fourth reminder.

#### 5. Exam-style case study

A random variable has:

\[
P(X=x)=p(1-p)^{x-1},\quad x=1,2,\ldots
\]

Derive the MGF, CGF, and mean.

Solution:

\[
M_X(t)=\sum_{x=1}^{\infty}e^{tx}p(1-p)^{x-1}
\]

\[
=pe^t\sum_{x=1}^{\infty}\left((1-p)e^t\right)^{x-1}
\]

\[
=\frac{pe^t}{1-(1-p)e^t}
\]

\[
K_X(t)=\log p+t-\log(1-(1-p)e^t)
\]

\[
E(X)=K_X'(0)=\frac{1}{p}
\]

#### 6. Real-world actuarial case study

A life insurer sends renewal reminders to policyholders. If each reminder independently has a 20% chance of producing payment, the number of reminders until payment can be modelled using a shifted geometric distribution.

The expected number of reminders is:

\[
\frac{1}{0.20}=5
\]

This helps estimate operational workload and reminder costs.

#### 7. Common mistakes

- Confusing this with the version that counts failures before the first success.
- Starting the sum at \(x=0\) instead of \(x=1\).
- Forgetting the condition \(|(1-p)e^t|<1\).
- Writing the CGF as the MGF without taking logs.
- Using \(1/(1-p)\) instead of \(1/p\) for the mean.

#### 8. Revision checkpoint

You should be able to:

- identify a shifted geometric distribution
- derive its MGF from a geometric series
- write its CGF
- calculate \(E(X)=1/p\)

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- Begin by checking support. A count model lives on non-negative integers, a severity model often lives on positive real values, and a transformed model may live on the whole real line after taking logs. Many exam errors happen before calculation because the wrong support is chosen.
- Separate the probability law from its moments. The probability law tells you every probability statement; the mean and variance are summaries. In actuarial work a model with the correct mean but the wrong tail can still be dangerous because solvency questions are tail questions.
- Use generating functions when sums are involved. MGFs, CGFs, and PGFs turn convolutions into algebra, but only under the conditions where they exist and where independence assumptions are valid.

### Step-by-step working method

1. State the random variable and its support.
2. Write the probability mass function, density, or distribution function.
3. Identify parameters and their actuarial meaning.
4. Calculate the required moment, probability, percentile, or transform.
5. Check whether the answer is sensible using units, range, and limiting cases.

### Extra practical actuarial examples

- Claim count example: if a motor portfolio has many zero-claim policies and a few multi-claim policies, compare Poisson and negative binomial assumptions by checking whether sample variance is close to, below, or above the sample mean.
- Severity example: for small routine medical claims a lognormal or gamma model may be adequate; for catastrophe-like property claims a heavier tail such as Pareto or Weibull may be needed.
- Exam example: after finding a probability, ask whether the question wanted an unconditional probability or a conditional probability after observing a claim, survival, or selection event.

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

### Topic 59: Binomial Occupancy MLE and Two-Proportion Confidence Interval

#### 1. Concept theory

If each car has \(n\) available passenger seats and each seat is occupied independently with probability \(p\), then the number of occupied seats can be modelled as:

\[
X\sim Bin(n,p)
\]

The maximum likelihood estimate of \(p\) is:

\[
\hat p=\frac{\text{total occupied seats}}{\text{total available seats}}
\]

For comparing two cities, we often compare two proportions using a confidence interval for:

\[
\theta_1-\theta_2
\]

#### 2. Why actuaries care

This style of question is very close to actuarial rating and experience analysis:

- proportion of policies with no claims
- proportion of customers lapsing
- proportion of claims settled below a threshold
- proportion of hospitals with high utilisation
- comparison of two branches, cities, products, or portfolios

The exam skill is to identify the correct denominator.

#### 3. Mathematical derivation

For grouped binomial observations, suppose \(f_x\) cars have \(x\) occupied seats, where:

\[
x=0,1,\ldots,n
\]

The likelihood is:

\[
L(p)=\prod_x \left[\binom{n}{x}p^x(1-p)^{n-x}\right]^{f_x}
\]

The log-likelihood is:

\[
\ell(p)=C+\sum_x f_xx\log p+\sum_x f_x(n-x)\log(1-p)
\]

Differentiate:

\[
\frac{d\ell}{dp}=\frac{\sum f_xx}{p}-\frac{\sum f_x(n-x)}{1-p}
\]

Set equal to zero:

\[
\frac{\sum f_xx}{p}=\frac{\sum f_x(n-x)}{1-p}
\]

\[
(1-p)\sum f_xx=p\sum f_x(n-x)
\]

\[
\sum f_xx=p\sum f_xn
\]

Therefore:

\[
\hat p=\frac{\sum f_xx}{n\sum f_x}
\]

For two sample proportions:

\[
\hat\theta_1=\frac{x_1}{n_1},\quad \hat\theta_2=\frac{x_2}{n_2}
\]

An approximate 95% confidence interval for \(\theta_1-\theta_2\) is:

\[
(\hat\theta_1-\hat\theta_2)
\pm 1.96
\sqrt{
\frac{\hat\theta_1(1-\hat\theta_1)}{n_1}
+
\frac{\hat\theta_2(1-\hat\theta_2)}{n_2}
}
\]

#### 4. Simple example

City A observes 100 cars and 160 occupied seats across 400 available seats.

\[
\hat p=\frac{160}{400}=0.40
\]

If 30 out of 100 cars in City A and 45 out of 120 cars in City B have fewer than two passengers:

\[
\hat\theta_1=0.30,\quad \hat\theta_2=0.375
\]

\[
\hat\theta_1-\hat\theta_2=-0.075
\]

The standard error is:

\[
\sqrt{\frac{0.3(0.7)}{100}+\frac{0.375(0.625)}{120}}
=0.0637
\]

95% interval:

\[
-0.075\pm1.96(0.0637)
\]

\[
=(-0.200,\ 0.050)
\]

Since zero is inside the interval, the difference is not statistically clear at 5%.

#### 5. Exam-style case study

For City A, observed cars have 0, 1, 2, 3, and 4 occupied passenger seats with frequencies:

\[
70,\ 120,\ 201,\ 80,\ 29
\]

There are 4 passenger seats per car.

Total occupied seats:

\[
0(70)+1(120)+2(201)+3(80)+4(29)=878
\]

Total seats:

\[
4(500)=2000
\]

So:

\[
\hat p=\frac{878}{2000}=0.439
\]

For "less than 2 passengers":

City A:

\[
\hat\theta_1=\frac{70+120}{500}=0.380
\]

City B:

\[
\hat\theta_2=\frac{40+100}{540}=0.2593
\]

Difference:

\[
\hat\theta_1-\hat\theta_2=0.1207
\]

Approximate 95% interval:

\[
0.1207\pm1.96
\sqrt{
\frac{0.38(0.62)}{500}
+
\frac{0.2593(0.7407)}{540}
}
\]

Since the interval is above zero if the lower bound remains positive, it suggests City A has a higher proportion of cars with fewer than two passengers.

#### 6. Real-world actuarial case study

A motor insurer compares two cities. City A has more cars with low occupancy, which may affect injury claim frequency per accident. If low occupancy is related to lower bodily injury exposure, this could become a rating or underwriting insight.

But the actuary must avoid jumping directly from a proportion difference to pricing action. Traffic density, car type, commute distance, and road type may also matter.

#### 7. Common mistakes

- Using number of cars as the denominator for \(\hat p\) instead of total seats.
- Treating "less than 2 passengers" as only zero passengers instead of zero or one.
- Forgetting that City A and City B can have different sample sizes.
- Using a pooled standard error when the question asks for a confidence interval rather than a hypothesis test.
- Claiming a difference is significant when the confidence interval contains zero.

#### 8. Revision checkpoint

You should be able to:

- derive the binomial MLE from grouped data
- calculate total occupied seats from a frequency table
- calculate a two-proportion confidence interval
- interpret whether zero inside the interval means no clear difference

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- Inference is a disciplined way of deciding whether observed experience is consistent with an assumption. A confidence interval estimates a plausible range for a parameter; a hypothesis test checks whether the data are surprising under a stated null hypothesis.
- Type I error is rejecting a true null hypothesis. Type II error is failing to reject a false null hypothesis. Power is the probability of detecting an effect when it is truly present.
- A prediction interval is wider than a confidence interval for a mean because it includes both uncertainty about the average and the randomness of a new observation.

### Step-by-step working method

1. Identify the parameter being estimated or tested.
2. Write the null and alternative hypotheses, if a test is required.
3. Choose the reference distribution and degrees of freedom.
4. Compute the statistic, p-value, interval, or critical region.
5. Finish with a plain-English actuarial conclusion.

### Extra practical actuarial examples

- Fraud model example: a higher observed detection rate is useful only if the improvement is statistically convincing and operationally valuable after false positives are considered.
- Claims inflation example: test whether average claim cost has changed before changing a pricing basis, but also review exposure mix and large claims.
- Reserve example: a confidence interval for mean settlement cost should not be confused with a prediction interval for a single future claim.

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

### Topic 60: Student t Distribution and Confidence Interval for a Normal Mean

#### 1. Concept theory

The Student t distribution is used when estimating a normal population mean and the population variance is unknown.

Instead of using the true standard deviation \(\sigma\), we use the sample standard deviation \(S\). This extra uncertainty makes the t distribution wider than the standard normal distribution, especially for small samples.

#### 2. Why actuaries care

Actuaries often estimate an average from limited data:

- average claim size from a small sample
- average expense per policy
- average settlement delay
- average hospital cost
- average investment return over a short period

When the sample is small and \(\sigma\) is unknown, the t interval is more appropriate than a normal interval.

#### 3. Mathematical derivation

Let:

\[
X_1,\ldots,X_n \sim N(\mu,\sigma^2)
\]

Then:

\[
\bar X \sim N\left(\mu,\frac{\sigma^2}{n}\right)
\]

and:

\[
\frac{(n-1)S^2}{\sigma^2}\sim \chi^2_{n-1}
\]

The t statistic is:

\[
t_k=\frac{Z}{\sqrt{U/k}}
\]

where:

- \(Z\sim N(0,1)\)
- \(U\sim \chi^2_k\)
- \(Z\) and \(U\) are independent
- \(k\) is the degrees of freedom

For a sample mean:

\[
\frac{\bar X-\mu}{S/\sqrt n}\sim t_{n-1}
\]

For \(k>2\):

\[
E(t_k)=0
\]

\[
Var(t_k)=\frac{k}{k-2}
\]

The confidence interval for \(\mu\) is:

\[
\bar x\pm t_{n-1,\alpha/2}\frac{s}{\sqrt n}
\]

#### 4. Simple example

Suppose:

\[
n=10,\quad \bar x=50,\quad s^2=48.667
\]

Then:

\[
s=\sqrt{48.667}=6.976
\]

At 99% confidence with \(9\) degrees of freedom:

\[
t_{9,0.005}\approx3.25
\]

So:

\[
50\pm3.25\frac{6.976}{\sqrt{10}}
\]

\[
=50\pm7.17
\]

The interval is:

\[
(42.83,\ 57.17)
\]

#### 5. Exam-style case study

A sample of 10 observations from a normal population has:

\[
\bar x=50,\quad s^2=48.667
\]

Find a 99% confidence interval for the population mean.

Solution:

\[
\frac{\bar X-\mu}{S/\sqrt n}\sim t_9
\]

Using:

\[
t_{9,0.005}=3.25
\]

\[
CI=50\pm3.25\sqrt{\frac{48.667}{10}}
\]

\[
=(42.83,\ 57.17)
\]

If the t distribution is approximated by a normal distribution using:

\[
t_k\approx N\left(0,\frac{k}{k-2}\right)
\]

for \(k=9\):

\[
Var(t_9)=\frac{9}{7}
\]

The adjusted normal interval uses:

\[
2.58\sqrt{\frac{9}{7}}\frac{s}{\sqrt n}
\]

This gives an interval close to:

\[
(43.54,\ 56.45)
\]

#### 6. Real-world actuarial case study

A health insurer has only 10 large hospital claims from a new benefit design. The sample average is useful, but the uncertainty is high.

Using a t interval gives wider bounds for the true average cost. This prevents the pricing team from being overconfident when experience data is thin.

#### 7. Common mistakes

- Using a normal critical value for a small sample when \(\sigma\) is unknown.
- Using \(n\) degrees of freedom instead of \(n-1\).
- Forgetting to take the square root of the sample variance.
- Treating \(Var(t_k)=1\); it is \(k/(k-2)\) for \(k>2\).
- Mixing up 99% critical values with 95% critical values.

#### 8. Revision checkpoint

You should be able to:

- define a t random variable as \(Z/\sqrt{U/k}\)
- state its mean and variance
- build a confidence interval for a normal mean with unknown variance
- explain why the t interval is wider than the normal interval for small samples

### Expanded deep explanation

A distribution is not just a formula; it is a story about how risk is generated. Always connect support, parameters, mean, variance, tail behaviour, and actuarial use.

- Inference is a disciplined way of deciding whether observed experience is consistent with an assumption. A confidence interval estimates a plausible range for a parameter; a hypothesis test checks whether the data are surprising under a stated null hypothesis.
- Type I error is rejecting a true null hypothesis. Type II error is failing to reject a false null hypothesis. Power is the probability of detecting an effect when it is truly present.
- A prediction interval is wider than a confidence interval for a mean because it includes both uncertainty about the average and the randomness of a new observation.

### Step-by-step working method

1. Identify the parameter being estimated or tested.
2. Write the null and alternative hypotheses, if a test is required.
3. Choose the reference distribution and degrees of freedom.
4. Compute the statistic, p-value, interval, or critical region.
5. Finish with a plain-English actuarial conclusion.

### Extra practical actuarial examples

- Fraud model example: a higher observed detection rate is useful only if the improvement is statistically convincing and operationally valuable after false positives are considered.
- Claims inflation example: test whether average claim cost has changed before changing a pricing basis, but also review exposure mix and large claims.
- Reserve example: a confidence interval for mean settlement cost should not be confused with a prediction interval for a single future claim.

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

## Master Chapter 3: Aggregate Claims, Poisson Processes, and Risk Capital

Aggregate loss work separates frequency from severity, then recombines them. Pricing needs the expected loss, while solvency and risk loading need spread, skewness, and tail behaviour.

### Topics in this master chapter

- Topic 61: Compound Poisson Aggregate Loss
- Topic 62: Poisson Processes, Arrival Times, and Thinning
- Topic 63: Independent Bernoulli Benefit Payouts and Conditional Claim Amount
- Topic 64: Poisson Process Counts, Waiting Times, and Simulation
- Topic 65: Aggregate Poisson Claim Testing, Type I Error, Power, and Parameter CI
- Topic 66: Bayesian Updating for Coin Probability from Waiting Times Until Heads

### Topic 61: Compound Poisson Aggregate Loss

#### 1. Concept theory

Aggregate loss is total loss over a period:

```text
S = X_1 + X_2 + ... + X_N
```

where:

```text
N = number of claims
X_i = size of claim i
```

If `N` is random, `S` is a compound distribution.

#### 2. Why actuaries care

Insurance pricing depends on total claim cost, not only claim count or claim size separately.

Premium often includes:

```text
expected loss + expense loading + profit margin + risk margin
```

#### 3. Mathematical derivation

Condition on `N`.

Expected value:

```text
E[S | N] = N E[X]
```

Taking expectation:

```text
E[S] = E[E[S | N]]
     = E[N E[X]]
     = E[N] E[X]
```

If `N ~ Poisson(lambda)`:

```text
E[S] = lambda E[X]
```

Variance:

```text
Var(S) = E[Var(S | N)] + Var(E[S | N])
```

Now:

```text
Var(S | N) = N Var(X)
E[S | N] = N E[X]
```

So:

```text
Var(S) = E[N Var(X)] + Var(N E[X])
       = E[N] Var(X) + Var(N) E[X]^2
```

For Poisson:

```text
E[N] = Var(N) = lambda
```

Therefore:

```text
Var(S) = lambda Var(X) + lambda E[X]^2
       = lambda(Var(X) + E[X]^2)
```

#### 4. Simple example

Suppose:

```text
lambda = 2.7
mean severity = 7,350
sd severity = 5,120
```

Expected aggregate loss:

```text
E[S] = 2.7 * 7,350 = 19,845
```

Aggregate variance:

```text
Var(S) = 2.7(5,120^2 + 7,350^2)
```

Aggregate standard deviation:

```text
SD(S) = 14,718.68
```

Premium with 30% standard deviation loading:

```text
Premium = 19,845 + 0.30(14,718.68)
        = 24,260.60
```

#### 5. Exam-style case study

In cyber risk, one company may have:

```text
low frequency but high severity
```

A premium based only on expected loss ignores volatility. A standard deviation loading adds a risk margin.

#### 6. Real-world actuarial case study

For cyber insurance, one ransomware attack can produce legal costs, downtime costs, data recovery costs, and notification costs. Even if expected frequency is low, severity volatility can be high, so capital loading becomes important.

#### 7. Common mistakes

- Using only `lambda Var(X)` for aggregate variance.
- Forgetting the `E[X]^2` term.
- Treating expected loss as a sufficient premium.

#### 8. Revision checkpoint

Memorise:

```text
For compound Poisson:
E[S] = lambda m
Var(S) = lambda(s^2 + m^2)
```

### Expanded deep explanation

Aggregate loss work separates frequency from severity, then recombines them. Pricing needs the expected loss, while solvency and risk loading need spread, skewness, and tail behaviour.

- Aggregate loss modelling combines frequency and severity. Frequency explains how often losses occur; severity explains how large they are when they occur. Their product gives expected loss only when the frequency and severity assumptions are aligned.
- The variance of aggregate loss has two sources: random claim sizes and random claim counts. This is why aggregate variance usually contains both a severity variance term and a squared mean severity term.
- Capital work is more demanding than pricing work. Pricing may focus on expected loss, while capital and reinsurance decisions need standard deviation, percentiles, stress tests, and tail scenarios.

### Step-by-step working method

1. Define the claim count variable and the individual severity variables.
2. Check independence and identical distribution assumptions.
3. Condition on the claim count first.
4. Use total expectation and total variance.
5. Translate the result into premium, reserve, or capital language.

### Extra practical actuarial examples

- Pricing example: expected annual loss equals expected claim count times expected claim size, then expenses, commission, profit margin, and risk loading are added.
- Capital example: two portfolios with the same expected loss can require different capital if one has low-frequency high-severity claims.
- Operational example: for cyber insurance, one ransomware event may produce legal, notification, downtime, and recovery costs, so severity volatility drives risk loading.

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

### Topic 62: Poisson Processes, Arrival Times, and Thinning

#### 1. Concept theory

A Poisson process models events happening randomly over time.

If events arrive at rate `lambda` per unit time:

```text
N(t) ~ Poisson(lambda t)
```

where `N(t)` is the number of events by time `t`.

Important properties:

```text
independent increments
stationary increments
Poisson count distribution over fixed intervals
exponential waiting time between events
```

#### 2. Why actuaries care

Poisson processes are used for:

```text
claim arrivals
hospital admissions
customer complaints
cyber incidents
IT system failures
fraud alerts
```

#### 3. Mathematical derivation

If alerts arrive at rate `lambda`, then:

```text
N(t) ~ Poisson(lambda t)
```

The time of the 4th arrival is more than 1 hour if fewer than 4 arrivals occurred in the first hour:

```text
T_4 > 1  <=>  N(1) <= 3
```

Therefore:

```text
P(T_4 > 1) = P(N(1) <= 3)
           = sum from k=0 to 3 e^(-lambda) lambda^k / k!
```

Thinning:

If each event is independently classified as type B with probability `p`, then type B events form a Poisson process with rate:

```text
lambda_B = lambda p
```

#### 4. Simple example

Security alerts arrive at rate:

```text
lambda = 4 per hour
```

Find probability that the 4th alert arrives after 1 hour:

```text
P(T_4 > 1) = P(N(1) <= 3)
```

Since:

```text
N(1) ~ Poisson(4)
```

```text
P(N(1) <= 3) = e^(-4)(1 + 4 + 4^2/2! + 4^3/3!)
              approximately 0.433
```

#### 5. Exam-style case study

Tickets arrive at rate:

```text
6 per hour
```

45% are medium-priority. Then:

```text
medium-priority rate = 6 * 0.45 = 2.7 per hour
```

In 2 hours:

```text
N_B ~ Poisson(5.4)
```

So:

```text
P(N_B >= 2) = 1 - P(N_B=0) - P(N_B=1)
            = 1 - e^(-5.4) - 5.4e^(-5.4)
            = 1 - 6.4e^(-5.4)
            approximately 0.971
```

#### 6. Real-world actuarial case study

A claims department receives motor claim notifications through the day. If high-severity claims are 10% of all notifications and total notifications follow a Poisson process, high-severity notifications can be modelled as a thinned Poisson process.

This helps allocate specialist claim handlers.

#### 7. Common mistakes

- Confusing arrival time questions with count questions.
- Forgetting to multiply rate by time.
- Forgetting to multiply rate by category probability in thinning problems.

#### 8. Revision checkpoint

Remember:

```text
4th arrival after time t = at most 3 arrivals by time t
```

### Expanded deep explanation

Aggregate loss work separates frequency from severity, then recombines them. Pricing needs the expected loss, while solvency and risk loading need spread, skewness, and tail behaviour.

- Aggregate loss modelling combines frequency and severity. Frequency explains how often losses occur; severity explains how large they are when they occur. Their product gives expected loss only when the frequency and severity assumptions are aligned.
- The variance of aggregate loss has two sources: random claim sizes and random claim counts. This is why aggregate variance usually contains both a severity variance term and a squared mean severity term.
- Capital work is more demanding than pricing work. Pricing may focus on expected loss, while capital and reinsurance decisions need standard deviation, percentiles, stress tests, and tail scenarios.

### Step-by-step working method

1. Define the claim count variable and the individual severity variables.
2. Check independence and identical distribution assumptions.
3. Condition on the claim count first.
4. Use total expectation and total variance.
5. Translate the result into premium, reserve, or capital language.

### Extra practical actuarial examples

- Pricing example: expected annual loss equals expected claim count times expected claim size, then expenses, commission, profit margin, and risk loading are added.
- Capital example: two portfolios with the same expected loss can require different capital if one has low-frequency high-severity claims.
- Operational example: for cyber insurance, one ransomware event may produce legal, notification, downtime, and recovery costs, so severity volatility drives risk loading.

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

### Topic 63: Independent Bernoulli Benefit Payouts and Conditional Claim Amount

#### 1. Concept theory

When a policy can pay several separate fixed benefits, each benefit can be represented by a Bernoulli random variable.

For example:

```text
I_H = 1 if heart claim occurs, 0 otherwise
I_C = 1 if cancer claim occurs, 0 otherwise
I_L = 1 if liver claim occurs, 0 otherwise
```

The total claim amount is a weighted sum of these indicators.

#### 2. Why actuaries care

This is common in health insurance, critical illness, riders, and bundled benefits. A single policy may have multiple independent or dependent benefit triggers.

Actuaries need expected payout, variance, and expected payout conditional on a claim occurring.

#### 3. Mathematical derivation

Let:

```text
Y = aI1 + bI2 + cI3
```

where:

```text
Ii ~ Bernoulli(pi)
```

Then:

```text
E[Ii] = pi
Var(Ii) = pi(1 - pi)
```

So:

```text
E[Y] = a p1 + b p2 + c p3
```

If the indicators are independent:

```text
Var(Y) = a^2 p1(1 - p1) + b^2 p2(1 - p2) + c^2 p3(1 - p3)
```

If the question asks for expected payout given at least one claim:

```text
E[Y | Y > 0] = E[Y] / P(Y > 0)
```

because `Y = 0` when no claim occurs.

For independent risks:

```text
P(Y = 0) = product(1 - pi)
P(Y > 0) = 1 - product(1 - pi)
```

#### 4. Simple example

Suppose benefits are:

```text
10, 20
```

with probabilities:

```text
0.1, 0.2
```

Expected payout:

```text
E[Y] = 10(0.1) + 20(0.2) = 5
```

Probability of at least one claim:

```text
1 - (0.9)(0.8) = 0.28
```

Expected payout given a claim:

```text
5 / 0.28 = 17.86
```

#### 5. Exam-style case study

In the December 2022 health product question:

```text
Heart benefit = 20, p = 0.01
Cancer benefit = 25, p = 0.02
Liver benefit = 15, p = 0.005
```

Mean total payout:

```text
E[Y] = 20(0.01) + 25(0.02) + 15(0.005)
     = 0.775 lakhs
```

Variance:

```text
20^2(0.01)(0.99) + 25^2(0.02)(0.98) + 15^2(0.005)(0.995)
= 3.96 + 12.25 + 1.1194
= 17.3294
```

Standard deviation:

```text
sqrt(17.3294) = 4.16 lakhs approximately
```

Probability of at least one claim:

```text
1 - (0.99)(0.98)(0.995) = 0.03465 approximately
```

Expected payout given at least one claim:

```text
0.775 / 0.03465 = 22.37 lakhs approximately
```

#### 6. Real-world actuarial case study

A critical illness product pays separate fixed amounts for cancer, heart attack, and stroke. The pricing actuary needs the average claim cost per policy, but the claims team wants the average amount paid when a customer actually claims.

These are different quantities. The policy-level mean includes many zero-claim policies. The conditional mean given claim excludes those zeros.

#### 7. Common mistakes

- Confusing expected payout per policy with expected payout given a claim.
- Forgetting that fixed benefits are multiplied by Bernoulli indicators.
- Adding standard deviations instead of variances.
- Ignoring dependence if the question does not say risks are independent.
- Forgetting that coverage can continue for other ailments after one claim.

#### 8. Revision checkpoint

Without notes, you should be able to calculate:

```text
E[Y] = sum benefit_i p_i
Var(Y) = sum benefit_i^2 p_i(1 - p_i)
E[Y | Y > 0] = E[Y] / P(Y > 0)
```

### Expanded deep explanation

Aggregate loss work separates frequency from severity, then recombines them. Pricing needs the expected loss, while solvency and risk loading need spread, skewness, and tail behaviour.

- Begin by checking support. A count model lives on non-negative integers, a severity model often lives on positive real values, and a transformed model may live on the whole real line after taking logs. Many exam errors happen before calculation because the wrong support is chosen.
- Separate the probability law from its moments. The probability law tells you every probability statement; the mean and variance are summaries. In actuarial work a model with the correct mean but the wrong tail can still be dangerous because solvency questions are tail questions.
- Use generating functions when sums are involved. MGFs, CGFs, and PGFs turn convolutions into algebra, but only under the conditions where they exist and where independence assumptions are valid.

### Step-by-step working method

1. State the random variable and its support.
2. Write the probability mass function, density, or distribution function.
3. Identify parameters and their actuarial meaning.
4. Calculate the required moment, probability, percentile, or transform.
5. Check whether the answer is sensible using units, range, and limiting cases.

### Extra practical actuarial examples

- Claim count example: if a motor portfolio has many zero-claim policies and a few multi-claim policies, compare Poisson and negative binomial assumptions by checking whether sample variance is close to, below, or above the sample mean.
- Severity example: for small routine medical claims a lognormal or gamma model may be adequate; for catastrophe-like property claims a heavier tail such as Pareto or Weibull may be needed.
- Exam example: after finding a probability, ask whether the question wanted an unconditional probability or a conditional probability after observing a claim, survival, or selection event.

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

### Topic 64: Poisson Process Counts, Waiting Times, and Simulation

#### 1. Concept theory

A Poisson process models random events occurring over time at a constant average rate.

If the rate is `lambda` per year:

```text
N(t) ~ Poisson(lambda t)
```

The waiting time until the next event is exponential:

```text
T ~ Exponential(rate lambda)
```

#### 2. Why actuaries care

Poisson processes model claim arrivals, hospital admissions, breakdowns, call-centre volumes, and operational losses.

They are useful when claims occur randomly over time and the rate is approximately stable.

#### 3. Mathematical derivation

For one year:

```text
P(N = k) = exp(-lambda) lambda^k / k!
```

Probability of no claims:

```text
P(N = 0) = exp(-lambda)
```

If each year independently has probability:

```text
p = P(at least one claim) = 1 - exp(-lambda)
```

then the number of years with at least one claim in 4 years is:

```text
Binomial(4, p)
```

Waiting time:

```text
P(T > t) = exp(-lambda t)
```

To simulate one Poisson observation using a uniform random number `u`, use cumulative probabilities:

```text
Find smallest n such that P(N <= n) >= u
```

#### 4. Simple example

If:

```text
lambda = 0.3
```

then:

```text
P(no claim in one year) = exp(-0.3) = 0.7408
```

Probability waiting more than 3 years:

```text
P(T > 3) = exp(-0.3 x 3) = exp(-0.9) = 0.4066
```

#### 5. Exam-style case study

In March 2022:

```text
lambda = 0.3 per year
```

For four consecutive years, exactly two years have at least one claim:

```text
p = 1 - exp(-0.3)
P = C(4,2)p^2(1 - p)^2
```

For simulation:

```text
P(N = 0) = exp(-0.3)
P(N <= 1) = P(0) + P(1)
P(N <= 2) = P(0) + P(1) + P(2)
```

Compare the uniform random number with these cumulative probabilities.

#### 6. Real-world actuarial case study

A health insurer simulates the number of hospital claims in a one-year policy. For each policy, the model draws a uniform random number and converts it to a Poisson claim count using cumulative probabilities.

This creates simulated annual claim counts for pricing and capital testing.

#### 7. Common mistakes

- Using `Poisson(lambda)` for four years instead of `Poisson(4lambda)` when modelling total claims over four years.
- Forgetting that "at least one claim in a year" is a Bernoulli event.
- Confusing the count distribution with the waiting-time distribution.
- Simulating by comparing `u` with individual probabilities instead of cumulative probabilities.

#### 8. Revision checkpoint

Without notes, you should be able to write:

```text
N(t) ~ Poisson(lambda t)
P(N = 0) = exp(-lambda t)
T ~ Exponential(lambda)
P(T > t) = exp(-lambda t)
```

and simulate a Poisson count using cumulative probabilities.

### Expanded deep explanation

Aggregate loss work separates frequency from severity, then recombines them. Pricing needs the expected loss, while solvency and risk loading need spread, skewness, and tail behaviour.

- Aggregate loss modelling combines frequency and severity. Frequency explains how often losses occur; severity explains how large they are when they occur. Their product gives expected loss only when the frequency and severity assumptions are aligned.
- The variance of aggregate loss has two sources: random claim sizes and random claim counts. This is why aggregate variance usually contains both a severity variance term and a squared mean severity term.
- Capital work is more demanding than pricing work. Pricing may focus on expected loss, while capital and reinsurance decisions need standard deviation, percentiles, stress tests, and tail scenarios.

### Step-by-step working method

1. Define the claim count variable and the individual severity variables.
2. Check independence and identical distribution assumptions.
3. Condition on the claim count first.
4. Use total expectation and total variance.
5. Translate the result into premium, reserve, or capital language.

### Extra practical actuarial examples

- Pricing example: expected annual loss equals expected claim count times expected claim size, then expenses, commission, profit margin, and risk loading are added.
- Capital example: two portfolios with the same expected loss can require different capital if one has low-frequency high-severity claims.
- Operational example: for cyber insurance, one ransomware event may produce legal, notification, downtime, and recovery costs, so severity volatility drives risk loading.

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

### Topic 65: Aggregate Poisson Claim Testing, Type I Error, Power, and Parameter CI

#### 1. Concept theory

If each policy has claim count:

\[
X_i\sim Poisson(\mu)
\]

and there are \(n\) independent policies, then total claims:

\[
S=\sum_{i=1}^{n}X_i\sim Poisson(n\mu)
\]

This allows a hypothesis about the claim frequency \(\mu\) to be tested using the total number of observed claims.

#### 2. Why actuaries care

This is a core actuarial monitoring problem:

- checking whether claim frequency assumption is still reasonable
- monitoring pricing adequacy
- detecting adverse portfolio experience
- comparing actual claims against expected claims
- setting simple control limits

#### 3. Mathematical derivation

Suppose the null hypothesis is:

\[
H_0:\mu=\mu_0
\]

and the test rejects \(H_0\) if:

\[
S\ge c
\]

Under \(H_0\):

\[
S\sim Poisson(n\mu_0)
\]

Type I error is:

\[
\alpha=P(\text{reject }H_0\mid H_0\text{ true})
\]

\[
=P(S\ge c\mid S\sim Poisson(n\mu_0))
\]

Using normal approximation:

\[
S\approx N(n\mu_0,n\mu_0)
\]

With continuity correction:

\[
P(S\ge c)\approx P\left(Z>\frac{c-0.5-n\mu_0}{\sqrt{n\mu_0}}\right)
\]

Power at actual parameter \(\mu\) is:

\[
P(\text{reject }H_0\mid \mu)
\]

\[
=P(S\ge c\mid S\sim Poisson(n\mu))
\]

For a confidence interval for \(\mu\), use:

\[
\hat\mu=\frac{S}{n}
\]

Approximate standard error:

\[
SE(\hat\mu)=\sqrt{\frac{\hat\mu}{n}}
\]

So an approximate confidence interval is:

\[
\hat\mu\pm z_{\alpha/2}\sqrt{\frac{\hat\mu}{n}}
\]

#### 4. Simple example

Suppose:

\[
n=1000,\quad \mu_0=3
\]

Then:

\[
S\sim Poisson(3000)
\]

Reject if:

\[
S\ge3100
\]

Using normal approximation:

\[
Z=\frac{3099.5-3000}{\sqrt{3000}}
\]

\[
=1.817
\]

So:

\[
\alpha\approx P(Z>1.817)=0.0346
\]

#### 5. Exam-style case study

Claims from 1000 policies are observed for one year. The proposed Poisson parameter is 3. The rule is:

- accept \(\mu=3\) if observed claims are less than 3100
- reject otherwise

Under the null:

\[
S\sim Poisson(1000\times3)=Poisson(3000)
\]

Type I error:

\[
\alpha=P(S\ge3100\mid \mu=3)
\]

Approximation:

\[
\alpha\approx P\left(Z>\frac{3099.5-3000}{\sqrt{3000}}\right)
\]

\[
=P(Z>1.817)
\]

Power at actual \(\mu\):

\[
Power(\mu)=P(S\ge3100\mid S\sim Poisson(1000\mu))
\]

Normal approximation:

\[
Power(\mu)\approx
P\left(Z>\frac{3099.5-1000\mu}{\sqrt{1000\mu}}\right)
\]

If actual total claims are 2900:

\[
\hat\mu=\frac{2900}{1000}=2.9
\]

Approximate 99% confidence interval:

\[
2.9\pm2.576\sqrt{\frac{2.9}{1000}}
\]

#### 6. Real-world actuarial case study

A motor insurer expects 3 claims per 100 policy-years in a small product cell. After observing 1000 exposure units, the total claims are checked against a threshold.

If claims exceed the threshold, the pricing assumption is flagged for review. The power function tells the actuary how likely the test is to detect a true increase in claim frequency.

#### 7. Common mistakes

- Forgetting that the aggregate count is \(Poisson(n\mu)\), not \(Poisson(\mu)\).
- Reversing Type I and Type II errors.
- Forgetting the continuity correction when using the normal approximation.
- Treating power as fixed, when it depends on the actual value of \(\mu\).
- Using total claims \(S\) instead of \(\hat\mu=S/n\) as the parameter estimate.

#### 8. Revision checkpoint

You should be able to:

- aggregate independent Poisson variables
- define Type I error, Type II error, and power
- compute an approximate Type I error using normal approximation
- write the power function in terms of \(\mu\)
- form an approximate confidence interval for a Poisson rate

### Expanded deep explanation

Aggregate loss work separates frequency from severity, then recombines them. Pricing needs the expected loss, while solvency and risk loading need spread, skewness, and tail behaviour.

- Inference is a disciplined way of deciding whether observed experience is consistent with an assumption. A confidence interval estimates a plausible range for a parameter; a hypothesis test checks whether the data are surprising under a stated null hypothesis.
- Type I error is rejecting a true null hypothesis. Type II error is failing to reject a false null hypothesis. Power is the probability of detecting an effect when it is truly present.
- A prediction interval is wider than a confidence interval for a mean because it includes both uncertainty about the average and the randomness of a new observation.

### Step-by-step working method

1. Identify the parameter being estimated or tested.
2. Write the null and alternative hypotheses, if a test is required.
3. Choose the reference distribution and degrees of freedom.
4. Compute the statistic, p-value, interval, or critical region.
5. Finish with a plain-English actuarial conclusion.

### Extra practical actuarial examples

- Fraud model example: a higher observed detection rate is useful only if the improvement is statistically convincing and operationally valuable after false positives are considered.
- Claims inflation example: test whether average claim cost has changed before changing a pricing basis, but also review exposure mix and large claims.
- Reserve example: a confidence interval for mean settlement cost should not be confused with a prediction interval for a single future claim.

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

### Topic 66: Bayesian Updating for Coin Probability from Waiting Times Until Heads

#### 1. Concept theory

When the probability of heads \(p\) is unknown, we can place a prior distribution on \(p\). If the prior is uniform on \([0,1]\), it is the same as:

\[
p\sim Beta(1,1)
\]

Observing waiting times until heads gives information about how likely heads is. More failures before heads push the posterior toward smaller values of \(p\).

#### 2. Why actuaries care

This is a simple model for event probability learning:

- probability of policy renewal after reminders
- probability of claim recovery after repeated attempts
- probability of customer response after calls
- probability of default after repeated missed signals
- probability of fraud detection after checks

The actuarial idea is to update an unknown probability after observing the pattern of failures and successes.

#### 3. Mathematical derivation

Suppose:

\[
p\sim Beta(\alpha,\beta)
\]

The beta density is proportional to:

\[
p^{\alpha-1}(1-p)^{\beta-1}
\]

If the first head occurs on the \(m\)th flip, then there are:

- 1 head
- \(m-1\) tails

The likelihood is:

\[
L(p)\propto p(1-p)^{m-1}
\]

With a uniform prior:

\[
f(p)\propto 1
\]

Posterior:

\[
f(p\mid data)\propto p(1-p)^{m-1}
\]

This is:

\[
p\mid data\sim Beta(2,m)
\]

If after that the second head occurs after a further \(n\) flips, then the total observations contain:

- 2 heads
- \(m+n-2\) tails

So:

\[
L(p)\propto p^2(1-p)^{m+n-2}
\]

With uniform prior:

\[
p\mid data\sim Beta(3,m+n-1)
\]

#### 4. Simple example

Suppose the first head appears on the 4th flip.

Then the data are:

\[
T,T,T,H
\]

So:

\[
p\mid data\sim Beta(2,4)
\]

Posterior mean:

\[
E(p\mid data)=\frac{2}{2+4}=\frac{1}{3}
\]

The estimate is below 0.5 because heads took several flips to appear.

#### 5. Exam-style case study

A coin has unknown probability \(p\) of heads. Initially:

\[
p\sim Uniform(0,1)
\]

You flip until the first head appears on the \(m\)th flip.

Likelihood:

\[
L(p)=p(1-p)^{m-1}
\]

Posterior:

\[
f(p\mid data)\propto p(1-p)^{m-1}
\]

Therefore:

\[
p\mid data\sim Beta(2,m)
\]

If the second head occurs after a further \(n\) flips:

\[
L(p)=p^2(1-p)^{m+n-2}
\]

Posterior:

\[
p\mid data\sim Beta(3,m+n-1)
\]

If the wording says "after \(m\) failures" rather than "on the \(m\)th flip", adjust the number of tails by one. This is why exam solutions may accept alternative beta parameters when the wording is ambiguous.

#### 6. Real-world actuarial case study

An insurer contacts customers to renew a policy. The probability of renewal after each contact is unknown. If the first renewal occurs only after many failed contacts, the posterior distribution for the response probability shifts downward.

After more renewals are observed, the posterior becomes more stable. This helps decide how many reminders are commercially worthwhile.

#### 7. Common mistakes

- Confusing "first head on the \(m\)th flip" with "first head after \(m\) tails".
- Forgetting that the uniform prior is \(Beta(1,1)\).
- Putting heads into the beta second parameter instead of the first.
- Ignoring the failures before the first success.
- Updating from the posterior and also multiplying by all old data again incorrectly.

#### 8. Revision checkpoint

You should be able to:

- convert waiting-time data into numbers of heads and tails
- combine a beta prior with Bernoulli/binomial likelihood
- derive \(Beta(2,m)\) after the first head on the \(m\)th flip
- explain how wording changes the parameter count

### Expanded deep explanation

Aggregate loss work separates frequency from severity, then recombines them. Pricing needs the expected loss, while solvency and risk loading need spread, skewness, and tail behaviour.

- Bayesian analysis combines prior information and data through the likelihood. The posterior distribution is proportional to prior times likelihood.
- Credibility theory has the same actuarial instinct: blend individual experience with collective experience. The more stable and relevant the individual data, the more credibility it receives.
- Conjugate priors are useful because the posterior stays in the same family. This makes updating transparent and exam calculations faster.

### Step-by-step working method

1. Write the prior distribution and identify its parameters.
2. Write the likelihood from the observed data.
3. Multiply prior and likelihood, keeping parameter-dependent terms.
4. Recognise the posterior family or normalise if required.
5. Use the posterior mean, mode, interval, or credibility form for the decision.

### Extra practical actuarial examples

- Renewal example: a beta prior for renewal probability can be updated after observing renewals and non-renewals.
- Claim count example: a gamma prior for a Poisson rate leads to a gamma posterior and a credibility-style blended estimate.
- Pricing example: a small employer group should not be priced only from its own volatile claims experience; credibility blends group experience with portfolio experience.

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

## Master Chapter 4: Data Analysis, Summaries, Correlation, and Dimension Reduction

Data analysis starts before modelling. Check the variable type, missingness, outliers, scale, dependence, and whether the summary statistic actually answers the business question.

### Topics in this master chapter

- Topic 67: Bootstrapping, Correlation, and PCA
- Topic 68: Descriptive Analysis, Central Tendency, and Dispersion
- Topic 69: Two-Sample Confidence Intervals and Correlation Tests
- Topic 70: Regression Slope Test and Its Relationship with Correlation
- Topic 71: Data Types, GLM Components, AIC, and Practical Model Choice
- Topic 72: Rank Correlation, Pearson Correlation, and Correlation Is Not Causation
- Topic 73: Linear Predictor Traps and Poisson Pearson Residuals
- Topic 74: Data Analysis Process and Bayesian Probability Statements
- Topic 75: Correlation, Rebased Indices, and Diversification Interpretation
- Topic 76: Forms of Data Analysis, Big Data Properties, and Discrete Joint Distributions
- Topic 77: Descriptive, Inferential, and Predictive Analysis
- Topic 78: PCA Variance Explained and Factor Analysis Purpose
- Topic 79: Fisher Transformation Test for Pearson Correlation
- Topic 80: Kendall Pair Count, F-Ratio Variance Probability, and Quick Distribution Checks
- Topic 81: Saturated Models, Pearson Residuals, Deviance Residuals, and Scaled Deviance Selection
- Topic 82: Regression Summary Statistics, Correlation, and Prediction Intervals

### Topic 67: Bootstrapping, Correlation, and PCA

#### 1. Concept theory

Bootstrapping resamples observed data to estimate variability without assuming a full parametric distribution.

Correlation measures association.

PCA reduces many correlated variables into fewer uncorrelated components.

#### 2. Why actuaries care

These methods help when:

```text
data is messy
variables are highly correlated
model assumptions are doubtful
dimension is too high
```

#### 3. Mathematical derivation

Pearson correlation:

```text
r = S_xy / sqrt(S_xx S_yy)
```

Spearman rank correlation:

```text
rho = 1 - [6 sum d_i^2] / [n(n^2 - 1)]
```

where:

```text
d_i = difference between ranks
```

PCA:

```text
variance of principal component = eigenvalue
standard deviation of principal component = sqrt(eigenvalue)
```

So:

```text
eigenvalue = standard deviation^2
```

Kaiser's criterion:

```text
keep principal components with eigenvalue > 1
```

when data is standardised.

#### 4. Simple example

If:

```text
PC3 standard deviation = 1.2214
```

then:

```text
PC3 variance = 1.2214^2 = 1.4918
```

Since this is greater than 1, PC3 is retained under Kaiser's criterion.

#### 5. Exam-style case study

If customer spending variables have high correlation, PCA can reduce 10 variables into fewer components while preserving most of the variance.

#### 6. Real-world actuarial case study

In health insurance, variables such as age, number of prescriptions, chronic disease indicators, and hospital visits may be correlated. PCA can create summary health-risk components before modelling claim cost.

#### 7. Common mistakes

- Using PCA on unstandardised variables when scales differ.
- Interpreting principal components without checking loadings.
- Assuming correlation means causation.
- Forgetting that Pearson correlation is sensitive to outliers.

#### 8. Revision checkpoint

You understand this chapter if you can explain why PCA is useful when many predictors are strongly correlated.

### Expanded deep explanation

Data analysis starts before modelling. Check the variable type, missingness, outliers, scale, dependence, and whether the summary statistic actually answers the business question.

- Good actuarial modelling starts with good data understanding. The same numerical value can mean count, amount, rate, duration, category code, or index depending on context.
- Correlation measures association, not causation. A high correlation may disappear after controlling for exposure mix, calendar period, or another confounding variable.
- Dimension reduction methods such as PCA summarise variation, but components must be interpreted carefully because mathematical variance is not the same as business importance.

### Step-by-step working method

1. Classify each variable as categorical, ordinal, count, continuous, or time-based.
2. Check missing data, outliers, exposure, and units.
3. Summarise centre, spread, skewness, and dependence.
4. Visualise before modelling.
5. Document limitations before making assumptions.

### Extra practical actuarial examples

- Portfolio example: average claim cost can rise because severity increased, or because the mix shifted toward higher-risk policies.
- Investment example: correlation between asset returns matters for diversification, but correlations can increase during market stress.
- Health example: large hospital claims can dominate the mean, so median and percentile summaries may tell a different story.

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

### Topic 68: Descriptive Analysis, Central Tendency, and Dispersion

#### 1. Concept theory

Descriptive analysis summarises data so it becomes easier to understand.

It does not mainly try to predict the future or prove a hypothesis. Its job is to describe what is in front of us.

Central tendency tells us where the data is centred:

```text
mean
median
mode
```

Dispersion tells us how spread out the data is:

```text
range
variance
standard deviation
interquartile range
```

#### 2. Why actuaries care

Before building a model, an actuary first asks:

```text
What does the data look like?
Is the average stable?
Are claims highly spread out?
Are there extreme observations?
Are there different groups behaving differently?
```

Descriptive analysis is the first defence against blindly fitting a wrong model.

#### 3. Mathematical derivation

Sample mean:

```text
x_bar = sum x_i / n
```

Sample variance:

```text
s^2 = sum (x_i - x_bar)^2 / (n - 1)
```

The denominator `n - 1` is used for the unbiased sample variance because one degree of freedom is used in estimating the sample mean.

#### 4. Simple example

Claim counts:

```text
0, 0, 1, 1, 3
```

Mean:

```text
(0 + 0 + 1 + 1 + 3) / 5 = 1
```

This tells us the average claim count.

But the values are spread from 0 to 3, so we also need dispersion.

#### 5. Exam-style case study

If the question asks the purpose of descriptive analysis, the best answer is:

```text
to summarize and present data in a simpler, more understandable format
```

If the question asks the difference between central tendency and dispersion:

```text
central tendency = average/centre
dispersion = spread/variability
```

#### 6. Real-world actuarial case study

A reserving actuary first summarises claim payments by accident year. If one year has a much higher average and much wider spread, the actuary investigates before applying a standard reserving method.

#### 7. Common mistakes

- Jumping into modelling before understanding the data.
- Reporting only the mean when the data is skewed.
- Ignoring outliers without understanding whether they are errors or genuine large claims.

#### 8. Revision checkpoint

For any dataset, you should be able to describe:

```text
centre
spread
shape
outliers
```

### Expanded deep explanation

Data analysis starts before modelling. Check the variable type, missingness, outliers, scale, dependence, and whether the summary statistic actually answers the business question.

- Good actuarial modelling starts with good data understanding. The same numerical value can mean count, amount, rate, duration, category code, or index depending on context.
- Correlation measures association, not causation. A high correlation may disappear after controlling for exposure mix, calendar period, or another confounding variable.
- Dimension reduction methods such as PCA summarise variation, but components must be interpreted carefully because mathematical variance is not the same as business importance.

### Step-by-step working method

1. Classify each variable as categorical, ordinal, count, continuous, or time-based.
2. Check missing data, outliers, exposure, and units.
3. Summarise centre, spread, skewness, and dependence.
4. Visualise before modelling.
5. Document limitations before making assumptions.

### Extra practical actuarial examples

- Portfolio example: average claim cost can rise because severity increased, or because the mix shifted toward higher-risk policies.
- Investment example: correlation between asset returns matters for diversification, but correlations can increase during market stress.
- Health example: large hospital claims can dominate the mean, so median and percentile summaries may tell a different story.

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

### Topic 69: Two-Sample Confidence Intervals and Correlation Tests

#### 1. Concept theory

Two-sample confidence intervals compare two population means.

Correlation tests assess whether two variables have a specified true correlation.

#### 2. Why actuaries care

Actuaries compare:

```text
claim cost between regions
lapse rates between channels
screen time between lifestyle groups
relationship between economic factors and claims
asset returns and inflation
```

#### 3. Mathematical derivation

For independent samples with known standard deviations:

```text
(x_bar - y_bar) +/- z * sqrt(sigma_1^2/n_1 + sigma_2^2/n_2)
```

For Pearson correlation:

```text
r = [n sum xy - sum x sum y] /
    sqrt([n sum x^2 - (sum x)^2][n sum y^2 - (sum y)^2])
```

For testing correlation, Fisher transformation is commonly used:

```text
z = 0.5 log((1+r)/(1-r))
```

Approximately:

```text
z_r ~ Normal(z_rho, 1/(n-3))
```

#### 4. Simple example

For two groups:

```text
n_1 = 35, sum x = 205.45, sigma_1 = 1.5
n_2 = 30, sum y = 120.81, sigma_2 = 1
```

Means:

```text
x_bar = 205.45/35 = 5.87
y_bar = 120.81/30 = 4.027
difference = 1.843
```

Standard error:

```text
SE = sqrt(1.5^2/35 + 1^2/30)
   = 0.3124
```

95% confidence interval:

```text
1.843 +/- 1.96(0.3124)
= (1.23, 2.45)
```

#### 5. Exam-style case study

If the interval for `mu_1 - mu_2` is fully positive, the first group has a significantly larger mean at the 5% level.

If the interval includes zero, the observed difference may plausibly be due to sampling variation.

#### 6. Real-world actuarial case study

A health insurer compares average claim cost between policyholders who participate in a wellness programme and those who do not. A two-sample confidence interval helps judge whether the difference is statistically meaningful.

#### 7. Common mistakes

- Forgetting to square standard deviations inside the standard error.
- Reversing the order of the difference.
- Treating correlation as causation.
- Using ordinary normal approximation for correlation when Fisher transformation is expected.

#### 8. Revision checkpoint

For two-sample intervals, always write:

```text
estimate = difference in sample means
SE = square root of sum of variance terms
interval = estimate +/- critical value * SE
```

### Expanded deep explanation

Data analysis starts before modelling. Check the variable type, missingness, outliers, scale, dependence, and whether the summary statistic actually answers the business question.

- Inference is a disciplined way of deciding whether observed experience is consistent with an assumption. A confidence interval estimates a plausible range for a parameter; a hypothesis test checks whether the data are surprising under a stated null hypothesis.
- Type I error is rejecting a true null hypothesis. Type II error is failing to reject a false null hypothesis. Power is the probability of detecting an effect when it is truly present.
- A prediction interval is wider than a confidence interval for a mean because it includes both uncertainty about the average and the randomness of a new observation.

### Step-by-step working method

1. Identify the parameter being estimated or tested.
2. Write the null and alternative hypotheses, if a test is required.
3. Choose the reference distribution and degrees of freedom.
4. Compute the statistic, p-value, interval, or critical region.
5. Finish with a plain-English actuarial conclusion.

### Extra practical actuarial examples

- Fraud model example: a higher observed detection rate is useful only if the improvement is statistically convincing and operationally valuable after false positives are considered.
- Claims inflation example: test whether average claim cost has changed before changing a pricing basis, but also review exposure mix and large claims.
- Reserve example: a confidence interval for mean settlement cost should not be confused with a prediction interval for a single future claim.

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

### Topic 70: Regression Slope Test and Its Relationship with Correlation

#### 1. Concept theory

In simple linear regression, testing whether the slope is zero is equivalent to testing whether the population correlation is zero.

Regression model:

```text
Y = alpha + beta X + error
```

Hypothesis:

```text
H0: beta = 0
H1: beta != 0
```

If `beta = 0`, there is no linear relationship between `X` and `Y` in the model.

#### 2. Why actuaries care

Actuaries often need to decide whether a predictor is useful:

```text
exit-poll seats vs actual seats
age vs claim cost
past claims vs future claims
sum insured vs severity
economic inflation vs claim inflation
```

The slope test tells whether the observed linear relationship is statistically significant.

#### 3. Mathematical derivation

Slope estimate:

```text
beta_hat = S_xy / S_xx
```

Residual variance estimate:

```text
sigma_hat^2 = [S_yy - S_xy^2/S_xx] / (n - 2)
```

Standard error:

```text
se(beta_hat) = sqrt(sigma_hat^2 / S_xx)
```

Test statistic:

```text
t = beta_hat / se(beta_hat)
```

The sample correlation is:

```text
r = S_xy / sqrt(S_xx S_yy)
```

For simple regression, the equivalent test statistic is:

```text
t = r sqrt(n - 2) / sqrt(1 - r^2)
```

#### 4. Simple example

Suppose:

```text
r = 0.80
n = 10
```

Then:

```text
t = 0.80 * sqrt(8) / sqrt(1 - 0.80^2)
  = 0.80 * 2.828 / sqrt(0.36)
  = 2.262 / 0.60
  = 3.77
```

This is the same style of statistic as:

```text
beta_hat / se(beta_hat)
```

#### 5. Exam-style case study

For the election regression question:

```text
Sxx = 92362
Syy = 44844
Sxy = 62350
n = 8
```

Slope:

```text
beta_hat = 62350 / 92362 = 0.6751
```

Residual variance:

```text
sigma_hat^2 = [44844 - 62350^2/92362] / 6
             = 458.9893
```

Standard error:

```text
se(beta_hat) = sqrt(458.9893 / 92362)
              = 0.07049
```

Test statistic:

```text
t = 0.6751 / 0.07049 = 9.58
```

Correlation:

```text
r = 62350 / sqrt(92362 * 44844) = 0.9688
```

Equivalent statistic:

```text
t = r sqrt(n-2) / sqrt(1-r^2)
  = 0.9688 sqrt(6) / sqrt(1 - 0.9688^2)
  = 9.58
```

#### 6. Real-world actuarial case study

A pricing actuary tests whether vehicle age is linearly related to claim frequency. If the slope test is significant, vehicle age may become a rating factor. But the actuary still checks whether the relationship is stable, explainable, and not caused by another hidden variable.

#### 7. Common mistakes

- Using `n-1` instead of `n-2` in the slope test.
- Forgetting the square root in `sqrt(1-r^2)`.
- Thinking a large correlation proves causation.
- Predicting outside the range of observed `X` without warning.

#### 8. Revision checkpoint

You should be able to move between:

```text
beta_hat / se(beta_hat)
```

and:

```text
r sqrt(n-2) / sqrt(1-r^2)
```

for a simple linear regression slope test.

### Expanded deep explanation

Data analysis starts before modelling. Check the variable type, missingness, outliers, scale, dependence, and whether the summary statistic actually answers the business question.

- Regression estimates a conditional mean, not a causal law by default. The coefficient tells how the fitted average response changes when a predictor changes, usually holding other predictors fixed.
- The residuals are as important as the fitted line. Patterns, changing spread, influential points, and non-normal residuals can reveal that the chosen model is too simple or the scale is wrong.
- Prediction requires two layers of uncertainty: uncertainty in the fitted mean and random variation of the individual outcome. This is why individual prediction intervals are wider than mean response intervals.

### Step-by-step working method

1. Plot or inspect the relationship before fitting.
2. Fit the model and write the fitted equation.
3. Interpret each coefficient in business units.
4. Check residuals, leverage, fit statistics, and uncertainty.
5. Decide whether the model is suitable for pricing, reserving, or explanation.

### Extra practical actuarial examples

- Motor pricing example: claim frequency may increase with driver age band, vehicle group, region, and prior claims, but coefficient interpretation must respect all variables included in the model.
- Life insurance example: a transformed mortality rating can produce a smooth premium formula, but the formula must be checked against underwriting judgement and table constraints.
- Exam example: if asked for an individual prediction, include the extra 1 inside the square root of the prediction standard error.

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

### Topic 71: Data Types, GLM Components, AIC, and Practical Model Choice

#### 1. Concept theory

Before modelling, we must understand the data type.

Common exam data types are:

- cross-sectional data: many units at one time
- longitudinal data: same units followed over time
- censored data: exact value is not fully observed, only a bound is known
- truncated data: observations are excluded because they fall outside a range or period

A GLM has three components:

1. random component
2. systematic component
3. link function

#### 2. Why actuaries care

Insurance data may be incomplete, observed over time, grouped by policyholder, or limited by policy terms.

Correctly identifying the data type and model structure helps avoid wrong assumptions in pricing, reserving, capital modelling, and experience analysis.

#### 3. Mathematical derivation

For a GLM:

```text
eta = beta0 + beta1 x1 + beta2 x2 + ...
```

and:

```text
g(mu) = eta
```

where:

```text
mu = E[Y]
g = link function
```

AIC is:

```text
AIC = -2 log-likelihood + 2p
```

where `p` is the number of fitted parameters.

If:

```text
scaled deviance = 2(l_saturated - l_fitted)
```

then:

```text
l_fitted = l_saturated - scaled deviance / 2
```

#### 4. Simple example

Suppose:

```text
scaled deviance = 12
l_saturated = 20
p = 5
```

Then:

```text
l_fitted = 20 - 12 / 2 = 14
AIC = -2(14) + 2(5) = -18
```

Lower AIC is preferred when comparing models.

#### 5. Exam-style case study

A GLM is:

```text
Age + Passes + Experience + Duration + Experience.Duration
```

If `Passes` is represented by 13 binary variables, main-effect parameters are:

```text
Age: 1
Passes: 13
Experience: 1
Duration: 1
Total = 16
```

The interaction:

```text
Experience.Duration
```

has:

```text
1 parameter
```

Always check whether the question asks for covariate parameters only or total parameters including intercept.

#### 6. Real-world actuarial case study

A health insurer models claim cost using age, region, chronic condition status, policy duration, and past claims.

The actuary compares models using AIC. A model with lower AIC is preferred, but the actuary also checks whether the model is stable, explainable, and sensible for pricing.

#### 7. Common mistakes

- Confusing censored data and truncated data.
- Forgetting the link function in GLM theory.
- Counting binary variables incorrectly.
- Forgetting interaction parameters.
- Choosing the model with higher AIC.
- Treating AIC as an absolute measure rather than a comparison tool.

#### 8. Revision checkpoint

Without notes, you should be able to define cross-sectional, longitudinal, censored, and truncated data; list the three GLM components; count simple model parameters; and compute:

```text
AIC = -2l + 2p
```

### Expanded deep explanation

Data analysis starts before modelling. Check the variable type, missingness, outliers, scale, dependence, and whether the summary statistic actually answers the business question.

- A GLM has three parts: a random component for the response distribution, a systematic component for the linear predictor, and a link function connecting the mean to the linear predictor.
- The link function determines coefficient interpretation. With a log link, a one-unit increase multiplies the mean by an exponential factor; with a logit link, a coefficient changes log-odds.
- Deviance compares fitted likelihoods. A smaller deviance can mean better fit, but model selection must balance fit, complexity, interpretability, and out-of-sample stability.

### Step-by-step working method

1. Choose the response distribution based on the data type.
2. Choose the link function and write the linear predictor.
3. Include exposure or offset terms where required.
4. Fit the model and interpret coefficients on the correct scale.
5. Use residuals, deviance, AIC, and validation to judge suitability.

### Extra practical actuarial examples

- Frequency example: Poisson GLM with log exposure offset estimates claim frequency per unit exposure.
- Binary example: a logit model can estimate probability of fraud, lapse, or claim occurrence.
- Severity example: gamma GLM with log link is often used for positive skewed claim amounts.

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

### Topic 72: Rank Correlation, Pearson Correlation, and Correlation Is Not Causation

#### 1. Concept theory

Correlation measures association between two variables.

Pearson correlation measures linear association using numerical values. Spearman and Kendall correlations measure rank association.

Rank correlations are useful when the relationship is monotonic but not necessarily linear, or when data contain outliers.

#### 2. Why actuaries care

Actuaries use correlation in risk aggregation, investment modelling, claim drivers, underwriting factors, and operational analysis.

But correlation alone does not prove that one variable causes another.

#### 3. Mathematical derivation

Pearson correlation:

```text
r = Sxy / sqrt(Sxx Syy)
```

Spearman rank correlation:

```text
rho_s = 1 - [6 sum d_i^2] / [n(n^2 - 1)]
```

where `d_i` is the difference between ranks.

Kendall correlation:

```text
tau = (C - D) / [n(n - 1) / 2]
```

where:

```text
C = number of concordant pairs
D = number of discordant pairs
```

Testing Pearson correlation:

```text
H0: rho = 0
```

Use:

```text
t = r sqrt(n - 2) / sqrt(1 - r^2)
```

with:

```text
n - 2 degrees of freedom
```

#### 4. Simple example

If:

```text
n = 10
r = 0.6
```

then:

```text
t = 0.6 sqrt(8) / sqrt(1 - 0.36)
  = 1.697 / 0.8
  = 2.12
```

Compare this with a `t` critical value with 8 degrees of freedom.

#### 5. Exam-style case study

In the police and crime question, a positive correlation between number of police and number of crimes does not mean that police cause crime.

A more realistic explanation is that more police are deployed in areas with higher crime risk, higher population, or higher reporting rates.

So the conclusion:

```text
more police cause more crimes
```

is not justified by correlation alone.

#### 6. Real-world actuarial case study

An insurer finds that customers with more medical tests have higher claim costs. It would be wrong to conclude that medical tests cause claims.

The likely reason is that sicker customers receive more tests and also have higher claim costs. Health status is a confounding factor.

Actuaries must look for causal structure, confounding variables, and business logic before acting on correlation.

#### 7. Common mistakes

- Treating correlation as causation.
- Using Pearson correlation when only ranks are meaningful.
- Forgetting to rank the data before Spearman correlation.
- Confusing concordant and discordant pairs in Kendall correlation.
- Testing correlation with `n - 1` degrees of freedom instead of `n - 2`.

#### 8. Revision checkpoint

Without notes, you should be able to compute or explain:

```text
Pearson r = Sxy / sqrt(Sxx Syy)
Spearman rho_s = 1 - 6 sum d^2 / [n(n^2 - 1)]
Kendall tau = (C - D) / [n(n - 1)/2]
t = r sqrt(n - 2) / sqrt(1 - r^2)
```

and state why correlation does not prove causation.

### Expanded deep explanation

Data analysis starts before modelling. Check the variable type, missingness, outliers, scale, dependence, and whether the summary statistic actually answers the business question.

- Good actuarial modelling starts with good data understanding. The same numerical value can mean count, amount, rate, duration, category code, or index depending on context.
- Correlation measures association, not causation. A high correlation may disappear after controlling for exposure mix, calendar period, or another confounding variable.
- Dimension reduction methods such as PCA summarise variation, but components must be interpreted carefully because mathematical variance is not the same as business importance.

### Step-by-step working method

1. Classify each variable as categorical, ordinal, count, continuous, or time-based.
2. Check missing data, outliers, exposure, and units.
3. Summarise centre, spread, skewness, and dependence.
4. Visualise before modelling.
5. Document limitations before making assumptions.

### Extra practical actuarial examples

- Portfolio example: average claim cost can rise because severity increased, or because the mix shifted toward higher-risk policies.
- Investment example: correlation between asset returns matters for diversification, but correlations can increase during market stress.
- Health example: large hospital claims can dominate the mean, so median and percentile summaries may tell a different story.

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

### Topic 73: Linear Predictor Traps and Poisson Pearson Residuals

#### 1. Concept theory

A linear predictor must be linear in the parameters, not necessarily linear in the explanatory variables.

For example:

```text
eta = alpha + beta x^2
eta = alpha + beta / x
```

are linear predictors because `alpha` and `beta` enter linearly.

But:

```text
eta = alpha + beta^2 x
```

is not linear in the parameter `beta`.

For Poisson GLMs, Pearson residuals are often used to check model fit:

```text
Pearson residual = (observed - fitted) / sqrt(fitted)
```

#### 2. Why actuaries care

GLMs are widely used in insurance pricing, claim frequency modelling, mortality analysis, and lapse modelling.

Actuaries need to know what counts as a valid linear predictor and how to check whether residuals reveal model problems.

#### 3. Mathematical derivation

In a GLM:

```text
g(mu_i) = eta_i
```

where:

```text
eta_i = beta0 + beta1 z1i + beta2 z2i + ...
```

The transformed variables `z1i`, `z2i` may be functions of covariates:

```text
z1i = x_i^2
z2i = 1 / x_i
```

The predictor is still linear if it is linear in the beta parameters.

For Poisson:

```text
Y_i ~ Poisson(mu_i)
Var(Y_i) = mu_i
```

So the Pearson residual standardises by the model standard deviation:

```text
r_i = (y_i - mu_hat_i) / sqrt(mu_hat_i)
```

#### 4. Simple example

Valid linear predictors:

```text
eta = alpha + beta x^2
eta = alpha + beta(1 / x)
eta = alpha + beta1 x + beta2 x^2
```

Not valid as linear predictors:

```text
eta = alpha + beta^2 x
eta = alpha + exp(beta)x
eta = alpha + beta1 beta2 x
```

For a Poisson fitted value:

```text
y = 5
mu_hat = 3
```

Pearson residual:

```text
(5 - 3) / sqrt(3) = 1.155
```

#### 5. Exam-style case study

In the December 2022 solution, the statement:

```text
Y = alpha + beta^2 X
```

is not a linear predictor because the parameter appears as `beta^2`.

But:

```text
Y = alpha + beta X^2
Y = alpha + beta(1 / X)
```

are linear in the parameters.

For the Poisson GLM residual question, write:

```text
r_i = (y_i - y_hat_i) / sqrt(y_hat_i)
```

and explain that Pearson residual plots can be hard to interpret for Poisson data because residuals are often skewed, especially when fitted means are small.

#### 6. Real-world actuarial case study

A motor insurer models claim frequency using vehicle age. The actuary may include:

```text
vehicle_age
vehicle_age^2
1 / vehicle_age
```

as covariates while keeping the model linear in coefficients.

After fitting the Poisson GLM, the actuary checks Pearson residuals. If residuals show systematic patterns or large skewness, the model may need extra rating factors, overdispersion adjustment, or a negative binomial model.

#### 7. Common mistakes

- Thinking the predictor must be linear in the original covariate.
- Missing that `beta^2` makes the predictor nonlinear in parameters.
- Using normal residual assumptions too casually for Poisson data.
- Forgetting that Poisson variance equals the mean.
- Treating a residual plot as a formal proof of model adequacy.

#### 8. Revision checkpoint

Without notes, you should be able to identify whether a predictor is linear in parameters and write:

```text
Pearson residual = (y_i - mu_hat_i) / sqrt(mu_hat_i)
```

for a Poisson GLM, with one limitation of Pearson residual plots.

### Expanded deep explanation

Data analysis starts before modelling. Check the variable type, missingness, outliers, scale, dependence, and whether the summary statistic actually answers the business question.

- Begin by checking support. A count model lives on non-negative integers, a severity model often lives on positive real values, and a transformed model may live on the whole real line after taking logs. Many exam errors happen before calculation because the wrong support is chosen.
- Separate the probability law from its moments. The probability law tells you every probability statement; the mean and variance are summaries. In actuarial work a model with the correct mean but the wrong tail can still be dangerous because solvency questions are tail questions.
- Use generating functions when sums are involved. MGFs, CGFs, and PGFs turn convolutions into algebra, but only under the conditions where they exist and where independence assumptions are valid.

### Step-by-step working method

1. State the random variable and its support.
2. Write the probability mass function, density, or distribution function.
3. Identify parameters and their actuarial meaning.
4. Calculate the required moment, probability, percentile, or transform.
5. Check whether the answer is sensible using units, range, and limiting cases.

### Extra practical actuarial examples

- Claim count example: if a motor portfolio has many zero-claim policies and a few multi-claim policies, compare Poisson and negative binomial assumptions by checking whether sample variance is close to, below, or above the sample mean.
- Severity example: for small routine medical claims a lognormal or gamma model may be adequate; for catastrophe-like property claims a heavier tail such as Pareto or Weibull may be needed.
- Exam example: after finding a probability, ask whether the question wanted an unconditional probability or a conditional probability after observing a claim, survival, or selection event.

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

### Topic 74: Data Analysis Process and Bayesian Probability Statements

#### 1. Concept theory

A data analysis process is the structured path from a business question to a statistical conclusion.

Typical steps are:

1. define the problem
2. collect relevant data
3. clean and validate the data
4. explore the data
5. choose and fit a model
6. validate assumptions
7. interpret results
8. communicate findings and limitations

Bayesian statistics allows probability statements about unknown parameters. Classical statistics treats parameters as fixed unknown constants, so probability statements are made about estimators or intervals, not about the parameter itself.

#### 2. Why actuaries care

Actuarial work starts with a real business problem: pricing, reserving, capital, mortality, claims, or fraud. Bad data process leads to bad actuarial decisions.

Bayesian thinking is useful when actuaries combine prior expert judgement with observed claims experience.

#### 3. Mathematical derivation

In classical statistics:

```text
mu is fixed but unknown
```

A 95 percent confidence interval means:

```text
Over repeated samples, 95 percent of similarly constructed intervals contain mu.
```

It does not mean:

```text
P(10 < mu < 50) = 0.95
```

In Bayesian statistics:

```text
mu has a prior distribution
data updates the prior to a posterior distribution
```

So a valid Bayesian statement is:

```text
P(10 < mu < 50 | data) = 0.95
```

#### 4. Simple example

If an actuary says:

```text
There is a 95 percent probability that claim frequency lambda lies between 10 and 50.
```

that is Bayesian language.

If the actuary says:

```text
This 95 percent confidence interval was produced by a method that captures lambda 95 percent of the time.
```

that is classical language.

#### 5. Exam-style case study

If the exam asks:

```text
The probability of mu being between 10 and 50 is 95 percent. This is valid in which approach?
```

Answer:

```text
Bayesian statistics only.
```

Reason:

```text
Bayesian statistics treats mu as a random variable with a posterior distribution.
```

#### 6. Real-world actuarial case study

A reinsurer gives a prior view that cancer claim frequency is around 15 per 1,000. The insurer observes Indian portfolio data and updates the estimate.

The posterior distribution can support a statement such as:

```text
There is 95 percent posterior probability that the true claim frequency lies in this range.
```

This is more natural for business decision-making than a purely repeated-sampling interpretation.

#### 7. Common mistakes

- Interpreting a classical confidence interval as a probability statement about the parameter.
- Skipping data validation before modelling.
- Treating model output as final without checking assumptions.
- Forgetting to communicate limitations.

#### 8. Revision checkpoint

Without notes, you should be able to list the data analysis steps and explain why:

```text
P(parameter lies in interval | data)
```

is Bayesian language.

### Expanded deep explanation

Data analysis starts before modelling. Check the variable type, missingness, outliers, scale, dependence, and whether the summary statistic actually answers the business question.

- Bayesian analysis combines prior information and data through the likelihood. The posterior distribution is proportional to prior times likelihood.
- Credibility theory has the same actuarial instinct: blend individual experience with collective experience. The more stable and relevant the individual data, the more credibility it receives.
- Conjugate priors are useful because the posterior stays in the same family. This makes updating transparent and exam calculations faster.

### Step-by-step working method

1. Write the prior distribution and identify its parameters.
2. Write the likelihood from the observed data.
3. Multiply prior and likelihood, keeping parameter-dependent terms.
4. Recognise the posterior family or normalise if required.
5. Use the posterior mean, mode, interval, or credibility form for the decision.

### Extra practical actuarial examples

- Renewal example: a beta prior for renewal probability can be updated after observing renewals and non-renewals.
- Claim count example: a gamma prior for a Poisson rate leads to a gamma posterior and a credibility-style blended estimate.
- Pricing example: a small employer group should not be priced only from its own volatile claims experience; credibility blends group experience with portfolio experience.

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

### Topic 75: Correlation, Rebased Indices, and Diversification Interpretation

#### 1. Concept theory

Correlation measures linear association between two variables. But correlation can be distorted if data from different periods use different index bases or scales.

Before combining index observations across periods, values must be expressed on a common base.

#### 2. Why actuaries care

Investment actuaries use correlations for diversification, capital models, asset allocation, and risk aggregation.

If index levels are not comparable across time or markets, correlation estimates can be misleading.

#### 3. Mathematical derivation

Pearson correlation is:

```text
r = Sxy / sqrt(Sxx Syy)
```

If all values of one variable are multiplied by a positive constant within a consistent data set, the correlation does not change.

But if only one period is rebased and the data are then combined, the relative positions of the points can change. The combined correlation may be very different from the within-period correlations.

#### 4. Simple example

Period 1:

```text
X: 10, 30
Y: 10, 20
```

With two points, correlation is:

```text
r = 1
```

Period 2:

```text
X: 10, 30
Y: 10, 15
```

Again, with two increasing points:

```text
r = 1
```

But after rebasing period 2 and combining all four observations, the overall correlation may be lower or even misleading.

#### 5. Exam-style case study

July 2022 asks whether diversification fails because two index periods each show strong positive correlation.

Method:

1. calculate correlation in each period
2. rebase period 2 to period 1 base
3. calculate combined correlation
4. interpret carefully

Two-point period correlations are not strong evidence. With only two points, any non-flat straight-line movement gives correlation `+1` or `-1`.

#### 6. Real-world actuarial case study

An insurer analyses equity sector indices from two vendors. One vendor changed the index base after a methodology update.

If the actuary combines values without rebasing, the correlation matrix may give false diversification signals. This can distort capital allocation.

#### 7. Common mistakes

- Treating two-point correlation as reliable evidence.
- Combining index data with different bases.
- Assuming high historical correlation proves no diversification.
- Forgetting correlation does not capture all forms of dependence.
- Ignoring economic reasoning about unrelated industries.

#### 8. Revision checkpoint

Without notes, you should be able to explain why index values must be on a common base before combined correlation is calculated, and why two observations are not enough for a reliable diversification conclusion.

### Expanded deep explanation

Data analysis starts before modelling. Check the variable type, missingness, outliers, scale, dependence, and whether the summary statistic actually answers the business question.

- Good actuarial modelling starts with good data understanding. The same numerical value can mean count, amount, rate, duration, category code, or index depending on context.
- Correlation measures association, not causation. A high correlation may disappear after controlling for exposure mix, calendar period, or another confounding variable.
- Dimension reduction methods such as PCA summarise variation, but components must be interpreted carefully because mathematical variance is not the same as business importance.

### Step-by-step working method

1. Classify each variable as categorical, ordinal, count, continuous, or time-based.
2. Check missing data, outliers, exposure, and units.
3. Summarise centre, spread, skewness, and dependence.
4. Visualise before modelling.
5. Document limitations before making assumptions.

### Extra practical actuarial examples

- Portfolio example: average claim cost can rise because severity increased, or because the mix shifted toward higher-risk policies.
- Investment example: correlation between asset returns matters for diversification, but correlations can increase during market stress.
- Health example: large hospital claims can dominate the mean, so median and percentile summaries may tell a different story.

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

### Topic 76: Forms of Data Analysis, Big Data Properties, and Discrete Joint Distributions

#### 1. Concept theory

Data analysis can take several forms:

- descriptive analysis: explains what happened
- diagnostic analysis: explains why it happened
- predictive analysis: estimates what may happen next

Big data is often described using properties such as volume, velocity, variety, and veracity. Homogeneity is not normally a big-data property; big data is often varied and messy.

A discrete joint distribution gives probabilities for pairs of values of two random variables.

#### 2. Why actuaries care

Actuaries use descriptive analysis for past claim experience, diagnostic analysis for claim drivers, and predictive analysis for pricing, reserving, lapse, mortality, and fraud models.

Discrete joint distributions appear in small-count risk tables, benefit combinations, and contingency-style probability questions.

#### 3. Mathematical derivation

For a discrete joint probability function:

```text
p(x,y)
```

the marginal distribution of `Y` is:

```text
pY(y) = sum over all x of p(x,y)
```

The conditional probability is:

```text
P(X = x | Y = y) = p(x,y) / pY(y)
```

provided:

```text
pY(y) > 0
```

#### 4. Simple example

Suppose:

```text
p(0,1) = 0.2
p(1,1) = 0.3
p(2,1) = 0.1
```

Then:

```text
pY(1) = 0.2 + 0.3 + 0.1 = 0.6
```

and:

```text
P(X = 2 | Y = 1) = 0.1 / 0.6 = 0.1667
```

#### 5. Exam-style case study

In September 2021:

```text
f(x,y) = (1/27)(2x + y)
```

where:

```text
x, y = 0, 1, 2
```

To find the marginal distribution of `Y`, sum over all possible `x`:

```text
pY(y) = sum from x = 0 to 2 of (1/27)(2x + y)
```

To calculate:

```text
P(X = 2 | Y = 1)
```

use:

```text
f(2,1) / pY(1)
```

#### 6. Real-world actuarial case study

An insurer studies two categorical policyholder features: number of small claims and number of late premium payments, each grouped as 0, 1, or 2.

A joint distribution helps the actuary understand whether higher late-payment count is associated with more claims.

#### 7. Common mistakes

- Integrating instead of summing for discrete variables.
- Forgetting to include all possible `x` values when finding `pY(y)`.
- Dividing by total probability instead of the marginal probability for the condition.
- Thinking big data must be homogeneous.

#### 8. Revision checkpoint

Without notes, you should be able to explain descriptive, diagnostic, and predictive analysis; identify common big-data properties; and compute:

```text
pY(y) = sum_x p(x,y)
P(X = x | Y = y) = p(x,y) / pY(y)
```

### Expanded deep explanation

Data analysis starts before modelling. Check the variable type, missingness, outliers, scale, dependence, and whether the summary statistic actually answers the business question.

- Good actuarial modelling starts with good data understanding. The same numerical value can mean count, amount, rate, duration, category code, or index depending on context.
- Correlation measures association, not causation. A high correlation may disappear after controlling for exposure mix, calendar period, or another confounding variable.
- Dimension reduction methods such as PCA summarise variation, but components must be interpreted carefully because mathematical variance is not the same as business importance.

### Step-by-step working method

1. Classify each variable as categorical, ordinal, count, continuous, or time-based.
2. Check missing data, outliers, exposure, and units.
3. Summarise centre, spread, skewness, and dependence.
4. Visualise before modelling.
5. Document limitations before making assumptions.

### Extra practical actuarial examples

- Portfolio example: average claim cost can rise because severity increased, or because the mix shifted toward higher-risk policies.
- Investment example: correlation between asset returns matters for diversification, but correlations can increase during market stress.
- Health example: large hospital claims can dominate the mean, so median and percentile summaries may tell a different story.

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

### Topic 77: Descriptive, Inferential, and Predictive Analysis

#### 1. Concept theory

A common exam classification of data analysis is:

- descriptive analysis: summarises what the data show
- inferential analysis: uses sample data to estimate population quantities or test hypotheses
- predictive analysis: uses past data to predict future outcomes

Descriptive analysis stays close to the observed data. Inferential analysis makes statements about a wider population. Predictive analysis focuses on future values.

#### 2. Why actuaries care

Actuaries use all three forms:

- descriptive: claim averages, claim counts, lapse summaries
- inferential: confidence intervals and hypothesis tests
- predictive: pricing models, mortality forecasts, lapse prediction, capital simulations

Knowing the difference helps frame the actuarial question correctly.

#### 3. Mathematical derivation

There is no single formula, but the distinction can be shown as:

```text
Descriptive: calculate xbar and s from observed data
Inferential: use xbar to estimate mu or test H0: mu = mu0
Predictive: use a fitted model to estimate future Y
```

For example:

```text
Xbar = sample mean
CI for mu = Xbar +/- margin of error
Predicted future value = model fitted value
```

#### 4. Simple example

For motor claims:

```text
Descriptive: average claims last month were 120.
Inferential: the true monthly average is estimated between 110 and 130.
Predictive: next month is forecast to have 125 claims.
```

#### 5. Exam-style case study

In September 2021, examples:

```text
Descriptive analysis: mean and standard deviation of daily motor claims.
Inferential analysis: testing whether mall visits are higher on weekends.
Predictive analysis: forecasting future lapses from past lapse data.
```

For big data, "homogeneity within data set" is not a standard big-data property. Big data is often large, fast-arriving, varied, and may have uncertain reliability.

#### 6. Real-world actuarial case study

A health insurer studies COVID hospitalisations.

First, it summarises monthly counts. Then it tests whether older ages have higher claim rates. Finally, it predicts next month's hospitalisation volume for reserving and staffing.

#### 7. Common mistakes

- Calling a forecast descriptive analysis.
- Treating sample summaries as population conclusions without inference.
- Forgetting hypothesis testing is inferential analysis.
- Saying big data must be homogeneous.

#### 8. Revision checkpoint

Without notes, you should be able to distinguish:

```text
descriptive = what happened
inferential = what can we conclude about the population
predictive = what may happen next
```

### Expanded deep explanation

Data analysis starts before modelling. Check the variable type, missingness, outliers, scale, dependence, and whether the summary statistic actually answers the business question.

- Good actuarial modelling starts with good data understanding. The same numerical value can mean count, amount, rate, duration, category code, or index depending on context.
- Correlation measures association, not causation. A high correlation may disappear after controlling for exposure mix, calendar period, or another confounding variable.
- Dimension reduction methods such as PCA summarise variation, but components must be interpreted carefully because mathematical variance is not the same as business importance.

### Step-by-step working method

1. Classify each variable as categorical, ordinal, count, continuous, or time-based.
2. Check missing data, outliers, exposure, and units.
3. Summarise centre, spread, skewness, and dependence.
4. Visualise before modelling.
5. Document limitations before making assumptions.

### Extra practical actuarial examples

- Portfolio example: average claim cost can rise because severity increased, or because the mix shifted toward higher-risk policies.
- Investment example: correlation between asset returns matters for diversification, but correlations can increase during market stress.
- Health example: large hospital claims can dominate the mean, so median and percentile summaries may tell a different story.

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

### Topic 78: PCA Variance Explained and Factor Analysis Purpose

#### 1. Concept theory

Factor analysis and principal component analysis reduce a large set of related variables into fewer underlying components or factors.

Principal components are uncorrelated linear combinations of the original variables. The first component explains the largest possible amount of variance, the second explains the next largest amount, and so on.

#### 2. Why actuaries care

Actuaries use PCA and factor analysis for economic scenarios, yield curves, equity sectors, claim drivers, and large rating-factor sets.

They help simplify complex data without losing too much information.

#### 3. Mathematical derivation

If the covariance matrix of principal components has diagonal entries:

```text
lambda1, lambda2, ..., lambdap
```

then total variance is:

```text
lambda1 + lambda2 + ... + lambdap
```

Percentage explained by component `i`:

```text
lambda_i / total variance x 100%
```

Off-diagonal entries are zero because principal components are uncorrelated.

#### 4. Simple example

Suppose diagonal variances are:

```text
4, 1, 1
```

Total variance:

```text
6
```

Percentages:

```text
PC1 = 4/6 = 66.7%
PC2 = 1/6 = 16.7%
PC3 = 1/6 = 16.7%
```

#### 5. Exam-style case study

In April 2021, diagonal entries are:

```text
0.456, 0.137, 0.080, 0.0165, 0.012
```

Total:

```text
0.7015
```

So:

```text
PC1 percentage = 0.456 / 0.7015 x 100%
PC2 percentage = 0.137 / 0.7015 x 100%
```

and similarly for the other PCs.

Conclusion:

```text
PC1 explains most of the variation; early PCs may capture most information.
```

#### 6. Real-world actuarial case study

An investment actuary has 20 economic variables: interest rates, inflation, equity returns, credit spreads, and currency movements.

PCA can reduce these into a few uncorrelated drivers such as level, slope, spread, and equity-risk components.

#### 7. Common mistakes

- Thinking PCA removes variables automatically. It transforms variables first; the modeller chooses how many components to keep.
- Forgetting to divide by total variance.
- Treating principal components as original variables.
- Forgetting principal components are uncorrelated, not necessarily independent.

#### 8. Revision checkpoint

Without notes, you should be able to compute:

```text
percentage variance explained = component variance / total variance
```

and explain why PCA is useful for dimension reduction.

### Expanded deep explanation

Data analysis starts before modelling. Check the variable type, missingness, outliers, scale, dependence, and whether the summary statistic actually answers the business question.

- Good actuarial modelling starts with good data understanding. The same numerical value can mean count, amount, rate, duration, category code, or index depending on context.
- Correlation measures association, not causation. A high correlation may disappear after controlling for exposure mix, calendar period, or another confounding variable.
- Dimension reduction methods such as PCA summarise variation, but components must be interpreted carefully because mathematical variance is not the same as business importance.

### Step-by-step working method

1. Classify each variable as categorical, ordinal, count, continuous, or time-based.
2. Check missing data, outliers, exposure, and units.
3. Summarise centre, spread, skewness, and dependence.
4. Visualise before modelling.
5. Document limitations before making assumptions.

### Extra practical actuarial examples

- Portfolio example: average claim cost can rise because severity increased, or because the mix shifted toward higher-risk policies.
- Investment example: correlation between asset returns matters for diversification, but correlations can increase during market stress.
- Health example: large hospital claims can dominate the mean, so median and percentile summaries may tell a different story.

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

### Topic 79: Fisher Transformation Test for Pearson Correlation

#### 1. Concept theory

Pearson correlation measures linear association. Fisher's transformation turns a sample correlation into a quantity that is approximately normally distributed.

This is useful for testing whether the population correlation is significantly different from zero.

#### 2. Why actuaries care

Actuaries test correlations between economic variables, claims and risk factors, social behaviour and outcomes, or asset returns.

Correlation tests help decide whether a relationship is statistically meaningful.

#### 3. Mathematical derivation

For sample correlation `r`, Fisher transformation is:

```text
z = 0.5 log[(1 + r)/(1 - r)]
```

If the true correlation is `rho`, then:

```text
z approximately N(0.5 log[(1 + rho)/(1 - rho)], 1/(n - 3))
```

To test:

```text
H0: rho = 0
```

the null transformed value is:

```text
0.5 log(1/1) = 0
```

So test statistic:

```text
Z = z sqrt(n - 3)
```

For negative correlation:

```text
H1: rho < 0
```

reject for sufficiently negative `Z`.

#### 4. Simple example

Suppose:

```text
n = 20
r = -0.5
```

Then:

```text
z = 0.5 log(0.5/1.5)
  = 0.5 log(1/3)
  = -0.5493
```

Test statistic:

```text
Z = -0.5493 sqrt(17)
```

#### 5. Exam-style case study

In April 2021, the data compares social media hours and marks.

The method is:

1. calculate Pearson correlation `r`
2. state hypotheses:

```text
H0: rho = 0
H1: rho < 0
```

3. apply Fisher transformation:

```text
z = 0.5 log[(1+r)/(1-r)]
```

4. compare:

```text
z sqrt(n - 3)
```

with the standard normal critical value.

Assumption: pairs of observations are independent and come from a bivariate normal population, or the sample is large enough for the approximation.

#### 6. Real-world actuarial case study

An insurer investigates whether more app engagement is associated with fewer customer lapses.

The actuary calculates correlation and tests whether the negative relationship is statistically significant before using it as a rating or retention feature.

#### 7. Common mistakes

- Using Fisher transformation formula incorrectly.
- Forgetting `n - 3`.
- Testing the wrong tail.
- Treating correlation as causation.
- Ignoring bivariate normal or approximation assumptions.

#### 8. Revision checkpoint

Without notes, you should be able to write:

```text
z = 0.5 log[(1+r)/(1-r)]
Z = z sqrt(n-3) under H0: rho = 0
```

and set up a one-sided negative correlation test.

### Expanded deep explanation

Data analysis starts before modelling. Check the variable type, missingness, outliers, scale, dependence, and whether the summary statistic actually answers the business question.

- Inference is a disciplined way of deciding whether observed experience is consistent with an assumption. A confidence interval estimates a plausible range for a parameter; a hypothesis test checks whether the data are surprising under a stated null hypothesis.
- Type I error is rejecting a true null hypothesis. Type II error is failing to reject a false null hypothesis. Power is the probability of detecting an effect when it is truly present.
- A prediction interval is wider than a confidence interval for a mean because it includes both uncertainty about the average and the randomness of a new observation.

### Step-by-step working method

1. Identify the parameter being estimated or tested.
2. Write the null and alternative hypotheses, if a test is required.
3. Choose the reference distribution and degrees of freedom.
4. Compute the statistic, p-value, interval, or critical region.
5. Finish with a plain-English actuarial conclusion.

### Extra practical actuarial examples

- Fraud model example: a higher observed detection rate is useful only if the improvement is statistically convincing and operationally valuable after false positives are considered.
- Claims inflation example: test whether average claim cost has changed before changing a pricing basis, but also review exposure mix and large claims.
- Reserve example: a confidence interval for mean settlement cost should not be confused with a prediction interval for a single future claim.

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

### Topic 80: Kendall Pair Count, F-Ratio Variance Probability, and Quick Distribution Checks

#### 1. Concept theory

Some exam questions test whether you can recognise a distribution result quickly rather than perform a long calculation.

Common quick checks:

- Kendall's correlation uses the number of pairs.
- Comparing two independent normal sample variances uses an F distribution.
- A linear combination of independent normal variables is normal.

#### 2. Why actuaries care

Actuaries often need fast checks on volatility, correlation, and approximate distribution behaviour before building a full model.

These tools appear in investment risk, claim variability, and dependency analysis.

#### 3. Mathematical derivation

For Kendall's rank correlation with no ties:

```text
total pairs = n(n - 1) / 2
```

If:

```text
Nc = number of concordant pairs
Nd = number of discordant pairs
```

then:

```text
Nc + Nd = n(n - 1) / 2
```

and:

```text
tau = (Nc - Nd) / [n(n - 1)/2]
```

For comparing variances from independent normal samples:

```text
(S1^2 / sigma1^2) / (S2^2 / sigma2^2) ~ F(n1 - 1, n2 - 1)
```

If population variances are equal:

```text
S1^2 / S2^2 ~ F(n1 - 1, n2 - 1)
```

For independent standard normals:

```text
aX + bY ~ N(0, a^2 + b^2)
```

#### 4. Simple example

If:

```text
Nc = 6
Nd = 4
```

then:

```text
total pairs = 10
```

Solve:

```text
n(n - 1)/2 = 10
```

So:

```text
n = 5
```

Kendall's tau:

```text
(6 - 4) / 10 = 0.2
```

#### 5. Exam-style case study

In April 2021:

```text
Nc = 87
Nd = 123
```

Total pairs:

```text
87 + 123 = 210
```

Solve:

```text
n(n - 1) / 2 = 210
n(n - 1) = 420
```

The positive solution is:

```text
n = 21
```

For:

```text
P(Sx^2 / Sy^2 > 3)
```

with sample sizes 5 and 17 and equal population variances:

```text
Sx^2 / Sy^2 ~ F(4,16)
```

Since 3 is just below the 5 percent point 3.007, the probability is just over 5 percent.

For independent standard normals:

```text
5X - 4Y ~ N(0, 25 + 16) = N(0,41)
```

#### 6. Real-world actuarial case study

An investment actuary compares volatility estimates from two independent funds. The sample variance ratio can be checked against an F distribution.

Separately, rank methods such as Kendall's tau can be useful when investment return relationships are monotonic but not necessarily linear.

#### 7. Common mistakes

- Forgetting total Kendall pairs are `n(n-1)/2`.
- Using `Nc - Nd` as the number of observations.
- Using F degrees of freedom `n1` and `n2` instead of `n1 - 1` and `n2 - 1`.
- Subtracting variances in `5X - 4Y`.
- Forgetting coefficients are squared in variance calculations.

#### 8. Revision checkpoint

Without notes, you should be able to write:

```text
Nc + Nd = n(n - 1)/2
tau = (Nc - Nd)/(Nc + Nd)
S1^2/S2^2 ~ F(n1-1,n2-1) when variances are equal
aX+bY ~ Normal with variance a^2 Var(X)+b^2 Var(Y)
```

### Expanded deep explanation

Data analysis starts before modelling. Check the variable type, missingness, outliers, scale, dependence, and whether the summary statistic actually answers the business question.

- Begin by checking support. A count model lives on non-negative integers, a severity model often lives on positive real values, and a transformed model may live on the whole real line after taking logs. Many exam errors happen before calculation because the wrong support is chosen.
- Separate the probability law from its moments. The probability law tells you every probability statement; the mean and variance are summaries. In actuarial work a model with the correct mean but the wrong tail can still be dangerous because solvency questions are tail questions.
- Use generating functions when sums are involved. MGFs, CGFs, and PGFs turn convolutions into algebra, but only under the conditions where they exist and where independence assumptions are valid.

### Step-by-step working method

1. State the random variable and its support.
2. Write the probability mass function, density, or distribution function.
3. Identify parameters and their actuarial meaning.
4. Calculate the required moment, probability, percentile, or transform.
5. Check whether the answer is sensible using units, range, and limiting cases.

### Extra practical actuarial examples

- Claim count example: if a motor portfolio has many zero-claim policies and a few multi-claim policies, compare Poisson and negative binomial assumptions by checking whether sample variance is close to, below, or above the sample mean.
- Severity example: for small routine medical claims a lognormal or gamma model may be adequate; for catastrophe-like property claims a heavier tail such as Pareto or Weibull may be needed.
- Exam example: after finding a probability, ask whether the question wanted an unconditional probability or a conditional probability after observing a claim, survival, or selection event.

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

### Topic 81: Saturated Models, Pearson Residuals, Deviance Residuals, and Scaled Deviance Selection

#### 1. Concept theory

A saturated model fits the data perfectly by having as many parameters as needed to reproduce every observation.

It is usually not useful for prediction, but it provides a benchmark for deviance.

Pearson and deviance residuals measure how far observations are from fitted values.

#### 2. Why actuaries care

GLMs are used in claim modelling, pricing, reserving, and risk classification. Deviance comparisons help decide whether adding factors such as age, BMI, or interactions improves the model enough.

#### 3. Mathematical derivation

Pearson residual:

```text
r_P = (observed - fitted) / estimated standard deviation
```

For many GLMs:

```text
r_P = (y_i - mu_hat_i) / sqrt[V(mu_hat_i)]
```

Deviance residual:

```text
r_D = sign(y_i - mu_hat_i) sqrt(individual deviance contribution)
```

The deviance compares the fitted model with the saturated model:

```text
D = 2(log L_saturated - log L_fitted)
```

For nested models, reduction in scaled deviance is compared with the extra degrees of freedom used.

#### 4. Simple example

Suppose:

```text
Model A deviance = 900
Model B deviance = 800
```

Adding the factor reduces deviance by:

```text
100
```

If this reduction is large relative to the extra parameters, the new factor is useful.

#### 5. Exam-style case study

In November 2020:

```text
Model A: 1, deviance 900
Model B: Age, deviance 800
Model C: Age + BMI, deviance 770
Model D: Age * BMI, deviance 760
```

Method:

1. compare A to B: age reduces deviance by 100
2. compare B to C: BMI reduces deviance by 30
3. compare C to D: interaction reduces deviance by 10

Then judge each reduction against the extra degrees of freedom. Prefer the simplest model that gives a meaningful improvement.

#### 6. Real-world actuarial case study

A health insurer models claim amounts using age and BMI. Age clearly improves the model. BMI may add extra risk information. But the age-BMI interaction should be included only if it improves fit enough to justify added complexity.

This avoids overfitting and keeps pricing explainable.

#### 7. Common mistakes

- Thinking a saturated model is best because it fits perfectly.
- Forgetting saturated models usually overfit and predict poorly.
- Confusing Pearson residuals with deviance residuals.
- Adding interaction terms without checking the improvement.
- Looking only at deviance reduction without considering degrees of freedom.

#### 8. Revision checkpoint

Without notes, you should be able to explain:

```text
saturated model = perfect fit benchmark
deviance = 2(log L_saturated - log L_fitted)
Pearson residual = standardised observed minus fitted
```

and use scaled deviance reductions to compare nested GLMs.

### Expanded deep explanation

Data analysis starts before modelling. Check the variable type, missingness, outliers, scale, dependence, and whether the summary statistic actually answers the business question.

- A GLM has three parts: a random component for the response distribution, a systematic component for the linear predictor, and a link function connecting the mean to the linear predictor.
- The link function determines coefficient interpretation. With a log link, a one-unit increase multiplies the mean by an exponential factor; with a logit link, a coefficient changes log-odds.
- Deviance compares fitted likelihoods. A smaller deviance can mean better fit, but model selection must balance fit, complexity, interpretability, and out-of-sample stability.

### Step-by-step working method

1. Choose the response distribution based on the data type.
2. Choose the link function and write the linear predictor.
3. Include exposure or offset terms where required.
4. Fit the model and interpret coefficients on the correct scale.
5. Use residuals, deviance, AIC, and validation to judge suitability.

### Extra practical actuarial examples

- Frequency example: Poisson GLM with log exposure offset estimates claim frequency per unit exposure.
- Binary example: a logit model can estimate probability of fraud, lapse, or claim occurrence.
- Severity example: gamma GLM with log link is often used for positive skewed claim amounts.

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

### Topic 82: Regression Summary Statistics, Correlation, and Prediction Intervals

#### 1. Concept theory

Simple linear regression can be fitted from summary statistics without seeing the full dataset.

The key quantities are:

\[
S_{xx},\quad S_{yy},\quad S_{xy}
\]

They allow us to estimate the regression line, test whether the slope is zero, calculate correlation, and build confidence or prediction intervals.

#### 2. Why actuaries care

Actuarial reports often provide summarised experience data rather than raw data. An actuary may need to:

- fit a premium formula
- test whether a rating factor is statistically useful
- estimate mean premium at a rating level
- estimate an individual premium rate
- judge whether a smooth formula is replacing a table too aggressively

#### 3. Mathematical derivation

Given:

\[
\bar x=\frac{\sum x}{n},\quad \bar y=\frac{\sum y}{n}
\]

\[
S_{xx}=\sum x^2-\frac{(\sum x)^2}{n}
\]

\[
S_{yy}=\sum y^2-\frac{(\sum y)^2}{n}
\]

\[
S_{xy}=\sum(x-\bar x)(y-\bar y)
\]

The slope is:

\[
\hat\beta=\frac{S_{xy}}{S_{xx}}
\]

The intercept is:

\[
\hat\alpha=\bar y-\hat\beta\bar x
\]

The residual sum of squares is:

\[
SSE=S_{yy}-\frac{S_{xy}^2}{S_{xx}}
\]

The residual variance estimate is:

\[
\hat\sigma^2=\frac{SSE}{n-2}
\]

The standard error of the slope is:

\[
SE(\hat\beta)=\sqrt{\frac{\hat\sigma^2}{S_{xx}}}
\]

Test statistic for:

\[
H_0:\beta=0
\]

is:

\[
t=\frac{\hat\beta}{SE(\hat\beta)}
\]

The sample correlation coefficient is:

\[
r=\frac{S_{xy}}{\sqrt{S_{xx}S_{yy}}}
\]

#### 4. Simple example

Suppose:

\[
S_{xx}=100,\quad S_{xy}=50,\quad S_{yy}=40,\quad n=10
\]

Then:

\[
\hat\beta=\frac{50}{100}=0.5
\]

\[
SSE=40-\frac{50^2}{100}=15
\]

\[
\hat\sigma^2=\frac{15}{8}=1.875
\]

\[
SE(\hat\beta)=\sqrt{\frac{1.875}{100}}=0.1369
\]

\[
t=\frac{0.5}{0.1369}=3.65
\]

Correlation:

\[
r=\frac{50}{\sqrt{100(40)}}=0.791
\]

#### 5. Exam-style case study

Premium rates are regressed on:

\[
X=(\text{mortality rating})^3
\]

Given:

\[
n=11,\quad \sum x=174.13,\quad \sum y=197.84
\]

\[
\sum x^2=7545.90,\quad \sum y^2=4747.45,\quad S_{xy}=2176.84
\]

Compute:

\[
S_{xx}=7545.90-\frac{174.13^2}{11}=4789.42
\]

\[
S_{yy}=4747.45-\frac{197.84^2}{11}=1189.21
\]

Slope:

\[
\hat\beta=\frac{2176.84}{4789.42}=0.4545
\]

Intercept:

\[
\bar x=\frac{174.13}{11}=15.83,\quad \bar y=\frac{197.84}{11}=17.99
\]

\[
\hat\alpha=\bar y-\hat\beta\bar x\approx10.79
\]

Regression equation:

\[
\hat y=10.79+0.4545x
\]

Residual variance:

\[
\hat\sigma^2=\frac{1}{9}\left(1189.21-\frac{2176.84^2}{4789.42}\right)=22.20
\]

Slope standard error:

\[
SE(\hat\beta)=\sqrt{\frac{22.20}{4789.42}}=0.0681
\]

Test statistic:

\[
t=\frac{0.4545}{0.0681}=6.674
\]

With 9 degrees of freedom, this is larger than the 5% two-sided critical value, so reject:

\[
H_0:\beta=0
\]

Correlation:

\[
r=\frac{2176.84}{\sqrt{4789.42(1189.21)}}=0.91
\]

At \(x_0=25\):

\[
\hat y_0=10.79+0.4545(25)\approx22.15
\]

Mean response variance:

\[
\hat\sigma^2\left[\frac{1}{n}+\frac{(x_0-\bar x)^2}{S_{xx}}\right]
\]

Individual response variance:

\[
\hat\sigma^2\left[1+\frac{1}{n}+\frac{(x_0-\bar x)^2}{S_{xx}}\right]
\]

The individual interval is wider because it includes random variation of a single future observation.

#### 6. Real-world actuarial case study

A pricing team wants to replace a premium table with a formula based on mortality rating. The fitted line has high correlation, but the residual plot shows a pattern.

This means the formula may systematically undercharge at low and high mortality ratings. The actuary should not rely only on correlation; residual diagnostics matter.

#### 7. Common mistakes

- Using \(\sum x^2\) directly as \(S_{xx}\) without subtracting \((\sum x)^2/n\).
- Forgetting \(n-2\) degrees of freedom for residual variance.
- Saying high correlation proves the model is appropriate.
- Confusing confidence interval for mean response with prediction interval for an individual response.
- Forgetting to use transformed \(X\), not raw mortality rating.
- Ignoring residual plots.

#### 8. Revision checkpoint

You should be able to:

- calculate \(S_{xx},S_{yy},S_{xy}\)
- fit \(\hat y=\hat\alpha+\hat\beta x\)
- test whether the slope is zero
- calculate Pearson correlation from summary statistics
- build mean and individual response interval formulas
- comment on a residual pattern even when \(r\) is high

## Master Template for Future Notes

Use this template whenever you send a new question or solution:

```text
Topic:

1. Concept theory
Explain the idea in plain English.

2. Why actuaries care
Explain where this appears in insurance, pensions, finance, health, or risk.

3. Mathematical derivation
Derive the key formula step by step.

4. Simple example
Use small numbers so the idea is easy.

5. Exam-style case study
Use the question-paper style and show the method.

6. Real-world actuarial case study
Explain a practical business situation.

7. Common mistakes
List traps and wrong assumptions.

8. Revision checkpoint
Write what I should be able to do without notes.
```

### Expanded deep explanation

Data analysis starts before modelling. Check the variable type, missingness, outliers, scale, dependence, and whether the summary statistic actually answers the business question.

- Regression estimates a conditional mean, not a causal law by default. The coefficient tells how the fitted average response changes when a predictor changes, usually holding other predictors fixed.
- The residuals are as important as the fitted line. Patterns, changing spread, influential points, and non-normal residuals can reveal that the chosen model is too simple or the scale is wrong.
- Prediction requires two layers of uncertainty: uncertainty in the fitted mean and random variation of the individual outcome. This is why individual prediction intervals are wider than mean response intervals.

### Step-by-step working method

1. Plot or inspect the relationship before fitting.
2. Fit the model and write the fitted equation.
3. Interpret each coefficient in business units.
4. Check residuals, leverage, fit statistics, and uncertainty.
5. Decide whether the model is suitable for pricing, reserving, or explanation.

### Extra practical actuarial examples

- Motor pricing example: claim frequency may increase with driver age band, vehicle group, region, and prior claims, but coefficient interpretation must respect all variables included in the model.
- Life insurance example: a transformed mortality rating can produce a smooth premium formula, but the formula must be checked against underwriting judgement and table constraints.
- Exam example: if asked for an individual prediction, include the extra 1 inside the square root of the prediction standard error.

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

## Master Chapter 5: Estimation, Likelihood, Bias, and Simulation

Estimation is the bridge between data and assumptions. You choose an estimator, understand its sampling behaviour, and check whether the method is stable for the data available.

### Topics in this master chapter

- Topic 83: Method of Moments, MLE, Bias, MSE, and CRLB
- Topic 84: Poisson GLM Log-Likelihood and Scaled Deviance
- Topic 85: Sampling Distribution of the Sample Mean and Sample Variance
- Topic 86: Poisson Regression with Exposure: MOM, MLE, and LSE
- Topic 87: Biased Sample Variance with Denominator n
- Topic 88: Two-Sample Variance and Mean Tests
- Topic 89: Implicit MLE Equations and Linear Interpolation

### Topic 83: Method of Moments, MLE, Bias, MSE, and CRLB

#### 1. Concept theory

Estimation means using sample data to estimate unknown parameters.

Common estimators:

```text
Method of Moments Estimator (MME)
Maximum Likelihood Estimator (MLE)
Bayesian estimator
```

Bias measures whether the estimator is correct on average:

```text
Bias(theta_hat) = E[theta_hat] - theta
```

Mean square error combines variance and bias:

```text
MSE(theta_hat) = Var(theta_hat) + Bias(theta_hat)^2
```

The Cramer-Rao lower bound gives a lower bound for the variance of unbiased estimators.

#### 2. Why actuaries care

Actuaries estimate:

```text
claim frequency
claim severity
lapse rates
mortality rates
fraud probabilities
expense inflation
```

The quality of these estimates affects premiums, reserves, and capital.

#### 3. Mathematical derivation

Method of moments:

```text
sample moment = population moment
```

If:

```text
E[X] = g(theta)
```

then:

```text
x_bar = g(theta_hat)
```

and solve for `theta_hat`.

Maximum likelihood:

```text
L(theta) = probability/density of observed data as a function of theta
```

MLE:

```text
theta_hat = value of theta that maximises L(theta)
```

CRLB:

```text
Var(theta_hat) >= 1 / I(theta)
```

where Fisher information is:

```text
I(theta) = -E[second derivative of log likelihood]
```

#### 4. Simple example

Suppose a Gamma distribution has:

```text
alpha = 9
lambda = 3/theta
```

using rate parameterisation:

```text
E[X] = alpha / lambda = 9 / (3/theta) = 3theta
```

Method of moments:

```text
x_bar = 3theta_hat
theta_hat = x_bar / 3
```

If:

```text
Var(X) = alpha / lambda^2 = 9 / (9/theta^2) = theta^2
```

then:

```text
Var(x_bar) = theta^2 / n
Var(theta_hat) = Var(x_bar / 3) = theta^2 / (9n)
```

Since `E[theta_hat] = theta`, bias is zero and:

```text
MSE(theta_hat) = theta^2 / (9n)
```

#### 5. Exam-style case study

For the accident-count distribution:

```text
P(0 accidents) = 4q
P(1 accident)  = 2q
P(2 accidents) = q
P(>2 accidents)= 1 - 7q
```

The probability of at most two accidents is:

```text
7q
```

If `X` out of `n` cars have at most two accidents:

```text
X ~ Binomial(n, 7q)
```

MLE:

```text
7q_hat = X/n
q_hat = X/(7n)
```

For CRLB:

```text
I(q) = 7n / [q(1 - 7q)]
CRLB = q(1 - 7q) / (7n)
```

#### 6. Real-world actuarial case study

A motor insurer estimates the probability that a car has at most two claims in a year. If the insurer has grouped data, it may not know each exact claim count above 2, but it can still estimate the grouped probability parameter.

#### 7. Common mistakes

- Mixing rate and scale parameterisation in Gamma distributions.
- Forgetting that MSE includes both variance and bias.
- Maximising likelihood without checking the parameter range.

#### 8. Revision checkpoint

For every estimator, ask:

```text
Is it unbiased?
What is its variance?
What is its MSE?
Does it achieve or approach the CRLB?
```

### Expanded deep explanation

Estimation is the bridge between data and assumptions. You choose an estimator, understand its sampling behaviour, and check whether the method is stable for the data available.

- An estimator is a rule for turning data into a parameter estimate. Different estimators can have different bias, variance, mean squared error, and robustness.
- Maximum likelihood chooses the parameter that makes the observed data most probable under the model. Method of moments matches sample moments to theoretical moments.
- Large-sample theory can give approximate intervals, but boundary estimates, censored observations, small samples, and heavy tails can make approximations unreliable.

### Step-by-step working method

1. Write the model and parameter space.
2. Choose MOM, MLE, least squares, or another estimator.
3. Derive or solve the estimating equation.
4. Check bias, variance, consistency, and practical stability where possible.
5. Interpret the estimate in actuarial units.

### Extra practical actuarial examples

- Severity example: estimating a Pareto tail parameter from a few large losses may be highly unstable, so sensitivity testing is essential.
- Frequency example: the Poisson MLE for a constant rate is total claims divided by total exposure.
- Simulation example: inverse transform simulation uses uniform random numbers to generate losses from a fitted distribution for stress testing.

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

### Topic 84: Poisson GLM Log-Likelihood and Scaled Deviance

#### 1. Concept theory

For a Poisson GLM, the response is a count:

```text
Y_i ~ Poisson(mu_i)
```

The canonical link is:

```text
log(mu_i) = eta_i
```

The log-likelihood measures how well the fitted model explains the observed counts.

Deviance compares the fitted model to a saturated model, which fits the data perfectly.

#### 2. Why actuaries care

Poisson GLMs are widely used for frequency modelling:

```text
number of claims
number of lapses
number of complaints
number of accidents
number of compatibility matches or website events
```

Deviance helps actuaries assess model fit.

#### 3. Mathematical derivation

For one Poisson observation:

```text
P(Y_i = y_i) = exp(-mu_i) mu_i^y_i / y_i!
```

Log-likelihood:

```text
log L_i = -mu_i + y_i log(mu_i) - log(y_i!)
```

For all observations:

```text
log L = sum [y_i log(mu_i) - mu_i - log(y_i!)]
```

For the fitted model:

```text
mu_i = y_hat_i
```

For the saturated model:

```text
mu_i = y_i
```

Poisson deviance:

```text
D = 2 sum [y_i log(y_i / y_hat_i) - (y_i - y_hat_i)]
```

with the convention:

```text
0 log(0/y_hat_i) = 0
```

#### 4. Simple example

Suppose:

```text
y = 3
y_hat = 2
```

Contribution to deviance:

```text
2[3 log(3/2) - (3-2)]
= 2[3 log(1.5) - 1]
= 2[1.216 - 1]
= 0.432
```

Small contribution means the fitted value is close to the observed value.

#### 5. Exam-style case study

For a Poisson GLM compatibility-score model:

```text
log L = sum [y_i log(y_hat_i) - y_hat_i - log(y_i!)]
```

The saturated model uses:

```text
y_hat_i = y_i
```

So scaled deviance is:

```text
D = 2(log L_saturated - log L_model)
```

or equivalently:

```text
D = 2 sum [y_i log(y_i/y_hat_i) - y_i + y_hat_i]
```

#### 6. Real-world actuarial case study

A motor insurer fits a Poisson GLM to claim counts. If deviance is much larger than expected for the degrees of freedom, the model may not fit well. This could indicate missing rating factors, overdispersion, or clustering of claims.

#### 7. Common mistakes

- Forgetting the `-log(y_i!)` term in the log-likelihood.
- Confusing fitted model likelihood with saturated model likelihood.
- Using Normal residual ideas directly for Poisson count data.
- Forgetting that lower deviance generally indicates better fit.

#### 8. Revision checkpoint

You should be able to write:

```text
Poisson log-likelihood
Poisson deviance
canonical link log(mu)
```

and explain why deviance compares fitted and saturated models.

### Expanded deep explanation

Estimation is the bridge between data and assumptions. You choose an estimator, understand its sampling behaviour, and check whether the method is stable for the data available.

- A GLM has three parts: a random component for the response distribution, a systematic component for the linear predictor, and a link function connecting the mean to the linear predictor.
- The link function determines coefficient interpretation. With a log link, a one-unit increase multiplies the mean by an exponential factor; with a logit link, a coefficient changes log-odds.
- Deviance compares fitted likelihoods. A smaller deviance can mean better fit, but model selection must balance fit, complexity, interpretability, and out-of-sample stability.

### Step-by-step working method

1. Choose the response distribution based on the data type.
2. Choose the link function and write the linear predictor.
3. Include exposure or offset terms where required.
4. Fit the model and interpret coefficients on the correct scale.
5. Use residuals, deviance, AIC, and validation to judge suitability.

### Extra practical actuarial examples

- Frequency example: Poisson GLM with log exposure offset estimates claim frequency per unit exposure.
- Binary example: a logit model can estimate probability of fraud, lapse, or claim occurrence.
- Severity example: gamma GLM with log link is often used for positive skewed claim amounts.

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

### Topic 85: Sampling Distribution of the Sample Mean and Sample Variance

#### 1. Concept theory

If we repeatedly take samples from the same normal population, the sample mean and sample variance are themselves random variables.

The sample mean measures the centre of the sample. The sample variance measures the spread of the sample. In a normal sample, these two statistics have very special distributions:

- the sample mean is normally distributed
- the scaled sample variance follows a chi-square distribution
- the sample mean and sample variance are independent

This is one of the most important results behind confidence intervals, variance testing, and t-tests.

#### 2. Why actuaries care

Actuaries often estimate claim severity, mortality rates, expense amounts, or investment returns from samples. We need to know not only the estimate, but also how uncertain the estimate is.

For example, if an insurer estimates average claim size from 100 claims, the sample mean tells the estimated average. The sampling distribution tells how much that estimate might vary if another 100 claims were observed.

#### 3. Mathematical derivation

Let

```text
X1, X2, ..., Xn are independent N(mu, sigma^2)
```

The sample mean is:

```text
Xbar = (X1 + X2 + ... + Xn) / n
```

Since a linear combination of independent normal variables is normal:

```text
Xbar ~ N(mu, sigma^2 / n)
```

So:

```text
Z = (Xbar - mu) / (sigma / sqrt(n)) ~ N(0,1)
```

The sample variance is:

```text
S^2 = sum(Xi - Xbar)^2 / (n - 1)
```

For a normal sample:

```text
(n - 1)S^2 / sigma^2 ~ chi-square(n - 1)
```

The chi-square distribution has:

```text
E[chi-square_v] = v
Var[chi-square_v] = 2v
```

Therefore:

```text
E[(n - 1)S^2 / sigma^2] = n - 1
```

So:

```text
E[S^2] = sigma^2
```

This means `S^2` is an unbiased estimator of `sigma^2`.

Also:

```text
Var((n - 1)S^2 / sigma^2) = 2(n - 1)
```

So:

```text
Var(S^2) = 2 sigma^4 / (n - 1)
```

For normal samples:

```text
Xbar and S^2 are independent
```

This independence is a special normal-distribution result.

#### 4. Simple example

Suppose individual claim sizes are approximately:

```text
X ~ N(20, 5^2)
```

Take a sample of:

```text
n = 25
```

Then:

```text
Xbar ~ N(20, 25 / 25)
Xbar ~ N(20, 1)
```

So the standard error is:

```text
SE(Xbar) = 1
```

Probability that the sample mean is less than 18:

```text
P(Xbar < 18) = P(Z < (18 - 20) / 1)
             = P(Z < -2)
             = 0.0228
```

Only about 2.28 percent of samples of size 25 would have an average below 18.

#### 5. Exam-style case study

An insurer believes claim amounts follow `N(mu, sigma^2)`. A sample of 16 claims is taken.

Find the distributions of:

```text
Xbar
S^2
```

Method:

```text
Xbar ~ N(mu, sigma^2 / 16)
```

and:

```text
15S^2 / sigma^2 ~ chi-square(15)
```

If the question asks whether `Xbar` and `S^2` are independent, answer:

```text
Yes, because the original sample is normal.
```

If the original data were not normal, this independence would generally not be guaranteed.

#### 6. Real-world actuarial case study

A health insurer estimates the average cost of a minor surgery from a sample of 64 claims. The sample average is useful, but management also needs to know whether the estimate is stable.

If the claim amounts are roughly normal, the actuary can use the sampling distribution of the mean to build a confidence interval. If the insurer also needs to test whether volatility has changed, the actuary can use the chi-square distribution of the sample variance.

This helps decide whether pricing assumptions are reliable or whether more claim data is needed.

#### 7. Common mistakes

- Using `sigma` instead of `sigma / sqrt(n)` for the standard error of the sample mean.
- Forgetting the degrees of freedom are `n - 1`, not `n`.
- Assuming `Xbar` and `S^2` are always independent. This is a normal-sample result.
- Confusing the population variance `sigma^2` with the sample variance `S^2`.
- Using a normal distribution for `S^2`; the scaled sample variance follows chi-square.

#### 8. Revision checkpoint

Without notes, you should be able to write:

```text
Xbar ~ N(mu, sigma^2 / n)
(n - 1)S^2 / sigma^2 ~ chi-square(n - 1)
E[S^2] = sigma^2
Var(S^2) = 2sigma^4 / (n - 1)
```

and explain why these results matter for confidence intervals and variance testing.

### Expanded deep explanation

Estimation is the bridge between data and assumptions. You choose an estimator, understand its sampling behaviour, and check whether the method is stable for the data available.

- Begin by checking support. A count model lives on non-negative integers, a severity model often lives on positive real values, and a transformed model may live on the whole real line after taking logs. Many exam errors happen before calculation because the wrong support is chosen.
- Separate the probability law from its moments. The probability law tells you every probability statement; the mean and variance are summaries. In actuarial work a model with the correct mean but the wrong tail can still be dangerous because solvency questions are tail questions.
- Use generating functions when sums are involved. MGFs, CGFs, and PGFs turn convolutions into algebra, but only under the conditions where they exist and where independence assumptions are valid.

### Step-by-step working method

1. State the random variable and its support.
2. Write the probability mass function, density, or distribution function.
3. Identify parameters and their actuarial meaning.
4. Calculate the required moment, probability, percentile, or transform.
5. Check whether the answer is sensible using units, range, and limiting cases.

### Extra practical actuarial examples

- Claim count example: if a motor portfolio has many zero-claim policies and a few multi-claim policies, compare Poisson and negative binomial assumptions by checking whether sample variance is close to, below, or above the sample mean.
- Severity example: for small routine medical claims a lognormal or gamma model may be adequate; for catastrophe-like property claims a heavier tail such as Pareto or Weibull may be needed.
- Exam example: after finding a probability, ask whether the question wanted an unconditional probability or a conditional probability after observing a claim, survival, or selection event.

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

### Topic 86: Poisson Regression with Exposure: MOM, MLE, and LSE

#### 1. Concept theory

When claim counts depend on exposure, the expected number of claims should increase with the amount of exposure.

For example, a policy exposed for 12 months should normally be expected to produce more claims than a similar policy exposed for 1 month.

A simple exposure-based Poisson model is:

```text
Yi ~ Poisson(lambda xi)
```

where:

```text
Yi = claim count
xi = exposure
lambda = claim rate per unit exposure
```

#### 2. Why actuaries care

This model is central to insurance frequency modelling. It appears in motor insurance, health insurance, general insurance pricing, mortality exposure analysis, and operational risk.

The key actuarial idea is that we should compare claims after adjusting for exposure. Ten claims from 10,000 policy-years is very different from ten claims from 100 policy-years.

#### 3. Mathematical derivation

Assume independent observations:

```text
Yi ~ Poisson(lambda xi)
```

Then:

```text
E[Yi] = lambda xi
Var(Yi) = lambda xi
```

#### Method of moments

Use:

```text
observed total claims = expected total claims
```

So:

```text
sum yi = sum lambda xi
sum yi = lambda sum xi
```

Therefore:

```text
lambda_hat_MOM = sum yi / sum xi
```

#### Maximum likelihood

The likelihood is:

```text
L(lambda) = product [exp(-lambda xi)(lambda xi)^yi / yi!]
```

So:

```text
L(lambda) = exp(-lambda sum xi) lambda^(sum yi) product(xi^yi) / product(yi!)
```

The log-likelihood, ignoring constants not involving `lambda`, is:

```text
l(lambda) = -lambda sum xi + (sum yi)log(lambda)
```

Differentiate:

```text
dl/dlambda = -sum xi + (sum yi)/lambda
```

Set equal to zero:

```text
lambda_hat_MLE = sum yi / sum xi
```

So in this model:

```text
MOM estimate = MLE
```

#### Least squares through the origin

If we fit:

```text
yi approximately lambda xi
```

by minimising:

```text
sum(yi - lambda xi)^2
```

Differentiate:

```text
-2 sum xi(yi - lambda xi) = 0
```

So:

```text
lambda_hat_LSE = sum xi yi / sum xi^2
```

This is generally not the same as the MLE.

#### 4. Simple example

Suppose:

```text
Exposure x: 1, 2, 3
Claims y:   0, 1, 2
```

MOM and MLE:

```text
lambda_hat = sum y / sum x
           = 3 / 6
           = 0.5
```

LSE:

```text
lambda_hat = sum xy / sum x^2
           = (1)(0) + (2)(1) + (3)(2) / (1^2 + 2^2 + 3^2)
           = 8 / 14
           = 0.5714
```

The estimates differ because least squares gives more weight to observations with larger `x`.

#### 5. Exam-style case study

A question gives policy exposures and claim counts and asks for method of moments and maximum likelihood estimates.

Use:

```text
Yi ~ Poisson(lambda xi)
```

Then immediately write:

```text
E[Yi] = lambda xi
sum yi = lambda sum xi
lambda_hat = sum yi / sum xi
```

For MLE, write the likelihood:

```text
L(lambda) = exp(-lambda sum xi) lambda^(sum yi) product(xi^yi) / product(yi!)
```

Then:

```text
l(lambda) = -lambda sum xi + (sum yi)log(lambda) + constant
```

and obtain:

```text
lambda_hat = sum yi / sum xi
```

If least squares is requested, use:

```text
lambda_hat_LSE = sum xi yi / sum xi^2
```

#### 6. Real-world actuarial case study

A motor insurer wants to estimate claim frequency per vehicle-year. One customer was insured for a full year, another for six months, and another for only two months.

The actuary should not simply average claim counts across policies. Instead, the actuary divides total claims by total exposure:

```text
claim frequency = total claims / total vehicle-years
```

This gives a fair exposure-adjusted claim rate for pricing and reserving.

#### 7. Common mistakes

- Treating all policies as if they have the same exposure.
- Using `mean(y)` instead of `sum y / sum x`.
- Forgetting that the Poisson mean is `lambda xi`, not just `lambda`.
- Assuming the LSE must equal the MLE.
- Omitting the factorial terms in the likelihood when the full likelihood is requested.

#### 8. Revision checkpoint

Without notes, you should be able to derive:

```text
lambda_hat_MOM = sum y / sum x
lambda_hat_MLE = sum y / sum x
lambda_hat_LSE = sum xy / sum x^2
```

for the model:

```text
Y ~ Poisson(lambda x)
```

and explain why exposure adjustment is essential in insurance.

### Expanded deep explanation

Estimation is the bridge between data and assumptions. You choose an estimator, understand its sampling behaviour, and check whether the method is stable for the data available.

- Regression estimates a conditional mean, not a causal law by default. The coefficient tells how the fitted average response changes when a predictor changes, usually holding other predictors fixed.
- The residuals are as important as the fitted line. Patterns, changing spread, influential points, and non-normal residuals can reveal that the chosen model is too simple or the scale is wrong.
- Prediction requires two layers of uncertainty: uncertainty in the fitted mean and random variation of the individual outcome. This is why individual prediction intervals are wider than mean response intervals.

### Step-by-step working method

1. Plot or inspect the relationship before fitting.
2. Fit the model and write the fitted equation.
3. Interpret each coefficient in business units.
4. Check residuals, leverage, fit statistics, and uncertainty.
5. Decide whether the model is suitable for pricing, reserving, or explanation.

### Extra practical actuarial examples

- Motor pricing example: claim frequency may increase with driver age band, vehicle group, region, and prior claims, but coefficient interpretation must respect all variables included in the model.
- Life insurance example: a transformed mortality rating can produce a smooth premium formula, but the formula must be checked against underwriting judgement and table constraints.
- Exam example: if asked for an individual prediction, include the extra 1 inside the square root of the prediction standard error.

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

### Topic 87: Biased Sample Variance with Denominator n

#### 1. Concept theory

There are two common sample variance formulae:

```text
S^2 = sum(Xi - Xbar)^2 / (n - 1)
S_n'^2 = sum(Xi - Xbar)^2 / n
```

The first one is unbiased for the population variance. The second one is biased downward because it divides by `n` instead of `n - 1`.

#### 2. Why actuaries care

Actuaries estimate volatility in claims, mortality rates, asset returns, expenses, and risk factors. A downward-biased variance can make risk look smaller than it really is.

This matters for capital, pricing margins, and model validation.

#### 3. Mathematical derivation

For a random sample:

```text
E[sum(Xi - Xbar)^2] = (n - 1)sigma^2
```

So:

```text
E[S^2] = E[sum(Xi - Xbar)^2 / (n - 1)] = sigma^2
```

But:

```text
E[S_n'^2] = E[sum(Xi - Xbar)^2 / n]
          = (n - 1)sigma^2 / n
```

Therefore:

```text
Bias(S_n'^2) = E[S_n'^2] - sigma^2
             = [(n - 1)/n]sigma^2 - sigma^2
             = -sigma^2 / n
```

#### 4. Simple example

If:

```text
n = 5
sigma^2 = 20
```

then:

```text
Bias = -20 / 5 = -4
```

The estimator using denominator `n` underestimates the true variance by 4 on average.

#### 5. Exam-style case study

In the December 2022 style question:

```text
n = 13
sigma^2 = 3.4224
```

Bias:

```text
Bias = -sigma^2 / n
     = -3.4224 / 13
     = -0.2633 approximately
```

The given sample mean is not needed for this bias calculation.

#### 6. Real-world actuarial case study

An investment actuary estimates annual return volatility using 13 years of data. If the actuary uses denominator `n`, the variance estimate is biased downward.

For small samples, this can materially understate investment risk and capital requirements.

#### 7. Common mistakes

- Using `n - 1` when the question specifically defines the estimator with denominator `n`.
- Thinking the sample mean is needed for the theoretical bias.
- Saying the bias is positive.
- Forgetting bias means `E[estimator] - parameter`.

#### 8. Revision checkpoint

Without notes, you should be able to write:

```text
E[S_n'^2] = ((n - 1)/n)sigma^2
Bias(S_n'^2) = -sigma^2 / n
```

### Expanded deep explanation

Estimation is the bridge between data and assumptions. You choose an estimator, understand its sampling behaviour, and check whether the method is stable for the data available.

- An estimator is a rule for turning data into a parameter estimate. Different estimators can have different bias, variance, mean squared error, and robustness.
- Maximum likelihood chooses the parameter that makes the observed data most probable under the model. Method of moments matches sample moments to theoretical moments.
- Large-sample theory can give approximate intervals, but boundary estimates, censored observations, small samples, and heavy tails can make approximations unreliable.

### Step-by-step working method

1. Write the model and parameter space.
2. Choose MOM, MLE, least squares, or another estimator.
3. Derive or solve the estimating equation.
4. Check bias, variance, consistency, and practical stability where possible.
5. Interpret the estimate in actuarial units.

### Extra practical actuarial examples

- Severity example: estimating a Pareto tail parameter from a few large losses may be highly unstable, so sensitivity testing is essential.
- Frequency example: the Poisson MLE for a constant rate is total claims divided by total exposure.
- Simulation example: inverse transform simulation uses uniform random numbers to generate losses from a fitted distribution for stress testing.

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

### Topic 88: Two-Sample Variance and Mean Tests

#### 1. Concept theory

When comparing two groups, we may need to test whether their variances are equal and whether their means differ.

The usual sequence is:

1. test or assess equality of variances
2. choose an appropriate two-sample mean test

#### 2. Why actuaries care

Actuaries compare claim amounts, visitor counts, lapse rates, expenses, and loss ratios across groups such as cold versus warm weeks, regions, products, or channels.

#### 3. Mathematical derivation

For two independent normal samples:

```text
F = S1^2 / S2^2
```

under equal variances:

```text
F ~ F(n1 - 1, n2 - 1)
```

For means, if equal variances are reasonable, use pooled variance:

```text
Sp^2 = [(n1 - 1)S1^2 + (n2 - 1)S2^2] / (n1 + n2 - 2)
```

Then:

```text
t = (Xbar1 - Xbar2) / [Sp sqrt(1/n1 + 1/n2)]
```

with:

```text
n1 + n2 - 2 degrees of freedom
```

If variances are not equal, use Welch's test.

#### 4. Simple example

Suppose:

```text
n1 = 10, S1^2 = 100
n2 = 12, S2^2 = 50
```

Variance ratio:

```text
F = 100 / 50 = 2
```

Compare with:

```text
F(9,11)
```

#### 5. Exam-style case study

In November 2020, theme park visitors are split into:

```text
25 cold weeks
35 warm weeks
```

The variance test is:

```text
H0: sigma_cold^2 = sigma_warm^2
H1: sigma_cold^2 > sigma_warm^2
```

Use:

```text
F = S_cold^2 / S_warm^2
```

Then test the mean difference using the appropriate two-sample t method.

#### 6. Real-world actuarial case study

An insurer compares weekly claim volumes in monsoon and non-monsoon weeks.

Before comparing average claims, the actuary checks whether variability differs. If monsoon weeks are much more volatile, a simple equal-variance t-test may be inappropriate.

#### 7. Common mistakes

- Testing means before noticing variance inequality.
- Putting the smaller variance in the numerator for a one-sided larger-variance test.
- Forgetting F degrees of freedom are `n1 - 1`, `n2 - 1`.
- Using paired tests for independent groups.
- Assuming equal variances without checking.

#### 8. Revision checkpoint

Without notes, you should be able to write:

```text
F = S1^2/S2^2
Sp^2 = [(n1-1)S1^2 + (n2-1)S2^2]/(n1+n2-2)
t = (Xbar1-Xbar2)/(Sp sqrt(1/n1+1/n2))
```

### Expanded deep explanation

Estimation is the bridge between data and assumptions. You choose an estimator, understand its sampling behaviour, and check whether the method is stable for the data available.

- Inference is a disciplined way of deciding whether observed experience is consistent with an assumption. A confidence interval estimates a plausible range for a parameter; a hypothesis test checks whether the data are surprising under a stated null hypothesis.
- Type I error is rejecting a true null hypothesis. Type II error is failing to reject a false null hypothesis. Power is the probability of detecting an effect when it is truly present.
- A prediction interval is wider than a confidence interval for a mean because it includes both uncertainty about the average and the randomness of a new observation.

### Step-by-step working method

1. Identify the parameter being estimated or tested.
2. Write the null and alternative hypotheses, if a test is required.
3. Choose the reference distribution and degrees of freedom.
4. Compute the statistic, p-value, interval, or critical region.
5. Finish with a plain-English actuarial conclusion.

### Extra practical actuarial examples

- Fraud model example: a higher observed detection rate is useful only if the improvement is statistically convincing and operationally valuable after false positives are considered.
- Claims inflation example: test whether average claim cost has changed before changing a pricing basis, but also review exposure mix and large claims.
- Reserve example: a confidence interval for mean settlement cost should not be confused with a prediction interval for a single future claim.

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

### Topic 89: Implicit MLE Equations and Linear Interpolation

#### 1. Concept theory

Some maximum likelihood estimators cannot be solved neatly by algebra. Instead, the likelihood equation gives an implicit equation such as:

\[
g(\theta)=\text{sample statistic}
\]

In exams, you may be asked to solve it approximately. If two trial values are given, linear interpolation can produce a quick estimate.

#### 2. Why actuaries care

Actuarial models often involve parameters that are estimated numerically:

- truncated claim count models
- grouped loss data
- censored survival models
- GLMs fitted by iterative methods
- claim severity distributions with no closed-form MLE

An actuary must understand the equation being solved, not just press software buttons.

#### 3. Mathematical derivation

Suppose the MLE must satisfy:

\[
g(\theta)=c
\]

where \(c\) is calculated from data.

Assume:

\[
g(\theta_1)=g_1,\quad g(\theta_2)=g_2
\]

and:

\[
g_1<c<g_2
\]

Using straight-line interpolation:

\[
\frac{\hat\theta-\theta_1}{\theta_2-\theta_1}
=
\frac{c-g_1}{g_2-g_1}
\]

So:

\[
\hat\theta
=\theta_1+
(\theta_2-\theta_1)\frac{c-g_1}{g_2-g_1}
\]

This is not exact unless \(g\) is linear, but it is often good enough for exam estimation.

#### 4. Simple example

Suppose:

\[
g(0.6)=2.2131,\quad g(0.7)=2.2539
\]

and the required value is:

\[
c=2.2275
\]

Then:

\[
\hat\lambda
=0.6+(0.7-0.6)\frac{2.2275-2.2131}{2.2539-2.2131}
\]

\[
=0.6+0.1\frac{0.0144}{0.0408}
\]

\[
=0.6353
\]

So:

\[
\hat\lambda \approx 0.635
\]

#### 5. Exam-style case study

A truncated Poisson model is fitted to observed claim counts, where only policies with at least two claims are included.

Observed frequencies:

| Claim count | Frequency |
|---:|---:|
| 2 | 230 |
| 3 | 54 |
| 4 or more | 6 |

Using 4 as the grouped value for the final class:

\[
\bar x=\frac{230(2)+54(3)+6(4)}{230+54+6}
\]

\[
=\frac{646}{290}=2.2275
\]

The likelihood equation gives:

\[
g(\lambda)=2.2275
\]

Given:

\[
g(0.6)=2.2131,\quad g(0.7)=2.2539
\]

Use interpolation:

\[
\hat\lambda
=0.6+0.1\frac{2.2275-2.2131}{2.2539-2.2131}
\]

\[
\hat\lambda \approx 0.635
\]

#### 6. Real-world actuarial case study

A motor insurer studies repeat claimants. The dataset only contains policies with at least two claims because single-claim policies were handled by another reporting system.

If the actuary fits an ordinary Poisson model without allowing for truncation, the claim frequency will be overstated. Instead, the actuary fits a zero-one truncated Poisson model and solves the likelihood equation numerically.

Even if final estimation is done in software, interpolation helps the actuary check whether the software output is reasonable.

#### 7. Common mistakes

- Treating a truncated sample as an ordinary full sample.
- Forgetting that grouped observations such as "4 or more" may require an approximation or grouped likelihood.
- Interpolating in the wrong direction.
- Using the two nearest parameter values but not checking whether the target lies between the two function values.
- Reporting too many decimals when the input values are approximate.

#### 8. Revision checkpoint

You should be able to:

- recognise when an MLE equation has no simple closed form
- calculate a sample statistic from grouped data
- use linear interpolation to approximate a parameter
- explain why truncation changes the likelihood

### Expanded deep explanation

Estimation is the bridge between data and assumptions. You choose an estimator, understand its sampling behaviour, and check whether the method is stable for the data available.

- An estimator is a rule for turning data into a parameter estimate. Different estimators can have different bias, variance, mean squared error, and robustness.
- Maximum likelihood chooses the parameter that makes the observed data most probable under the model. Method of moments matches sample moments to theoretical moments.
- Large-sample theory can give approximate intervals, but boundary estimates, censored observations, small samples, and heavy tails can make approximations unreliable.

### Step-by-step working method

1. Write the model and parameter space.
2. Choose MOM, MLE, least squares, or another estimator.
3. Derive or solve the estimating equation.
4. Check bias, variance, consistency, and practical stability where possible.
5. Interpret the estimate in actuarial units.

### Extra practical actuarial examples

- Severity example: estimating a Pareto tail parameter from a few large losses may be highly unstable, so sensitivity testing is essential.
- Frequency example: the Poisson MLE for a constant rate is total claims divided by total exposure.
- Simulation example: inverse transform simulation uses uniform random numbers to generate losses from a fitted distribution for stress testing.

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

## Master Chapter 6: Confidence Intervals, Hypothesis Tests, Power, and Non-Parametric Tests

Inference converts sample evidence into a controlled decision. State the parameter, write hypotheses, choose the statistic, use the correct reference distribution, and translate the result into business language.

### Topics in this master chapter

- Topic 90: Hypothesis Testing, Confidence Intervals, and Prediction Intervals
- Topic 91: Type I Error, Type II Error, Power, and Critical Regions
- Topic 92: Regression Inference: Slope Confidence Interval, ANOVA, and F-Test
- Topic 93: Paired Data and Confidence Interval for Mean Reduction
- Topic 94: Contingency Tables, Non-parametric Tests, and Simpson's Paradox
- Topic 95: Diagnostic Test Tables, Type I Error, Type II Error, and Accuracy
- Topic 96: Poisson Goodness-of-Fit Test with Grouping
- Topic 97: Gompertz Mortality Regression and Confidence Interval for Mean Response
- Topic 98: Specificity, Power, False Positives, and False Negatives
- Topic 99: One-Sample t-Tests, Confidence Intervals, Variance, Sample Size, and Outliers

### Topic 90: Hypothesis Testing, Confidence Intervals, and Prediction Intervals

#### 1. Concept theory

Hypothesis testing asks whether sample evidence is strong enough to reject a default claim.

```text
H0 = null hypothesis
H1 = alternative hypothesis
```

Confidence intervals estimate unknown population parameters.

Prediction intervals estimate future individual observations.

#### 2. Why actuaries care

Actuaries test whether:

```text
new fraud model is better
average claim cost changed
mortality assumption is still valid
new underwriting rule reduced losses
```

#### 3. Mathematical derivation

For a sample mean:

```text
X_bar approximately Normal(mu, sigma^2/n)
```

So:

```text
Z = (X_bar - mu) / (s/sqrt(n))
```

Confidence interval:

```text
X_bar +/- z * s/sqrt(n)
```

Prediction interval:

```text
X_bar +/- z * s * sqrt(1 + 1/n)
```

The prediction interval is wider because it includes:

```text
uncertainty in the mean + randomness of a new observation
```

#### 4. Simple example

For a giant wheel:

```text
sample mean maximum height = 41
s = 3
n = 300
z = 1.96
```

Confidence interval:

```text
41 +/- 1.96 * 3/sqrt(300)
= 41 +/- 0.34
= (40.66, 41.34)
```

Prediction interval:

```text
41 +/- 1.96 * 3 * sqrt(1 + 1/300)
= 41 +/- 5.89
= (35.11, 46.89)
```

#### 5. Exam-style case study

A fraud detection model has observed sensitivity:

```text
p_hat = 680/800 = 0.85
```

Test:

```text
H0: p = 0.80
H1: p > 0.80
```

Approximate standard error under `H0`:

```text
SE = sqrt(0.80 * 0.20 / 800)
```

The test checks whether 85% is far enough above 80% to be statistically significant.

#### 6. Real-world actuarial case study

If an insurer introduces a fraud model, a false positive means genuine customers are investigated. A false negative means fraud is paid. The statistical test must be connected to operational cost.

#### 7. Common mistakes

- Using a two-sided critical value in a one-sided test without thinking.
- Saying "accept H0" instead of "fail to reject H0".
- Confusing confidence interval with prediction interval.

#### 8. Revision checkpoint

Always finish a test with business language:

```text
There is / is not sufficient evidence that the model sensitivity exceeds 80%.
```

### Expanded deep explanation

Inference converts sample evidence into a controlled decision. State the parameter, write hypotheses, choose the statistic, use the correct reference distribution, and translate the result into business language.

- Regression estimates a conditional mean, not a causal law by default. The coefficient tells how the fitted average response changes when a predictor changes, usually holding other predictors fixed.
- The residuals are as important as the fitted line. Patterns, changing spread, influential points, and non-normal residuals can reveal that the chosen model is too simple or the scale is wrong.
- Prediction requires two layers of uncertainty: uncertainty in the fitted mean and random variation of the individual outcome. This is why individual prediction intervals are wider than mean response intervals.

### Step-by-step working method

1. Plot or inspect the relationship before fitting.
2. Fit the model and write the fitted equation.
3. Interpret each coefficient in business units.
4. Check residuals, leverage, fit statistics, and uncertainty.
5. Decide whether the model is suitable for pricing, reserving, or explanation.

### Extra practical actuarial examples

- Motor pricing example: claim frequency may increase with driver age band, vehicle group, region, and prior claims, but coefficient interpretation must respect all variables included in the model.
- Life insurance example: a transformed mortality rating can produce a smooth premium formula, but the formula must be checked against underwriting judgement and table constraints.
- Exam example: if asked for an individual prediction, include the extra 1 inside the square root of the prediction standard error.

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

### Topic 91: Type I Error, Type II Error, Power, and Critical Regions

#### 1. Concept theory

In hypothesis testing:

```text
Type I error = reject H0 when H0 is true
Type II error = fail to reject H0 when H1 is true
Power = 1 - Type II error
```

The size of a test is the probability of Type I error:

```text
alpha = P(reject H0 | H0 true)
```

#### 2. Why actuaries care

Every business test has two risks.

Example:

```text
H0: fraud model is not better than current model
H1: fraud model is better
```

Type I error:

```text
adopt a model that is not actually better
```

Type II error:

```text
fail to adopt a model that really is better
```

#### 3. Mathematical derivation

Suppose:

```text
X_bar ~ Normal(mu, sigma^2/n)
```

For a one-sided test:

```text
Reject H0 if X_bar >= c
```

To control Type I error:

```text
P(X_bar >= c | H0 true) <= alpha
```

So:

```text
c = mu_0 + z_alpha_right * sigma/sqrt(n)
```

Type II error at a specific alternative `mu_1`:

```text
beta = P(X_bar < c | mu = mu_1)
```

#### 4. Simple example

Test:

```text
H0: mu <= 50
H1: mu > 50
```

Known:

```text
sigma = 3
n = 10
alpha = 5%
```

Critical value:

```text
c = 50 + 1.64485 * 3/sqrt(10)
  = 51.560
```

If true mean is 52:

```text
beta = P(X_bar < 51.560 | mu=52)
```

Standardise:

```text
Z = (51.560 - 52) / (3/sqrt(10))
  = -0.4637
```

So:

```text
beta = Phi(-0.4637) = 0.321
```

#### 5. Exam-style case study

For exponential lifetime with mean `theta`, reject `H0` if:

```text
X > c
```

If under the alternative `theta = 30`, Type II error is 0.75:

```text
P(fail to reject H0 | theta=30) = P(X <= c | theta=30) = 0.75
```

For exponential mean 30:

```text
F(c) = 1 - exp(-c/30)
```

Set:

```text
1 - exp(-c/30) = 0.75
exp(-c/30) = 0.25
c = 30 log(4) = 41.59
```

Type I error under `theta = 20`:

```text
alpha = P(X > 41.59 | theta=20)
      = exp(-41.59/20)
      = 0.125
```

#### 6. Real-world actuarial case study

A pension scheme tests whether mortality improvement has increased. A Type I error may cause the actuary to hold unnecessarily high reserves. A Type II error may cause under-reserving if longevity risk has truly increased.

#### 7. Common mistakes

- Mixing up Type I and Type II error.
- Calculating Type II error under the null instead of under the alternative.
- Forgetting that power is `1 - beta`.

#### 8. Revision checkpoint

Always write:

```text
alpha uses H0 distribution
beta uses H1 distribution
```

### Expanded deep explanation

Inference converts sample evidence into a controlled decision. State the parameter, write hypotheses, choose the statistic, use the correct reference distribution, and translate the result into business language.

- Inference is a disciplined way of deciding whether observed experience is consistent with an assumption. A confidence interval estimates a plausible range for a parameter; a hypothesis test checks whether the data are surprising under a stated null hypothesis.
- Type I error is rejecting a true null hypothesis. Type II error is failing to reject a false null hypothesis. Power is the probability of detecting an effect when it is truly present.
- A prediction interval is wider than a confidence interval for a mean because it includes both uncertainty about the average and the randomness of a new observation.

### Step-by-step working method

1. Identify the parameter being estimated or tested.
2. Write the null and alternative hypotheses, if a test is required.
3. Choose the reference distribution and degrees of freedom.
4. Compute the statistic, p-value, interval, or critical region.
5. Finish with a plain-English actuarial conclusion.

### Extra practical actuarial examples

- Fraud model example: a higher observed detection rate is useful only if the improvement is statistically convincing and operationally valuable after false positives are considered.
- Claims inflation example: test whether average claim cost has changed before changing a pricing basis, but also review exposure mix and large claims.
- Reserve example: a confidence interval for mean settlement cost should not be confused with a prediction interval for a single future claim.

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

### Topic 92: Regression Inference: Slope Confidence Interval, ANOVA, and F-Test

#### 1. Concept theory

After fitting a regression line, we need to test whether the relationship is statistically meaningful.

For simple linear regression:

```text
Y = alpha + beta X + error
```

The key inference questions are:

```text
Is beta significantly different from zero?
What is a confidence interval for beta?
How much variation does the model explain?
```

#### 2. Why actuaries care

Actuaries use regression to decide whether a rating factor should enter a pricing model.

Examples:

```text
Does number of past claims predict future claims?
Does age affect medical cost?
Does social media activity predict daily sales for a commercial client?
Does temperature affect delivery delays?
```

#### 3. Mathematical derivation

For simple regression:

```text
beta_hat = S_xy / S_xx
alpha_hat = y_bar - beta_hat x_bar
```

The estimated error variance is:

```text
MSE = SSE / (n - 2)
```

The standard error of the slope is:

```text
SE(beta_hat) = sqrt(MSE / S_xx)
```

A confidence interval for the slope is:

```text
beta_hat +/- t_critical * SE(beta_hat)
```

ANOVA decomposition:

```text
SST = SSR + SSE
```

Mean squares:

```text
MSR = SSR / df_regression
MSE = SSE / df_residual
```

F-test:

```text
F = MSR / MSE
```

For simple regression:

```text
df_regression = 1
df_residual = n - 2
```

#### 4. Simple example

Suppose:

```text
n = 30
Sxx = 213.9667
Sxy = 290.8067
SSE = 246.15
```

Slope:

```text
beta_hat = 290.8067 / 213.9667 = 1.3591
```

MSE:

```text
MSE = 246.15 / 28 = 8.7911
```

Standard error:

```text
SE(beta_hat) = sqrt(8.7911 / 213.9667) = 0.2027
```

Using `t = 2.0484`:

```text
CI = 1.3591 +/- 2.0484(0.2027)
   = (0.9439, 1.7743)
```

#### 5. Exam-style case study

Given ANOVA table:

```text
SSR = 395.29
SSE = 246.15
SST = 641.44
n = 30
```

Degrees of freedom:

```text
Regression df = 1
Residual df = 30 - 2 = 28
Total df = 29
```

Mean squares:

```text
MSR = 395.29 / 1 = 395.29
MSE = 246.15 / 28 = 8.79
```

Coefficient of determination:

```text
R^2 = SSR / SST = 395.29 / 641.44 = 0.6163
```

F-test:

```text
F = 395.29 / 8.79 = 44.96
```

If the critical value is about `4.196`, then:

```text
44.96 > 4.196
```

Reject `H0: beta = 0`. There is significant evidence of a linear relationship.

#### 6. Real-world actuarial case study

A commercial insurer studies whether the number of safety inspections predicts claim frequency. A positive slope may mean more inspections are associated with higher-risk firms, not necessarily that inspections cause claims.

The actuary uses the regression test to decide whether the variable is statistically useful, then uses business judgement to interpret it.

#### 7. Common mistakes

- Using `n - 1` instead of `n - 2` for residual degrees of freedom in simple regression.
- Confusing `SSE`, `SSR`, and `SST`.
- Treating a significant slope as proof of causation.
- Forgetting that a confidence interval for slope uses a `t` critical value.

#### 8. Revision checkpoint

You should be able to complete an ANOVA table and calculate:

```text
beta_hat
SE(beta_hat)
slope confidence interval
R^2
F statistic
regression conclusion
```

### Expanded deep explanation

Inference converts sample evidence into a controlled decision. State the parameter, write hypotheses, choose the statistic, use the correct reference distribution, and translate the result into business language.

- Regression estimates a conditional mean, not a causal law by default. The coefficient tells how the fitted average response changes when a predictor changes, usually holding other predictors fixed.
- The residuals are as important as the fitted line. Patterns, changing spread, influential points, and non-normal residuals can reveal that the chosen model is too simple or the scale is wrong.
- Prediction requires two layers of uncertainty: uncertainty in the fitted mean and random variation of the individual outcome. This is why individual prediction intervals are wider than mean response intervals.

### Step-by-step working method

1. Plot or inspect the relationship before fitting.
2. Fit the model and write the fitted equation.
3. Interpret each coefficient in business units.
4. Check residuals, leverage, fit statistics, and uncertainty.
5. Decide whether the model is suitable for pricing, reserving, or explanation.

### Extra practical actuarial examples

- Motor pricing example: claim frequency may increase with driver age band, vehicle group, region, and prior claims, but coefficient interpretation must respect all variables included in the model.
- Life insurance example: a transformed mortality rating can produce a smooth premium formula, but the formula must be checked against underwriting judgement and table constraints.
- Exam example: if asked for an individual prediction, include the extra 1 inside the square root of the prediction standard error.

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

### Topic 93: Paired Data and Confidence Interval for Mean Reduction

#### 1. Concept theory

Paired data occurs when two observations are taken from the same subject or matched unit.

Examples:

```text
before and after treatment
same policy before and after underwriting change
same driver before and after safety training
same patient before and after medication
```

For paired data, analyse the differences, not the two samples separately.

#### 2. Why actuaries care

Actuaries often assess interventions:

```text
did wellness programme reduce claims?
did fraud control reduce leakage?
did safety training reduce accident frequency?
did medication reduce health-risk score?
```

The paired structure removes subject-level variation and focuses on the change.

#### 3. Mathematical derivation

Let:

```text
D_i = before_i - after_i
```

Then calculate:

```text
D_bar = sum D_i / n
s_D = sample standard deviation of differences
```

Confidence interval:

```text
D_bar +/- t * s_D/sqrt(n)
```

Use `n-1` degrees of freedom.

#### 4. Simple example

Before:

```text
10, 12, 9
```

After:

```text
6, 7, 5
```

Differences:

```text
4, 5, 4
```

Mean reduction:

```text
D_bar = 4.33
```

Now build the confidence interval using the standard deviation of:

```text
4, 5, 4
```

not the standard deviations of the two separate samples.

#### 5. Exam-style case study

For BAC before and after a drug:

```text
before: 56, 32, 49, 57, 44
after : 8, 4, 7, 6, 5
```

Differences:

```text
48, 28, 42, 51, 39
```

Mean reduction:

```text
41.6
```

Sample standard deviation of reductions:

```text
8.96
```

A confidence interval for reduction is:

```text
41.6 +/- t * 8.96/sqrt(5)
```

#### 6. Real-world actuarial case study

A health insurer tests whether a diabetes management programme reduces annual claim cost. Comparing each member before and after enrolment is paired analysis. This is more powerful than comparing two unrelated groups because each person acts as their own control.

#### 7. Common mistakes

- Treating paired data as independent two-sample data.
- Taking `after - before` when the question asks for reduction.
- Using the original observations instead of differences.
- Using `n-2` degrees of freedom instead of `n-1`.

#### 8. Revision checkpoint

You should be able to:

```text
create paired differences
calculate mean difference
calculate standard deviation of differences
build a t confidence interval
```

### Expanded deep explanation

Inference converts sample evidence into a controlled decision. State the parameter, write hypotheses, choose the statistic, use the correct reference distribution, and translate the result into business language.

- Inference is a disciplined way of deciding whether observed experience is consistent with an assumption. A confidence interval estimates a plausible range for a parameter; a hypothesis test checks whether the data are surprising under a stated null hypothesis.
- Type I error is rejecting a true null hypothesis. Type II error is failing to reject a false null hypothesis. Power is the probability of detecting an effect when it is truly present.
- A prediction interval is wider than a confidence interval for a mean because it includes both uncertainty about the average and the randomness of a new observation.

### Step-by-step working method

1. Identify the parameter being estimated or tested.
2. Write the null and alternative hypotheses, if a test is required.
3. Choose the reference distribution and degrees of freedom.
4. Compute the statistic, p-value, interval, or critical region.
5. Finish with a plain-English actuarial conclusion.

### Extra practical actuarial examples

- Fraud model example: a higher observed detection rate is useful only if the improvement is statistically convincing and operationally valuable after false positives are considered.
- Claims inflation example: test whether average claim cost has changed before changing a pricing basis, but also review exposure mix and large claims.
- Reserve example: a confidence interval for mean settlement cost should not be confused with a prediction interval for a single future claim.

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

### Topic 94: Contingency Tables, Non-parametric Tests, and Simpson's Paradox

#### 1. Concept theory

A contingency table summarises counts across categories. It is used to study whether two categorical variables are related.

Non-parametric tests are tests that do not rely heavily on a specific distribution such as normality. They are useful when data are ordinal, categorical, skewed, or when assumptions for parametric tests are doubtful.

Simpson's paradox occurs when an overall comparison gives one conclusion, but the conclusion changes after splitting the data into important subgroups.

#### 2. Why actuaries care

Actuaries frequently compare claim rates, pass rates, mortality rates, lapse rates, and fraud rates across groups.

A simple overall percentage can be misleading if the mix of risks differs between groups. This is especially important in pricing, underwriting, health insurance, and experience investigations.

#### 3. Mathematical derivation

For a two-way contingency table, the expected count under independence is:

```text
Expected count = row total x column total / grand total
```

The chi-square test statistic is:

```text
X^2 = sum (Observed - Expected)^2 / Expected
```

Large values suggest the variables are not independent.

For a `r x c` table, the degrees of freedom are:

```text
(r - 1)(c - 1)
```

Simpson's paradox happens because an overall rate is a weighted average of subgroup rates:

```text
overall rate = sum subgroup successes / sum subgroup totals
```

The weights are the subgroup sizes. If two groups have very different subgroup mixes, the overall comparison can be distorted.

#### 4. Simple example

Two regions sell easy and difficult insurance products.

```text
Easy product:
Region A: 80 successes out of 100 = 80%
Region B: 45 successes out of 50  = 90%
```

```text
Difficult product:
Region A: 10 successes out of 50  = 20%
Region B: 18 successes out of 100 = 18%
```

By product:

- Region B is better for the easy product.
- Region A is better for the difficult product.

Overall:

```text
Region A: 90 / 150 = 60%
Region B: 63 / 150 = 42%
```

The overall result suggests Region A is better, but the subgroup story is more subtle. This is why actuaries must check the risk mix before drawing conclusions.

#### 5. Exam-style case study

An exam question compares pass rates across two regions and two subjects.

The wrong approach is to compare only:

```text
total passes / total candidates
```

The better approach is:

1. compare pass rates within each subject
2. check whether one region had more candidates in the easier subject
3. explain that overall pass rates may be affected by subject mix

If asked for a test of association, set up a contingency table and use:

```text
Expected = row total x column total / grand total
X^2 = sum (O - E)^2 / E
```

If sample sizes are small, mention that Fisher's exact test may be more appropriate.

#### 6. Real-world actuarial case study

A health insurer compares hospital readmission rates between two hospital networks.

Network A appears to have a higher readmission rate overall. But after splitting patients by severity, Network A has lower readmission rates in each severity group.

The overall rate was misleading because Network A treated a higher proportion of severe cases.

An actuary would adjust for case mix before concluding that one network performs worse. This can affect provider contracts, reserving assumptions, and pricing.

#### 7. Common mistakes

- Comparing overall percentages without checking subgroup mix.
- Using a chi-square test when expected counts are too small.
- Treating association as causation.
- Ignoring confounding variables.
- Forgetting the degrees of freedom formula `(r - 1)(c - 1)`.

#### 8. Revision checkpoint

Without notes, you should be able to compute expected counts, calculate a chi-square statistic, state the degrees of freedom, and explain Simpson's paradox using subgroup rates.

### Expanded deep explanation

Inference converts sample evidence into a controlled decision. State the parameter, write hypotheses, choose the statistic, use the correct reference distribution, and translate the result into business language.

- Inference is a disciplined way of deciding whether observed experience is consistent with an assumption. A confidence interval estimates a plausible range for a parameter; a hypothesis test checks whether the data are surprising under a stated null hypothesis.
- Type I error is rejecting a true null hypothesis. Type II error is failing to reject a false null hypothesis. Power is the probability of detecting an effect when it is truly present.
- A prediction interval is wider than a confidence interval for a mean because it includes both uncertainty about the average and the randomness of a new observation.

### Step-by-step working method

1. Identify the parameter being estimated or tested.
2. Write the null and alternative hypotheses, if a test is required.
3. Choose the reference distribution and degrees of freedom.
4. Compute the statistic, p-value, interval, or critical region.
5. Finish with a plain-English actuarial conclusion.

### Extra practical actuarial examples

- Fraud model example: a higher observed detection rate is useful only if the improvement is statistically convincing and operationally valuable after false positives are considered.
- Claims inflation example: test whether average claim cost has changed before changing a pricing basis, but also review exposure mix and large claims.
- Reserve example: a confidence interval for mean settlement cost should not be confused with a prediction interval for a single future claim.

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

### Topic 95: Diagnostic Test Tables, Type I Error, Type II Error, and Accuracy

#### 1. Concept theory

A diagnostic test compares a test result with the true state of the world.

In a two-by-two table:

- true positive: actually guilty/diseased/high risk and test says positive
- true negative: actually innocent/healthy/low risk and test says negative
- false positive: actually negative, but test says positive
- false negative: actually positive, but test says negative

In hypothesis-testing language:

```text
Type I error = false positive
Type II error = false negative
```

The meaning of positive and negative depends on the context. In the lie-detector question, "positive" means found guilty by the test.

#### 2. Why actuaries care

Actuaries use classification models in underwriting, fraud detection, lapse prediction, health risk scoring, and claim triage.

A model may look accurate overall but still create costly false positives or false negatives. For example, wrongly flagging a genuine claim as fraud damages customer experience, while missing real fraud increases claims cost.

#### 3. Mathematical derivation

Use this general table:

```text
                         Test negative     Test positive
Actually negative             A                 B
Actually positive             C                 D
```

Then:

```text
False positive count = B
False negative count = C
Correct count = A + D
Total count = A + B + C + D
```

Overall accuracy:

```text
Accuracy = (A + D) / (A + B + C + D)
```

In the November 2023 lie-detector wording:

```text
I  = actually innocent
G  = actually guilty
LG = lie detector says guilty
LI = lie detector says innocent
```

Type I error probability is:

```text
P(I | LG)
```

because the test says guilty, but the person is actually innocent.

Type II error probability is:

```text
P(G | LI)
```

because the test says innocent, but the person is actually guilty.

Using counts:

```text
P(I | LG) = B / (B + D)
P(G | LI) = C / (A + C)
```

#### 4. Simple example

Suppose:

```text
A = 80
B = 10
C = 5
D = 105
```

Accuracy:

```text
(A + D) / total = (80 + 105) / 200 = 0.925
```

Type I error:

```text
B / (B + D) = 10 / 115 = 0.087
```

Type II error:

```text
C / (A + C) = 5 / 85 = 0.059
```

#### 5. Exam-style case study

From the November 2023 solution:

```text
A = 356
B = 111
C = 105
D = 428
```

Accuracy:

```text
(356 + 428) / 1000 = 0.784
```

Type I error:

```text
P(I | LG) = B / (B + D)
          = 111 / (111 + 428)
          = 0.206
```

Type II error:

```text
P(G | LI) = C / (A + C)
          = 105 / (356 + 105)
          = 0.228
```

#### 6. Real-world actuarial case study

An insurer builds a fraud detection model. A flagged claim is sent for investigation.

False positives are genuine customers wrongly investigated. False negatives are fraudulent claims paid without detection.

The actuary must balance the cost of investigation, customer friction, fraud savings, and fairness. The best model is not always the one with the highest raw accuracy.

#### 7. Common mistakes

- Forgetting that "positive" depends on the problem definition.
- Dividing by the total when the question asks for a conditional probability.
- Mixing up `P(I | LG)` with `P(LG | I)`.
- Calling false negative a Type I error.
- Looking only at accuracy and ignoring error types.

#### 8. Revision checkpoint

Without notes, you should be able to draw a two-by-two table and calculate:

```text
Accuracy = (A + D) / total
Type I error = B / (B + D)
Type II error = C / (A + C)
```

after identifying what positive and negative mean in the question.

### Expanded deep explanation

Inference converts sample evidence into a controlled decision. State the parameter, write hypotheses, choose the statistic, use the correct reference distribution, and translate the result into business language.

- Inference is a disciplined way of deciding whether observed experience is consistent with an assumption. A confidence interval estimates a plausible range for a parameter; a hypothesis test checks whether the data are surprising under a stated null hypothesis.
- Type I error is rejecting a true null hypothesis. Type II error is failing to reject a false null hypothesis. Power is the probability of detecting an effect when it is truly present.
- A prediction interval is wider than a confidence interval for a mean because it includes both uncertainty about the average and the randomness of a new observation.

### Step-by-step working method

1. Identify the parameter being estimated or tested.
2. Write the null and alternative hypotheses, if a test is required.
3. Choose the reference distribution and degrees of freedom.
4. Compute the statistic, p-value, interval, or critical region.
5. Finish with a plain-English actuarial conclusion.

### Extra practical actuarial examples

- Fraud model example: a higher observed detection rate is useful only if the improvement is statistically convincing and operationally valuable after false positives are considered.
- Claims inflation example: test whether average claim cost has changed before changing a pricing basis, but also review exposure mix and large claims.
- Reserve example: a confidence interval for mean settlement cost should not be confused with a prediction interval for a single future claim.

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

### Topic 96: Poisson Goodness-of-Fit Test with Grouping

#### 1. Concept theory

A goodness-of-fit test checks whether observed frequencies are consistent with a proposed probability distribution.

For claim counts, a Poisson distribution is often tested because it is a natural model for counts of events over a fixed exposure period.

#### 2. Why actuaries care

Claim count modelling is central to pricing and reserving. If the Poisson fit is poor, a model with overdispersion or heterogeneity may be needed.

Examples include motor claims, health visits, pet insurance claims, and operational incidents.

#### 3. Mathematical derivation

For:

```text
X ~ Poisson(lambda)
```

the probability is:

```text
P(X = k) = exp(-lambda) lambda^k / k!
```

The recurrence relation is:

```text
P(X = k + 1) = lambda / (k + 1) x P(X = k)
```

Expected frequency in class `k`:

```text
Expected_k = n P(X = k)
```

Chi-square goodness-of-fit statistic:

```text
X^2 = sum (Observed - Expected)^2 / Expected
```

Degrees of freedom:

```text
number of grouped classes - 1 - number of estimated parameters
```

For a fitted Poisson with estimated `lambda`, subtract 1 parameter.

#### 4. Simple example

Suppose:

```text
n = 100
lambda_hat = 1
```

Then:

```text
P(0) = e^-1 = 0.3679
Expected 0 claims = 36.79
```

Using recurrence:

```text
P(1) = 1/1 x P(0) = 0.3679
P(2) = 1/2 x P(1) = 0.1839
```

Expected frequencies:

```text
36.79, 36.79, 18.39
```

#### 5. Exam-style case study

In the December 2022 claim-count question, the MLE is given:

```text
lambda_hat = 1.186
n = 1000
```

Start with:

```text
P(0) = exp(-1.186)
```

Then use:

```text
P(k + 1) = 1.186 / (k + 1) x P(k)
```

Multiply each probability by 1000 to get expected frequencies.

If high claim-count classes have expected counts below 5, combine tail groups such as:

```text
6, 7, 8+
```

Then calculate:

```text
X^2 = sum (O - E)^2 / E
```

and compare with the chi-square critical value using:

```text
df = grouped classes - 1 - 1
```

#### 6. Real-world actuarial case study

A pet insurer checks whether annual claim counts per dog follow a Poisson distribution. If observed data show too many zero-claim and high-claim policies, the Poisson model may be too simple.

The actuary may then consider a negative binomial model, risk categories, or a GLM with rating factors.

#### 7. Common mistakes

- Forgetting to estimate expected frequencies from probabilities.
- Not grouping cells with small expected counts.
- Forgetting to subtract the estimated parameter in degrees of freedom.
- Using observed frequencies as probabilities.
- Accepting the Poisson model without checking overdispersion.

#### 8. Revision checkpoint

Without notes, you should be able to write:

```text
P(k + 1) = lambda P(k) / (k + 1)
Expected = n x probability
X^2 = sum (O - E)^2 / E
df = grouped classes - 1 - estimated parameters
```

### Expanded deep explanation

Inference converts sample evidence into a controlled decision. State the parameter, write hypotheses, choose the statistic, use the correct reference distribution, and translate the result into business language.

- Inference is a disciplined way of deciding whether observed experience is consistent with an assumption. A confidence interval estimates a plausible range for a parameter; a hypothesis test checks whether the data are surprising under a stated null hypothesis.
- Type I error is rejecting a true null hypothesis. Type II error is failing to reject a false null hypothesis. Power is the probability of detecting an effect when it is truly present.
- A prediction interval is wider than a confidence interval for a mean because it includes both uncertainty about the average and the randomness of a new observation.

### Step-by-step working method

1. Identify the parameter being estimated or tested.
2. Write the null and alternative hypotheses, if a test is required.
3. Choose the reference distribution and degrees of freedom.
4. Compute the statistic, p-value, interval, or critical region.
5. Finish with a plain-English actuarial conclusion.

### Extra practical actuarial examples

- Fraud model example: a higher observed detection rate is useful only if the improvement is statistically convincing and operationally valuable after false positives are considered.
- Claims inflation example: test whether average claim cost has changed before changing a pricing basis, but also review exposure mix and large claims.
- Reserve example: a confidence interval for mean settlement cost should not be confused with a prediction interval for a single future claim.

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

### Topic 97: Gompertz Mortality Regression and Confidence Interval for Mean Response

#### 1. Concept theory

The Gompertz mortality law is:

```text
mu_x = B C^x
```

Taking logs turns it into a linear regression:

```text
ln(mu_x) = ln(B) + x ln(C)
```

So we can fit:

```text
Y = alpha + beta X
```

where:

```text
Y = ln(mu_x)
alpha = ln(B)
beta = ln(C)
```

#### 2. Why actuaries care

Mortality rates usually increase with age. Log-linear mortality models help actuaries estimate mortality, price life insurance, value annuities, and build mortality assumptions.

#### 3. Mathematical derivation

From:

```text
mu_x = B C^x
```

take natural logs:

```text
ln(mu_x) = ln(B) + x ln(C)
```

Fit regression:

```text
Y_i = alpha + beta X_i + error_i
```

Then:

```text
B_hat = exp(alpha_hat)
C_hat = exp(beta_hat)
```

For mean response at `x0`:

```text
Y_hat0 = alpha_hat + beta_hat x0
```

Standard error:

```text
SE(Y_hat0) = s sqrt[1/n + (x0 - xbar)^2 / Sxx]
```

Confidence interval:

```text
Y_hat0 +/- t_(n-2) SE(Y_hat0)
```

Transform back:

```text
mu interval = exp(log-scale interval)
```

#### 4. Simple example

If:

```text
alpha_hat = -10
beta_hat = 0.08
```

then:

```text
B_hat = exp(-10)
C_hat = exp(0.08)
```

At age 50:

```text
ln(mu_50) = -10 + 0.08(50) = -6
mu_50 = exp(-6)
```

#### 5. Exam-style case study

July 2022 gives `ln(mu_x)` and age data. The method is:

1. calculate `Sxx`, `Sxy`
2. estimate `beta = Sxy / Sxx`
3. estimate `alpha = ybar - beta xbar`
4. convert:

```text
B_hat = exp(alpha_hat)
C_hat = exp(beta_hat)
```

For a confidence interval for `mu_45`, first calculate the interval for:

```text
ln(mu_45)
```

then exponentiate both limits.

The interval is narrowest near `xbar`; therefore a predicted mean at age 41 is likely to have a narrower interval than one farther from the mean age.

#### 6. Real-world actuarial case study

A life insurer estimates cancer diagnosis rates by age. The raw rates increase nonlinearly, but the log rates are close to linear.

The actuary fits a log-linear model, checks residuals, and then uses the fitted curve to price age-specific benefits.

#### 7. Common mistakes

- Fitting regression to `mu_x` directly when the model is log-linear.
- Forgetting to exponentiate `alpha` and `beta` to get `B` and `C`.
- Treating a confidence interval on log scale as if it were already on mortality scale.
- Confusing confidence interval for mean response with prediction interval for an individual observation.
- Forgetting intervals are narrower near `xbar`.

#### 8. Revision checkpoint

Without notes, you should be able to transform:

```text
mu_x = B C^x
```

into:

```text
ln(mu_x) = ln(B) + x ln(C)
```

and build a confidence interval for mean log mortality before exponentiating.

### Expanded deep explanation

Inference converts sample evidence into a controlled decision. State the parameter, write hypotheses, choose the statistic, use the correct reference distribution, and translate the result into business language.

- Regression estimates a conditional mean, not a causal law by default. The coefficient tells how the fitted average response changes when a predictor changes, usually holding other predictors fixed.
- The residuals are as important as the fitted line. Patterns, changing spread, influential points, and non-normal residuals can reveal that the chosen model is too simple or the scale is wrong.
- Prediction requires two layers of uncertainty: uncertainty in the fitted mean and random variation of the individual outcome. This is why individual prediction intervals are wider than mean response intervals.

### Step-by-step working method

1. Plot or inspect the relationship before fitting.
2. Fit the model and write the fitted equation.
3. Interpret each coefficient in business units.
4. Check residuals, leverage, fit statistics, and uncertainty.
5. Decide whether the model is suitable for pricing, reserving, or explanation.

### Extra practical actuarial examples

- Motor pricing example: claim frequency may increase with driver age band, vehicle group, region, and prior claims, but coefficient interpretation must respect all variables included in the model.
- Life insurance example: a transformed mortality rating can produce a smooth premium formula, but the formula must be checked against underwriting judgement and table constraints.
- Exam example: if asked for an individual prediction, include the extra 1 inside the square root of the prediction standard error.

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

### Topic 98: Specificity, Power, False Positives, and False Negatives

#### 1. Concept theory

In testing and classification, we compare a model or test result against the true state.

Key terms:

- false positive: test says positive when truth is negative
- false negative: test says negative when truth is positive
- specificity: true negative rate
- power: true positive rate

Type I error is a false positive. Type II error is a false negative.

#### 2. Why actuaries care

Actuaries use classification in fraud detection, underwriting, claim triage, lapse prediction, medical screening, and risk alerts.

The cost of errors is not always symmetric. Rejecting a good customer may be very different from accepting a bad risk.

#### 3. Mathematical derivation

Use the standard table:

```text
                         Test positive     Test negative
Actually positive        True positive     False negative
Actually negative        False positive    True negative
```

Then:

```text
Type I error = false positive
Type II error = false negative
Power = P(reject H0 | H1 true) = true positive rate
Specificity = P(test negative | actually negative) = true negative rate
```

Also:

```text
Sensitivity = true positive rate = power
Specificity = true negative rate
```

#### 4. Simple example

Suppose a fraud model checks 100 claims:

```text
20 are truly fraudulent
80 are genuine
```

The model catches 16 fraudulent claims and wrongly flags 8 genuine claims.

Then:

```text
Power = 16 / 20 = 80%
Specificity = 72 / 80 = 90%
False positive rate = 8 / 80 = 10%
False negative rate = 4 / 20 = 20%
```

#### 5. Exam-style case study

In July 2022, the matching is:

```text
Type I error -> false positive
Type II error -> false negative
Specificity -> true negative
Power -> true positive
```

This is a quick scoring concept. The trap is to remember that power is not true negative; power is the ability to detect the effect when the alternative is true.

#### 6. Real-world actuarial case study

A life insurer uses an underwriting model to flag high-risk applications.

High power means it catches many truly high-risk applicants. High specificity means it does not wrongly flag too many low-risk applicants.

The business decision depends on balancing claim cost, customer fairness, and operational review cost.

#### 7. Common mistakes

- Confusing specificity with sensitivity.
- Saying power is the probability of accepting the null.
- Forgetting Type I error is false positive.
- Forgetting Type II error is false negative.
- Ignoring that the definition of positive depends on the problem.

#### 8. Revision checkpoint

Without notes, you should be able to match:

```text
Type I error = false positive
Type II error = false negative
Specificity = true negative
Power = true positive
```

### Expanded deep explanation

Inference converts sample evidence into a controlled decision. State the parameter, write hypotheses, choose the statistic, use the correct reference distribution, and translate the result into business language.

- Inference is a disciplined way of deciding whether observed experience is consistent with an assumption. A confidence interval estimates a plausible range for a parameter; a hypothesis test checks whether the data are surprising under a stated null hypothesis.
- Type I error is rejecting a true null hypothesis. Type II error is failing to reject a false null hypothesis. Power is the probability of detecting an effect when it is truly present.
- A prediction interval is wider than a confidence interval for a mean because it includes both uncertainty about the average and the randomness of a new observation.

### Step-by-step working method

1. Identify the parameter being estimated or tested.
2. Write the null and alternative hypotheses, if a test is required.
3. Choose the reference distribution and degrees of freedom.
4. Compute the statistic, p-value, interval, or critical region.
5. Finish with a plain-English actuarial conclusion.

### Extra practical actuarial examples

- Fraud model example: a higher observed detection rate is useful only if the improvement is statistically convincing and operationally valuable after false positives are considered.
- Claims inflation example: test whether average claim cost has changed before changing a pricing basis, but also review exposure mix and large claims.
- Reserve example: a confidence interval for mean settlement cost should not be confused with a prediction interval for a single future claim.

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

### Topic 99: One-Sample t-Tests, Confidence Intervals, Variance, Sample Size, and Outliers

#### 1. Concept theory

When the population variance is unknown and the data are approximately normal, a one-sample t-test is used to test a population mean.

The test statistic compares:

```text
sample mean - hypothesised mean
```

relative to the standard error.

A confidence interval gives a range of plausible values for the population mean.

#### 2. Why actuaries care

Actuaries test whether average claim size, average expense, average settlement delay, or average policy value differs from an assumption.

The same sample mean can produce different conclusions if the sample variance differs.

#### 3. Mathematical derivation

For a sample of size `n`:

```text
Xbar = sample mean
S^2 = sample variance
```

To test:

```text
H0: mu = mu0
H1: mu != mu0
```

use:

```text
t = (Xbar - mu0) / sqrt(S^2 / n)
```

with:

```text
n - 1 degrees of freedom
```

A two-sided confidence interval is:

```text
Xbar +/- t critical x sqrt(S^2 / n)
```

The width depends on:

```text
sample variance S^2
sample size n
t critical value
```

Higher variance widens the interval. Larger sample size narrows the interval.

#### 4. Simple example

Suppose:

```text
n = 16
Xbar = 20
S^2 = 16
mu0 = 18
```

Then:

```text
t = (20 - 18) / sqrt(16 / 16)
  = 2 / 1
  = 2
```

If the sample variance were 64 instead:

```text
t = 2 / sqrt(64 / 16)
  = 2 / 2
  = 1
```

Same mean difference, weaker evidence because the data are more variable.

#### 5. Exam-style case study

In March 2022, both samples had:

```text
n = 15
Xbar = 19.8
```

but different variances:

```text
Sample 1 variance = 6.385
Sample 2 variance = 22.814
```

For Sample 1:

```text
t = (19.8 - 18) / sqrt(6.385 / 15)
```

This is large enough to reject at the 98 percent confidence level.

For Sample 2, the same mean has a much wider confidence interval because the variance is larger. Since 18 lies inside the interval, the null is not rejected.

If a new sample has size 100, the standard error becomes smaller:

```text
sqrt(S^2 / 100)
```

so the confidence interval is narrower.

If an extreme outlier is replaced by a normal value, the variance is expected to reduce and the interval becomes narrower. The mean may also shift.

#### 6. Real-world actuarial case study

A claims team tests whether average claim settlement is above an assumed 18 days. Two portfolios have the same average settlement time, but one has many volatile claim delays.

The stable portfolio may show statistically significant evidence of higher average settlement time, while the volatile portfolio may not. This is why actuaries examine variance, not only the average.

#### 7. Common mistakes

- Using a z-test when variance is unknown and sample size is small.
- Ignoring that higher variance reduces significance.
- Thinking same mean and same sample size must give same test result.
- Forgetting outliers affect both mean and variance.
- Forgetting degrees of freedom are `n - 1`.

#### 8. Revision checkpoint

Without notes, you should be able to write:

```text
t = (Xbar - mu0) / sqrt(S^2 / n)
CI = Xbar +/- t critical x sqrt(S^2 / n)
```

and explain how variance, sample size, and outliers affect the result.

### Expanded deep explanation

Inference converts sample evidence into a controlled decision. State the parameter, write hypotheses, choose the statistic, use the correct reference distribution, and translate the result into business language.

- Inference is a disciplined way of deciding whether observed experience is consistent with an assumption. A confidence interval estimates a plausible range for a parameter; a hypothesis test checks whether the data are surprising under a stated null hypothesis.
- Type I error is rejecting a true null hypothesis. Type II error is failing to reject a false null hypothesis. Power is the probability of detecting an effect when it is truly present.
- A prediction interval is wider than a confidence interval for a mean because it includes both uncertainty about the average and the randomness of a new observation.

### Step-by-step working method

1. Identify the parameter being estimated or tested.
2. Write the null and alternative hypotheses, if a test is required.
3. Choose the reference distribution and degrees of freedom.
4. Compute the statistic, p-value, interval, or critical region.
5. Finish with a plain-English actuarial conclusion.

### Extra practical actuarial examples

- Fraud model example: a higher observed detection rate is useful only if the improvement is statistically convincing and operationally valuable after false positives are considered.
- Claims inflation example: test whether average claim cost has changed before changing a pricing basis, but also review exposure mix and large claims.
- Reserve example: a confidence interval for mean settlement cost should not be confused with a prediction interval for a single future claim.

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

## Master Chapter 7: Regression, ANOVA, and Predictive Modelling

Regression is a conditional mean model. The formula is only useful if the assumptions, residual behaviour, interpretation of coefficients, and prediction uncertainty are understood.

### Topics in this master chapter

- Topic 100: Regression and ANOVA
- Topic 101: Model Comparison: R-Squared, Adjusted R-Squared, and Overfitting
- Topic 102: Regression Through the Origin
- Topic 103: Log-Linear Regression by Transforming the Response
- Topic 104: Multiple Regression Output, Overall F-Test, and Backward Selection
- Topic 105: Linear Regression Calculation Workflow, Slope CI, Residual Checks, and Adjusted R-Squared
- Topic 106: ANOVA, F-Test, R-Squared, Deviance Tables, and Interaction Model Choice
- Topic 107: One-Way ANOVA from Group Means and Standard Deviations
- Topic 108: One-Parameter Least Squares and Weighted Least Squares
- Topic 109: Transformed Predictor Regression, Individual Prediction, and Mean Response

### Topic 100: Regression and ANOVA

#### 1. Concept theory

Regression explains how one variable changes with another.

Simple regression:

```text
Y = alpha + beta X + error
```

Multiple regression:

```text
Y = alpha + beta_1 X_1 + beta_2 X_2 + ... + error
```

In multiple regression, each coefficient is interpreted holding other variables constant.

#### 2. Why actuaries care

Regression helps actuaries quantify risk drivers:

```text
age
vehicle type
sum insured
occupation
region
past claim count
health indicators
```

#### 3. Mathematical derivation

OLS chooses `alpha` and `beta` to minimise:

```text
SSE = sum (y_i - alpha - beta x_i)^2
```

The slope estimate is:

```text
beta_hat = S_xy / S_xx
```

where:

```text
S_xy = sum (x_i - x_bar)(y_i - y_bar)
S_xx = sum (x_i - x_bar)^2
```

The intercept is:

```text
alpha_hat = y_bar - beta_hat x_bar
```

ANOVA decomposition:

```text
SST = SSR + SSE
```

Coefficient of determination:

```text
R^2 = SSR / SST
```

#### 4. Simple example

Using summary values:

```text
S_xx = 2,310
S_xy = -225
x_bar = 77
y_bar = 3.5
```

Slope:

```text
beta_hat = -225 / 2310 = -0.0974
```

Intercept:

```text
alpha_hat = 3.5 - (-0.0974)(77)
          = 11
```

Regression line:

```text
y_hat = 11 - 0.0974x
```

#### 5. Exam-style case study

If training hours increase and incidents decrease, the slope will be negative.

ANOVA checks:

```text
H0: beta = 0
H1: beta != 0
```

If the F-statistic is large, the slope is significantly different from zero.

#### 6. Real-world actuarial case study

A workers' compensation insurer may model workplace injury frequency against safety training hours. If the slope is significantly negative, the insurer may offer premium discounts for verified safety programmes.

#### 7. Common mistakes

- Treating correlation as causation.
- Ignoring outliers.
- Saying a higher `R^2` always means a better model.
- Forgetting "holding other variables constant" in multiple regression.

#### 8. Revision checkpoint

You should be able to explain:

```text
slope
intercept
residual
R^2
ANOVA F-test
```

### Expanded deep explanation

Regression is a conditional mean model. The formula is only useful if the assumptions, residual behaviour, interpretation of coefficients, and prediction uncertainty are understood.

- Regression estimates a conditional mean, not a causal law by default. The coefficient tells how the fitted average response changes when a predictor changes, usually holding other predictors fixed.
- The residuals are as important as the fitted line. Patterns, changing spread, influential points, and non-normal residuals can reveal that the chosen model is too simple or the scale is wrong.
- Prediction requires two layers of uncertainty: uncertainty in the fitted mean and random variation of the individual outcome. This is why individual prediction intervals are wider than mean response intervals.

### Step-by-step working method

1. Plot or inspect the relationship before fitting.
2. Fit the model and write the fitted equation.
3. Interpret each coefficient in business units.
4. Check residuals, leverage, fit statistics, and uncertainty.
5. Decide whether the model is suitable for pricing, reserving, or explanation.

### Extra practical actuarial examples

- Motor pricing example: claim frequency may increase with driver age band, vehicle group, region, and prior claims, but coefficient interpretation must respect all variables included in the model.
- Life insurance example: a transformed mortality rating can produce a smooth premium formula, but the formula must be checked against underwriting judgement and table constraints.
- Exam example: if asked for an individual prediction, include the extra 1 inside the square root of the prediction standard error.

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

### Topic 101: Model Comparison: R-Squared, Adjusted R-Squared, and Overfitting

#### 1. Concept theory

`R^2` measures the proportion of variation explained by a regression model:

```text
R^2 = SSR / SST = 1 - SSE/SST
```

But `R^2` never decreases when more predictors are added. This can encourage overfitting.

Adjusted `R^2` penalises unnecessary predictors.

#### 2. Why actuaries care

Pricing models often have many possible rating factors. Adding too many variables may improve historical fit but perform poorly on new data.

#### 3. Mathematical derivation

Adjusted `R^2`:

```text
R_adj^2 = 1 - [(n - 1)/(n - k - 1)](1 - R^2)
```

where:

```text
n = sample size
k = number of predictors
```

The penalty term:

```text
(n - 1)/(n - k - 1)
```

gets larger as predictors are added.

#### 4. Simple example

Model 1:

```text
n = 50
k = 4
R^2 = 0.712
```

```text
R_adj^2 = 1 - (49/45)(1 - 0.712)
          = 0.6864
```

Model 2:

```text
n = 50
k = 7
R^2 = 0.725
```

```text
R_adj^2 = 1 - (49/42)(1 - 0.725)
          = 0.6792
```

Even though Model 2 has higher `R^2`, Model 1 has higher adjusted `R^2`.

#### 5. Exam-style case study

If a question says:

```text
Model 2 is definitely better because R^2 is higher
```

the correct critique is:

```text
R^2 alone is not enough when models have different numbers of predictors.
Adjusted R^2 is better because it penalises complexity.
```

#### 6. Real-world actuarial case study

In motor pricing, a model with hundreds of variables may fit past claims well but be unstable. Regulators and management also need explainability. A slightly simpler model with better adjusted performance may be preferable.

#### 7. Common mistakes

- Choosing a model only because `R^2` is higher.
- Ignoring degrees of freedom.
- Forgetting that predictive performance should be checked on validation data.

#### 8. Revision checkpoint

When comparing models with different numbers of predictors, check adjusted `R^2`, deviance, AIC/BIC, or validation performance, not plain `R^2` alone.

### Expanded deep explanation

Regression is a conditional mean model. The formula is only useful if the assumptions, residual behaviour, interpretation of coefficients, and prediction uncertainty are understood.

- Regression estimates a conditional mean, not a causal law by default. The coefficient tells how the fitted average response changes when a predictor changes, usually holding other predictors fixed.
- The residuals are as important as the fitted line. Patterns, changing spread, influential points, and non-normal residuals can reveal that the chosen model is too simple or the scale is wrong.
- Prediction requires two layers of uncertainty: uncertainty in the fitted mean and random variation of the individual outcome. This is why individual prediction intervals are wider than mean response intervals.

### Step-by-step working method

1. Plot or inspect the relationship before fitting.
2. Fit the model and write the fitted equation.
3. Interpret each coefficient in business units.
4. Check residuals, leverage, fit statistics, and uncertainty.
5. Decide whether the model is suitable for pricing, reserving, or explanation.

### Extra practical actuarial examples

- Motor pricing example: claim frequency may increase with driver age band, vehicle group, region, and prior claims, but coefficient interpretation must respect all variables included in the model.
- Life insurance example: a transformed mortality rating can produce a smooth premium formula, but the formula must be checked against underwriting judgement and table constraints.
- Exam example: if asked for an individual prediction, include the extra 1 inside the square root of the prediction standard error.

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

### Topic 102: Regression Through the Origin

#### 1. Concept theory

Regression through the origin is a linear model with no intercept:

```text
Y_i = beta x_i + e_i
```

This forces the fitted line to pass through `(0,0)`.

It should only be used when the business meaning supports it.

#### 2. Why actuaries care

Some relationships naturally pass through zero:

```text
zero exposure gives zero expected claims
zero sum insured gives zero insured loss
zero fuel use gives zero distance
```

But many relationships do not. Forcing the intercept to zero can bias the slope if the true relationship has a fixed base cost.

#### 3. Mathematical derivation

Minimise:

```text
SSE = sum (y_i - beta x_i)^2
```

Differentiate with respect to `beta`:

```text
dSSE/dbeta = sum 2(y_i - beta x_i)(-x_i)
```

Set equal to zero:

```text
sum x_i y_i - beta sum x_i^2 = 0
```

So:

```text
beta_hat = sum x_i y_i / sum x_i^2
```

Since:

```text
Y_i | x_i ~ Normal(beta x_i, sigma^2)
```

we have:

```text
E[beta_hat] = beta
```

and:

```text
Var(beta_hat) = sigma^2 / sum x_i^2
```

Because the estimator is unbiased:

```text
MSE(beta_hat) = Var(beta_hat) = sigma^2 / sum x_i^2
```

#### 4. Simple example

Suppose:

```text
sum x_i y_i = 100
sum x_i^2 = 20
```

Then:

```text
beta_hat = 100 / 20 = 5
```

The fitted model is:

```text
y_hat = 5x
```

If:

```text
sigma^2 = 9
```

then:

```text
Var(beta_hat) = 9/20
```

#### 5. Exam-style case study

For aircraft fuel and speed:

```text
Y_i = beta x_i + e_i
e_i ~ Normal(0, sigma^2)
```

Then:

```text
Y_i | x_i ~ Normal(beta x_i, sigma^2)
beta_hat_LSE = sum x_i y_i / sum x_i^2
E[beta_hat_LSE] = beta
Var(beta_hat_LSE) = sigma^2 / sum x_i^2
MSE(beta_hat_LSE) = sigma^2 / sum x_i^2
```

#### 6. Real-world actuarial case study

In exposure-based pricing, an actuary may model expected claim count as proportional to exposure:

```text
expected claims = rate * exposure
```

If exposure is zero, expected claims should be zero, so a through-origin model may be sensible.

#### 7. Common mistakes

- Using the ordinary regression slope formula `Sxy/Sxx` when the model has no intercept.
- Forcing intercept zero without business justification.
- Forgetting that degrees of freedom differ from ordinary regression.
- Assuming `x_i` is random when the model conditions on observed `x_i`.

#### 8. Revision checkpoint

You should be able to derive:

```text
beta_hat = sum xy / sum x^2
```

from least squares.

### Expanded deep explanation

Regression is a conditional mean model. The formula is only useful if the assumptions, residual behaviour, interpretation of coefficients, and prediction uncertainty are understood.

- Regression estimates a conditional mean, not a causal law by default. The coefficient tells how the fitted average response changes when a predictor changes, usually holding other predictors fixed.
- The residuals are as important as the fitted line. Patterns, changing spread, influential points, and non-normal residuals can reveal that the chosen model is too simple or the scale is wrong.
- Prediction requires two layers of uncertainty: uncertainty in the fitted mean and random variation of the individual outcome. This is why individual prediction intervals are wider than mean response intervals.

### Step-by-step working method

1. Plot or inspect the relationship before fitting.
2. Fit the model and write the fitted equation.
3. Interpret each coefficient in business units.
4. Check residuals, leverage, fit statistics, and uncertainty.
5. Decide whether the model is suitable for pricing, reserving, or explanation.

### Extra practical actuarial examples

- Motor pricing example: claim frequency may increase with driver age band, vehicle group, region, and prior claims, but coefficient interpretation must respect all variables included in the model.
- Life insurance example: a transformed mortality rating can produce a smooth premium formula, but the formula must be checked against underwriting judgement and table constraints.
- Exam example: if asked for an individual prediction, include the extra 1 inside the square root of the prediction standard error.

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

### Topic 103: Log-Linear Regression by Transforming the Response

#### 1. Concept theory

Some relationships are multiplicative rather than additive.

Instead of:

```text
Y = alpha + beta X + error
```

we may have:

```text
Y = sigma * exp(lambda X + error)
```

Taking logs converts it into a linear model:

```text
log Y = log sigma + lambda X + error
```

#### 2. Why actuaries care

Claim sizes, rainfall damage, medical costs, and financial losses often grow multiplicatively.

Using logs can:

```text
make skewed data more linear
stabilise variance
ensure fitted values are positive
turn multiplicative effects into additive coefficients
```

#### 3. Mathematical derivation

Start with:

```text
Y = sigma * exp(lambda X + e)
```

Take natural logs:

```text
log Y = log sigma + lambda X + e
```

Let:

```text
Y_star = log Y
alpha_star = log sigma
beta_star = lambda
```

Then:

```text
Y_star = alpha_star + beta_star X + e
```

So:

```text
lambda_hat = S_x,logy / S_xx
log sigma_hat = mean(log y) - lambda_hat mean(x)
sigma_hat = exp(log sigma_hat)
```

#### 4. Simple example

Suppose:

```text
log sigma_hat = 0.70
lambda_hat = 0.10
```

Then:

```text
sigma_hat = exp(0.70) = 2.01
```

Model:

```text
Y_hat = 2.01 * exp(0.10X)
```

If `X` increases by 1, expected `Y` is multiplied by:

```text
exp(0.10) = 1.105
```

or about 10.5%.

#### 5. Exam-style case study

In the rainfall and fallen-trees question:

```text
Y = sigma * exp(lambda X + e)
```

Use the provided sums:

```text
sum log y
sum x log y
sum x
sum x^2
```

Fit a simple regression with:

```text
response = log y
explanatory variable = x
```

Then:

```text
lambda_hat = [sum x log y - n x_bar mean(log y)] / [sum x^2 - n x_bar^2]
```

and:

```text
sigma_hat = exp(mean(log y) - lambda_hat x_bar)
```

#### 6. Real-world actuarial case study

A property insurer models storm damage against rainfall. A linear model may predict negative damage for low rainfall, which is impossible. A log-linear model keeps predictions positive and often reflects the reality that each extra centimetre of rainfall has a multiplicative effect on loss.

#### 7. Common mistakes

- Fitting the model to `Y` instead of `log Y`.
- Forgetting to exponentiate the intercept to get `sigma`.
- Interpreting `lambda` as an additive change in original `Y`.
- Ignoring that back-transforming introduces bias if estimating mean on original scale.

#### 8. Revision checkpoint

You should be able to transform:

```text
Y = sigma exp(lambda X + e)
```

into:

```text
log Y = log sigma + lambda X + e
```

and estimate `lambda` and `sigma`.

### Expanded deep explanation

Regression is a conditional mean model. The formula is only useful if the assumptions, residual behaviour, interpretation of coefficients, and prediction uncertainty are understood.

- Regression estimates a conditional mean, not a causal law by default. The coefficient tells how the fitted average response changes when a predictor changes, usually holding other predictors fixed.
- The residuals are as important as the fitted line. Patterns, changing spread, influential points, and non-normal residuals can reveal that the chosen model is too simple or the scale is wrong.
- Prediction requires two layers of uncertainty: uncertainty in the fitted mean and random variation of the individual outcome. This is why individual prediction intervals are wider than mean response intervals.

### Step-by-step working method

1. Plot or inspect the relationship before fitting.
2. Fit the model and write the fitted equation.
3. Interpret each coefficient in business units.
4. Check residuals, leverage, fit statistics, and uncertainty.
5. Decide whether the model is suitable for pricing, reserving, or explanation.

### Extra practical actuarial examples

- Motor pricing example: claim frequency may increase with driver age band, vehicle group, region, and prior claims, but coefficient interpretation must respect all variables included in the model.
- Life insurance example: a transformed mortality rating can produce a smooth premium formula, but the formula must be checked against underwriting judgement and table constraints.
- Exam example: if asked for an individual prediction, include the extra 1 inside the square root of the prediction standard error.

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

### Topic 104: Multiple Regression Output, Overall F-Test, and Backward Selection

#### 1. Concept theory

Multiple regression uses several explanatory variables:

```text
Y = beta_0 + beta_1 X_1 + ... + beta_k X_k + error
```

There are two levels of testing:

```text
individual coefficient tests
overall model F-test
model selection
```

An individual coefficient is significant if its confidence interval does not contain zero.

The overall F-test checks whether at least one explanatory variable matters.

Backward selection starts with the full model and removes variables that do not add enough value.

#### 2. Why actuaries care

Pricing and risk models often have many candidate variables:

```text
age
duration
usage
region
claim history
hospital type
call failures
```

The actuary needs a model that is predictive, stable, and not unnecessarily complex.

#### 3. Mathematical derivation

ANOVA for multiple regression:

```text
df_regression = k
df_residual = n - k - 1
df_total = n - 1
```

Mean squares:

```text
MSR = SSR / k
MSE = SSE / (n-k-1)
```

Overall F-test:

```text
F = MSR / MSE
```

Hypotheses:

```text
H0: beta_1 = beta_2 = ... = beta_k = 0
H1: at least one beta_j != 0
```

Coefficient of determination:

```text
R^2 = SSR / SST
```

Adjusted R-squared:

```text
R_adj^2 = 1 - [(n-1)/(n-k-1)](1-R^2)
```

#### 4. Simple example

Suppose:

```text
n = 25
k = 4
MSR = 1602.75
SSE = 2137.00
```

Residual degrees of freedom:

```text
25 - 4 - 1 = 20
```

MSE:

```text
2137.00 / 20 = 106.85
```

F statistic:

```text
1602.75 / 106.85 = 15.00
```

If this is greater than the critical value, reject the view that no explanatory variable matters.

#### 5. Exam-style case study

If coefficient confidence intervals are:

```text
X1: (0.0783, 0.0987)
X2: (0.3313, 1.2337)
X3: (-9.8019, -6.2269)
X4: (-1.1590, 0.2846)
```

Then:

```text
X1, X2, X3 are significant because their intervals exclude zero.
X4 is not significant because its interval includes zero.
```

For backward selection, compare adjusted R-squared:

```text
Full model
X1 + X2 + X3
X1 + X2
X1
```

Choose the model with strong fit and better adjusted R-squared, often after removing non-significant variables.

#### 6. Real-world actuarial case study

A telecom insurer or embedded-insurance provider predicts customer value using usage, subscription length, age, and call failures. If call failures are not significant, keeping them may add noise and reduce interpretability.

#### 7. Common mistakes

- Thinking the overall F-test proves all variables are significant.
- Dropping variables only because their coefficient is small, not because they are statistically or practically weak.
- Choosing plain `R^2` instead of adjusted `R^2`.
- Ignoring business sense during backward selection.

#### 8. Revision checkpoint

You should be able to:

```text
identify significant variables from confidence intervals
complete a multiple-regression ANOVA table
calculate R^2 and adjusted R^2
interpret the overall F-test
explain backward selection
```

### Expanded deep explanation

Regression is a conditional mean model. The formula is only useful if the assumptions, residual behaviour, interpretation of coefficients, and prediction uncertainty are understood.

- Regression estimates a conditional mean, not a causal law by default. The coefficient tells how the fitted average response changes when a predictor changes, usually holding other predictors fixed.
- The residuals are as important as the fitted line. Patterns, changing spread, influential points, and non-normal residuals can reveal that the chosen model is too simple or the scale is wrong.
- Prediction requires two layers of uncertainty: uncertainty in the fitted mean and random variation of the individual outcome. This is why individual prediction intervals are wider than mean response intervals.

### Step-by-step working method

1. Plot or inspect the relationship before fitting.
2. Fit the model and write the fitted equation.
3. Interpret each coefficient in business units.
4. Check residuals, leverage, fit statistics, and uncertainty.
5. Decide whether the model is suitable for pricing, reserving, or explanation.

### Extra practical actuarial examples

- Motor pricing example: claim frequency may increase with driver age band, vehicle group, region, and prior claims, but coefficient interpretation must respect all variables included in the model.
- Life insurance example: a transformed mortality rating can produce a smooth premium formula, but the formula must be checked against underwriting judgement and table constraints.
- Exam example: if asked for an individual prediction, include the extra 1 inside the square root of the prediction standard error.

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

### Topic 105: Linear Regression Calculation Workflow, Slope CI, Residual Checks, and Adjusted R-Squared

#### 1. Concept theory

Simple linear regression fits a straight line:

```text
y_hat = alpha + beta x
```

The slope `beta` measures the expected change in `Y` for a one-unit increase in `X`.

The exam often asks for the full calculation workflow: means, sums of squares, fitted line, confidence interval for slope, residual interpretation, `R^2`, and adjusted `R^2`.

#### 2. Why actuaries care

Regression is used for expense modelling, claim cost trends, mortality improvement, lapse analysis, investment relationships, and pricing models.

An actuary must not only fit the line, but also check whether the fitted relationship is useful, stable, and not overfitted.

#### 3. Mathematical derivation

Define:

```text
Sxx = sum x^2 - n xbar^2
Syy = sum y^2 - n ybar^2
Sxy = sum xy - n xbar ybar
```

The least-squares slope is:

```text
beta_hat = Sxy / Sxx
```

The intercept is:

```text
alpha_hat = ybar - beta_hat xbar
```

Residual variance estimate:

```text
s^2 = (Syy - Sxy^2 / Sxx) / (n - 2)
```

Standard error of the slope:

```text
SE(beta_hat) = sqrt(s^2 / Sxx)
```

Confidence interval for slope:

```text
beta_hat +/- t_(n-2, upper tail) x SE(beta_hat)
```

Coefficient of determination:

```text
R^2 = Sxy^2 / (Sxx Syy)
```

Adjusted `R^2`:

```text
Adjusted R^2 = 1 - [(n - 1) / (n - k - 1)](1 - R^2)
```

where `k` is the number of explanatory variables.

#### 4. Simple example

Suppose:

```text
Sxx = 20
Syy = 45
Sxy = 24
n = 6
```

Slope:

```text
beta_hat = 24 / 20 = 1.2
```

R-squared:

```text
R^2 = 24^2 / (20 x 45)
    = 576 / 900
    = 0.64
```

So 64 percent of the variation in `Y` is explained by the regression on `X`.

#### 5. Exam-style case study

From the November 2023 solution:

```text
xbar = 13.8
ybar = 7.58
Sxx = 230.8
Syy = 172.828
Sxy = 196.78
```

Slope:

```text
beta_hat = 196.78 / 230.8 = 0.85
```

Intercept:

```text
alpha_hat = 7.58 - 0.85(13.8)
          = -4.186
```

Fitted line:

```text
y_hat = -4.186 + 0.85x
```

Residual variance:

```text
s^2 = [172.828 - 196.78^2 / 230.8] / 3
    = 1.6845
```

For a 99 percent confidence interval:

```text
0.85 +/- t x sqrt(1.6845 / 230.8)
```

Using the given exam value:

```text
CI = (0.351, 1.349)
```

If:

```text
n = 6
R^2 one-variable model = 0.84
R^2 two-variable model = 0.87
```

Adjusted `R^2`:

```text
One-variable = 1 - [(6 - 1)/(6 - 1 - 1)](1 - 0.84) = 0.80
Two-variable = 1 - [(6 - 1)/(6 - 2 - 1)](1 - 0.87) = 0.78
```

The one-variable model is preferred by adjusted `R^2`.

#### 6. Real-world actuarial case study

A real estate insurer models rebuilding cost using local population density. A positive slope suggests higher-density areas have higher floor-price indices and therefore higher insured values.

But before using the model, the actuary checks residuals. If residuals show no clear pattern and are roughly centred around zero, the simple linear model may be acceptable. If residuals curve or fan out, the model may need transformation or additional predictors.

#### 7. Common mistakes

- Using `Sxy / Syy` instead of `Sxy / Sxx` for the slope.
- Forgetting the intercept formula `ybar - beta_hat xbar`.
- Using `n - 1` instead of `n - 2` for simple regression residual degrees of freedom.
- Thinking higher `R^2` always means a better model.
- Ignoring residual patterns.

#### 8. Revision checkpoint

Without notes, you should be able to compute:

```text
Sxx, Syy, Sxy
beta_hat = Sxy / Sxx
alpha_hat = ybar - beta_hat xbar
R^2 = Sxy^2 / (Sxx Syy)
Adjusted R^2 = 1 - [(n - 1)/(n - k - 1)](1 - R^2)
```

and explain how residuals are used to check model suitability.

### Expanded deep explanation

Regression is a conditional mean model. The formula is only useful if the assumptions, residual behaviour, interpretation of coefficients, and prediction uncertainty are understood.

- Regression estimates a conditional mean, not a causal law by default. The coefficient tells how the fitted average response changes when a predictor changes, usually holding other predictors fixed.
- The residuals are as important as the fitted line. Patterns, changing spread, influential points, and non-normal residuals can reveal that the chosen model is too simple or the scale is wrong.
- Prediction requires two layers of uncertainty: uncertainty in the fitted mean and random variation of the individual outcome. This is why individual prediction intervals are wider than mean response intervals.

### Step-by-step working method

1. Plot or inspect the relationship before fitting.
2. Fit the model and write the fitted equation.
3. Interpret each coefficient in business units.
4. Check residuals, leverage, fit statistics, and uncertainty.
5. Decide whether the model is suitable for pricing, reserving, or explanation.

### Extra practical actuarial examples

- Motor pricing example: claim frequency may increase with driver age band, vehicle group, region, and prior claims, but coefficient interpretation must respect all variables included in the model.
- Life insurance example: a transformed mortality rating can produce a smooth premium formula, but the formula must be checked against underwriting judgement and table constraints.
- Exam example: if asked for an individual prediction, include the extra 1 inside the square root of the prediction standard error.

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

### Topic 106: ANOVA, F-Test, R-Squared, Deviance Tables, and Interaction Model Choice

#### 1. Concept theory

ANOVA and deviance tables compare how much variation is explained by a model.

In linear regression, the F-test checks whether the regression explains a significant amount of variation.

In GLMs, analysis of deviance compares nested models. A small p-value means the larger model gives a statistically significant improvement.

#### 2. Why actuaries care

Actuaries compare models constantly:

- pricing models with and without rating factors
- lapse models with and without interactions
- claim models with regional effects
- renewal models with different link functions

The goal is not just best fit, but useful, stable, explainable fit.

#### 3. Mathematical derivation

For simple regression ANOVA:

```text
SST = SSR + SSE
```

Degrees of freedom:

```text
Regression df = 1
Residual df = n - 2
Total df = n - 1
```

Mean squares:

```text
MSR = SSR / regression df
MSE = SSE / residual df
```

F-statistic:

```text
F = MSR / MSE
```

Coefficient of determination:

```text
R^2 = SSR / SST
```

For nested models in a deviance table:

```text
larger model preferred if deviance reduction is significant
```

But if an interaction is not significant, prefer the simpler additive model.

#### 4. Simple example

Suppose:

```text
SST = 100
SSR = 40
SSE = 60
regression df = 1
residual df = 12
```

Then:

```text
MSR = 40 / 1 = 40
MSE = 60 / 12 = 5
F = 40 / 5 = 8
R^2 = 40 / 100 = 40%
```

#### 5. Exam-style case study

March 2022 gives:

```text
Total SS = 5.38
Residual df = 12
Regression df = 1
SSREG = 1.38 for Model 1
```

Then:

```text
SSE = 5.38 - 1.38 = 4.00
MSE = 4.00 / 12
F = 1.38 / (4.00 / 12)
```

For Model 2, use:

```text
SSREG = 2.38
SSE = 5.38 - 2.38
R^2 = SSREG / 5.38
```

For deviance-table model selection:

- `y ~ x` significantly improves over `y ~ 1` if p-value is tiny.
- `y ~ x * region` improves over `y ~ x` if p-value is small.
- But if `y ~ x * region` does not improve over `y ~ x + region`, the interaction is unnecessary.
- Prefer `y ~ x + region` when the region main effect matters but interaction does not.

#### 6. Real-world actuarial case study

A renewal model predicts policy renewal rate using premium increase and region.

The actuary tests:

```text
Model 1: premium increase only
Model 2: premium increase + region
Model 3: premium increase * region
```

If region improves fit but the interaction is insignificant, the actuary uses the simpler additive region model. This is easier to explain and less likely to overfit.

#### 7. Common mistakes

- Confusing residual sum of squares with regression sum of squares.
- Choosing the model with higher `R^2` without checking significance or complexity.
- Treating interaction significance as the same as main-effect significance.
- Forgetting the hypothesis in an F-test: slope equals zero or no improvement from added terms.
- Ignoring p-values in deviance tables.

#### 8. Revision checkpoint

Without notes, you should be able to write:

```text
SST = SSR + SSE
F = MSR / MSE
R^2 = SSR / SST
```

and explain nested GLM model choice using deviance reduction and p-values.

### Expanded deep explanation

Regression is a conditional mean model. The formula is only useful if the assumptions, residual behaviour, interpretation of coefficients, and prediction uncertainty are understood.

- Regression estimates a conditional mean, not a causal law by default. The coefficient tells how the fitted average response changes when a predictor changes, usually holding other predictors fixed.
- The residuals are as important as the fitted line. Patterns, changing spread, influential points, and non-normal residuals can reveal that the chosen model is too simple or the scale is wrong.
- Prediction requires two layers of uncertainty: uncertainty in the fitted mean and random variation of the individual outcome. This is why individual prediction intervals are wider than mean response intervals.

### Step-by-step working method

1. Plot or inspect the relationship before fitting.
2. Fit the model and write the fitted equation.
3. Interpret each coefficient in business units.
4. Check residuals, leverage, fit statistics, and uncertainty.
5. Decide whether the model is suitable for pricing, reserving, or explanation.

### Extra practical actuarial examples

- Motor pricing example: claim frequency may increase with driver age band, vehicle group, region, and prior claims, but coefficient interpretation must respect all variables included in the model.
- Life insurance example: a transformed mortality rating can produce a smooth premium formula, but the formula must be checked against underwriting judgement and table constraints.
- Exam example: if asked for an individual prediction, include the extra 1 inside the square root of the prediction standard error.

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

### Topic 107: One-Way ANOVA from Group Means and Standard Deviations

#### 1. Concept theory

One-way ANOVA tests whether several group means are equal.

It compares:

- variation between group means
- variation within groups

If between-group variation is large relative to within-group variation, group membership may have an effect.

#### 2. Why actuaries care

Actuaries compare claim rates, lapse rates, resignation rates, expenses, or mortality across multiple groups.

ANOVA is useful when comparing more than two means at once.

#### 3. Mathematical derivation

Suppose there are `k` groups, each with `n_i` observations.

Total sample size:

```text
N = sum n_i
```

Grand mean:

```text
xbar = sum n_i xbar_i / N
```

Between-group sum of squares:

```text
SSB = sum n_i (xbar_i - xbar)^2
```

Within-group sum of squares:

```text
SSW = sum (n_i - 1)s_i^2
```

Degrees of freedom:

```text
df_between = k - 1
df_within = N - k
```

Mean squares:

```text
MSB = SSB / (k - 1)
MSW = SSW / (N - k)
```

F-statistic:

```text
F = MSB / MSW
```

#### 4. Simple example

Three groups have means:

```text
10, 12, 14
```

with equal sample size 5. The grand mean is 12.

Between sum of squares:

```text
5(10-12)^2 + 5(12-12)^2 + 5(14-12)^2
= 40
```

Then compare with within-group variation using the F-test.

#### 5. Exam-style case study

In April 2021, three industries each have:

```text
n = 20
means = 27%, 36%, 30%
standard deviations = 5%, 10%, 8%
```

Method:

1. calculate grand mean:

```text
(27 + 36 + 30) / 3 = 31%
```

2. calculate:

```text
SSB = 20[(27-31)^2 + (36-31)^2 + (30-31)^2]
```

3. calculate:

```text
SSW = 19(5^2) + 19(10^2) + 19(8^2)
```

4. calculate:

```text
F = MSB / MSW
```

5. compare with the F critical value with:

```text
df = 2 and 57
```

#### 6. Real-world actuarial case study

A group insurer compares resignation rates across manufacturing, IT, and consulting clients.

If ANOVA shows significant differences, industry may be a useful rating or segmentation variable for employee benefits assumptions.

#### 7. Common mistakes

- Running multiple two-sample tests instead of one ANOVA.
- Forgetting to weight group means by group sizes.
- Using standard deviation instead of variance in SSW.
- Forgetting within-group degrees of freedom are `N - k`.
- Treating ANOVA as proving which specific groups differ without follow-up analysis.

#### 8. Revision checkpoint

Without notes, you should be able to write:

```text
SSB = sum n_i(xbar_i - grand mean)^2
SSW = sum (n_i - 1)s_i^2
F = [SSB/(k-1)] / [SSW/(N-k)]
```

### Expanded deep explanation

Regression is a conditional mean model. The formula is only useful if the assumptions, residual behaviour, interpretation of coefficients, and prediction uncertainty are understood.

- Regression estimates a conditional mean, not a causal law by default. The coefficient tells how the fitted average response changes when a predictor changes, usually holding other predictors fixed.
- The residuals are as important as the fitted line. Patterns, changing spread, influential points, and non-normal residuals can reveal that the chosen model is too simple or the scale is wrong.
- Prediction requires two layers of uncertainty: uncertainty in the fitted mean and random variation of the individual outcome. This is why individual prediction intervals are wider than mean response intervals.

### Step-by-step working method

1. Plot or inspect the relationship before fitting.
2. Fit the model and write the fitted equation.
3. Interpret each coefficient in business units.
4. Check residuals, leverage, fit statistics, and uncertainty.
5. Decide whether the model is suitable for pricing, reserving, or explanation.

### Extra practical actuarial examples

- Motor pricing example: claim frequency may increase with driver age band, vehicle group, region, and prior claims, but coefficient interpretation must respect all variables included in the model.
- Life insurance example: a transformed mortality rating can produce a smooth premium formula, but the formula must be checked against underwriting judgement and table constraints.
- Exam example: if asked for an individual prediction, include the extra 1 inside the square root of the prediction standard error.

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

### Topic 108: One-Parameter Least Squares and Weighted Least Squares

#### 1. Concept theory

Sometimes a model has only one unknown parameter. For example:

\[
E(Y_i)=\gamma e^{x_i}
\]

This is not a standard straight-line regression in \(x_i\), but it is linear in the unknown parameter \(\gamma\).

Least squares chooses \(\gamma\) to minimise the sum of squared errors.

Weighted least squares gives some observations more or less influence by minimising weighted squared errors.

#### 2. Why actuaries care

Weighted least squares is common in actuarial modelling because observations may have unequal reliability:

- larger exposure cells are more credible
- claim amounts may have variance increasing with exposure
- high-volume policy groups should influence fitted rates more
- small cells should not dominate a model

#### 3. Mathematical derivation

For:

\[
Y_i=\gamma e^{x_i}+\epsilon_i
\]

ordinary least squares minimises:

\[
S(\gamma)=\sum_{i=1}^{n}(y_i-\gamma e^{x_i})^2
\]

Differentiate:

\[
\frac{dS}{d\gamma}
=\sum 2(y_i-\gamma e^{x_i})(-e^{x_i})
\]

Set equal to zero:

\[
\sum y_ie^{x_i}-\gamma\sum e^{2x_i}=0
\]

So:

\[
\hat\gamma=\frac{\sum y_ie^{x_i}}{\sum e^{2x_i}}
\]

For weighted least squares with weights \(w_i\):

\[
S_w(\gamma)=\sum w_i(y_i-\gamma e^{x_i})^2
\]

Differentiate:

\[
\frac{dS_w}{d\gamma}
=\sum 2w_i(y_i-\gamma e^{x_i})(-e^{x_i})
\]

Set equal to zero:

\[
\sum w_iy_ie^{x_i}-\gamma\sum w_ie^{2x_i}=0
\]

Thus:

\[
\hat\gamma_w=
\frac{\sum w_iy_ie^{x_i}}{\sum w_ie^{2x_i}}
\]

If:

\[
w_i=\frac{1}{x_i}
\]

then:

\[
\hat\gamma_w=
\frac{\sum \frac{y_ie^{x_i}}{x_i}}{\sum \frac{e^{2x_i}}{x_i}}
\]

#### 4. Simple example

Suppose:

| \(x_i\) | \(y_i\) | \(e^{x_i}\) |
|---:|---:|---:|
| 0 | 2 | 1 |
| 1 | 6 | 2.718 |

OLS estimate:

\[
\hat\gamma=\frac{2(1)+6(2.718)}{1^2+2.718^2}
\]

\[
=\frac{18.308}{8.389}=2.183
\]

So the fitted model is:

\[
\hat y=2.183e^x
\]

#### 5. Exam-style case study

Given the model:

\[
E(Y_i)=\gamma e^{x_i}
\]

derive the least squares estimate of \(\gamma\).

Solution:

\[
S(\gamma)=\sum(y_i-\gamma e^{x_i})^2
\]

\[
\frac{dS}{d\gamma}=-2\sum e^{x_i}(y_i-\gamma e^{x_i})
\]

Set equal to zero:

\[
\sum y_ie^{x_i}=\gamma\sum e^{2x_i}
\]

\[
\hat\gamma=\frac{\sum y_ie^{x_i}}{\sum e^{2x_i}}
\]

If weighted by \(1/x_i\):

\[
\hat\gamma=
\frac{\sum y_ie^{x_i}/x_i}{\sum e^{2x_i}/x_i}
\]

#### 6. Real-world actuarial case study

An insurer models average claim cost as increasing exponentially with vehicle age. Some age groups have many policies, while others have very few.

The actuary may use weighted least squares so that high-exposure groups receive more weight. If variance increases with \(x_i\), weights such as \(1/x_i\) may reduce the influence of high-variance observations.

#### 7. Common mistakes

- Trying to take logs even when the model is \(E(Y)=\gamma e^x\), not \(\log Y=\alpha+\beta x\).
- Forgetting to square \(e^{x_i}\) in the denominator.
- Applying weights to errors instead of squared errors.
- Using \(x_i\) instead of \(1/x_i\) when the question specifies inverse weights.
- Treating WLS as changing the model mean instead of changing the fitting criterion.

#### 8. Revision checkpoint

You should be able to:

- write the least squares objective function
- differentiate with respect to one parameter
- derive \(\hat\gamma\) for \(E(Y)=\gamma e^x\)
- derive the weighted least squares version

### Expanded deep explanation

Regression is a conditional mean model. The formula is only useful if the assumptions, residual behaviour, interpretation of coefficients, and prediction uncertainty are understood.

- Regression estimates a conditional mean, not a causal law by default. The coefficient tells how the fitted average response changes when a predictor changes, usually holding other predictors fixed.
- The residuals are as important as the fitted line. Patterns, changing spread, influential points, and non-normal residuals can reveal that the chosen model is too simple or the scale is wrong.
- Prediction requires two layers of uncertainty: uncertainty in the fitted mean and random variation of the individual outcome. This is why individual prediction intervals are wider than mean response intervals.

### Step-by-step working method

1. Plot or inspect the relationship before fitting.
2. Fit the model and write the fitted equation.
3. Interpret each coefficient in business units.
4. Check residuals, leverage, fit statistics, and uncertainty.
5. Decide whether the model is suitable for pricing, reserving, or explanation.

### Extra practical actuarial examples

- Motor pricing example: claim frequency may increase with driver age band, vehicle group, region, and prior claims, but coefficient interpretation must respect all variables included in the model.
- Life insurance example: a transformed mortality rating can produce a smooth premium formula, but the formula must be checked against underwriting judgement and table constraints.
- Exam example: if asked for an individual prediction, include the extra 1 inside the square root of the prediction standard error.

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

### Topic 109: Transformed Predictor Regression, Individual Prediction, and Mean Response

#### 1. Concept theory

Sometimes a relationship is made linear by transforming the predictor. For example, premium rate may be modelled as:

\[
Y=\alpha+\beta X+\epsilon
\]

where:

\[
X=(\text{mortality rating})^3
\]

The regression is still a simple linear regression, but the input variable is transformed before fitting.

There are two different intervals:

- confidence interval for the mean response
- prediction interval for an individual response

The individual prediction interval is wider because it includes both parameter uncertainty and individual random error.

#### 2. Why actuaries care

Actuaries frequently transform rating variables:

- age
- mortality rating
- sum assured
- duration
- vehicle value
- claim size

The fitted formula may be convenient, but it can hide practical problems such as poor fit, extrapolation, or loss of table detail.

#### 3. Mathematical derivation

For simple linear regression:

\[
Y_i=\alpha+\beta x_i+\epsilon_i
\]

where:

\[
\epsilon_i\sim N(0,\sigma^2)
\]

The slope estimate is:

\[
\hat\beta=\frac{\sum(x_i-\bar x)(y_i-\bar y)}{\sum(x_i-\bar x)^2}
\]

The intercept estimate is:

\[
\hat\alpha=\bar y-\hat\beta\bar x
\]

The fitted mean at \(x_0\) is:

\[
\hat y_0=\hat\alpha+\hat\beta x_0
\]

The residual standard error is:

\[
s=\sqrt{\frac{SSE}{n-2}}
\]

Mean response confidence interval:

\[
\hat y_0
\pm t_{n-2,\alpha/2}s
\sqrt{
\frac{1}{n}
+
\frac{(x_0-\bar x)^2}{S_{xx}}
}
\]

Individual response prediction interval:

\[
\hat y_0
\pm t_{n-2,\alpha/2}s
\sqrt{
1+
\frac{1}{n}
+
\frac{(x_0-\bar x)^2}{S_{xx}}
}
\]

where:

\[
S_{xx}=\sum(x_i-\bar x)^2
\]

#### 4. Simple example

Suppose:

\[
\hat y=5+2x,\quad s=3,\quad n=10,\quad \bar x=4,\quad S_{xx}=40
\]

Find intervals at:

\[
x_0=6
\]

Fitted value:

\[
\hat y_0=5+2(6)=17
\]

Mean response standard error:

\[
3\sqrt{\frac{1}{10}+\frac{(6-4)^2}{40}}
\]

\[
=3\sqrt{0.1+0.1}=1.342
\]

Individual response standard error:

\[
3\sqrt{1+\frac{1}{10}+\frac{(6-4)^2}{40}}
\]

\[
=3\sqrt{1.2}=3.286
\]

The prediction interval is wider.

#### 5. Exam-style case study

Premium rates are fitted against:

\[
X=(\text{mortality rating})^3
\]

using:

\[
Y=\alpha+\beta X+\epsilon
\]

Given:

\[
\sum x=174.13,\quad \sum y=197.84
\]

\[
\sum x^2=7545.90,\quad \sum(x-\bar x)(y-\bar y)=2176.84
\]

and \(n=11\).

First:

\[
\bar x=\frac{174.13}{11},\quad \bar y=\frac{197.84}{11}
\]

\[
S_{xx}=\sum x^2-\frac{(\sum x)^2}{n}
\]

\[
\hat\beta=\frac{2176.84}{S_{xx}}
\]

\[
\hat\alpha=\bar y-\hat\beta\bar x
\]

Then the regression equation is:

\[
\hat y=\hat\alpha+\hat\beta x
\]

To test no linear relationship:

\[
H_0:\beta=0
\]

Use:

\[
t=\frac{\hat\beta}{SE(\hat\beta)}
\]

or the equivalent \(F=t^2\) test.

At \(x_0=25\), calculate:

\[
\hat y_0=\hat\alpha+\hat\beta(25)
\]

Then use the mean response and individual response interval formulas. The individual interval must be wider.

#### 6. Real-world actuarial case study

A life insurer wants a quick formula to convert mortality rating into a premium rate. A cubic transformation gives a smooth formula, but the original table may contain underwriting judgement, rounding, and product constraints.

If residuals show systematic patterns, the formula may be convenient but not faithful. Using the fitted equation instead of the full premium table can lead to underpricing in some rating bands and overpricing in others.

#### 7. Common mistakes

- Forgetting that the regression is on transformed \(X\), not the raw mortality rating.
- Using the mean response interval when the question asks for an individual response.
- Forgetting the extra \(1\) inside the square root for an individual prediction interval.
- Testing correlation but not stating regression assumptions.
- Trusting a fitted formula even when residuals show a pattern.
- Extrapolating beyond the observed mortality ratings.

#### 8. Revision checkpoint

You should be able to:

- fit simple linear regression using summary statistics
- test \(H_0:\beta=0\)
- calculate and interpret sample correlation
- distinguish mean response CI from individual prediction interval
- comment on residual plots and limitations of replacing a table with a formula

### Expanded deep explanation

Regression is a conditional mean model. The formula is only useful if the assumptions, residual behaviour, interpretation of coefficients, and prediction uncertainty are understood.

- Regression estimates a conditional mean, not a causal law by default. The coefficient tells how the fitted average response changes when a predictor changes, usually holding other predictors fixed.
- The residuals are as important as the fitted line. Patterns, changing spread, influential points, and non-normal residuals can reveal that the chosen model is too simple or the scale is wrong.
- Prediction requires two layers of uncertainty: uncertainty in the fitted mean and random variation of the individual outcome. This is why individual prediction intervals are wider than mean response intervals.

### Step-by-step working method

1. Plot or inspect the relationship before fitting.
2. Fit the model and write the fitted equation.
3. Interpret each coefficient in business units.
4. Check residuals, leverage, fit statistics, and uncertainty.
5. Decide whether the model is suitable for pricing, reserving, or explanation.

### Extra practical actuarial examples

- Motor pricing example: claim frequency may increase with driver age band, vehicle group, region, and prior claims, but coefficient interpretation must respect all variables included in the model.
- Life insurance example: a transformed mortality rating can produce a smooth premium formula, but the formula must be checked against underwriting judgement and table constraints.
- Exam example: if asked for an individual prediction, include the extra 1 inside the square root of the prediction standard error.

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

## Master Chapter 8: Generalised Linear Models and Exponential Family

GLMs extend regression to non-normal responses. Identify the random component, systematic component, link function, variance function, likelihood, and deviance before interpreting coefficients.

### Topics in this master chapter

- Topic 110: Generalised Linear Models
- Topic 111: GLM Scaled Deviance and Nested Model Selection
- Topic 112: GLM Prediction with Log Link, Interactions, and AIC
- Topic 113: Two-Group Poisson GLM, Deviance, AIC, and Equivalent Parameterisations
- Topic 114: Poisson GLM with Age, Gender, and Interaction

### Topic 110: Generalised Linear Models

#### 1. Concept theory

A GLM extends linear regression to non-normal data.

It has three parts:

```text
1. Random component: distribution of Y
2. Systematic component: eta = X beta
3. Link function: g(mu) = eta
```

Common actuarial GLMs:

```text
claim count      -> Poisson
claim severity   -> Gamma
fraud indicator  -> Binomial
continuous score -> Normal
```

#### 2. Why actuaries care

GLMs are central in insurance pricing. They allow risk classification by rating factors such as:

```text
age
vehicle type
location
occupation
claim history
policy duration
```

#### 3. Mathematical derivation

The exponential family form is:

```text
f(y; theta, phi) = exp{[y theta - b(theta)] / a(phi) + c(y, phi)}
```

Mean and variance:

```text
E[Y] = b'(theta)
Var(Y) = b''(theta) a(phi)
```

For Bernoulli:

```text
P(Y=y) = p^y(1-p)^(1-y)
```

Rewrite:

```text
P(Y=y) = exp[y log(p/(1-p)) + log(1-p)]
```

Since:

```text
theta = log(p/(1-p))
p = e^theta / (1 + e^theta)
```

and:

```text
log(1-p) = -log(1 + e^theta)
```

So:

```text
b(theta) = log(1 + e^theta)
```

The canonical link is:

```text
logit(p) = log(p/(1-p))
```

#### 4. Simple example

Canonical links:

```text
Poisson  -> log
Gamma    -> inverse
Binomial -> logit
Normal   -> identity
```

If a Gamma GLM uses log link:

```text
log(mu) = eta
mu = exp(eta)
```

A coefficient of `0.4` multiplies the expected response by:

```text
exp(0.4) = 1.492
```

#### 5. Exam-style case study

For factor variables:

```text
marital status has 4 levels -> 3 dummy parameters
employment status has 5 levels -> 4 dummy parameters
```

With a base level, the intercept captures the base category.

For interactions:

```text
A:B = interaction only
A*B = A + B + A:B
```

#### 6. Real-world actuarial case study

In motor pricing, location and vehicle type may interact. A sports car in a rural area may not have the same relative risk as a sports car in a congested city. Interaction terms capture this combined effect.

#### 7. Common mistakes

- Confusing chosen link with canonical link.
- Counting base levels as extra parameters.
- Forgetting that `A*B` includes main effects.
- Using identity link for probabilities, which can predict values below 0 or above 1.

#### 8. Revision checkpoint

For any GLM question, write:

```text
distribution
mean
link
linear predictor
parameters
interpretation
```

### Expanded deep explanation

GLMs extend regression to non-normal responses. Identify the random component, systematic component, link function, variance function, likelihood, and deviance before interpreting coefficients.

- Begin by checking support. A count model lives on non-negative integers, a severity model often lives on positive real values, and a transformed model may live on the whole real line after taking logs. Many exam errors happen before calculation because the wrong support is chosen.
- Separate the probability law from its moments. The probability law tells you every probability statement; the mean and variance are summaries. In actuarial work a model with the correct mean but the wrong tail can still be dangerous because solvency questions are tail questions.
- Use generating functions when sums are involved. MGFs, CGFs, and PGFs turn convolutions into algebra, but only under the conditions where they exist and where independence assumptions are valid.

### Step-by-step working method

1. State the random variable and its support.
2. Write the probability mass function, density, or distribution function.
3. Identify parameters and their actuarial meaning.
4. Calculate the required moment, probability, percentile, or transform.
5. Check whether the answer is sensible using units, range, and limiting cases.

### Extra practical actuarial examples

- Claim count example: if a motor portfolio has many zero-claim policies and a few multi-claim policies, compare Poisson and negative binomial assumptions by checking whether sample variance is close to, below, or above the sample mean.
- Severity example: for small routine medical claims a lognormal or gamma model may be adequate; for catastrophe-like property claims a heavier tail such as Pareto or Weibull may be needed.
- Exam example: after finding a probability, ask whether the question wanted an unconditional probability or a conditional probability after observing a claim, survival, or selection event.

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

### Topic 111: GLM Scaled Deviance and Nested Model Selection

#### 1. Concept theory

In GLMs, deviance measures lack of fit. Lower deviance generally means better fit.

For nested models, compare the reduction in scaled deviance:

```text
test statistic = deviance of simpler model - deviance of complex model
```

If this reduction is large relative to a chi-square critical value, the more complex model is justified.

#### 2. Why actuaries care

Pricing models often compare:

```text
intercept-only model
main effects model
main effects plus rating factors
main effects plus interactions
```

The actuary wants enough complexity to explain risk, but not unnecessary complexity.

#### 3. Mathematical derivation

For nested models:

```text
D_simple - D_complex approximately Chi-square(df difference)
```

where:

```text
df difference = number of extra parameters in complex model
```

Decision:

```text
if test statistic > critical value:
    choose complex model
else:
    retain simpler model
```

#### 4. Simple example

Models:

```text
A: intercept only, deviance 220
B: temperature, deviance 180
```

Test statistic:

```text
220 - 180 = 40
```

If temperature has 3 categories, adding it uses:

```text
3 - 1 = 2 parameters
```

Critical value at 5%:

```text
chi-square with 2 df = 5.991
```

Since:

```text
40 > 5.991
```

Model B is better.

#### 5. Exam-style case study

For:

```text
B deviance = 180
C deviance = 130
```

test statistic:

```text
180 - 130 = 50
```

If the relevant critical value is 5.991 and:

```text
50 > 5.991
```

choose Model C.

For:

```text
C deviance = 130
D deviance = 125
```

test statistic:

```text
130 - 125 = 5
```

If the critical value is 9.488:

```text
5 < 9.488
```

retain Model C. The interaction does not add enough value.

#### 6. Real-world actuarial case study

A delivery-delay insurer or logistics risk model may include temperature and traffic. Main effects may be useful, but their interaction should only be included if it materially improves fit.

In insurance pricing, this prevents adding unstable interaction terms that look clever but do not earn their keep.

#### 7. Common mistakes

- Choosing the model with the lowest deviance without checking significance.
- Using the wrong degrees of freedom for factor variables.
- Forgetting that interaction terms add several parameters.

#### 8. Revision checkpoint

For every deviance comparison, write:

```text
deviance drop
extra parameters
critical value
model choice
```

### Expanded deep explanation

GLMs extend regression to non-normal responses. Identify the random component, systematic component, link function, variance function, likelihood, and deviance before interpreting coefficients.

- A GLM has three parts: a random component for the response distribution, a systematic component for the linear predictor, and a link function connecting the mean to the linear predictor.
- The link function determines coefficient interpretation. With a log link, a one-unit increase multiplies the mean by an exponential factor; with a logit link, a coefficient changes log-odds.
- Deviance compares fitted likelihoods. A smaller deviance can mean better fit, but model selection must balance fit, complexity, interpretability, and out-of-sample stability.

### Step-by-step working method

1. Choose the response distribution based on the data type.
2. Choose the link function and write the linear predictor.
3. Include exposure or offset terms where required.
4. Fit the model and interpret coefficients on the correct scale.
5. Use residuals, deviance, AIC, and validation to judge suitability.

### Extra practical actuarial examples

- Frequency example: Poisson GLM with log exposure offset estimates claim frequency per unit exposure.
- Binary example: a logit model can estimate probability of fraud, lapse, or claim occurrence.
- Severity example: gamma GLM with log link is often used for positive skewed claim amounts.

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

### Topic 112: GLM Prediction with Log Link, Interactions, and AIC

#### 1. Concept theory

For a GLM with log link:

```text
log(mu) = eta
```

so:

```text
mu = exp(eta)
```

Categorical variables are handled using base levels and coefficients for non-base levels.

Interaction models allow each combination of categories to have its own effect.

AIC compares models while penalising complexity:

```text
AIC = measure of fit + complexity penalty
```

Lower AIC is better.

#### 2. Why actuaries care

In health insurance, claim size is positive and skewed, so Gamma GLMs with log links are common.

Rating factors may include:

```text
gender
hospital type
region
age band
treatment type
```

The log link makes predictions positive and coefficients interpretable as multipliers.

#### 3. Mathematical derivation

For Model 1:

```text
log(mu) = beta_0 + gender effect + hospital effect
```

Prediction:

```text
mu_hat = exp(beta_0 + relevant coefficients)
```

If female and private hospital are base levels:

```text
female coefficient = 0
private coefficient = 0
```

For a female in a public hospital:

```text
eta = beta_0 + public coefficient
```

For interaction-only Model 2:

```text
log(mu) = alpha_0 + relevant interaction coefficient
```

If base interaction is female-private:

```text
alpha_0 = log(predicted claim for female-private)
```

For female-public:

```text
alpha_1 = log(predicted female-public claim) - alpha_0
```

#### 4. Simple example

Suppose:

```text
beta_0 = 3.25
public coefficient = -0.72
```

For a female in public hospital:

```text
eta = 3.25 - 0.72 = 2.53
mu = exp(2.53) = 12.55
```

If claim size is in lakhs:

```text
predicted claim size = INR 12.55 lakhs
```

#### 5. Exam-style case study

If Model 2 predicts INR 24.5537 lakhs for the base group female-private:

```text
alpha_0 = log(24.5537) = 3.20086
```

If it predicts INR 15.0813 lakhs for female-public:

```text
alpha_1 = log(15.0813) - alpha_0
        = -0.4874
```

Model 2 is better than Model 1 if:

```text
AIC(Model 2) < AIC(Model 1)
```

#### 6. Real-world actuarial case study

A health insurer may find that public hospitals have lower average claim sizes, but the difference may vary by gender, age, or treatment. Interaction models help capture these combination effects.

However, every interaction increases complexity. AIC helps decide whether the extra detail improves the model enough.

#### 7. Common mistakes

- Forgetting to exponentiate after using a log link.
- Adding base-level coefficients that are actually zero.
- Treating lower AIC as worse.
- Confusing main effects with interaction-only models.

#### 8. Revision checkpoint

You should be able to:

```text
build eta from coefficients
convert eta to mu using exp(eta)
recover a coefficient from a predicted value
choose lower AIC
```

### Expanded deep explanation

GLMs extend regression to non-normal responses. Identify the random component, systematic component, link function, variance function, likelihood, and deviance before interpreting coefficients.

- Regression estimates a conditional mean, not a causal law by default. The coefficient tells how the fitted average response changes when a predictor changes, usually holding other predictors fixed.
- The residuals are as important as the fitted line. Patterns, changing spread, influential points, and non-normal residuals can reveal that the chosen model is too simple or the scale is wrong.
- Prediction requires two layers of uncertainty: uncertainty in the fitted mean and random variation of the individual outcome. This is why individual prediction intervals are wider than mean response intervals.

### Step-by-step working method

1. Plot or inspect the relationship before fitting.
2. Fit the model and write the fitted equation.
3. Interpret each coefficient in business units.
4. Check residuals, leverage, fit statistics, and uncertainty.
5. Decide whether the model is suitable for pricing, reserving, or explanation.

### Extra practical actuarial examples

- Motor pricing example: claim frequency may increase with driver age band, vehicle group, region, and prior claims, but coefficient interpretation must respect all variables included in the model.
- Life insurance example: a transformed mortality rating can produce a smooth premium formula, but the formula must be checked against underwriting judgement and table constraints.
- Exam example: if asked for an individual prediction, include the extra 1 inside the square root of the prediction standard error.

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

### Topic 113: Two-Group Poisson GLM, Deviance, AIC, and Equivalent Parameterisations

#### 1. Concept theory

A Poisson GLM is used for count data. With a log link:

```text
log(mu_i) = linear predictor
```

For two groups, the model can either use an intercept plus a group indicator or separate parameters for each group. These can be equivalent ways of describing the same fitted means.

#### 2. Why actuaries care

Actuaries often compare claim frequencies between regions, cities, products, or customer groups.

Poisson GLMs help estimate group-level claim frequency and test whether group membership improves the model.

#### 3. Mathematical derivation

For Poisson observations:

```text
Yi ~ Poisson(mu_i)
```

The probability is:

```text
P(Yi = yi) = exp(-mu_i) mu_i^yi / yi!
```

This can be written in exponential family form:

```text
exp[yi log(mu_i) - mu_i - log(yi!)]
```

So the natural parameter is:

```text
theta_i = log(mu_i)
```

For Model 1:

```text
log(mu_i) = alpha
```

All observations have the same mean:

```text
mu_hat = total y / n
alpha_hat = log(mu_hat)
```

For Model 2 with a city indicator:

```text
log(mu_i) = alpha + beta x_i
```

where:

```text
x_i = 1 for City I
x_i = 0 for City II
```

Then:

```text
City II mean = exp(alpha)
City I mean = exp(alpha + beta)
```

So:

```text
alpha_hat = log(mean of City II)
beta_hat = log(mean of City I) - log(mean of City II)
```

Poisson deviance:

```text
D = 2 sum [y_i log(y_i / mu_hat_i) - (y_i - mu_hat_i)]
```

with convention:

```text
y log(y) = 0 when y = 0
```

AIC:

```text
AIC = -2l + 2p
```

#### 4. Simple example

Suppose:

```text
City I counts: 1, 2, 0
City II counts: 0, 1, 1
```

Means:

```text
City I mean = 3 / 3 = 1
City II mean = 2 / 3
```

Model 2 estimates:

```text
alpha_hat = log(2/3)
beta_hat = log(1) - log(2/3)
```

#### 5. Exam-style case study

In the December 2022 bus cancellation question, Model 2 is:

```text
log(mu_i) = alpha + beta x_i
```

where `x_i = 1` for City I and `0` for City II.

The method is:

1. calculate the sample mean for City I
2. calculate the sample mean for City II
3. set fitted means equal to group means
4. solve for `alpha` and `beta`

For probability of exactly 3 cancellations:

```text
P(Y = 3) = exp(-mu) mu^3 / 3!
```

using the fitted `mu` for the required city.

Model 3:

```text
log(mu_i) = delta for City I
log(mu_i) = gamma for City II
```

is equivalent to Model 2, because:

```text
delta = alpha + beta
gamma = alpha
```

#### 6. Real-world actuarial case study

A transport insurer compares accident counts in two cities. A one-mean model assumes both cities have the same claim frequency. A two-group model allows each city to have its own frequency.

If the deviance reduction is material, the city variable improves the model. If not, the simpler model may be preferred.

#### 7. Common mistakes

- Using a normal model for count data without checking suitability.
- Forgetting the log link means `mu = exp(eta)`.
- Treating Model 2 and Model 3 as fundamentally different when they can be equivalent.
- Forgetting `y log(y) = 0` when `y = 0` in deviance calculations.
- Comparing models only by deviance without considering parameter count.

#### 8. Revision checkpoint

Without notes, you should be able to write:

```text
alpha_hat = log(overall mean) for one-mean Poisson model
alpha_hat = log(mean of baseline group)
beta_hat = log(mean group 1) - log(mean baseline group)
D = 2 sum [y log(y / mu_hat) - (y - mu_hat)]
AIC = -2l + 2p
```

### Expanded deep explanation

GLMs extend regression to non-normal responses. Identify the random component, systematic component, link function, variance function, likelihood, and deviance before interpreting coefficients.

- A GLM has three parts: a random component for the response distribution, a systematic component for the linear predictor, and a link function connecting the mean to the linear predictor.
- The link function determines coefficient interpretation. With a log link, a one-unit increase multiplies the mean by an exponential factor; with a logit link, a coefficient changes log-odds.
- Deviance compares fitted likelihoods. A smaller deviance can mean better fit, but model selection must balance fit, complexity, interpretability, and out-of-sample stability.

### Step-by-step working method

1. Choose the response distribution based on the data type.
2. Choose the link function and write the linear predictor.
3. Include exposure or offset terms where required.
4. Fit the model and interpret coefficients on the correct scale.
5. Use residuals, deviance, AIC, and validation to judge suitability.

### Extra practical actuarial examples

- Frequency example: Poisson GLM with log exposure offset estimates claim frequency per unit exposure.
- Binary example: a logit model can estimate probability of fraud, lapse, or claim occurrence.
- Severity example: gamma GLM with log link is often used for positive skewed claim amounts.

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

### Topic 114: Poisson GLM with Age, Gender, and Interaction

#### 1. Concept theory

A Poisson GLM is used for count data. The usual link is the log link:

```text
log(mu) = eta
```

When a model includes age and gender, gender can affect the intercept, the slope, or both.

#### 2. Why actuaries care

Claim counts often depend on age, gender, region, policy type, and other rating factors.

Understanding interaction terms is crucial because the effect of age may differ by gender.

#### 3. Mathematical derivation

For a Poisson GLM:

```text
Y_i ~ Poisson(mu_i)
log(mu_i) = eta_i
```

With age only:

```text
eta_i = alpha + beta age_i
```

With age plus gender as main effects:

```text
eta_i = alpha_gender + beta age_i
```

This means different genders have different intercepts but the same age slope.

With age, gender, and interaction:

```text
eta_i = alpha_gender + beta_gender age_i
```

This means both intercept and age slope can differ by gender.

#### 4. Simple example

Without interaction:

```text
Male:   log(mu) = alpha_M + beta age
Female: log(mu) = alpha_F + beta age
```

The lines are parallel on the log scale.

With interaction:

```text
Male:   log(mu) = alpha_M + beta_M age
Female: log(mu) = alpha_F + beta_F age
```

The lines can have different slopes.

#### 5. Exam-style case study

In April 2021:

For:

```text
Age + Gender
```

write:

```text
eta = alpha_i + beta x
```

where:

```text
alpha_i depends on gender
beta is common across genders
```

For:

```text
Age + Gender + Age.Gender
```

write:

```text
eta = alpha_i + beta_i x
```

where both:

```text
alpha_i and beta_i depend on gender
```

#### 6. Real-world actuarial case study

A health insurer models claim counts by age and gender. Without interaction, the model assumes male and female claim rates rise with age at the same proportional rate.

With interaction, the model allows claim rates to rise faster with age for one gender than the other.

#### 7. Common mistakes

- Forgetting Poisson GLM normally uses log link.
- Treating interaction as only a new intercept.
- Failing to explain what changes when interaction is added.
- Writing a linear predictor for claim amount rather than claim count.
- Forgetting categorical variables require group-specific parameters.

#### 8. Revision checkpoint

Without notes, you should be able to write:

```text
Age + Gender: eta = alpha_gender + beta age
Age + Gender + Age.Gender: eta = alpha_gender + beta_gender age
log(mu) = eta
```

### Expanded deep explanation

GLMs extend regression to non-normal responses. Identify the random component, systematic component, link function, variance function, likelihood, and deviance before interpreting coefficients.

- A GLM has three parts: a random component for the response distribution, a systematic component for the linear predictor, and a link function connecting the mean to the linear predictor.
- The link function determines coefficient interpretation. With a log link, a one-unit increase multiplies the mean by an exponential factor; with a logit link, a coefficient changes log-odds.
- Deviance compares fitted likelihoods. A smaller deviance can mean better fit, but model selection must balance fit, complexity, interpretability, and out-of-sample stability.

### Step-by-step working method

1. Choose the response distribution based on the data type.
2. Choose the link function and write the linear predictor.
3. Include exposure or offset terms where required.
4. Fit the model and interpret coefficients on the correct scale.
5. Use residuals, deviance, AIC, and validation to judge suitability.

### Extra practical actuarial examples

- Frequency example: Poisson GLM with log exposure offset estimates claim frequency per unit exposure.
- Binary example: a logit model can estimate probability of fraud, lapse, or claim occurrence.
- Severity example: gamma GLM with log link is often used for positive skewed claim amounts.

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

## Master Chapter 9: Bayesian Statistics and Credibility

Bayesian and credibility methods update prior belief with experience. The actuarial skill is to recognise what information is prior, what information is data, and how the posterior should affect decisions.

### Topics in this master chapter

- Topic 115: Bayesian Statistics and Credibility
- Topic 116: Empirical Bayes Credibility Model 1
- Topic 117: Empirical Bayes Credibility Model 1: Full Calculation
- Topic 118: Empirical Bayes Credibility Model 2
- Topic 119: Discrete Bayesian Prior and Posterior Odds
- Topic 120: Recovering Beta Prior Parameters from Mean and Variance
- Topic 121: EBCT Model 1 with Inflation Trend and Portfolio Changes
- Topic 122: Bayes Theorem for Source Probabilities and Loss Function Estimates

### Topic 115: Bayesian Statistics and Credibility

#### 1. Concept theory

Bayesian statistics updates prior belief using data.

```text
posterior proportional to likelihood * prior
```

The prior represents belief before data. The likelihood represents observed data. The posterior is the updated belief.

#### 2. Why actuaries care

Actuaries often work with limited data:

```text
new product
small insurer
new disease cover
rare cyber risk
emerging market
```

Bayesian methods allow a blend of prior expert judgement and observed experience.

#### 3. Mathematical derivation

Beta-Binomial model:

```text
theta ~ Beta(alpha, beta)
X | theta ~ Binomial(n, theta)
```

Likelihood:

```text
L(theta) proportional to theta^x (1-theta)^(n-x)
```

Prior:

```text
pi(theta) proportional to theta^(alpha-1)(1-theta)^(beta-1)
```

Posterior:

```text
pi(theta | x) proportional to theta^(alpha+x-1)(1-theta)^(beta+n-x-1)
```

Therefore:

```text
theta | x ~ Beta(alpha + x, beta + n - x)
```

Gamma-Exponential model:

```text
t_i | lambda ~ Exponential(lambda)
lambda ~ Gamma(alpha, beta)
```

Likelihood:

```text
L(lambda) = lambda^n exp(-lambda sum t_i)
```

Prior:

```text
pi(lambda) proportional to lambda^(alpha-1) exp(-beta lambda)
```

Posterior:

```text
lambda | data ~ Gamma(alpha+n, beta+sum t_i)
```

#### 4. Simple example

Prior:

```text
p ~ Beta(2,5)
```

Data:

```text
n = 100
x = 60
```

Posterior:

```text
p | data ~ Beta(2+60, 5+40)
          = Beta(62,45)
```

Posterior mean:

```text
62 / (62 + 45) = 0.5794
```

#### 5. Exam-style case study

For credibility:

```text
Z = n / (n + alpha + beta)
```

If:

```text
alpha = 2
beta = 4
n = 12
```

then:

```text
Z = 12 / (12 + 6) = 0.6667
```

#### 6. Real-world actuarial case study

A new cyber product has only one year of internal data. The actuary uses industry cyber claims as the prior and company experience as the likelihood. The posterior assumption is more stable than using either source alone.

#### 7. Common mistakes

- Mixing Gamma rate and scale parameterisations.
- Forgetting to add failures to the second Beta parameter.
- Thinking the prior disappears after small data.
- Using posterior mode when the loss function asks for posterior mean.

#### 8. Revision checkpoint

Know these pairs:

```text
Binomial likelihood + Beta prior -> Beta posterior
Exponential likelihood + Gamma prior -> Gamma posterior
Poisson likelihood + Gamma prior -> Gamma posterior
```

### Expanded deep explanation

Bayesian and credibility methods update prior belief with experience. The actuarial skill is to recognise what information is prior, what information is data, and how the posterior should affect decisions.

- Bayesian analysis combines prior information and data through the likelihood. The posterior distribution is proportional to prior times likelihood.
- Credibility theory has the same actuarial instinct: blend individual experience with collective experience. The more stable and relevant the individual data, the more credibility it receives.
- Conjugate priors are useful because the posterior stays in the same family. This makes updating transparent and exam calculations faster.

### Step-by-step working method

1. Write the prior distribution and identify its parameters.
2. Write the likelihood from the observed data.
3. Multiply prior and likelihood, keeping parameter-dependent terms.
4. Recognise the posterior family or normalise if required.
5. Use the posterior mean, mode, interval, or credibility form for the decision.

### Extra practical actuarial examples

- Renewal example: a beta prior for renewal probability can be updated after observing renewals and non-renewals.
- Claim count example: a gamma prior for a Poisson rate leads to a gamma posterior and a credibility-style blended estimate.
- Pricing example: a small employer group should not be priced only from its own volatile claims experience; credibility blends group experience with portfolio experience.

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

### Topic 116: Empirical Bayes Credibility Model 1

#### 1. Concept theory

Credibility theory blends:

```text
individual risk experience
collective portfolio experience
```

The credibility estimate usually has the form:

```text
Credibility estimate = Z * individual mean + (1-Z) * collective mean
```

where:

```text
Z = credibility factor
```

`Z` is higher when individual experience is more reliable.

#### 2. Why actuaries care

Small portfolios are noisy. If one small client has a bad year, we should not always assume they are permanently high risk.

Credibility helps decide how much weight to give to:

```text
their own past claims
the wider portfolio average
```

#### 3. Mathematical derivation

In a simple empirical Bayes framework:

```text
estimate for risk i = Z * own average for risk i + (1-Z) * overall average
```

For a Beta-Binomial credibility model with `m` independent policies per month over `n` months:

```text
number of trials = mn
```

If the prior is:

```text
theta ~ Beta(alpha, beta)
```

and total claims observed are `x`, then:

```text
theta | x ~ Beta(alpha + x, beta + mn - x)
```

For the May 2025 question with 3 policies over `n` months:

```text
mn = 3n
theta | x ~ Beta(alpha + x, beta + 3n - x)
```

The posterior mean under squared error loss is:

```text
E[theta | x] = (alpha + x) / (alpha + beta + 3n)
```

Credibility factor:

```text
Z = 3n / (3n + alpha + beta)
```

#### 4. Simple example

Let:

```text
alpha = 2
beta = 4
n = 12
```

There are 3 policies each month, so:

```text
number of trials = 3n = 36
```

Credibility:

```text
Z = 36 / (36 + 2 + 4)
  = 36 / 42
  = 0.8571
```

#### 5. Exam-style case study

If total claims are:

```text
x = 9
```

then posterior mean is:

```text
(2 + 9) / (2 + 4 + 36)
= 11 / 42
= 0.2619
```

This is the Bayesian estimate of monthly claim probability per policy under squared error loss.

#### 6. Real-world actuarial case study

A device insurer has three categories:

```text
high-end laptops
smartphones
other portable electronics
```

Each category has its own claim history, but the company also has portfolio-wide experience. Empirical Bayes credibility produces Year 6 expected claims by blending each category's own average with the overall average.

#### 7. Common mistakes

- Forgetting that 3 policies over `n` months gives `3n` trials.
- Using `n` instead of total exposure.
- Thinking credibility is always 100% once any data exists.
- Confusing Bayesian posterior mean with raw sample mean.

#### 8. Revision checkpoint

Ask:

```text
How much individual data do I have?
How strong is the prior/collective experience?
What is Z?
```

### Expanded deep explanation

Bayesian and credibility methods update prior belief with experience. The actuarial skill is to recognise what information is prior, what information is data, and how the posterior should affect decisions.

- Bayesian analysis combines prior information and data through the likelihood. The posterior distribution is proportional to prior times likelihood.
- Credibility theory has the same actuarial instinct: blend individual experience with collective experience. The more stable and relevant the individual data, the more credibility it receives.
- Conjugate priors are useful because the posterior stays in the same family. This makes updating transparent and exam calculations faster.

### Step-by-step working method

1. Write the prior distribution and identify its parameters.
2. Write the likelihood from the observed data.
3. Multiply prior and likelihood, keeping parameter-dependent terms.
4. Recognise the posterior family or normalise if required.
5. Use the posterior mean, mode, interval, or credibility form for the decision.

### Extra practical actuarial examples

- Renewal example: a beta prior for renewal probability can be updated after observing renewals and non-renewals.
- Claim count example: a gamma prior for a Poisson rate leads to a gamma posterior and a credibility-style blended estimate.
- Pricing example: a small employer group should not be priced only from its own volatile claims experience; credibility blends group experience with portfolio experience.

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

### Topic 117: Empirical Bayes Credibility Model 1: Full Calculation

#### 1. Concept theory

Empirical Bayes Credibility Model 1 estimates future experience for each risk group by blending:

```text
individual group mean
overall portfolio mean
```

The estimate is:

```text
mu_hat_i = Z * x_bar_i + (1 - Z) * x_bar
```

where:

```text
x_bar_i = mean for group i
x_bar = overall mean
Z = credibility factor
```

#### 2. Why actuaries care

Some risk groups have limited data. Their own experience is useful, but it may be noisy.

Credibility theory answers:

```text
How much should we trust this group's own past experience?
How much should we pull it toward the portfolio average?
```

#### 3. Mathematical derivation

Let:

```text
I = number of risk groups
n = number of years per group
x_bar_i = average for group i
x_bar = overall average
```

Estimate the between-group variance:

```text
B = [1 / (I - 1)] * sum n(x_bar_i - x_bar)^2
```

Estimate the within-group variance:

```text
S^2 = average of within-group sums of squares divided by total within-group df
```

For each group:

```text
within SS_i = sum x_ij^2 - (sum x_ij)^2 / n
```

Then:

```text
S^2 = sum within SS_i / [I(n - 1)]
```

Credibility factor:

```text
Z = n / [n + S^2/B]
```

Final estimate:

```text
mu_hat_i = Z x_bar_i + (1-Z)x_bar
```

#### 4. Simple example

Suppose:

```text
n = 5
overall mean = 43.07
Category 1 mean = 47.8
Z = 0.9797
```

Then:

```text
mu_hat_1 = 0.9797(47.8) + 0.0203(43.07)
         = 47.57
```

Because `Z` is close to 1, the category's own experience receives most of the weight.

#### 5. Exam-style case study

For three device categories:

```text
Category 1 mean = 239/5 = 47.8
Category 2 mean = 268/5 = 53.6
Category 3 mean = 139/5 = 27.8
Overall mean = 646/15 = 43.07
```

Given:

```text
B = 915.225
S^2 = 94.53
n = 5
```

Credibility:

```text
Z = 5 / [5 + 94.53/915.225]
  = 0.9797
```

Year 6 expected claims:

```text
Category 1 = 0.9797(47.8) + 0.0203(43.07) = 47.57
Category 2 = 0.9797(53.6) + 0.0203(43.07) = 53.12
Category 3 = 0.9797(27.8) + 0.0203(43.07) = 28.50
```

#### 6. Real-world actuarial case study

A tech insurance company covers laptops, smartphones, and tablets. Each device class has different claim behaviour. If each class has several years of stable data, credibility will be high and pricing can rely heavily on class-specific experience.

If a class has very little data, its price should be pulled closer to the overall portfolio average.

#### 7. Common mistakes

- Forgetting to divide within-group sums of squares by total within-group degrees of freedom.
- Using total sums instead of group means in the final credibility formula.
- Thinking `Z` is chosen subjectively. In empirical Bayes, it is estimated from data.
- Ignoring the overall mean in the final estimate.

#### 8. Revision checkpoint

You should be able to compute:

```text
group means
overall mean
within-group variance S^2
between-group variance B
credibility factor Z
credibility estimate for each group
```

### Expanded deep explanation

Bayesian and credibility methods update prior belief with experience. The actuarial skill is to recognise what information is prior, what information is data, and how the posterior should affect decisions.

- Bayesian analysis combines prior information and data through the likelihood. The posterior distribution is proportional to prior times likelihood.
- Credibility theory has the same actuarial instinct: blend individual experience with collective experience. The more stable and relevant the individual data, the more credibility it receives.
- Conjugate priors are useful because the posterior stays in the same family. This makes updating transparent and exam calculations faster.

### Step-by-step working method

1. Write the prior distribution and identify its parameters.
2. Write the likelihood from the observed data.
3. Multiply prior and likelihood, keeping parameter-dependent terms.
4. Recognise the posterior family or normalise if required.
5. Use the posterior mean, mode, interval, or credibility form for the decision.

### Extra practical actuarial examples

- Renewal example: a beta prior for renewal probability can be updated after observing renewals and non-renewals.
- Claim count example: a gamma prior for a Poisson rate leads to a gamma posterior and a credibility-style blended estimate.
- Pricing example: a small employer group should not be priced only from its own volatile claims experience; credibility blends group experience with portfolio experience.

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

### Topic 118: Empirical Bayes Credibility Model 2

#### 1. Concept theory

Empirical Bayes Credibility Theory Model 2 is used when each risk group has different exposure.

Model 1 treats each observation equally. Model 2 gives more credibility to groups with larger exposure.

The credibility estimate has the familiar form:

```text
Credibility estimate = Z * individual experience + (1-Z) * collective experience
```

but now:

```text
Z_i = P_i / (P_i + a)
```

where:

```text
P_i = exposure for risk group i
a = E[s^2(theta)] / Var[m(theta)]
```

#### 2. Why actuaries care

In insurance and finance, different groups often have different exposure volumes.

Examples:

```text
different loan types have different numbers of loans
different policy classes have different numbers of policies
different hospitals have different claim volumes
different regions have different insured populations
```

A group with larger exposure usually deserves more credibility.

#### 3. Mathematical derivation

The Model 2 credibility formula is:

```text
Z_i = P_i / (P_i + a)
```

where:

```text
a = E[s^2(theta)] / Var[m(theta)]
```

The credibility estimate for group `i` is:

```text
m_hat_i = Z_i * X_bar_i + (1 - Z_i) * E[m(theta)]
```

For aggregate prediction:

```text
expected aggregate amount = target exposure * m_hat_i
```

If one credibility factor is known, we can back-solve `a`.

From:

```text
Z_i = P_i / (P_i + a)
```

multiply both sides:

```text
Z_i(P_i + a) = P_i
Z_i a = P_i - Z_i P_i
a = P_i(1 - Z_i) / Z_i
```

#### 4. Simple example

Suppose:

```text
P_i = 1,250
Z_i = 0.5196
```

Then:

```text
a = 1250(1 - 0.5196) / 0.5196
  = 1155.70
```

For another group with exposure:

```text
P_j = 3,410
```

credibility is:

```text
Z_j = 3410 / (3410 + 1155.70)
    = 0.7469
```

The larger exposure group receives higher credibility.

#### 5. Exam-style case study

For the NBFC loan-default question:

Overall average default size:

```text
E[m(theta)] =
[(1250 * 85000) + (2450 * 72000) + (3410 * 90000)]
/ (1250 + 2450 + 3410)
= INR 82,918.43
```

Education loan aggregate default estimate:

```text
INR 2.52 crore for 300 loans
```

So per-loan estimate:

```text
2.52 * 10^7 / 300 = INR 84,000
```

Set:

```text
84,000 = Z_E(85,000) + (1 - Z_E)(82,918.43)
```

Solving:

```text
Z_E = 0.5196
```

Then:

```text
a = 1250(1 - 0.5196)/0.5196 = 1155.70
```

For gold loans:

```text
Z_G = 3410 / (3410 + 1155.70) = 0.7469
```

Per-loan gold default estimate:

```text
0.7469(90,000) + (1 - 0.7469)(82,918.43)
= INR 88,207.65
```

For 1,100 gold loans:

```text
1,100 * 88,207.65 = INR 9.70 crore
```

#### 6. Real-world actuarial case study

A lending insurer prices credit protection for education loans, personal loans, and gold loans. Gold loans may have more historical defaults, so the gold-loan estimate should rely more heavily on gold-loan experience than on the overall portfolio average.

Model 2 handles this by making credibility exposure-sensitive.

#### 7. Common mistakes

- Using Model 1 when exposures differ materially.
- Using group totals instead of per-unit averages in the credibility formula.
- Forgetting to multiply the final per-unit estimate by target exposure.
- Thinking a larger claim total always means higher risk, without adjusting for exposure.

#### 8. Revision checkpoint

You should be able to compute:

```text
overall exposure-weighted mean
Z_i = P_i/(P_i+a)
back-solve a from one known Z
credibility estimate per unit
aggregate expected amount
```

### Expanded deep explanation

Bayesian and credibility methods update prior belief with experience. The actuarial skill is to recognise what information is prior, what information is data, and how the posterior should affect decisions.

- Bayesian analysis combines prior information and data through the likelihood. The posterior distribution is proportional to prior times likelihood.
- Credibility theory has the same actuarial instinct: blend individual experience with collective experience. The more stable and relevant the individual data, the more credibility it receives.
- Conjugate priors are useful because the posterior stays in the same family. This makes updating transparent and exam calculations faster.

### Step-by-step working method

1. Write the prior distribution and identify its parameters.
2. Write the likelihood from the observed data.
3. Multiply prior and likelihood, keeping parameter-dependent terms.
4. Recognise the posterior family or normalise if required.
5. Use the posterior mean, mode, interval, or credibility form for the decision.

### Extra practical actuarial examples

- Renewal example: a beta prior for renewal probability can be updated after observing renewals and non-renewals.
- Claim count example: a gamma prior for a Poisson rate leads to a gamma posterior and a credibility-style blended estimate.
- Pricing example: a small employer group should not be priced only from its own volatile claims experience; credibility blends group experience with portfolio experience.

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

### Topic 119: Discrete Bayesian Prior and Posterior Odds

#### 1. Concept theory

Not all priors are continuous. Sometimes the parameter can take only a few possible values.

Example:

```text
lambda = 100 with probability 0.5
lambda = 300 with probability 0.5
```

Bayesian updating compares how likely the observed evidence is under each possible parameter value.

#### 2. Why actuaries care

This appears when there are competing scenarios:

```text
recession vs no recession
government changes vs no change
high inflation vs low inflation
catastrophe regime vs normal regime
```

The prior gives initial scenario probabilities. Data updates them into posterior probabilities.

#### 3. Mathematical derivation

Bayes theorem for discrete parameter values:

```text
P(theta_i | data)
= P(data | theta_i)P(theta_i) / sum_j P(data | theta_j)P(theta_j)
```

For a Pareto distribution with:

```text
F(x | lambda) = 1 - lambda/(lambda + x)
```

the survival probability is:

```text
P(X > x | lambda) = lambda/(lambda + x)
```

If prior probabilities are equal:

```text
P(lambda=100) = 0.5
P(lambda=300) = 0.5
```

then posterior odds depend on:

```text
P(data | lambda=100)
P(data | lambda=300)
```

#### 4. Simple example

Suppose the observed evidence is:

```text
X > 500
```

Likelihood under `lambda = 100`:

```text
P(X > 500 | 100) = 100/(100+500) = 1/6
```

Likelihood under `lambda = 300`:

```text
P(X > 500 | 300) = 300/(300+500) = 3/8
```

Since:

```text
3/8 > 1/6
```

the evidence favours `lambda = 300`.

#### 5. Exam-style case study

In the election/market-index problem:

```text
lambda = 100 means ruling alliance
lambda = 300 means opposition alliance
prior probabilities are 0.5 each
```

If analysts observe:

```text
market growth > 500
```

then:

```text
P(growth > 500 | lambda=300)
>
P(growth > 500 | lambda=100)
```

So the posterior probability is higher for `lambda = 300`, meaning the evidence favours the opposition alliance.

#### 6. Real-world actuarial case study

An investment actuary may model future inflation under two regimes: stable economy or stressed economy. Market data during the year updates the probability of each regime. This posterior scenario probability then affects asset-liability projections.

#### 7. Common mistakes

- Treating a discrete prior as a density.
- Forgetting to multiply likelihood by prior probability.
- Using the PDF when the evidence is an interval or tail event.
- Forgetting to normalise posterior probabilities.

#### 8. Revision checkpoint

You should be able to compute:

```text
posterior probability for each scenario
= likelihood * prior / total likelihood-weighted prior
```

### Expanded deep explanation

Bayesian and credibility methods update prior belief with experience. The actuarial skill is to recognise what information is prior, what information is data, and how the posterior should affect decisions.

- Bayesian analysis combines prior information and data through the likelihood. The posterior distribution is proportional to prior times likelihood.
- Credibility theory has the same actuarial instinct: blend individual experience with collective experience. The more stable and relevant the individual data, the more credibility it receives.
- Conjugate priors are useful because the posterior stays in the same family. This makes updating transparent and exam calculations faster.

### Step-by-step working method

1. Write the prior distribution and identify its parameters.
2. Write the likelihood from the observed data.
3. Multiply prior and likelihood, keeping parameter-dependent terms.
4. Recognise the posterior family or normalise if required.
5. Use the posterior mean, mode, interval, or credibility form for the decision.

### Extra practical actuarial examples

- Renewal example: a beta prior for renewal probability can be updated after observing renewals and non-renewals.
- Claim count example: a gamma prior for a Poisson rate leads to a gamma posterior and a credibility-style blended estimate.
- Pricing example: a small employer group should not be priced only from its own volatile claims experience; credibility blends group experience with portfolio experience.

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

### Topic 120: Recovering Beta Prior Parameters from Mean and Variance

#### 1. Concept theory

A Beta distribution is used for unknown probabilities:

```text
theta ~ Beta(alpha, beta)
```

Sometimes the question gives the prior mean and variance instead of `alpha` and `beta`.

We then recover `alpha` and `beta` using method of moments.

#### 2. Why actuaries care

Actuaries often receive expert beliefs in practical language:

```text
expected fraud rate is 10%
uncertainty is small
expected wrong-answer rate is 10%
expected claim probability is 2%
```

To use Bayesian updating, these beliefs must be converted into prior distribution parameters.

#### 3. Mathematical derivation

For:

```text
theta ~ Beta(alpha, beta)
```

Mean:

```text
mu = alpha/(alpha+beta)
```

Variance:

```text
sigma^2 = alpha beta / [(alpha+beta)^2(alpha+beta+1)]
```

Let:

```text
s = alpha + beta
```

Then:

```text
alpha = mu s
beta = (1-mu)s
```

Variance becomes:

```text
sigma^2 = mu(1-mu)/(s+1)
```

Solve:

```text
s + 1 = mu(1-mu)/sigma^2
s = mu(1-mu)/sigma^2 - 1
```

Then:

```text
alpha = mu s
beta = (1-mu)s
```

#### 4. Simple example

Suppose:

```text
mu = 0.10
sigma^2 = 9/1100
```

Here:

```text
mu(1-mu) = 0.10 * 0.90 = 0.09
```

So:

```text
s = 0.09 / (9/1100) - 1
  = 11 - 1
  = 10
```

Then:

```text
alpha = 0.10 * 10 = 1
beta = 0.90 * 10 = 9
```

So:

```text
theta ~ Beta(1,9)
```

#### 5. Exam-style case study

For an AI chatbot wrong-answer rate:

```text
theta = proportion of wrong answers
prior mean = 10%
prior variance = 9/1100
```

The method of moments gives:

```text
alpha = 1
beta = 9
```

If a sample of `n` questions contains `w` wrong answers:

```text
theta | data ~ Beta(alpha + w, beta + n - w)
```

The MLE is:

```text
theta_hat = w/n
```

The credibility estimate is:

```text
Z * theta_hat + (1-Z) * mu
```

where:

```text
Z = n/(alpha+beta+n)
```

#### 6. Real-world actuarial case study

An insurer using AI claim triage may estimate the probability that the model gives a wrong decision. Before enough internal data exists, experts may state a prior expected error rate and uncertainty. A Beta prior converts that expert view into a form that can be updated with observed audit results.

#### 7. Common mistakes

- Using `alpha/(alpha+beta)` correctly but forgetting the variance denominator `alpha+beta+1`.
- Treating percentages inconsistently, such as using 10 instead of 0.10.
- Forgetting that `beta` receives the number of non-events.
- Confusing prior mean with posterior mean.

#### 8. Revision checkpoint

You should be able to recover:

```text
s = alpha + beta
alpha = mu s
beta = (1-mu)s
```

from a Beta prior mean and variance.

### Expanded deep explanation

Bayesian and credibility methods update prior belief with experience. The actuarial skill is to recognise what information is prior, what information is data, and how the posterior should affect decisions.

- Bayesian analysis combines prior information and data through the likelihood. The posterior distribution is proportional to prior times likelihood.
- Credibility theory has the same actuarial instinct: blend individual experience with collective experience. The more stable and relevant the individual data, the more credibility it receives.
- Conjugate priors are useful because the posterior stays in the same family. This makes updating transparent and exam calculations faster.

### Step-by-step working method

1. Write the prior distribution and identify its parameters.
2. Write the likelihood from the observed data.
3. Multiply prior and likelihood, keeping parameter-dependent terms.
4. Recognise the posterior family or normalise if required.
5. Use the posterior mean, mode, interval, or credibility form for the decision.

### Extra practical actuarial examples

- Renewal example: a beta prior for renewal probability can be updated after observing renewals and non-renewals.
- Claim count example: a gamma prior for a Poisson rate leads to a gamma posterior and a credibility-style blended estimate.
- Pricing example: a small employer group should not be priced only from its own volatile claims experience; credibility blends group experience with portfolio experience.

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

### Topic 121: EBCT Model 1 with Inflation Trend and Portfolio Changes

#### 1. Concept theory

EBCT Model 1 estimates a future premium by blending a risk's own experience with collective portfolio experience.

If historical claim amounts are affected by inflation, older years must be brought to a common price level before applying credibility.

#### 2. Why actuaries care

Health insurance claims are strongly affected by medical inflation. If past claims are not inflated to the renewal year, the premium estimate will be too low.

#### 3. Mathematical derivation

Basic EBCT Model 1:

```text
Credibility estimate = Z x risk mean + (1 - Z) x collective mean
```

where:

```text
Z = n / (n + EPV / VHM)
```

With inflation, each historical claim amount should be adjusted:

```text
adjusted claim = historical claim x inflation factor
```

For annual inflation rate `i`, a claim from `k` years before renewal is adjusted by:

```text
(1 + i)^k
```

Then calculate risk means, collective mean, EPV, VHM, and credibility using adjusted data.

#### 4. Simple example

If last year's claims are 100 and medical inflation is 20 percent:

```text
adjusted claim = 100 x 1.20 = 120
```

If claims from two years ago are 100:

```text
adjusted claim = 100 x 1.20^2 = 144
```

#### 5. Exam-style case study

In March 2022, the insurer has 4 policies and 3 years of claims. For Policy 4 renewal:

1. calculate Policy 4's average claim experience
2. calculate portfolio average
3. calculate EPV and VHM
4. compute `Z`
5. apply:

```text
Premium_4 = Z x Policy 4 mean + (1 - Z) x portfolio mean
```

For 20 percent medical inflation, first restate all past claims to the renewal date using suitable inflation factors. Additional data needed includes claim year/timing and the inflation index or inflation assumptions by year.

If Policy 1 is excluded, the estimate of between-policy variation may change. If the remaining policies are more similar, VHM may reduce and the credibility factor may reduce. If Policy 1 was making the portfolio more homogeneous, the opposite may happen. Direction should be justified from the data.

#### 6. Real-world actuarial case study

A group health insurer renews a corporate policy. The employer's own experience matters, but the insurer also uses portfolio experience.

Medical inflation has been 20 percent, so claims from earlier years are uplifted before calculating credibility. Otherwise the renewal premium would be based on outdated hospital cost levels.

#### 7. Common mistakes

- Applying credibility before adjusting for inflation.
- Using nominal claims from different years as if comparable.
- Forgetting exposure changes or policy count changes.
- Assuming removing one policy always increases credibility.
- Ignoring claim timing within a year.

#### 8. Revision checkpoint

Without notes, you should be able to explain:

```text
Credibility premium = Z own mean + (1 - Z) collective mean
```

and state how to adjust historical claims for medical inflation before EBCT calculations.

### Expanded deep explanation

Bayesian and credibility methods update prior belief with experience. The actuarial skill is to recognise what information is prior, what information is data, and how the posterior should affect decisions.

- Bayesian analysis combines prior information and data through the likelihood. The posterior distribution is proportional to prior times likelihood.
- Credibility theory has the same actuarial instinct: blend individual experience with collective experience. The more stable and relevant the individual data, the more credibility it receives.
- Conjugate priors are useful because the posterior stays in the same family. This makes updating transparent and exam calculations faster.

### Step-by-step working method

1. Write the prior distribution and identify its parameters.
2. Write the likelihood from the observed data.
3. Multiply prior and likelihood, keeping parameter-dependent terms.
4. Recognise the posterior family or normalise if required.
5. Use the posterior mean, mode, interval, or credibility form for the decision.

### Extra practical actuarial examples

- Renewal example: a beta prior for renewal probability can be updated after observing renewals and non-renewals.
- Claim count example: a gamma prior for a Poisson rate leads to a gamma posterior and a credibility-style blended estimate.
- Pricing example: a small employer group should not be priced only from its own volatile claims experience; credibility blends group experience with portfolio experience.

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

### Topic 122: Bayes Theorem for Source Probabilities and Loss Function Estimates

#### 1. Concept theory

Bayes theorem updates the probability of a cause after observing an event.

For example, if a candidate fails to join the company, Bayes theorem can estimate which institute the candidate most likely came from.

Bayesian estimates also depend on the loss function:

- squared error loss: posterior mean
- absolute error loss: posterior median
- all-or-nothing loss: posterior mode

#### 2. Why actuaries care

Actuaries use Bayes theorem to update probabilities after observing claims, lapses, fraud indicators, medical test results, and underwriting outcomes.

Loss functions matter because different business objectives lead to different "best" estimates.

#### 3. Mathematical derivation

Bayes theorem:

```text
P(A_i | F) = P(F | A_i)P(A_i) / sum_j P(F | A_j)P(A_j)
```

where:

```text
A_i = source or group
F = observed event
```

For a Gamma posterior:

```text
W | data ~ Gamma(alpha, rate lambda)
```

posterior mean:

```text
E[W | data] = alpha / lambda
```

posterior mode:

```text
(alpha - 1) / lambda
```

when `alpha > 1`.

The posterior median generally has no simple closed form for Gamma, but it lies between mean and mode for a right-skewed Gamma distribution:

```text
mode < median < mean
```

#### 4. Simple example

Suppose:

```text
P(A) = 0.4, P(B) = 0.6
P(F | A) = 0.1, P(F | B) = 0.2
```

Then:

```text
P(A | F) = 0.1(0.4) / [0.1(0.4) + 0.2(0.6)]
         = 0.04 / 0.16
         = 0.25
```

#### 5. Exam-style case study

In April 2021, selection proportions are:

```text
A = 20%, B = 20%, C = 30%, D = 30%
```

Failure-to-join probabilities are:

```text
A: 1%, B: 2%, C: 3%, D: 4%
```

For Institute A:

```text
P(A | fail) = P(fail | A)P(A) / total failure probability
```

where:

```text
total failure probability =
0.01(0.20) + 0.02(0.20) + 0.03(0.30) + 0.04(0.30)
```

For:

```text
W | data ~ Gamma(48, 4)
```

squared-error Bayes estimate:

```text
48 / 4 = 12
```

For absolute and all-or-nothing loss, do not assume they must equal the mean. For a right-skewed Gamma distribution:

```text
mode < median < mean
```

#### 6. Real-world actuarial case study

An insurer observes that a customer lapses. The customer could have come from different sales channels, each with different lapse behaviour.

Bayes theorem helps infer the most likely channel and improve future retention analysis.

#### 7. Common mistakes

- Dividing by `P(fail | A)` instead of total failure probability.
- Forgetting prior source proportions.
- Saying classical statistics treats parameters as random variables.
- Using posterior mean for every loss function.
- Forgetting Gamma mean and mode differ.

#### 8. Revision checkpoint

Without notes, you should be able to write:

```text
P(A_i | F) = P(F | A_i)P(A_i) / sum P(F | A_j)P(A_j)
Squared error -> posterior mean
Absolute error -> posterior median
All-or-nothing -> posterior mode
```

### Expanded deep explanation

Bayesian and credibility methods update prior belief with experience. The actuarial skill is to recognise what information is prior, what information is data, and how the posterior should affect decisions.

- Bayesian analysis combines prior information and data through the likelihood. The posterior distribution is proportional to prior times likelihood.
- Credibility theory has the same actuarial instinct: blend individual experience with collective experience. The more stable and relevant the individual data, the more credibility it receives.
- Conjugate priors are useful because the posterior stays in the same family. This makes updating transparent and exam calculations faster.

### Step-by-step working method

1. Write the prior distribution and identify its parameters.
2. Write the likelihood from the observed data.
3. Multiply prior and likelihood, keeping parameter-dependent terms.
4. Recognise the posterior family or normalise if required.
5. Use the posterior mean, mode, interval, or credibility form for the decision.

### Extra practical actuarial examples

- Renewal example: a beta prior for renewal probability can be updated after observing renewals and non-renewals.
- Claim count example: a gamma prior for a Poisson rate leads to a gamma posterior and a credibility-style blended estimate.
- Pricing example: a small employer group should not be priced only from its own volatile claims experience; credibility blends group experience with portfolio experience.

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

## Master Chapter 10: Model Choice, Diagnostics, and Practical Actuarial Judgement

A technically fitted model can still be poor for pricing. Diagnostics, stability, explainability, operational cost, fairness, and sensitivity matter before a model becomes an actuarial assumption.

### Topics in this master chapter

- Topic 123: Moment Generating Functions and Cumulant Generating Functions

### Topic 123: Moment Generating Functions and Cumulant Generating Functions

#### 1. Concept theory

The moment generating function, or MGF, is a compact way to represent a distribution:

```text
M_X(t) = E[e^(tX)]
```

The cumulant generating function, or CGF, is:

```text
K_X(t) = log M_X(t)
```

The MGF gives moments. The CGF gives cumulants.

Important cumulants:

```text
first cumulant = mean
second cumulant = variance
```

#### 2. Why actuaries care

MGFs are useful when combining risks.

If total claim cost is the sum of independent risks:

```text
S = X_1 + X_2 + ... + X_n
```

then:

```text
M_S(t) = M_X1(t) M_X2(t) ... M_Xn(t)
```

This makes aggregate risk calculations easier.

#### 3. Mathematical derivation

Using Taylor expansion:

```text
e^(tX) = 1 + tX + t^2 X^2 / 2! + t^3 X^3 / 3! + ...
```

Taking expectation:

```text
M_X(t) = E[e^(tX)]
       = 1 + tE[X] + t^2 E[X^2]/2! + t^3 E[X^3]/3! + ...
```

So derivatives at zero give raw moments:

```text
M'_X(0) = E[X]
M''_X(0) = E[X^2]
```

For the CGF:

```text
K_X(t) = log M_X(t)
```

Then:

```text
K'_X(0) = E[X]
K''_X(0) = Var(X)
```

#### 4. Simple example

Suppose:

```text
K_X(t) = 2t + 3t^2
```

Differentiate:

```text
K'_X(t) = 2 + 6t
K''_X(t) = 6
```

At `t = 0`:

```text
E[X] = K'_X(0) = 2
Var(X) = K''_X(0) = 6
```

#### 5. Exam-style case study

If:

```text
U = X - Y
```

then:

```text
M_U(t) = E[e^(tU)]
       = E[e^(t(X-Y))]
       = E[e^(tX - tY)]
       = M_X,Y(t, -t)
```

This uses the joint MGF.

#### 6. Real-world actuarial case study

Let:

```text
X = premium income
Y = claim outgo
U = underwriting profit = X - Y
```

If premium income and claim outgo are dependent, the joint MGF is needed. For example, high exposure can increase both premium and claims.

#### 7. Common mistakes

- Reading the coefficient of `t^2` in the CGF directly as variance.
- Using product of MGFs without checking independence.
- Confusing MGF and CGF.

#### 8. Revision checkpoint

Remember:

```text
MGF derivatives -> raw moments
CGF derivatives -> cumulants
```

### Expanded deep explanation

A technically fitted model can still be poor for pricing. Diagnostics, stability, explainability, operational cost, fairness, and sensitivity matter before a model becomes an actuarial assumption.

- Begin by checking support. A count model lives on non-negative integers, a severity model often lives on positive real values, and a transformed model may live on the whole real line after taking logs. Many exam errors happen before calculation because the wrong support is chosen.
- Separate the probability law from its moments. The probability law tells you every probability statement; the mean and variance are summaries. In actuarial work a model with the correct mean but the wrong tail can still be dangerous because solvency questions are tail questions.
- Use generating functions when sums are involved. MGFs, CGFs, and PGFs turn convolutions into algebra, but only under the conditions where they exist and where independence assumptions are valid.

### Step-by-step working method

1. State the random variable and its support.
2. Write the probability mass function, density, or distribution function.
3. Identify parameters and their actuarial meaning.
4. Calculate the required moment, probability, percentile, or transform.
5. Check whether the answer is sensible using units, range, and limiting cases.

### Extra practical actuarial examples

- Claim count example: if a motor portfolio has many zero-claim policies and a few multi-claim policies, compare Poisson and negative binomial assumptions by checking whether sample variance is close to, below, or above the sample mean.
- Severity example: for small routine medical claims a lognormal or gamma model may be adequate; for catastrophe-like property claims a heavier tail such as Pareto or Weibull may be needed.
- Exam example: after finding a probability, ask whether the question wanted an unconditional probability or a conditional probability after observing a claim, survival, or selection event.

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

## Final Revision Framework

For every actuarial statistics problem, move through these questions: What is the random variable? What distribution or model is being assumed? What parameter is unknown? What information has been observed? What calculation is required? What assumption would change the answer? What decision does the result support?

End every answer with business meaning. A number without interpretation is not yet actuarial work.
