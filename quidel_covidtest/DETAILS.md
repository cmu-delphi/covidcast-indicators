# Quidel COVID Test

\subsection{Background}
Starting May 9, 2020, we began getting Quidel COVID Test data and started reporting it from May 26, 2020 due to limitation in the data volume. The data contains a number of features for every test, including localization at 5-digit Zip Code level, a TestDate and StorageDate, patient age, and several identifiers that uniquely identify the device on which the test was performed (SofiaSerNum, the individual test (FluTestNum), and the result (ResultID). Multiple tests are stored on each device. The present Quidel COVID Test sensor concerns the positive rate in the test result.. \\

\subsection{Signal names}
\begin{enumerate}
\item[(a)] {\tt raw\_pct\_positive }: estimates of the percentage of positive tests in total tests 
\item[(b)] {\tt smoothed\_pct\_positive} : same as in (a), but where the estimates are formed by pooling together the last 7 days of data
\end{enumerate}

\subsection{Estimating percent positive test proportion}
Let $n$ be the number of total COVID tests taken over a given time period and a given location (the test result can be negative/positive/invalid). Let $x$ be the number of tests taken with positive results in this location over the given time period. We are interested in estimating the percentage of positive tests which is defined as:
\begin{equation} 
p = 100 \cdot \frac{x}{n} 
\end{equation}
We estimate $p$ across 3 temporal-spatial aggregation schemes:
\begin{enumerate}
\item daily, at the MSA (metropolitan statistical area) level;
\item daily, at the HRR (hospital referral region) level;
\item daily, at the state level.
\end{enumerate}
We are able to make these aggregations accurately because each test is reported with its 5-digit ZIP code. We do not report estimates for individual counties, as typically each county has too few tests to make the estimated value statistically meaningful.

\textbf{MSA and HRR levels}: In a given MSA or HRR, suppose N flu tests are taken in a certain time period, X is the number of tests taken with positive results. If $N \geq 50$, we simply use:
\begin{equation} 
\hat{p} = 100 \cdot \frac{X}{N} 
\end{equation}
If $N < 50$, we lend $50 - N $ fake samples from its home state to shrink the estimate to the state's mean, which means:
\begin{equation} 
\hat{p} = 100 \cdot [\frac{N}{50}\frac{X}{N} + \frac{50 - N}{50}\frac{X_s}{N_s}] 
\end{equation}
where $N_s$, $X_s$ are the number of flu tests and the number of flu tests taken with positive results taken in its home state in the same time period.

\textbf{State level}:  the states with sample sizes smaller than a certain threshold are discarded. (The threshold is set to be 50 temporarily). For the rest of the states with big enough sample sizes,
\begin{equation} 
\hat{p} = 100 \cdot \frac{X}{N}
\end{equation}

The estimated standard error is simply:
\begin{equation}
\label{eq:quidel_pct_positive_std}
\widehat{\mathrm{se}}(\hat{p}) = \frac{1}{100}\sqrt{\frac{\hat{p}(1-\hat{p})}{N}} 
\end{equation}
where we assume for each time point, the estimates follow a binomial distribution.


\subsection{Temporal Pooling}
Additionally, as with the Quidel COVID Test signal, we consider smoothed estimates formed by pooling data over time. That is, daily, for each location, we first pool all data available in that location over the last 7 days, and we then recompute everything described in the last two subsections. Pooling in this data makes estimates available in more geographic areas, as many areas report very few tests per day, but have enough data to report when 7 days are considered. 