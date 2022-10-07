import empyrical as ep

import numpy as np

from pluto.finance.metrics import metric


class BenchmarkMetrics(object):
    def __init__(self,
                 benchmark_source):
        self._benchmark_source = benchmark_source

        self._alpha_beta = metric.AlphaBeta()
        self._benchmark_returns = metric.BenchmarkReturnsAndVolatility()

    def compute_alpha_beta(self, ):
        ledger_returns = kwargs['returns_tracker'].daily_returns
        benchmark_returns = self._benchmark_source.daily_returns()

        alpha, beta = ep.alpha_beta_aligned(
            ledger_returns,
            benchmark_returns)

        if np.isnan(alpha):
            alpha = None
        if np.isnan(beta):
            beta = None

    def compute_benchmark_returns_and_volatility(self):
        pass

