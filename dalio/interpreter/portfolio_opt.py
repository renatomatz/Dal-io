"""Instanciate interpreters for portfolio optimization"""

from functools import partial

import numpy as np
import pandas as pd

from pypfopt import CLA, EfficientFrontier

from dalio.base import _Interpreter


class _PortfolioOptimizer(_Interpreter):
    """Interpreter for generalized portfolio optimization.

    This lays out the basic feature and mechanisms of portfolio optimization
    and its subclasses can implement them to fit a specific package.
    """

    def __init__(self):
        super().__init__()

        self.clear()

    def optimize(self,
                 data,
                 strategy,
                 *args,
                 weight_bounds=None,
                 gamma=None,
                 **kwargs):
        """Get optimum portfolio weights based on an optimization strategy"""
        raise NotImplementedError()

    @property
    def weights(self):
        """Optimum portfolio weights"""
        raise NotImplementedError()

    @property
    def objectives(self):
        """Get the current optimizer objectives"""
        raise NotImplementedError()

    def add_objective(self, objective, *args, **kwargs):
        """Set the objective functions"""
        raise NotImplementedError()

    @property
    def constraints(self):
        """Get the current optimizer objectives"""
        raise NotImplementedError()

    def add_constraint(self, constraint, *args, **kwargs):
        """Add a constraint function"""
        raise NotImplementedError()

    def efficient_frontier(self, *args, **kwargs):
        """Get the optimized model's efficient frontier"""
        raise NotImplementedError()

    def opt_performance(self, *args, **kwargs):
        """Get the optimized model's performance"""
        raise NotImplementedError()


class PyPfOptInterpreter(_PortfolioOptimizer):
    """Interpreter for the PyPortfolioOpt package

    Attributes:
        _efficient_frontier (array-like): actual unconstrained efficient
            frontier line. Set during optimization.
    """

    def __init__(self):
        super().__init__()
        self._efficient_frontier = None

        self.clear()

    def clear(self):
        self._engine \
            = self._efficient_frontier \
            = None

    @property
    def engine(self):
        if self._engine is not None:
            return self._engine

        raise AttributeError("efficient frontier model is not set")

    @engine.setter
    def engine(self, engine):
        if isinstance(engine, EfficientFrontier):
            self._engine = engine

        raise TypeError(f"engine parameter must be of type \
            {EfficientFrontier}, not {type(engine)}")

    def optimize(self,
                 data,
                 strategy,
                 *args,
                 weight_bounds=None,
                 gamma=None,
                 **kwargs):

        if isinstance(data, tuple) and len(data) == 2:
            mu, S = data
        elif isinstance(data, pd.DataFrame):
            mu, S = data.mean(), data.cov()
        else:
            mu, S = np.mean(data), np.cov(data)

        try:
            self.engine = EfficientFrontier(
                mu, S,
                weight_bounds=weight_bounds,
                gamma=gamma,
            ).__get_attribute__(strategy)(*args, **kwargs)

        except AttributeError:
            raise AttributeError(f"strategy {strategy} is not available")

        # get efficient frontier from the pypfopt critical line algorithm
        self._efficient_frontier = CLA(
            mu, S,
            weight_bounds=weight_bounds,
        ).efficient_frontier()

    @property
    def weights(self):
        return self.engine.clean_weights()

    @property
    def objectives(self):
        return self.engine._objectives

    def add_objective(self, objective, *args, **kwargs):
        self.engine.add_objective(
            partial(objective, *args), **kwargs
        )

    @property
    def constraints(self):
        return self.engine._constraints

    def add_constraint(self, constraint, *args, **kwargs):
        self.engine.add_constraint(
            partial(constraint, *args), **kwargs
        )

    def efficient_frontier(self, *args, **kwargs):
        return self._efficient_frontier

    def opt_performance(self, *args, **kwargs):
        return self.engine.portfolio_performance(*args, **kwargs)
