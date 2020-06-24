"""Define portfolio optimization models"""

from collections import OrderedDict
from functools import partial
from typing import Tuple, List, Dict, Union, Callable, Any

import numpy as np
import pandas as pd

from pypfopt import EfficientFrontier, CLA
from pypfopt.objective_functions import L2_reg

from dalio.base.constants import PORTFOLIO
from dalio.model import Model
from dalio.validator import HAS_DIMS, IS_TYPE
from dalio.validator.presets import STOCK_STREAM
from dalio.util import _Builder


class MakeCriticalLine(Model):
    """Fit a critical line algorithm

    This model takes in two sources: sample_covariance and expected_returns.
    These are self-explanatory. The model calculates the algorithm for a set
    of weight bounds.

    Attributes:
        weight_bounds (tuple): lower and upper bound for portfolio weights.
    """
    weight_bounds: Tuple[int]

    def __init__(self, weight_bounds=(-1, 1)):
        """Initialize instance

        Defines sample_covariance as two-dimensional.
        Defines expected_returns as one-dimensional.

        Args:
            weight_bounds (tuple, list): lower and upper bound for portfolio
                weights.

        Raises:
            TypeError: if weight bounds are not in a tuple or list.
            ValueError: if there are more than three elements in the weight
                bounds.
        """
        super().__init__()

        self._init_source([
            "sample_covariance",
            "expected_returns"
        ])

        self._get_source("sample_covariance")\
            .add_desc(HAS_DIMS(2))

        self._get_source("expected_returns")\
            .add_desc(HAS_DIMS(1))

        if len(weight_bounds) == 2:
            if isinstance(weight_bounds, tuple):
                self.weight_bounds = weight_bounds
            elif isinstance(weight_bounds, list):
                self.weight_bounds = tuple(weight_bounds)
            else:
                TypeError(f"weight bounds must be of type list or tuple, \
                        not {type(weight_bounds)}")
        else:
            ValueError(f"weight bounds must be of length 2, \
                    not {len(weight_bounds)}")

    def run(self, **kwargs):
        """Get source data and create critical line algorithm"""

        # TODO: Paralelise
        mu = self._source_from("expected_returns", **kwargs)
        S = self._source_from("sample_covariance", **kwargs)

        # dropping levels are allowed as by this point both mu and S are
        # derived from STOCK_STREAM dataframes
        cla = CLA(mu.droplevel(0),
                  S.droplevel(0),
                  weight_bounds=self.weight_bounds)

        return cla


class MakeEfficientFrontier(MakeCriticalLine):
    """Make an efficient frontier algorithm.

    Args:
        gamma (int): gamma optimization parameter.
    """

    _OBJECTIVE_PRESETS = {
        "L2_reg": L2_reg
    }

    weight_bounds: Tuple[int]
    gamma: int

    _objectives: List[Dict[str, Union[Callable, Dict[str, Any]]]]
    _constraints: List[Union[Callable, Dict[str, Union[str, float]]]]

    def __init__(self, weight_bounds=(0, 1), gamma=0):
        """Initialize instance.

        Initialize sources and weights from superclass.

        Raises:
            TypeError: if gamma is not an integer
        """
        super().__init__(weight_bounds=weight_bounds)

        if isinstance(gamma, (int, float)):
            self.gamma = gamma
        else:
            TypeError(f"gamma must be of type int, not {type(gamma)}")

        self._objectives = list()
        self._constraints = list()

    def run(self, **kwargs):
        """Make efficient frontier.

        Create efficient frontier given a set of weight constraints.
        """

        # TODO: Paralelise
        mu = self._source_from("expected_returns", **kwargs)
        S = self._source_from("sample_covariance", **kwargs)

        # dropping levels are allowed as by this point both mu and S are
        # derived from STOCK_STREAM dataframes
        ef = EfficientFrontier(mu.droplevel(0),
                               S.droplevel(0),
                               weight_bounds=self.weight_bounds,
                               gamma=self.gamma)

        for obj in self._objectives:
            ef.add_objective(obj["func"], *obj["args"], **obj["kwargs"])

        for const in self._constraints:

            if callable(const):

                ef.add_constraint(const)

            elif isinstance(const, dict):

                if isinstance(const["ticker"], str):
                    # this should be allowed under the STOCK_STREAM desc
                    if const["ticker"] in mu.droplevel(0):
                        i = ef.tickers.index(const["ticker"])
                    else:
                        # if ticker not in expected returns, ignore
                        break
                else:
                    i = const["ticker"]

                if const["comparisson"] in ["less", "smaller", "<", "<="]:
                    ef.add_constraint(lambda w: w[i] <= const["weight"])
                elif const["comparisson"] in ["is", "equal", "=", "=="]:
                    ef.add_constraint(lambda w: w[i] == const["weight"])
                elif const["comparisson"] in ["more", "larger", ">", ">="]:
                    ef.add_constraint(lambda w: w[i] >= const["weight"])
                else:
                    ValueError("invalid comparisson, opperator option")

        return ef

    def copy(self):
        """Copy superclass, objectives and constraints."""
        ret = super().copy(
            weight_bounds=self.weight_bounds,
            gamma=self.gamma
        )

        ret._objectives = self._objectives.copy()
        ret._constraints = self._constraints.copy()

        return ret

    def add_objective(self, new_objective, *args, **kwargs):
        """Wrapper to PyPortfolioOpt BaseConvexOptimizer function

        Add a new term into the objective function. This term must be convex,
        and built from cvxpy atomic functions.

        Args:
            new_objective (cp.Expression): the objective to be added

        Raises:
            ValueError: if the new objective is not supported.
            AttributeError: if new objective is not callable.
        """

        if isinstance(new_objective, str):
            if new_objective in MakeEfficientFrontier._OBJECTIVE_PRESETS:
                obj_func = MakeEfficientFrontier\
                        ._OBJECTIVE_PRESETS[new_objective]
            else:
                ValueError(f"{new_objective} not in preset list, specify one \
                    of {MakeEfficientFrontier._OBJECTIVE_PRESETS.keys()}")
        elif callable(new_objective):
            obj_func = new_objective
        else:
            AttributeError("new objective must be callable")

        self._objectives.append({
            "func": obj_func,
            "args": args,
            "kwargs": kwargs
        })

        return self

    def add_constraint(self, new_constraint):
        """Wrapper to PyPortfolioOpt BaseConvexOptimizer function

        Add a new constraint to the optimisation problem. This constraint must
        be linear and must be either an equality or simple inequality.

        Args:
            new_constraint (callable): the constraint to be added

        Raises:
            AttributeError: if new objective is not callable.
        """

        if callable(new_constraint):
            const_func = new_constraint
        else:
            AttributeError("new objective must be callable")

        self._constraints.append(const_func)

        return self

    def add_stock_weight_constraint(
            self,
            ticker=None,
            comparisson="is",
            weight=0.5):
        """Wrapper to add_constraint method. Adds constraing on a named
        ticker.

        This is a much more intuitive interface to add constraints, as these
        will often be stocks of an unknown order in a dataframe.

        Args:
            ticker (str, int): stock ticker or location to be constrained.
            comparisson (str): constraing comparisson.
            weight (float): weight to constrain.

        Raises:
            TypeError: if any of the arguments are of an invalid type
        """

        new_entry = dict()

        if isinstance(ticker, (str, int)):
            new_entry["ticker"] = ticker
        else:
            TypeError(f"ticker must be either of type str or int, \
                    not {type(ticker)}")

        if isinstance(comparisson, str):
            new_entry["comparisson"] = comparisson
        else:
            TypeError(f"comparisson must be of type str, \
                    not {type(comparisson)}")

        if isinstance(weight, float):
            new_entry["weight"] = weight
        else:
            TypeError(f"weight must be of type float, \
                    not {type(weight)}")

        self._constraints.append(new_entry)

        return self

    def add_sector_definitions(self, sector_defs=None, **kwargs):
        # TODO: add sector definitions
        # TODO: create big ticker to sector list
        # TODO: create a ticker to sector pipe
        pass

    def add_sector_weight_constraint(
            self,
            sector=None,
            constraint="is",
            weight=0.5):
        # TODO: add sector weight constraint
        pass


class OptimumWeights(MakeEfficientFrontier, _Builder):
    """Get optimum portfolio weights from an efficient frontier.

    This is also a builder with one piece: strategy. The strategy piece
    refers to the optimization strategy.
    """

    _STRATEGY_PRESETS = [
        "max_sharpe",
        "min_volatility",
        "max_quadratic_utility",
        "efficient_risk",
        "efficient_return"
    ]

    def __init__(self, weight_bounds=(0, 1), gamma=0):
        """Initialize instance and strategy builder piece."""
        super().__init__(
            weight_bounds=weight_bounds,
            gamma=gamma
        )

        self._init_piece([
            "strategy"
        ])

    def run(self, **kwargs):
        """Get efficient frontier, fit it to model and get weights"""
        ef = super().run(**kwargs)
        self.build_model(ef)()
        return ef.clean_weights()

    def build_model(self, data):
        strat = self._piece["strategy"]

        if strat["name"] is None:
            ValueError("piece 'strategy' is not set")
        elif strat["name"] == "max_sharpe":
            strat_func = data.max_sharpe
        elif strat["name"] == "min_volatility":
            strat_func = data.min_volatility
        elif strat["name"] == "max_quadratic_utility":
            strat_func = data.max_quadratic_utility
        elif strat["name"] == "dataficient_risk":
            strat_func = data.efficient_risk
        elif strat["name"] == "dataficient_return":
            strat_func = data.efficient_return

        return partial(strat_func,
                       *strat["args"],
                       **strat["kwargs"])

    def check_name(self, param, name):
        super().check_name(param, name)
        if name not in OptimumWeights._STRATEGY_PRESETS:
            ValueError("invalid strategy name, please select one of \
                {OptimumWeights._STRATEGY_PRESETS}")


class OptimumPortfolio(Model):
    """Create optimum portfolio of stocks given dictionary of weights.

    This model has two sources: weights_in and data_in. The weights_in source
    gets optimum weights for a set of tickers. The data_in source gets price
    data for these same tickers.
    """
    # TODO: make this a builder with strategies for weight rebalancing and
    # options for investing schedules.

    def __init__(self):
        """Initialize instance.

        Describe weights_in source as a dict.
        Describe data_in source as a stock stream validator preset.
        """
        super().__init__()

        self._init_source([
            "weights_in",
            "data_in"
        ])

        self._get_source("weights_in")\
            .add_desc(IS_TYPE(dict))

        self._get_source("data_in")\
            .add_desc(STOCK_STREAM)

    def run(self, **kwargs):
        """Gets weights and uses them to create portfolio prices if weights
        were kept constant.
        """

        # Paralelize
        # Fill unavailable stocks with 0
        prices = self._source_from("data_in", **kwargs).fillna(0)
        weights = self._source_from("weights_in", **kwargs)

        # sorted prices
        prices = prices.reindex(sorted(prices.columns), axis=1)
        # sorted weights
        weights = OrderedDict(sorted(weights.items(), key=lambda t: t[0]))

        p_val = prices.values
        w_val = np.array([*weights.values()]).reshape([-1, 1])

        port = np.matmul(p_val, w_val)

        port = pd.DataFrame(
            port,
            index=prices.index
        )

        # do this to conform to STOCK_STREAM description
        multi_idx = pd.MultiIndex.from_tuples(
            [(prices.columns[0][0], PORTFOLIO)],
            names=prices.columns.names
        )

        port.columns = multi_idx

        return port
