from typing import (
    Tuple,
    List,
    Dict,
    Union,
    Callable,
    Any,
)

from pypfopt.objective_functions import L2_reg

from dalio.model.app import TransformerApplication

from dalio.validator import HAS_DIMS


class MakeEfficientFrontier(TransformerApplication):
    """Fit an efficient frontier algorithm.

    This model takes in two sources: sample_covariance and expected_returns.
    These are self-explanatory. The model calculates the algorithm for a set
    of weight bounds.

    Attributes:
        weight_bounds (tuple): lower and upper bound for portfolio weights.
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
        """Initialize instance
        Defines sample_covariance as two-dimensional.
        Defines expected_returns as one-dimensional.
        Args:
            weight_bounds (tuple, list): lower and upper bound for portfolio
                weights.
        Raises:
            TypeError: if weight bounds are not in a tuple or list or if
                gamma is not an integer.
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

        if isinstance(gamma, (int, float)):
            self.gamma = gamma
        else:
            TypeError(f"gamma must be of type int, not {type(gamma)}")

        self._objectives = list()
        self._constraints = list()

    def run(self, **kwargs):
        """Collect source data, implement optional processing and
        pass it on to model builder.
        """

        # TODO: Paralelise
        mu = self._source_from("expected_returns", **kwargs)
        S = self._source_from("sample_covariance", **kwargs)

        return self.build_model((mu, S))

    def build_model(self, data, **kwargs):
        """Make efficient frontier.
        Create efficient frontier given a set of weight constraints.
        """

        mu, S = data

        # dropping levels are allowed as by this point both mu and S are
        # derived from STOCK_STREAM dataframes
        ef = self.interpreter

        ef.optimize((mu.droplevel(0), S.droplevel(0)),
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

    def copy(self, *args, **kwargs):
        """Copy superclass, objectives and constraints."""
        ret = super().copy(
            *args,
            weight_bounds=self.weight_bounds,
            gamma=self.gamma,
            **kwargs,
        )

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
