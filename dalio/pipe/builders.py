"""Builder Pipes"""
import warnings

from typing import List, Union, Callable
from functools import partial

import numpy as np
import pandas as pd

from sklearn.linear_model import LinearRegression

from pypfopt import CovarianceShrinkage
from pypfopt.expected_returns import return_model as ReturnModel

from dalio.base.constants import (
    SIC_CODE,
    TICKER,
    RETURNS,
    MAX_EXEDENCE,
    EXPECTED_SHORTFALL,
)

from dalio.interpreter import _ARCH

from dalio.pipe import Pipe, PipeBuilder

from dalio.validator import (
    HAS_COLS,
    HAS_DIMS,
    IS_TYPE,
    HAS_ATTR,
)

from dalio.validator.presets import STOCK_INFO, STOCK_STREAM

from dalio.ops import get_comps_by_sic

warnings.filterwarnings('ignore')


class StockComps(Pipe):
    """Get a list of a ticker's comparable stocks

    This can utilize any strategy of getting stock comparative companies and
    return up to a certain ammount of comps.

    Attributes:
        _strategy (str, callable): comparisson strategy name or function.
        max_ticks (int): maximum number of tickers to return.
    """
    # TODO: make this a builder

    _STRATEGY_PRESETS = ["sic_code"]

    _strategy: Union[str, Callable[[pd.DataFrame], List[str]]]
    max_ticks: int

    def __init__(self, strategy="sic_code", max_ticks=6):
        """Initialize instance.

        Define source as a stock info dataframe. Other definitions depend on
        the chosen strategy.

        Makes TICKER a required argument.

        Args:
            strategy (str, callable): comparisson strategy name or function.
            max_ticks (int): maximum number of tickers to return.
        """
        super().__init__()

        self._source\
            .add_desc(STOCK_INFO)

        if strategy == "sic_code":
            self._source\
                .add_desc(HAS_COLS(SIC_CODE))

        self._req_args.add(TICKER)

        if isinstance(strategy, str):
            if strategy in StockComps._STRATEGY_PRESETS:
                self._strategy = strategy
            else:
                raise ValueError(f"Argument strategy must be one of\
                    {StockComps._STRATEGY_PRESETS}")
        elif hasattr(strategy, "__iter__"):
            self._strategy = strategy
        else:
            raise ValueError("strategy must be either a string or a callable")

        self.max_ticks = max_ticks

    def run(self, **kwargs):
        """Gets ticker argument and passes an empty ticker request to
        transform.

        Empty ticker requests are supposed to return all tickers available in
        a source, so this allows the compariisson to be made in all stocks
        from a certain source.

        Raises:
            ValueError: if ticker is more than a single symbol.
        """
        ticker = kwargs.pop(TICKER)  # remove ticker request
        if isinstance(ticker, str):
            return self.transform(
                self._source.request(**kwargs),
                ticker=ticker)
        else:
            # TODO: allow for returning dict of symbols and comps.
            raise ValueError("ticker must be a single symbol")

    def transform(self, data, **kwargs):
        """Get comps according to the set strategy"""
        if self._strategy == "sic_code":
            return get_comps_by_sic(
                data,
                kwargs["ticker"],
                max_ticks=self.max_ticks
            )
        else:
            return self._strategy(
                data,
                kwargs["ticker"],
                max_ticks=self.max_ticks
            )

    def copy(self, *args, **kwargs):
        return super().copy(
            *args,
            strategy=self._strategy,
            **kwargs
        )


class PandasLinearModel(PipeBuilder):
    """Create a linear model from input pandas dataframe, using its index
    as the X value.

    This builder is made up of a single piece: strategy. This piece sets
    which linear model should be used to fit the data.
    """

    def __init__(self):
        """Initialize instance

        Source data is defined to have one or two dimensions.
        """
        super().__init__()

        self._init_piece([
            "strategy"
        ])

        self._source\
            .add_desc(HAS_DIMS(2, comparisson="<="))

    def transform(self, data, **kwargs):
        """Set up fitting parameters and fit built model.

        Returns:
            Fitted linear model
        """
        X = np.arange(len(data.index)).reshape([-1, 1])
        y = data.to_numpy()
        return self.build_model((X, y))

    def build_model(self, data, **kwargs):
        """Build model by returning the chosen model and initialization
        parameters

        Returns:
            Unfitted linear model
        """
        strategy = self._pieces["strategy"]
        return self.interpreter.fit(
            data,
            strategy.name,
            *strategy.args,
            **strategy.kwargs
        )


class CovShrink(PipeBuilder):
    """Perform Covariance Shrinkage on data

    Builder with a single piece: shirnkage. Shrinkage defines what kind of
    shrinkage to apply on a resultant covariance matrix. If none is set,
    covariance will not be shrunk.

    Attributes:
        frequency (int): data time period frequency
    """

    _SHRINKAGE_PRESETS = [
        "shrunk_covariance",
        "ledoit_wolf"
    ]

    frequency: int

    def __init__(self, frequency=252):
        """Initialize instance.

        Defines source data as a stock stream

        Args:
            frequency (int): data time period frequency.

        Raises:
            TypeError: if frequence argument is not an integer.
        """
        super().__init__()

        self._init_piece([
            "shrinkage"
        ])

        self._source\
            .add_desc(STOCK_STREAM)\

        if isinstance(frequency, int):
            self.frequency = frequency
        else:
            raise TypeError(f"argument frequency must be of type {int} not \
                {type(frequency)}")

    def transform(self, data, **kwargs):
        """Build model using data get results.

        Returns:
            A covariance matrix
        """
        return self.build_model(data)()

    def check_name(self, param, name):
        super().check_name(param, name)
        if name not in CovShrink._SHRINKAGE_PRESETS:
            raise ValueError(f"argument shrinkage must be one of \
                {CovShrink._SHRINKAGE_PRESETS}")

    def build_model(self, data, **kwargs):
        """Builds Covariance Srhinkage object and returns selected shrinkage
        strategy

        Returns:
            Function fitted on the data.
        """

        cs = CovarianceShrinkage(data, frequency=self.frequency)
        shrink = self._pieces["shrinkage"]

        if shrink.name is None:
            ValueError("shrinkage piece 'name' not set")
        elif shrink.name == "shrunk_covariance":
            shrink_func = cs.shrunk_covariance
        elif shrink.name == "ledoit_wolf":
            shrink_func = cs.ledoit_wolf

        return partial(shrink_func,
                       *shrink.args,
                       **shrink.kwargs)

    def copy(self, *args, **kwargs):
        return super().copy(
            *args,
            frequency=self.frequency,
            **kwargs
        )


class ExpectedReturns(PipeBuilder):
    """Get stock's time series expected returns.

    Builder with a single piece: return_model. return_model is what model to
    get the expected returns from.
    """

    _RETURN_MODEL_PRESETS = [
        "mean_historical_return",
        "ema_historical_return",
        "james_stein_shrinkage",
        "capm_return"
    ]

    def __init__(self):
        """Initialize instance.

        Defines data source as a stock stream.
        """
        super().__init__()

        self._init_piece([
            "return_model"
        ])

        self._source\
            .add_desc(STOCK_STREAM)

    def transform(self, data, **kwargs):
        """Builds model using data and gets expected returns from it"""
        return self.build_model(data)(data)

    def check_name(self, param, name):
        super().check_name(param, name)
        if name not in ExpectedReturns._RETURN_MODEL_PRESETS:
            raise ValueError(f"argument return_model must be one of \
                {ExpectedReturns._RETURN_MODEL_PRESETS}")

    def build_model(self, data, **kwargs):
        return_model = self._pieces["return_model"]
        return partial(ReturnModel,
                       *return_model.args,
                       method=return_model.name,
                       **return_model.kwargs)


class MakeARCH(PipeBuilder):
    """Build arch model and make it based on input data.

    This class allows for the creation of arch models by configuring three
    pieces: the mean, volatility and distribution and the parameters for
    fiting the model. These are set after initialization through the _Builder
    interface. They can be either names or objects compatible with the
    specific interpreter set.

    Attributes:
        _piece (list): see _Builder class.
    """

    def __init__(self):
        """Initialize instance and pieces"""
        super().__init__()

        self._init_piece([
            "mean",
            "volatility",
            "distribution",
            "fit",
        ])

        self._source\
            .add_desc(HAS_DIMS(1))

    def transform(self, data, **kwargs):
        """Build model with sourced data"""
        return self.build_model(data)

    def build_model(self, data, **kwargs):
        """Build ARCH Model using data, set pieces and their arguments

        Returns:
            A built arch model from the arch package.
        """

        # set mean model piece with the current data
        mean = self._pieces["mean"]
        self.interpreter.init_mean(
            data,
            mean.name,
            *mean.args,
            **mean.kwargs
        )

        # set volatility model piece
        vol = self._pieces["volatility"]
        if len(vol.args) + len(vol.kwargs) == 0:
            self.interpreter.volatility = vol.name
        else:
            self.interpreter.init_volatility(
                data,
                vol.name,
                *vol.args,
                **vol.kwargs,
            )

        # set distribution model piece
        dist = self._pieces["distribution"]
        if len(dist.args) + len(dist.kwargs) == 0:
            self.interpreter.distribution = dist.name
        else:
            self.interpreter.init_distribution(
                data,
                dist.name,
                *dist.args,
                **dist.kwargs,
            )

        fit = self._pieces["fit"]
        self.interpreter.fit(*fit.args, **fit.kwargs)

        return self.interpreter


class ValueAtRisk(Pipe):
    """Get the value at risk for data based on an ARHC Model

    This takes in an ARCH Model maker, not data, which might be unintuitive,
    yet necessary, as this allows users to modify the ARCH model generating
    these values separately. A useful strategy that allows for this
    is using a pipeline with an arch model as its first input and a
    ValueAtRisk instance as its second layer. This allows us to treat the
    PipeLine as a data input with VaR output and still have control over the
    ARCH Model pieces (given you left a local variable for it behind.)

    Attributes:
        _quantiles (list): list of quantiles to check the value at risk for.
    """

    _quantiles: List[float]

    def __init__(self, quantiles=None):
        """Initialize instance and set quantiles.

        Source requires an ARCHModel input with a .fit() method

        Raises:
            TypeError: if quantiles is neither a float or a list of floats.
        """
        super().__init__()

        self._source\
            .add_desc(IS_TYPE(_ARCH))\
            .add_desc(HAS_ATTR("fit"))

        if isinstance(quantiles, list):
            if sum([n for n in quantiles if not isinstance(n, float)]) > 0:
                raise TypeError("all quantiles must be floats")
            else:
                self._quantiles = sorted(quantiles)
        elif isinstance(quantiles, float):
            self._quantiles = [quantiles]
        elif quantiles is None:
            self._quantiles = [0.01, 0.05]
        else:
            raise TypeError("quantiles argument must either be a float or \
                    list of floats")

    def transform(self, data, **kwargs):
        """Get values at risk at each quantile and each results maximum
        exedence from the mean.

        The maximum exedence columns tells which quantile the loss is placed
        on. The word "maximum" might be misleading as it is compared to the
        minimum quantile, however, this definition is accurate as the column
        essentially answers the question: "what quantile furthest away from
        the mean does the data exeed?"

        Thank you for the creators of the arch package for the beautiful
        visualizations and ideas!

        Raises:
            ValueError: if ARCH model does not have returns. This is often
                the case for unfitted models. Ensure your graph is complete.
            TypeError: if ARCH model has unsuported distribution parameter.
        """

        # This might seem redundant considering the above FitARCHModel pipe
        # but keep in mind here we need both the fitted results and the model

        # prepare returns data
        returns = data.get_orig_data()
        while type(returns.columns).__name__ == "MultiIndex":
            returns = returns.droplevel(1, axis=1)
        returns.columns = [RETURNS]

        # start is the earliest index fitted by the algorithm
        start = returns.index[max(0, data.get_fit_indices()[0]-1)]

        if returns is None:
            raise ValueError("Invalid ARCH model: does not have returns")

        cond_mean = data.forecast_mean(start=start)[start:]
        cond_var = data.forecast_variance(start=start)[start:]

        params = data.get_fit_params()

        # set ppf parameters according to data distribution

        q = data.get_dist_ppf()(self._quantiles, params)

        cols = ["".join([str(n*100), "%"]) for n in self._quantiles]

        value_at_risk = -cond_mean.values \
            - np.sqrt(cond_var).values \
            * q[None, :]

        value_at_risk = pd.DataFrame(
            value_at_risk,
            columns=cols,
            index=cond_var.index
        )

        max_exedence = []
        for idx in value_at_risk.index:
            for i, quantile in enumerate(cols[::-1]):
                if float(returns.loc[idx]) < -value_at_risk.loc[idx, quantile]:
                    max_exedence.append(self._quantiles[-(i+1)])
                    break
            else:
                # this only executes if the whole for loop completes
                max_exedence.append(1)

        max_exedence = pd.DataFrame(
            max_exedence,
            columns=[MAX_EXEDENCE],
            index=cond_var.index
        )

        return pd.concat([returns, value_at_risk, max_exedence],
                         join="inner",
                         axis=1).dropna()

    def copy(self, *args, **kwargs):
        return super().copy(
            *args,
            quantiles=self._quantiles,
            **kwargs
        )


class ExpectedShortfall(ValueAtRisk):
    """Get expected shortfal for given quantiles

    See base class for more in depth explanation.
    """

    def transform(self, data, **kwargs):
        """Get the value at risk given by an arch model and calculate the
        expected shortfall at given quantiles.
        """
        data = super().transform(data, **kwargs)[[MAX_EXEDENCE, RETURNS]]

        ret = {}
        for quant in self._quantiles:
            # calculate mean returns that exeed a quantile
            ret[quant] = np.mean(data[RETURNS][data[MAX_EXEDENCE] <= quant])

        return pd.DataFrame.from_dict(
                ret,
                orient="index",
                columns=[EXPECTED_SHORTFALL]
        )


class OptimumWeights(Pipe):
    """Get optimum portfolio weights from an efficient frontier or CLA.
    This is also a builder with one piece: strategy. The strategy piece
    refers to the optimization strategy.
    """

    def __init__(self):
        """Initialize instance and strategy builder piece."""
        super().__init__()

        self._source\
            .add_desc(IS_TYPE(_PortfolioOptimizer))

    def transform(self, data, **kwargs):
        """Get efficient frontier, fit it to model and get weights"""
        return data.weights 
