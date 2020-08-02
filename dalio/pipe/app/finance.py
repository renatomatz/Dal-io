"""Builder Pipes"""

from typing import List, Union, Callable
from functools import partial

import pandas as pd

from pypfopt.expected_returns import return_model as ReturnModel

from dalio.base.constants import (
    SIC_CODE,
    TICKER,
)

from dalio.base import TransformerApplication

from dalio.validator import (
    HAS_COLS,
    HAS_DIMS,
)

from dalio.validator.presets import STOCK_INFO, STOCK_STREAM

from dalio.ops import get_comps_by_sic


class StockComps(TransformerApplication):
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


class ExpectedReturns(TransformerApplication):
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


class MakeARCH(TransformerApplication):
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

        # set all GARCH model pieces
        arch = self.interpreter

        for attr in ["mean", "volatility", "distribution"]:
            piece = self._pieces[attr]
            arch.init_attr(
                attr,
                piece.name,
                *piece.args,
                data=data,
                **piece.kwargs,
            )

        fit = self._pieces["fit"]
        arch.fit(*fit.args, **fit.kwargs)

        return arch
