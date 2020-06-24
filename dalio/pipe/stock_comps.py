"""Get stock comps"""
from typing import List, Union, Callable

from pandas import DataFrame

from dalio.base.constants import SIC_CODE, TICKER

from dalio.pipe import Pipe
from dalio.validator import HAS_COLS
from dalio.validator.presets import STOCK_INFO
from dalio.ops import get_comps_by_sic


class StockComps(Pipe):
    """Get a list of a ticker's comparable stocks

    This can utilize any strategy of getting stock comparative companies and
    return up to a certain ammount of comps.

    Attributes:
        _strategy (str, callable): comparisson strategy name or function.
        max_ticks (int): maximum number of tickers to return.
    """

    _STRATEGY_PRESETS = ["sic_code"]

    _strategy: Union[str, Callable[[DataFrame], List[str]]]
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
