from typing import List, Union, Callable

from pandas import DataFrame

from dalio.base.constants import SIC_CODE, TICKER

from dalio.pipe import Pipe
from dalio.validator import STOCK_INFO, HAS_COLS
from dalio.util import get_comps_by_sic


class StockComps(Pipe):

    _STRATEGY_PRESETS = ["sic_code"]

    _strategy: Union[str, Callable[[DataFrame], List[str]]]

    def __init__(self, strategy="sic_code", max_ticks=6):
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
        ticker = kwargs.pop(TICKER)  # remove ticker request
        # TODO: add a way to get multiple tickers too. idk for what, so maybe
        # just don't
        if isinstance(ticker, str):
            return self.transform(
                self._source.request(**kwargs),
                ticker=ticker)
        else:
            ValueError("ticker must be a single symbol")

    def transform(self, data, **kwargs):

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

    def copy(self):
        ret = type(self)(
            strategy=self._strategy
        )

        return ret
