"""Define comps analysis models"""

from collections import OrderedDict

import numpy as np
import pandas as pd

from dalio.model import Model

from dalio.base.constants import TICKER, PORTFOLIO

from dalio.validator import (
    ELEMS_TYPE,
    IS_PD_DF,
    IS_TYPE,
)

from dalio.validator.presets import STOCK_INFO, STOCK_STREAM


class CompsData(Model):
    """Get a ticker's comps and their data.

    This model has two sources: comps_in and data_in. comps_in gets a
    ticker's comparative stocks. data_in sources ticker data given a "TICKER"
    keyword argument.
    """
    def __init__(self):
        """Initialize instance.

        comps_in is defined as a list of strings.
        data_in is defined as a pandas dataframe.
        """
        super().__init__()

        self._init_source([
            "comps_in"
            "data_in",
        ])

        self._get_source("comps_in")\
            .add_desc(IS_TYPE(list))\
            .add_desc(ELEMS_TYPE(str))

        self._get_source("data_in")\
            .add_desc(IS_PD_DF())

    def run(self, **kwargs):
        ticker = self._source_from("comps_in", **kwargs)
        kwargs[TICKER] = ticker
        return self._source_from("data_in", **kwargs)


class CompsInfo(CompsData):
    """Subclass to CompsData for getting comps stock information"""

    def __init__(self):
        """Initialize instance.

        data_in is also described as a stock info validator preset.
        """
        super().__init__()

        self._get_source("data_in")\
            .add_desc(STOCK_INFO)


class CompsFinancials(CompsData):
    """Subclass to CompsData for getting stock price information"""

    def __init__(self):
        """Initialize instance.

        data_in is also described as a stock stream validator preset.
        """
        super().__init__()

        self._get_source("data_in")\
            .add_desc(STOCK_STREAM)


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
        weights = self._source_from("weights_in", **kwargs)

        # Get stock data from available weights
        kwargs.pop("ticker", None)
        kwargs["ticker"] = [*weights.keys()]

        prices = self._source_from("data_in", **kwargs)\
            .fillna(0)

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
