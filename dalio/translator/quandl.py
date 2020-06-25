"""Define Translator instances for data imported from quandl.

These should be designed with both input and output in mind as quandl inputs
can, for a good extent, known from the table and query, both of which are
known from the time of request. This means that these translators should be
designed to be more specific to the query instead of being flexible.
"""
import pandas as pd

from dalio.base.constants import (
    ATTRIBUTE,
    DATE,
    TICKER,
    CATEGORY,
    LAST_UPDATED,
    SIC_CODE,
    SCALE,
    IS_DELISTED,
)

from dalio.translator import Translator
from dalio.util import translate_df


class QuandlSharadarSF1Translator(Translator):
    """Import and translate data from the SHARADAR/SF1 table"""

    def __init__(self):
        super().__init__()
        self.update_translations({
            "calendardate": DATE
        })

    def run(self, **kwargs):
        """Get input from quandl's SHARADAR/SF1 table, and format
        according to the STOCK_STREAM validator preset.
        """

        # often when columns are selected, key data is left out
        if "columns" in kwargs:
            # guarantees timedate index for imported stock data
            if "calendardate" not in kwargs["columns"]:
                kwargs["columns"].append("calendardate")

            # guarantees ticker will be identifieable on imported data
            if "ticker" not in kwargs["columns"]:
                kwargs["columns"].append("ticker")

        kwargs["query"] = "SHARADAR/SF1"

        # establish connection if not connected
        if not self._source.check():
            self._source.authenticate()

        # get data from quandl connection
        ret = self._source.request(**kwargs)

        # apply translations
        translate_df(self, ret, inplace=True)

        ret[DATE] = pd.DatetimeIndex(ret[DATE])
        ret = ret.pivot(index=DATE, columns=TICKER)
        ret.columns.set_names([ATTRIBUTE, TICKER], inplace=True)

        return ret


class QuandlTickerInfoTranslator(Translator):
    """Import and translate data from the SHARADAR/TICKERS table"""

    def __init__(self):
        super().__init__()
        self.update_translations({
            "lastupdated": LAST_UPDATED,
            "category": CATEGORY,
            "siccode": SIC_CODE,
            "scalemarketcap": SCALE,
            "isdelisted": IS_DELISTED
        })

    def run(self, **kwargs):
        """Get input from quandl's SHARADAR/TICKER table, and format
        according to the STOCK_INFO validator preset.
        """

        # often when columns are selected, key data is left out
        if "columns" in kwargs:
            # guarantees timedate index for imported stock data
            if "lastupdated" not in kwargs["columns"]:
                kwargs["columns"].append("lastupdated")

            # guarantees ticker will be identifieable on imported data
            if "ticker" not in kwargs["columns"]:
                kwargs["columns"].append("ticker")

        kwargs["query"] = "SHARADAR/TICKERS"

        # establish connection if not connected
        if not self._source.check():
            self._source.authenticate()

        # get data from quandl connection
        ret = self._source.request(**kwargs)

        # apply translations
        translate_df(self, ret, inplace=True)

        ret[LAST_UPDATED] = pd.DatetimeIndex(ret[LAST_UPDATED])
        ret.sort_values(LAST_UPDATED, inplace=True)
        ret.columns.set_names([ATTRIBUTE], inplace=True)

        return ret
