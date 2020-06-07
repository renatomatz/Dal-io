import pandas as pd

from dalio.base.constants import ATTRIBUTE, DATE, TICKER, CATEGORY, LAST_UPDATED, \
        SIC_CODE, SCALE, IS_DELISTED
from dalio.translator import Translator
from dalio.util import translate_df


class QuandlSharadarSF1Translator(Translator):

    def __init__(self):
        super().__init__()
        self.update_translations({
            "calendardate": DATE
        })

    def run(self, **kwargs):

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
