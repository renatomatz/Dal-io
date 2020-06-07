import pandas as pd

from dalio.base.constants import ADJ_CLOSE, ATTRIBUTE, CLOSE, DATE, HIGH, LOW, OPEN, \
    TICKER, VOLUME

from dalio.translator import Translator
from dalio.util import translate_df


class YahooStockTranslator(Translator):

    def __init__(self):
        super().__init__()
        self._req_args.add(TICKER)
        self.update_translations({
            "Adj Close": ADJ_CLOSE,
            "Attributes": "attributes",
            "Close": CLOSE,
            "Date": DATE,
            "High": HIGH,
            "Low": LOW,
            "Open": OPEN,
            "Symbols": TICKER,
            "Volume": VOLUME
        })

    def run(self, **kwargs):

        if isinstance(kwargs[TICKER], str):
            kwargs[TICKER] = [kwargs[TICKER]]

        ret = self._source.request(**kwargs)

        # apply translations
        translate_df(self, ret, inplace=True)

        ret.columns.set_names([ATTRIBUTE, TICKER], inplace=True)

        return ret
