import numpy as np
import pandas as pd

from dalio.translator import Translator
from dalio.base.constants import PRICE, DATE, ATTRIBUTE, TICKER


class StockStreamFileTranslator(Translator):

    def __init__(self):
        super().__init__()

    def run(self, **kwargs):

        ret = self._source.request(parse_dates=True)

        # define the index col
        icol = [col for col in ["DATE", "Date", "date", ret.columns[0]] 
                if col in ret][0]

        if ret[icol].dtype != np.dtype("datetime64[ns]"):
            raise ValueError("File must have a date columnn named either\
                    'DATE', 'Date' or 'date' or be the first column")

        ret.set_index(icol, inplace=True)

        ret.index.set_names([DATE], inplace=True)

        if not isinstance (ret.columns, pd.MultiIndex):
            ret.columns = pd.MultiIndex.from_product(
                    [[PRICE], ret.columns])

        ret.columns.set_names([ATTRIBUTE, TICKER], inplace=True)

        tick = kwargs.get(TICKER, None)
        if isinstance(tick, str):
            ret = ret.loc(axis=1)[:, [tick]]
        elif isinstance(tick, (list, tuple)):
            ret = ret.loc(axis=1)[:, tick]

        return ret
