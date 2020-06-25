"""Translator for common file imports

These will often be very specific to the file being imported, but should
strive to still be as flexible as possible. These will often hold the format
translated to constant and try being adaptable with the data to fit it. So
it is more importat to begin with the output and then adapt to the input, not
the other way.
"""
import numpy as np
import pandas as pd

from dalio.translator import Translator
from dalio.base.constants import PRICE, DATE, ATTRIBUTE, TICKER


class StockStreamFileTranslator(Translator):
    """Create a DataFrame conforming to the STOCK_STREAM validator preset.

    The STOCK_STREAM preset includes:
        a) having a time series index,
        b) being a dataframe,
        c) having a multiindex column with levels named ATTRIBUTE and TICKER.
            Such that an imported excel file will have column names renamed
            that or assume a single column name row is of ticker names.

    Attributes:
        date_col (str): column name to get date data from.
        att_name (str): name of the attribute column if imported dataframe
            column has only one level.
    """

    def __init__(self, date_col=None, att_name=None):
        super().__init__()
        self.date_col = date_col
        self.att_name = att_name if att_name is not None else PRICE

    def run(self, **kwargs):
        """Request pandas data from file and format it into a dataframe that
        complies with the STOCK_STREAM validator preset

        Args:
            **kwargs: Optional request arguments
                TICKER: single ticker or iterable of tickers to filter for
                    in data.
        """

        ret = self._source.request(parse_dates=True)

        # define the index col
        if self.date_col is not None:
            icol = self.date_col
        else:
            icol = [col for col in ["DATE", "Date", "date", ret.columns[0]]
                    if col in ret][0]

        if ret[icol].dtype != np.dtype("datetime64[ns]"):
            raise ValueError("The specified date column or any column named \
                either 'DATE', 'Date', 'date' or the first column must be \
                of type datetime64[ns]")

        ret.set_index(icol, inplace=True)

        ret.index.set_names([DATE], inplace=True)

        if not isinstance(ret.columns, pd.MultiIndex):
            ret.columns = pd.MultiIndex.from_product(
                [[self.att_name], ret.columns])

        ret.columns.set_names([ATTRIBUTE, TICKER], inplace=True)

        tick = kwargs.get(TICKER, None)
        if isinstance(tick, str):
            ret = ret.loc(axis=1)[:, [tick]]
        elif isinstance(tick, (list, tuple)):
            ret = ret.loc(axis=1)[:, tick]

        return ret

    def copy(self, *args, **kwargs):
        return super().copy(
            *args,
            date_col=self.date_col,
            att_name=self.att_name,
            **kwargs
        )
