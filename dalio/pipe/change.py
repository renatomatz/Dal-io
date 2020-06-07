from typing import List, Union, Callable

from pandas import Series

from dalio.base.constants import PRICE, RETURNS
from dalio.pipe import Pipe
from dalio.validator import IS_PD_DF, HAS_COLS, STOCK_STREAM
from dalio.util import process_cols, process_new_colnames


class Change(Pipe):

    _PANDAS_PRESETS = ["pct_change", "diff"]

    _cols: List[str]
    _strategy: Union[str, Callable[[Series], Series]]
    _new_cols: Union[List[str], str]

    def __init__(self, cols=None, strategy="pct_change", new_cols=None):
        super().__init__()

        self._source\
            .add_desc(IS_PD_DF())\
            .add_desc(HAS_COLS(cols))

        self._cols = process_cols(cols)
        
        if isinstance(strategy, str)\
                and strategy not in Change._PANDAS_PRESETS:
            raise ValueError(f"Argument strategy must be one of\
                {Change._PANDAS_PRESETS}")

        self._strategy = strategy

        if isinstance(new_cols, list) and len(new_cols) != len(self._cols):
            raise ValueError(f"argument new_cols must either be a string or\
                a list with {len(self._cols)} elements")

        self._new_cols = new_cols

    def transform(self, data, **kwargs):

        data = data.copy()

        col_names = self._cols if self._cols is not None \
            else data.columns.to_list()

        new_col_names = process_new_colnames(col_names, self._new_cols)

        if self._strategy == "pct_change":
            # TODO: fix this for new values
            data.loc(axis=1)[new_col_names] = \
                    data[col_names].pct_change().fillna(0)
        elif self._strategy == "diff":
            data.loc(axis=1)[new_col_names] = \
                    data[col_names].diff().fillna(0)
        else:
            data.loc(axis=1)[new_col_names] = \
                    data[col_names].apply(self._strategy)

        return data

    def copy(self):
        ret = type(self)(
            cols=self._cols,
            strategy=self._strategy,
            new_cols=self._new_cols
        )

        return ret


def StockReturns(Change):

    def __init__(self):
        super().__init__(
            PRICE,
            func="pct_change",
            new_cols=[RETURNS]
        )

        self._source.clear_desc()
        self._source.add_desc(STOCK_STREAM)

    def transform(self, data, **kwargs):
        data = super().transform(data, **kwargs)

        col_names = self._cols if self._cols is not None \
            else data.columns.to_list()

        new_col_names = process_new_colnames(col_names, self._new_cols)

        data.loc(axis=1)[new_col_names] = \
            data[col_names].apply(lambda x: x * 100)

        return data
